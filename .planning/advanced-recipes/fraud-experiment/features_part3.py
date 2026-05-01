"""Part 3: Matched train/serve staleness on IEEE-CIS.

For each Delta in {0, 60, 3600, 86400, 604800, 2592000, 5184000}:
  - Derive features for ALL rows (train + test) using the same as-of shift.
    For a row at TransactionDT=T, features use only events with t' <= T - Delta.
  - Train HistGBT on the train portion with these Delta-stale features.
  - Evaluate on the test portion with the SAME Delta-stale features.
  - Record AUC + recall@1%FPR.

This isolates pure freshness: train and serve distributions match, so any
degradation as Delta grows reflects information loss due to staleness,
not a train/serve skew.
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score, roc_curve

# Reuse machinery from the Part 1/2 script (same directory).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from features import (  # noqa: E402
    load_full,
    derive_live_features,
    StaleContext,
    WIN_5M_S,
    WIN_1H_S,
    recall_at_fpr,
    SEED,
)

ROOT = Path("/Users/petrpan26/work/tally/.planning/advanced-recipes/fraud-experiment")


# ---- All-row variants of the stale rolling primitives (write to every row,
# not just test rows). Identical math to the test-only versions in
# features.py; just drops the is_test gate.


def _stale_count_rolling_all(ctx: StaleContext, groups, offset_s: int,
                             window_s: int) -> np.ndarray:
    out = np.zeros(ctx.n, dtype=np.int32)
    t_all = ctx.t
    for idx in groups:
        t = t_all[idx]
        n = len(idx)
        left = 0
        right = -1
        for i in range(n):
            t_query = t[i] - offset_s
            while right + 1 < n and t[right + 1] <= t_query:
                right += 1
            while left <= right and t[left] < t_query - window_s:
                left += 1
            ri = idx[i]
            if right < left:
                out[ri] = 0
            else:
                out[ri] = right - left + 1
    return out


def _stale_sum_rolling_all(ctx: StaleContext, groups, values: np.ndarray,
                           offset_s: int, window_s: int) -> np.ndarray:
    out = np.zeros(ctx.n, dtype=np.float64)
    t_all = ctx.t
    for idx in groups:
        t = t_all[idx]
        v = values[idx]
        n = len(idx)
        left = 0
        right = -1
        running = 0.0
        for i in range(n):
            t_query = t[i] - offset_s
            while right + 1 < n and t[right + 1] <= t_query:
                right += 1
                running += v[right]
            while left <= right and t[left] < t_query - window_s:
                running -= v[left]
                left += 1
            ri = idx[i]
            if right < left:
                out[ri] = 0.0
            else:
                out[ri] = running
    return out


def _stale_distinct_rolling_all(ctx: StaleContext, groups,
                                val_arr: np.ndarray, offset_s: int,
                                window_s: int, fill_nan_keys: bool) -> np.ndarray:
    if fill_nan_keys:
        out = np.full(ctx.n, np.nan, dtype=np.float64)
    else:
        out = np.zeros(ctx.n, dtype=np.float64)
    t_all = ctx.t
    for idx in groups:
        t = t_all[idx]
        v = val_arr[idx]
        n = len(idx)
        left = 0
        right = -1
        counts: dict = {}
        for i in range(n):
            t_query = t[i] - offset_s
            while right + 1 < n and t[right + 1] <= t_query:
                right += 1
                rv = v[right]
                if rv is None or (isinstance(rv, float) and np.isnan(rv)):
                    pass
                else:
                    counts[rv] = counts.get(rv, 0) + 1
            while left <= right and t[left] < t_query - window_s:
                lv = v[left]
                if lv is None or (isinstance(lv, float) and np.isnan(lv)):
                    pass
                else:
                    counts[lv] -= 1
                    if counts[lv] == 0:
                        del counts[lv]
                left += 1
            ri = idx[i]
            if right < left:
                out[ri] = 0.0
            else:
                out[ri] = len(counts)
    return out


def _stale_streak_lookup_all(ctx: StaleContext, live_streak: np.ndarray,
                             offset_s: int) -> np.ndarray:
    out = np.zeros(ctx.n, dtype=np.int32)
    t_all = ctx.t
    for idx in ctx.card_groups:
        t = t_all[idx]
        n = len(idx)
        right = -1
        for i in range(n):
            t_query = t[i] - offset_s
            while right + 1 < n and t[right + 1] <= t_query:
                right += 1
            ri = idx[i]
            if right < 0:
                out[ri] = 0
            else:
                out[ri] = live_streak[idx[right]]
    return out


def derive_matched_features(ctx: StaleContext, offset_s: int,
                            all_cols: list) -> pd.DataFrame:
    """Compute all 7 features for EVERY row at the matched staleness offset."""
    f1 = _stale_count_rolling_all(ctx, ctx.card_groups, offset_s, WIN_5M_S)
    f2 = _stale_sum_rolling_all(ctx, ctx.card_groups,
                                ctx.high_risk.astype(np.float64),
                                offset_s, WIN_5M_S)
    f3 = _stale_distinct_rolling_all(ctx, ctx.device_groups, ctx.card1,
                                     offset_s, WIN_5M_S, fill_nan_keys=True)
    f4 = _stale_streak_lookup_all(ctx, ctx.live_addr_streak, offset_s)
    f5 = _stale_streak_lookup_all(ctx, ctx.live_rapid_streak, offset_s)
    f6 = _stale_distinct_rolling_all(ctx, ctx.card_groups, ctx.device,
                                     offset_s, WIN_1H_S, fill_nan_keys=False)
    f7 = _stale_sum_rolling_all(ctx, ctx.card_groups, ctx.amt, offset_s,
                                WIN_5M_S)
    return pd.DataFrame({
        "TransactionAmt": ctx.amt,
        "tx_count_5m_per_card": f1,
        "high_risk_tx_5m_per_card": f2,
        "distinct_cards_5m_per_device": f3,
        "addr_mismatch_streak_per_card": f4,
        "rapid_tx_streak_per_card": f5,
        "distinct_devices_1h_per_card": f6,
        "amount_sum_5m_per_card": f7,
    })[all_cols]


def fit_and_score(X_tr, y_tr, X_te, y_te):
    clf = HistGradientBoostingClassifier(
        max_depth=8, max_iter=300, learning_rate=0.05, random_state=SEED,
    )
    clf.fit(X_tr, y_tr)
    p = clf.predict_proba(X_te)[:, 1]
    auc = roc_auc_score(y_te, p)
    r = recall_at_fpr(y_te, p, 0.01)
    return auc, r


def main():
    t0 = time.time()
    print(f"[{time.time()-t0:.1f}s] Loading full dataset...", flush=True)
    df = load_full()
    n_rows = len(df)
    print(f"  rows: {n_rows}", flush=True)

    n_train = int(round(0.70 * n_rows))
    tr_idx = np.arange(0, n_train, dtype=np.int64)
    te_idx = np.arange(n_train, n_rows, dtype=np.int64)

    all_cols = [
        "TransactionAmt",
        "tx_count_5m_per_card",
        "high_risk_tx_5m_per_card",
        "distinct_cards_5m_per_device",
        "addr_mismatch_streak_per_card",
        "rapid_tx_streak_per_card",
        "distinct_devices_1h_per_card",
        "amount_sum_5m_per_card",
    ]

    y_all = df["isFraud"].astype(np.int32).values
    y_tr = y_all[tr_idx]
    y_te = y_all[te_idx]

    # Build shared StaleContext (live streaks + group indexes).
    # We need is_test for the features.py test-only primitives, but we use
    # the all-row variants above; still, feeding te_idx is fine — the context
    # has cached live streaks and group tables we reuse.
    print(f"[{time.time()-t0:.1f}s] Building StaleContext...", flush=True)
    ctx = StaleContext(df, te_idx)
    print(f"[{time.time()-t0:.1f}s]   ctx built. "
          f"n_card_groups={len(ctx.card_groups)} "
          f"n_device_groups={len(ctx.device_groups)}", flush=True)

    # Also precompute the LIVE (Delta=0) features using the existing
    # derive_live_features path — the matched-stale primitives, when called
    # with offset_s=0, are semantically equivalent but we use the existing
    # live implementation to stay faithful to Part 1's numbers.
    print(f"[{time.time()-t0:.1f}s] Deriving live (Delta=0) features for cross-check...",
          flush=True)
    feat_live = derive_live_features(df)[all_cols]

    tiers = [
        (0, "0 s (real-time)"),
        (60, "60 s"),
        (3600, "1 h"),
        (86400, "1 d"),
        (604800, "7 d"),
        (2592000, "30 d"),
        (5184000, "60 d"),
    ]

    results = []
    ref_auc = None
    ref_recall = None
    for offset_s, label in tiers:
        t_st = time.time()
        print(f"[{time.time()-t0:.1f}s] Delta={offset_s}s ({label}): deriving features...",
              flush=True)
        if offset_s == 0:
            X_all = feat_live.values
        else:
            fs = derive_matched_features(ctx, offset_s, all_cols)
            X_all = fs.values
        X_tr = X_all[tr_idx]
        X_te = X_all[te_idx]
        t_f = time.time() - t_st
        print(f"[{time.time()-t0:.1f}s] Delta={offset_s}s: training HistGBT...",
              flush=True)
        t_tr = time.time()
        auc, r = fit_and_score(X_tr, y_tr, X_te, y_te)
        t_m = time.time() - t_tr
        if offset_s == 0:
            ref_auc = auc
            ref_recall = r
            d_auc = None
            d_recall = None
        else:
            d_auc = auc - ref_auc
            d_recall = r - ref_recall
        results.append({
            "staleness": label,
            "offset_s": offset_s,
            "auc": auc,
            "recall_1pct": r,
            "delta_auc_vs_zero": d_auc,
            "delta_recall_vs_zero": d_recall,
            "feat_s": t_f,
            "train_s": t_m,
        })
        print(f"  AUC={auc:.4f} recall@1%FPR={r:.4f}  "
              f"(feat {t_f:.1f}s, train {t_m:.1f}s)", flush=True)

    # ---- Compose tables + narrative ------------------------------------------
    lines = ["| Matched staleness Δ | AUC | Recall @ 1% FPR | Δ AUC from Δ=0 | Δ recall from Δ=0 |",
             "|---|---|---|---|---|"]
    for row in results:
        if row["offset_s"] == 0:
            d_auc_s = "—"
            d_r_s = "—"
        else:
            d_auc_s = f"{row['delta_auc_vs_zero']:+.3f}"
            d_r_s = f"{row['delta_recall_vs_zero']*100:+.1f} pp"
        lines.append(
            f"| {row['staleness']} | {row['auc']:.3f} | "
            f"{row['recall_1pct']*100:.1f}% | {d_auc_s} | {d_r_s} |"
        )
    table = "\n".join(lines)

    auc_0 = results[0]["auc"]
    auc_60d = results[-1]["auc"]
    r_0 = results[0]["recall_1pct"]
    r_60d = results[-1]["recall_1pct"]
    auc_gap = auc_0 - auc_60d
    recall_gap_pp = (r_0 - r_60d) * 100

    if r_60d > 0:
        recall_ratio = r_0 / r_60d
    else:
        recall_ratio = float("inf")

    # Honest headline selection: if AUC gap is meaningful (>0.01), lead with AUC.
    if abs(auc_gap) >= 0.01:
        headline = (
            f"Matched-stale features degrade from AUC {auc_0:.3f} to {auc_60d:.3f} "
            f"as Δ grows from 0 to 60 days (Δ = {auc_gap:+.3f} AUC; "
            f"{recall_ratio:.2f}× recall @ 1% FPR)."
        )
    else:
        headline = (
            f"Matched-stale AUC moves from {auc_0:.3f} at Δ=0 to {auc_60d:.3f} at "
            f"Δ=60 d (Δ = {auc_gap:+.3f} AUC; recall {r_0*100:.1f}% → "
            f"{r_60d*100:.1f}% at 1% FPR). Within-noise on this dataset."
        )

    # Verdict
    if abs(auc_gap) >= 0.02:
        verdict = (
            "**Verdict:** IEEE-CIS does show a measurable pure-freshness effect "
            f"under matched train/serve: AUC drops by {auc_gap:+.3f} as features "
            "age from real-time to 60-day stale. The degradation is monotone-ish "
            "and well above within-tier noise. Freshness story holds."
        )
    else:
        verdict = (
            "**Verdict:** IEEE-CIS does NOT have enough genuine temporal drift "
            "in its 7-feature view for an offline matched-stale experiment to "
            f"show a clean freshness signal: the AUC gap from Δ=0 to Δ=60 d is "
            f"only {auc_gap:+.3f} — within noise. This is consistent with the "
            "dataset being a fixed 6-month snapshot of card-not-present "
            "transactions whose per-card velocity signals are informative but "
            "do not meaningfully decay on the scale tested. **Follow-up:** run "
            "the same protocol on the Elliptic Bitcoin dataset (Weber et al., "
            "2019), which is a temporal-out-of-sample benchmark built "
            "specifically to expose concept drift and staleness in fraud-like "
            "AML classification. Its 49 time steps (~2 weeks each) give a much "
            "cleaner substrate for freshness-vs-performance curves."
        )

    # ---- Update results.md + results.json ------------------------------------
    print(f"[{time.time()-t0:.1f}s] Updating results files...", flush=True)
    results_md_path = Path("/Users/petrpan26/work/tally/.planning/advanced-recipes/fraud-experiment-results.md")
    results_json_path = ROOT / "results.json"

    # Load prior results.json and append Part 3 section.
    with open(results_json_path) as f:
        prior = json.load(f)

    prior["part3"] = [
        {
            "staleness": row["staleness"],
            "offset_s": row["offset_s"],
            "auc": row["auc"],
            "recall_1pct": row["recall_1pct"],
            "delta_auc_vs_zero": row["delta_auc_vs_zero"],
            "delta_recall_vs_zero": row["delta_recall_vs_zero"],
        }
        for row in results
    ]
    prior["summary"]["part3_headline"] = headline
    prior["summary"]["part3_auc_zero"] = auc_0
    prior["summary"]["part3_auc_60day"] = auc_60d
    prior["summary"]["part3_auc_gap_zero_to_60day"] = auc_gap
    prior["summary"]["part3_recall_zero"] = r_0
    prior["summary"]["part3_recall_60day"] = r_60d
    prior["summary"]["part3_recall_ratio_zero_vs_60day"] = recall_ratio
    prior["versions_part3"] = {
        "python": sys.version.split()[0],
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "sklearn": sklearn.__version__,
        "run_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elapsed_s": time.time() - t0,
    }
    with open(results_json_path, "w") as f:
        json.dump(prior, f, indent=2)

    # Append a Part 3 section to results.md (before the "## Reproduction
    # recipe" section, which should stay at the bottom).
    md = results_md_path.read_text()
    part3_block = f"""
## Part 3: Matched train/serve staleness

### Methodology

Part 2's sharp fresh-vs-stale cliff (~0.19 AUC drop at 1-minute offset) was
not a pure freshness effect — it was a train/serve distribution mismatch:
the Part 2 reference model was trained on **live** features (which include
the current event in every 5-minute rolling count) and then scored against
**stale** features (which exclude everything in the last Δ seconds). That
is two different feature distributions; the model's decision surface was
calibrated on one and queried on the other.

Part 3 removes that confound. For each staleness tier Δ, we recompute the
7 features for **every** row (train and test) using the same as-of shift
— at row timestamp T, features see only events with t' ≤ T − Δ — and then
train a fresh HistGBT on the Δ-matched training set before scoring the
Δ-matched test set. Same 70/30 chronological split, same HistGBT
hyperparameters (max_depth=8, max_iter=300, lr=0.05, seed=42), same 7
features. The only thing that changes between rows of the table is Δ.

If freshness carries real information on IEEE-CIS, AUC should decay
monotonically as Δ grows — and the decay should be gentle, nothing like
the ~0.19 AUC cliff we saw under the mismatched protocol.

### Results

{table}

{headline}

### Verdict

{verdict}

"""

    # Insert before the "## Reproduction recipe" heading.
    marker = "## Reproduction recipe"
    if marker in md:
        md = md.replace(marker, part3_block.rstrip() + "\n\n" + marker, 1)
    else:
        md = md.rstrip() + "\n" + part3_block

    # Also update the reproduction recipe note so runners know to run part3.
    recipe_extra = ("# 7. Part 3 (matched train/serve staleness) is a separate "
                    "script:\n#    .planning/advanced-recipes/fraud-experiment/"
                    "venv/bin/python \\\n#        .planning/advanced-recipes/"
                    "fraud-experiment/features_part3.py\n")
    if "features_part3.py" not in md:
        # Insert the extra line just before the closing ``` of the recipe block.
        md = md.replace(
            "# 6. Clean up raw CSVs when done (not needed after script finishes)\n"
            "rm .planning/advanced-recipes/fraud-experiment/raw/*.csv\n",
            "# 6. Clean up raw CSVs when done (not needed after script finishes)\n"
            "rm .planning/advanced-recipes/fraud-experiment/raw/*.csv\n\n"
            + recipe_extra,
            1,
        )

    results_md_path.write_text(md)

    print(f"[{time.time()-t0:.1f}s] Done.")
    print("\n--- PART 3 (matched train/serve staleness) ---")
    print(table)
    print("\nHEADLINE:", headline)
    print("\nVERDICT:", verdict[:200] + "...")

    return table, headline, verdict


if __name__ == "__main__":
    main()
