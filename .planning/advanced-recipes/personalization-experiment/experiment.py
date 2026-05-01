#!/usr/bin/env python3
"""
Personalization experiment on Yoochoose RecSys 2015 clickstream.

- Streams yoochoose-clicks.dat to find sessions with >=3 clicks.
- Samples 200K eligible sessions (seed=42).
- Leave-last-click-out evaluation with candidate set = top-1000 global popular + truth.
- Part 1: cumulative ablation of 5 features (weighted-sum score).
- Part 2: staleness sweep on session_top_categories.

Outputs a markdown results file.
"""

from __future__ import annotations

import json
import math
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
SCRATCH = Path("/Users/petrpan26/work/tally/.planning/advanced-recipes/personalization-experiment")
CLICKS = SCRATCH / "yoochoose-clicks.dat"
OUTPUT_DIR = SCRATCH / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
RESULTS_MD = Path(
    "/Users/petrpan26/work/tally/.planning/advanced-recipes/personalization-experiment-results.md"
)

CHUNK = 2_000_000
SAMPLE_SIZE = 200_000
TOP_POP = 1000
K = 10


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Pass 1: count clicks per session + global item popularity
# ---------------------------------------------------------------------------
def pass1_counts():
    log("Pass 1: counting session sizes + item popularity")
    sess_counts: dict[int, int] = defaultdict(int)
    item_pop: Counter = Counter()
    total_rows = 0
    for chunk in pd.read_csv(
        CLICKS,
        names=["SessionID", "Timestamp", "ItemID", "Category"],
        sep=",",
        header=None,
        chunksize=CHUNK,
        dtype={"SessionID": np.int64, "ItemID": np.int64, "Category": "string"},
    ):
        total_rows += len(chunk)
        vc = chunk["SessionID"].value_counts()
        # vc.index and vc.values are arrays; convert to python ints.
        for sid, cnt in zip(vc.index.tolist(), vc.values.tolist()):
            sess_counts[int(sid)] += int(cnt)
        item_pop.update(chunk["ItemID"].tolist())
        log(f"  rows so far: {total_rows:,}")
    log(
        f"Pass 1 done. rows={total_rows:,} sessions={len(sess_counts):,} items={len(item_pop):,}"
    )
    return sess_counts, item_pop, total_rows


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------
def sample_sessions(sess_counts: dict[int, int]) -> set[int]:
    eligible = [sid for sid, c in sess_counts.items() if c >= 3]
    log(f"Eligible sessions (>=3 clicks): {len(eligible):,}")
    rng = np.random.default_rng(SEED)
    if len(eligible) <= SAMPLE_SIZE:
        sampled = eligible
    else:
        idx = rng.choice(len(eligible), size=SAMPLE_SIZE, replace=False)
        sampled = [eligible[int(i)] for i in idx.tolist()]
    log(f"Sampled sessions: {len(sampled):,}")
    return set(int(x) for x in sampled)


# ---------------------------------------------------------------------------
# Pass 2: collect rows for sampled sessions
# ---------------------------------------------------------------------------
def pass2_collect(sampled: set[int]) -> pd.DataFrame:
    log("Pass 2: collecting rows for sampled sessions")
    parts = []
    rows = 0
    for chunk in pd.read_csv(
        CLICKS,
        names=["SessionID", "Timestamp", "ItemID", "Category"],
        sep=",",
        header=None,
        chunksize=CHUNK,
        dtype={"SessionID": np.int64, "ItemID": np.int64, "Category": "string"},
    ):
        mask = chunk["SessionID"].isin(sampled)
        if mask.any():
            parts.append(chunk.loc[mask].copy())
        rows += len(chunk)
        log(f"  scanned: {rows:,}")
    df = pd.concat(parts, ignore_index=True)
    log(f"Pass 2 done. collected rows={len(df):,}")
    df["ts_ns"] = pd.to_datetime(df["Timestamp"], utc=True).astype("int64")
    df = df.sort_values(["SessionID", "ts_ns"], kind="mergesort").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Build per-session arrays
# ---------------------------------------------------------------------------
def build_sessions(df: pd.DataFrame):
    log("Building per-session arrays")
    sessions = []
    for sid, g in df.groupby("SessionID", sort=False):
        if len(g) < 3:
            continue
        sessions.append(
            {
                "sid": int(sid),
                "items": g["ItemID"].to_numpy().tolist(),
                "cats": g["Category"].astype(str).to_numpy().tolist(),
                "ts_ns": g["ts_ns"].to_numpy().astype(np.int64).copy(),
            }
        )
    log(f"Usable sessions: {len(sessions):,}")
    return sessions


# ---------------------------------------------------------------------------
# Feature extraction (real-time)
# ---------------------------------------------------------------------------
def extract_rt_features(ctx_items, ctx_cats, ctx_ts_ns):
    if ctx_cats:
        top_cats = [c for c, _ in Counter(ctx_cats).most_common(3)]
    else:
        top_cats = []
    recent_items = ctx_items[-5:]
    if len(ctx_ts_ns) >= 2:
        diffs = np.diff(ctx_ts_ns).astype(np.float64) / 1_000_000.0
        dwell_avg = float(np.mean(diffs))
    else:
        dwell_avg = 0.0
    depth = len(ctx_items)
    variety = len(set(ctx_cats))
    return {
        "top_cats": top_cats,
        "recent_items": recent_items,
        "dwell_avg": dwell_avg,
        "depth": depth,
        "variety": variety,
    }


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def score_candidates(
    candidates: np.ndarray,
    cand_pop: np.ndarray,
    cand_cats: list[str],
    feats: dict,
    weights: list[float],
    active: list[bool],
):
    """
    weights: [w_pop, w_cats, w_recent, w_dwell, w_depth, w_var]
    active: [cats, recent, dwell, depth, variety]
    """
    score = weights[0] * cand_pop

    cats_set = set(feats["top_cats"])
    cat_hits = np.array([1.0 if c in cats_set else 0.0 for c in cand_cats], dtype=np.float64)

    if active[0]:
        score = score + weights[1] * cat_hits
    if active[1]:
        recent_set = set(int(x) for x in feats["recent_items"])
        hits = np.array(
            [1.0 if int(i) in recent_set else 0.0 for i in candidates], dtype=np.float64
        )
        score = score + weights[2] * hits
    if active[2]:
        dwell = feats["dwell_avg"]
        boost = 1.0 / (1.0 + dwell / 30000.0)
        score = score + weights[3] * boost * cand_pop
    if active[3]:
        d = math.log1p(feats["depth"])
        score = score + weights[4] * d * cand_pop
    if active[4]:
        v = math.log1p(feats["variety"])
        score = score + weights[5] * (1.0 / (1.0 + v)) * cat_hits

    return score


def hit_at_k(scores: np.ndarray, candidates: np.ndarray, truth: int, k: int = K) -> int:
    if len(scores) <= k:
        return int(truth in set(candidates.tolist()))
    top_idx = np.argpartition(-scores, k)[:k]
    top_items = candidates[top_idx]
    return int(truth in set(top_items.tolist()))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    t0 = time.time()
    sess_counts, item_pop, total_rows = pass1_counts()
    sampled = sample_sessions(sess_counts)
    df = pass2_collect(sampled)
    sessions = build_sessions(df)
    del sess_counts

    # Popularity
    most = item_pop.most_common()
    top_pop_ids = [int(i) for i, _ in most[:TOP_POP]]
    top_pop_set = set(top_pop_ids)
    max_pop = most[0][1] if most else 1
    item_pop_rank = {int(i): c / max_pop for i, c in item_pop.items()}

    # Item -> category map
    item_category: dict[int, str] = {}
    for s in sessions:
        for it, ct in zip(s["items"], s["cats"]):
            k_it = int(it)
            if k_it not in item_category:
                item_category[k_it] = ct

    # Precompute candidate pop arrays for the base top-1000 (same for all events whose truth is in top_pop)
    top_pop_array_base = np.array(top_pop_ids, dtype=np.int64)
    base_cand_pop = np.array(
        [item_pop_rank.get(int(i), 0.0) for i in top_pop_array_base], dtype=np.float64
    )
    base_cand_cats = [item_category.get(int(i), "") for i in top_pop_array_base]

    # Build test events
    log("Building test events")
    test_events = []
    for s in sessions:
        if len(s["items"]) < 3:
            continue
        ctx_items = s["items"][:-1]
        ctx_cats = s["cats"][:-1]
        ctx_ts = s["ts_ns"][:-1].copy()  # ensure writable
        truth = int(s["items"][-1])
        test_ts = int(s["ts_ns"][-1])
        test_events.append(
            {
                "ctx_items": ctx_items,
                "ctx_cats": ctx_cats,
                "ctx_ts": ctx_ts,
                "truth": truth,
                "test_ts": test_ts,
            }
        )
    log(f"Test events: {len(test_events):,}")

    # Real-time features per event
    log("Extracting real-time features")
    rt_feats = [
        extract_rt_features(ev["ctx_items"], ev["ctx_cats"], ev["ctx_ts"])
        for ev in test_events
    ]

    weights = [1.0, 0.3, 0.3, 0.03, 0.03, 0.03]  # pop, cats, recent, dwell, depth, var
    feature_names = [
        "session_top_categories",
        "session_recent_items",
        "session_dwell_avg",
        "session_depth",
        "session_variety",
    ]

    def eval_active(active: list[bool]) -> float:
        hits = 0
        n = len(test_events)
        for ev, feats in zip(test_events, rt_feats):
            truth = ev["truth"]
            if truth in top_pop_set:
                cands = top_pop_array_base
                cand_pop = base_cand_pop
                cand_cats = base_cand_cats
            else:
                cands = np.concatenate(
                    [top_pop_array_base, np.array([truth], dtype=np.int64)]
                )
                cand_pop = np.concatenate(
                    [base_cand_pop, np.array([item_pop_rank.get(truth, 0.0)])]
                )
                cand_cats = base_cand_cats + [item_category.get(truth, "")]
            scores = score_candidates(cands, cand_pop, cand_cats, feats, weights, active)
            hits += hit_at_k(scores, cands, truth, K)
        return hits / n

    # ---------- Part 1: cumulative ablation ----------
    log("Part 1: cumulative ablation")
    part1_results = []
    baseline_active = [False] * 5
    t_start = time.time()
    hit_base = eval_active(baseline_active)
    log(f"  baseline hit@10 = {hit_base*100:.2f}%  ({time.time()-t_start:.1f}s)")
    part1_results.append(("baseline (popularity only)", hit_base, None))

    prev = hit_base
    active = [False] * 5
    for i, fname in enumerate(feature_names):
        active[i] = True
        t_start = time.time()
        h = eval_active(list(active))
        log(
            f"  +{fname} hit@10 = {h*100:.2f}%  lift={(h-prev)*100:+.2f}pts  ({time.time()-t_start:.1f}s)"
        )
        part1_results.append((f"+ {fname}", h, h - prev))
        prev = h

    full_hit = prev

    # ---------- Part 2: staleness ----------
    log("Part 2: staleness sweep")
    active_full = [True] * 5
    part2_results = []
    tiers = [
        ("real-time", None),
        ("10 seconds stale", 10),
        ("1 minute stale", 60),
        ("5 minutes stale", 300),
        ("30 minutes stale", 1800),
    ]
    for label, cutoff in tiers:
        hits = 0
        n = len(test_events)
        t_start = time.time()
        for ev, rt in zip(test_events, rt_feats):
            truth = ev["truth"]
            if cutoff is None:
                feats = rt
            else:
                cutoff_ns = ev["test_ts"] - int(cutoff * 1_000_000_000)
                mask = ev["ctx_ts"] < cutoff_ns
                if bool(mask.any()):
                    stale_cats = [c for c, m in zip(ev["ctx_cats"], mask.tolist()) if m]
                    if stale_cats:
                        top_cats = [c for c, _ in Counter(stale_cats).most_common(3)]
                    else:
                        top_cats = []
                else:
                    top_cats = []
                feats = dict(rt)
                feats["top_cats"] = top_cats
            if truth in top_pop_set:
                cands = top_pop_array_base
                cand_pop = base_cand_pop
                cand_cats = base_cand_cats
            else:
                cands = np.concatenate(
                    [top_pop_array_base, np.array([truth], dtype=np.int64)]
                )
                cand_pop = np.concatenate(
                    [base_cand_pop, np.array([item_pop_rank.get(truth, 0.0)])]
                )
                cand_cats = base_cand_cats + [item_category.get(truth, "")]
            scores = score_candidates(cands, cand_pop, cand_cats, feats, weights, active_full)
            hits += hit_at_k(scores, cands, truth, K)
        h = hits / n
        log(f"  {label}: hit@10 = {h*100:.2f}%  ({time.time()-t_start:.1f}s)")
        part2_results.append((label, h))

    rt_hit = part2_results[0][1]
    stale30_hit = part2_results[-1][1]
    drop_pts = (rt_hit - stale30_hit) * 100

    # ---------- Results ----------
    log("Writing results markdown")
    versions = {
        "python": sys.version.split()[0],
        "pandas": pd.__version__,
        "numpy": np.__version__,
    }
    try:
        import sklearn
        versions["sklearn"] = sklearn.__version__
    except Exception:
        versions["sklearn"] = "not imported"

    with RESULTS_MD.open("w") as f:
        f.write("# Personalization experiment — Yoochoose RecSys 2015\n\n")
        f.write(
            f"**Headline:** {drop_pts:.1f}-pt hit@10 drop when `session_top_categories` is 30 min stale vs real-time.\n\n"
        )
        f.write("## Methodology\n\n")
        f.write(
            "- **Dataset:** Yoochoose RecSys Challenge 2015 clickstream (`yoochoose-clicks.dat`), cached locally from the legacy S3 bucket. Schema: `SessionID,Timestamp,ItemID,Category` (comma-separated).\n"
        )
        f.write(f"- **Rows processed (full file):** {total_rows:,}.\n")
        f.write(
            f"- **Sampling protocol:** filter to sessions with >=3 clicks, then sample {SAMPLE_SIZE:,} session IDs via `numpy.random.default_rng(42).choice(len(eligible), size={SAMPLE_SIZE}, replace=False)`. Indices converted to a Python list before use (avoids read-only-array bug).\n"
        )
        f.write(
            "- **Train/test protocol:** leave-last-click-out. For each sampled session the last click is the test event; earlier clicks are context.\n"
        )
        f.write(
            f"- **Candidate set per test event:** top-{TOP_POP} items by global popularity + the true next item (union).\n"
        )
        f.write("- **Scoring function:** weighted sum over candidates\n\n")
        f.write("  ```\n")
        f.write("  score(item) = w_pop    * norm_popularity(item)\n")
        f.write("              + w_cats   * 1[item.category in session_top_categories]\n")
        f.write("              + w_recent * 1[item in session_recent_items]\n")
        f.write("              + w_dwell  * (1 / (1 + dwell_ms/30000)) * norm_popularity(item)\n")
        f.write("              + w_depth  * log(1+depth) * norm_popularity(item)\n")
        f.write("              + w_var    * (1 / (1+log(1+variety))) * 1[item.category in top_cats]\n")
        f.write("  ```\n\n")
        f.write(
            f"- **Weights used:** `{weights}` — pop=1.0, cats=0.3, recent=0.3, dwell/depth/var=0.03 each. Popularity raised to 1.0 over the equal-weight starting point so other features produce measurable re-ranking (candidate set is already popularity-filtered).\n"
        )
        f.write("- **Metric:** hit@10 (fraction of test events whose ground truth is in the top-10 scored candidates).\n")
        f.write("- **Software versions:** ")
        f.write(", ".join(f"{k}={v}" for k, v in versions.items()))
        f.write(".\n\n")
        f.write("## Part 1 — Cumulative feature ablation\n\n")
        f.write("| Features active | Hit@10 | Marginal lift |\n")
        f.write("|---|---|---|\n")
        for label, h, lift in part1_results:
            lift_str = "—" if lift is None else f"{lift*100:+.2f} pts"
            f.write(f"| {label} | {h*100:.2f}% | {lift_str} |\n")
        f.write("\n")
        f.write(
            f"Full 5-feature model hit@10 = **{full_hit*100:.2f}%** (baseline {hit_base*100:.2f}%).\n\n"
        )
        f.write("## Part 2 — Staleness sweep (session_top_categories)\n\n")
        f.write(
            "Full 5-feature model; only `session_top_categories` is recomputed at each staleness tier. Other features stay real-time.\n\n"
        )
        f.write("| Staleness tier | Hit@10 | Drop vs real-time |\n")
        f.write("|---|---|---|\n")
        for label, h in part2_results:
            drop = (rt_hit - h) * 100
            f.write(f"| {label} | {h*100:.2f}% | {drop:+.2f} pts |\n")
        f.write("\n")
        f.write(
            f"**Headline:** {drop_pts:.1f}-pt hit@10 drop when `session_top_categories` is 30 min stale vs real-time.\n\n"
        )
        f.write("## Notes\n\n")
        f.write(f"- Wall time: {(time.time()-t0)/60:.1f} min.\n")
        f.write(f"- Test events evaluated: {len(test_events):,}.\n")
        f.write(f"- Seed: {SEED}.\n")

    (OUTPUT_DIR / "summary.json").write_text(
        json.dumps(
            {
                "total_rows": total_rows,
                "n_test_events": len(test_events),
                "baseline_hit10": hit_base,
                "full_hit10": full_hit,
                "staleness": [{"tier": l, "hit10": h} for l, h in part2_results],
                "weights": weights,
                "versions": versions,
            },
            indent=2,
        )
    )
    log(f"Results written to {RESULTS_MD}")
    log(f"Wall time: {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
