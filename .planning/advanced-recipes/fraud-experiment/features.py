"""
IEEE-CIS fraud detection: beava-style feature ablation + staleness experiment.

TIME-SPLIT run (590K rows, seed=42, chronological 70/30 train/test split).

Protocol:
  1. Load train_transaction.csv + train_identity.csv (DeviceInfo, id_30 join).
  2. Full dataset (no sub-sampling beyond the chronological split).
  3. Sort by TransactionDT; earliest 70% -> train, latest 30% -> test.
  4. Derive 7 beava-analog features LIVE (offset=0) on the full dataframe
     via rolling windows. Features use the full history available at row t;
     because train and test are time-separated, no information from test
     leaks into train.
  5. Part 1: 8-step cumulative ablation (baseline + each feature), HistGBT,
     AUC + recall@1%FPR — all measured on the temporal holdout.
  6. Part 2: staleness — train ONE reference GBT on the fresh-feature
     training set. For each staleness tier Delta, recompute features ONLY
     for the test set, scored by that model. Tiers:
       {60s, 3600s, 86400s, 604800s, 2592000s, 5184000s}
       = {1 min, 1 h, 1 d, 7 d, 30 d, 60 d}.
     For each test row at TransactionDT=T, features are computed from all
     events (train+test) occurring at or before (T - Delta).

Reproducibility: all seeds = 42. Script is idempotent; re-run produces
identical numbers given the same input CSVs.
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score, roc_curve

ROOT = Path("/Users/petrpan26/work/tally/.planning/advanced-recipes/fraud-experiment")
RAW = ROOT / "raw"
SEED = 42

# ----- 1. Load full dataset -------------------------------------------------

def load_full() -> pd.DataFrame:
    """Load all 590K train rows and left-join identity for DeviceInfo / id_30 / M-flags."""
    tx = pd.read_csv(
        RAW / "train_transaction.csv",
        usecols=[
            "TransactionID", "isFraud", "TransactionDT", "TransactionAmt",
            "ProductCD", "card1",
            "M1", "M2", "M3", "M4", "M5", "M6",
        ],
    )
    idn = pd.read_csv(
        RAW / "train_identity.csv",
        usecols=["TransactionID", "DeviceInfo", "id_30"],
    )
    df = tx.merge(idn, on="TransactionID", how="left")
    # Chronological ordering is load-bearing for both feature derivation
    # and the time-split.
    return df.sort_values("TransactionDT", kind="mergesort").reset_index(drop=True)


# ----- 2. Feature derivation ------------------------------------------------

WIN_5M_S = 5 * 60
WIN_1H_S = 60 * 60


def _rolling_count_per_group(df: pd.DataFrame, key: str, time_col: str, window_s: int) -> pd.Series:
    """Count of rows per key in (t - window_s, t]. Ordered traversal, deque-like."""
    out = np.zeros(len(df), dtype=np.int32)
    for _, group in df.groupby(key, sort=False):
        idx = group.index.to_numpy()
        t = group[time_col].to_numpy()
        left = 0
        for i in range(len(idx)):
            while t[i] - t[left] > window_s:
                left += 1
            out[idx[i]] = i - left + 1
    return pd.Series(out, index=df.index)


def _rolling_sum_per_group(df: pd.DataFrame, key: str, time_col: str, val_col: str, window_s: int) -> pd.Series:
    out = np.zeros(len(df), dtype=np.float64)
    for _, group in df.groupby(key, sort=False):
        idx = group.index.to_numpy()
        t = group[time_col].to_numpy()
        v = group[val_col].to_numpy()
        left = 0
        running = 0.0
        for i in range(len(idx)):
            running += v[i]
            while t[i] - t[left] > window_s:
                running -= v[left]
                left += 1
            out[idx[i]] = running
    return pd.Series(out, index=df.index)


def _rolling_distinct_per_group(df: pd.DataFrame, key: str, time_col: str, val_col: str, window_s: int) -> pd.Series:
    """Distinct count of val_col per key within rolling window. Multiset sliding window."""
    out = np.full(len(df), np.nan, dtype=np.float64)
    mask = df[key].notna()
    sub = df[mask]
    for _, group in sub.groupby(key, sort=False):
        idx = group.index.to_numpy()
        t = group[time_col].to_numpy()
        v = group[val_col].to_numpy()
        left = 0
        counts: dict = {}
        for i in range(len(idx)):
            if isinstance(v[i], float) and np.isnan(v[i]):
                pass
            else:
                counts[v[i]] = counts.get(v[i], 0) + 1
            while t[i] - t[left] > window_s:
                lv = v[left]
                if not (isinstance(lv, float) and np.isnan(lv)):
                    counts[lv] -= 1
                    if counts[lv] == 0:
                        del counts[lv]
                left += 1
            out[idx[i]] = len(counts)
    return pd.Series(out, index=df.index)


def _rapid_streak_per_card(df: pd.DataFrame) -> pd.Series:
    """Consecutive inter-arrival < 60s run length (including current), per card1."""
    out = np.zeros(len(df), dtype=np.int32)
    for _, group in df.groupby("card1", sort=False):
        idx = group.index.to_numpy()
        t = group["TransactionDT"].to_numpy()
        streak = 1
        out[idx[0]] = streak
        for i in range(1, len(idx)):
            if t[i] - t[i - 1] < 60:
                streak += 1
            else:
                streak = 1
            out[idx[i]] = streak
    return pd.Series(out, index=df.index)


def _addr_mismatch_streak_per_card(df: pd.DataFrame) -> pd.Series:
    """Consecutive-row streak per card1 where any address/match flag is a 'mismatch'.

    A row counts as a mismatch if ANY of:
      - M1 == 'F' | M2 == 'F' | M3 == 'F'  (names / billing / address mismatches)
      - M4 != 'M0' AND M4 is not NaN  (M4 is categorical; M0 is the baseline 'match')
      - M5 == 'F' | M6 == 'F'
    A row is NOT a mismatch if all relevant M-columns are NaN (no info), so
    NaN-only rows reset the streak to 0.

    Output is the run length ending at the current row (0 if current row
    is not a mismatch).
    """
    m1 = df["M1"].to_numpy()
    m2 = df["M2"].to_numpy()
    m3 = df["M3"].to_numpy()
    m4 = df["M4"].to_numpy()
    m5 = df["M5"].to_numpy()
    m6 = df["M6"].to_numpy()
    is_mismatch = (
        (m1 == "F")
        | (m2 == "F")
        | (m3 == "F")
        | ((m4 != "M0") & pd.notna(df["M4"]).to_numpy())
        | (m5 == "F")
        | (m6 == "F")
    )
    out = np.zeros(len(df), dtype=np.int32)
    for _, group in df.groupby("card1", sort=False):
        idx = group.index.to_numpy()
        streak = 0
        for j, i in enumerate(idx):
            if is_mismatch[i]:
                streak += 1
            else:
                streak = 0
            out[i] = streak
    return pd.Series(out, index=df.index)


def derive_live_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the 7 features LIVE (as of TransactionDT) on every row."""
    d = df.copy().reset_index(drop=True)

    # Feature 1
    d["tx_count_5m_per_card"] = _rolling_count_per_group(d, "card1", "TransactionDT", WIN_5M_S)

    # Feature 2: M-flag "high-risk" indicator + its rolling sum
    m4 = d["M4"]
    is_m4_fail = m4.notna() & (m4 != "M0")
    d["_is_high_risk"] = (
        (d["M1"] == "F")
        | (d["M2"] == "F")
        | (d["M3"] == "F")
        | is_m4_fail
        | (d["M5"] == "F")
        | (d["M6"] == "F")
    ).astype(np.int32)
    d["high_risk_tx_5m_per_card"] = _rolling_sum_per_group(
        d, "card1", "TransactionDT", "_is_high_risk", WIN_5M_S
    )

    # Feature 3
    d["distinct_cards_5m_per_device"] = _rolling_distinct_per_group(
        d, "DeviceInfo", "TransactionDT", "card1", WIN_5M_S
    )

    # Feature 4
    d["addr_mismatch_streak_per_card"] = _addr_mismatch_streak_per_card(d)

    # Feature 5
    d["rapid_tx_streak_per_card"] = _rapid_streak_per_card(d)

    # Feature 6
    d["distinct_devices_1h_per_card"] = _rolling_distinct_per_group(
        d, "card1", "TransactionDT", "DeviceInfo", WIN_1H_S
    )

    # Feature 7
    d["amount_sum_5m_per_card"] = _rolling_sum_per_group(
        d, "card1", "TransactionDT", "TransactionAmt", WIN_5M_S
    )
    return d


# ----- 2b. Staleness: recompute features for test rows as of (t - Delta) ---
#
# Design:
#   * We pre-build per-group index arrays ONCE (in StaleContext.build_groups).
#   * Membership "is this row a test row?" uses a preallocated boolean mask
#     instead of set hashing — much faster in the hot loop.
#   * All per-tier feature derivations reuse the same group layout; only the
#     `offset_s` query shifts.
#   * The two streak features are computed LIVE once on the full dataframe;
#     each stale tier point-lookups the streak of the most recent prior
#     event per card1.


class StaleContext:
    """Precomputed shared state for staleness derivation. Built ONCE from the
    full dataframe; reused across all 6 staleness tiers.
    """

    def __init__(self, df_all: pd.DataFrame, test_idx: np.ndarray):
        self.n = len(df_all)
        self.df = df_all

        # Boolean test-membership mask (O(1) indexed lookup in hot loop).
        self.is_test = np.zeros(self.n, dtype=bool)
        self.is_test[test_idx] = True

        # Per-card1 group index arrays (sorted by row order, which IS time
        # order because df_all is sorted by TransactionDT).
        self.card_groups = self._build_groups(df_all["card1"].to_numpy(),
                                              skip_nan=False)

        # Per-DeviceInfo groups for the device-fanout feature; skip NaN.
        self.device_groups = self._build_groups(df_all["DeviceInfo"].to_numpy(),
                                                skip_nan=True)

        # Shared column views.
        self.t = df_all["TransactionDT"].to_numpy()
        self.amt = df_all["TransactionAmt"].to_numpy()
        m4 = df_all["M4"]
        is_m4_fail = m4.notna() & (m4 != "M0")
        self.high_risk = (
            (df_all["M1"] == "F")
            | (df_all["M2"] == "F")
            | (df_all["M3"] == "F")
            | is_m4_fail
            | (df_all["M5"] == "F")
            | (df_all["M6"] == "F")
        ).astype(np.int32).to_numpy()
        self.card1 = df_all["card1"].to_numpy()
        self.device = df_all["DeviceInfo"].to_numpy()

        # Pre-compute live streaks ONCE (shared across all stale tiers).
        self.live_addr_streak = _addr_mismatch_streak_per_card(df_all).to_numpy()
        self.live_rapid_streak = _rapid_streak_per_card(df_all).to_numpy()

    @staticmethod
    def _build_groups(keys: np.ndarray, *, skip_nan: bool) -> list:
        """Return a list of np.int64 index arrays, one per unique non-null
        (or all) key value, preserving within-group time order (by virtue
        of input being sorted by TransactionDT)."""
        from collections import defaultdict
        buckets = defaultdict(list)
        if skip_nan:
            # Object-dtype column — NaN check via is-float-and-isnan.
            for i, k in enumerate(keys):
                if k is None:
                    continue
                if isinstance(k, float) and np.isnan(k):
                    continue
                buckets[k].append(i)
        else:
            # card1 is integer; no NaN expected.
            for i, k in enumerate(keys):
                buckets[k].append(i)
        return [np.asarray(v, dtype=np.int64) for v in buckets.values()]


def _stale_count_rolling(ctx: StaleContext, groups: list, offset_s: int,
                         window_s: int) -> np.ndarray:
    """Per-group rolling COUNT with as-of shift.

    Window semantics match the live feature: inclusive on both ends.
    For a row at TransactionDT=T, count events with
        t' in [T - offset_s - window_s, T - offset_s].
    """
    out = np.zeros(ctx.n, dtype=np.int32)
    t_all = ctx.t
    is_test = ctx.is_test
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
            if is_test[ri]:
                if right < left:
                    out[ri] = 0
                else:
                    out[ri] = right - left + 1
    return out


def _stale_sum_rolling(ctx: StaleContext, groups: list, values: np.ndarray,
                       offset_s: int, window_s: int) -> np.ndarray:
    """Per-group rolling SUM of `values[idx]` with as-of shift."""
    out = np.zeros(ctx.n, dtype=np.float64)
    t_all = ctx.t
    is_test = ctx.is_test
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
            if is_test[ri]:
                if right < left:
                    out[ri] = 0.0
                else:
                    out[ri] = running
    return out


def _stale_distinct_rolling(ctx: StaleContext, groups: list,
                            val_arr: np.ndarray, offset_s: int,
                            window_s: int, fill_nan_keys: bool) -> np.ndarray:
    """Per-group rolling DISTINCT count of `val_arr[idx]` with as-of shift.

    If `fill_nan_keys` is True, rows whose group key is NaN keep a NaN
    (the live feature outputs NaN for missing DeviceInfo).
    """
    if fill_nan_keys:
        out = np.full(ctx.n, np.nan, dtype=np.float64)
    else:
        out = np.zeros(ctx.n, dtype=np.float64)
    t_all = ctx.t
    is_test = ctx.is_test
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
            if is_test[ri]:
                if right < left:
                    out[ri] = 0.0
                else:
                    out[ri] = len(counts)
    return out


def _stale_streak_lookup(ctx: StaleContext, live_streak: np.ndarray,
                         offset_s: int) -> np.ndarray:
    """Point-lookup the live streak of the most recent event with t' <=
    t - offset_s, per card1 group. Emits 0 if no such prior event."""
    out = np.zeros(ctx.n, dtype=np.int32)
    t_all = ctx.t
    is_test = ctx.is_test
    for idx in ctx.card_groups:
        t = t_all[idx]
        n = len(idx)
        right = -1
        for i in range(n):
            t_query = t[i] - offset_s
            while right + 1 < n and t[right + 1] <= t_query:
                right += 1
            ri = idx[i]
            if is_test[ri]:
                if right < 0:
                    out[ri] = 0
                else:
                    out[ri] = live_streak[idx[right]]
    return out


def derive_stale_test_features(
    ctx: StaleContext, offset_s: int, all_cols: list,
) -> pd.DataFrame:
    """Build a (n_rows x len(all_cols)) feature frame where, for each test
    row, features are computed as of (TransactionDT - offset_s). Train
    rows are filled with 0s / NaNs and NOT scored.

    Returns a plain DataFrame (indexable by the original row positions).
    """
    # 1. tx count 5m per card
    f1 = _stale_count_rolling(ctx, ctx.card_groups, offset_s, WIN_5M_S)
    # 2. high-risk sum 5m per card
    f2 = _stale_sum_rolling(ctx, ctx.card_groups, ctx.high_risk.astype(np.float64),
                            offset_s, WIN_5M_S)
    # 3. distinct cards 5m per device (skip NaN device rows)
    f3 = _stale_distinct_rolling(ctx, ctx.device_groups, ctx.card1,
                                 offset_s, WIN_5M_S, fill_nan_keys=True)
    # 4. addr-mismatch streak per card (stale lookup of live)
    f4 = _stale_streak_lookup(ctx, ctx.live_addr_streak, offset_s)
    # 5. rapid-tx streak per card (stale lookup of live)
    f5 = _stale_streak_lookup(ctx, ctx.live_rapid_streak, offset_s)
    # 6. distinct devices 1h per card
    f6 = _stale_distinct_rolling(ctx, ctx.card_groups, ctx.device,
                                 offset_s, WIN_1H_S, fill_nan_keys=False)
    # 7. amount sum 5m per card
    f7 = _stale_sum_rolling(ctx, ctx.card_groups, ctx.amt, offset_s, WIN_5M_S)

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


# ----- 3. Model + metrics ---------------------------------------------------

def recall_at_fpr(y_true: np.ndarray, y_score: np.ndarray, target_fpr: float) -> float:
    fpr, tpr, _ = roc_curve(y_true, y_score)
    return float(np.interp(target_fpr, fpr, tpr))


def fit_and_score(X_tr, y_tr, X_te, y_te):
    clf = HistGradientBoostingClassifier(
        max_depth=8, max_iter=300, learning_rate=0.05, random_state=SEED,
    )
    clf.fit(X_tr, y_tr)
    p = clf.predict_proba(X_te)[:, 1]
    auc = roc_auc_score(y_te, p)
    r = recall_at_fpr(y_te, p, 0.01)
    return auc, r, clf


# ----- 4. Main pipeline -----------------------------------------------------

def main():
    t0 = time.time()
    print(f"[{time.time()-t0:.1f}s] Loading full dataset...", flush=True)
    df = load_full()
    n_rows = len(df)
    fraud_rate = float(df.isFraud.mean())
    n_unique_card1 = int(df["card1"].nunique())
    n_with_identity = int(df["DeviceInfo"].notna().sum())
    identity_coverage = n_with_identity / n_rows
    print(f"  rows: {n_rows}, fraud rate: {fraud_rate:.4%}, "
          f"unique card1: {n_unique_card1}, "
          f"identity coverage: {identity_coverage:.2%}", flush=True)

    # M-column summary
    m_summary = {}
    for col in ["M1", "M2", "M3", "M4", "M5", "M6"]:
        vc = df[col].value_counts(dropna=False).to_dict()
        m_summary[col] = {str(k): int(v) for k, v in vc.items()}

    print(f"[{time.time()-t0:.1f}s] Deriving live features (offset=0)...", flush=True)
    feat = derive_live_features(df)
    print(f"[{time.time()-t0:.1f}s]   features derived.", flush=True)

    # Feature matrix columns (ablation order)
    base_col = "TransactionAmt"
    feature_cols = [
        "tx_count_5m_per_card",
        "high_risk_tx_5m_per_card",
        "distinct_cards_5m_per_device",
        "addr_mismatch_streak_per_card",
        "rapid_tx_streak_per_card",
        "distinct_devices_1h_per_card",
        "amount_sum_5m_per_card",
    ]
    all_cols = [base_col] + feature_cols

    # Audit: first 1000 rows of live features.
    audit = feat[["TransactionID", "TransactionDT", "card1", "DeviceInfo", "isFraud"]
                 + all_cols].head(1000).copy()
    audit.to_csv(ROOT / "features_derived.csv", index=False)

    # --- Chronological 70/30 split ---
    print(f"[{time.time()-t0:.1f}s] Temporal 70/30 split...", flush=True)
    n_train = int(round(0.70 * n_rows))
    tr_idx = np.arange(0, n_train, dtype=np.int64)
    te_idx = np.arange(n_train, n_rows, dtype=np.int64)
    # Boundary timestamp info
    t_min = int(df["TransactionDT"].iloc[0])
    t_split = int(df["TransactionDT"].iloc[n_train])
    t_max = int(df["TransactionDT"].iloc[-1])
    train_span_days = (df["TransactionDT"].iloc[n_train - 1] - t_min) / 86400.0
    test_span_days = (t_max - t_split) / 86400.0
    fraud_rate_train = float(df["isFraud"].iloc[tr_idx].mean())
    fraud_rate_test = float(df["isFraud"].iloc[te_idx].mean())
    print(f"  train: {len(tr_idx)} rows, {fraud_rate_train:.4%} fraud, "
          f"spans {train_span_days:.1f} days", flush=True)
    print(f"  test:  {len(te_idx)} rows, {fraud_rate_test:.4%} fraud, "
          f"spans {test_span_days:.1f} days", flush=True)
    print(f"  split timestamp (TransactionDT): {t_split}", flush=True)

    X_all = feat[all_cols].copy().values
    y_all = feat["isFraud"].astype(np.int32).values
    X_tr_all = X_all[tr_idx]
    X_te_all = X_all[te_idx]
    y_tr = y_all[tr_idx]
    y_te = y_all[te_idx]

    # --- Part 1: cumulative ablation on temporal holdout ---
    print(f"[{time.time()-t0:.1f}s] Part 1: feature ablation (time-split)...", flush=True)
    results_p1 = []
    auc, r, _ = fit_and_score(X_tr_all[:, :1], y_tr, X_te_all[:, :1], y_te)
    results_p1.append(("baseline (TransactionAmt only)", auc, r, None))
    prev_auc = auc

    labels = [
        "+ tx_count_5m_per_card",
        "+ high_risk_tx_5m_per_card",
        "+ distinct_cards_5m_per_device",
        "+ addr_mismatch_streak_per_card",
        "+ rapid_tx_streak_per_card",
        "+ distinct_devices_1h_per_card",
        "+ amount_sum_5m_per_card",
    ]
    for i, lbl in enumerate(labels, start=1):
        cols = 1 + i
        auc, r, _ = fit_and_score(X_tr_all[:, :cols], y_tr, X_te_all[:, :cols], y_te)
        results_p1.append((lbl, auc, r, auc - prev_auc))
        prev_auc = auc
        print(f"  {lbl}: AUC={auc:.4f} recall@1%FPR={r:.4f}", flush=True)

    # --- Part 2: staleness ---
    # Train one reference model on the FRESH training set (full feature set).
    print(f"[{time.time()-t0:.1f}s] Part 2: training reference GBT (fresh train)...", flush=True)
    clf_ref = HistGradientBoostingClassifier(
        max_depth=8, max_iter=300, learning_rate=0.05, random_state=SEED,
    )
    clf_ref.fit(X_tr_all, y_tr)
    auc_fresh = roc_auc_score(y_te, clf_ref.predict_proba(X_te_all)[:, 1])
    r_fresh = recall_at_fpr(y_te, clf_ref.predict_proba(X_te_all)[:, 1], 0.01)
    print(f"  reference fresh-test AUC={auc_fresh:.4f} recall@1%FPR={r_fresh:.4f}", flush=True)

    staleness_tiers = [
        (60, "1 min"),
        (3600, "1 hour"),
        (86400, "1 day"),
        (604800, "7 days"),
        (2592000, "30 days"),
        (5184000, "60 days"),
    ]
    print(f"[{time.time()-t0:.1f}s] Building StaleContext (shared across tiers)...",
          flush=True)
    ctx = StaleContext(df, te_idx)
    print(f"[{time.time()-t0:.1f}s]   ctx built. n_card_groups={len(ctx.card_groups)} "
          f"n_device_groups={len(ctx.device_groups)}", flush=True)

    results_p2 = []
    for offset_s, label in staleness_tiers:
        print(f"  staleness offset={offset_s}s ({label})...", flush=True)
        t_st = time.time()
        fs = derive_stale_test_features(ctx, offset_s, all_cols)
        X_te_stale = fs.iloc[te_idx].values
        p = clf_ref.predict_proba(X_te_stale)[:, 1]
        auc = roc_auc_score(y_te, p)
        r = recall_at_fpr(y_te, p, 0.01)
        dt = time.time() - t_st
        results_p2.append((label, offset_s, auc, r))
        print(f"    AUC={auc:.4f} recall@1%FPR={r:.4f}  ({dt:.1f}s)", flush=True)

    # --- Format results
    print(f"[{time.time()-t0:.1f}s] Writing results...", flush=True)

    p1_lines = ["| Features active | AUC | Recall @ 1% FPR | Marginal lift (AUC) |",
                "|---|---|---|---|"]
    for lbl, auc, r, delta in results_p1:
        d_str = "—" if delta is None else f"{delta:+.3f}"
        p1_lines.append(f"| {lbl} | {auc:.3f} | {r*100:.1f}% | {d_str} |")
    p1_table = "\n".join(p1_lines)

    # Find the "fresh-like" reference row for the delta column (1-min acts
    # as the "basically fresh" baseline per the protocol spec).
    r_1min = [r for lbl, _, _, r in results_p2 if lbl == "1 min"][0]
    auc_1min = [a for lbl, _, a, _ in results_p2 if lbl == "1 min"][0]
    p2_lines = ["| Feature staleness | AUC | Recall @ 1% FPR | Δ from fresh (AUC) |",
                "|---|---|---|---|"]
    for lbl, offset_s, auc, r in results_p2:
        if lbl == "1 min":
            d_str = "—"
        else:
            d_str = f"{auc - auc_1min:+.3f}"
        p2_lines.append(f"| {lbl} | {auc:.3f} | {r*100:.1f}% | {d_str} |")
    p2_table = "\n".join(p2_lines)

    # Headline: recall ratio 1-min vs 60-day
    r_60d = [r for lbl, _, _, r in results_p2 if lbl == "60 days"][0]
    auc_60d = [a for lbl, _, a, _ in results_p2 if lbl == "60 days"][0]
    if r_60d > 0:
        ratio = r_1min / r_60d
    else:
        ratio = float("inf")
    headline = (
        f"{ratio:.2f}x more fraud caught at 1% FPR with 1-min-fresh features "
        f"vs 60-day-stale"
    )

    # New-feature marginal lift
    new_feat_lift_auc = results_p1[4][1] - results_p1[3][1]
    new_feat_lift_recall = results_p1[4][2] - results_p1[3][2]

    summary = {
        "protocol": "time-split (chronological 70/30 by TransactionDT)",
        "sample_size": n_rows,
        "fraud_rate_overall": fraud_rate,
        "fraud_rate_train": fraud_rate_train,
        "fraud_rate_test": fraud_rate_test,
        "train_rows": int(len(tr_idx)),
        "test_rows": int(len(te_idx)),
        "train_span_days": train_span_days,
        "test_span_days": test_span_days,
        "split_transactiondt": t_split,
        "unique_card1": n_unique_card1,
        "identity_coverage": identity_coverage,
        "part1_final_auc": results_p1[-1][1],
        "part1_final_recall_at_1pct_fpr": results_p1[-1][2],
        "part1_baseline_auc": results_p1[0][1],
        "part1_baseline_recall_at_1pct_fpr": results_p1[0][2],
        "new_feature_marginal_auc_lift": new_feat_lift_auc,
        "new_feature_marginal_recall_lift": new_feat_lift_recall,
        "staleness_headline": headline,
        "staleness_ratio_1min_vs_60day": ratio,
        "staleness_1min_auc": auc_1min,
        "staleness_60day_auc": auc_60d,
        "staleness_1min_recall": r_1min,
        "staleness_60day_recall": r_60d,
        "reference_model_fresh_auc": auc_fresh,
        "reference_model_fresh_recall": r_fresh,
    }

    versions = {
        "python": sys.version.split()[0],
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "sklearn": sklearn.__version__,
        "run_timestamp_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elapsed_s": time.time() - t0,
    }

    with open(ROOT / "results.json", "w") as f:
        json.dump({
            "summary": summary,
            "m_column_value_counts": m_summary,
            "part1": [{"row": lbl, "auc": auc, "recall_1pct": r,
                       "marginal_lift": delta} for lbl, auc, r, delta in results_p1],
            "part2": [{"staleness": lbl, "offset_s": offset_s, "auc": auc,
                       "recall_1pct": r}
                      for lbl, offset_s, auc, r in results_p2],
            "versions": versions,
        }, f, indent=2)

    print(f"[{time.time()-t0:.1f}s] Done.")
    print("\n--- PART 1 (time-split ablation) ---")
    print(p1_table)
    print("\n--- PART 2 (staleness) ---")
    print(p2_table)
    print("\nHEADLINE:", headline)
    print(f"\nFraud-rate train {fraud_rate_train:.4%} / test {fraud_rate_test:.4%} "
          f"(Δ={fraud_rate_test - fraud_rate_train:+.4%})")
    print("\nVERSIONS:", json.dumps(versions))

    return p1_table, p2_table, headline, versions, summary


if __name__ == "__main__":
    main()
