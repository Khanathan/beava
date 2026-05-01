"""
Matched-staleness anomaly detection experiment on Numenta Anomaly Benchmark (NAB).

Detector: streaming EWMA z-score.
  halflife = 10 points
  alpha    = 1 - 2^(-1/halflife)
  EWMA_t   = alpha * v_t + (1 - alpha) * EWMA_{t-1}
  EWVar_t  = (1 - alpha) * (EWVar_{t-1} + alpha * (v_t - EWMA_{t-1})^2)
  z_t      = (v_t - EWMA_{t-Δ-1}) / sqrt(EWVar_{t-Δ-1} + 1e-6)

Part 1 (fixed-threshold, |z|>3.5): original protocol, preserved verbatim.
Part 2 (matched-recall): for each Δ, compute z-scores once, sweep τ over a grid,
  and select the τ whose aggregate window-recall is closest to each target
  operating point in {0.60, 0.80, 0.90}. Report FP count, precision, MTTD at
  each selected τ.

Scoring (both parts): window-based TS anomaly matching.
  TP = prediction falls inside any labeled window
  FN = labeled window with no prediction inside
  FP = prediction outside every window
  Window-recall = (# windows with >=1 prediction inside) / (# windows with labels)
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
NAB = HERE / "NAB"
DATA_DIR = NAB / "data"
LABELS_PATH = NAB / "labels" / "combined_windows.json"

HALFLIFE = 10
ALPHA = 1.0 - 2.0 ** (-1.0 / HALFLIFE)
FIXED_THRESHOLD = 3.5
INIT_WINDOW = 30
DELTAS = [0, 1, 6, 12, 72, 288]

THRESHOLD_GRID = [2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 7.0, 8.0, 10.0]
TARGET_RECALLS = [0.60, 0.80, 0.90]


def load_labels() -> dict:
    with open(LABELS_PATH) as f:
        return json.load(f)


def parse_windows(raw_windows, timestamps: pd.Series) -> List[Tuple[int, int]]:
    out = []
    ts = timestamps.values
    for start_s, end_s in raw_windows:
        start = np.datetime64(pd.Timestamp(start_s))
        end = np.datetime64(pd.Timestamp(end_s))
        lo = int(np.searchsorted(ts, start, side="left"))
        hi = int(np.searchsorted(ts, end, side="right") - 1)
        if hi < lo:
            continue
        out.append((lo, hi))
    return out


def run_detector(values: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    n = len(values)
    ewma = np.empty(n, dtype=np.float64)
    ewvar = np.empty(n, dtype=np.float64)
    init_n = min(INIT_WINDOW, n)
    init_mean = float(np.mean(values[:init_n]))
    init_var = float(np.var(values[:init_n])) if init_n > 1 else 0.0
    prev_mean = init_mean
    prev_var = init_var
    for i in range(n):
        v = float(values[i])
        delta = v - prev_mean
        new_mean = ALPHA * v + (1.0 - ALPHA) * prev_mean
        new_var = (1.0 - ALPHA) * (prev_var + ALPHA * delta * delta)
        ewma[i] = new_mean
        ewvar[i] = new_var
        prev_mean = new_mean
        prev_var = new_var
    return ewma, ewvar


def zscores_for_delta(
    values: np.ndarray, ewma: np.ndarray, ewvar: np.ndarray, delta: int
) -> np.ndarray:
    """
    Return a per-tick |z| score array. NaN at ticks where the reference is not
    available yet (ref < INIT_WINDOW - 1). Computing |z| once per Δ lets us
    sweep thresholds essentially for free.
    """
    n = len(values)
    z = np.full(n, np.nan, dtype=np.float64)
    for t in range(n):
        ref = t - delta - 1
        if ref < INIT_WINDOW - 1:
            continue
        mean = ewma[ref]
        var = ewvar[ref]
        z[t] = abs((values[t] - mean) / math.sqrt(var + 1e-6))
    return z


def predictions_from_z(z: np.ndarray, threshold: float) -> np.ndarray:
    preds = np.zeros(len(z), dtype=bool)
    valid = ~np.isnan(z)
    preds[valid] = z[valid] > threshold
    return preds


def score(preds: np.ndarray, windows: List[Tuple[int, int]]):
    """Return (tp, fp, fn, mttd_sum, mttd_count, windows_hit)."""
    pred_idx = np.where(preds)[0]
    tp = 0
    fp = 0
    window_first_hit: Dict[int, int] = {}
    wins = sorted(windows)
    starts = np.array([w[0] for w in wins], dtype=np.int64) if wins else np.zeros(0, dtype=np.int64)
    ends = np.array([w[1] for w in wins], dtype=np.int64) if wins else np.zeros(0, dtype=np.int64)
    for p in pred_idx:
        if len(wins) == 0:
            fp += 1
            continue
        j = int(np.searchsorted(starts, p, side="right") - 1)
        hit = False
        if j >= 0 and p <= ends[j]:
            hit = True
        if hit:
            tp += 1
            if j not in window_first_hit or p < window_first_hit[j]:
                window_first_hit[j] = p
        else:
            fp += 1
    fn = len(wins) - len(window_first_hit)
    mttd_sum = 0.0
    mttd_count = 0
    for j, first_p in window_first_hit.items():
        mttd_sum += float(first_p - wins[j][0])
        mttd_count += 1
    return tp, fp, fn, mttd_sum, mttd_count, len(window_first_hit)


def f1_from(tp: int, fp: int, fn: int) -> Tuple[float, float, float]:
    prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    if prec + rec == 0:
        return 0.0, prec, rec
    return 2 * prec * rec / (prec + rec), prec, rec


def main():
    labels = load_labels()
    series_names = sorted(labels.keys())

    # ---- Load everything once ----
    series_data = []  # list of (name, values, windows)
    total_points = 0
    total_windows = 0
    for name in series_names:
        csv_path = DATA_DIR / name
        if not csv_path.exists():
            print(f"MISSING: {csv_path}", file=sys.stderr)
            continue
        df = pd.read_csv(csv_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        values = df["value"].to_numpy(dtype=np.float64)
        n = len(values)
        if n < INIT_WINDOW + 2:
            continue
        windows = parse_windows(labels[name], df["timestamp"])
        series_data.append((name, values, windows))
        total_points += n
        total_windows += len(windows)
    series_count = len(series_data)
    print(f"Processed {series_count} series, {total_points} total points, {total_windows} labeled windows")

    # ---- Precompute EWMA/EWVar and per-delta |z| ----
    # Dict: delta -> list of (name, z_array, windows) with aligned order.
    z_by_delta: Dict[int, List[Tuple[str, np.ndarray, List[Tuple[int, int]]]]] = {d: [] for d in DELTAS}
    for name, values, windows in series_data:
        ewma, ewvar = run_detector(values)
        for d in DELTAS:
            z = zscores_for_delta(values, ewma, ewvar, d)
            z_by_delta[d].append((name, z, windows))

    # ---- Part 1: fixed-threshold (|z|>3.5) ----
    part1_rows = []
    part1_cat = {}
    for d in DELTAS:
        per_series_stats = []
        per_cat = {}
        for name, z, windows in z_by_delta[d]:
            preds = predictions_from_z(z, FIXED_THRESHOLD)
            tp, fp, fn, mttd_sum, mttd_count, _ = score(preds, windows)
            f1, prec, rec = f1_from(tp, fp, fn)
            mttd = (mttd_sum / mttd_count) if mttd_count > 0 else None
            per_series_stats.append((f1, prec, rec, mttd, tp, fp, fn, mttd_count))
            cat = name.split("/")[0]
            per_cat.setdefault(cat, []).append((f1, prec, rec, mttd))
        f1s = [s[0] for s in per_series_stats]
        precs = [s[1] for s in per_series_stats]
        recs = [s[2] for s in per_series_stats]
        mttds = [s[3] for s in per_series_stats if s[3] is not None]
        tp = sum(s[4] for s in per_series_stats)
        fp = sum(s[5] for s in per_series_stats)
        fn = sum(s[6] for s in per_series_stats)
        mean_f1 = float(np.mean(f1s))
        mean_prec = float(np.mean(precs))
        mean_rec = float(np.mean(recs))
        mean_mttd = float(np.mean(mttds)) if mttds else float("nan")
        part1_rows.append({
            "delta": d,
            "f1": mean_f1,
            "precision": mean_prec,
            "recall": mean_rec,
            "mttd_ticks": mean_mttd,
            "windows_detected": len(mttds),
            "tp_total": tp,
            "fp_total": fp,
            "fn_total": fn,
        })
        for cat, stats in per_cat.items():
            f1s_c = [s[0] for s in stats]
            mttds_c = [s[3] for s in stats if s[3] is not None]
            part1_cat.setdefault(cat, {})[d] = {
                "f1": float(np.mean(f1s_c)),
                "mttd": float(np.mean(mttds_c)) if mttds_c else float("nan"),
                "n_series": len(stats),
            }
        print(f"[P1] Δ={d:>4}  F1={mean_f1:.3f}  R={mean_rec:.3f}  TP={tp} FP={fp} FN={fn}  MTTD={mean_mttd:.2f}")

    # ---- Part 2: threshold sweep + matched recall ----
    # Aggregate window-recall is defined as windows_hit_total / total_windows.
    # Pick τ per Δ per target recall that minimizes |agg_recall - target|; tie-break
    # on FP count (prefer fewer FPs).
    part2 = {str(tr): [] for tr in TARGET_RECALLS}
    # For debugging / docs, also dump the full per-threshold grid per Δ.
    part2_grid = {}
    for d in DELTAS:
        grid_rows = []
        for tau in THRESHOLD_GRID:
            # Aggregate metrics across all series at this (Δ, τ)
            tp_total = 0
            fp_total = 0
            fn_total = 0
            windows_hit_total = 0
            mttd_sum_agg = 0.0
            mttd_count_agg = 0
            # per-series for mean precision/recall/mttd
            per_series = []
            for name, z, windows in z_by_delta[d]:
                preds = predictions_from_z(z, tau)
                tp, fp, fn, mttd_sum, mttd_count, wins_hit = score(preds, windows)
                tp_total += tp
                fp_total += fp
                fn_total += fn
                windows_hit_total += wins_hit
                mttd_sum_agg += mttd_sum
                mttd_count_agg += mttd_count
                f1, prec, rec = f1_from(tp, fp, fn)
                mttd = (mttd_sum / mttd_count) if mttd_count > 0 else None
                per_series.append((f1, prec, rec, mttd))
            windows_with_labels = fn_total + windows_hit_total  # == total_windows in coverable series
            agg_recall = (windows_hit_total / windows_with_labels) if windows_with_labels > 0 else 0.0
            mean_prec = float(np.mean([s[1] for s in per_series]))
            mttds = [s[3] for s in per_series if s[3] is not None]
            mean_mttd = float(np.mean(mttds)) if mttds else float("nan")
            agg_mttd = (mttd_sum_agg / mttd_count_agg) if mttd_count_agg > 0 else float("nan")
            grid_rows.append({
                "tau": tau,
                "tp": tp_total,
                "fp": fp_total,
                "fn": fn_total,
                "agg_recall": agg_recall,
                "mean_precision": mean_prec,
                "mean_mttd_ticks": mean_mttd,
                "agg_mttd_ticks": agg_mttd,
                "windows_hit": windows_hit_total,
                "windows_with_labels": windows_with_labels,
            })
        part2_grid[d] = grid_rows

        for target in TARGET_RECALLS:
            # Sort candidates by |recall - target|, then by fp_total ascending
            best = min(
                grid_rows,
                key=lambda r: (abs(r["agg_recall"] - target), r["fp"]),
            )
            part2[str(target)].append({
                "delta": d,
                "target_recall": target,
                "selected_tau": best["tau"],
                "achieved_recall": best["agg_recall"],
                "fp_count": best["fp"],
                "tp_count": best["tp"],
                "fn_count": best["fn"],
                "agg_precision": (best["tp"] / (best["tp"] + best["fp"])) if (best["tp"] + best["fp"]) > 0 else 0.0,
                "mean_precision": best["mean_precision"],
                "mean_mttd_ticks": best["mean_mttd_ticks"],
                "agg_mttd_ticks": best["agg_mttd_ticks"],
                "windows_hit": best["windows_hit"],
                "windows_with_labels": best["windows_with_labels"],
            })
            print(f"[P2] Δ={d:>4}  target_R={target:.2f}  τ={best['tau']:>4}  R={best['agg_recall']:.3f}  FP={best['fp']}  prec={best['mean_precision']:.3f}  MTTD={best['agg_mttd_ticks']:.2f}")

    out = {
        "n_series": series_count,
        "total_points": total_points,
        "total_windows": total_windows,
        "part1_fixed_threshold": {
            "threshold": FIXED_THRESHOLD,
            "rows": part1_rows,
            "categories": part1_cat,
        },
        "part2_matched_recall": {
            "threshold_grid": THRESHOLD_GRID,
            "target_recalls": TARGET_RECALLS,
            "operating_points": part2,
            "full_grid": {str(d): part2_grid[d] for d in DELTAS},
        },
        "config": {
            "halflife": HALFLIFE,
            "alpha": ALPHA,
            "init_window": INIT_WINDOW,
            "deltas": DELTAS,
        },
    }
    # Preserve legacy top-level keys so old readers don't break
    out["rows"] = part1_rows
    out["categories"] = part1_cat

    with open(HERE / "results.json", "w") as f:
        json.dump(out, f, indent=2)
    print("Wrote", HERE / "results.json")


if __name__ == "__main__":
    main()
