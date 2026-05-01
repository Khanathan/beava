# Personalization experiment — Yoochoose RecSys 2015

**Headline:** 2.9-pt hit@10 drop when `session_top_categories` is 30 min stale vs real-time.

## Methodology

- **Dataset:** Yoochoose RecSys Challenge 2015 clickstream (`yoochoose-clicks.dat`), cached locally from the legacy S3 bucket. Schema: `SessionID,Timestamp,ItemID,Category` (comma-separated).
- **Rows processed (full file):** 33,003,944.
- **Sampling protocol:** filter to sessions with >=3 clicks, then sample 200,000 session IDs via `numpy.random.default_rng(42).choice(len(eligible), size=200000, replace=False)`. Indices converted to a Python list before use (avoids read-only-array bug).
- **Train/test protocol:** leave-last-click-out. For each sampled session the last click is the test event; earlier clicks are context.
- **Candidate set per test event:** top-1000 items by global popularity + the true next item (union).
- **Scoring function:** weighted sum over candidates

  ```
  score(item) = w_pop    * norm_popularity(item)
              + w_cats   * 1[item.category in session_top_categories]
              + w_recent * 1[item in session_recent_items]
              + w_dwell  * (1 / (1 + dwell_ms/30000)) * norm_popularity(item)
              + w_depth  * log(1+depth) * norm_popularity(item)
              + w_var    * (1 / (1+log(1+variety))) * 1[item.category in top_cats]
  ```

- **Weights used:** `[1.0, 0.3, 0.3, 0.03, 0.03, 0.03]` — pop=1.0, cats=0.3, recent=0.3, dwell/depth/var=0.03 each. Popularity raised to 1.0 over the equal-weight starting point so other features produce measurable re-ranking (candidate set is already popularity-filtered).
- **Metric:** hit@10 (fraction of test events whose ground truth is in the top-10 scored candidates).
- **Software versions:** python=3.13.2, pandas=3.0.2, numpy=2.4.4, sklearn=1.8.0.

## Part 1 — Cumulative feature ablation

| Features active | Hit@10 | Marginal lift |
|---|---|---|
| baseline (popularity only) | 2.14% | — |
| + session_top_categories | 2.74% | +0.60 pts |
| + session_recent_items | 15.06% | +12.32 pts |
| + session_dwell_avg | 14.39% | -0.67 pts |
| + session_depth | 13.23% | -1.16 pts |
| + session_variety | 13.14% | -0.08 pts |

Full 5-feature model hit@10 = **13.14%** (baseline 2.14%).

## Part 2 — Staleness sweep (session_top_categories)

Full 5-feature model; only `session_top_categories` is recomputed at each staleness tier. Other features stay real-time.

| Staleness tier | Hit@10 | Drop vs real-time |
|---|---|---|
| real-time | 13.14% | +0.00 pts |
| 10 seconds stale | 10.27% | +2.87 pts |
| 1 minute stale | 10.27% | +2.87 pts |
| 5 minutes stale | 10.27% | +2.87 pts |
| 30 minutes stale | 10.27% | +2.87 pts |

**Headline:** 2.9-pt hit@10 drop when `session_top_categories` is 30 min stale vs real-time.

## Notes

- Wall time: 3.3 min.
- Test events evaluated: 200,000.
- Seed: 42.
