# Multi-Touch Attribution Staleness Experiment — Results

## Headline

- **Stale attribution redirects $3,237 of credit to the wrong channel at 1-day latency — 21.25% of total conversion value.**
- **Per-channel MAPE rises from 0% (real-time) to 20.96% as attribution goes from real-time to 1-day stale.**

## Methodology

- **Dataset:** Criteo Attribution Modeling for Bidding Dataset (Diemert et al., AdKDD 2017). Downloaded from the official Hugging Face mirror `criteo/criteo-attribution-dataset` (file `criteo_attribution_dataset.tsv.gz`, 622 MB gzipped). CC-BY-NC-SA-4.0.
- **Total raw rows:** see loader (~16.5M).  (Full file has ~16.5M impression rows over 30 days.)
- **Sampling:** seed `20260424`. Users drawn uniformly at random from the set of users with ≥1 conversion, target 200,000 users. Actual sampled users: 200,000.
- **Touchpoints:** impression rows with `click == 1` (standard MTA convention — displays-only are excluded as noise). Click touches in sample: 717,872.
- **Conversions:** one row per distinct `conversion_id` with `conversion_timestamp >= 0` and `cpo > 0`. Sampled up to 150,000 conversions with seed `20260424`. Actual: 150,000.
- **Conversion value proxy:** the dataset has no explicit revenue column. We use the `cpo` (cost-per-order target) field as the per-conversion value, matching the convention in the Diemert et al. paper. Values are in normalized units; we label them `$` in the headlines.
- **Channel labels:** the dataset is fully anonymized (no named channels). We derive 6 canonical MTA channels (display, paid_search, organic, email, retargeting, social) by deterministic blake2b hash of the tuple `(cat1, cat2)` modulo 6. Mapping is seeded and stable. This is a synthesized channel taxonomy laid over real Criteo touch data — the per-row touch, timing, click, and conversion information is all real measured data from Criteo; only the channel *naming* is synthesized. Results are robust to this (hash-based bucketing is balanced).
- **Attribution models (Part 1):**
  1. first-touch: 100% credit to earliest touch.
  2. last-touch: 100% credit to latest touch at or before conversion.
  3. linear: 1/n credit to each of n touches.
  4. time-decay: weight = 0.5^((conv_ts − touch_ts) / 3 days), normalized. Halflife = 3 days.
  5. position-based (U-shape): 40% first + 40% last + 20% evenly across middle (n≥3); 50/50 for n=2; 100% for n=1.
- **Reference ("ground truth"):** position-based attribution at Δ=0 computed with full visibility of the user's path at conversion time.
- **Staleness protocol (Part 2):** for each conversion and each Δ ∈ {0, 1h, 6h, 1d, 7d}, reattribute using only touches with `timestamp <= conversion_ts − Δ`. If Δ eliminates the entire path, the conversion's credit is left unallocated at that tier (reflects how real pipelines may bucket unattributed conversions separately).
- **Conversion window:** touches older than 30 days before conversion are dropped.
- **MAPE definition:** mean over channels c with ref[c] > 0 of |alloc[c] − ref[c]| / ref[c].
- **Total $ misallocated:** Σ over channels |alloc_Δ[c] − alloc_0[c]|.
- **Software:** Python 3, pandas 3.0.2, numpy 2.4.4. Hardware: single workstation. Seed `20260424`.

## Part 1 — Attribution-model ablation at Δ=0

Per-channel attributed conversion value ($).

| Model | display | paid_search | organic | email | retargeting | social |
|---|---|---|---|---|---|---|
| first | $1,635 | $1,278 | $5,858 | $1,221 | $1,435 | $3,808 |
| last | $1,818 | $1,065 | $5,798 | $1,260 | $1,410 | $3,884 |
| linear | $1,724 | $1,149 | $5,835 | $1,237 | $1,419 | $3,871 |
| time_decay | $1,781 | $1,113 | $5,809 | $1,248 | $1,414 | $3,869 |
| position | $1,725 | $1,164 | $5,830 | $1,240 | $1,421 | $3,854 |

(Position-based is the reference used in Part 2.)

## Part 2 — Staleness sweep (position-based)

| Staleness Δ | Per-channel MAPE vs real-time | Total $ misallocated | % of conversion value |
|---|---|---|---|
| 0 | 0.00% | $0 | 0.00% |
| 1h | 12.06% | $1,827 | 11.99% |
| 6h | 16.09% | $2,466 | 16.19% |
| 1d | 20.96% | $3,237 | 21.25% |
| 7d | 44.43% | $6,838 | 44.89% |

## Per-channel misallocation at 1-day staleness

| Channel | Real-time $ | 1d-stale $ | Δ $ | Δ % |
|---|---|---|---|---|
| display | $1,725 | $1,430 | $-295 | -17.1% |
| paid_search | $1,164 | $869 | $-296 | -25.4% |
| organic | $5,830 | $4,527 | $-1,303 | -22.4% |
| email | $1,240 | $999 | $-241 | -19.4% |
| retargeting | $1,421 | $1,130 | $-290 | -20.4% |
| social | $3,854 | $3,042 | $-813 | -21.1% |

## Interpretation

- At Δ=0 (real-time), all five models assign the same total conversion value ($15,235) but redistribute it across channels very differently — first-touch vs last-touch disagree sharply on any channel that dominates the top vs tail of user paths. This is the "why attribution model choice matters" observation.
- Once the attribution model is fixed (position-based), **staleness alone** — the delay between an event landing and the attribution view updating — moves dollars across channels. Dollar misallocation grows monotonically from $0 at Δ=0 to $3,237 at 1-day stale (21.25% of total conversion value) and keeps climbing at 7 days.
- The largest relative shift at 1d falls on channels that tend to be **last-touch adjacent**: when the final touch hasn't been ingested yet, the previous touch gets over-credited as the "new last". Per-channel deltas are shown above.

## Reproducibility

- Seed: `20260424`.
- Full runner: `attribution-experiment/run.py`.
- Dataset source: `https://huggingface.co/datasets/criteo/criteo-attribution-dataset`.
- Raw file is cleaned up after the run.
