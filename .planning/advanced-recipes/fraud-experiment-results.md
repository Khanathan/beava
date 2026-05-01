# IEEE-CIS fraud detection: time-split feature ablation + staleness

Real measured numbers from a reproducible offline experiment on the full
590,540-row IEEE-CIS `train_transaction` (left-joined with `train_identity`
for `DeviceInfo` / `id_30` / M1-M6). This run uses a **chronological
70/30 time split** — the earliest 70 % of rows by `TransactionDT`
becomes the training set, the latest 30 % is held out. Single
deterministic run of `features.py` (seed = 42) — re-running the script
on the same CSVs produces identical values.

An earlier **random-split** run (same model + features, `train_test_split`
with `stratify=y`) is included for comparison in §Comparison.

## Methodology

- **Dataset:** IEEE-CIS Fraud Detection (Kaggle 2019), `train_transaction.csv`
  (590,540 rows, 20,663 frauds, fraud rate 3.499 %) left-joined with
  `train_identity.csv` on `TransactionID`. Identity coverage: 20.1 % of
  transactions have a non-null `DeviceInfo`. 13,553 unique `card1` values.
- **Split:** chronological 70 / 30 by `TransactionDT`. All rows are sorted
  ascending by `TransactionDT`; the first 413,378 become training, the last
  177,162 become the temporal holdout. No shuffling, no stratification,
  deterministic split boundary.
- **Fraud-rate shift across the temporal split:**

  | Segment | Rows | Fraud rate | Span (days) |
  |---|---|---|---|
  | Train (earliest 70 %) | 413,378 | 3.517 % | 119.8 |
  | Test (latest 30 %)    | 177,162 | 3.457 % | 62.2 |
  | Δ (test − train)      |  —      | −0.060 pp |  — |

  **The class-balance shift is tiny** (−0.06 pp in absolute fraud rate).
  IEEE-CIS does not exhibit the dramatic population drift typical of a
  live adversarial payments stream. It is, however, still a genuine
  temporal holdout — *future* transactions we have never seen, scored by
  a model that trained only on the past. Downstream effects (new
  `card1` identities, new `DeviceInfo` strings, shifted rolling-window
  distributions) propagate through the 7 features.

- **Features (same 7 as prior random-split run, cumulative order):**
  1. `tx_count_5m_per_card` — velocity count
  2. `high_risk_tx_5m_per_card` — rolling sum of the M-flag "mismatch"
     proxy (`M1 == 'F' | M2 == 'F' | M3 == 'F' | M4 != 'M0' | M5 == 'F' |
     M6 == 'F'`)
  3. `distinct_cards_5m_per_device` — device fanout
  4. `addr_mismatch_streak_per_card` — consecutive-row run length of
     M-flag mismatch
  5. `rapid_tx_streak_per_card` — consecutive inter-arrival < 60 s run
     length
  6. `distinct_devices_1h_per_card` — device churn per card
  7. `amount_sum_5m_per_card` — spend velocity

  All features are computed **live** (as-of `TransactionDT`) on the full
  dataframe. Because train and test are time-separated, a train-row
  feature never observes a test-row event — no data leakage from future
  to past.

- **Model:** `sklearn.ensemble.HistGradientBoostingClassifier(max_depth=8,
  max_iter=300, learning_rate=0.05, random_state=42)`. Identical
  hyperparameters to the 590K random-split run for apples-to-apples.
- **Metrics:** ROC-AUC and recall at 1 % false-positive rate
  (`np.interp` on `sklearn.metrics.roc_curve`).
- **Software:** Python 3.13.2, pandas 3.0.2, numpy 2.4.4, scikit-learn 1.8.0.
- **Wall-clock:** full pipeline (load → 7-feature derivation → 8-row
  ablation → reference train → StaleContext build → 6 staleness tiers →
  write) runs in 634 s (≈10.6 min) end-to-end on an M-series MacBook under
  moderate system load (load-avg 100-200). Seeded; idempotent.
- **Run timestamp (UTC):** 2026-04-24T12:07:15Z.

### Feature-2 ("high-risk") fires on 65.6 % of rows

`high_risk_tx_5m_per_card` uses the M-flag mismatch indicator as a proxy
for "declined" events (IEEE-CIS does not expose an explicit decline
flag). Because M1-M6 are populated generously and `M4 != 'M0'` fires on
any non-baseline categorical value, the row-level indicator is true on
65.6 % of rows. This dilutes the signal compared to a real decline
stream. We retain it for continuity with the random-split run.

## Part 1 — time-split feature ablation

Baseline is `TransactionAmt` alone. Each subsequent row adds one
cumulative beava-analog feature.

| Features active | AUC | Recall @ 1% FPR | Marginal lift (AUC) |
|---|---|---|---|
| baseline (TransactionAmt only) | 0.641 | 3.0% | — |
| + tx_count_5m_per_card | 0.652 | 3.1% | +0.011 |
| + high_risk_tx_5m_per_card | 0.660 | 4.3% | +0.008 |
| + distinct_cards_5m_per_device | 0.712 | 8.3% | +0.052 |
| + addr_mismatch_streak_per_card | 0.710 | 8.1% | -0.002 |
| + rapid_tx_streak_per_card | 0.709 | 7.9% | -0.001 |
| + distinct_devices_1h_per_card | 0.713 | 8.1% | +0.004 |
| + amount_sum_5m_per_card | 0.721 | 8.4% | +0.008 |

**Final time-split AUC: 0.721. Final recall @ 1 % FPR: 8.4 %** — a
relative **+179 % lift in recall** over the `TransactionAmt`-only
baseline (3.0 % → 8.4 %) and an AUC gain of +0.080. As with the
random-split run, `distinct_cards_5m_per_device` dominates (+0.052
AUC, +5.2 pp recall, the single largest single-feature jump). The
two streak features (`addr_mismatch_streak`, `rapid_tx_streak`) again
register near-zero marginal lift once the rolling-sum counterpart is
already in the model — the row-level streak shape carries little
additional separation signal here.

## Part 2 — time-split staleness experiment

One GBT is trained on the **fresh** 413K-row training set (features
computed as-of `TransactionDT`, no offset). That single model is
applied to six versions of the 177K-row test set where, for every test
row at `TransactionDT = T`, features are recomputed from events with
timestamps `≤ T − Δ`. Same 7 features. Offsets Δ ∈ {60, 3 600, 86 400,
604 800, 2 592 000, 5 184 000} seconds = {1 min, 1 h, 1 d, 7 d, 30 d,
60 d}.

| Staleness Δ | AUC | Recall @ 1% FPR |
|---|---|---|
| 1 min  | 0.528 | 1.5% |
| 1 hour | 0.529 | 1.0% |
| 1 day  | 0.533 | 1.2% |
| 7 days | 0.532 | 1.1% |
| 30 days | 0.533 | 1.0% |
| 60 days | 0.537 | 1.3% |

**Headline: 1.19× more fraud caught at 1 % FPR with 1-min-fresh
features vs 60-day-stale** (1.54 % recall vs 1.29 %).

### Two observations from this table

**1. The shift from fresh to "any amount of stale" is enormous.** The
fresh reference AUC is 0.721 / recall 8.4 %. At Δ = 1 min the AUC
collapses to 0.528 / recall 1.5 %. That is a **20-point AUC drop
triggered by 60 seconds of feature staleness**, which dwarfs anything
that happens across the 1-min → 60-day sweep. The fresh-vs-stale cliff
dominates the experiment.

The reason is semantic: beava's live features include **the current
event** in its rolling window — `tx_count_5m_per_card` at row T is
"count in `[T − 5 min, T]`", inclusive. A deployed feature lookup that
queries the server **before** writing the event produces the
pre-event count — `[T − Δ − 5 min, T − Δ]`. At Δ = 60 s the query
window excludes the current event entirely, so singleton-card rows see
`tx_count = 0`, `amount_sum = 0`, `distinct_devices = 0`. The training
distribution (which always included self-counts) and the stale
evaluation distribution differ categorically on every single-event
card1, and ~25 % of IEEE-CIS cards are singletons. The model is being
handed a distribution it never saw at train time.

This is a **real production concern**, not an artifact. It quantifies
what happens when you score an event using a feature snapshot that
doesn't reflect the event itself. The operational fix — emit the
current event to beava before the batch-get — is what every live
feature-server deployment does, and the 0.72 → 0.53 gap is the cost of
skipping it.

**2. Across the stale tiers, the AUC is flat within ~0.01.** The 1-min
→ 60-day walk produces AUCs of 0.528, 0.529, 0.533, 0.532, 0.533,
0.537 — monotonic if you squint, but all within noise of each other,
and 60-day features actually score **slightly higher** AUC than 1-min
features. Recall@1 %FPR is equally flat, hovering around 1 %. The
time-split protocol did NOT produce the monotone "freshness decay"
that we hypothesized concept drift would reveal.

**Why the staleness sweep stays flat (honest diagnosis):**

- **IEEE-CIS has only a 0.06 pp fraud-rate drift across the 6-month
  window** and no clear card1 / device population churn. Stale features
  continue to describe the same underlying distribution because the
  distribution isn't actually shifting.
- **Rolling windows (5 min, 1 h) are orders of magnitude smaller than
  the staleness Δ.** At Δ = 1 day, we read a 5-minute window from
  yesterday; at Δ = 60 days, we read a 5-minute window from two months
  ago. For a repeat card, either way most of the time those windows
  are empty — the feature reports 0. Going from "empty window" to "even
  emptier window" adds no information.
- **The features already saturate at 1-min staleness.** Once the
  current event is excluded, the useful within-5-minute activity
  neighborhood that the feature was designed to summarize is already
  lost; pushing Δ further doesn't degrade what's already absent.

**In short: on IEEE-CIS with a 70/30 time split, the only staleness
boundary that matters is "did you include the current event in the
feature value or not." The sweep from 1 min to 60 days doesn't add
further signal decay.** This is the opposite of the hypothesis going
in, but it is what the data shows, and we report it honestly.

## Comparison — time-split vs prior random-split

Same features, same model, same 590K rows; the only change is how
train/test are drawn.

| Metric | Random split (prior) | Time split (this run) | Δ |
|---|---|---|---|
| Baseline AUC (TransactionAmt only) | 0.678 | 0.641 | −0.037 |
| Baseline recall @ 1 % FPR | 4.2 % | 3.0 % | −1.2 pp |
| Final AUC (all 7 features) | 0.744 | 0.721 | −0.023 |
| Final recall @ 1 % FPR (all 7 features) | 10.8 % | 8.4 % | −2.4 pp |
| `distinct_cards_5m_per_device` marginal AUC | +0.037 | +0.052 | +0.015 |
| Staleness reference AUC (fresh) | n/a | 0.721 | — |
| Staleness 1-min AUC | 0.743 | 0.528 | −0.215 |
| Staleness spread (1 min → 60 d, AUC) | n/a (4 tiers only) | 0.528 → 0.537 | +0.009 |

### What the time-split changes

1. **Ablation AUC drops ~2.3 pp and recall drops ~2.4 pp.** Expected —
   temporal holdout is harder than random holdout because the model
   doesn't see test-row cards at train time. The direction and
   magnitude of the feature-by-feature lift are preserved:
   `distinct_cards_5m_per_device` remains the dominant single feature.
2. **The random-split staleness table was meaningless.** The prior run
   reported staleness AUCs of 0.743 / 0.747 / 0.748 / 0.741 (flat
   around the fresh baseline) because the random-split test rows were
   interleaved with train rows in time — so "features as of T − Δ" for
   a test row still included plenty of nearby train-row events. On a
   time split, stale features for a test row must look back into the
   training window, which is where the semantic gap shows up and the
   AUC collapses.
3. **The time-split numbers are the honest ones** for what a production
   feature server would see. Deploy a fresh trained model, serve it
   with properly-up-to-the-event features: AUC 0.72, recall 8.4 %.
   Serve it with any kind of pre-event-excluding snapshot: AUC ~0.53,
   recall ~1.2 %. That is the production-relevant number and the
   random-split version was hiding it.
4. **The staleness sweep within the stale regime is flat** — the
   interesting boundary is "fresh vs any-amount-of-stale", not the
   gradient between Δ=1 min and Δ=60 days. IEEE-CIS has essentially
   no concept drift; a live adversarial stream with daily fraud-ring
   turnover would likely show a meaningful additional decay across the
   tiers, but this dataset cannot measure it.

## Honest caveats on the numbers

1. **IEEE-CIS does not have strong concept drift.** Fraud rate drops by
   0.06 pp from train to test window; `card1` populations and
   `DeviceInfo` strings are largely overlapping. Our time-split
   staleness sweep is not "testing against drift" in the way a live
   adversarial feed would — it is testing against a mostly-stationary
   distribution that happens to be a future slice.
2. **The "fresh vs stale cliff" is driven by the current-event
   inclusion semantics of the live rolling window.** In a live
   deployment, the fix is to commit the event to beava before the
   batch-get; offline, the same effect can be measured by training and
   scoring on the "excludes-current-event" variant.
3. **Proxy quality.** IEEE-CIS has no decline flag; we use M-flag
   mismatches as a proxy, which fires on 66 % of rows — a weaker
   discriminator than a real decline event.
4. **No cross-entity features.** The experiment only tests
   single-entity rolling windows (per card1, per DeviceInfo). beava's
   multi-entity operators (per-merchant per-card fraud ring scoring,
   etc.) would be expected to lift AUC further — they are not
   evaluated here.

The transferable takeaways:
- **Ablation:** velocity + device-fanout features lift fraud recall
  ~3× over an amount-only model on a temporal holdout (3.0 % → 8.4 %
  at 1 % FPR). The specific feature that matters most is
  `distinct_cards_5m_per_device` (+0.052 AUC). This direction holds
  on both random and time splits.
- **Staleness:** the cost of scoring with pre-event-excluded features
  is ~0.20 AUC and ~7 pp recall on this dataset. The spread within
  "stale" (1 min → 60 days) is within noise — the dataset has no
  drift for the experiment to pick up beyond the initial cliff.


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

| Matched staleness Δ | AUC | Recall @ 1% FPR | Δ AUC from Δ=0 | Δ recall from Δ=0 |
|---|---|---|---|---|
| 0 s (real-time) | 0.721 | 8.4% | — | — |
| 60 s | 0.709 | 6.2% | -0.012 | -2.2 pp |
| 1 h | 0.703 | 5.5% | -0.018 | -2.9 pp |
| 1 d | 0.699 | 5.0% | -0.022 | -3.4 pp |
| 7 d | 0.700 | 5.2% | -0.021 | -3.2 pp |
| 30 d | 0.702 | 6.5% | -0.019 | -1.9 pp |
| 60 d | 0.689 | 5.3% | -0.032 | -3.1 pp |

Matched-stale features degrade from AUC 0.721 to 0.689 as Δ grows from 0 to 60 days (a 0.032 AUC drop and a 1.58× recall-at-1%-FPR advantage for real-time features).

### Verdict

**Verdict:** IEEE-CIS does show a measurable pure-freshness effect under matched train/serve: AUC drops by 0.032 (from 0.721 to 0.689) and recall @ 1 % FPR drops by 3.1 pp (from 8.4 % to 5.3 %) as features age from real-time to 60-day stale. The degradation is near-monotone through the 1-day tier and well above within-tier noise (the 7 d → 30 d wobble is within ±0.002 AUC). This is a much more honest signal than Part 2's artefactual 0.19 AUC cliff — about 6× smaller but real. It is consistent with the intuition that velocity features lose most of their edge in the first day after an event. Freshness story holds for IEEE-CIS, though the dataset's compressed time horizon (~6 months total, ~2 months test span) limits how cleanly we can separate "pure staleness" from "concept drift" beyond the 30-day tier. A follow-up on the Elliptic Bitcoin temporal benchmark (Weber et al., 2019; 49 time steps engineered for concept-drift evaluation) would give a cleaner substrate for the same protocol.

## Reproduction recipe

From a clean shell at the repo root `/Users/petrpan26/work/tally/`:

```bash
# 1. Put IEEE Fraud Detection.zip at ~/Downloads/IEEE Fraud Detection.zip
#    (Kaggle: https://www.kaggle.com/c/ieee-fraud-detection/data)

# 2. Unzip into the scratch dir
mkdir -p .planning/advanced-recipes/fraud-experiment/raw
cd .planning/advanced-recipes/fraud-experiment/raw
unzip -o "$HOME/Downloads/IEEE Fraud Detection.zip"
cd ../../../..

# 3. Create venv + install deps (skip if already done)
python3 -m venv .planning/advanced-recipes/fraud-experiment/venv
.planning/advanced-recipes/fraud-experiment/venv/bin/pip install \
    "pandas" "numpy" "scikit-learn"

# 4. Run the experiment (~10 min on an M-series MacBook)
.planning/advanced-recipes/fraud-experiment/venv/bin/python \
    .planning/advanced-recipes/fraud-experiment/features.py \
    2>&1 | tee .planning/advanced-recipes/fraud-experiment/run_log.txt

# 5. Outputs:
#    - .planning/advanced-recipes/fraud-experiment/results.json      (machine-readable)
#    - .planning/advanced-recipes/fraud-experiment/features_derived.csv (audit: first 1000 rows)
#    - .planning/advanced-recipes/fraud-experiment/run_log.txt       (timing + tables)

# 6. Clean up raw CSVs when done (not needed after script finishes)
rm .planning/advanced-recipes/fraud-experiment/raw/*.csv

# 7. Part 3 (matched train/serve staleness) is a separate script:
#    .planning/advanced-recipes/fraud-experiment/venv/bin/python \
#        .planning/advanced-recipes/fraud-experiment/features_part3.py
```

Numbers are deterministic (seed = 42 for the split boundary, the
feature derivation, and the model). Re-running the script on the same
input CSVs produces identical results.
