# Quest mode — design doc for next phase

Date: 2026-04-25.
Companion to `RECIPE-PATTERN.md` (the v2 page conventions) and `NEXT-SESSION-2.md` (the rebuild handoff). Read those first for context.

## The problem we're solving

The current chapters render correctly but feel **passive**. Reading-to-doing ratio is ~80/20. The "do" is just clicking a register button and pushing random events — no stakes, no goal, no job-to-be-done. Reader doesn't experience the cited lift; they just see the number on the page.

User's verdict: **"Right now the tutorial seems shallow and doesn't help users get in the zone."**

## The fix: quest mode

Each chapter ships with 2-4 **quests**. Each quest is a job-to-be-done scenario: persona, situation, verifiable outcome. The reader plays the role; the pipeline is the tool that answers the question; completing the quest auto-checks against state.

### Quest format (canonical)

```
☐ Quest N · <verb-phrase title>
You're the <persona>. <Situation in 1-2 sentences with a real
business stakes anchor.>

→ <Specific action: push these events, with these values, in this order.>
✓ Auto-completes when: state[uid].field >= threshold
🎯 Payoff: <One line — what you just did at scale, with the cited
   number from the experiment baked in.>
```

Quest auto-checks every render. Green checkmark + reveal of payoff line when satisfied. Final quest unlocks the green `YouBuiltThis` early — "you finished all the quests; you actually did the work."

### Shared traits across all quests

1. **Second-person persona.** "You're the fraud analyst on call." Not "the system flags."
2. **Time / business pressure.** Stripe just lost X. The customer is waiting. Marketing wants the answer by EOD.
3. **Verifiable end state.** A `done(state) => bool` function reads the page's live state and decides if the quest is met.
4. **Cited business payoff.** Every quest reveal echoes the experiment's published number ("Stripe catches 250M card-tests/yr; you just caught one of them").
5. **Quest validator must be deterministic.** Reader can rerun, refresh, push extra events and quests stay green.

## Per-chapter quest catalog (draft — refine in implementation)

### Chapter 1 · Per-customer dashboard
*Persona: "You're the PM at an e-commerce site."*

- **Quest 1 · Track a VIP.** "VIP customer `u_a33e` is shopping right now. Push 3 of her page-views from any 3 categories. After: how many distinct categories has she browsed today?"
  - Validator: `state['u_a33e'].views_total >= 3 && state['u_a33e'].distinct_cats_7d.size >= 3`
  - Payoff: "You just answered customer-success's most-asked question without a Postgres query."

- **Quest 2 · Identify power users.** "Marketing wants the top-3 power users this hour. Push activity for 5 different users (≥2 events each). Identify the top by `views_24h`."
  - Validator: `Object.keys(state).length >= 5 && Object.values(state).filter(u => u.views_24h >= 2).length >= 3`
  - Payoff: "Live cohort discovery, no nightly Spark job."

- **Quest 3 · Diagnose churn risk.** "Find a user with `views_24h ≥ 3` but `distinct_cats_7d == 1`. Customer success wants to reach out before they bounce."
  - Validator: any user matching that shape exists
  - Payoff: "You found a power-user-on-one-product — high churn risk pattern."

### Chapter 2 · Fraud detection
*Persona: "You're the fraud analyst on call."*

- **Quest 1 · Catch card-testing.** "Attacker is using card `c_7f3c` against 4 merchants in 60s. Push 8 declined transactions across 4 different merchants. Did your pipeline flag before the 8th?"
  - Validator: `state['c_7f3c'].tx_count_5m >= 8 && state['c_7f3c'].distinct_merchants_5m.size >= 4`
  - Payoff: "Each missed card-test costs the merchant ~$280 in Stripe chargebacks. You just saved 8 of them."

- **Quest 2 · Spot device fanout.** "5 cards swiped from one device in 30s. Push them. Watch `distinct_merchants_5m` spike on each."
  - Validator: `Object.values(state).filter(u => u.tx_count_5m >= 1).length >= 5`
  - Payoff: "Distinct-merchants-per-card is the single feature that does 5.2pp of the recall lift in our IEEE-CIS run."

- **Quest 3 · Confirm no false positive.** "A regular customer makes 3 normal purchases ($20-$80). Push them. Verify they DIDN'T get flagged."
  - Validator: pushed 3 events for one user, score < 60 threshold
  - Payoff: "Calibrated threshold. Your customer success team will not page you tonight."

### Chapter 3 · Personalization
*Persona: "You're the growth PM running A/B for the homepage."*

- **Quest 1 · Build session intent.** "User `s_returning` browsed 4 'shoes' items. Push the views. After: confirm `top_categories[0] == 'shoes'`. That's the boost your ranker would apply."
  - Validator: state['s_returning']'s top category from `top_categories` is shoes
  - Payoff: "Right-hot-category boost lifts CTR 3-7% in production. Yoochoose: 7.2% vs 0% baseline."

- **Quest 2 · Demote already-seen.** "Push 5 views on belt items 1-5 for `s_skipped`. Verify `recent_items` has 5 entries, all belts. The ranker uses this to demote what they've already seen."
  - Validator: `state['s_skipped'].recent_items.length === 5`
  - Payoff: "'Seen but didn't click' demotion. Sessions average 4-6min — batch can't do this."

### Chapter 4 · Anomaly detection
*Persona: "You're on call. /api/checkout just paged."*

- **Quest 1 · Catch a real incident.** "Push 5 normal events (latency ~200ms), then 3 incident events (~1500ms). Did z-score cross 3σ?"
  - Validator: `state['/api/checkout'].count_1m >= 8 && Math.sqrt(state['/api/checkout'].ewvar) > 200`
  - Payoff: "You caught it before customer-facing impact. NAB benchmark: 5.2× fewer false alarms than a stale detector at matched recall."

- **Quest 2 · No false alarm.** "Push 10 normal events to 5 different routes. Verify NONE got flagged."
  - Validator: 10 events pushed, no high-variance routes
  - Payoff: "Calibrated. Your on-call rotation stays calm."

### Chapter 5 · Attribution
*Persona: "You're the marketing analyst."*

- **Quest 1 · Trace a real path.** "User `u_7f3c` converted at $200. Their actual path: paid_search → email → display → buy. Push those 3 touches + the converted touch. Verify `first_touch=paid_search`, `last_touch=display`."
  - Validator: state shape matches the expected path
  - Payoff: "First-touch and last-touch credit live. Stale data would have lost the email touch entirely."

- **Quest 2 · Stale credit vanishes.** "Now query as-of-1-day-ago. Watch `display` lose credit. That's the 21% misallocation problem live, on data you just generated."
  - Validator: requires the staleness toggle (Quest 2 implies the side-by-side compare)
  - Payoff: "Criteo dataset: at 1-day staleness, 21.25% of conversion value vanishes."

### Chapter 6 · Geospatial ETA
*Persona: "You're the dispatcher. Rush hour just hit."*

- **Quest 1 · Watch traffic shift.** "Push 5 trips on midtown→west-village at 14-min duration (typical: 7 min). Query: has `avg_duration_s` climbed? Your next quoted ETA uses that."
  - Validator: `state['midtown→west-village'].avg_duration_s >= 700`
  - Payoff: "Without this, you'd quote 7min on a 14min trip and the customer thinks you're lying."

- **Quest 2 · The 6h-stale trap.** "Re-run with 6h-stale features. The ETA reverts to overnight-quiet (~6 min). NYC TLC: 6h-stale is worse than 1d-stale because periodicity beats recency."
  - Validator: requires the side-by-side compare
  - Payoff: "12.8s MAE improvement on the median 12-min trip. At 1M trips/day, that's 12,800s saved per fleet per day."

## Open design choices (decide before implementation)

### 1. Gating vs discoverable

- **Gated** (Ch1 should be this): Quest 1 must complete before Quest 2 visible. Dashboard partly hidden until Q1 done. Teaches concept ladder.
- **Discoverable** (Ch2-6 should be this): All 3 quests visible at start. Reader picks any. Less hand-holdy; respects that they've already done Ch1.

My recommendation: gated for Ch1 (pedagogy), discoverable for recipes.

### 2. Quest validation timing

Three options:
- **Immediate** — every state change re-checks. Quest goes green the moment criteria met. Risk: confusing if the reader hasn't read the explanation yet.
- **On Send-request** — quest only counts when reader explicitly queries. Reinforces the request gate from the v2 pattern. My recommendation.
- **On click** — explicit "Check quest" button. Most ceremony but gives the reader a deliberate moment.

### 3. Payoff reveal style

- **Inline checkmark + payoff line** (compact, fits in QuestList).
- **Expand into a callout** (more celebratory, takes more vertical space).
- **Auto-scroll the payoff into view + small confetti** (most "you did it" feeling, but fights the ground-truth-style voice).

My recommendation: inline checkmark + payoff line, with a brief background flash on the row.

### 4. What if reader does it accidentally?

The dashboard already updates from random pushes. If the reader hits "Send a random event" 10 times, multiple quests might pre-validate. Two strategies:
- **Allow it.** "You did the right thing without realizing — here's what you actually accomplished." Honest, low ceremony.
- **Lock until reader reads the quest.** Quest validators only fire if the reader has clicked an "I'm trying this quest" button. Annoying.

Recommendation: allow it. The payoff line teaches what they did.

### 5. Side-by-side compare component (for Ch4 / Ch5 / Ch6)

Quests 2 in those chapters require the **stale-vs-fresh side-by-side** I called direction "C" in the brainstorm. That's its own component build:

- 2 dashboards rendered side-by-side, same width as wide-prose / 2.
- Same events flow into both.
- Stale dashboard delays writes by a configurable Δ (1h, 1d, 6h, etc).
- Reader watches the divergence in real time.

Cost: ~120 lines for a `<StaleSideBySide Δ="1d" stream={events} />` component. Reusable across Ch4, Ch5, Ch6.

## Implementation plan

### Phase 1: ship `<QuestList>` (1 turn)
- Add to `Shared.jsx`. Takes `quests: [{ id, title, persona, body, validator, payoff }]` + `state` prop.
- Renders as a vertical card list above the Pusher.
- Auto-runs validators on every state change (or on Send-request — pick one above).
- Matches `.evidence-block` styling but with green checkmarks instead of orange surprise callouts.

### Phase 2: write Ch1 quests (1 turn)
- Pedagogy chapter is the riskiest; gating + validators must feel right.
- Validator functions for the 3 quests above.
- One screenshot pass to confirm reading flow + verify the QuestList doesn't fight the dashboard.

### Phase 3: write Ch2-6 quests (1 turn each)
- Use Ch1 + RECIPE-PATTERN.md as references.
- Each chapter gets 2-3 discoverable quests.
- Side-by-side stale-vs-fresh component built during Ch4 (anomaly), reused in Ch5/Ch6.

### Phase 4: polish (1 turn)
- Quest copy passes — make sure each quest has a real persona voice.
- Visual: green flash on completion.
- Accessibility: quest checkmarks announced to screen readers.

## Risk: scope creep

Quest mode tempts adding more interactivity (live code edit, data replay, achievements). **Resist.** Quests + side-by-side cover the "feel the lift" story. The other directions are v3.

## Files involved when this lands

- `beava-website/project/js/Shared.jsx` — add `QuestList`, `StaleSideBySide`
- `beava-website/project/styles/site.css` — `.quest-list`, `.quest`, `.quest.done`, `.quest-payoff`
- `beava-website/project/guide/chapter-1/index.html` — quest data + integration
- `beava-website/project/guide/recipes/{fraud,personalization,anomaly,attribution,geospatial}/index.html` — same
- `RECIPE-PATTERN.md` — append a "Quests section" describing the pattern so future recipes follow it
