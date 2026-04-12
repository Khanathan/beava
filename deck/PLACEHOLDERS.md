# Streamlet VAS 2026 Deck — Placeholders to Replace Before Submission

Every item below is marked in the PDF with a yellow dashed box. Swap them in
before shipping the deck to Vietnam AI Stars. These are the only spots in the
deck that rely on aspirational data — everything else is grounded in real code
or real resume content.

## 1. Slide 6 — "Developer experience: features as code"

**Placeholder:** demo GIF / screenshot of the Python SDK running end-to-end.

**What to capture:**
- Open a Python REPL (or a Jupyter cell).
- Define a small `@st.stream` class with 3–4 features.
- Call `app.register(...)` once.
- Call `app.push(MyStream, {...})` with a fake event.
- Show the returned feature dict printed in the terminal.
- Ideally, push 2–3 events in a row so the window counts visibly increment.

**Recommended format:** 8–12 second animated GIF, 1200×500 px, ≤1 MB.

**Path expected by the generator:** `deck/assets/demo_push.gif`
(The current build uses a text placeholder — swap in an image by editing
`slide_06_sdk` in `generate_deck.py` to replace the `draw_placeholder_badge`
call with `c.drawImage("assets/demo_push.gif", ...)` or a PNG frame.)

**Fallback if you can't record in time:** paste a screenshot of the REPL
output showing the feature dict returned from `push()`.

## 2. Slide 8 — "Performance targets"

**Placeholder:** four stat cards at the top of the slide currently show
design targets (100K+ events/sec, <100µs PUSH p99, <50µs GET p99, <5KB/key).
These were chosen from characteristics of comparable systems — they are
**not** measured from Streamlet yet.

**What to replace them with:** measured numbers from your own bench harness.

**How to produce the numbers:**
1. Write (or finish) the microbenchmarks in `benches/latency.rs` and
   `benches/throughput.rs` (already listed in the project structure).
2. Run `cargo bench` on a representative machine (specify it on the slide —
   e.g. "M1 Pro, 10 cores, single-threaded"). Judges will respect honest
   hardware disclosure.
3. Replace the four numbers in `slide_08_performance` with the measured
   values. Keep the accent colors and layout.
4. Delete the yellow `PLACEHOLDER — BENCHMARKS` banner at the bottom of
   the slide once real numbers are in.

**Minimum bar to keep the story credible:**
- PUSH p99 should be comfortably under 1 ms (target <100 µs).
- GET p99 should be under 500 µs (target <50 µs).
- Sustained throughput on one thread should exceed 50K events/sec.

If the first run falls short, that is useful information — either
optimize, or lower the claim and lean harder on the "one binary, zero ops"
story instead of raw numbers. Judges punish inflated benchmarks more than
modest ones.

## Optional polish (not strictly placeholders)

- **Slide 2 stat sources**: the four stats cite Google e-Conomy SEA 2024,
  MIC 2024, and Ministry of Public Security. If you want to be extra safe,
  add a small footer with the exact URLs or report titles. Judge Dr. Quoc
  Le in particular is a stickler for sourcing.
- **Slide 13 avatar**: the team slide uses an "HP" monogram circle. If you
  have a professional headshot you're comfortable sharing, swap the
  `c.circle(...)` + `c.drawCentredString("HP")` calls in `slide_13_team`
  for a `c.drawImage("assets/hoang_headshot.jpg", ...)` call.
- **Slide 1 logo**: consider adding a small Streamlet wordmark / logo
  above the "Streamlet" title. If you have one from `LOGO_PROMPT.md`,
  drop it into `deck/assets/logo.png` and add a `drawImage` call at the
  top of `slide_01_title`.

## How to rebuild after edits

```bash
cd deck
/opt/homebrew/bin/python3.13 generate_deck.py
# → writes Streamlet_VAS2026_Deck.pdf
```

The generator is deterministic — every edit to `generate_deck.py`
reproduces the same layout.
