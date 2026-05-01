# beava guide chapter / recipe pattern (v2)

**Source of truth.** Every chapter and recipe page on `/guide/` follows this exact pattern. Reference implementations:

- `beava-website/project/guide/chapter-1/index.html` — full pedagogy chapter (4 parts, 2 dashboards)
- `beava-website/project/guide/recipes/fraud/index.html` — recipe (3 parts, 1 dashboard)

If you're writing a new recipe, copy fraud and modify. If you change the pattern, update this doc + the references.

---

## File layout

```html
<!doctype html>
<html lang="en">
<head>
  <link rel="stylesheet" href="/styles/colors_and_type.css">
  <link rel="stylesheet" href="/styles/site.css">
  <!-- no inline <style> — everything ships from site.css -->
</head>
<body class="beava">
  <div id="root"></div>
  <script src="https://unpkg.com/react@18.3.1/..."></script>
  <script src="https://unpkg.com/react-dom@18.3.1/..."></script>
  <script src="https://unpkg.com/@babel/standalone@7.29.0/babel.min.js"></script>
  <script type="text/babel" src="/js/Shared.jsx"></script>
  <script type="text/babel">
    /* page-specific JSX: data, useXState, App, render */
  </script>
</body>
</html>
```

Page-specific code lives entirely inside the `<script type="text/babel">` block. **Never** add inline `<style>` — extend `site.css` if you need new classes.

---

## Width system

Outer container: `.beava-container` (1040px max).

Inside:

- `.prose-width` — 780px content + 40px each-side padding = 860px outer. Narrative text, headers, action buttons, Pusher form.
- `.wide-prose` — 860px max. Code blocks, dashboards, request/response pairs, cost estimators. **Same outer left/right edges as prose-width.**

Interactive widgets bleed past the prose-text edge by 40px on each side — that's the design intent. Don't fight it.

---

## Color contract (immutable)

| Color | Use |
|---|---|
| **Orange** (`var(--accent)`) | In-progress / actions / Claude / live feed pulse / "auto-refresh" pill / category chips |
| **Green** (`var(--beava-success)`) | "you did it" celebration ONLY (`.done` block + `pill-v2.ok` for 200 OK status) |
| **Cream / paper** | Backgrounds, code-well surfaces |
| **Brown-ink** | Body text, dark terminal background |
| **Info wash** (`var(--beava-info-wash)`) | "Surprise" callouts inside `.evidence-block` |

Never use orange for end-of-post. Never use green mid-post. The contract isolates the celebration moment.

---

## Page sequence (canonical order)

1. **Banner** — dark `<Banner>` (cloud waitlist), top of every page.
2. **Nav** — `<Nav active="...">`.
3. **`<div className="beava-container">`** — opens here.
4. **PostHeader** — breadcrumbs → orange eyebrow → 68px Alegreya title → 17px sub → 180px mascot right.
5. **InstallCallout** — cream-paper card with dark terminal showing docker/brew/curl. Optional but recommended.
6. **Part 1 · The stakes** — `SectionMarker` + 2-3 paragraphs with citations. Sets the persona, the real-world problem, the specific number we'll improve.
7. **Part 2 · The pipeline** —
   - `SectionMarker` (eyebrow + 40px H2)
   - Wide pipeline code block (`pre.code` inside `.wide-prose`, `padding: '28px 32px', fontSize: 13.5, lineHeight: 1.8`)
   - `AskClaudeSidebar` — exactly **1 per page**.
   - `ActionPill` — registration button.
   - `Pusher` — push form with `onRandom` + `onPush`.
8. **Send request gate** — see "Request flow" below. Hidden until first push.
9. **Send request reveal** — request + response pair, LiveFeed (Part 2) OR EntityDashboard + KPIs (Part 3 / recipe), StatusStrip with `onSend`.
10. **Part 3 · What the numbers say** — `SectionMarker` + paragraph + `<div className="evidence-block">`. Includes:
    - `eb-eyebrow` (cited dataset name + size + protocol)
    - `eb-headline` (the lift in plain language)
    - cited `<table>` with the ablation
    - `<div className="surprise-callout">` — info-wash bordered callout with **one** non-obvious finding
    - `eb-citations` (paper / dataset link, repro link)
11. **Part 4 · Cost at scale** — `SectionMarker` + `CostEstimator` with realistic per-entity bytes. **Always** include the "Events don't add memory" footer explaining why.
12. **YouBuiltThis** — green `.done` block. `title="Nice work. You just built"` + `titleEm="<thing>"` (italic green) + `primary={...}` + `secondary={...}` + `mascot="..."`. Body text states the perf number and one human reason it matters.
13. **NextPosts** — 2-card grid pointing at the next chapter + an adjacent recipe.
14. **`</div>`** — close container.
15. **Footer** — shared `<Footer/>`.

For recipes (3-part variant), Part 4 (Cost) and Part 3 (Evidence) can swap order — pick whichever sequences better. Fraud puts Evidence before Cost; that reads more cleanly because the Evidence motivates the cost discussion.

---

## Request flow (the hard-won UX)

User pushes events into a stream, but **the dashboard does not appear** until they explicitly click **Send request**. This mirrors how a real app uses beava: events go IN via push; features come OUT via batch-get.

State machine per dashboard section:

```
INITIAL  →  user has pushed N events  →  user clicks "Send request"
   ↓                  ↓                            ↓
hidden          (still hidden,                full dashboard
                 show curl preview            visible. live updates
                 + Send button)               on subsequent pushes.
```

### Implementation

1. Page-level state: `const [req, setReq] = React.useState(0);` (one counter per dashboard section)
2. Below the Pusher form, render conditionally:
    ```jsx
    {state.length > 0 && req === 0 && (
      <>
        <div>This is the request your app would send to beava:</div>
        <pre className="code">$ curl -X POST .../batch/X -d '[...]'</pre>
        <button className="run" onClick={() => setReq(n => n + 1)}>
          <span className="chev">&gt;_</span>Send request
        </button>
      </>
    )}
    {req > 0 && <div className="ff-note">↓ rendered from the query response</div>}
    ```
3. Wrap the wide-prose dashboard reveal in `<LiveWrap req={req}>`:
    ```jsx
    const LiveWrap = ({ req, children }) => {
      const [live, setLive] = React.useState(false);
      React.useEffect(() => {
        if (req > 0) {
          const id = requestAnimationFrame(() => requestAnimationFrame(() => setLive(true)));
          return () => cancelAnimationFrame(id);
        }
        setLive(false);
      }, [req]);
      if (req === 0) return null;
      return <div className={live ? 'live' : ''}>{children}</div>;
    };
    ```
4. Inside, use `<StatusStrip onSend={() => setReq(n => n + 1)}/>` — clicks bump the counter so subsequent pushes land in the live state with smooth flashes.

The `LiveWrap` ensures the `.live` class lands AFTER first paint, so the initial reveal does not burst-animate every existing entry. New entries arriving after that DO animate in.

---

## Animation rules

All entrance animations are **scoped to `.live`** so the initial dashboard reveal paints clean. Re-runs and new pushes animate normally.

Already in `site.css`:

- `.live .rail .event` — 600ms slide-from-top + orange-wash → white
- `.live .ecard` — 700ms fade-in + 8px rise
- `.live .rrow` — 600ms inset orange-wash flash
- `.ecard-flash-overlay` — per-update overlay (mounted via `key={flashNonce}` on the parent state's flashUid). 900ms orange pulse → fade.

Page-level state pattern:
```jsx
const [flashUid, setFlashUid]     = useState(null);
const [flashNonce, setFlashNonce] = useState(0);

const pushX = (...) => {
  setFlashUid(uid_of_thing_that_changed);
  setFlashNonce(n => n + 1);
  // ... update events + state ...
};

// Pass to EntityDashboard:
<EntityDashboard ... flashUid={flashUid} flashNonce={flashNonce}/>
```

Every push triggers a brief overlay pulse on the affected card. KPI numbers and bar fills transition smoothly via CSS transitions.

---

## Required content per section

### Part 1 — The stakes

- A specific persona or cited team. Stripe, Uber, Meta, an academic paper, a YC company.
- A concrete number that is the problem: "$280B in chargebacks", "98% of card-testers escape batch jobs", "ETA error costs $0.34 per delivery".
- A specific incident, not a generic "fraud is bad."

### Part 2 — The pipeline

- Headline: a number that sums up what we'll do. "Three features. One table. Live." or "18 lines."
- Code block must be runnable Python — `import beava as bv`, `@bv.stream`, `@bv.table(key=...)`, `e.agg(...)`, `bv.App(...).register(...).serve()`. No pseudocode.
- Gaegu `← annotation` after each agg line explaining what it computes.

### Part 3 — Evidence (`evidence-block`)

- **Real measured numbers.** Not aspirational. If you don't have a real experiment, link to a published one (with citation) or run the experiment first.
- **At least one Surprise callout.** Non-obvious finding the experiment surfaced. The "what surprised us" carries trust signal.
- **Methodology citation** at the bottom — dataset name, year, paper, your repro path on GitHub.

### Part 4 — Cost at scale

- `perEntityBytes` must be honest — sum the actual rolling-state operators.
- Footer must explain *why* events don't add memory (windows + sketches age out + per-entity, not per-event).
- Include 5 scale tiers spanning 100K → 1B for fraud-shaped problems, 10K → 100M for user-keyed problems.

### YouBuiltThis (green)

- `title` is the lead-in: "Nice work. You just built"
- `titleEm` is the punch: "live card-testing detection" or "a real feature store" — italic green.
- Body states: feature count, perf number, one provocative real-world cost statement.

---

## Per-entity sketch sizing reference

For the CostEstimator `perEntityBytes` math:

| Operator | Bytes per entity (hot) | Notes |
|---|---|---|
| `count()` (lifetime, i64) | 8 | |
| `count(window="24h")` (24× hourly buckets) | 192 | |
| `count(window="5m")` (single rolling counter) | 16 | |
| `sum(field, window="5m")` | 8 | |
| `latest(e.ts)` | 8 | |
| `distinct(field, window="5m")` (3-tier) | 96–128 | 95% in ExactArray (≤16), 5% in HashSet (≤1024); HLL for tail |
| `distinct(field, window="7d")` (3-tier) | 480 | wider tail → more HashSet promotion |
| GeoVelocity / GeoDistance | 32 | last position + velocity |
| EwMa / EwVar / EwZScore | 24 | running stats |
| LastN (10 items) | 80–160 | per-item depending on type |
| `key` (string + hash slot) | ~100 | varies with key length |
| row overhead | 64 | schema ptr + version + tombstone |

Sum the operators in the recipe + key + overhead. Round up 5-10% for slack.

---

## Mascot pose mapping

| Pose | Use |
|---|---|
| `pose-3` (action / log-flexed) | Chapter 1 (pedagogy), AskClaude (always), tutorial heroes |
| `pose-2` (greeting / wave) | Recipe heroes, YouBuiltThis on most chapters, footers |
| `work-pose` (with logs) | Guide landing hero, anything didactic |
| `mark-geometric` (flat icon) | nav/favicon/footer/avatars at <40px |

YOU BUILT THIS rotates poses per chapter for visual variety:
- Ch1: pose-3
- Fraud: pose-2
- Personalization: pose-3
- Anomaly: work-pose
- Attribution: pose-2
- Geospatial: pose-3

---

## Common Pusher form shapes

| Domain | Fields |
|---|---|
| Pedagogy (Ch1) | user_id, path, category (derived from path) |
| Fraud | card_id, merchant, amount |
| Personalization | session_id, item_id, action (view/click/cart) |
| Anomaly | route, status, latency_ms |
| Attribution | user_id, channel, touchpoint |
| Geospatial | driver_id, lat, lng (or pickup/dropoff for ETA) |

Keep forms to ≤4 fields. The `derived` flag marks computed fields (readonly, orange-wash bg).

---

## Ask Claude prompts (one per page)

Always render `<AskClaudeSidebar prompt="/beava ..." />` exactly **once** per page, in Part 2 right after the pipeline code. The prompt should be a plain-English description of the same pipeline — what a developer would type to Claude to generate it.

Avoid the phrase "let me build" — use action verbs: "track", "build", "score", "flag", "alert", "rank".

---

## Don't do

- ❌ Multiple AskClaudeSidebar blocks per page (the chat said this clutters; we shipped one variant).
- ❌ "Re-run now" — always "Send request".
- ❌ Auto-refresh that the user didn't ask for. The send-request gate is the model.
- ❌ Empty-state placeholders inside `.wide-prose`. If empty, hide the whole section.
- ❌ Different code-block styling per page. Always `pre.code` inside `.wide-prose` for hero pipelines, with the wide padding/leading.
- ❌ Inline `<style>` blocks — extend `site.css` instead.
- ❌ Orange end-of-post celebration. Always `.done` (green).

---

## Reference: data sources for the 5 use-case recipes

| Recipe | Dataset | Headline lift | Path |
|---|---|---|---|
| Fraud | IEEE-CIS 590K, 70/30 time split | 3.0% → 8.4% recall@1%FPR (2.8×) + 1.58× matched-staleness | `/guide/recipes/fraud/` |
| Personalization | Yoochoose RecSys 2015 | 7× CTR vs daily-batch | `/guide/recipes/personalization/` |
| Anomaly | NAB matched-recall | 5.2× fewer false alarms at equal recall | `/guide/recipes/anomaly/` |
| Attribution | Criteo display | per-channel MAPE at staleness tiers | `/guide/recipes/attribution/` |
| Geospatial | NYC TLC yellow taxi (Jan/Feb 2024) | 12.8s MAE improvement vs 1d-stale on 12.1m median trip | `/guide/recipes/geospatial/` |

Each lives in `.planning/advanced-recipes/{name}-experiment-results.md` with full reproduction recipe and citations.
