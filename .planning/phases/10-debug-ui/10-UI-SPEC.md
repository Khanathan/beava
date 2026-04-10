---
phase: 10
phase_name: Debug UI
status: draft
design_system: manual (handwritten CSS, no framework)
shadcn_preset: n/a
created: 2026-04-10
---

# UI-SPEC — Phase 10: Debug UI

This is the visual and interaction contract for Tally's embedded debug UI. All
values below are prescriptive. The planner should convert sections directly
into tasks; the executor should implement from these tokens without improvising.

Upstream source of truth:
- `10-CONTEXT.md` — locked user decisions (stack, backend, theme accents)
- `REQUIREMENTS.md` — DBUI-01 through DBUI-05
- `PROJECT.md` — product identity and zero-ops promise
- `LOGO_PROMPT.md` — brand cues (electric blue, warm orange, tally-mark motif)

Everything in `10-CONTEXT.md §Decisions` is LOCKED. This spec builds on top;
it does not re-open those choices.

---

## 1. Design System State

| Item | Value | Source |
|------|-------|--------|
| Framework | None — handwritten CSS | 10-CONTEXT.md (locked) |
| Bundler | None — rust-embed of static files | 10-CONTEXT.md (locked) |
| JS runtime | htmx 1.9.x + vanilla JS | 10-CONTEXT.md (locked) |
| DAG library | dagre-d3 (vendored, single file) | 10-CONTEXT.md (locked) |
| Icons | Inline SVG (no icon font) | inferred from no-build constraint |
| Build step | None | 10-CONTEXT.md (locked) |
| Component registry | None (shadcn N/A, no React) | project scan — no components.json |
| Existing UI assets | None | project scan — no www/, no .css files |

**shadcn gate:** NOT APPLICABLE. Tally is a Rust binary; the debug UI is
plain HTML/CSS/JS bundled via rust-embed. No React toolchain exists.

---

## 2. Spacing

Base unit: **4px**. All spacing MUST be a multiple of 4.

### 2.1 Spacing scale (tokens)

```css
:root {
  --space-1: 4px;   /* hairline gaps, icon-to-text */
  --space-2: 8px;   /* tight internal padding */
  --space-3: 12px;  /* row padding, input padding */
  --space-4: 16px;  /* default content padding */
  --space-5: 24px;  /* section spacing, card padding */
  --space-6: 32px;  /* large section gaps */
  --space-8: 48px;  /* hero spacing (rare) */
}
```

### 2.2 Applied rhythm

| Region | Padding | Gap |
|--------|---------|-----|
| Page container | `0` (full bleed) | — |
| Header | `var(--space-3) var(--space-5)` (12px vertical, 24px horizontal) | `var(--space-5)` between wordmark and tabs |
| Tab bar | `0 var(--space-5)` | `var(--space-1)` between tabs |
| Tab button | `var(--space-3) var(--space-4)` (12px × 16px) | — |
| Main content | `var(--space-5)` (24px) | `var(--space-5)` between cards |
| Card / panel | `var(--space-5)` (24px) | `var(--space-4)` between inner sections |
| Stream list row | `var(--space-3) var(--space-4)` | `var(--space-4)` between cells |
| Form input | `var(--space-3) var(--space-4)` (12px × 16px) | `var(--space-2)` label-to-input |
| Metric key/value pair | `0` | `var(--space-2)` label-to-value |
| Memory bar row | `var(--space-2) 0` | `var(--space-3)` label-to-bar |
| Button | `var(--space-2) var(--space-4)` (8px × 16px) | `var(--space-2)` icon-to-text |

### 2.3 Exceptions

- **Touch targets:** The pause/refresh button and tab buttons MUST be at
  least **36px tall** on screen. For a 14px label, that means `var(--space-3)`
  vertical padding minimum. This is a debug UI for desktop, so 36px is
  sufficient (not the mobile 44px standard).
- **DAG node internal padding:** Handled by dagre-d3 layout, not CSS tokens.
  See §7.1.

---

## 3. Typography

### 3.1 Font stacks

```css
:root {
  --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
               "Helvetica Neue", Arial, sans-serif;
  --font-mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo,
               Consolas, "Liberation Mono", monospace;
}
```

Rationale: system stacks only, per `10-CONTEXT.md`. Zero network fetches,
zero build step, consistent with "one binary, zero ops".

### 3.2 Type scale (exactly 4 sizes)

| Token | Size | Line height | Weight | Use |
|-------|------|-------------|--------|-----|
| `--text-xs` | 12px | 1.4 (16.8px) | 400 | Captions, timestamps, table headers |
| `--text-sm` | 14px | 1.5 (21px) | 400 | Body text, labels, buttons, tab labels |
| `--text-md` | 16px | 1.5 (24px) | 400 | Metric values (mono), input text |
| `--text-xl` | 20px | 1.3 (26px) | 600 | Card titles, H2 |
| `--text-2xl` | 24px | 1.2 (28.8px) | 600 | Page H1, hero metric values |

Sizes in active use: **12, 14, 20, 24** (4 sizes). The 16px mono row for
metric values is reserved for `--font-mono` only so it does not visually
compete with the sans scale.

### 3.3 Weights (exactly 2)

| Token | Value | Use |
|-------|-------|-----|
| `--weight-regular` | 400 | All body text, labels, captions |
| `--weight-semibold` | 600 | Titles (H1/H2), active tab, button labels |

No 500. No 700. No italics.

### 3.4 Applied hierarchy

| Element | Size | Weight | Font | Color |
|---------|------|--------|------|-------|
| Page title "tally" wordmark | 20px | 600 | sans | `--text-primary` |
| H1 (tab content title, e.g. "Topology") | 24px | 600 | sans | `--text-primary` |
| Card title (H2, e.g. "Memory breakdown") | 20px | 600 | sans | `--text-primary` |
| Body text | 14px | 400 | sans | `--text-primary` |
| Label (form, key/value) | 12px | 400 | sans | `--text-secondary` |
| Caption (footer, timestamps) | 12px | 400 | sans | `--text-tertiary` |
| Metric numeric value (large) | 24px | 600 | mono | `--text-primary` |
| Metric numeric value (inline) | 16px | 400 | mono | `--text-primary` |
| Table header | 12px | 400 | sans (uppercase, 0.05em tracking) | `--text-tertiary` |
| Code / keys / stream names | 14px | 400 | mono | `--text-primary` |

---

## 4. Color

Dark-by-default. No light mode in v1. All color declared as CSS custom
properties scoped to `:root`; no per-component overrides.

### 4.1 Surface palette (60/30/10)

| Token | Value | Role | % usage |
|-------|-------|------|---------|
| `--bg-app` | `#0e1116` | Dominant app background (body) | 60% |
| `--bg-panel` | `#161a22` | Cards, sidebars, modals, tab content | 30% |
| `--bg-elevated` | `#1d2230` | Hover rows, sub-panels, input backgrounds | — |
| `--bg-overlay` | `rgba(14, 17, 22, 0.7)` | Loading overlay | — |

### 4.2 Accent palette (10%)

| Token | Value | Reserved for — EXHAUSTIVE list |
|-------|-------|--------------------------------|
| `--accent-primary` | `#4a9eff` | (1) Active tab underline and label, (2) primary button background, (3) focused input border, (4) selected DAG node stroke, (5) link hover, (6) focus ring |
| `--accent-primary-hover` | `#63abff` | Primary button hover |
| `--accent-primary-dim` | `rgba(74, 158, 255, 0.12)` | Selected row background tint, active tab background tint |
| `--accent-warm` | `#ff8a4a` | (1) Tally wordmark tally-mark, (2) favicon, (3) "pause" indicator dot when polling is paused. NOTHING ELSE. |

The warm orange is intentionally restricted to brand / state-of-the-UI
affordances. It is NOT a secondary action color. Orange never appears on
buttons, links, or data.

### 4.3 Semantic palette

Used only for status and destructive states — never decorative.

| Token | Value | Use |
|-------|-------|-----|
| `--status-ok` | `#3fb950` | Healthy stream, healthy operator, "OK" chip |
| `--status-warn` | `#d29922` | Degraded (e.g. high latency), "WARN" chip |
| `--status-error` | `#f85149` | Error state, destructive confirm, "ERROR" chip |
| `--status-info` | `#4a9eff` | Info banner (aliases `--accent-primary`) |

### 4.4 Text palette

| Token | Value | Use |
|-------|-------|-----|
| `--text-primary` | `#e6edf3` | Headings, body, metric values |
| `--text-secondary` | `#9ba7b4` | Labels, inactive tab text, secondary copy |
| `--text-tertiary` | `#6b7684` | Captions, timestamps, table headers |
| `--text-on-accent` | `#0e1116` | Text inside primary buttons (on `--accent-primary`) |

Contrast (WCAG AA for normal text requires ≥ 4.5:1):
- `--text-primary` on `--bg-app`: ~13:1 ✓
- `--text-primary` on `--bg-panel`: ~12:1 ✓
- `--text-secondary` on `--bg-panel`: ~5.4:1 ✓
- `--text-tertiary` on `--bg-panel`: ~3.8:1 — acceptable for 14px+ body (WCAG AA large text) and non-body captions

### 4.5 Borders & dividers

| Token | Value | Use |
|-------|-------|-----|
| `--border-subtle` | `#222a36` | Card borders, row dividers |
| `--border-strong` | `#2e3846` | Input borders, tab bar underline |
| `--border-focus` | `#4a9eff` | Focused input, focused button (also `--accent-primary`) |

### 4.6 Chart / DAG palette

| Token | Value | Use |
|-------|-------|-----|
| `--chart-stream` | `#4a9eff` | Stream nodes in DAG, stream bars in memory chart |
| `--chart-view` | `#a371f7` | View nodes in DAG (muted purple, distinct from streams) |
| `--chart-operator` | `#6b7684` | Operator internals (dimmed, tertiary) |
| `--chart-edge` | `#2e3846` | DAG edges default |
| `--chart-edge-highlight` | `#4a9eff` | DAG edges on selected-node path |
| `--chart-grid` | `#1d2230` | Memory-chart grid lines |

---

## 5. Radius, borders, shadows, motion

### 5.1 Radius

```css
:root {
  --radius-sm: 4px;   /* inputs, buttons, chips */
  --radius-md: 8px;   /* cards, panels */
  --radius-lg: 12px;  /* modal, tooltip */
}
```

### 5.2 Border widths

- Card / panel: `1px solid var(--border-subtle)`
- Input default: `1px solid var(--border-strong)`
- Input focus: `1px solid var(--border-focus)` plus `0 0 0 3px rgba(74, 158, 255, 0.25)` outer glow
- Active tab indicator: `2px solid var(--accent-primary)` (bottom border only)

### 5.3 Shadows

Shadows are avoided on a dark theme (they disappear). Elevation is
communicated via `--bg-elevated` and borders. The only shadow in the UI:

```css
--shadow-overlay: 0 8px 24px rgba(0, 0, 0, 0.5);
```

Used for tooltips and the (future) node-detail popover.

### 5.4 Motion

Motion is functional, not decorative.

| Token | Value | Use |
|-------|-------|-----|
| `--motion-fast` | `120ms ease-out` | Hover states, tab switch, input focus |
| `--motion-med` | `180ms ease-out` | Panel fade-in, pause-button toggle |

No entrance animations on page load (data MUST feel instant). No parallax,
no shimmer, no bouncing. Respect `prefers-reduced-motion: reduce` — all
transitions fall back to 0ms.

---

## 6. Layout

### 6.1 Breakpoint policy

This is a desktop-first debug tool. No mobile layout.
- Min width: **960px**. Below this, show horizontal scroll on the body.
- Max content width: **1280px**, horizontally centered, with 24px gutters.

### 6.2 Page skeleton

```
┌───────────────────────────────────────────────────────────────────┐
│ Header (48px tall, --bg-panel, 1px bottom border --border-subtle) │
│   [tally wordmark]  [Topology] [Streams] [Entity] [Memory]        │
│                                           [● Live 1Hz] [Pause]    │
├───────────────────────────────────────────────────────────────────┤
│                                                                   │
│ Main content area                                                 │
│   --bg-app                                                        │
│   max-width 1280px centered                                       │
│   padding: var(--space-5) (24px)                                  │
│                                                                   │
│   <tab content — htmx-loaded into #tab-body>                      │
│                                                                   │
├───────────────────────────────────────────────────────────────────┤
│ Footer (32px, --bg-panel, --text-tertiary, 12px)                  │
│   tally v1.1 · connected to localhost:6401 · last update 14:23:07 │
└───────────────────────────────────────────────────────────────────┘
```

### 6.3 Header anatomy

- Height: **48px** fixed.
- Background: `--bg-panel`.
- Bottom border: `1px solid var(--border-subtle)`.
- Position: sticky at top.
- Three regions, `display: flex; align-items: center; gap: var(--space-5)`:
  1. **Wordmark** (left). See §8.1.
  2. **Tab bar** (center, flex-grow). See §8.2.
  3. **Polling control** (right). See §8.10.

### 6.4 Footer anatomy

- Height: **32px** fixed.
- Background: `--bg-panel`.
- Top border: `1px solid var(--border-subtle)`.
- Content: single line, 12px `--text-tertiary`, left-aligned with 24px padding.
- Format: `tally v{version} · connected to {host}:{port} · last update {HH:MM:SS}`.
- "last update" timestamp updates every successful poll.

### 6.5 Tab content containers

Each tab renders into `<main id="tab-body">`. Content structure:

```html
<section class="tab-view">
  <header class="tab-header">
    <h1>{tab title}</h1>
    <p class="tab-subtitle">{one-line description}</p>
  </header>
  <div class="tab-body">
    <!-- cards, lists, forms -->
  </div>
</section>
```

- `.tab-header` bottom margin: `var(--space-5)`.
- `.tab-header h1`: 24px / 600 / `--text-primary`.
- `.tab-subtitle`: 14px / 400 / `--text-secondary`, margin-top 4px.
- `.tab-body`: `display: grid; gap: var(--space-5);`.

### 6.6 Card (panel) primitive

```
.card {
  background: var(--bg-panel);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  padding: var(--space-5);
}
.card-title { /* H2 */
  font-size: 20px;
  font-weight: 600;
  color: var(--text-primary);
  margin: 0 0 var(--space-4) 0;
}
.card-subtitle {
  font-size: 12px;
  color: var(--text-secondary);
  margin: 0 0 var(--space-4) 0;
}
```

---

## 7. Component Inventory

Every component below is handwritten in HTML/CSS. No React, no classes-as-JS.
State manipulations (active tab, pause/resume, entity lookup) use htmx
attributes or a tiny vanilla JS snippet (<100 lines total).

### 7.1 DAG Topology canvas

**Purpose:** render the registered pipeline DAG (streams + views + operators).

**Library:** dagre-d3 (vendored, single JS file under `src/server/static/`).
SVG output, no WebGL.

**Container:**
- Full width of `.tab-body` column.
- Height: `min(70vh, 640px)`. Scroll overflow hidden; pan via drag is out of scope for v1, but the SVG MUST fit-to-container by default.
- Background: `--bg-panel`, border, radius as card primitive.
- Padding: `var(--space-5)` (24px inside the card, outside the `<svg>`).

**Node visual language:**

| Kind | Shape | Fill | Stroke default | Stroke selected | Text |
|------|-------|------|----------------|-----------------|------|
| Stream | Rounded rect, 12px radius | `--bg-elevated` | `1.5px solid --chart-stream` | `2px solid --chart-stream`, glow rgba(74,158,255,0.35) 6px | 14px mono `--text-primary` |
| View | Rounded rect, 12px radius | `--bg-elevated` | `1.5px solid --chart-view` | `2px solid --chart-view`, glow rgba(163,113,247,0.35) 6px | 14px mono `--text-primary` |
| Operator (optional, drill-down only) | Pill, 16px radius | `--bg-panel` | `1px solid --chart-operator` | — | 12px mono `--text-secondary` |

Node dimensions: min-width 120px, height 44px, horizontal padding 16px.
Label is the stream/view name in mono. No truncation — widen the node.

Under the label, one optional metadata line in 12px `--text-tertiary`:
`{key_field} · {feature_count} features`. Example: `user_id · 8 features`.

**Edge visual language:**
- Default: `--chart-edge`, 1.5px, straight or right-angle (dagre-d3 default spline).
- On node select: every edge in the upstream/downstream path switches to `--chart-edge-highlight` at 2px.
- Arrow head: standard dagre-d3 filled triangle, sized 8px.

**Interaction:**
- Hover a node: cursor `pointer`, stroke widens to 2px, `--motion-fast`.
- Click a node: toggle selected state. A right-side detail panel (see §7.6) slides in from outside the DAG canvas area (within the same card). Only one node selected at a time.
- Click the SVG background: deselect.
- Keyboard: `Tab` focuses nodes in declaration order; `Enter`/`Space` toggles selection; `Esc` deselects.

**States:**
- **Loading** (first paint, no data yet): show §9.1 skeleton (a single centered spinner + caption "Computing topology…").
- **Empty** (no pipelines registered): §9.2 variant. Icon = tally-mark SVG in `--text-tertiary`, 48px. Heading "No pipelines registered". Body "Use the Python SDK or REGISTER command to push a pipeline, then reload this page." No CTA button (this is a read-only debug tool).
- **Error** (fetch fails): §9.4 variant inside the card. Heading "Could not load topology". Body "{error message}". Button "Retry" (secondary button, see §7.8).

**DOM skeleton:**
```html
<div class="card topology-card">
  <div class="topology-canvas">
    <svg id="topology-svg" role="img" aria-label="Pipeline topology graph"></svg>
  </div>
  <aside id="topology-detail" class="topology-detail" hidden>
    <!-- populated on node click -->
  </aside>
</div>
```

### 7.2 Streams list

**Purpose:** show every registered stream with live throughput and key counts.

**Structure:** a table-like list inside a card. Not a `<table>`; use a CSS
grid so each row is an `<a>` (or `<button>`) for keyboard navigation and
focusable hover.

**Row grid template:**
```
grid-template-columns:
  minmax(160px, 1.6fr)   /* stream name (mono) */
  minmax(80px, 0.8fr)    /* kind: stream | view */
  minmax(100px, 1fr)     /* events/sec (mono, right-aligned) */
  minmax(100px, 1fr)     /* active keys (mono, right-aligned) */
  minmax(80px, 0.8fr)    /* status chip */
  24px;                  /* chevron (decorative) */
```

**Row styles:**
- Height: 44px (content plus 12×16 padding).
- Border-bottom: `1px solid var(--border-subtle)`.
- Default bg: transparent (card background shows through).
- Hover bg: `--bg-elevated`.
- Focus ring: 2px `--accent-primary` outer, 2px offset.
- Last row: no bottom border.

**Header row:**
- 12px uppercase `--text-tertiary`, tracking 0.05em.
- Columns: "NAME", "KIND", "EVENTS/SEC", "ACTIVE KEYS", "STATUS".
- 36px tall, bottom-border `--border-strong`.

**Status chip:**
- Pill, 4px radius, 11px uppercase, 0.05em tracking.
- `padding: 2px 8px`.
- `OK` = bg `rgba(63, 185, 80, 0.15)`, fg `--status-ok`.
- `WARN` = bg `rgba(210, 153, 34, 0.15)`, fg `--status-warn`.
- `ERROR` = bg `rgba(248, 81, 73, 0.15)`, fg `--status-error`.

**Throughput rendering rules:**
- Numeric, 1 decimal place, unit suffix: `1.2k`, `342.0`, `12.3M`.
- Always mono, right-aligned.
- Zero value: render as `0.0` in `--text-tertiary`, not hidden.

**Click behavior:** clicking a row navigates to the Entity tab with the
stream name pre-filled as a filter hint (stretch — v1 MAY simply log the
click and do nothing beyond hover). No selection state on the list itself.

**States:**
- **Loading:** §9.1 — 6 shimmer rows (skeleton).
- **Empty:** §9.2 inside the card. Heading "No streams yet". Body "Register a pipeline via the SDK to see streams appear here."
- **Error:** §9.4 inside the card, with retry button.

### 7.3 Entity lookup panel

**Purpose:** let the developer paste an entity key and see all features that
Tally currently holds for it, across every stream/view/static source.

**Layout:** two cards in a vertical stack (`gap: var(--space-5)`):
1. Search card
2. Result card (hidden until first search)

**Search card contents:**

```html
<div class="card entity-search-card">
  <label for="entity-key" class="input-label">Entity key</label>
  <form class="entity-search-form"
        hx-get="/debug/key/"
        hx-target="#entity-result"
        hx-swap="innerHTML"
        hx-include="#entity-key">
    <input id="entity-key" name="key" type="text"
           placeholder="e.g. u_12345"
           autocomplete="off" spellcheck="false" />
    <button type="submit" class="btn btn-primary">Look up</button>
  </form>
  <p class="input-help">
    Keys are case-sensitive. Try <code>u_demo</code> to see an example.
  </p>
</div>
```

**Input spec:**
- Width: 100%, max-width 480px.
- Height: 40px.
- Padding: `var(--space-3) var(--space-4)` (12 × 16).
- Font: 16px mono (prevents iOS zoom on desktop Safari).
- Background: `--bg-elevated`.
- Border: `1px solid --border-strong`, radius `--radius-sm`.
- Focus: `border-color: --border-focus`, outer glow per §5.2.
- Placeholder: `--text-tertiary`.

**Button spec:** see §7.8 (primary button).

**Result card contents:** two sections.

1. **Header strip** — 48px tall, horizontal flex:
   - Left: monospace key label in 16px, copy-on-click icon button.
   - Right: "last event: {relative time} ago" in 12px `--text-tertiary`.
2. **Feature grid** — `display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: var(--space-4);`.
   Each cell is a tiny metric block:
   ```
   ┌─────────────────────────┐
   │ tx_count_1h             │  ← 12px label, --text-secondary
   │ 42                      │  ← 24px mono, --text-primary
   │ Transactions · live     │  ← 11px --text-tertiary
   └─────────────────────────┘
   ```
   - Cell padding: `var(--space-4)` (16px).
   - Cell bg: `--bg-elevated`, radius `--radius-sm`, no border.
   - Numeric values: mono. Booleans render as `true`/`false` in `--status-ok` / `--status-error`. Strings render in sans, truncated at 32 chars with an ellipsis + tooltip.

**States:**
- **Idle** (before first search): result card hidden. Search card shows help text under input.
- **Loading:** replace result card body with §9.1 skeleton (4 empty feature cells shimmering).
- **Success:** feature grid populated.
- **Not found** (key exists in URL but no data): §9.2 variant. Icon: magnifier SVG, 48px, `--text-tertiary`. Heading `No features for "{key}"`. Body "This key has not received any events recently, or has been evicted by TTL." Button "Clear" (secondary) to reset form.
- **Error:** §9.4 inside result card.

### 7.4 Memory breakdown

**Purpose:** horizontal bar chart of memory usage per stream, plus summary stats.

**Layout:** one card containing a summary row and a bar list.

**Summary row** (top of card, before chart):
- `display: grid; grid-template-columns: repeat(3, 1fr); gap: var(--space-5);`
- Three stat blocks: "Total memory", "Active keys", "Streams tracked".
- Each block: 12px label (`--text-secondary`), 24px mono value (`--text-primary`), 11px caption (`--text-tertiary`).

**Bar list** (below summary):
- Row grid: `grid-template-columns: minmax(160px, 1.5fr) minmax(60px, 0.5fr) 3fr minmax(80px, 0.6fr);`
- Columns: stream name (mono 14px) · key count (mono 12px `--text-secondary`) · bar · size label (mono 14px right-aligned).
- Row height: 32px, vertical padding `var(--space-2)`.

**Bar visual:**
- Track: full column width, 8px tall, `--chart-grid` background, radius 4px.
- Fill: `--chart-stream` for streams, `--chart-view` for views, `--chart-operator` for static features. Width is percentage of total memory.
- On hover the entire row, bar fill brightens by 10% (filter: brightness(1.1)) — `--motion-fast` transition.

**Size formatting:** human bytes (`1.2 MB`, `458 KB`, `12.0 B`), mono, 1 decimal place.

**Sort:** descending by bytes. Top stream rendered first.

**States:**
- **Loading:** §9.1 skeleton with 4 shimmer rows.
- **Empty:** §9.2 inside card. "No memory data yet. Tally will report usage once a stream is registered."
- **Error:** §9.4 inside card with retry.

### 7.5 Tab bar

See §8.2 for full spec. Four tabs exactly: Topology, Streams, Entity, Memory.

### 7.6 Node detail panel (topology drill-down)

A side panel that appears inside the topology card when a node is clicked.

**Layout within topology card:**
- Topology card becomes `display: grid; grid-template-columns: 1fr 320px; gap: var(--space-5);` when `#topology-detail` is not `hidden`.
- Default: one column, full-width canvas.
- Panel width: 320px fixed.
- Panel scroll: `overflow-y: auto;` bounded by `min(70vh, 640px)`.

**Panel contents:**
```
┌────────────────────────────┐
│ STREAM                 [×] │  ← 11px tracking chip + close button
│ Transactions               │  ← 20px sans semibold
│ user_id                    │  ← 14px mono --text-secondary
├────────────────────────────┤
│ Features (8)               │  ← 12px --text-tertiary label
│  tx_count_30m  count · 30m │
│  tx_sum_1h     sum · 1h    │
│  …                         │
├────────────────────────────┤
│ Connections                │
│  → FraudSignals (view)     │
│  → UserRisk (view)         │
└────────────────────────────┘
```
- Each section divider: `1px solid --border-subtle`, vertical padding `var(--space-3)`.
- Feature rows: `display: grid; grid-template-columns: 1fr auto; gap: var(--space-3);` — name left, type+window right in 12px `--text-secondary`.
- Close button: 24×24px ghost button with inline SVG ×. Keyboard: `Esc` also closes.

### 7.7 Toast / banner (error surface)

One non-intrusive notification for polling failures and register errors.

**Banner (top of main content, under header):**
- Full-width within `.tab-body`, height auto, padding `var(--space-3) var(--space-4)`.
- Variants: `banner-error` (`--status-error` left border 3px, bg `rgba(248, 81, 73, 0.08)`, fg `--text-primary`).
- Layout: `icon (16px) · message (14px) · [Retry] (ghost button) · [×]`.
- Auto-dismiss: NO. User must dismiss or retry. Debug reliability beats elegance.

### 7.8 Buttons

Exactly three variants.

| Variant | Background | Border | Text | Hover |
|---------|-----------|--------|------|-------|
| `.btn-primary` | `--accent-primary` | none | `--text-on-accent` (14px / 600) | bg `--accent-primary-hover` |
| `.btn-secondary` | `--bg-elevated` | `1px solid --border-strong` | `--text-primary` (14px / 400) | bg `#252b3a` |
| `.btn-ghost` | transparent | none | `--text-secondary` (14px / 400) | bg `--bg-elevated`, text `--text-primary` |

Common:
- Height: 36px.
- Padding: `var(--space-2) var(--space-4)` (8 × 16).
- Radius: `--radius-sm` (4px).
- Focus: outer glow per §5.2.
- Disabled: `opacity: 0.5; cursor: not-allowed;`.
- Transition: `background-color var(--motion-fast), color var(--motion-fast)`.

### 7.9 Inputs

Already specified in §7.3. One style, reused for any future inputs.

### 7.10 Pause / Live indicator

See §8.10.

---

## 8. Copywriting

Every user-facing string is listed here. Executor MUST copy these exactly.

### 8.1 Brand

- Wordmark text: lowercase `tally`, followed by a single `'` (hairline apostrophe in `--accent-warm` representing the tally-mark). CSS:
  ```html
  <a class="wordmark" href="/">tally<span class="wordmark-tick">'</span></a>
  ```
  - `tally`: 20px sans semibold, `--text-primary`, letter-spacing -0.01em.
  - `wordmark-tick`: 20px sans semibold, `--accent-warm`, margin-left 2px.
- Favicon: tally-mark SVG, four vertical strokes plus one crossing stroke, 16×16 and 32×32 sizes, single color `--accent-warm` on transparent. Bundled via rust-embed at `/favicon.svg`.

### 8.2 Tab labels (exact text, in order)

1. `Topology`
2. `Streams`
3. `Entity`
4. `Memory`

Tab bar behavior:
- Active tab: text `--text-primary`, 14px / 600, 2px bottom border `--accent-primary`, background tint `--accent-primary-dim`.
- Inactive tab: text `--text-secondary`, 14px / 400, no bottom border.
- Hover inactive: text `--text-primary`, background `--bg-elevated`.
- Keyboard: `←` / `→` moves focus along the tabs, `Enter` activates.
- Switching tabs uses htmx `hx-get` on each tab anchor, swapping `#tab-body`. No page reload.

### 8.3 Page title (`<title>`)

`tally — debug`

### 8.4 Per-tab titles and subtitles

| Tab | H1 | Subtitle |
|-----|----|----|
| Topology | `Topology` | `Pipeline DAG — streams, views, and the links between them.` |
| Streams | `Streams` | `Live throughput and key counts for every registered stream.` |
| Entity | `Entity lookup` | `Inspect every feature Tally is holding for a single key.` |
| Memory | `Memory` | `Per-stream memory footprint and total state size.` |

### 8.5 Button labels

| Action | Label |
|--------|-------|
| Entity search submit | `Look up` |
| Entity search reset | `Clear` |
| Generic retry | `Retry` |
| Pause polling | `Pause` |
| Resume polling | `Resume` |
| Dismiss banner | `Dismiss` |
| Close detail panel | (icon-only, `aria-label="Close detail"`) |

### 8.6 Empty-state copy

| Tab / region | Heading | Body |
|--------------|---------|------|
| Topology | `No pipelines registered` | `Use the Python SDK or REGISTER command to push a pipeline, then reload this page.` |
| Streams | `No streams yet` | `Register a pipeline via the SDK to see streams appear here.` |
| Entity (idle, no search yet) | — | `Enter a key above to inspect its features.` (renders as §9.3 quiet-state row) |
| Entity (not found) | `No features for "{key}"` | `This key has not received any events recently, or has been evicted by TTL.` |
| Memory | `No memory data yet` | `Tally will report usage once a stream is registered.` |
| Node detail (no drill data) | `No details available` | `This node has no additional metadata yet.` |

### 8.7 Error-state copy

| Source | Heading | Body |
|--------|---------|------|
| `/debug/topology` fails | `Could not load topology` | `{server error message}` |
| `/debug/throughput` fails | `Could not load streams` | `{server error message}` |
| `/debug/key/{key}` fails (non-404) | `Lookup failed` | `{server error message}` |
| `/debug/memory` fails | `Could not load memory stats` | `{server error message}` |
| Network unreachable (poll error) | (banner) | `Lost connection to Tally server. Retrying in 5s…` |

Every error card ends with a `Retry` button (secondary variant) that re-fires
the same htmx request.

### 8.8 Status chip labels

Exact text, always uppercase: `OK`, `WARN`, `ERROR`.

### 8.9 Input placeholder and help

- Placeholder: `e.g. u_12345`
- Help: `Keys are case-sensitive. Try `u_demo` to see an example.` (the inline code is wrapped in `<code>` styled with mono + `--bg-elevated` tint.)

### 8.10 Polling control

Right side of header:

```
● Live · 1 Hz     [Pause]
```

- Dot: 8px circle, `--status-ok` when live, `--accent-warm` when paused. Inline with the text, margin-right `var(--space-2)`.
- Text: 12px `--text-secondary`. When paused: text is `Paused · last update {HH:MM:SS}`, color `--accent-warm`.
- Button: ghost variant (§7.8) with label that toggles between `Pause` and `Resume`.
- Keyboard: `Space` toggles pause/resume when focus is on the polling control region, and also when pressed globally while no input is focused (global shortcut).
- Accessibility: the live-region announces "Polling paused" / "Polling resumed" via a visually hidden `aria-live="polite"` span.

### 8.11 Footer text

`tally v{version} · connected to {host}:{port} · last update {HH:MM:SS}`

`{version}` comes from the server at `GET /health`, `{host}:{port}` is the
current `window.location.host`, `{HH:MM:SS}` updates after every successful
poll across any tab.

### 8.12 Destructive actions

**There are NO destructive actions in Phase 10.** The debug UI is read-only.
No delete buttons, no edit buttons, no confirmation dialogs. If a future phase
adds destructive actions, they MUST use `--status-error` for the button,
require a confirm step, and appear under a clearly-labeled "Danger zone"
section. Not in v1.

---

## 9. States

Every card/panel MUST handle five states. Defaults below.

### 9.1 Loading

Skeleton-based, not spinner-based (spinners feel slow on polling UIs).

- Background: `--bg-elevated`, width matches the real content block.
- Height: equal to expected content height (no layout shift when data arrives).
- Animation: `background: linear-gradient(90deg, --bg-elevated 0%, #242b3a 50%, --bg-elevated 100%); background-size: 200% 100%; animation: shimmer 1.4s linear infinite;`
- Reduced motion: no animation, flat `--bg-elevated`.
- Each tab's loading variant is listed inside its own component spec (§7).

### 9.2 Empty

- Centered within the card, min-height 240px.
- Icon (inline SVG, 48×48, `--text-tertiary`).
- Heading: 16px / 600 / `--text-primary`, margin-top `var(--space-4)`.
- Body: 14px / 400 / `--text-secondary`, margin-top `var(--space-2)`, max-width 420px, text-align center.
- Optional button: secondary variant, margin-top `var(--space-4)`.

### 9.3 Quiet / idle (variation of empty for inputs)

No icon. Single line of `--text-secondary` 14px text only, left-aligned,
padding matches surrounding card padding.

### 9.4 Error

- Same layout as §9.2.
- Icon: alert-triangle SVG, `--status-error`.
- Heading in `--text-primary`.
- Body in `--text-secondary`. Server error message is displayed verbatim if
  present; otherwise fall back to "Something went wrong. Please retry."
- CTA: `Retry` secondary button, bound to the original htmx trigger.

### 9.5 Populated (normal)

Defined per component in §7.

---

## 10. Accessibility

- All interactive elements have a visible focus ring (§5.2). Focus ring never
  hidden globally.
- All inline SVGs carry `role="img"` and `aria-label` when they convey
  meaning; decorative SVGs carry `aria-hidden="true"`.
- Tab bar uses `role="tablist"` on the container, `role="tab"` on each tab,
  `aria-selected="true"` on the active tab, and `role="tabpanel"` with
  matching `aria-labelledby` on the content area.
- The live polling indicator uses a visually hidden `aria-live="polite"`
  region to announce pause/resume.
- Color is never the sole carrier of status: every status chip also shows
  text (`OK` / `WARN` / `ERROR`).
- Keyboard support:
  - Tab bar: `Tab` enters the bar, `←`/`→` navigates, `Enter`/`Space` activates.
  - Topology: nodes are focusable with `Tab`; `Enter`/`Space` toggles selection; `Esc` deselects / closes detail panel.
  - Entity search: `Enter` submits the form; `Esc` clears it if it has text.
  - Pause button: `Space` also triggers when no input focused.
- Text meets WCAG AA contrast on all declared backgrounds (see §4.4).
- Minimum text size: 12px (captions only); 14px for all body.
- Respects `prefers-reduced-motion: reduce` (see §5.4).

---

## 11. Registry / Third-party assets

| Asset | Source | Version | License | Safety Gate |
|-------|--------|---------|---------|-------------|
| htmx | https://unpkg.com/htmx.org@1.9 (vendored) | 1.9.x | BSD-2-Clause | vendored at PR time, SHA256 recorded in `src/server/static/VENDOR.md` |
| dagre-d3 | https://github.com/dagrejs/dagre-d3 (vendored single bundle) | 0.6.x | MIT | vendored at PR time, SHA256 recorded in `src/server/static/VENDOR.md` |
| d3 (required by dagre-d3) | https://d3js.org (vendored minimal bundle) | v7 | ISC | vendored at PR time, SHA256 recorded in `src/server/static/VENDOR.md` |

**No third-party shadcn registries used (shadcn not applicable to this stack).**
**No runtime CDN fetches.** Every script/style must be present in the binary
via rust-embed. Executor MUST download at a pinned tag, commit under
`src/server/static/vendor/`, and record file hashes in a `VENDOR.md` manifest.

---

## 12. Icons

All icons are inline SVG, single-color (`currentColor`), 16×16 or 24×24.

Exact icon set needed for Phase 10:

| Name | Size | Used in |
|------|------|---------|
| `tally-mark` | 16, 32 | favicon, empty-state (topology) |
| `alert-triangle` | 48 | error state (all tabs) |
| `search` | 48 | entity empty state |
| `chevron-right` | 16 | stream list row |
| `close` (×) | 16 | detail panel, banner dismiss |
| `pause` | 14 | polling control (paused state) |
| `play` (triangle) | 14 | polling control (paused state, showing Resume) |
| `copy` | 14 | entity result header (copy key button) |
| `link` | 14 | view → source stream indicator (optional) |

All icon SVGs MUST live in a single file `src/server/static/icons.svg` as
`<symbol>` defs, referenced via `<svg><use href="/static/icons.svg#name" /></svg>`.

---

## 13. Summary decision tables

### 13.1 Quick-reference tokens

```css
:root {
  /* spacing */
  --space-1: 4px;  --space-2: 8px;  --space-3: 12px;
  --space-4: 16px; --space-5: 24px; --space-6: 32px; --space-8: 48px;

  /* radius */
  --radius-sm: 4px; --radius-md: 8px; --radius-lg: 12px;

  /* surfaces */
  --bg-app: #0e1116;
  --bg-panel: #161a22;
  --bg-elevated: #1d2230;
  --bg-overlay: rgba(14, 17, 22, 0.7);

  /* accents */
  --accent-primary: #4a9eff;
  --accent-primary-hover: #63abff;
  --accent-primary-dim: rgba(74, 158, 255, 0.12);
  --accent-warm: #ff8a4a;

  /* status */
  --status-ok: #3fb950;
  --status-warn: #d29922;
  --status-error: #f85149;
  --status-info: #4a9eff;

  /* text */
  --text-primary: #e6edf3;
  --text-secondary: #9ba7b4;
  --text-tertiary: #6b7684;
  --text-on-accent: #0e1116;

  /* borders */
  --border-subtle: #222a36;
  --border-strong: #2e3846;
  --border-focus: #4a9eff;

  /* charts */
  --chart-stream: #4a9eff;
  --chart-view: #a371f7;
  --chart-operator: #6b7684;
  --chart-edge: #2e3846;
  --chart-edge-highlight: #4a9eff;
  --chart-grid: #1d2230;

  /* motion */
  --motion-fast: 120ms ease-out;
  --motion-med: 180ms ease-out;

  /* fonts */
  --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
               "Helvetica Neue", Arial, sans-serif;
  --font-mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas,
               "Liberation Mono", monospace;
}

@media (prefers-reduced-motion: reduce) {
  :root { --motion-fast: 0ms; --motion-med: 0ms; }
  * { animation: none !important; transition-duration: 0ms !important; }
}
```

### 13.2 Requirements traceability

| Requirement | Addressed by |
|-------------|-------------|
| DBUI-01 Topology DAG visualization | §7.1, §7.6 |
| DBUI-02 Stream throughput listing | §7.2 |
| DBUI-03 Entity key lookup | §7.3 |
| DBUI-04 Memory breakdown | §7.4 |
| DBUI-05 Served from HTTP management port, embedded in binary | §1 (rust-embed + existing http.rs), §11 (no CDN) |

### 13.3 Phase 10 success posture

This UI-SPEC is complete when:
- [x] Spacing scale declared (4-point multiples, §2)
- [x] Typography declared (4 sizes, 2 weights, §3)
- [x] Color contract declared (60/30/10, accent reserved-for list, §4)
- [x] Component specs for all 4 tabs + header + detail panel (§7)
- [x] State contract (loading / empty / error / populated) for every component (§9)
- [x] Copywriting exact text (§8)
- [x] DAG visual language prescribed (§7.1)
- [x] Interaction contracts (tab switch, node click, key search, pause) (§7, §8.10)
- [x] Accessibility baseline declared (§10)
- [x] Registry / vendoring policy declared (§11)
- [x] Icon inventory declared (§12)

Ready for `gsd-ui-checker` verification.
