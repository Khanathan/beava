# Docs page archetypes

Eight page archetypes, calibrated against the vision page. Every doc page on the site fits one of these shapes. New pages must declare their archetype.

---

## 1. Tutorial — `/get-started/*`, `/install/`, `/quickstart/`

**Goal:** get the reader from zero to a working result in under N minutes.

**Voice:** imperative ("Install", "Push", "Query"). Lowercase beava. Sentence-case headings.

**Length:** ≤80 lines.

**Structure:**
1. **Title + tagline** — one promise (`first feature in 60 seconds`).
2. **Smallest runnable code block** — 8–15 lines. Real code; not pseudo-code.
3. **One-paragraph explanation** of what just happened. No "this document".
4. **Next path** — one or two sub-headings (`## Install`, `## Over HTTP`) with code-first content.
5. **What's next** — exactly one or two onward links.

**Calibration page:** `/docs/quickstart/`.

---

## 2. Concept — `/concepts/*`, `/vision/*`, narrative pages

**Goal:** install a mental model. Reader leaves knowing *what* something is and *when* to reach for it.

**Voice:** vision-tone — first-person plural, concession + position framing, bold soundbites. Lowercase beava. Sentence-case headings.

**Length:** ≤120 lines (vision narrative pages can run longer).

**Structure:**
1. **Title + 1-line lede** in `> blockquote` form.
2. **One small code block** if it grounds the concept faster than prose. (Vision pages skip this — pure prose.)
3. **2–4 short prose paragraphs**. Concession + position. Bold soundbite at the end of one paragraph if it lands.
4. **`## When to use this`** OR **`## What's different`** — concrete contrast.
5. **`## See also`** — one to three links.

**Soft callouts:** `> **Heads up:**` (warnings, irreversible actions), `> **Tip:**` (non-obvious shortcuts).

**Calibration pages:** `/docs/vision/why-beava/`, `/docs/concepts/streams/`.

---

## 3. SDK reference — `/sdk-api/*`

**Goal:** the developer can install, register a pipeline, push events, and query features in their language. Reference for the rest.

**Voice:** part tutorial, part reference. Lowercase beava. Sentence-case headings. Idiomatic per language (camelCase for JS, snake_case for Python, exported types for Go).

**Length:** ≤300 lines (Python is the canonical surface, so larger; TS/Go ≤120).

**Structure:**
1. **Title + 1-line lede.**
2. **8–15 line code example** showing register → push → get.
3. **Two paragraphs** of prose: what this SDK does, what it doesn't (e.g., TS/Go are communicate-only).
4. **`## Install`** — one-liner per package manager.
5. **`## App`** — what calling `App()` looks like; embed mode vs HTTP/TCP. Code-first.
6. **`## Push events`** — code, then prose.
7. **`## Get features`** — code, then prose.
8. **`## Test fixtures`** — short snippet for unit tests.
9. **`## What's next`** — links to wire-spec, operator catalog, recipes.

**No `**Args:** / **Returns:** / **Raises:**` blocks.** Fold types into prose with inline snippets.

**Calibration pages:** `/docs/sdk-api/python/`, `/docs/sdk-api/shared/`.

---

## 4. Operator family — `/operators/*/`

**Goal:** the developer can find the right operator and copy a working example.

**Voice:** terse, code-led. Lowercase beava. Sentence-case headings.

**Length:** ≤180 lines per family.

**Structure:**
1. **Title** (`# Recency ops`).
2. **1-line lede** in `> blockquote`.
3. **One opening paragraph** framing what the family answers, plus shared invariants (e.g., "all recency ops use processing-time arrival order").
4. **One H3 per operator** — the order matters: alphabetical OR most-common-first.
   - Per H3: small code example → 1-paragraph prose → bolded `**Signature:**` line.
   - Optional: `*(Previously called `bv.<old>`)*` for renamed ops.
5. **`## See also`** — link to operator catalog and cost classes.

**Don't include:** Wire JSON, complexity tables, edge-case lists, Rust state structs. Those live elsewhere (wire-spec / cost-class / source).

**Calibration pages:** `/docs/operators/core/`, `/docs/operators/recency/`.

---

## 5. Wire / API / spec reference — `/wire-spec/`, `/http-api/`, `/error-codes/`, `/schema-evolution/`

**Goal:** SDK author or careful integrator reads this to write a correct client. Precision matters; voice still has to be human.

**Voice:** clear, deliberately precise. Lowercase beava. Sentence-case headings. **No** RFC-2119 SHOUTING — `Heads up — this can't be undone` instead of `**MUST NOT** be called in production`.

**Length:** ≤300 lines.

**Structure:**
1. **Title + 1-line lede** describing what the document is.
2. **Smallest end-to-end example** — one curl request/response, or one wire frame hex dump, or one error-handling snippet. Code FIRST.
3. **Reference body** — tables, opcodes, route definitions, error tables. Brief 1-2 sentence prose framing each section.
4. **`## See also`** — cross-link to related specs.

**No status banners** (`> **Status:** Authoritative for v0`).
**No Plan/Authored-by footers.**

**Calibration pages:** `/docs/wire-spec/`, `/docs/error-codes/`.

---

## 6. Vision narrative — `/vision/*`

**Goal:** install the project's why-and-bet in the reader. Pure persuasion, no code.

**Voice:** vision-tone, pure prose, lowercase beava, first-person plural.

**Length:** 100–250 lines.

**Structure:**
1. **Title.**
2. **`## The gap we felt`** — observation about the world that motivated beava.
3. **`## The existing stack is heavy`** (or equivalent) — concession + position.
4. **`## The beava bet`** — what we're trying.
5. **Bold soundbite** at the end of the body.

No code blocks. No tables (unless comparing alternatives).

**Calibration page:** `/docs/vision/why-beava/`.

---

## 7. Architecture deep-dive — `/architecture/*` ← **NEW; applies to Wave 5**

**Goal:** the reader who wants to *understand how beava works* (not just use it) leaves with a clear mental model of one design choice.

**Audience:** intermediate-to-advanced. Familiar with the basics; curious about the reasoning.

**Voice:** vision-tone for the framing, plain technical prose for the mechanism. No Rust struct/enum names, no source paths, no test paths, no `crates/` references. Lowercase beava. Sentence-case headings.

**Length:** ≤200 lines per page.

**Structure:**
1. **Title** (e.g., `# Single-threaded apply`).
2. **1-line lede** in `> blockquote` framing the design choice.
3. **`## Why we chose this`** — concession + position. What seemed obvious that we didn't do, and why our choice fits beava's shape better. **One bold soundbite** here.
4. **`## How it works`** — the mechanism, in plain prose. One small diagram (ASCII art or simple bulleted flow) is fine. No source links.
5. **`## What this gives you`** — observable consequences for the user (latency, durability, throughput, restart behavior, debuggability).
6. **`## Limits`** — honest tradeoffs. The thing we can't do because we made this choice.
7. **`## See also`** — links to related architecture pages and the relevant concept page.

**Don't include:** "(per `project_*`)", "Per ADR-NNN", "Phase NN", `crates/...`, Rust enum names, test paths, "CI tripwire enforced by ...".

**Examples that fit:** `single-thread-apply.md`, `mio-data-plane.md`, `wal-snapshot.md`, `memory-budget.md`, `memory-governance.md`, `observability.md`.

---

## 8. DSL spec — `/pipeline-dsl/*` ← **NEW; applies to Wave 5**

**Goal:** the reader can read a pipeline written in beava's DSL and write their own. Includes the rules for how Python source becomes wire JSON (for SDK porters and advanced users).

**Audience:** primary readers are users writing pipelines; secondary readers are SDK authors porting to a new language.

**Voice:** code-led, technical-precise. Lowercase beava. Sentence-case headings.

**Length:**
- `overview.md` ≤200 lines (user-facing).
- `expressions.md` ≤150 lines (user-facing).
- `compilation-rules.md` ≤300 lines (SDK-porter reference).

**Structure for `overview.md` (user-facing):**
1. **Title** (`# Pipelines`).
2. **1-line lede.**
3. **8–15 line example** showing a complete `@bv.event` + `@bv.table` + register + push + get cycle.
4. **`## Events`** — `@bv.event` decorator, types, retention.
5. **`## Tables`** — `@bv.table(key=)`, aggregation outputs, global vs per-entity.
6. **`## Register, push, query`** — short prose; link to SDK refs for per-language detail.
7. **`## What's next`** — link to expressions, compilation-rules.

**Structure for `expressions.md` (user-facing):**
1. **Title.**
2. **1-line lede.**
3. **Code example** showing `bv.col`, operators, `bv.lit`.
4. **`## Columns`** — `bv.col(...)` and inferred-from-event-source forms.
5. **`## Operators`** — comparison, arithmetic, boolean.
6. **`## Literals`** — `bv.lit`, when needed vs inferred.
7. **`## Predicates`** — `where=`.
8. **`## See also`** — links to overview, operator catalog.

**Structure for `compilation-rules.md` (SDK-porter reference):**
1. **Title** (`# How pipelines compile`).
2. **1-line lede** stating the audience: "If you're porting beava to a new language, this is the canonical compilation contract."
3. **`## Source → wire JSON`** — high-level flow.
4. **`## Ambiguity matrix`** — the table that defines what's ALLOWED / FORBIDDEN / UNDEFINED. Keep the table; drop the planning citations in the rationale column.
5. **`## Validation rules`** — what the server rejects at register time.
6. **`## See also`** — wire-spec, expressions, overview.

**Soft callouts:** Same as elsewhere (`> **Heads up:**`, `> **Tip:**`).

---

## Cross-cutting rules (apply to every archetype)

- **Lowercase `beava`** in prose. Capital "Beava" only at sentence-start in narrative copy. Code identifiers (e.g., TypeScript `BeavaApp` class) keep their language-idiomatic case.
- **Sentence-case headings.** Exceptions: `HTTP`, `JSON`, `TCP`, `WAL`, `API`, `SDK`, `mio` (lowercase per the brand).
- **No status banners** at page top.
- **No Plan-level traceability** footers.
- **No `crates/...`, `python/beava/...`, `python/beava/__init__.py`** source paths.
- **No phase numbers, ADR refs, plan IDs, decision IDs (D-NN, Q1 Path B), `project_*` slugs, `V0-MEM-GOV` codes, `PLANNER-SURFACED CONCERN`** anywhere.
- **No `**Args:**` / `**Returns:**` / `**Raises:**` API-generator blocks.** Use prose with inline snippets.
- **Soft callouts only:** `> **Heads up:**` for irreversible / lossy actions; `> **Tip:**` for non-obvious shortcuts; `> **Note:**` sparingly. No `**WARNING**` / `**Destructive.**`.
- **One example per concept**, not three. Pick the simplest.
- **Code-first** wherever possible. The reader should see what the thing looks like before reading prose about it.
