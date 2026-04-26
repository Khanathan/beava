# Nexmark Gap Analysis

**Generated:** 2026-04-26
**Source:** github.com/nexmark/nexmark queries q0-q22 (Flink SQL definitions)
**Beava reference:** STATE.md (55 ops shipped) + PROJECT.md (push/get HTTP+TCP, single-thread apply, in-memory state)

## Executive summary

- **Total queries analyzed:** 23 (q0..q22)
- **HAVE today (Bucket A):** 7 queries (q0, q1, q2, q14, q15, q17, q21*) — *q21 with a string-helper extension
- **GAP — small ops (Bucket B):** 9 queries (q3, q5, q7, q8, q11, q16, q18, q19, q22)
- **GAP — major capability (Bucket C):** 6 queries (q4, q6, q9, q12, q13, q20)
- **IMPOSSIBLE / non-feature (skip):** 1 query (q10 — file-system sink)

The unifying observation: Nexmark queries split cleanly into "per-key feature aggregation" (which Beava does natively and well — q15, q17, q1, q2, q14) and "row-level join + retraction streams" (q4, q6, q9, q20 — which model an *output stream of joined records*, not a per-entity feature). The latter is the strategic gap: Beava's `Event` and `Table` semantics produce *queryable state*, not *output rows*. For a Beava-Nexmark adapter, the bridge will be a periodic `/get-multi` drain over the keyed output table at a fixed cadence — Flink emits rows continuously, Beava emits the latest aggregate on demand.

## Per-Query Gap Table

| Query | Name | SQL primitives used | Beava ops needed | Status | Notes |
|-------|------|---------------------|------------------|--------|-------|
| q0 | Pass-through | `SELECT … FROM bid` | `@bv.event(Bid)` | **HAVE** | No-op identity; tests ingest baseline. Map to ingest-only `/push`. |
| q1 | Currency conversion | scalar arithmetic on bid stream | `@bv.event(Bid).with_columns(price_eur = bv.col('price') * 0.908)` | **HAVE** | Pure stateless transform. Expression DSL covers `*` already. |
| q2 | Selection by id mod | `WHERE MOD(auction, 123) = 0` | `@bv.event(Bid).filter(bv.col('auction') % 123 == 0)` | **HAVE** | Needs `%` (modulo) operator on expression DSL — verify it exists; if not, tiny extension. |
| q3 | Local item suggestion | INNER JOIN auction × person on seller=id, filter category=10, state IN (OR/ID/CA) | event↔event windowed join OR event↔table enrichment | **GAP** | Person should be `@bv.table(key=id)` (upsertable identity); auction is `@bv.event`; need `event↔table enrichment with as_of=` on seller key. Plus `bv.col('state').isin([...])` — `isin` is a missing expression op. |
| q4 | Avg price per category | nested: `MAX(B.price) GROUP BY auction` then `AVG(...) GROUP BY category`; correlated time-bound `B.dateTime BETWEEN A.dateTime AND A.expires` | windowed event↔event join with custom expiration window + 2-stage agg + retraction-aware downstream agg | **GAP (major)** | The "winning bid" stage is per-auction `bv.max(price)` keyed on auction — fine. Stage 2 averages MAXes over auctions in the same category — that's an **agg over a Table**, which means table↔aggregation (re-aggregating a derived table). Beava's `.agg()` runs on `Event`, not `Table`. New capability: `Table.agg()` over the column distribution. Also requires retraction propagation when winning bid changes. |
| q5 | Hot items (sliding) | `HOP(bid, 2s, 10s)` count per auction, then global max per window, join back | sliding/HOP window; global cross-key TOP-1 over windowed counts | **GAP** | (1) HOP not in operator list (only TUMBLE via uniform bucketing). (2) Cross-key max-of-aggregations (find auction with highest count) — that's `top_k(k=1)` over the *output table* keyed by auction, not over events. Both feasible with new ops. |
| q6 | Avg of last-10 winning prices per seller | row-windowed `OVER (PARTITION BY seller ORDER BY dateTime ROWS BETWEEN 10 PRECEDING)` over filtered "winning bids" stream | new op: rolling-N-row average per key | **NEW-OP** | Flink itself flags this as unsupported (`OVER WINDOW operator doesn't support to consume retractions`). Beava could ship a purpose-built `last_n_avg(price, n=10)` op (similar shape to `last_n` + sum/count rolling) — small new op. Wins = "closed auctions" = auction expired + winning bid known → requires PIT join logic. |
| q7 | Highest bid per period | `TUMBLE(bid, 10s)` MAX(price), join back to bid stream where price=maxprice in that window | TUMBLE max + cross-key join from window-aggregate back to events | **GAP** | The MAX-per-window is `bv.max(price)` per tumble bucket = trivial. The "join back to original bids that match maxprice" is a *retraction stream over the Event* — Beava emits state, not stream. Workable as: keep Bid as event, store `(window, maxprice)` as derived table, then on a `/get-multi` query return the bid records whose price=maxprice. Adapter logic, not a new op — but the query semantic ("emit the bid rows themselves") doesn't fit Beava's feature-server model cleanly. |
| q8 | New users w/ auctions | TUMBLE(person, 10s) ⨝ TUMBLE(auction, 10s) on seller=id with same window | windowed event↔event join (already in catalogue) + tumble alignment | **GAP** | Beava has windowed event↔event joins (per CLAUDE.md). Tumble is uniform bucketing. The catch: equal-window-boundary semantics (must be same window in both sides) — currently Beava's join is on a sliding window from the join site, not aligned tumble buckets. Small extension: `align="tumble"` on join. |
| q9 | Winning bid per auction | `ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC, dateTime ASC) WHERE rownum=1` over time-bounded join | per-auction "argmax(price) within (auction.start, auction.expires)" | **GAP (major)** | Beava has `bv.max(price)` (single-value max) but no **argmax** that returns the full row tied to the max. Need `arg_max(field=price, return=[bidder, dateTime, ...])` op. Plus auction.expires is a per-auction PIT bound — needs Phase 15 watermark/event-time PIT. Until then, can approximate with `last(price)` keyed on auction filtered to auction-window via enrichment. |
| q10 | Log to filesystem | partitioned filesystem sink | n/a | **IMPOSSIBLE** | Beava is a feature server, not an ETL sink. Skip in adapter; document rationale. |
| q11 | User sessions | `SESSION(bid, 10s) GROUP BY bidder` → count | session windows | **GAP** | Beava has no session-window operator. New op: `session_count(gap=10s)` + analog `session_sum/avg`. Or generalize: a `WindowedOp::Session` window kind alongside the existing tumble bucketing — moderate effort because session boundaries are *data-driven*, not uniform. |
| q12 | Processing-time tumble | `TUMBLE` on `PROCTIME()` per bidder | processing-time bucketing | **GAP** | Beava's tumble bucketing is event-time. Processing-time tumble needs an injected ingest timestamp at apply time. One-line addition: a `proc_time` virtual column populated by the apply loop, then any existing windowed op works. Small extension. |
| q13 | Bounded side-input join | join bid × CSV-from-disk lookup | n/a — file-side input | **IMPOSSIBLE-ish / GAP** | Beava could load the side input as a static `@bv.table` populated via `/upsert` at startup. Mechanism is trivially expressible (table↔event enrichment) but the file-loading step lives in the *adapter*, not Beava itself. Mark adapter responsibility. |
| q14 | Calculation + UDF | scalar arithmetic, `CASE WHEN HOUR(...)` , `count_char` UDF, range filter | expression DSL + time-bucket + scalar UDF | **HAVE** | Arithmetic + filter: have. `CASE WHEN`: needs `bv.when().then().otherwise()` — verify exists in expression DSL; if not, small expression-DSL extension. `HOUR(...)` is a date-part scalar — small extension. `count_char` is a string UDF — Beava doesn't expose UDFs in expression DSL today. For benchmark purposes, replace with a built-in `bv.col('extra').len()` or skip the UDF column. |
| q15 | Bidding stats per day | `count(*) FILTER (WHERE …)` × 3 buckets, `count(distinct bidder)`, `count(distinct auction)` per day | filtered counts + count_distinct (HLL) per day-bucket | **HAVE** | Day = time bucket via `@bv.event` + `key=DATE_FORMAT(dateTime,'yyyy-MM-dd')` (or `floor_to_day` on `bv.col('dateTime')`). All three rank counts = filtered `bv.count()` with `bv.col('price') < 10000` etc. Distinct counts = `bv.count_distinct(bidder)` (HLL is shipped). Just need `FILTER` in `.agg()` — i.e. `bv.count(filter=bv.col('price') < 10000)`. If filter-on-aggregation isn't yet a pattern, it's a small DSL addition; otherwise have. |
| q16 | Bidding stats per channel-day | same as q15 grouped by `(channel, day)` | same as q15 | **GAP** | Adds composite-key grouping `(channel, day)`. Plus `max(DATE_FORMAT(...))` as latest-minute string in window — that's `bv.last(bv.col('dateTime').format('HH:mm'))` per group — needs a date-format scalar op. Multi-key group_by likely already supported (`group_by([channel, day])`); confirm. Same `FILTER` need as q15. |
| q17 | Auction stats per day | `count(*) FILTER`, min/max/avg/sum per (auction, day) | filtered counts + min/max/avg/sum keyed on (auction, day) | **HAVE** | Identical pattern to q15 plus `min/sum/avg/max` (all shipped). Composite key `(auction, day)`. Need `count(filter=...)` syntax confirmation. |
| q18 | Last bid per (bidder, auction) | `ROW_NUMBER() OVER PARTITION BY (bidder,auction) ORDER BY dateTime DESC, rank<=1` = deduplicate / keep-latest | latest-per-key | **GAP** | This is *the* canonical `@bv.table(key=(bidder, auction))` upsert pattern — store full bid row, last writer wins. Beava already has `@bv.table` with key + MVCC. The "GAP" piece: Beava tables today take the *latest scalar fields*, but to emit the full row needs `bv.last(*)` or "store the entire event as the value". This is a small extension — likely just a "row-as-value" table mode. Or expressible as `bv.last(field) for each field`. |
| q19 | Top-10 bids per auction | `ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) WHERE rank<=10` | per-auction top-N rows by price | **GAP** | Beava has `bv.top_k(k=10, by=price)` (SpaceSaving sketch) but it returns *frequency-sorted* items, not arbitrary-field-sorted. Need `top_n_by(k=10, by=price, return=[bidder, channel, url, dateTime, extra])` — exact (not sketch) heap-based top-N over a column. Different op from SpaceSaving (which is for stream-frequency mode); call it `top_n` to disambiguate. Per-auction key = standard. |
| q20 | Bid + auction enrichment | `INNER JOIN auction WHERE category=10` | event↔table enrichment with `as_of=`, then filter | **GAP** | Closely related to q3 — auction-as-table needs auctions stored upserted by id. Beava has `event↔table enrichment with as_of=`. Filter-after-enrich is trivial. Like q3, this models "emit the joined row" — works as a derived-event stream of bids enriched with auction columns; downstream `/get` returns the latest values per bid. The semantic mismatch ("emit every joined row" vs "emit current state per key") means this can be benchmarked but the contract differs. Mark **GAP** for the row-emission semantic, **HAVE** for the underlying join op. |
| q21 | Channel-id extraction | `lower()`, `CASE WHEN`, `REGEXP_EXTRACT`, `IN` filter | string ops + when-then-else + regex | **HAVE\*** | All scalar; no aggregation. Need: `bv.col('channel').lower()`, `bv.when().then().otherwise()`, `bv.col('url').regex_extract(pattern, group)`, `.isin([...])`. None of these are in the listed catalogue. Plausibly small expression-DSL extensions; mark HAVE-with-asterisk. |
| q22 | URL directory split | `SPLIT_INDEX(url, '/', N)` | string `split_index` | **HAVE\*** | One scalar string op. Add `bv.col('url').split('/').nth(3)` to expression DSL. Tiny. |

---

## Bucket A — Implementable with current ops

These run today on Beava with just a Nexmark→Beava schema adapter. Each needs `@bv.event(Bid)` (and for q3-relatives, `@bv.table(Person)` and `@bv.event(Auction)`) registered against the Nexmark generator's record stream.

- **q0** — Pass-through. `@bv.event(Bid)` and ingest-only; verify ingest baseline EPS.
- **q1** — `@bv.event(Bid).with_columns(price_eur=bv.col('price') * 0.908)`. Stateless transform.
- **q2** — `@bv.event(Bid).filter(bv.col('auction') % 123 == 0)`. Stateless filter (assumes `%` is in the DSL — confirm; otherwise tiny extension).
- **q14** — `@bv.event(Bid).filter(bv.col('price') * 0.908 > 1_000_000 & bv.col('price') * 0.908 < 50_000_000).with_columns(...)`. CASE-WHEN on hour-of-day needs `bv.col('dateTime').hour()` + `bv.when().then().otherwise()`; both small. Drop the `count_char` UDF column or stub it.
- **q15** — `@bv.event(Bid).group_by([day_bucket]).agg(total=bv.count(), rank1=bv.count(filter=bv.col('price')<10000), ..., distinct_bidders=bv.count_distinct(bv.col('bidder')), ...)`. Needs `count(filter=...)` syntax confirmation (Bucket A iff confirmed; else tiny `Bucket B`).
- **q17** — Same shape as q15 with composite key `(auction, day)`; uses `min`/`max`/`avg`/`sum` already shipped.
- **q21\*** / **q22\*** — Stateless scalar transforms. Need string ops (`lower`, `regex_extract`, `split_index`, `isin`) and `when().then()`. All small expression-DSL extensions; if the DSL accepts arbitrary scalar Rust UDFs, even faster to wire up. Marked Bucket A with asterisk because they require these scalar additions, but no stateful op is missing.

**Beava DAG idiom for Bucket A:** stateless transforms are `Event → with_columns/filter/select → /get` (where `/get` is just "tap the output event tail"). Aggregating queries (q15, q17) are `Event → group_by(keys).agg(...) → Table → /get-multi(keys)` to drain results.

---

## Bucket B — Small new ops needed

Each item names the gap-closer and a rough effort estimate (S = days, M = ~1 week, L = ~2-3 weeks).

- **q3** — *event↔table enrichment with* `bv.col('state').isin([...])`. **`isin` expression op** (S). Person registered as `@bv.table(key=id)` is already supported.
- **q5** — *Hot items in sliding window.* Need: (1) **HOP/sliding window** alongside TUMBLE bucketing (M — generalize the bucketing engine), (2) **cross-key top-1** over windowed-aggregation-output (`top_n_by(k=1, by=count)` keyed at the global level — this is "argmax over all keys in a window").
- **q7** — *Max-bid-per-window join back to bids.* Underlying max op is shipped; the "join back" is **adapter logic** (drain `/get-multi` for window's max, scan recent events). Small adapter work, no new op.
- **q8** — *Tumble-aligned event↔event join.* Need **`align="tumble"` option on event↔event join** (S-M) so both sides snap to identical window boundaries.
- **q11** — *Session windows.* New **session-window kind** (`session_count(gap=10s)`, `session_first_event_time`, `session_last_event_time`). Data-driven boundaries vs uniform tumble = a real new code path (M-L). Reuses the bounded-buffer + apply machinery; the per-key state is `(session_start, last_seen, count)`.
- **q12** — *Processing-time tumble.* Add **`proc_time` virtual column** at apply time; existing windowed ops take it as the time field (S).
- **q16** — *Composite group_by + filtered counts + format scalar.* Need `bv.col('dateTime').format('HH:mm')` (S) and confirm composite `group_by([channel, day])` works (likely yes per CLAUDE.md). Same `count(filter=...)` as q15.
- **q18** — *Latest full-row per composite key.* Add **`@bv.table(key=…, mode='row')`** (a "store the entire event row" mode, not field-by-field) (S-M). Or pragmatically: write `bv.last(field)` for each field — works today, less clean.
- **q19** — *Top-N rows per key by arbitrary field.* Add **`top_n_by(k=10, by=price, return=[fields])`** — exact heap-of-N op, distinct from SpaceSaving (M). Heap state is bounded; per-key memory = N × row-size.
- **q22** — *Split-index scalar.* Add `bv.col('url').split('/').nth(N)` (S).

**Estimated total Bucket B effort:** ~6-8 weeks of operator work. Most items are S/M; the heaviest are session windows (q11) and HOP windows (q5).

---

## Bucket C — Major capability gaps

These need architectural moves beyond a new op.

- **q4** — *Avg-of-MAX per category.* Two-stage aggregation where stage-2 aggregates over a **derived table's column distribution**. Beava `.agg()` runs on `Event`, not on `Table`. **Capability:** *table-level aggregation* (i.e., make `Table.agg()` first-class). Plus **retraction propagation** — when stage-1 max changes, stage-2 avg must recompute. Beava docs note "v0 table retraction"; downstream-aware retraction in a chain is a real engineering item. *Impact:* unlocks q4 directly and any "agg of agg" pattern (cohort statistics, leaderboards over leaderboards). Significant — multi-week with new state semantics.

- **q6** — *Rolling N-row average over a stream of "winning bids per auction per seller".* Two pieces: (a) the "winning bid" derivation requires PIT-bound join (auction.start..auction.expires) which is **Phase 15 watermark/event-time PIT**; (b) a **rolling-N op** (`last_n_avg(price, n=10)` per seller). The rolling-N is small (S-M, like `last_n` plus running sum); the PIT join is the major item. Even Flink can't do q6 today (incompatibility between OVER WINDOW and retractions), so Beava has cover.

- **q9** — *Winning bid (full row) per auction within auction time bounds.* Same PIT requirement as q6. Plus needs the **`arg_max`** op (return the row tied to max(price)) — distinct from `bv.max` (returns scalar). M for arg_max once PIT is shipped. Major because PIT is Phase 15.

- **q12** — Reclassified into Bucket B above (proc-time is small).

- **q13** — *Bounded side-input from CSV file.* Adapter responsibility, not a Beava capability gap, but worth flagging: needs a "bulk load → `/upsert` into `@bv.table`" entry point in the adapter. Minor adapter work; in Beava core, the receiving side is shipped.

- **q20** — *Filtered enrichment join emitting full joined rows.* The join itself is shipped (event↔table enrichment with `as_of=`). The **semantic gap** is "Flink emits a row per join match; Beava emits state per key." For benchmark purity, the adapter must drain matched rows via `/get-multi` at a cadence and synthesize a row stream. This is a *measurement-contract* gap, not a feature gap. Major in the sense that *all* row-emitting Nexmark queries (q0, q1, q2, q3, q7, q9, q14, q18, q19, q20, q21, q22) depend on this drain pattern. See "Beyond-ops infrastructure" below.

---

## Top missing operators (ranked by query coverage)

| Op | Queries unlocked | Effort | Notes |
|----|------------------|--------|-------|
| `count(filter=expr)` aggregation modifier | q15, q16, q17 (3) | **S** | Probably the cheapest single change with the highest immediate yield. Also unblocks Beava's own fraud recipes ("count_distinct unique_ip filter where amount>$threshold"). |
| `top_n_by(k, by, return=[...])` exact heap top-N | q19, q5 (and adjacent recipe surface: "top-10 customers by spend", "top-N IPs by failed-login") | **M** | Distinct from SpaceSaving (which is frequency mode). Returns full rows, not just counts. Pairs naturally with `arg_max` (k=1 case). |
| `arg_max(by=expr, return=[fields])` | q9, q7, q6 (winning-bid family) | **M** | Returns the row tied to max. Underpins all "winner" patterns. Combine with `top_n_by` (k=1). |
| Session window kind (`session_count`, `session_sum`, etc.) | q11 (and the entire user-engagement / fraud-session recipe family — login bursts, cart sessions) | **M-L** | Data-driven boundaries are a real new windowing primitive. High strategic value beyond Nexmark for fraud + product analytics. |
| HOP / sliding-window kind | q5 (and "rolling 60-second velocity" beyond uniform tumble) | **M** | Generalize bucketing to a sliding-window iterator. Already partly enabled by uniform bucketing + 64-bucket cap; HOP is just "report every step" instead of "tumble every period". |
| `isin` expression op + scalar string ops (`lower`, `regex_extract`, `split_index`, `format`, `hour`, `when().then().otherwise()`) | q3, q14, q16, q21, q22 (5) | **S each** | These are small enough to bundle as one expression-DSL extension PR. Combined yield is huge. Also cleans up the Beava recipe surface (channel parsing, URL parsing, time-bucket-name formatting). |
| `@bv.table(mode='row')` row-as-value table | q18 (and dedup/keep-latest patterns broadly) | **S-M** | Currently users have to `bv.last(field)` per column. A row-mode table is a clean primitive. |
| Tumble-alignment on event↔event join | q8 | **S-M** | Add an `align='tumble'` option to the existing windowed join. |
| `Table.agg()` (table-level re-aggregation) | q4 (and "average of leaderboards", "sum of distinct counts") | **L** | Architecturally larger — implies the DAG can layer aggregations and propagate retractions through stages. Defer until after the simpler ops above ship. |
| Processing-time virtual column | q12 | **S** | Inject `proc_time` at apply; reuse all existing windowed ops. |

**Top 3-5 unlocking the most coverage:**

1. **`count(filter=expr)`** — unlocks q15, q16, q17 trivially; also a Beava-recipe staple. **S.**
2. **Scalar string + boolean-predicate kit** (`isin`, `lower`, `regex_extract`, `split_index`, `format`, `hour`, `when().then().otherwise()`) — bundles q3, q14, q16, q21, q22. **S each, batchable.**
3. **`top_n_by` + `arg_max`** — q19, q5, q9, q7. **M.** Flagship "winner" ops.
4. **Session windows** — q11 plus large strategic value for fraud/engagement recipes. **M-L.**
5. **HOP/sliding window** — q5 plus general "rolling" velocity recipes. **M.**

Together, these unlock 14 of 23 queries (Bucket A's 7 + 7 of Bucket B's 9), with the remaining 6 in Bucket C deferred until Phase 15 PIT and table-level aggregation land.

---

## Non-Beava queries (skip in adapter)

These test things Beava intentionally doesn't do. Flag in adapter as `SKIP` with rationale.

- **q10 — Log to file system (partitioned CSV sink).** Beava is a feature server. Output is `/get`/`/get-multi` over HTTP and TCP, not a Hadoop-style CSV partitioner. Flink's q10 measures sink throughput; Beava's analog is `/push` ingest throughput, already covered by q0. **Skip.**
- **q13 — Bounded side-input from CSV file.** The *file-loading* part is adapter responsibility (run a one-shot loader against `@bv.table.upsert`). The *join* part runs on Beava's existing event↔table enrichment. **Run the join half; skip the CSV-discovery half.**
- **Full SQL surface in q14, q21, q22** — the UDF (`count_char`) and arbitrary `REGEXP_EXTRACT`/`SPLIT_INDEX` semantics are SQL-completeness goals, not feature-server goals. Implement the common slice (`lower`, `split_index`, simple regex_extract) and skip arbitrary user-defined UDFs in DSL. Flag where adapter approximates.
- **q12's `PROCTIME()` semantics** — Beava is event-time-first; processing-time only as a virtual column. Run with the virtual column; document that semantics may differ when ingest is bursty.

---

## Beyond-ops infrastructure for Nexmark adapter

Three pieces of infra outside Beava's operator catalogue need building before Nexmark can run:

### 1. Data generator integration

Nexmark's reference generator (Beam-based) produces deterministic Person/Auction/Bid streams with Zipfian distributions over keys (auction popularity, seller frequency). Two paths:

- **(Preferred)** Port the deterministic generator as a Rust crate (`crates/nexmark-gen/`). Inputs: `events_per_second`, `total_events`, `seed`, ratio knobs (Beam defaults: 92% bid, 6% auction, 2% person). Output: a `Stream<NexmarkRecord>` that an adapter shim translates into Beava `/push` payloads (HTTP or framed TCP).
- **(Fallback)** Pre-generate a flat file via the upstream Java generator, drop into S3/local disk, and replay through `crates/beava-bench` as a record source. Slower iteration but bit-exact to Flink.

Recommend the Rust port — small (~1KLOC), avoids JVM dep in the bench harness, and gives us deterministic seed control for cross-engine regression comparisons.

### 2. Throughput measurement mapping

- **Flink Nexmark metric:** `cores × wall-clock-time` per query, comparing engines at fixed event rate.
- **Beava metric:** EPS sustained on a single core (per Beava's "≥3M events/sec/core" target).

Map: report Beava as `<EPS> events/sec/core, single-thread`. For a like-for-like comparison, normalize Flink's "cores × time" to Flink's effective EPS-per-core and compare. Also report **P99 batch-get latency** alongside EPS — Flink doesn't measure this because it has no comparable read API; report it as the Beava-specific dimension (Beava is push *and* serve, Flink is push-and-emit). This goes in `.planning/throughput-baselines.md` per the Phase 7.5 contract. Add a `pipeline-shape = nexmark-q{N}` row per query.

### 3. Result correctness pattern

Flink emits result rows; Beava emits feature reads. Bridge:

- For each Bucket A/B query, the adapter declares a **drain key set** and a **drain cadence** (e.g., every 1s, drain `/get-multi(keys=[…all auctions…])` for q5). Drained rows are the Beava equivalent of Flink output rows.
- For "row-emitting" Nexmark queries (q0, q1, q2, q14, q21, q22 — pure stateless transforms), Beava can emit transformed events to a tap/output `Event`, drained by a *tail* consumer. Spec: a `/tail?event=<name>` HTTP+TCP endpoint that streams emitted rows. This already exists if Beava events have a tail consumer; if not, it's a small adapter-side WebSocket-from-WAL bridge.
- **Correctness check:** run both Beava and Flink against the same deterministic generator seed, hash the output row sequences (sort-then-hash for keyed-aggregation queries; raw-then-hash for streaming-tail queries), assert equality. For row-emitting queries, exact byte equality is the bar; for aggregation queries with sketches (`count_distinct`, `percentile`), allow ±epsilon tolerance per Beava's existing sketch error bounds.
- This drains-vs-emits mismatch is the **single most important benchmark-design decision** to lock down before phase planning. Flag explicitly in the adapter README.

---

## Recommended phasing

Three-tier plan slotting into the existing roadmap. Numbering provisional; calibrate against current Phase 18-19-20 sequence in STATE.md.

### Tier 1 — Phase ~22 (or first post-Phase-20 phase): "Nexmark MVP slice — Bucket A"

Goal: Run q0, q1, q2, q14, q15, q17, q21, q22 on Beava with the Nexmark generator in `crates/beava-bench`. Land cross-engine throughput numbers vs Flink for these 8 queries.

Tasks:
- `crates/nexmark-gen/` Rust port of Nexmark generator (deterministic seed).
- `crates/beava-bench/` `--nexmark` mode that wires generator → Beava `/push` (HTTP + TCP) and drains `/get-multi`.
- Expression DSL extensions (one PR, batched): `isin`, `lower`, `regex_extract`, `split_index`, `format`, `hour`, `when().then().otherwise()`, `%` (modulo) — all S each.
- Aggregation modifier: `count(filter=expr)` and `count_distinct(filter=expr)` — S.
- Result-drain pattern + correctness harness (hash-and-compare against Flink reference outputs).
- Append per-query rows to `.planning/throughput-baselines.md`.
- Criterion microbench: `nexmark_q15` end-to-end + filtered-count. (Per `.planning/perf-baselines.md` discipline.)

Output: 8 queries green vs Flink, baselined. README chapter in `beava-website/project/guide/recipes/nexmark/`.

### Tier 2 — Phase ~23: "Nexmark winner-ops + windowing — Bucket B"

Goal: Add `top_n_by`, `arg_max`, `@bv.table(mode='row')`, HOP, session, processing-time. Cover q3, q5, q7, q8, q11, q16, q18, q19. (Adapter already exists from Tier 1; just adds operator support and registers more queries.)

Tasks (each task = red→green per TDD discipline):
- `top_n_by(k, by, return=[...])` exact heap op + per-key memory bound + property tests. — M.
- `arg_max(by, return=[...])` op (or k=1 specialization of `top_n_by`). — M.
- `@bv.table(mode='row')` storage mode — store full event payload as table value. — S-M.
- Session window kind: `session_count`, `session_sum`, `session_first_event_time`, `session_last_event_time` with `gap=` parameter. — M-L (data-driven boundaries are nontrivial state machine).
- HOP/sliding-window iterator on top of bucketing engine. — M.
- `proc_time` virtual column on apply. — S.
- Tumble-alignment option on event↔event join: `align='tumble'`. — S-M.
- Per-query benches added; row added per query to `throughput-baselines.md`.
- Smoke: q3, q5, q7, q8, q11, q16, q18, q19 green vs Flink.

### Tier 3 — Phase ~24+ (after Phase 15 PIT): "Retraction-aware joins + table-level agg — Bucket C"

Goal: Cover q4, q6, q9, q20 — the queries blocked on event-time PIT (Phase 15) and on table-level re-aggregation.

Tasks:
- Depends on **Phase 15 watermark + event-time PIT join** (auction.dateTime..auction.expires bounds).
- `Table.agg()` table-level re-aggregation primitive with retraction propagation. — L.
- `last_n_avg(field, n=10)` rolling op (small once PIT lands). — S-M.
- Row-emission contract for q20-style "emit every joined row" queries — likely a `/tail?event=` streaming endpoint formalized.
- Per-query benches; final 4 queries green vs Flink.
- Document the q10/q13 skip rationale in the bench README.

### Stretch — "Nexmark Plus": Beava-native query family

Once 22/23 are green, add Beava-shaped sister queries that *only* Beava can run cleanly: per-entity P99 latency reads under load, batch-get fanout, fraud-shape feature packs (the kind documented in `crates/beava-bench/`'s small/medium/large pipelines). Position this as the "Beava native" benchmark complement to Nexmark — Flink won't have an equivalent, which is the point.

---

## Open questions for the human (not blockers)

1. Is `count(filter=expr)` already supported in Beava's `.agg()` syntax, or is it a new addition? (Determines whether q15/q17 are Bucket A pure or Bucket A-with-tiny-extension.)
2. Does `bv.col(...) % N` (modulo) exist in the expression DSL today? (Determines q2 status.)
3. Is composite-key `group_by([col1, col2])` a shipped pattern? (q16, q17 depend on it.)
4. Is there an established "tail" / output-stream consumption pattern for `Event`s that would let a benchmark drain row-emission queries (q0, q1, q14, q20, q21, q22) cleanly? Or do we need to spec a `/tail?event=` endpoint as part of Tier 1?
5. Phase 15 (watermark/event-time PIT) timing — is Tier 3 unblocked by mid-2026? Affects tier sequencing.

These are flagged so the executor agent for Tier 1 can resolve before plan-checker review.
