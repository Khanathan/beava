# YC Application — Beava (Summer 2026)

> Ruthlessly cut. Each answer fits in a YC text box without scrolling. Numbers survive; explanation dies.

---

## Company name
Beava

---

## 50-char description
Real-time features in a single Apache 2.0 binary.

---

## Company URL
https://beava.dev

---

## Where do you live / where based after YC
Toronto, Canada / San Francisco, USA

---

## Explain location
Toronto today; SF for the batch. Most target design partners (YC fintech, ad-tech) are SF-anchored.

---

## What is your company going to make?

Beava is a single-binary real-time feature server. Apache 2.0. `pip install beava`, declare aggregations in Python (Polars-shape), push events over HTTP, query features by key in microseconds.

Replaces what teams currently glue together: Redis with 200 lines of Lua for windowed counters, Kafka + Flink + a feature-store SaaS, or Tecton / Chalk for evaluators who can't stomach $10k+/mo.

47 operators across 7 families — count, sum, quantile, top_k, time_since, EWMA, rate_of_change. Sub-ms reads. ~3M EPS/core peak. ~7 KB/entity. WAL + snapshot. No external storage.

v0 is single-node by design. Event-time, joins, and tiered storage are public RFCs.

Mental model: Redis with stateful operators. Redis won as one binary with clean primitives. Beava is the same shape — but ships the operators apps keep re-implementing in Lua.

The bet: the bottom 90% — Series-A fintech, ad-tech, agentic apps — wants the simple thing more than the complete thing. Same OSS-then-cloud playbook as DuckDB / Polars / ClickHouse.

---

## How far along?

v0 ships in 2-4 weeks.

- Single Rust binary, 14 MB
- 47 operators, Python SDK with `@bv.event` / `@bv.table` decorators, TS / Go push-and-query SDKs
- Wire spec stable: framed TCP + HTTP/JSON
- Hand-rolled WAL + snapshot, recovery tested under crash injection
- ~3M EPS/core peak; P99 batch-get < 10ms (Linux Xeon, single instance)
- Docs live at beava.dev — Hetzner box dogfoods Beava for live metrics
- Architecture committed via public ADRs / RFCs

No public users yet. Repo opens on Show HN. ~5 design partners targeted first.

---

## How long full-time

1 month on Beava. Years of feature-store work prior — Faire, Fennel, Viggle.

---

## Tech stack

Rust core. mio data plane, no async in hot path. Tokio admin sidecar. HTTP/1.1 + JSON for reach; custom framed TCP for fast-path. Hand-rolled WAL + snapshot — no RocksDB, no fjall. Python SDK (sync + fire-and-forget); TS / Go SDKs.

Claude Code (Opus 4.x) drives ~80% of plan / execute / review. I hand-iterate the load-bearing parts: WAL, mio loop, operator semantics, wire spec.

---

## Are people using your product?
No. Pre-launch.

---

## When will you have a version people can use?
2-4 weeks.

---

## Revenue?
No. Apache 2.0, design-partner phase.

---

## Why this idea / domain expertise / how do you know

Three companies of context:

- **Faire** — built feature store + model store.
- **Fennel** — built the real-time feature store from inside (acquired by Databricks).
- **Viggle** — tried to deploy real-time personalization (rank pages by recent click counts). Picked Quix Streams, the lightest option. Still wasn't light enough. Kafka + glue scripts broke constantly. The aggregation should have been one Python file. There was no light path.

That's why Beava exists. The 47 operators every team hand-rolls belong in a binary, not a script.

Public validation: Yash Batra's "Redis Lua bottleneck" post hit 1M views in 2025; iFood, MPL, Wix all rebuilt feature stores in 2025 on the heavy path because no light option existed; Best Egg's ML team is bridging real-time and batch with Hamilton + Narwhals — the exact unification a single binary collapses.

---

## Competitors / what you understand they don't

- Tecton — Series B, just acquired by Databricks, sunsetting.
- Chalk — Series A, $10M, closed source.
- Fennel — Series A, also acquired by Databricks.
- Feast — OSS but offline-first; BYO online store, schedulers, transforms.
- Hopsworks — heavy Spark + Hudi.
- DIY Redis + Lua + cron — the actual incumbent.

What I understand: the market segments by org size. Enterprise vendors target Series-C+ with data-infra teams. The bottom 90% — Series-A fintech, ad-tech, agentic apps — has none of those teams and rolls their own. Nobody serves them because SaaS economics look bad; OSS economics look great (DuckDB / Polars / ClickHouse playbook).

From inside Fennel: even the team building the heavy version struggled to maintain it. If the platform's authors can't run it cleanly, the customer can't either.

---

## How will you make money?

v0–v0.x: Apache 2.0 only. Mindshare and design-partner depth.

v1+: Beava Cloud — managed, same binary. Enterprise tier (HA, cross-region, SOC2 / PCI / HIPAA) is additional surface, never a feature gate.

Real-time feature platforms ~$2-3B in 2026, accelerating. 5% of the bottom-90% segment = $50-100M ARR opportunity. Year-2 commercial target: $1-3M ARR from 30-50 customers at $20k-100k ACV.

---

## Category
Developer tools / Data infrastructure

---

## Other ideas considered
None. Beava is the focus.

---

## Legal entity formed?
No.

---

## Equity
Hoang Phan, CEO, 100% pre-formation. 10-15% reserved for cofounder / early hires.

---

## Investment taken?
No.

---

## Currently fundraising?
Yes.

---

## Cofounder status

Looking. Solo by design — v0's architectural decisions needed one decisive owner. v0 is locked now. One contributor onboarding.

Looking for a deep engineer who's lived this pain — ex-Tecton / Chalk / Fennel / Feast, or hand-rolled Redis-Lua at scale — to own the cloud and infra surface. I stay outward-facing on OSS, design partners, narrative. Welcome YC introductions.

---

## Who writes code
I write code. One contributor (just starting) on docs and small features. All architectural and core engine work is mine.

---

## What convinced you to apply

Three things converged in 2025-2026, and the timing window is short.

OSS-infra wave hit critical mass. DuckDB, Polars, Bun, ClickHouse Cloud all proved the Apache-2.0-then-cloud playbook beats heavy incumbents.

AI / agentic apps push real-time decisioning from optional to required. Lean teams cannot afford heavy infra maintenance. A single binary is the only shape that fits.

Competitor landscape just calcified. Tecton acquired by Databricks, sunsetting. Fennel acquired by Databricks before that. Chalk priced itself out of seed / A. Feast plateaued. The bottom 90% was abandoned in 2024-2025 consolidation. No OSS engine has a serious vision for this space right now.

YC's network is the fastest path to the design-partner cohort I'm targeting (W25 / S25 fintech). I worked at a YC company (Faire) so I know how the network compounds.

---

## How did you hear about YC?
Worked at a YC startup (Faire) before founding Beava.

---

## Batch preference
Summer 2026

---

## Founder video script
See `2026-05-05-yc-founder-video-script.md` (separate file).
