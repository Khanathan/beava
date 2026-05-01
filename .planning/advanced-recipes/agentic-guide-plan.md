# Agentic guide — plan (parked until realtime guide ships)

Date: 2026-04-24
Status: PARKED. Do not start implementation until the `/guide/` realtime guide has all 6 chapters reworked and shipped.

## IA

Two parallel guidebooks sharing the same design system, voice, chapter template.

```
/guide/                          REAL-TIME FEATURES guide (current)
  chapter-1/  Dashboard
  chapter-2/  Fraud detection          (current: /guide/recipes/fraud/)
  chapter-3/  Personalization          (current: /guide/recipes/personalization/)
  chapter-4/  Anomaly detection        (NEW — replaces leaderboard)
  chapter-5/  Rate limiting            (current: /guide/recipes/rate-limiting/)
  chapter-6/  Usage metering           (current: /guide/recipes/usage-metering/)

/agents/                         AGENTIC patterns guide (new)
  chapter-1/  Why agents need real-time memory
  chapter-2/  Customer-support agent with live context
  chapter-3/  Sales / RevOps agent with intent signals
  chapter-4/  Incident-triage / alert-routing agent
```

Top nav: `Logo | Guide | Agents | Docs | Community | GitHub★`

## Messaging split

| | `/guide/` | `/agents/` |
|---|---|---|
| Frame | engineering | agentic |
| Audience | teams building feature pipelines | teams wiring beava into an LLM decision loop |
| Headline shape | "How to build [feature] in real time" | "Give your [agent] real-time memory / context" |
| Narrative arc | event → feature table → query → UI | event → feature update → agent reads → decides → emits → loop |
| Visual motif | split-view "batch vs real-time" | closed-loop diagram + agent-decision trace |

## Chapter specs (outline only — fill in content during implementation)

### Chapter 1: Why agents need real-time memory

- Foundational; pairs with `/guide/chapter-1/` role.
- Persona: "You're building an agent that does X. Here's what goes wrong when its memory is a day old."
- The closed-loop diagram: event → beava update → agent reads → decides → action event → loop.
- Pandas-mapping equivalent: "stateful chat turn with SQL query each turn" → "beava + agent reading declared features directly."
- Toy pipeline: 3-line beava pipeline + 10-line agent wrapper (agent reads features, emits a log line, re-reads). Interactive demo: feed events, watch agent's context window update on every tick.
- Evidence block: foundational, so no specific experiment. Cite Anthropic's "Building effective agents" (2024); LangChain's memory patterns; Reflexion paper (Shinn et al. 2024) on self-reflective agents needing memory.
- Capability map: the beava ops an agent-memory layer tends to use — `Latest`, `LastN`, `Count`, `TimeSince`.

### Chapter 2: Customer-support agent with live context

- Persona: Meera, support lead at mid-sized SaaS. Using Intercom Fin or Zendesk AI.
- Stakes: at 14:02 UTC an outage starts affecting EU customers. Agent gets a ticket at 14:07 from a user hit by the outage. Without real-time context, it apologizes for yesterday's batch-cached billing issue. With real-time context, it sees the incident, states scope, ETA, workarounds.
- Evidence block:
  - Published numbers: Intercom Fin resolution rates (public). Zendesk AI Advanced Answers. Decagon case studies.
  - Optional offline experiment: Customer Support Twitter dataset (Kaggle, ~3M tweets) with ticket timestamps — can measure a retrieval-recall-at-N analog for "context matched to active incident." Freshness tiers: 1min / 10min / 1h / 1d.
- Pipeline:
  - `@bv.stream` SupportEvent (ticket, resolution, incident status change, feature deploy)
  - `@bv.table(key="user_id")` UserContext: `open_ticket_count`, `last_ticket_ts`, `product_usage_24h`, `churn_risk_score`
  - `@bv.table(key="incident_id")` IncidentContext: `start_ts`, `current_status`, `affected_users_count`
  - `@bv.table(key="__global__")` DeployContext: `last_deploy_ts`, `last_deploy_services`
- Interactive demo: split-view — agent handling the same ticket with batch-cached context (misses active incident) vs real-time context (knows about 14:02 incident). Trace shows features pulled + agent's final response.
- Capability map: `Latest`, `LastN`, `Count`, `HasSeen`, `TimeSince`, `FirstSeenInWindow`.
- Next evolutions: multi-turn with session memory; chain to sales-agent escalation; add churn-prediction feature.

### Chapter 3: Sales / RevOps agent with intent signals

- Persona: Emmanuel, RevOps at B2B SaaS, 30-person sales team. Using Outreach or Clay.
- Stakes: hot prospect visits `/pricing` at 10:14 AM, team-member from same account views `/docs/api` at 10:17, agent drafts outreach at 11:30. Without real-time intent signals, the email opens with yesterday's demo follow-up. With real-time: "I saw your team was exploring our pricing and API docs this morning — happy to answer questions."
- Evidence block:
  - Published: Outreach, Clay, Apollo lift numbers (blog posts; less rigorous than academic but citable).
  - Offline experiment option: public web-analytics dataset (e.g., GCP sample Google Analytics export, or a marketing dataset) measuring session-intent correlation with conversion.
- Pipeline:
  - `@bv.stream` WebEvent (pageview, form-submit, doc-open)
  - `@bv.table(key="account_id")` AccountIntent: `pricing_views_24h`, `docs_views_1h`, `team_member_activity_count`, `last_activity_ts`, `pages_viewed_session`
- Interactive demo: agent drafts outreach; reader toggles "real-time intent on/off" to see the generated text change. Real-time version references the morning's activity; stale version references yesterday.
- Capability map: `Count (windowed)`, `LastN`, `Latest`, `CountDistinct`.
- Next evolutions: agent proposes deal-stage transition; multi-agent (SDR + AE) coordination; churn-risk flag.

### Chapter 4: Incident-triage / alert-routing agent

- Persona: Priyanka, staff SRE at a B2C marketplace. PagerDuty + runbook fatigue.
- Stakes: 47 alerts land at 03:00 UTC from a partial cloud outage. Current on-call paged for all 47, triaged manually over 2 hours. With real-time classification + dedup agent, 47 alerts collapse to 3 root-cause groupings, routed correctly.
- Evidence block:
  - Published: PagerDuty's alert-fatigue reports; Rootly / Incident.io case studies.
  - Optional offline experiment: Kaggle incident datasets (service logs from ML4Ops papers); measure dedup ratio at different freshness tiers.
- Pipeline:
  - `@bv.stream` Alert (alert_id, service, severity, timestamp, metadata)
  - `@bv.table(key="service_id")` ServiceHealth: `alert_count_5m`, `alert_count_1h`, `severity_ewma`, `last_deploy_ts`
  - `@bv.table(key="alert_pattern_hash")` AlertCluster: `count`, `first_seen`, `members[LastN]`
- Interactive demo: stream of 30-40 simulated alerts; agent groups + routes them in real-time; split-view with batch agent that sees yesterday's cluster definitions and fails to group the novel 03:00 patterns.
- Capability map: `Count (windowed)`, `Ewma`, `EwZScore`, `LastN`, `TimeSince`, `CountDistinct` (for clustering via hashing).
- Next evolutions: auto-remediation for well-known patterns; post-mortem synthesis; learning loop (feedback from on-call → feature weights).

## Shared patterns across the agentic guide

Every agentic chapter has:
1. The closed-loop diagram (unique to this guide).
2. A "decision trace" widget showing the agent reading features, reasoning, emitting output. Matches Chapter 2 (realtime)'s split-view fraud decision pattern but for LLM agents.
3. Notes on context-window economics: "at 10K agents/min each reading 5 features, you're at 50K beava queries/sec; your RAG pipeline can't."
4. Pointer back to the realtime guide's corresponding chapter (Chapter 2 of realtime ↔ Chapter 4 of agentic for the alert-triage → anomaly-detection connection).

## Execution phases (when ready)

- **Phase 1 — Finish realtime guide.** Lock chapters 1-6; ship the Anomaly rewrite; polish; merge the conflicting phase-11.5 Cargo.toml; deploy.
- **Phase 2 — Agentic IA.** Build `/agents/` landing page. Add Nav link. Wire progress tracking (`agents:N` key in localStorage, separate from `chapter:N`).
- **Phase 3 — Agentic Chapter 1** (foundational, written in my main session with max effort) — foundation sets tone for subsequent chapters.
- **Phase 4 — Agentic chapters 2-4 in parallel** — launched as agents with full chapter-specs from this doc + Chapter 1 as reference.
- **Phase 5 — Cross-linking** — bidirectional links between realtime and agentic chapters that map to each other.

## Parked for later discussion

- Do we need offline experiments for the agentic chapters, or are published numbers + architecture arguments enough? (My current lean: one experiment for Chapter 2 CX; the rest rely on published numbers + the closed-loop argument.)
- Should agentic guide have its own progress tracking or share with realtime guide?
- Does `/agents/` need its own nav-second-level (sub-nav within), or does a single landing page with 4 chapter cards suffice? (Current lean: landing + cards.)

End of plan.
