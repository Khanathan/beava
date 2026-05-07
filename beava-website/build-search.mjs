// build-search.mjs — generates beava-website/project/_pagefind/ from the PAGES list below.
// Run: `npm run build:search` (or `node build-search.mjs`) from beava-website/.
//
// Why custom records instead of HTML crawl: pages render via React-via-Babel client-side,
// so the source HTML files have no extractable content. When/if docs migrate to static HTML,
// switch to `index.addDirectory({ path: 'project' })`.

import { createIndex, close } from 'pagefind';

const PAGES = [
  // — Getting started —
  {
    url: '/docs/',
    title: 'Introduction',
    section: 'Getting started',
    content:
      'beava turns events into fresh, queryable state. Push events over TCP or HTTP, declare features in a Polars-shape Python SDK, query the latest values by key with get and mget. ' +
      'Single binary. Sub-millisecond reads. No Java, no Kafka, no coordinator. ' +
      'Fresh state without a streaming platform. Stream processing should not require a platform team. Real-time features should feel like application code. ' +
      'How it works: events flow into streams, pipelines aggregate them into tables, your app queries tables by key. ' +
      'What you can build: personalization, recommendations, real-time bidding, fraud detection, agentic state, product analytics, ML feature iteration. ' +
      'Quickstart, recipes, RFCs, weekly dev call.',
  },
  {
    url: '/docs/get-started/quickstart/',
    title: 'Quickstart',
    section: 'Getting started',
    content:
      'Build your first real-time feature pipeline. Install beava with pip install beava. Start a server. ' +
      'Define a stream with @bv.stream. Define a table with @bv.table key. Register the pipeline with bv.App. ' +
      'Push events with curl POST. Query features with curl GET. ' +
      'Define events. Maintain tables. Query fresh state. ' +
      'Start small. Understand the system. Grow with the use case.',
  },
  {
    url: '/docs/get-started/define-a-pipeline/',
    title: 'Define a pipeline',
    section: 'Getting started',
    content:
      'Streams describe events. Tables describe state. ' +
      'Use @bv.stream for event types like BidEvent. Use @bv.table for state like UserCampaignFeatures. ' +
      'Composite keys: user_id and campaign_id. ' +
      'Think in features, not jobs. What fresh state does your application need by key? ' +
      'Feature owners should own feature logic. ' +
      'A good beava table has a clear key, clear input stream, small set of features, windowed aggregations, tests.',
  },
  {
    url: '/docs/get-started/push-events/',
    title: 'Push events',
    section: 'Getting started',
    content:
      'Send events to beava over HTTP or TCP. An event is a fact: user viewed an item, campaign won an auction, ' +
      'customer started checkout, model made a prediction, agent called a tool, workflow changed state. ' +
      'curl POST /push/UserEvent with JSON body. TCP push for higher throughput via bv.Client. ' +
      'When freshness is fast enough, the architecture gets simpler. ' +
      'beava is not trying to hide complexity behind magic. Durability settings should be explicit.',
  },
  {
    url: '/docs/get-started/query-features/',
    title: 'Query features',
    section: 'Getting started',
    content:
      'beava serves the latest feature values through simple key-based queries. ' +
      'Use get for one key. Use mget for many keys. ' +
      'GET /features/UserActivityFeatures/u123 returns JSON with views_1h, clicks_1h, purchases_24h, distinct_categories_24h. ' +
      'POST /features/.../mget with keys array. ' +
      'Composite keys for relationships like user x campaign, user x item, publisher x ad slot. ' +
      'Fresh enough for the decision path.',
  },

  // — Concepts —
  {
    url: '/docs/concepts/streams/',
    title: 'Streams',
    section: 'Concepts',
    content:
      'A stream is an event type. @bv.stream describes the shape of records beava accepts from your application. ' +
      'Streams are not just transport topics. They are part of the application model. ' +
      'Tables depend on streams. One stream can feed many tables: user activity, item popularity, category affinity, session, fraud velocity. ' +
      'Good streams are close to the application event, easy to test, stable, explicit about timestamps and entity identifiers. ' +
      'Events are product facts. beava turns them into queryable state.',
  },
  {
    url: '/docs/concepts/tables/',
    title: 'Tables',
    section: 'Concepts',
    content:
      'A table is the state beava maintains from one or more streams. @bv.table key with group_by and agg. ' +
      'Tables are application state, not only analytics outputs. User behavior over the last hour. Campaign spend today. ' +
      'Publisher quality, fraud velocity, agent tool usage, product demand. ' +
      'Product analytics can become product logic. ' +
      'Every table has a key: user_id, or composite (user_id, campaign_id). ' +
      'Real-time features should feel like application code.',
  },
  {
    url: '/docs/concepts/windows/',
    title: 'Windows',
    section: 'Concepts',
    content:
      'Windowed aggregations let beava maintain features over recent time ranges. ' +
      'Count clicks in the last 10 minutes. Sum spend in the last 1 hour. Track distinct users in the last 5 minutes. ' +
      'bv.sum, bv.count with window=1h, 24h, etc. ' +
      'Fresh state is usually windowed state. Recommendation models care about recent clicks. Fraud models care about velocity. ' +
      'beava makes common windowed aggregation straightforward. ' +
      'Windowed features should be testable: push events, advance time, assert aggregates.',
  },
  {
    url: '/docs/concepts/get-and-mget/',
    title: 'get and mget',
    section: 'Concepts',
    content:
      'beava serves features through simple key-based queries. get returns features for one key. mget returns features for many keys. ' +
      'Familiar to Redis users, supporting feature tables generated from event streams. ' +
      'Use get for one entity: rank a feed, fetch campaign pacing, fetch account risk, fetch agent workflow state. ' +
      'Use mget when a decision needs many lookups: rank 100 candidates, score multiple ads, check several entities, fetch features for several tools. ' +
      'Push events. Maintain tables. Query by key.',
  },
  {
    url: '/docs/concepts/freshness/',
    title: 'Freshness',
    section: 'Concepts',
    content:
      'Freshness is the time between an event being written and its effect becoming visible in queryable state. ' +
      'Low-latency reads are useful; low-latency freshness changes the architecture. ' +
      'Read latency vs write-to-read freshness. Both matter. ' +
      'A click that updates a recommendation after the user leaves is less useful. A fraud signal that updates after the transaction is approved is less useful. ' +
      'When freshness is fast enough, the event bus becomes optional for many application workloads. ' +
      'Fresh enough for the decision path.',
  },

  // — Vision —
  {
    url: '/docs/vision/why-beava/',
    title: 'Why beava',
    section: 'Vision',
    content:
      'The vision for beava. The gap we felt: powerful infrastructure, inaccessible developer experience. ' +
      'The path from first clone to production application-serving workflow can feel unclear or platform-heavy. ' +
      'The existing stack — event bus, stream processor, serving database, feature store, glue code — is powerful but heavy. ' +
      'beava starts from a different question: what if fresh, windowed, queryable state could start as application code? ' +
      'Stream processing should not require a platform team. Real-time features should feel like application code.',
  },
  {
    url: '/docs/vision/open-source/',
    title: 'Open source commitment',
    section: 'Vision',
    content:
      'The open-source project should be the real system. Transparent, inspectable, understandable. ' +
      'Open source and managed service are not opposing ideas. A managed service should remove operational burden. ' +
      'TiDB-style commitment: source code and production-grade features are part of the public identity. ' +
      'Open source should not be a limited demo of the real system. ' +
      'Transparent infrastructure builds trust. Public RFCs, weekly dev calls, summaries, examples, reproducible benchmarks.',
  },
  {
    url: '/docs/vision/non-goals/',
    title: 'Non-goals and tradeoffs',
    section: 'Vision',
    content:
      'What beava is deliberately not. beava is not trying to replace every streaming system. ' +
      'beava is not hiding complexity behind magic. beava is not only a feature store. ' +
      'beava is not forcing distributed coordination into the first user experience. ' +
      'Single-node first: 600k EPS smaller workload, 100k EPS heavier fraud-style with sketches. ' +
      'Start with one box, push it far, scale when needed. ' +
      'Simple should not mean unserious.',
  },
  {
    url: '/docs/vision/benchmarks/',
    title: 'Benchmarks',
    section: 'Vision',
    content:
      'Benchmarks should be public, reproducible, workload-specific. ' +
      'Current numbers: ~600k events per second on smaller workload, ~100k events per second on heavier fraud-style workload with many sketches. ' +
      'What we report: hardware, cloud instance, CPU, memory, storage, workload shape, event schema, streams, tables, keys, ' +
      'aggregation types, window sizes, sketches, durability, throughput, latency, freshness, p50 p95 p99. ' +
      'Freshness is the benchmark that matters most.',
  },

  // — Community —
  {
    url: '/docs/community/rfcs/',
    title: 'About RFCs',
    section: 'RFCs',
    content:
      'beava uses public RFCs to discuss important design decisions. Shape beava in the open. ' +
      'Active RFCs: tiered storage, stream-to-table lookup, table upsert and delete, event log retention and out-of-order events, event-log query and replay, online pipeline migration. ' +
      'Use an RFC for changes affecting programming model, stream or table semantics, window behavior, aggregation behavior, ' +
      'storage design, durability, replay, backfill, deployment modes, query APIs, compatibility, operational behavior. ' +
      'RFC format: Summary, Motivation, Design, Examples, Tradeoffs, Alternatives, Open Questions. ' +
      'Transparent design builds trust.',
  },
  {
    url: '/docs/community/rfcs/rfc-001-tiered-storage/',
    title: 'RFC-001 — Tiered storage',
    section: 'RFCs',
    content:
      'Spill cold per-key state from RAM to local SSD and S3 so memory budget stops being the cap on total entity count. ' +
      'Hot path stays sub-millisecond; cold reads accept a degraded latency contract. ' +
      'Today beava holds all entity state in RAM. 7KB per entity, 1TB box caps at ~100M entities. ' +
      'Long-tail keys waste memory on rarely-touched state. Working set in RAM, warm set on NVMe, cold set in S3. ' +
      'Open questions: tier policy, cold-read latency contract, WAL interaction, S3 cost model, ops UX. ' +
      'Relationship to today design: v0 ships in-memory only and tiered storage changes that.',
  },
  {
    url: '/docs/community/rfcs/rfc-002-table-ingestion/',
    title: 'RFC-002 — Table ingestion',
    section: 'RFCs',
    content:
      'Direct table ingestion via app.upsert and app.delete. Native home for static and slowly-changing state in beava. ' +
      'Static or slowly-changing data: user tiers, merchant categories, allow/block lists, feature flags, tenant config. ' +
      'No native home in beava today. Prerequisite for RFC-003 stream-to-table joins. ' +
      'HTTP and TCP wire format for upsert and delete. ' +
      'Open questions: WAL serialization, snapshot interaction, TTL, aggregation tables forbidden, authorization model.',
  },
  {
    url: '/docs/community/rfcs/rfc-003-stream-to-table-join/',
    title: 'RFC-003 — Stream-to-table join',
    section: 'RFCs',
    content:
      'Stream-to-table join inside a pipeline. Join an event stream against a sibling table; snapshot read of the table side. Distinct from stream-stream joins, which v0 does not do. ' +
      'ev.join(UserTier, on="user_id"). ' +
      'Real-time decisions need static or slowly-changing context: user tier, account status, merchant category, allow/block list. ' +
      'Today teams denormalize at push time or query two stores app-side. Pipeline-level join keeps enrichment inside beava. ' +
      'Open questions: snapshot semantics read-latest, missing key behavior, chained joins, performance contract, register-time vs runtime errors, boundary with stream-stream joins.',
  },
  {
    url: '/docs/community/rfcs/rfc-004-event-log-retention/',
    title: 'RFC-004 — Event log retention and out-of-order events',
    section: 'RFCs',
    content:
      'Persist raw events past the in-memory window so they can be replayed for backfill, schema migrations, historical feature extraction. ' +
      'Tolerate events that arrive out of order or late, within a bounded window. ' +
      'Today beava persists aggregates not events. ' +
      'Out-of-order half revises v0 processing-time-only default. ' +
      'Open questions: storage tier local SSD vs S3, out-of-order tolerance bound, event-time vs ingestion-time, replay correctness, retention windows.',
  },
  {
    url: '/docs/community/rfcs/rfc-005-event-log-query-replay/',
    title: 'RFC-005 — Event-log query and replay',
    section: 'RFCs',
    content:
      'Read the raw event log directly: filter by stream, key, time range. Replay subsets into a new pipeline for backfill or A/B feature tests. ' +
      'Backfill should be a query, not a deployment. ' +
      'Today evaluating a new feature definition requires re-running production traffic. Direct replay enables iterative feature development against real history. ' +
      'curl /events/PaymentAttempt with key and since. beava replay --pipeline new_risk.py --since 2026-04-01. ' +
      'Open questions: query language SQL vs DSL vs HTTP, replay isolation, throughput contract, authorization, cost guardrails, backfill correctness.',
  },
  {
    url: '/docs/community/rfcs/rfc-006-online-pipeline-migration/',
    title: 'RFC-006 — Online pipeline migration',
    section: 'RFCs',
    content:
      'Change a pipeline definition without losing aggregate state or downtime. Add features, change windows, fix expressions. ' +
      'Today changing a pipeline means re-registration which discards in-memory state. ' +
      'Iterating on a feature should not cost a 30-day warm-up. ' +
      'Migration shapes: compatible additive, compatible expression, window change, key change, stream schema change. ' +
      'Open questions: compatibility classifier, hot-swap mechanics, rollback, schema versioning, declarative migration UX, depends on RFC-005 replay.',
  },
  {
    url: '/docs/community/dev-calls/',
    title: 'Weekly dev calls',
    section: 'Community',
    content:
      'beava hosts a weekly dev call for contributors, users, and people exploring the project. Build beava with us. ' +
      'Discuss RFCs, ask about the roadmap, share a use case, review benchmarks, talk through operational concerns. ' +
      'For product engineers, ML engineers, data engineers, infra engineers, founders, OSS contributors. ' +
      'Stream processing should not require a platform team. ' +
      'Public call summaries: topics, decisions, open questions, RFCs mentioned, follow-up tasks.',
  },
  {
    url: '/docs/community/contributing/',
    title: 'Contributing',
    section: 'Community',
    content:
      'Help make real-time features feel like application code. ' +
      'Contribute by writing code, improving docs, testing examples, sharing benchmarks, reviewing RFCs, bringing real use cases. ' +
      'Try beava locally, write examples, improve SDK ergonomics, add tests, help with benchmarks, review RFCs, improve deployment docs. ' +
      'Good first contributions: fix unclear docs, add small examples, improve error messages, add aggregation tests, reproduce a benchmark, write a tutorial. ' +
      'Feature owners should own feature logic.',
  },
];

const { index, errors: createErrors } = await createIndex({});
if (createErrors && createErrors.length) {
  console.error('createIndex errors:', createErrors);
  process.exit(1);
}

for (const p of PAGES) {
  const res = await index.addCustomRecord({
    url: p.url,
    content: p.content,
    language: 'en',
    meta: { title: p.title, section: p.section },
    filters: { section: [p.section] },
    sort: { title: p.title },
  });
  if (res.errors && res.errors.length) {
    console.error(`addCustomRecord errors for ${p.url}:`, res.errors);
    process.exit(1);
  }
}

const { errors: writeErrors } = await index.writeFiles({ outputPath: 'project/_pagefind' });
if (writeErrors && writeErrors.length) {
  console.error('writeFiles errors:', writeErrors);
  process.exit(1);
}

await close();
console.log(`Built ${PAGES.length} records to beava-website/project/_pagefind/`);
