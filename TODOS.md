# TODOS

## P1 — Next up after v1

### Rename Streamlet to Tally in CLAUDE.md
- **What:** Update all references from "Streamlet" to "Tally" in the spec
- **Why:** Design doc approved the rename during /office-hours. CLAUDE.md is stale.
- **Effort:** S (CC: ~5 min)
- **Depends on:** Nothing. Prerequisite for v1 implementation.

### Cross-stream views and lookups
- **What:** Enable `tx_to_login_ratio = Transactions.tx_count_1h / Logins.login_count_1h` and cross-key lookups via `st.lookup(MerchantActivity.chargeback_count_24h, on="merchant_id")`
- **Why:** Makes Tally a platform, not just an aggregation engine. Enables composite fraud signals across entity types.
- **Effort:** L (human: ~2-3 days / CC: ~1 hour)
- **Risk:** Medium. Cross-key resolution, view recomputation ordering, circular dependency detection.
- **Depends on:** Tally v1 complete.

### Incremental snapshot serialization
- **What:** Instead of serializing all state at once (blocking event loop), serialize in chunks between events or use copy-on-write data structures (im::HashMap) that can snapshot without blocking.
- **Why:** At 100K+ keys, snapshot serialization blocks the event loop for ~500ms every 30s, violating sub-millisecond latency promise. Production users will hit this.
- **Effort:** M (CC: ~2 hours)
- **Risk:** Medium. COW structures change the data layer. Chunked serialization needs careful borrow management.
- **Depends on:** v1 snapshot working.

### Batch GET endpoint (MGET)
- **What:** GET /mget that accepts multiple keys, returns features for all in one response
- **Why:** ML training pipelines need features for 10K+ users. Without batch GET, it's 10K individual round trips.
- **Effort:** S (CC: ~15 min)
- **Depends on:** v1 GET working.

### Schema evolution (add/remove features without full reset)
- **What:** Diff old vs new stream definitions on re-register. Preserve compatible features, reset only incompatible ones.
- **Why:** ML engineers iterate on feature definitions constantly. Full state reset on every change is painful.
- **Effort:** M (CC: ~1 hour)
- **Risk:** Medium. Need careful diffing logic.
- **Depends on:** v1 registration + snapshot working.

## P2 — Nice to have

### HLL query precision hint
- **What:** Let users specify `st.distinct_count(field, window, precision='fast')` to pre-compute a running merged HLL (O(1) read) vs `precision='exact'` which merges all bucket HLLs on read (O(buckets)).
- **Why:** HLL merge-on-read for 24h windows is ~1-5ms per feature per GET. 'fast' mode enables sub-100us GET for HLL features at the cost of slightly less accurate window expiry granularity.
- **Effort:** S (CC: ~30 min)
- **Depends on:** v1 HLL working.

### Event fan-out to multiple streams
- **What:** A single event with both user_id and merchant_id updates both Transactions and MerchantActivity streams.
- **Why:** Enables multi-entity feature computation from a single event push.
- **Effort:** M (human: ~1 day / CC: ~15 min)
- **Depends on:** v1 complete.

### HTTP management API
- **What:** /pipelines CRUD, /metrics Prometheus endpoint, /health check
- **Why:** Standard observability for production deployments.
- **Effort:** S (CC: ~15 min)

### Connection and stream count limits
- **What:** Max TCP connections, max registered streams, max features per stream
- **Why:** Prevents resource exhaustion in shared/production environments
- **Effort:** S (CC: ~10 min)

### Multi-tenancy / namespace isolation
- **What:** Namespace streams so multiple applications can share one Tally instance
- **Why:** Operational simplicity for teams running multiple services
- **Effort:** M (CC: ~30 min)

### Bundled binary distribution (pip install tally)
- **What:** Ruff-style bundled Rust binary inside pip wheel. `pip install tally` installs pre-built binary for the platform. `import tally` auto-starts the server as a background process on localhost. Cross-compilation CI for linux/macOS x86_64/arm64 using maturin for wheel building.
- **Why:** "pip install and 5 lines of Python" is the DX promise. Separate binary install adds friction. The ruff/uv model proves this distribution approach works at scale.
- **Effort:** L (CC: ~8 hours). Includes: maturin config, cross-compilation GitHub Actions, subprocess management for auto-start, health checking, graceful shutdown of background process.
- **Risk:** Medium. Cross-compilation matrix is fiddly. macOS code signing. Linux glibc vs musl.
- **Depends on:** v1 complete and validated. Python SDK working with separate server.

## P3 — Future

### Key-partitioned multi-threading
- **What:** Shard keyspace across cores within one process. No locks needed.
- **Why:** Vertical scaling beyond single-core throughput limit (~200K events/sec)
- **Effort:** XL (CC: ~2-3 hours). Different architecture, not a bolt-on.
- **Depends on:** v1 proven at scale, actual user demand.
