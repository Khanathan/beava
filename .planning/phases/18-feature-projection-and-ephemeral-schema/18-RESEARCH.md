# Phase 18: Feature Projection and Ephemeral Schema - Research

**Researched:** 2026-04-12
**Domain:** Response filtering, schema extension, serde backward compatibility
**Confidence:** HIGH

## Summary

Phase 18 adds two distinct capabilities: (1) feature projection -- `select()`/`drop()` on datasets that restrict which features appear in PUSH/GET responses, and (2) ephemeral pipeline schema fields (`projection`, `ephemeral`, `ttl`, `max_keys`) on RegisterRequest with `#[serde(default)]` for backward compatibility. The ephemeral fields are schema-only in v2.0; lifecycle enforcement is deferred to post-launch (FUT-01).

The codebase is well-structured for both changes. Projection is a response-layer filter: operators still compute all features, but `push_internal` and `get_features` filter their output `FeatureMap` before returning. The RegisterRequest already uses `#[serde(default)]` extensively (7 existing optional fields), so adding 4 more follows an established pattern. The snapshot round-trip path stores raw JSON strings in `SerializablePipeline.raw_register_json`, meaning new fields survive serialization automatically as long as they are present in the stored JSON.

**Primary recommendation:** Add projection as an `Option<Projection>` field on `StreamDefinition` (populated from RegisterRequest), then filter the `FeatureMap` in `push_internal` and `get_features` before return. Add ephemeral fields to RegisterRequest with `#[serde(default)]` and store them on StreamDefinition for future use. Python SDK adds `select()`/`drop()` methods on `DatasetDef` that emit a `projection` field in `_compile()`.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
None explicitly locked -- infrastructure phase with all choices at Claude's discretion.

### Key Constraints from STATE.md
- C-3: All new RegisterRequest fields use `#[serde(default)]` for backward compat. A v1.3-format RegisterRequest must load on v2.0 server.
- Projection is response-layer filtering -- operators still compute all features, projection only filters what's returned.
- Ephemeral fields are schema-only in v2.0 -- lifecycle enforcement deferred to post-launch.

### Claude's Discretion
All implementation choices are at Claude's discretion -- infrastructure phase.

### Deferred Ideas (OUT OF SCOPE)
None.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ENG-02 | Feature projection -- `select()`/`drop()` on a dataset restricts which features appear in PUSH/GET responses (response-layer filtering) | Projection enum on StreamDefinition, filter in push_internal + get_features, Python SDK methods on DatasetDef |
| ENG-03 | Ephemeral pipeline flag -- `ephemeral: bool`, `ttl`, `max_keys` fields on RegisterRequest with `#[serde(default)]` (schema-only, lifecycle deferred post-launch) | New fields on RegisterRequest + StreamDefinition, serde(default) pattern, snapshot round-trip via raw JSON |
</phase_requirements>

## Standard Stack

No new external dependencies required. This phase modifies existing Rust structs and Python classes.

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| serde | (existing) | Serialization with `#[serde(default)]` | Already used throughout RegisterRequest [VERIFIED: src/server/protocol.rs] |
| postcard | (existing) | Snapshot binary serialization | Already used for SnapshotState [VERIFIED: src/state/snapshot.rs] |
| ahash | (existing) | AHashMap/AHashSet for FeatureMap | Already used throughout engine [VERIFIED: src/engine/pipeline.rs] |

**No new dependencies needed.** [VERIFIED: codebase inspection]

## Architecture Patterns

### Pattern 1: Projection as Response-Layer Filter

**What:** A `Projection` enum stored on `StreamDefinition` that filters the `FeatureMap` after operators compute all features but before the map is returned to callers.

**Why response-layer, not computation-layer:** Operators must still compute all features because derive expressions can reference any feature. Computation-pruning projection (FUT-04) is deferred to v2.1+. [VERIFIED: REQUIREMENTS.md FUT-04]

**Where projection filtering goes:**

1. **`push_internal` (line ~632):** After features are collected and derives evaluated, filter the `FeatureMap` before `Ok(features)` at line 672. [VERIFIED: src/engine/pipeline.rs:632-672]

2. **`get_features` (line ~1086):** After all features (streams + views + derives) are collected, filter before returning. [VERIFIED: src/engine/pipeline.rs:1086-1170]

**Data model:**
```rust
// Source: design recommendation based on codebase analysis
#[derive(Debug, Clone)]
pub enum Projection {
    /// Only include these features in responses
    Select(AHashSet<String>),
    /// Exclude these features from responses
    Drop(AHashSet<String>),
}

impl Projection {
    pub fn apply(&self, features: &mut FeatureMap) {
        match self {
            Projection::Select(allowed) => {
                features.retain(|k, _| allowed.contains(k));
            }
            Projection::Drop(excluded) => {
                features.retain(|k, _| !excluded.contains(k));
            }
        }
    }
}
```

**Where to store it:**
```rust
// StreamDefinition gains a new field:
pub struct StreamDefinition {
    // ... existing fields ...
    pub projection: Option<Projection>,
}
```

### Pattern 2: RegisterRequest Extension with serde(default)

**What:** Add new optional fields to `RegisterRequest` that default to `None` when absent from the JSON payload. This is the established pattern -- 7 fields already use `#[serde(default)]`. [VERIFIED: src/server/protocol.rs:409-424]

**Existing pattern (RegisterRequest, line 409-424):**
```rust
pub struct RegisterRequest {
    pub name: String,
    #[serde(default)]
    pub key_field: Option<String>,
    #[serde(default, rename = "type")]
    pub definition_type: Option<String>,
    pub features: Vec<FeatureDefRequest>,
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub entity_ttl: Option<String>,
    #[serde(default)]
    pub history_ttl: Option<String>,
}
```

**New fields to add:**
```rust
#[serde(default)]
pub projection: Option<ProjectionRequest>,  // ENG-02
#[serde(default)]
pub ephemeral: Option<bool>,                // ENG-03
#[serde(default)]
pub ttl: Option<String>,                    // ENG-03 (pipeline-level TTL)
#[serde(default)]
pub max_keys: Option<u64>,                  // ENG-03
```

**ProjectionRequest struct:**
```rust
#[derive(Debug, Deserialize)]
pub struct ProjectionRequest {
    #[serde(default)]
    pub select: Option<Vec<String>>,
    #[serde(default)]
    pub drop: Option<Vec<String>>,
}
```

### Pattern 3: Snapshot Round-Trip Preservation

**What:** New RegisterRequest fields survive snapshot save/load automatically because:
1. On REGISTER, the raw JSON is stored via `engine.store_raw_register_json(&name, json_val)` [VERIFIED: src/main.rs:125]
2. On snapshot save, the raw JSON is serialized to `SerializablePipeline.raw_register_json` as a String [VERIFIED: src/main.rs:276]
3. On snapshot restore, `raw_register_json` is parsed back to `serde_json::Value`, then deserialized as `RegisterRequest` [VERIFIED: src/main.rs:108-125]

**Key insight:** Because the raw JSON string preserves ALL fields (including new ones), and `RegisterRequest` uses `#[serde(default)]` for the new fields, a v1.3-format JSON (without `projection`/`ephemeral`/`ttl`/`max_keys`) deserializes correctly with those fields as `None`. Conversely, a v2.0 JSON with those fields present round-trips through the snapshot unchanged.

**No snapshot format migration needed.** The postcard format for `SerializablePipeline` is unchanged (it stores an opaque JSON string). [VERIFIED: src/state/snapshot.rs:97-106]

### Pattern 4: Python SDK select()/drop() on DatasetDef

**What:** Add `select()` and `drop()` methods to `DatasetDef` that return a new `DatasetDef` with a `projection` field set. The `_compile()` method emits this as a `projection` key in the RegisterRequest JSON.

**Reference implementation (old API, _dataframe.py:327-341):** The old `Table` class had `select()` and `drop()` that filtered the features dict, creating a new stream with fewer features. [VERIFIED: python/tally/_dataframe.py:327-341]

**New v2.0 approach is different:** Instead of creating a new stream with fewer feature definitions, `select()`/`drop()` set a projection field that tells the server to filter responses. This is better because:
- Operators still compute all features (needed for derives)
- No need to create synthetic stream names
- Single stream registration, single entity state

```python
# Python SDK usage:
@dataset(depends_on=[RawTxns])
class UserTxns:
    features = group_by("user_id").agg(
        tx_count=tl.count(window="1h"),
        tx_sum=tl.sum("amount", window="1h"),
        internal_metric=tl.count(window="24h"),
    )

# Only tx_count and tx_sum in responses:
UserTxns_public = UserTxns.select(["tx_count", "tx_sum"])
# Or equivalently:
UserTxns_public = UserTxns.drop(["internal_metric"])
```

### Anti-Patterns to Avoid

- **Computation-layer projection:** Do NOT skip operator evaluation for dropped features. Derive expressions may reference them. This is explicitly deferred to FUT-04. [VERIFIED: REQUIREMENTS.md FUT-04]
- **Projection as a new stream:** Do NOT create a synthetic stream for projections (the old `_dataframe.py` pattern of `f"{name}__select"`). Projection is metadata on the same stream.
- **Mutable DatasetDef:** `select()`/`drop()` should return a NEW DatasetDef (immutable pattern), not mutate the existing one. This matches `GroupedDataset.agg()` which returns a new instance. [VERIFIED: python/tally/_dataset.py:38-52]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Optional field deserialization | Manual JSON parsing for new fields | `#[serde(default)]` on `Option<T>` | Established pattern, 7 existing fields already use it |
| Feature set membership check | Vec::contains for projection filtering | `AHashSet<String>` | O(1) lookup vs O(n) per feature |
| Snapshot format migration | New postcard struct version | Raw JSON string passthrough | Fields already stored as opaque JSON, no binary format change needed |

## Common Pitfalls

### Pitfall 1: Projection Breaks Derive Evaluation
**What goes wrong:** If projection filtering happens BEFORE derive evaluation, derives that reference dropped features get `Missing` values.
**Why it happens:** Temptation to filter early for "performance."
**How to avoid:** Filter the FeatureMap AFTER all derives are evaluated. The filter is the last step before return.
**Warning signs:** Derive expressions returning `Missing` when their referenced features are in the drop list.

### Pitfall 2: Qualified Names in Projection
**What goes wrong:** `get_features` inserts qualified names like `"Transactions.tx_count_1h"` into the FeatureMap. If projection uses `Select({"tx_count_1h"})`, the qualified version leaks through.
**Why it happens:** Projection filter runs on the full FeatureMap which contains both qualified and unqualified names.
**How to avoid:** Apply projection BEFORE qualified names are inserted (in `get_features`), OR also strip qualified names after projection. The existing MGET handler already strips dot-containing names (tcp.rs:1252). Simplest: apply projection, then strip qualified names.
**Warning signs:** GET responses containing "StreamName.feature" keys.

### Pitfall 3: select() and drop() Both Set -- Conflict
**What goes wrong:** User calls both `select()` and `drop()` on the same dataset, creating ambiguous projection.
**Why it happens:** API allows chaining.
**How to avoid:** `ProjectionRequest` validation at registration time: if both `select` and `drop` are provided, return an error. In Python, `select()` and `drop()` each replace the projection (not additive).
**Warning signs:** Test that calls both and expects a clear error.

### Pitfall 4: Backward Compat -- Missing serde(default)
**What goes wrong:** A v1.3 RegisterRequest JSON (without new fields) fails to deserialize on v2.0 server.
**Why it happens:** Forgetting `#[serde(default)]` on any new field.
**How to avoid:** Every new field on RegisterRequest MUST have `#[serde(default)]`. Write a test that deserializes a minimal v1.3 JSON (just `name` + `features`).
**Warning signs:** "missing field" deserialization errors in snapshot restore.

### Pitfall 5: Ephemeral Fields Accidentally Enforced
**What goes wrong:** Adding `ephemeral: true` actually triggers cleanup behavior that doesn't exist yet.
**Why it happens:** Overzealous implementation.
**How to avoid:** Store the fields on StreamDefinition but do NOT add any runtime enforcement. They are schema-only markers for future use. The `if ephemeral { ... }` code paths belong to FUT-01. [VERIFIED: REQUIREMENTS.md FUT-01]
**Warning signs:** Pipelines disappearing or keys being evicted unexpectedly.

## Code Examples

### Rust: Projection Enum and Application
```rust
// Source: design based on codebase patterns
use ahash::AHashSet;

#[derive(Debug, Clone)]
pub enum Projection {
    Select(AHashSet<String>),
    Drop(AHashSet<String>),
}

impl Projection {
    pub fn apply(&self, features: &mut FeatureMap) {
        match self {
            Projection::Select(allowed) => {
                features.retain(|k, _| allowed.contains(k));
            }
            Projection::Drop(excluded) => {
                features.retain(|k, _| !excluded.contains(k));
            }
        }
    }
}
```

### Rust: RegisterRequest New Fields
```rust
// Source: extension of src/server/protocol.rs:409-424
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    // ... existing fields unchanged ...
    #[serde(default)]
    pub projection: Option<ProjectionRequest>,
    #[serde(default)]
    pub ephemeral: Option<bool>,
    #[serde(default)]
    pub ttl: Option<String>,       // pipeline-level TTL, e.g. "1h"
    #[serde(default)]
    pub max_keys: Option<u64>,     // max entity keys for this pipeline
}

#[derive(Debug, Deserialize)]
pub struct ProjectionRequest {
    #[serde(default)]
    pub select: Option<Vec<String>>,
    #[serde(default)]
    pub drop: Option<Vec<String>>,
}
```

### Rust: convert_register_request Extension
```rust
// Source: extension of src/server/protocol.rs:476+
// Inside convert_register_request, after building features vec:
let projection = match req.projection {
    Some(proj) => {
        if proj.select.is_some() && proj.drop.is_some() {
            return Err(TallyError::Protocol(
                "projection cannot have both 'select' and 'drop'".into(),
            ));
        }
        if let Some(select) = proj.select {
            Some(Projection::Select(select.into_iter().collect()))
        } else if let Some(drop_list) = proj.drop {
            Some(Projection::Drop(drop_list.into_iter().collect()))
        } else {
            None
        }
    }
    None => None,
};
```

### Rust: Filtering in push_internal
```rust
// Source: modification at src/engine/pipeline.rs:670-672
// After derives evaluated, before return:
if let Some(ref proj) = stream.projection {
    proj.apply(&mut features);
}
Ok(features)
```

### Python: DatasetDef.select() and DatasetDef.drop()
```python
# Source: extension of python/tally/_dataset.py DatasetDef class
def select(self, feature_names: list[str]) -> DatasetDef:
    """Return a new DatasetDef that only includes the named features in responses."""
    new = DatasetDef(
        name=self._name,
        depends_on=self._depends_on,
        grouped_dataset=self._grouped_dataset,
        extra_features=self._extra_features,
        event_schema=self._event_schema,
        entity_ttl=self._entity_ttl,
        history_ttl=self._history_ttl,
    )
    new._projection = {"select": feature_names}
    return new

def drop(self, feature_names: list[str]) -> DatasetDef:
    """Return a new DatasetDef that excludes the named features from responses."""
    new = DatasetDef(
        name=self._name,
        depends_on=self._depends_on,
        grouped_dataset=self._grouped_dataset,
        extra_features=self._extra_features,
        event_schema=self._event_schema,
        entity_ttl=self._entity_ttl,
        history_ttl=self._history_ttl,
    )
    new._projection = {"drop": feature_names}
    return new
```

### Python: _compile() Emitting Projection
```python
# Source: extension of python/tally/_dataset.py DatasetDef._compile()
# After existing fields in the dict:
if hasattr(self, '_projection') and self._projection:
    d["projection"] = self._projection
```

### Rust: Backward Compat Test
```rust
// Source: test design
#[test]
fn test_v1_3_register_request_loads_on_v2() {
    // Minimal v1.3-format JSON: no projection, no ephemeral, no ttl, no max_keys
    let json = r#"{
        "name": "Transactions",
        "key_field": "user_id",
        "features": [{"name": "tx_count_1h", "type": "count", "window": "1h"}]
    }"#;
    let req: RegisterRequest = serde_json::from_str(json).unwrap();
    assert_eq!(req.name, "Transactions");
    assert!(req.projection.is_none());
    assert!(req.ephemeral.is_none());
    assert!(req.ttl.is_none());
    assert!(req.max_keys.is_none());
}
```

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in `#[test]` + integration tests |
| Config file | `Cargo.toml` (workspace) |
| Quick run command | `cargo test --lib` |
| Full suite command | `cargo test` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ENG-02a | select() filters PUSH response | integration | `cargo test test_projection_select_push -p tally` | Wave 0 |
| ENG-02b | select() filters GET response | integration | `cargo test test_projection_select_get -p tally` | Wave 0 |
| ENG-02c | drop() filters PUSH response | integration | `cargo test test_projection_drop_push -p tally` | Wave 0 |
| ENG-02d | drop() filters GET response | integration | `cargo test test_projection_drop_get -p tally` | Wave 0 |
| ENG-02e | derives still evaluate with dropped features | unit | `cargo test test_projection_derive_still_evaluates -p tally` | Wave 0 |
| ENG-02f | Python select()/drop() emits correct JSON | unit (Python) | `python -m pytest python/tests/test_new_api.py -k projection` | Wave 0 |
| ENG-03a | New fields deserialize with serde(default) | unit | `cargo test test_v1_3_register_request_loads -p tally` | Wave 0 |
| ENG-03b | New fields preserved in RegisterRequest | unit | `cargo test test_ephemeral_fields_roundtrip -p tally` | Wave 0 |
| C-3 | Snapshot round-trip with new fields | integration | `cargo test test_snapshot_roundtrip_new_fields -p tally` | Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo test --lib -p tally`
- **Per wave merge:** `cargo test`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] Projection unit tests in `src/engine/pipeline.rs` (inline `#[cfg(test)]` module)
- [ ] Backward compat deserialization test in `src/server/protocol.rs` tests
- [ ] Snapshot round-trip test in `tests/test_snapshot.rs`
- [ ] Python projection tests in `python/tests/test_new_api.py`

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `ttl` field name won't conflict with `entity_ttl`/`history_ttl` | Architecture Patterns | Confusing API -- could rename to `pipeline_ttl` |
| A2 | `max_keys` is `u64` (not `usize` or `u32`) | Code Examples | Wrong type for very large or very small limits |

## Open Questions

1. **Projection + Views interaction**
   - What we know: `get_features` collects features from ALL streams + views. Projection is per-stream.
   - What's unclear: Should projection on stream X also filter view features that reference X? Or does each view have its own projection?
   - Recommendation: Projection applies per-stream only. Views are separate definitions and would need their own projection if desired. For now, `get_features` applies per-stream projections independently, then views see the full (unfiltered) feature set for derive evaluation, but view features are NOT filtered by any stream's projection.

2. **`ttl` field naming**
   - What we know: RegisterRequest already has `entity_ttl` and `history_ttl`. Adding bare `ttl` for pipeline-level TTL.
   - What's unclear: Is `ttl` too ambiguous? Could be confused with the existing TTL fields.
   - Recommendation: Use `pipeline_ttl` for clarity. But since this is schema-only and not enforced in v2.0, the name can be adjusted before FUT-01 enforcement.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | -- |
| V3 Session Management | no | -- |
| V4 Access Control | no | -- |
| V5 Input Validation | yes | serde deserialization + explicit validation in convert_register_request |
| V6 Cryptography | no | -- |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Malformed projection field | Tampering | serde deserialization rejects unknown types; explicit select/drop mutual exclusion validation |
| Oversized select/drop lists | DoS | Reasonable -- each entry is a string match against existing features; no amplification |

## Sources

### Primary (HIGH confidence)
- `src/server/protocol.rs:409-449` -- Current RegisterRequest and FeatureDefRequest structs
- `src/engine/pipeline.rs:506-672` -- push_internal implementation (feature collection + return)
- `src/engine/pipeline.rs:1086-1170` -- get_features implementation
- `src/engine/pipeline.rs:231-258` -- StreamDefinition struct
- `src/main.rs:105-130` -- Snapshot restore: re-register pipelines from raw JSON
- `src/main.rs:262-290` -- Snapshot save: serialize raw JSON to SerializablePipeline
- `src/state/snapshot.rs:97-106` -- SerializablePipeline struct (raw_register_json: String)
- `python/tally/_dataset.py:106-268` -- DatasetDef class and _compile()
- `python/tally/_dataframe.py:327-341` -- Old select()/drop() reference implementation

### Secondary (MEDIUM confidence)
- `.planning/REQUIREMENTS.md` -- ENG-02, ENG-03, FUT-01, FUT-04 definitions
- `.planning/STATE.md` -- C-3 pitfall, v2.0 decisions

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, all existing crate patterns
- Architecture: HIGH -- clear insertion points identified in codebase with line numbers
- Pitfalls: HIGH -- backward compat pattern well-established, 7 existing serde(default) fields as precedent

**Research date:** 2026-04-12
**Valid until:** 2026-05-12 (stable patterns, no external dependency churn)
