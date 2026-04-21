//! Phase 59.6 Wave 1 (TPC-PERF-11) — typed-row runtime schema.
//!
//! See `.planning/phases/59.6-typed-pipeline-records/59.6-CONTEXT.md`
//! Area A (D-A1, D-A2, D-A3) for the full design contract.
//!
//! # Overview
//!
//! This module introduces a fixed-layout row representation that replaces
//! `serde_json::Value` as the in-pipeline event/state representation on
//! the hot path. Streams registered via `@bv.stream` with typed annotations
//! ship a `schema:` block in the REGISTER JSON — the engine consumes that
//! block here into a `RegisteredSchema` and keeps it in a per-engine
//! `SchemaRegistry`. Downstream waves (W2 wire codec, W3+ operators) branch
//! on `engine.is_typed_stream(name)` to take the typed fast-path.
//!
//! # Types
//!
//! - [`FieldTy`]: compact enum over the scalar column types we support on
//!   the typed path (i64, f64, bool, inline/long strings, bytes).
//! - [`FieldSpec`]: one column's declaration — name, type, byte offset in
//!   the payload, nullable flag.
//! - [`RegisteredSchema`]: a fully-declared schema for a stream or source
//!   table; assigned a monotonic [`SchemaId`] when inserted into the
//!   registry.
//! - [`Row`]: a single typed record — `schema_id` + fixed-layout `payload`
//!   + per-row `arena` (for long strings and bytes).
//! - [`SchemaRegistry`]: per-engine map from stream name → `Arc<RegisteredSchema>`,
//!   plus the inverse id → schema lookup used by the wire codec.
//!
//! # Inline-string slot width (D-A1 clarification)
//!
//! `inline_str_cap` is the raw byte count of user-visible characters that
//! fit inline. The *slot* reserved in the payload is `inline_str_cap + 1`
//! bytes — the extra byte holds a NUL terminator so readers can recover
//! strings shorter than the cap without tracking per-row lengths. At a
//! default cap of 15, the slot is 16 bytes wide; a 15-byte string writes
//! all 15 bytes and the terminator is truncated (readers observe the full
//! 15 bytes). This matches CONTEXT.md's example layout where `user_id` at
//! offset 0 and `country_code` at offset 16 are both InlineStr columns.

use std::sync::Arc;

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

/// Monotonic identifier assigned at registration time. Stable within a
/// single process lifetime; snapshots (Wave 5+) serialize `schema_id`
/// alongside the payload bytes so re-registration after restart preserves
/// the id through the `by_id` inverse map.
pub type SchemaId = u32;

/// Scalar field type supported by the typed-row path.
///
/// See `fixed_width` for the slot size reserved at `FieldSpec::offset`
/// for each variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldTy {
    /// 64-bit signed integer, stored little-endian at `offset..offset+8`.
    I64,
    /// 64-bit IEEE-754 float, stored little-endian at `offset..offset+8`.
    F64,
    /// 1-byte boolean (0/1) at `offset..offset+1`.
    Bool,
    /// Inline string of up to `inline_str_cap` bytes. Stored directly at
    /// `offset..offset+inline_str_cap+1`; the trailing byte is a NUL
    /// terminator for strings shorter than the cap.
    InlineStr,
    /// Long string stored in the row's arena. The 8 bytes at `offset`
    /// hold `(start: u32, len: u32)` pointing into `Row::arena`.
    String,
    /// Raw bytes; identical `(start, len)` shape as `String` but no UTF-8
    /// guarantee.
    Bytes,
}

impl FieldTy {
    /// Fixed-layout byte width at the row's offset (before arena).
    ///
    /// For `InlineStr` the slot is `inline_str_cap + 1` — the extra byte
    /// holds a NUL terminator. See the module-level doc-comment.
    pub fn fixed_width(&self, inline_str_cap: u8) -> u16 {
        match self {
            FieldTy::I64 | FieldTy::F64 => 8,
            FieldTy::Bool => 1,
            FieldTy::InlineStr => inline_str_cap as u16 + 1,
            FieldTy::String | FieldTy::Bytes => 8, // (start: u32, len: u32)
        }
    }
}

/// One column's declaration inside a [`RegisteredSchema`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldSpec {
    pub name: String,
    pub ty: FieldTy,
    pub offset: u16,
    #[serde(default)]
    pub nullable: bool,
}

fn default_inline_str_cap() -> u8 {
    15
}

/// Fully-declared schema for a typed stream or source table.
///
/// Constructed from REGISTER JSON by `engine::register` (see Wave 1 Task 2);
/// stored in a [`SchemaRegistry`] under the stream name. The `schema_id`
/// field is populated by `SchemaRegistry::insert` — callers supplying a
/// freshly-parsed schema should leave it as 0.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisteredSchema {
    pub schema_id: SchemaId,
    pub name: String,
    pub fields: Vec<FieldSpec>,
    #[serde(default = "default_inline_str_cap")]
    pub inline_str_cap: u8,
    pub row_size: u16,
}

/// D-A4 cap: reject schemas larger than 64 KiB at register time. Bounds
/// malicious or buggy REGISTER payloads from blowing out per-row
/// allocations on the hot path.
pub const MAX_ROW_SIZE: u16 = u16::MAX; // == 65535 (< 64 KiB by one byte).

/// Error returned by [`RegisteredSchema::validate_layout`] when the
/// REGISTER payload's `schema:` block describes a physically-impossible
/// row layout. The server translates this into a `BeavaError::Protocol`.
#[derive(Debug, Clone)]
pub enum SchemaValidateError {
    /// A field's `offset + fixed_width(ty)` exceeds `row_size` — decoding
    /// that field would read past the end of the payload.
    FieldOverflow {
        field: String,
        offset: u16,
        width: u16,
        row_size: u16,
    },
    /// Two fields overlap in the payload (offsets + widths intersect).
    /// Ambiguous decoding; rejected at register time.
    FieldOverlap {
        a: String,
        b: String,
    },
    /// The computed `expected_row_size` from the field list doesn't match
    /// the declared `row_size`. Client-side CompiledSchema is out of sync
    /// with the server's layout math.
    RowSizeMismatch {
        declared: u16,
        expected: u16,
    },
    /// Row is larger than `MAX_ROW_SIZE` — DoS mitigation (T-59.6-01-03).
    RowTooLarge {
        row_size: u32,
    },
}

impl std::fmt::Display for SchemaValidateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaValidateError::FieldOverflow {
                field,
                offset,
                width,
                row_size,
            } => write!(
                f,
                "field {field:?} offset {offset} + width {width} exceeds row_size {row_size}"
            ),
            SchemaValidateError::FieldOverlap { a, b } => {
                write!(f, "fields {a:?} and {b:?} overlap in the payload")
            }
            SchemaValidateError::RowSizeMismatch { declared, expected } => write!(
                f,
                "declared row_size {declared} != computed row_size {expected}"
            ),
            SchemaValidateError::RowTooLarge { row_size } => {
                write!(f, "row_size {row_size} exceeds MAX_ROW_SIZE {MAX_ROW_SIZE}")
            }
        }
    }
}

impl std::error::Error for SchemaValidateError {}

impl RegisteredSchema {
    /// Field lookup by name — O(N) linear scan. N is typically ≤ 10 so
    /// this is faster than a HashMap; callers that need repeated lookups
    /// should cache the index via [`Self::field_index`] at register time.
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    /// Byte offset of a named field's fixed slot within a Row's payload.
    pub fn field_offset(&self, name: &str) -> Option<u16> {
        self.field_index(name).map(|i| self.fields[i].offset)
    }

    /// Validate the layout — every field's slot fits inside `row_size`,
    /// no two fields overlap, and the declared `row_size` matches the
    /// sum of field widths. Called by `to_registered_schema` at REGISTER
    /// time (T-59.6-01-01 mitigation).
    pub fn validate_layout(&self) -> Result<(), SchemaValidateError> {
        if (self.row_size as u32) > MAX_ROW_SIZE as u32 {
            return Err(SchemaValidateError::RowTooLarge {
                row_size: self.row_size as u32,
            });
        }
        // Per-field overflow + compute expected row_size as max(offset + width).
        let mut expected: u32 = 0;
        for f in &self.fields {
            let width = f.ty.fixed_width(self.inline_str_cap);
            let end = (f.offset as u32) + (width as u32);
            if end > self.row_size as u32 {
                return Err(SchemaValidateError::FieldOverflow {
                    field: f.name.clone(),
                    offset: f.offset,
                    width,
                    row_size: self.row_size,
                });
            }
            if end > expected {
                expected = end;
            }
        }
        // Overlap check — O(N²) but N ≤ ~32 on realistic schemas.
        for i in 0..self.fields.len() {
            let a = &self.fields[i];
            let aw = a.ty.fixed_width(self.inline_str_cap) as u32;
            let astart = a.offset as u32;
            let aend = astart + aw;
            for j in (i + 1)..self.fields.len() {
                let b = &self.fields[j];
                let bw = b.ty.fixed_width(self.inline_str_cap) as u32;
                let bstart = b.offset as u32;
                let bend = bstart + bw;
                if astart < bend && bstart < aend {
                    return Err(SchemaValidateError::FieldOverlap {
                        a: a.name.clone(),
                        b: b.name.clone(),
                    });
                }
            }
        }
        // expected_row_size (tightest-packed layout) must match declared.
        // We allow declared >= expected only when all fields pack perfectly
        // (expected == declared) to catch off-by-one errors in client
        // CompiledSchema emission.
        if (self.row_size as u32) != expected {
            return Err(SchemaValidateError::RowSizeMismatch {
                declared: self.row_size,
                expected: expected as u16,
            });
        }
        Ok(())
    }
}

/// Phase 59.6 Wave 1 — typed row. Fixed-layout `payload` + per-row `arena`
/// for long strings / bytes. NOT a SOA column-store; each row is contiguous
/// for cache locality on the hot agg-update path.
///
/// Wave 1 stores owned `Vec<u8>` for both payload and arena. Wave 4+ may
/// introduce an `Arc<Vec<u8>>` arena-sharing variant if per-batch reuse
/// proves profitable in profiling.
#[derive(Clone, Debug)]
#[repr(C)]
pub struct RowInner {
    /// Fixed-layout bytes — length = schema.row_size.
    pub payload: Vec<u8>,
    /// Variable-length tail for long strings / bytes. Empty if no long
    /// fields are populated.
    pub arena: Vec<u8>,
}

/// Public row wrapper. Holds `schema_id` alongside the fixed-layout inner
/// struct so Wave 2+ wire codec can validate the row against a registered
/// schema before decoding column values.
#[derive(Clone, Debug)]
pub struct Row {
    pub schema_id: SchemaId,
    pub payload: Vec<u8>,
    pub arena: Vec<u8>,
}

impl Row {
    /// Allocate a zeroed row of the given schema's `row_size`.
    pub fn zeroed(schema: &RegisteredSchema) -> Self {
        Self {
            schema_id: schema.schema_id,
            payload: vec![0u8; schema.row_size as usize],
            arena: Vec::new(),
        }
    }

    /// Read an i64 at the given offset.
    ///
    /// # Panics
    /// Panics if `offset + 8 > payload.len()`. Callers must supply an
    /// offset sourced from a validated `FieldSpec` (T-59.6-01-02
    /// mitigation: bounds enforced at register time by `validate_layout`).
    #[inline]
    pub fn read_i64(&self, offset: u16) -> i64 {
        let o = offset as usize;
        let bytes = &self.payload[o..o + 8];
        i64::from_le_bytes(bytes.try_into().unwrap())
    }

    #[inline]
    pub fn write_i64(&mut self, offset: u16, val: i64) {
        let o = offset as usize;
        self.payload[o..o + 8].copy_from_slice(&val.to_le_bytes());
    }

    #[inline]
    pub fn read_f64(&self, offset: u16) -> f64 {
        let o = offset as usize;
        let bytes = &self.payload[o..o + 8];
        f64::from_le_bytes(bytes.try_into().unwrap())
    }

    #[inline]
    pub fn write_f64(&mut self, offset: u16, val: f64) {
        let o = offset as usize;
        self.payload[o..o + 8].copy_from_slice(&val.to_le_bytes());
    }

    #[inline]
    pub fn read_bool(&self, offset: u16) -> bool {
        self.payload[offset as usize] != 0
    }

    #[inline]
    pub fn write_bool(&mut self, offset: u16, val: bool) {
        self.payload[offset as usize] = if val { 1 } else { 0 };
    }

    /// Read an inline string at offset. The slot is `inline_str_cap + 1`
    /// bytes wide; we truncate at the first NUL byte within the slot.
    pub fn read_inline_str(&self, offset: u16, inline_str_cap: u8) -> &str {
        let cap = inline_str_cap as usize;
        let slot = cap + 1;
        let start = offset as usize;
        let end = start + slot;
        let slice = &self.payload[start..end];
        // Find NUL terminator within the slot; if none, strings may have
        // filled the full cap and are read as cap bytes (no terminator).
        let nul = slice.iter().position(|&b| b == 0).unwrap_or(cap);
        std::str::from_utf8(&slice[..nul.min(cap)]).unwrap_or("")
    }

    /// Write an inline string at offset. Truncates to `inline_str_cap`
    /// bytes if the input is longer; zero-pads the remainder of the slot.
    pub fn write_inline_str(&mut self, offset: u16, inline_str_cap: u8, s: &str) {
        let bytes = s.as_bytes();
        let cap = inline_str_cap as usize;
        let slot = cap + 1;
        let copy_len = bytes.len().min(cap);
        let start = offset as usize;
        self.payload[start..start + copy_len].copy_from_slice(&bytes[..copy_len]);
        // Zero-pad the rest of the slot (including the trailing NUL byte).
        for b in &mut self.payload[start + copy_len..start + slot] {
            *b = 0;
        }
    }

    /// Read a long string via arena. The 8 bytes at `offset` hold
    /// `(start: u32, len: u32)` pointing into `arena`.
    pub fn read_string(&self, offset: u16) -> &str {
        let pos = offset as usize;
        let start =
            u32::from_le_bytes(self.payload[pos..pos + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(self.payload[pos + 4..pos + 8].try_into().unwrap())
            as usize;
        std::str::from_utf8(&self.arena[start..start + len]).unwrap_or("")
    }

    /// Append `s` to the arena and write its `(start, len)` pair at offset.
    pub fn write_string(&mut self, offset: u16, s: &str) {
        let start = self.arena.len() as u32;
        self.arena.extend_from_slice(s.as_bytes());
        let len = s.len() as u32;
        let pos = offset as usize;
        self.payload[pos..pos + 4].copy_from_slice(&start.to_le_bytes());
        self.payload[pos + 4..pos + 8].copy_from_slice(&len.to_le_bytes());
    }

    /// Read raw bytes via arena. Same `(start, len)` layout as `read_string`
    /// but no UTF-8 guarantee.
    pub fn read_bytes(&self, offset: u16) -> &[u8] {
        let pos = offset as usize;
        let start =
            u32::from_le_bytes(self.payload[pos..pos + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(self.payload[pos + 4..pos + 8].try_into().unwrap())
            as usize;
        &self.arena[start..start + len]
    }

    /// Append raw bytes to the arena and write the `(start, len)` pair.
    pub fn write_bytes(&mut self, offset: u16, b: &[u8]) {
        let start = self.arena.len() as u32;
        self.arena.extend_from_slice(b);
        let len = b.len() as u32;
        let pos = offset as usize;
        self.payload[pos..pos + 4].copy_from_slice(&start.to_le_bytes());
        self.payload[pos + 4..pos + 8].copy_from_slice(&len.to_le_bytes());
    }
}

/// Phase 59.6 Wave 2 bridge (D-B1, TPC-PERF-11): convert a typed `Row` back
/// to a `serde_json::Value` by walking the schema's `FieldSpec` entries.
///
/// Wave 2 uses this to bridge the typed decode path onto the existing
/// `push_with_cascade_on_shard` entry point — operators still run on
/// `Value`, but the wire decode has already happened and the server has
/// validated the schema. Wave 3+ replaces this bridge by threading the
/// `Row` directly through to typed operators, removing the JSON
/// re-serialize entirely from the hot path.
///
/// The bridge is intentionally slow (per-field `Value::from` allocations) —
/// it exists only so Wave 2 can ship wire-level correctness without
/// touching operator execution. Performance work happens in Wave 3 when
/// operators gain typed fast-paths.
pub fn row_to_value(row: &Row, schema: &RegisteredSchema) -> serde_json::Value {
    let mut obj = serde_json::Map::with_capacity(schema.fields.len());
    for f in &schema.fields {
        let v = match f.ty {
            FieldTy::I64 => {
                serde_json::Value::Number(serde_json::Number::from(row.read_i64(f.offset)))
            }
            FieldTy::F64 => {
                let n = row.read_f64(f.offset);
                serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
            FieldTy::Bool => serde_json::Value::Bool(row.read_bool(f.offset)),
            FieldTy::InlineStr => serde_json::Value::String(
                row.read_inline_str(f.offset, schema.inline_str_cap).to_string(),
            ),
            FieldTy::String => serde_json::Value::String(row.read_string(f.offset).to_string()),
            FieldTy::Bytes => {
                // Wave 2 simplification: render bytes as lossy UTF-8 string.
                // Operators consuming Bytes-typed fields in Wave 3+ will
                // bypass this bridge entirely.
                let b = row.read_bytes(f.offset);
                serde_json::Value::String(String::from_utf8_lossy(b).to_string())
            }
        };
        obj.insert(f.name.clone(), v);
    }
    serde_json::Value::Object(obj)
}

/// Phase 59.6 Wave 3 (TPC-PERF-11): inverse of `row_to_value`. Writes a
/// serde_json::Value into a freshly-allocated Row following the schema's
/// field layout. Used by the typed-path fallback in `push_typed_on_shard`
/// (Value → Row upgrade at the operator boundary) and by the mixed-mode
/// EnrichFromTable path where the right-side source_table is still Value
/// (Wave 5 makes source_tables typed).
///
/// Behavior:
/// - Scalar fields are written via the matching `Row::write_*` helper; on
///   type mismatch (e.g. field declared i64 but Value holds a string) a
///   `BeavaError::Protocol` is returned.
/// - Missing fields: if nullable → left as zero; if required → error.
/// - `FieldTy::Bytes` reads a string value (symmetric with `row_to_value`'s
///   lossy UTF-8 emission — the Wave-3 bridge stays on string-compatible
///   payloads; Wave 5+ revisits true bytes dispatch).
pub fn value_to_row(
    value: &serde_json::Value,
    schema: &RegisteredSchema,
) -> Result<Row, crate::error::BeavaError> {
    use crate::error::BeavaError;
    let mut row = Row::zeroed(schema);
    let obj = value.as_object().ok_or_else(|| {
        BeavaError::Protocol("value_to_row: expected JSON object".to_string())
    })?;
    for f in &schema.fields {
        let v = match obj.get(&f.name) {
            Some(v) => v,
            None if f.nullable => continue,
            None => {
                return Err(BeavaError::Protocol(format!(
                    "value_to_row: missing required field '{}'",
                    f.name
                )));
            }
        };
        if v.is_null() {
            if f.nullable {
                continue;
            }
            return Err(BeavaError::Protocol(format!(
                "value_to_row: field '{}' is null but non-nullable",
                f.name
            )));
        }
        match f.ty {
            FieldTy::I64 => {
                let n = v.as_i64().ok_or_else(|| {
                    BeavaError::Protocol(format!(
                        "value_to_row: field '{}' expected i64, got {:?}",
                        f.name, v
                    ))
                })?;
                row.write_i64(f.offset, n);
            }
            FieldTy::F64 => {
                let n = v.as_f64().ok_or_else(|| {
                    BeavaError::Protocol(format!(
                        "value_to_row: field '{}' expected f64, got {:?}",
                        f.name, v
                    ))
                })?;
                row.write_f64(f.offset, n);
            }
            FieldTy::Bool => {
                let b = v.as_bool().ok_or_else(|| {
                    BeavaError::Protocol(format!(
                        "value_to_row: field '{}' expected bool, got {:?}",
                        f.name, v
                    ))
                })?;
                row.write_bool(f.offset, b);
            }
            FieldTy::InlineStr => {
                let s = v.as_str().ok_or_else(|| {
                    BeavaError::Protocol(format!(
                        "value_to_row: field '{}' expected string, got {:?}",
                        f.name, v
                    ))
                })?;
                row.write_inline_str(f.offset, schema.inline_str_cap, s);
            }
            FieldTy::String | FieldTy::Bytes => {
                let s = v.as_str().ok_or_else(|| {
                    BeavaError::Protocol(format!(
                        "value_to_row: field '{}' expected string, got {:?}",
                        f.name, v
                    ))
                })?;
                row.write_string(f.offset, s);
            }
        }
    }
    Ok(row)
}

/// Phase 59.6 Wave 3 (TPC-PERF-11): derive a shard_hint from a typed Row
/// by hashing the declared shard_key field's byte representation. Mirrors
/// Phase 48's `shard_hint_for_event` behavior for the Value path —
/// stringifies scalars, reads inline/arena strings directly — so the
/// routing decision is identical across the typed and Value paths.
pub fn shard_hint_from_row(
    row: &Row,
    schema: &RegisteredSchema,
    shard_key_field: &str,
) -> u32 {
    use std::hash::{Hash, Hasher};
    let idx = match schema.field_index(shard_key_field) {
        Some(i) => i,
        None => return 0,
    };
    let f = &schema.fields[idx];
    let key_bytes: Vec<u8> = match f.ty {
        FieldTy::InlineStr => row
            .read_inline_str(f.offset, schema.inline_str_cap)
            .as_bytes()
            .to_vec(),
        FieldTy::String => row.read_string(f.offset).as_bytes().to_vec(),
        FieldTy::I64 => row.read_i64(f.offset).to_string().into_bytes(),
        FieldTy::F64 => row.read_f64(f.offset).to_string().into_bytes(),
        FieldTy::Bool => {
            let v = if row.read_bool(f.offset) { "true" } else { "false" };
            v.as_bytes().to_vec()
        }
        FieldTy::Bytes => row.read_bytes(f.offset).to_vec(),
    };
    let mut h = ahash::AHasher::default();
    key_bytes.hash(&mut h);
    (h.finish() % (u32::MAX as u64)) as u32
}

/// Schema registry — keyed by stream name; value is `Arc<RegisteredSchema>`
/// so operators can clone cheaply.
///
/// Phase 59.6 Wave 1 does not support schema evolution — re-registering a
/// stream with a different schema is destructive (old schema dropped;
/// new `schema_id` assigned). Wave 5+ revisits evolution.
#[derive(Default, Debug)]
pub struct SchemaRegistry {
    by_name: AHashMap<String, Arc<RegisteredSchema>>,
    by_id: AHashMap<SchemaId, Arc<RegisteredSchema>>,
    next_id: SchemaId,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a schema under the given stream name. Assigns the next
    /// monotonic `SchemaId`, overwrites the `name` field with the given
    /// `name`, and returns the assigned id. Overwrites any prior
    /// registration for the same name (destructive — Wave 1 does not
    /// support evolution).
    pub fn insert(&mut self, name: &str, mut schema: RegisteredSchema) -> SchemaId {
        self.next_id = self.next_id.wrapping_add(1);
        schema.schema_id = self.next_id;
        schema.name = name.to_string();
        let arc = Arc::new(schema);
        self.by_name.insert(name.to_string(), arc.clone());
        self.by_id.insert(arc.schema_id, arc);
        self.next_id
    }

    /// Look up a schema by stream name.
    pub fn get(&self, name: &str) -> Option<Arc<RegisteredSchema>> {
        self.by_name.get(name).cloned()
    }

    /// Look up a schema by its assigned id.
    pub fn get_by_id(&self, id: SchemaId) -> Option<Arc<RegisteredSchema>> {
        self.by_id.get(&id).cloned()
    }

    /// Whether a stream has a registered typed schema.
    pub fn is_registered(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// Number of schemas currently registered.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.len() == 0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema(fields: Vec<FieldSpec>, inline_str_cap: u8) -> RegisteredSchema {
        // row_size is the tightest pack = max(offset + width).
        let row_size = fields
            .iter()
            .map(|f| f.offset as u32 + f.ty.fixed_width(inline_str_cap) as u32)
            .max()
            .unwrap_or(0) as u16;
        RegisteredSchema {
            schema_id: 0,
            name: String::new(),
            fields,
            inline_str_cap,
            row_size,
        }
    }

    #[test]
    fn schema_registry_assigns_monotonic_ids() {
        let mut reg = SchemaRegistry::new();
        let s1 = make_schema(
            vec![FieldSpec {
                name: "a".into(),
                ty: FieldTy::I64,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let s2 = make_schema(
            vec![FieldSpec {
                name: "b".into(),
                ty: FieldTy::F64,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let s3 = make_schema(
            vec![FieldSpec {
                name: "c".into(),
                ty: FieldTy::Bool,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let id1 = reg.insert("S1", s1);
        let id2 = reg.insert("S2", s2);
        let id3 = reg.insert("S3", s3);
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
        assert_eq!(reg.len(), 3);
    }

    #[test]
    fn schema_field_offset_lookup() {
        // user_id: InlineStr@0, country_code: InlineStr@16, amount: F64@32.
        // Slot sizes at inline_str_cap=15: 16 (cap+1), 16, 8.
        let schema = make_schema(
            vec![
                FieldSpec {
                    name: "user_id".into(),
                    ty: FieldTy::InlineStr,
                    offset: 0,
                    nullable: false,
                },
                FieldSpec {
                    name: "country_code".into(),
                    ty: FieldTy::InlineStr,
                    offset: 16,
                    nullable: false,
                },
                FieldSpec {
                    name: "amount".into(),
                    ty: FieldTy::F64,
                    offset: 32,
                    nullable: false,
                },
            ],
            15,
        );
        assert_eq!(schema.field_offset("user_id"), Some(0));
        assert_eq!(schema.field_offset("country_code"), Some(16));
        assert_eq!(schema.field_offset("amount"), Some(32));
        assert_eq!(schema.field_offset("missing"), None);
        assert_eq!(schema.row_size, 40);
        schema.validate_layout().expect("layout valid");
    }

    #[test]
    fn row_read_write_i64_roundtrip() {
        let schema = make_schema(
            vec![FieldSpec {
                name: "v".into(),
                ty: FieldTy::I64,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let mut row = Row::zeroed(&schema);
        row.write_i64(0, 42);
        assert_eq!(row.read_i64(0), 42);
        row.write_i64(0, -123456789);
        assert_eq!(row.read_i64(0), -123456789);
    }

    #[test]
    fn row_read_write_inline_str_roundtrip_under_cap() {
        let schema = make_schema(
            vec![FieldSpec {
                name: "s".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let mut row = Row::zeroed(&schema);
        row.write_inline_str(0, 15, "alice");
        assert_eq!(row.read_inline_str(0, 15), "alice");
        // Bytes 5..16 (after the 5-byte payload up to the slot end) must be zero.
        for b in &row.payload[5..16] {
            assert_eq!(*b, 0, "expected zero-padded tail");
        }
    }

    #[test]
    fn row_read_write_inline_str_at_exact_cap() {
        let schema = make_schema(
            vec![FieldSpec {
                name: "s".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let mut row = Row::zeroed(&schema);
        let s = "0123456789ABCDE"; // exactly 15 bytes
        row.write_inline_str(0, 15, s);
        assert_eq!(row.read_inline_str(0, 15), s);
    }

    #[test]
    fn row_write_inline_str_truncates_over_cap() {
        let schema = make_schema(
            vec![FieldSpec {
                name: "s".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let mut row = Row::zeroed(&schema);
        let s = "0123456789ABCDEFGHIJ"; // 20 bytes
        row.write_inline_str(0, 15, s);
        assert_eq!(row.read_inline_str(0, 15), "0123456789ABCDE");
    }

    #[test]
    fn row_arena_string_roundtrip() {
        let schema = make_schema(
            vec![FieldSpec {
                name: "msg".into(),
                ty: FieldTy::String,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let mut row = Row::zeroed(&schema);
        let long = "this is a long string exceeding the inline cap";
        row.write_string(0, long);
        assert_eq!(row.read_string(0), long);
        assert_eq!(row.arena.len(), long.len());
    }

    #[test]
    fn schema_registry_get_by_id_symmetric_with_get_by_name() {
        let mut reg = SchemaRegistry::new();
        let schema = make_schema(
            vec![FieldSpec {
                name: "a".into(),
                ty: FieldTy::I64,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let id = reg.insert("S", schema);
        let by_name = reg.get("S").expect("present");
        let by_id = reg.get_by_id(id).expect("present");
        assert!(Arc::ptr_eq(&by_name, &by_id));
        assert_eq!(by_name.schema_id, id);
        assert_eq!(by_name.name, "S");
        assert!(reg.is_registered("S"));
        assert!(!reg.is_registered("missing"));
    }

    #[test]
    fn row_read_write_f64_and_bool_roundtrip() {
        let schema = make_schema(
            vec![
                FieldSpec {
                    name: "x".into(),
                    ty: FieldTy::F64,
                    offset: 0,
                    nullable: false,
                },
                FieldSpec {
                    name: "flag".into(),
                    ty: FieldTy::Bool,
                    offset: 8,
                    nullable: false,
                },
            ],
            15,
        );
        let mut row = Row::zeroed(&schema);
        row.write_f64(0, 3.14159);
        row.write_bool(8, true);
        assert_eq!(row.read_f64(0), 3.14159);
        assert!(row.read_bool(8));
        row.write_bool(8, false);
        assert!(!row.read_bool(8));
    }

    #[test]
    fn validate_layout_rejects_field_overflow() {
        let schema = RegisteredSchema {
            schema_id: 0,
            name: "S".into(),
            fields: vec![FieldSpec {
                name: "oops".into(),
                ty: FieldTy::I64,
                offset: 4,
                nullable: false,
            }],
            inline_str_cap: 15,
            row_size: 8, // field needs offset+8 = 12, row_size says 8 → overflow
        };
        let err = schema.validate_layout().expect_err("overflow detected");
        match err {
            SchemaValidateError::FieldOverflow { .. } => {}
            other => panic!("expected FieldOverflow, got {other:?}"),
        }
    }

    #[test]
    fn validate_layout_rejects_field_overlap() {
        let schema = RegisteredSchema {
            schema_id: 0,
            name: "S".into(),
            fields: vec![
                FieldSpec {
                    name: "a".into(),
                    ty: FieldTy::I64,
                    offset: 0,
                    nullable: false,
                },
                FieldSpec {
                    name: "b".into(),
                    ty: FieldTy::I64,
                    offset: 4, // overlaps with "a" (0..8)
                    nullable: false,
                },
            ],
            inline_str_cap: 15,
            row_size: 12,
        };
        let err = schema.validate_layout().expect_err("overlap detected");
        match err {
            SchemaValidateError::FieldOverlap { .. } => {}
            other => panic!("expected FieldOverlap, got {other:?}"),
        }
    }

    #[test]
    fn validate_layout_rejects_row_size_mismatch() {
        let schema = RegisteredSchema {
            schema_id: 0,
            name: "S".into(),
            fields: vec![FieldSpec {
                name: "a".into(),
                ty: FieldTy::I64,
                offset: 0,
                nullable: false,
            }],
            inline_str_cap: 15,
            row_size: 16, // should be 8
        };
        let err = schema
            .validate_layout()
            .expect_err("row_size mismatch detected");
        match err {
            SchemaValidateError::RowSizeMismatch {
                declared: 16,
                expected: 8,
            } => {}
            other => panic!("expected RowSizeMismatch(16, 8), got {other:?}"),
        }
    }

    #[test]
    fn field_ty_fixed_width_inline_str_includes_nul() {
        // cap=15 → slot=16. cap=23 → slot=24.
        assert_eq!(FieldTy::InlineStr.fixed_width(15), 16);
        assert_eq!(FieldTy::InlineStr.fixed_width(23), 24);
        assert_eq!(FieldTy::I64.fixed_width(15), 8);
        assert_eq!(FieldTy::F64.fixed_width(15), 8);
        assert_eq!(FieldTy::Bool.fixed_width(15), 1);
        assert_eq!(FieldTy::String.fixed_width(15), 8);
        assert_eq!(FieldTy::Bytes.fixed_width(15), 8);
    }

    #[test]
    fn field_ty_serde_round_trip_snake_case() {
        // Verify serde emits snake_case so the Python emitter and Rust
        // consumer agree on the wire strings.
        let variants = [
            (FieldTy::I64, "\"i64\""),
            (FieldTy::F64, "\"f64\""),
            (FieldTy::Bool, "\"bool\""),
            (FieldTy::InlineStr, "\"inline_str\""),
            (FieldTy::String, "\"string\""),
            (FieldTy::Bytes, "\"bytes\""),
        ];
        for (v, expected) in variants {
            let s = serde_json::to_string(&v).expect("serialize");
            assert_eq!(s, expected, "serialize {v:?}");
            let back: FieldTy = serde_json::from_str(&s).expect("deserialize");
            assert_eq!(back, v);
        }
    }

    #[test]
    fn row_to_value_bridge_renders_all_scalar_types() {
        // Phase 59.6 Wave 2 bridge sanity: renders the expected JSON
        // Value shape so existing operators (still Value-based in Wave 2)
        // see the same object they would have built from OP_PUSH_BATCH.
        let schema = RegisteredSchema {
            schema_id: 1,
            name: "Evt".into(),
            fields: vec![
                FieldSpec { name: "uid".into(), ty: FieldTy::InlineStr, offset: 0, nullable: false },
                FieldSpec { name: "amt".into(), ty: FieldTy::F64, offset: 16, nullable: false },
                FieldSpec { name: "cnt".into(), ty: FieldTy::I64, offset: 24, nullable: false },
                FieldSpec { name: "ok".into(),  ty: FieldTy::Bool, offset: 32, nullable: false },
            ],
            inline_str_cap: 15,
            row_size: 33,
        };
        schema.validate_layout().expect("valid");
        let mut row = Row::zeroed(&schema);
        row.write_inline_str(0, 15, "alice");
        row.write_f64(16, 9.5);
        row.write_i64(24, -7);
        row.write_bool(32, true);
        let v = row_to_value(&row, &schema);
        assert_eq!(v["uid"], "alice");
        assert!((v["amt"].as_f64().unwrap() - 9.5).abs() < 1e-9);
        assert_eq!(v["cnt"], -7);
        assert_eq!(v["ok"], true);
    }

    #[test]
    fn value_to_row_roundtrip_scalars_is_symmetric_with_row_to_value() {
        let schema = RegisteredSchema {
            schema_id: 1,
            name: "Evt".into(),
            fields: vec![
                FieldSpec { name: "uid".into(), ty: FieldTy::InlineStr, offset: 0, nullable: false },
                FieldSpec { name: "amt".into(), ty: FieldTy::F64, offset: 16, nullable: false },
                FieldSpec { name: "cnt".into(), ty: FieldTy::I64, offset: 24, nullable: false },
                FieldSpec { name: "ok".into(),  ty: FieldTy::Bool, offset: 32, nullable: false },
            ],
            inline_str_cap: 15,
            row_size: 33,
        };
        schema.validate_layout().expect("valid");
        let mut row = Row::zeroed(&schema);
        row.write_inline_str(0, 15, "alice");
        row.write_f64(16, 9.5);
        row.write_i64(24, -7);
        row.write_bool(32, true);
        let v = row_to_value(&row, &schema);
        let row2 = value_to_row(&v, &schema).expect("round-trip");
        // Scalar readback parity via field readers (payload bytes may
        // differ in zero-pad of the trailing slot byte, which is fine).
        assert_eq!(row2.read_inline_str(0, 15), "alice");
        assert!((row2.read_f64(16) - 9.5).abs() < 1e-9);
        assert_eq!(row2.read_i64(24), -7);
        assert!(row2.read_bool(32));
    }

    #[test]
    fn value_to_row_roundtrip_long_string() {
        let schema = RegisteredSchema {
            schema_id: 0,
            name: "S".into(),
            fields: vec![FieldSpec {
                name: "msg".into(),
                ty: FieldTy::String,
                offset: 0,
                nullable: false,
            }],
            inline_str_cap: 15,
            row_size: 8,
        };
        schema.validate_layout().expect("valid");
        let long = "this is a long string exceeding the inline cap";
        let v = serde_json::json!({ "msg": long });
        let row = value_to_row(&v, &schema).expect("ok");
        assert_eq!(row.read_string(0), long);
    }

    #[test]
    fn value_to_row_rejects_missing_required_field() {
        let schema = RegisteredSchema {
            schema_id: 0,
            name: "S".into(),
            fields: vec![FieldSpec {
                name: "uid".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            }],
            inline_str_cap: 15,
            row_size: 16,
        };
        let v = serde_json::json!({});
        let err = value_to_row(&v, &schema).expect_err("missing required");
        let msg = format!("{:?}", err);
        assert!(msg.contains("uid"), "unexpected error {msg}");
    }

    #[test]
    fn shard_hint_from_row_matches_string_key_hash() {
        let schema = RegisteredSchema {
            schema_id: 0,
            name: "S".into(),
            fields: vec![FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            }],
            inline_str_cap: 15,
            row_size: 16,
        };
        let mut row = Row::zeroed(&schema);
        row.write_inline_str(0, 15, "alice");
        let h1 = shard_hint_from_row(&row, &schema, "user_id");
        // Same key → same hint (determinism check).
        let h2 = shard_hint_from_row(&row, &schema, "user_id");
        assert_eq!(h1, h2);
        // Different key → (with overwhelming probability) different hint.
        row.write_inline_str(0, 15, "bob");
        let h3 = shard_hint_from_row(&row, &schema, "user_id");
        assert_ne!(h1, h3, "alice and bob should hash differently");
    }

    #[test]
    fn row_arena_bytes_roundtrip() {
        let schema = make_schema(
            vec![FieldSpec {
                name: "raw".into(),
                ty: FieldTy::Bytes,
                offset: 0,
                nullable: false,
            }],
            15,
        );
        let mut row = Row::zeroed(&schema);
        let data: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x42];
        row.write_bytes(0, data);
        assert_eq!(row.read_bytes(0), data);
    }
}
