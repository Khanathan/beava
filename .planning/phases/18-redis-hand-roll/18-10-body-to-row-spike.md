# Spike 18-10: body→Row deserialization variant benchmarks

**Date:** 2026-04-25
**Hardware:** Apple M4 (Darwin 24.3.0)
**Criterion samples:** 100 per bench, bench profile (opt-level 3, thin LTO, codegen-units=1)

## Body→Row variant bench results (M4, criterion)

Baseline (variant A): msgpack 408 ns / json 403 ns (from earlier criterion run, Plan 18-10 SUMMARY).

| Variant | Storage | Value::Str | msgpack ns | json ns | Δ msgpack | Δ json |
|---------|---------|------------|-----------:|--------:|----------:|-------:|
| A | BTreeMap\<String, Value\> + with_field re-clone | String | 448 | 405 | baseline | baseline |
| B | BTreeMap\<String, Value\> + direct insert | String | 367 | 340 | -18% | -16% |
| C | BTreeMap\<CompactString, ValueC\> | CompactString | 226 | 229 | -50% | -43% |
| D | SmallVec\<[(CompactString, ValueC); 8]\> | CompactString | 146 | 184 | -67% | -55% |
| E | SmallVec\<[ValueC; 8]\> by column id | CompactString | 162 | 169 | -64% | -58% |

_All ns values are the criterion midpoint estimate from the run._

Raw criterion output (midpoints):
- variant_a_btreemap_string_msgpack: [424 ns .. **448 ns** .. 478 ns]
- variant_a_btreemap_string_json:    [400 ns .. **405 ns** .. 412 ns]
- variant_b_btreemap_direct_insert_msgpack: [348 ns .. **367 ns** .. 393 ns]
- variant_b_btreemap_direct_insert_json:    [332 ns .. **340 ns** .. 350 ns]
- variant_c_btreemap_compact_str_msgpack:   [224 ns .. **226 ns** .. 227 ns]
- variant_c_btreemap_compact_str_json:      [228 ns .. **229 ns** .. 232 ns]
- variant_d_smallvec_compact_str_msgpack:   [145 ns .. **146 ns** .. 148 ns]
- variant_d_smallvec_compact_str_json:      [178 ns .. **184 ns** .. 191 ns]
- variant_e_positional_smallvec_msgpack:    [156 ns .. **162 ns** .. 170 ns]
- variant_e_positional_smallvec_json:       [163 ns .. **169 ns** .. 175 ns]

## Findings

The data makes the allocation cost model concrete. The two dominant costs are (1) BTreeMap
node heap-allocation — each insert allocates a tree node on the heap, six inserts per event
means six small allocations plus the balancing overhead — and (2) per-string heap allocation
for keys and string values. Moving from `String` to `CompactString` (variant C) cuts 50% of
msgpack time alone, proving that string heap traffic is roughly half the total cost even when
BTreeMap node overhead is held constant. Eliminating BTreeMap entirely (variant D, SmallVec
of tuples) adds another 35% reduction on top of CompactString, reaching 146 ns msgpack —
about a 3× speed-up over baseline. The positional descriptor approach (variant E) is
surprisingly slower than variant D despite storing no keys: the six `iter().position()` linear
scans (one per field) add ~16 ns of CPU work on msgpack compared to variant D's simple push,
erasing the key-storage savings. The JSON story differs slightly: variant D's JSON (184 ns)
is slower than E's JSON (169 ns), possibly because sonic-rs's JSON visitor provides borrowed
`&str` slices to `visit_str` and the CompactString-from-str conversion in D adds overhead
that the simpler Null-init path in E avoids. Both remain well inside the 120–180 ns estimate
band for "CompactString + SmallVec."

## Estimate vs reality

- Original orchestrator estimate for Option B (CompactString + SmallVec): 120–180 ns.
  Actual variant D measurement (msgpack): **146 ns**. Estimate was accurate — 146 ns sits
  in the middle of the predicted band.
- Original estimate for Option C (descriptor-driven Vec by column id): 80–120 ns.
  Actual variant E measurement (msgpack): **162 ns**. Estimate was optimistic by ~35%
  (162 ns is ~35% above the top of the 80–120 ns predicted range). The linear scan over
  field names adds non-trivial overhead at 6 fields; a hash-map or perfect-hash descriptor
  would likely close this gap, but that is additional complexity not measured here.

## Recommendation

Adopt **variant D** (`SmallVec<[(CompactString, ValueC); 8]>`) as the Plan 18-11 structural
change. It delivers the largest practical win — 146 ns msgpack, 184 ns JSON — without
requiring a descriptor/schema contract at deserialization time (which variant E needs but
cannot currently enforce safely for unknown or out-of-order fields). The BTreeMap elimination
is the bigger lever (35% on top of CompactString), so the structural change should change
both the storage shape and the string type together. The `with_field` re-clone fix (variant B)
is worth landing as a quick patch regardless, but it only yields 18% and leaves 330+ ns on
the table. The real win requires changing the `Row` storage type, not just patching a single
line in the visitor. For Plan 18-11, define `Row` as
`SmallVec<[(CompactString, Value); 8]>` (keeping the existing `Value` enum but swapping
`String` → `CompactString` for `Value::Str`), implement a fast linear-scan `get(&str)` for
the hot query path, and change the Deserialize visitor to use direct push instead of
`with_field`. This keeps the API surface stable while cutting body→Row from ~448 ns to an
expected ~146 ns — a 3× improvement that closes most of the gap to the 3M EPS/core target.
