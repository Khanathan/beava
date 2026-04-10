# Vendored Frontend Assets — Phase 10 Debug UI

These JavaScript libraries are committed to the repository rather than fetched at
runtime so that the Tally binary ships with its debug UI fully self-contained
(DBUI-05). The pinned versions below are embedded at compile time via
`rust-embed` (see `src/server/ui.rs`).

| File | Version | License | Source URL | SHA256 |
|------|---------|---------|------------|--------|
| htmx.min.js | 1.9.12 | BSD-2-Clause | https://unpkg.com/htmx.org@1.9.12/dist/htmx.min.js | 449317ade7881e949510db614991e195c3a099c4c791c24dacec55f9f4a2a452 |
| d3.min.js | 7.8.5 | ISC | https://unpkg.com/d3@7.8.5/dist/d3.min.js | d6b03aefc9f6c44c7bc78713679c78c295028fa914319119e5cc4b4954855b1c |
| dagre-d3.min.js | 0.6.4 | MIT | https://unpkg.com/dagre-d3@0.6.4/dist/dagre-d3.min.js | 74f9b84c0f18f4f639ab99a6b563244463823072432b2df866bc5d6c1180f5cb |

## Why these versions

- **htmx 1.9.12** — last release on the 1.9.x line with the `hx-trigger="every 1s"` polling semantics CONTEXT.md locks. 2.x is a major breaking release; we stay on 1.9.
- **d3 7.8.5** — required peer of dagre-d3 0.6.4 via the `d3.select` / `d3.zoom` APIs the dagre-d3 render function uses. d3 v6+ shares the same selection API; 7.8.5 is the latest minor on the v7 line and is the version RESEARCH §A3 calls out for the mandatory compatibility smoke test.
- **dagre-d3 0.6.4** — last published release. Project is in maintenance mode per GitHub; we pin the final release and vendor rather than depend on future releases that may never ship.

## License compliance

All three libraries are permissive (BSD-2-Clause, ISC, MIT) and allow
redistribution inside a binary with attribution. This file IS the attribution.

## Drift detection

`tests/test_debug_ui.rs` contains automated tests (`static_htmx_is_vendored_and_hashed`,
`static_d3_is_vendored_and_hashed`, `static_dagre_is_vendored_and_hashed`) that
re-hash each file at test time and compare against the entries above. If a
future PR updates a vendored file without updating this manifest, those tests
will fail.
