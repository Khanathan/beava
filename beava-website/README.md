# beava-website

The beava.dev docs site. Phase 13.7 integrated Phase 13.0's spec docs (`docs/*.md` in the repo root) into this hand-rolled static site, with Pagefind search and Cloudflare Pages auto-deploy.

## Local development

```bash
cd beava-website
npm install
npm run build       # renders docs/*.md → project/docs/<...>/index.html + builds Pagefind index
npm run check:links # post-build link audit (must exit 0 before pushing)
```

To preview locally:

```bash
python3 -m http.server 8000 --directory project
# open http://localhost:8000/
```

## Build pipeline

```
docs/*.md                                              (canonical source — also used by SDK + CI)
   |
   v  scripts/render-docs.mjs (markdown-it + markdown-it-anchor)
   |
project/docs/<route>/index.html                        (server-rendered, static HTML)
   |
   v  scripts/build-search.mjs (pagefind 1.5)
   |
project/_pagefind/                                     (search index, served as static)
   |
   v  Cloudflare Pages auto-deploy on push
   |
beava.dev
```

- **`scripts/render-docs.mjs`** — Markdown→HTML converter. Reads `scripts/render-docs-config.json` for sidebar IA + skip list; emits server-rendered HTML matching the existing `colors_and_type.css` + `site.css` design tokens. Idempotent (re-running produces zero git diffs). (Phase 13.7 Plan 01.)
- **`scripts/build-search.mjs`** — Pagefind 1.5 index builder. Crawls `project/docs/**/*.html` via `addDirectory` + adds curated entries for the legacy React+Babel pages (home, guide, field-guide, design-system) via `addCustomRecord`. (Phase 13.7 Plan 02.)
- **`scripts/check-links.mjs`** — Internal-link audit. Walks `project/docs/`, classifies every `<a href>` as OK / BROKEN / WARN / EXTERNAL_SKIP / ANCHOR_SKIP. Exits non-zero on BROKEN. Cross-repo links to `.planning/...` / `examples/...` / `crates/...` / `python/...` are emitted as `https://github.com/beava-dev/beava/blob/main/...` and reported as WARN. (Phase 13.7 Plan 02.)

## Deploy (Cloudflare Pages)

Auto-deploy is configured via `wrangler.toml` + Cloudflare's dashboard. One-time setup (run by the user, not Claude):

1. Log in to https://dash.cloudflare.com → Pages → "Create a project" → "Connect to Git"
2. Authorize the GitHub org owning `tally` (likely `beava-dev`)
3. Select the `tally` repository
4. Build settings (Cloudflare auto-detects from `wrangler.toml`):
   - Production branch: `v2/greenfield` (then `main` post-merge)
   - Build command: `npm install && npm run build`
   - Build output directory: `project`
   - Root directory: `beava-website`
5. Click "Save and Deploy" — first build takes ~2-3 min.
6. Once green: visit the auto-generated `<pages-project>.pages.dev` URL and smoke-test:
   - `/` — home loads
   - `/docs/` — docs landing with the new sidebar
   - `/docs/quickstart/` — quickstart with the SVG demo (Plan 03)
   - `/docs/operators/core/count/` — a representative op page
   - Search box — type "histogram" → expect a hit at `/docs/operators/buffer-geo/histogram/`
7. Add custom domain: Pages project → Custom domains → set up `beava.dev` + `www.beava.dev`.
8. DNS (Cloudflare DNS for beava.dev — same account if already there; else manual):
   - CNAME `beava.dev` → `<pages-project>.pages.dev` (proxied)
   - CNAME `www.beava.dev` → `<pages-project>.pages.dev` (proxied)
9. Wait ~5 min for SSL provisioning, then visit beava.dev.

After setup: every push to `v2/greenfield` auto-deploys. PR branches get preview URLs at `<branch>.<pages-project>.pages.dev`.

## Phase 13.7 scope (locked)

Per `.planning/phases/13.7-docs-site-beava-dev/13.7-CONTEXT.md`:

- Integrate Phase 13.0 spec docs into existing site (NOT new MkDocs spin-up) — D-01
- Reuse `beava-design-system/` tokens — D-02
- /guide/ vertical pages stay as "coming soon" stubs — D-03
- Cloudflare Pages auto-deploy — D-04

Vertical guides (adtech / fraud / ecommerce) deferred to v0.1+ user-authored interactive follow-up.

## Original handoff bundle

This README replaces a Claude Design (claude.ai/design) handoff README. The original chat transcripts that informed the home page + guide design live under `chats/`. The hand-rolled `project/index.html`, `project/field-guide-ch{1,2}.html`, and `project/guide/` pages are the design implementation; `project/docs/` is Phase 13.7's docs integration.
