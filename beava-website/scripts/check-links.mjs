// scripts/check-links.mjs
//
// Phase 13.7 — internal-link audit for the rendered docs tree.
//
// Walks beava-website/project/docs/, extracts every <a href="..."> in the
// rendered HTML, classifies each, reports BROKEN / WARN / OK / EXTERNAL_SKIP,
// exits non-zero if any BROKEN entries are found.
//
// Classifications:
//   OK             — internal /docs/.../ that resolves to an index.html on disk
//                   OR /styles/... / /_pagefind/... / /assets/... that exists
//   BROKEN         — internal /docs/... that does NOT resolve
//   WARN           — cross-repo (.planning/..., examples/..., crates/...) — kept; opens in github
//                   OR external GitHub blob URL we generated
//   EXTERNAL_SKIP  — http(s):// not in our domain
//   ANCHOR_SKIP    — pure #fragment

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '../..');
const SITE_ROOT = path.join(REPO_ROOT, 'beava-website/project');
const DOCS_OUT = path.join(SITE_ROOT, 'docs');

function walk(dir, acc = []) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walk(p, acc);
    else if (e.isFile() && p.endsWith('.html')) acc.push(p);
  }
  return acc;
}

const HREF_RE = /\bhref\s*=\s*"([^"]+)"/g;

function classifyAndCheck(href, fromAbs) {
  if (!href) return { status: 'WARN', reason: 'empty-href' };
  if (href.startsWith('#')) return { status: 'ANCHOR_SKIP' };
  if (href.startsWith('mailto:') || href.startsWith('tel:')) return { status: 'EXTERNAL_SKIP' };
  if (/^https?:\/\//.test(href)) {
    // external — note GitHub blob URLs as WARN (we generated them)
    if (href.startsWith('https://github.com/beava-dev/beava/blob/')) {
      return { status: 'WARN', reason: 'cross-repo-github' };
    }
    return { status: 'EXTERNAL_SKIP' };
  }

  // Strip query + fragment
  const noFrag = href.replace(/#.*$/, '').replace(/\?.*$/, '');
  if (!noFrag.startsWith('/')) {
    // Relative path — resolve against fromAbs's parent
    return { status: 'WARN', reason: 'unexpected-relative', target: noFrag };
  }

  // Absolute site-rooted path
  // Map /foo/bar/ -> SITE_ROOT/foo/bar/index.html
  // Map /foo/bar  -> SITE_ROOT/foo/bar (file) OR /foo/bar/index.html
  let candidatePaths;
  if (noFrag.endsWith('/')) {
    candidatePaths = [path.join(SITE_ROOT, noFrag, 'index.html')];
  } else {
    candidatePaths = [
      path.join(SITE_ROOT, noFrag),
      path.join(SITE_ROOT, noFrag, 'index.html'),
      path.join(SITE_ROOT, noFrag + '.html'),
    ];
  }
  for (const cp of candidatePaths) {
    if (fs.existsSync(cp)) return { status: 'OK', target: cp };
  }
  return { status: 'BROKEN', reason: 'not-found-on-disk', target: noFrag, tried: candidatePaths };
}

const files = walk(DOCS_OUT);
const counts = { OK: 0, BROKEN: 0, WARN: 0, EXTERNAL_SKIP: 0, ANCHOR_SKIP: 0 };
const broken = [];
const warns = [];

for (const f of files) {
  const html = fs.readFileSync(f, 'utf8');
  let m;
  while ((m = HREF_RE.exec(html)) !== null) {
    const href = m[1];
    const r = classifyAndCheck(href, f);
    counts[r.status] = (counts[r.status] || 0) + 1;
    if (r.status === 'BROKEN') {
      const fromRel = path.relative(REPO_ROOT, f);
      broken.push(`${fromRel} -> ${href}`);
    } else if (r.status === 'WARN') {
      const fromRel = path.relative(REPO_ROOT, f);
      warns.push(`${fromRel} -> ${href} (${r.reason})`);
    }
  }
}

console.log(`check-links scanned ${files.length} html files`);
console.log(`OK=${counts.OK} BROKEN=${counts.BROKEN} WARN=${counts.WARN} EXTERNAL=${counts.EXTERNAL_SKIP} ANCHOR=${counts.ANCHOR_SKIP}`);
console.log(`BROKEN_COUNT=${counts.BROKEN} WARN_COUNT=${counts.WARN}`);

if (counts.BROKEN > 0) {
  console.error('\nBROKEN links:');
  for (const b of broken.slice(0, 50)) console.error('  ' + b);
  if (broken.length > 50) console.error(`  ... and ${broken.length - 50} more`);
  process.exit(1);
}
