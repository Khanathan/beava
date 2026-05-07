// scripts/scrub-slop.mjs
//
// Strip internal-planning artifacts from docs/*.md so user-facing prose
// reads like project documentation, not a status update. Regex-only —
// review the output before committing.
//
// Patterns:
//   - "(per [ADR-NNN](path/to/ADR.md))"            → drop
//   - "[ADR-NNN](path)"                            → "ADR-NNN" (plain)
//   - "*(renamed from `bv.X` per ADR-002)*"        → drop
//   - "(per ADR-NNN)" / "Per [ADR-NNN]..."         → drop
//   - "Phase NN.N" / "Phase NN.N (description)"    → drop / rephrase
//   - "Plan NN.N-NN"                               → drop
//   - "V0-MEM-GOV-NN"                              → drop
//   - "(verified Phase NN.NN YYYY-MM-DD)"          → drop
//   - "locked 2026-MM-DD"                          → drop
//   - "(per `project_*`)"                          → drop
//   - "CI tripwire enforced by ..."                → drop
//   - "(authored by Plan NN.N-NN)"                 → drop
//   - "Active phase: ... " full paragraph          → drop
//
// Usage:  node scripts/scrub-slop.mjs [--dry-run] [path/to/file.md ...]

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '../..');
const DOCS_SRC = path.join(REPO_ROOT, 'docs');

const args = process.argv.slice(2);
const dryRun = args.includes('--dry-run');
const explicitFiles = args.filter(a => !a.startsWith('--'));

const RULES = [
  // ────────────────── Round 2: agent-flagged residue ──────────────────

  // Capital "Beava" → lowercase "beava" (in prose only — the regex is
  // word-bounded; will hit code spans too which is rare and harmless since
  // beava the python module is lowercase as well).
  { name: 'capital-beava',
    re: /\bBeava\b/g,
    sub: 'beava' },

  // Decision IDs: D-01, D-02, D-05a, D-05b ...
  { name: 'd-decision-id-paren',
    re: /\s*\(per D-\d+[a-z]?\)/g,
    sub: '' },
  { name: 'd-decision-id-bare',
    re: /\bper D-\d+[a-z]?\b/g,
    sub: '' },
  { name: 'd-decision-id-inline',
    re: /\s*\bD-\d+[a-z]?\s*(?:per|locked|fast-path|decision)?/g,
    sub: '' },

  // Q-path decisions: Q1 Path B, Q2 Path A, etc.
  { name: 'q-path-link',
    re: /\(?per \[Q\d+ Path [A-Z]\]\([^)]*\)\)?/g,
    sub: '' },
  { name: 'q-path-bare',
    re: /\bper Q\d+ Path [A-Z]\b/g,
    sub: '' },
  { name: 'q-path-inline',
    re: /\bQ\d+ Path [A-Z]\b/g,
    sub: '' },

  // Threat-model ticket IDs: T-03-02-01, T-NN-NN-NN
  { name: 't-ticket-id',
    re: /\s*\(T-\d{2}-\d{2}-\d{2}\)/g,
    sub: '' },
  { name: 't-ticket-id-bare',
    re: /\bT-\d{2}-\d{2}-\d{2}\b/g,
    sub: '' },

  // REQ-ID / USER-LOCKED / PLANNER-SURFACED CONCERN
  { name: 'req-id-bare',
    re: /\bREQ-[A-Z\d-]+\b/g,
    sub: '' },
  { name: 'user-locked-bare',
    re: /\(?USER-LOCKED\)?/g,
    sub: '' },
  { name: 'planner-surfaced-concern',
    re: /\s*PLANNER-SURFACED CONCERN\s*\d*/g,
    sub: '' },

  // Env-var leak: BEAVA_MEMORY_GOV_ENFORCE
  { name: 'memory-gov-env',
    re: /BEAVA_MEMORY_GOV_ENFORCE(?:=\d+)?/g,
    sub: '' },

  // Memory-file slugs: project_v0_events_only_scope, project_no_sharded_apply,
  // project_phase18_*, project_redis_shaped_*, project_v2_devex_first, etc.
  // Drop wrapping framing first (so we don't leave empty backticks behind),
  // then catch link / code-span / bare forms.
  { name: 'per-project-memory-multi',
    re: /\s*\(per project memory[\s\S]*?\)/g,
    sub: '' },
  { name: 'project-slug-link',
    re: /\[`project_[\w-]+`\]\([^)]*\)/g,
    sub: '' },
  { name: 'project-slug-paren',
    re: /\s*\(per `?project_[\w-]+`?\)/g,
    sub: '' },
  { name: 'project-slug-per',
    re: /\bper `?project_[\w-]+`?,?\s*/g,
    sub: '' },
  { name: 'project-slug-bare',
    re: /\bproject_[a-z][\w-]+\b/g,
    sub: '' },

  // Source-path leaks: crates/beava-core/...
  { name: 'crate-source-path-rs',
    re: /\s*\(?\s*crates\/[\w/-]+\.rs(?:::[a-z_]+)?(?:\s*\(~?line\s+\d+(?:[-–]\d+)?\))?\)?/g,
    sub: '' },
  { name: 'crate-source-path-toml',
    re: /\bcrates\/[\w/-]+\.toml\b/g,
    sub: '' },
  { name: 'python-source-path',
    re: /\s*\(?\s*python\/beava\/[\w/.-]+\.py(?:\s*\(~?line\s+\d+\))?\)?/g,
    sub: '' },

  // .planning/ links: drop the link, keep the text. Handles forms like
  //   [foo](.planning/...) or [foo](../.planning/...) or [foo](../../.planning/...)
  { name: 'planning-link',
    re: /\[([^\]]+)\]\((?:\.\.?\/)*\.planning\/[^)]+\)/g,
    sub: '$1' },
  // Bare ../planning/ paths in prose
  { name: 'planning-path-bare',
    re: /\s*(?:\.\.?\/)*\.planning\/[\w/.-]+/g,
    sub: '' },

  // ~/.claude/... memory paths
  { name: 'claude-memory-path',
    re: /\s*~?\/?\.?[Cc]laude\/projects\/[^\s)]+\.md/g,
    sub: '' },

  // CLAUDE.md links: drop link wrapper, keep text
  { name: 'claude-md-link',
    re: /\[([^\]]+)\]\((?:\.\.?\/)*CLAUDE\.md(?:#[^)]*)?\)/g,
    sub: '$1' },

  // Status banners (blockquote starting with "Status:")
  { name: 'status-banner-line',
    re: /^>\s*\*\*Status:\*\*[^\n]*\n?/gm,
    sub: '' },
  { name: 'status-callout-block',
    re: /^>\s*\*\*Status\b[^\n]*\n(?:>[^\n]*\n)+/gm,
    sub: '' },

  // "Last reviewed: YYYY-MM-DD" lines
  { name: 'last-reviewed-line',
    re: /^[>*\s]*\*?\*?Last reviewed:?\*?\*?\s*\d{4}-\d{2}-\d{2}[^\n]*\n?/gm,
    sub: '' },

  // "post-13.X target" / "post-13.X" mentions
  { name: 'post-version-target',
    re: /\bpost-13\.\d+(?:\s+target)?\b/g,
    sub: '' },

  // "(this phase)" / "(this Phase)"
  { name: 'this-phase-paren',
    re: /\s*\(this [Pp]hase\)/g,
    sub: '' },

  // "(forward-ref ...)" placeholders left when refs got stripped
  { name: 'forward-ref-paren',
    re: /\s*\(forward-ref[^)]*\)/g,
    sub: '' },

  // Hanging "per" tail-ends from round 1 stripping
  { name: 'hanging-per-bracket',
    re: /\bper \[\s*\]\([^)]*\)/g,
    sub: '' },
  { name: 'hanging-per-period',
    re: /\bper\s*\.(?=\s|$)/g,
    sub: '.' },
  { name: 'hanging-per-semi',
    re: /\bper\s*;/g,
    sub: ';' },
  { name: 'hanging-per-from',
    re: /\bper from\b/g,
    sub: '' },
  { name: 'hanging-per-comma',
    re: /\bper\s*,/g,
    sub: ',' },
  { name: 'hanging-per-eol',
    re: /\bper\s*$/gm,
    sub: '' },

  // Section heading: V0-MEM-GOV-N contract → "the memory contract"
  { name: 'v0-mem-gov-heading',
    re: /^#+\s*V0-[A-Z-]+-\d+(?:\/\d+)*\s+contract\s*$/gm,
    sub: '## the memory contract' },

  // "Phase N /" prefix in table cells (cost-class.md repeats this 50 times).
  // Strip the phase prefix; keep the family name.
  { name: 'phase-slash-prefix',
    re: /\bPhase\s+\d+\s*\/\s*/g,
    sub: '' },

  // "(renamed in ADR-NNN, -NN)" / "(renamed in ADR-NNN)" parens
  { name: 'renamed-in-adr-paren',
    re: /\s*\(renamed in ADR-\d+(?:,?\s*-?\d+)?\)/g,
    sub: '' },

  // "in Phase NN" / "since Phase NN" / "for Phase NN" inline tail-modifiers
  { name: 'inline-phase-modifier',
    re: /\b(?:in|since|for|after|before|during|under)\s+Phase\s+\d+\.\d+(?:\.\d+)?\b/g,
    sub: '' },

  // Bare "ADR-NNN" mention (no link wrapper, no "per/[" prefix)
  // Drop "Per ADR-NNN, " sentence starts, and inline ", per ADR-NNN".
  { name: 'sentence-start-per-adr',
    re: /^Per ADR-\d+,?\s*/gm,
    sub: '' },
  { name: 'inline-comma-per-adr',
    re: /,?\s*per ADR-\d+\b/g,
    sub: '' },
  { name: 'parenthetical-per-adr',
    re: /\s*\(per ADR-\d+\)/g,
    sub: '' },
  { name: 'allowed-per-adr',
    re: /\bALLOWED\s+(?:\+\s+RECOMMENDED\s+)?(?:for [^|]+? )?per ADR-\d+\b/g,
    sub: 'ALLOWED' },
  { name: 'bare-adr-ref',
    re: /\bADR-\d+\b/g,
    sub: '' },

  // Phase NN (no decimal): "Phase 2", "Phase 18-09", "Phase 18", etc.
  { name: 'phase-dash-numeric',
    re: /\bPhase\s+\d+(?:-\d+)?\b/g,
    sub: '' },

  // Plan NN (no decimal): "Plan 06", "Plan 03"
  { name: 'plan-no-decimal',
    re: /\bPlan\s+\d{2,}\b/g,
    sub: '' },
  // ", per Plan NN" → drop
  { name: 'comma-per-plan',
    re: /,?\s*per Plan\s+\d+(?:[-.]\d+)?/g,
    sub: '' },

  // V0-MEM-GOV (without trailing -NN): "V0-MEM-GOV invariants"
  { name: 'v0-mem-gov-bare',
    re: /\bV0-[A-Z]+-[A-Z]+\b/g,
    sub: '' },

  // "(locked )" / "(LOCKED )" with empty content
  { name: 'locked-empty-paren',
    re: /\s*\(locked\s*\)/gi,
    sub: '' },

  // Sentence trailing "Per:" or "Per ." or starting "Per " at line start with nothing after
  { name: 'orphan-per-trailing',
    re: /\bPer\s*[.:;]\s*$/gm,
    sub: '' },

  // "**Authored by Wave 1**" footer line
  { name: 'authored-by-footer',
    re: /\*\*Authored by\b[^\n]*\n?/g,
    sub: '' },

  // "## Plan-level traceability" full section to EOF or next H1/H2
  { name: 'plan-traceability-section',
    re: /^##\s+Plan-level traceability[\s\S]*?(?=^##? |\Z)/gm,
    sub: '' },

  // "## See also" sections that are entirely .planning/ links — kill them.
  // Conservative: only when next 5 lines look like .planning links.
  // Skip — too aggressive; round 1's planning-link rule already strips inner
  // links so the section becomes plain text. Manual cleanup if needed.

  // "no actually" type editorial-slip prose — extremely specific
  { name: 'no-actually-slip',
    re: /\s*no actually\s+/gi,
    sub: ' ' },

  // Apostrophe-orphans from earlier scrubs: line starting with "'s X"
  { name: 'apostrophe-orphan',
    re: /\b's\s+(?=lockless|RefCell|fast-path)/g,
    sub: '' },

  // ────────────────── Round 1: original mechanical scrubs ──────────────────

  // Drop "(verified Phase NN.NN YYYY-MM-DD; ...)" parens
  { name: 'verified-phase-stamp',
    re: /\s*\(verified Phase\s+\d+\.\d+(?:\.\d+)?(?:[^()]*?\d{4}-\d{2}-\d{2})?(?:[^()]*?ship-pitch numbers)?\)/g,
    sub: '' },
  // Drop "(per [ADR-NNN](path))"
  { name: 'per-adr-link-paren',
    re: /\s*\(per \[ADR-\d+\]\([^)]*\)\)/g,
    sub: '' },
  // Drop "(per ADR-NNN)"
  { name: 'per-adr-paren',
    re: /\s*\(per ADR-\d+\)/g,
    sub: '' },
  // Drop "*(renamed from `bv.X` per ADR-NNN)*"
  { name: 'renamed-from-italic',
    re: /\s*\*\(renamed from\s+`?bv\.\w+`?\s+per ADR-\d+\)\*/g,
    sub: '' },
  // Inline "Per [ADR-NNN](...)" or "Per ADR-NNN," at sentence start → drop
  { name: 'per-adr-sentence-start',
    re: /Per \[ADR-\d+\]\([^)]*\),?\s+/g,
    sub: '' },
  // ", per [ADR-NNN](...)" in mid-sentence → drop just the reference
  { name: 'comma-per-adr-link',
    re: /,\s*per \[ADR-\d+\]\([^)]*\)/g,
    sub: '' },
  // ", per ADR-NNN" in mid-sentence → drop
  { name: 'comma-per-adr',
    re: /,\s*per ADR-\d+/g,
    sub: '' },
  // "[ADR-NNN](path)" inline → drop entire reference (the linked text + link)
  { name: 'adr-link-inline',
    re: /\[ADR-\d+\]\([^)]*\)/g,
    sub: '' },
  // Drop "(per `project_*`)" parens
  { name: 'per-project-memory',
    re: /\s*\(per `project_[\w-]+`\)/g,
    sub: '' },
  // Drop "(per <code>project_*</code>)" parens
  { name: 'per-project-memory-html',
    re: /\s*\(per <code>project_[\w-]+<\/code>\)/g,
    sub: '' },
  // Drop "(authored by Plan NN.N-NN)"
  { name: 'authored-by-plan',
    re: /\s*\(authored by Plan\s+\d+\.\d+(?:-\d+)?\)/g,
    sub: '' },
  // Drop "(forthcoming, Plan NN.N-NN)"
  { name: 'forthcoming-plan',
    re: /\s*\(forthcoming,\s*Plan\s+\d+\.\d+(?:-\d+)?\)/g,
    sub: '' },
  // Drop "Phase NN.N (parenthetical)" — soft form, dash-led: " — Phase 19.2"
  { name: 'em-dash-phase',
    re: /\s+—\s+Phase\s+\d+\.\d+(?:\.\d+)?\b[^.\n]*/g,
    sub: '' },
  // Drop "(Phase NN.N ...)"
  { name: 'phase-paren',
    re: /\s*\(Phase\s+\d+\.\d+(?:\.\d+)?[^()]*\)/g,
    sub: '' },
  // Drop "alive Phase NN.N metadata" / "alive Phase NN.N" — awkward source phrasing
  { name: 'alive-phase',
    re: /\s*—?\s*alive Phase\s+\d+\.\d+(?:\.\d+)?\b\s*(metadata)?/g,
    sub: '' },
  // Drop standalone "Phase NN.N(.NN)" mentions outright
  { name: 'standalone-phase',
    re: /\bPhase\s+\d+\.\d+(?:\.\d+)?\b/g,
    sub: '' },
  // Drop V0-MEM-GOV-NN refs (e.g., "(per V0-MEM-GOV-01)" or inline "V0-MEM-GOV-02")
  { name: 'v0-mem-gov-paren',
    re: /\s*\(per V0-[A-Z-]+-\d+(?:\/\d+)*\)/g,
    sub: '' },
  { name: 'v0-mem-gov-inline',
    re: /\bV0-[A-Z-]+-\d+(?:\/\d+)*\b/g,
    sub: '' },
  // Drop "Plan NN.N-NN" refs
  { name: 'plan-ref',
    re: /\bPlan\s+\d+\.\d+(?:-\d+)?\b/g,
    sub: '' },
  // Drop "locked YYYY-MM-DD" - keep just the surrounding text
  { name: 'locked-date',
    re: /\s*\(locked\s+\d{4}-\d{2}-\d{2}[^()]*\)/g,
    sub: '' },
  // Drop "CI tripwire enforced by `crates/...`" sentences
  { name: 'ci-tripwire',
    re: /CI tripwire enforced by [`'][^`']+[`'][^.\n]*\.?/g,
    sub: '' },
  // Drop "Active phase:" lines (often whole paragraphs)
  { name: 'active-phase-line',
    re: /^Active phase:[^\n]*\n?/gm,
    sub: '' },
  // Drop "GA tag" mentions in versioning context
  { name: 'ga-tag',
    re: /\bGA tag\b/g,
    sub: 'release' },
  // Drop "pre-12.X" / "pre-Phase-NN.N"
  { name: 'pre-phase',
    re: /\bpre-(?:Phase-)?\d+\.\d+(?:\.\d+)?\b/g,
    sub: 'previously' },
  // Drop "post-Phase-NN.N"
  { name: 'post-phase',
    re: /\bpost-Phase-\d+\.\d+(?:\.\d+)?\b/g,
    sub: 'recent' },
];

function scrub(src) {
  let out = src;
  const counts = {};
  for (const r of RULES) {
    const before = out;
    out = out.replace(r.re, r.sub);
    const hits = (before.match(r.re) || []).length;
    if (hits) counts[r.name] = hits;
  }
  // Cleanup pass — fixes artifacts created by the substitutions above.
  // Empty backtick pairs `` left when slugs got dropped — but DO NOT touch
  // triple-backtick code fences. Lookbehind/lookahead enforce the pair is
  // exactly two backticks, not adjacent to a third.
  out = out.replace(/(?<!`)``\s*\+\s*``(?!`)/g, '');
  out = out.replace(/(?<!`)``\s*,\s*``(?!`)/g, '');
  out = out.replace(/\((?<!`)``(?!`)\)/g, '');
  out = out.replace(/\(\s*(?<!`)``(?!`)\s*\)/g, '');
  out = out.replace(/(?<!`)``(?!`)/g, '');
  // NOTE: We deliberately do NOT strip empty () or [] — those are common in
  // code (`app.reset()`, `arr[]`) and false positives are catastrophic. A few
  // stray empties left by the drops above are acceptable.
  // " + " floating with nothing on one side (only outside code)
  out = out.replace(/\s+\+\s+\)/g, ')');
  out = out.replace(/\(\s+\+\s+/g, '(');
  // Trailing whitespace on lines
  out = out.replace(/[ \t]+\n/g, '\n');
  // Collapse 3+ newlines to 2
  out = out.replace(/\n{3,}/g, '\n\n');
  // Collapse double spaces
  out = out.replace(/  +/g, ' ');
  // Fix " ." / " ," artifacts left by paren drops — but only outside code
  // fences. Skip the cleanup if we're not certain we're outside a fence.
  // Conservative: apply only when the preceding char isn't a newline (so
  // we don't munge indented code) and the punctuation isn't followed by
  // more whitespace-then-code-continuation.
  out = out.replace(/(\S)\s+([.,;:])/g, '$1$2');
  return { out, counts };
}

function walkMd(dir, acc = []) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) walkMd(p, acc);
    else if (e.isFile() && p.endsWith('.md')) acc.push(p);
  }
  return acc;
}

const files = explicitFiles.length
  ? explicitFiles.map(f => path.resolve(f))
  : walkMd(DOCS_SRC);

let totalHits = 0;
let touched = 0;
const summary = {};

for (const f of files) {
  const src = fs.readFileSync(f, 'utf8');
  const { out, counts } = scrub(src);
  const hits = Object.values(counts).reduce((a, b) => a + b, 0);
  if (hits === 0) continue;
  totalHits += hits;
  touched++;
  for (const [k, v] of Object.entries(counts)) {
    summary[k] = (summary[k] || 0) + v;
  }
  if (!dryRun) fs.writeFileSync(f, out);
  const rel = path.relative(REPO_ROOT, f);
  console.log(`${dryRun ? '[dry-run]' : '[wrote]   '} ${rel}  (-${hits})`);
}

console.log('');
console.log(`Touched ${touched}/${files.length} files, ${totalHits} substitutions.`);
console.log('Per-rule breakdown:');
for (const [k, v] of Object.entries(summary).sort((a, b) => b[1] - a[1])) {
  console.log(`  ${v.toString().padStart(4)}  ${k}`);
}
