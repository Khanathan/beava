# Phase 13.7.6-09 trailer-strip commit_callback per D-02 (trailer-only surgical strip).
#
# Used as the body of `git filter-repo --commit-callback "$(cat 13.7.6-09-trailer-strip.py)"`.
# Per filter-repo convention this file is the BODY of the commit_callback (not a `def`); the
# tool wraps it in `def commit_callback(commit, _meta):` internally before exec'ing.
# filter-repo executes this body against the variable `commit`, whose `.message`
# attribute is BYTES. We:
#   1. decode bytes → str (utf-8)
#   2. operate on str (line-by-line)
#   3. encode str → bytes (utf-8) and assign back to commit.message
#
# Removes:
#   - Co-Authored-By: ... lines whose value mentions Claude or noreply@anthropic.com
#   - "🤖 Generated with [Claude Code]..." lines (and the bare "Generated with [Claude Code]" variant)
#   - Orphaned trailing blank lines created by the above removals
#
# Preserves:
#   - Every other line of the message (subjects mentioning Claude survive — D-02 rationale)
#   - Other Co-Authored-By trailers that don't reference Claude/Anthropic (T-13.7.6-09-03 mitigation)
#
# Bytes/str discipline (W2-warning fix from iter-1 plan-checker review): the only
# bytes ↔ str boundary is the explicit decode/encode pair below; every intermediate
# operation runs on str.

msg = commit.message.decode('utf-8')
lines = msg.splitlines()
out = []
for line in lines:
    stripped = line.strip()
    stripped_lower = stripped.lower()
    # Drop Co-Authored-By trailers that mention Claude / Anthropic noreply.
    if stripped_lower.startswith('co-authored-by:') and (
        'claude' in stripped_lower
        or 'noreply@anthropic.com' in stripped_lower
    ):
        continue
    # Drop the "🤖 Generated with [Claude Code]" markers and the bare-text variant.
    if stripped.startswith('🤖 Generated with') and 'Claude' in stripped:
        continue
    if stripped.startswith('Generated with [Claude Code]'):
        continue
    out.append(line)
# Trim orphaned trailing blank lines.
while out and out[-1].strip() == '':
    out.pop()
commit.message = ('\n'.join(out) + '\n').encode('utf-8')
