## Summary

<1-3 sentences. What does this PR change? Why?>

## Linked issues

<Closes #N / Related to #N — if applicable.>

## Test plan

How did you verify this works locally? Commands run, results observed.

- [ ] `cargo test -- --test-threads=1` passes
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo fmt --check` passes
- [ ] `cd python && python -m pytest tests/ -q` passes (if Python touched)
- [ ] If user-visible behavior changed, attached a curl session, screenshot,
      or log excerpt demonstrating the new behavior

## Author checklist

- [ ] Tests added or updated to cover the change
- [ ] No stale repository URLs introduced (canonical: `beava-dev/beava`)
- [ ] CHANGELOG.md updated under `## [Unreleased]` (if user-visible change)
- [ ] Public API / wire surface changes are documented (README, docs/, or
      decorator docstrings as applicable)

## Breaking changes

<Any user-visible API, wire-format, or behavior changes? List them; flag
clearly if existing pipelines need to be updated.>

## Notes for reviewer

<Anything reviewer should pay attention to: tricky areas, follow-ups
intentionally deferred, performance-sensitive code paths.>
