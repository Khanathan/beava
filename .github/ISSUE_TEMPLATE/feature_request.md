---
name: Feature request
about: Propose a new operator, decorator, or wire-surface change
title: "[feature] "
labels: ["enhancement"]
---

## Summary

<One-sentence what + why.>

## Use case

<Concrete scenario. What pipeline are you trying to author? What's missing in
the current operator catalog or wire surface that blocks you?>

## Proposed API

<If you have a sketch of the surface (Python decorator, JSON wire-spec, HTTP
endpoint), paste it here. Concrete code blocks preferred. Otherwise leave blank.>

```python
# Example sketch:
```

## Alternatives considered

<Any workaround you've tried with the existing catalog. If you've already
combined existing operators to approximate this, share that pipeline.>

## Already in the catalog or deferred?

Before opening, please check:

- 53-op v0 catalog: https://github.com/beava-dev/beava/tree/main/docs/operators
- v0 scope locks: tables (no upsert/delete/retract), joins, session windows,
  event-time / watermarks, and `bv.fork` are descoped to v0.1+ or permanently.
  See ROADMAP.md for the current deferred-surface list.

Is this:

- [ ] In v0 territory (operator missing from current catalog, or fits the
      events-only Redis-shaped model)
- [ ] In v0.1+ territory (event-time, joins, sessions, tables — already
      planned, this is a +1 / refinement)
- [ ] Unclear — open to a quick triage discussion

## Additional context

<Anything else: workload shape, references to upstream systems, links.>
