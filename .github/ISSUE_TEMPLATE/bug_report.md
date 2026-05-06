---
name: Bug report
about: Report a defect or regression in Beava
title: "[bug] "
labels: ["bug"]
---

## Summary

<One-line description of the unexpected behavior.>

## Steps to reproduce

Numbered, minimal sequence that triggers the bug:

1.
2.
3.

Include the pipeline definition (Python decorator or wire-spec JSON; redact PII)
and the sequence of pushes / gets that triggers the bug.

## Expected behavior

<What you expected to happen.>

## Actual behavior

<What actually happened. Include error messages, stack traces, or `/metrics`
output if relevant.>

## Environment

- Beava version: `<v0.0.x>` (from `pip show beava`, `beava --version`, or commit SHA)
- Transport: `http` | `tcp` | `embed`
- Operating system + arch: `<Linux x86_64 / Linux ARM64 / macOS ARM64 / ...>`
- Python version (if using SDK): `<x.y.z>`
- How installed: `cargo install` | release binary | source build | `pip install beava`

## Minimal reproducer (if applicable)

```
<Smallest pipeline + push/get sequence that demonstrates the bug. Self-contained.>
```

## Logs

```
<Server stderr at INFO/WARN/ERROR — first 50 lines around the failure.>
```

## Additional context

<Anything else: workload shape, entity count, related issues, recent changes.>
