# Beava Maintainers

## Current maintainers

### Hoang Phan — sole maintainer, project lead

- **Email:** phan.minhhoang2606@gmail.com
- **GitHub:** [@petrpan26](https://github.com/petrpan26)
- **LinkedIn:** [linkedin.com/in/hoang-phan](https://www.linkedin.com/in/hoang-phan-4a79a8189/)

Hoang makes every call on merges, releases, and the roadmap. See
[GOVERNANCE.md](GOVERNANCE.md) for the full decision model and bus-factor
acknowledgment.

## Bus factor of 1, disclosed up front

Today Beava has one maintainer (me). Bringing on a second committer is
the goal but I don't have a committed timeline. The actual contingency
is the license: Apache 2.0, no CLA. If I disappear, the codebase,
server, SDK, and debug endpoints can be forked by anyone, with no legal
or licensing obstacle.

That's not a substitute for a second committer — it's the honest answer
about the risk you're taking on by depending on Beava today. Two design
partner agreements include a 90-day notice clause for exactly this
reason.

If you are a senior Rust engineer with streaming or ML-platform
experience and would consider being a second committer down the line,
I'd love to hear from you (no role open today, but the conversation is
worth having early).

## How to become a maintainer

This section is forward-looking — no one is at this stage today, and
Hoang will personally reach out to candidates before opening the gate.

In the future, an established contributor becomes a maintainer when:

1. They have shipped at least 10 non-trivial PRs merged over at least 3
   months. "Non-trivial" = more than a typo fix or a version bump.
2. They have reviewed at least 20 PRs from other contributors with
   substantive feedback.
3. They are nominated by an existing maintainer and approved by a
   majority of current maintainers (once there is more than one).
4. They agree to the expectations below.

**Maintainer expectations:**

- Respond to PRs and issues within one week.
- Follow [SEMANTICS.md](SEMANTICS.md) and flag any PR that claims
  guarantees the code doesn't deliver.
- Follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) in all project
  interactions.
- Do not merge your own substantial PRs without a second maintainer's
  review (except for trivial fixes).
- Keep the [GOVERNANCE.md](GOVERNANCE.md) list of Apache 2.0 forever
  components honest; flag any attempt to quietly move something.

A maintainer may step down at any time by updating this file.

## Escalation path

For things that don't belong in a public GitHub issue — security
vulnerabilities, code-of-conduct reports, legal questions — email Hoang
directly at **phan.minhhoang2606@gmail.com**.

For security specifically, see [SECURITY.md](SECURITY.md).

For code-of-conduct reports, see the contact in
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Emeritus maintainers

None yet. If anyone steps down in the future, their name stays here with
their contribution dates.
