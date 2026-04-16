# Beava Governance

Honest governance for a one-maintainer pre-launch open-source project. No
foundation, no committee, no fantasy of a committers' guild that doesn't
exist yet.

## Who decides

**Hoang Phan** is the current benevolent-dictator-for-now. Hoang merges
every PR, cuts every release, and makes every roadmap call. See
[MAINTAINERS.md](MAINTAINERS.md) for contact details.

**Bus factor: 1.** If Hoang is hit by a bus, the repo continues under
Apache 2.0 and any motivated person may fork and continue. That is the
intended fallback. There is no succession plan encoded in the repo
besides "it is Apache 2.0 and the code is on GitHub." That is enough for
a project at this stage; it will not be enough at v1.0 scale.

## Bus factor of 1 — disclosed up front

Beava has one maintainer (Hoang) today. Bringing on a second committer
is a goal — there's no committed timeline behind it.

The honest contingency is the license: Apache 2.0 with no CLA. If Hoang
disappears, the codebase, server, SDK, debug endpoints, and Claude
skill can be forked by anyone, with no legal or licensing obstacle.
That's not a substitute for a second committer; it's the actual
disclosure of what you're depending on if you build on Beava today.

When a second committer does join:

- They share review load on all non-trivial PRs.
- They have unilateral merge authority on bug-fix and documentation
  PRs, same as Hoang.
- They can cut releases and publish tags.
- Architecture decisions require alignment between the two maintainers;
  Hoang retains tie-break until a third maintainer joins.

This document will be updated when that happens — not before.

## What stays Apache 2.0 forever

The following code and specifications are, and will remain, under the
Apache 2.0 license as published in this repository:

- **Rust server core.** Every file under `src/`.
- **All 16 operators** shipped in v0.x: count, sum, avg, min, max, stddev,
  percentile, distinct_count (HLL++), last, first, lag, ema, last_n,
  exact_min, exact_max, derive.
- **Python SDK** including `@bv.stream`, `@bv.table`, `bv.replay()`, and
  `bv.fork()`. Everything under `python/` today.
- **Binary TCP protocol** at port 6400, HTTP management API at 6401. The
  wire format is and will stay documented and stable-modulo-versioning.
- **Debug endpoints:** `/debug/*` routes, `/metrics`, `/public/stats`.
- **Primary/replica replication:** `OP_LOG_FETCH`, `OP_SUBSCRIBE`, the
  reconnect protocol. Everything needed to run a geo-replica yourself.
- **Manual failover primitives:** snapshot export/import, event log
  replay, cursor-based resume.
- **Claude / Cursor / Codex skill:** the `beava` skill file in
  `.agents/skills/beava/`. This is the AI-assisted setup path and stays
  free as long as an AI provider can call it.

If it ships in the open-source repo, it is Apache 2.0. No dual-licensing,
no key-files, no "community edition vs enterprise edition" of the same
feature. If a future commercial feature lives in this repo it will be
under a different path with a clear license notice at that file's top.

## What moves to Beava Cloud

Managed-service features that depend on operator-hosted infrastructure
will live in Beava Cloud, not this repo. Expected scope:

- Managed hosting (Beava-as-a-service, zero-ops).
- Automatic high-availability and failover across nodes.
- Multi-region replication with SLO-managed lag.
- SOC 2 Type II and HIPAA attestations.
- Role-based access control (RBAC), SSO, SCIM user provisioning.
- Audit logs with long-term retention.
- Managed backups and point-in-time recovery.
- Support with an uptime SLA.

**What this means concretely:** if you self-host Beava, you get the same
core feature server that Beava Cloud runs on. What you don't get is the
managed operations surface. Exactly like Postgres: the database is free,
RDS is not.

Beava Cloud does not exist at the time of this writing. This is a
commitment to keep the open-source repo intact when and if Beava Cloud
launches.

## Trademark policy

The name **Beava** is **unregistered as of the date of this document**.

When the trademark is registered (we expect this in 2026), the policy
will be:

- **Forks and derivatives are welcome.** You may call your fork "based on
  Beava" or "compatible with Beava" without asking. This is how the
  ecosystem should work.
- **Technical compatibility claims are welcome.** If your product
  implements a Beava wire-compatible client or exposes a Beava-compatible
  API surface, you may describe it as such without asking.
- **Commercial products trading on the Beava name require a license.**
  If you ship a paid product whose marketing positions the product *as*
  Beava (not "based on Beava"), that's a trademark conversation. Email
  hoang@beava.dev.

Until the trademark is registered, this section is aspirational. In
practice, use common sense and attribute the project.

## Fork-friendly posture

Apache 2.0 was chosen over any non-commercial or source-available license
because **if Hoang disappears, you should be able to fork**. That is the
point of an open-source license at this stage of the project.

A fork is allowed for any reason. You do not need permission. The
trademark policy above is the only limit, and it only kicks in for
commercial use of the *name*.

## Contributor License Agreement (CLA)

**There is no CLA.** Apache 2.0 `Section 5` (inbound=outbound by default)
covers PR contributions without a separate agreement. We will revisit
this decision when the project has a foundation or a multi-vendor
committer group — not before.

If you are contributing on behalf of an employer, confirm your employer
allows Apache 2.0 contributions before opening a PR. That's your
responsibility, not ours.

## Pull request review process

**Today (one maintainer):**

- Hoang reviews every PR.
- Target first-response time: 48 hours for small PRs (< 50 lines),
  1 week for larger changes. This is a goal, not a guarantee — I get
  backed up.
- Review focus: correctness, tests, semantics alignment with
  [SEMANTICS.md](SEMANTICS.md), performance regression risk on the
  ingest hot path.
- Approval is a single maintainer's `Approve` + green CI. No
  multi-maintainer sign-off required.

**Once a second committer joins (two maintainers — no committed timeline today):**

- Hoang and the second committer share review load.
- Either maintainer can merge bug fixes and documentation.
- Non-trivial architecture or API changes require both maintainers to
  acknowledge the PR (not necessarily approve, but see it and raise
  objections in time). This is lightweight — the goal is to avoid
  one-maintainer-merges-a-breaking-change-while-the-other-is-on-vacation,
  not to block progress.

**CI requirements (always):**

- `cargo test -- --test-threads=1` passes.
- `cargo clippy --all-targets -- -D warnings` passes.
- `cargo fmt --check` passes.
- Python tests pass: `cd python && python -m pytest tests/ -q`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full PR checklist.

## Public discussion

Discussion happens in:

- **GitHub Issues** — bug reports, feature requests, roadmap questions.
- **GitHub Discussions** — open-ended design conversations.
- **Pull requests** — the actual decisions and rationale land here.

There is no Discord, Slack, or mailing list. If this changes we will
update this document.

## Changing this document

Governance changes require a PR. Hoang approves. Once a second
maintainer joins, both maintainers must approve changes to this
document.
