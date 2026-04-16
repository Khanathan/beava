# UNSAFE.md — audit of `unsafe` blocks in Beava

**Summary:** 4 `unsafe` blocks, all in `src/state/event_log.rs`, all libc FFI calls for direct write/fsync syscalls. Zero `unsafe` in the Tokio hot path, pipeline engine, state store, or TCP server.

Counted with: `grep -rn "unsafe {" src/ --include="*.rs"`

## Inventory

| # | File:line | Purpose | Classification |
|---|---|---|---|
| 1 | `src/state/event_log.rs:121` | `libc::write()` raw syscall for batched event-log append | FFI — write-syscall path |
| 2 | `src/state/event_log.rs:161` | `libc::write()` raw syscall for single-event append | FFI — write-syscall path |
| 3 | `src/state/event_log.rs:199` | `libc::fdatasync()` on the event-log fd (Linux fast-path) | FFI — durability fsync |
| 4 | `src/state/event_log.rs:201` | `libc::fsync()` on the event-log fd (non-Linux fallback) | FFI — durability fsync |

## Why these exist

Beava's durability story is group-commit fsync-before-ack. To hit the latency target (~1s worst-case data loss under sustained load), the write path needs direct syscall access — `std::fs::File::write_all` plus `std::fs::File::sync_all` works correctly but:

- Doesn't expose the `O_APPEND` atomicity guarantee on POSIX (where concurrent writes under a single file descriptor are atomic up to PIPE_BUF)
- `sync_all()` calls `fsync` not `fdatasync` on Linux — `fdatasync` is roughly 2× faster for append-only workloads because it skips metadata sync
- Error handling is clearer with direct `errno` than Rust's `io::Error` wrapping

Each block is 3-5 lines. The invariants are: (1) the file descriptor is valid for the lifetime of the `EventLog` struct (guaranteed by the struct's `Drop` impl), (2) the buffer pointer + length is valid Rust-owned memory (guaranteed by passing a `&[u8]` to the helper).

## Classification definitions

- **FFI:** calls to non-Rust code (libc, OS syscalls). Unsafe because the compiler can't verify the foreign contract. Always wrapped with invariant comments at the call site.
- **Hot-path perf:** unsafe used purely for performance (e.g., unchecked indexing, `MaybeUninit`, custom allocators). **Beava has zero of these.**
- **Parser / deserialization:** unsafe in wire protocol decoding. **Beava has zero of these** (we use safe `bytes`/`bincode`-style parsing).

## Launch-copy verification

The public landing/README promises:

> "Zero `unsafe` outside FFI in the hot path."

Verified against the current tree. All 4 unsafe blocks are FFI. Auditable in one afternoon:

```bash
grep -rn "unsafe {" src/ --include="*.rs"
```

Returns the 4 lines above. No others.

## Changes

If new `unsafe` is added to Beava, this document must be updated in the same PR. A pre-commit hook will block PRs that increase the unsafe count without a matching UNSAFE.md update (planned; not yet enforced).

## Tooling

Run locally:

```bash
cargo geiger                # summary of unsafe in deps + crate
cargo clippy --all-targets  # catches common unsafe pitfalls
```

Report issues: open a GitHub Issue or email hoang (see MAINTAINERS.md).
