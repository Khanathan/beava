"""Phase 39-01: ``bv.fork()`` — Python-native DX for scoped local replicas.

The scientist workflow is::

    import beava as bv

    @bv.stream
    class Transactions:
        user_id: str
        amount: float

    def _summary(t: Transactions) -> bv.Table:
        return t.group_by("user_id").agg(
            count=bv.count(window="1h"),
            total=bv.sum("amount", window="1h"),
        )
    _summary.__name__ = "txn_summary"
    TxnSummary = bv.table(key="user_id")(_summary)

    with bv.fork(
        remote="prod.beava.dev:6400",
        streams=[Transactions],
        keys=["u1", "u2"],
        token="replica-token",
        pipelines=[TxnSummary],
    ) as fork:
        print(fork.get(TxnSummary, key="u1"))

This module is a *pure-Python* wrapper over the Phase 37 ``beava fork``
CLI. It does not add any Rust surface: it serialises the scientist's
pipelines to a REGISTER JSON file (same bytes ``App.register`` would
send), spawns the ``beava fork`` subprocess with ``--pipeline-file``,
polls ``/debug/ready``, and exposes a small query helper.

Error hierarchy:

* :class:`ForkError` — base class.
* :class:`ForkValidationError` — caller-arg errors (raised before the
  subprocess spawn).
* :class:`ForkTimeoutError` — ``/debug/ready`` didn't return 200 in
  ``ready_timeout``. Carries the last 50 lines of stderr.
* :class:`ForkSubprocessError` — binary exited unexpectedly during start.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ForkError(Exception):
    """Base class for all :func:`beava.fork` failures."""


class ForkValidationError(ForkError):
    """Raised when ``fork()`` arguments are invalid (before subprocess spawn)."""


class ForkTimeoutError(ForkError):
    """Raised when the fork subprocess did not reach ``/debug/ready`` in time."""


class ForkSubprocessError(ForkError):
    """Raised when the fork subprocess exits unexpectedly during start."""


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_fork_args(
    *,
    remote: str,
    streams: list,
    keys: list[str] | None,
    key_prefix: str | None,
    token: str | None,
    pipelines: list | None,
    ready_timeout: float,
) -> tuple[list[str], str]:
    """Validate args. Returns ``(stream_names, resolved_token)``."""
    if not isinstance(remote, str) or ":" not in remote:
        raise ForkValidationError(
            f"remote must be 'HOST:PORT', got {remote!r}"
        )

    if not streams:
        raise ForkValidationError("streams must be a non-empty list")

    seen: set[int] = set()
    stream_names: list[str] = []
    for s in streams:
        if id(s) in seen:
            raise ForkValidationError(
                f"streams contains duplicate descriptor {s!r}"
            )
        seen.add(id(s))
        # A StreamSource (class-form @bv.stream) has _beava_kind == 'stream'
        # and _beava_stream_name; reject anything that doesn't quack.
        kind = getattr(s, "_beava_kind", None)
        name = getattr(s, "_beava_stream_name", None)
        if kind != "stream" or not isinstance(name, str) or not name:
            raise ForkValidationError(
                f"streams[?] must be a @bv.stream descriptor; "
                f"got {s!r} (kind={kind!r})"
            )
        if not hasattr(s, "_to_register_json"):
            raise ForkValidationError(
                f"streams[?] {name!r} lacks _to_register_json() — "
                f"not a registerable descriptor"
            )
        stream_names.append(name)

    if keys is not None and key_prefix is not None:
        raise ForkValidationError(
            "keys and key_prefix are mutually exclusive; pass exactly one "
            "(or neither for unfiltered replication)"
        )
    if keys is not None:
        if not isinstance(keys, (list, tuple)) or not keys:
            raise ForkValidationError("keys must be a non-empty list of strings")
        for k in keys:
            if not isinstance(k, str) or not k:
                raise ForkValidationError(
                    f"keys entries must be non-empty strings; got {k!r}"
                )
    if key_prefix is not None and (not isinstance(key_prefix, str) or not key_prefix):
        raise ForkValidationError(
            f"key_prefix must be a non-empty string; got {key_prefix!r}"
        )

    if pipelines is not None:
        if not isinstance(pipelines, (list, tuple)):
            raise ForkValidationError(
                f"pipelines must be a list; got {type(pipelines).__name__}"
            )
        for p in pipelines:
            if not hasattr(p, "_to_register_json"):
                raise ForkValidationError(
                    f"pipelines[?] {p!r} is not a registerable descriptor "
                    f"(needs _to_register_json())"
                )

    resolved_token = token or os.environ.get("BEAVA_REPLICA_TOKEN")
    if not resolved_token:
        raise ForkValidationError(
            "token required: pass token=... or set BEAVA_REPLICA_TOKEN env"
        )

    if not isinstance(ready_timeout, (int, float)) or ready_timeout <= 0:
        raise ForkValidationError(
            f"ready_timeout must be positive, got {ready_timeout!r}"
        )

    return stream_names, resolved_token


# ---------------------------------------------------------------------------
# Seed-file serialisation
# ---------------------------------------------------------------------------


def _build_register_bundle(
    streams: list,
    pipelines: list | None,
) -> list[dict[str, Any]]:
    """Walk streams + pipelines and produce a deduped list of REGISTER dicts.

    Mirrors ``App.register``: prefers ``_collect_registrations`` (which
    handles transitive upstream walk for aggregations / joins / unions);
    falls back to ``_to_register_json`` for leaf-only descriptors.

    The output is the same byte-shape ``src/main.rs::seed_pipelines_from_file``
    consumes (a JSON array of register objects).
    """
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(defn: dict[str, Any]) -> None:
        name = defn.get("name")
        if not isinstance(name, str):
            raise ForkValidationError(
                f"register JSON missing 'name' field: {defn!r}"
            )
        if name in seen:
            return
        seen.add(name)
        out.append(defn)

    descriptors: list = list(streams) + list(pipelines or [])
    for desc in descriptors:
        if hasattr(desc, "_collect_registrations"):
            for reg in desc._collect_registrations():
                _add(reg)
        elif hasattr(desc, "_to_register_json"):
            _add(desc._to_register_json())
        else:
            raise ForkValidationError(
                f"{desc!r} is not a registerable descriptor"
            )
    return out


def _write_seed_file(bundle: list[dict[str, Any]]) -> Path:
    """Write the REGISTER bundle to a named temp file and return its path."""
    fd, path = tempfile.mkstemp(prefix="beava-fork-seed-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(bundle, f)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return Path(path)


# ---------------------------------------------------------------------------
# Port allocation
# ---------------------------------------------------------------------------


def _pick_free_port() -> int:
    """Bind to 127.0.0.1:0, grab the OS-assigned port, close.

    Races with other allocators — acceptable for demo/dev. The ``beava
    fork`` CLI takes both HTTP on ``local_port`` and TCP on
    ``local_port + 1``; we probe P+1 opportunistically up to 10 tries.
    """
    for _ in range(10):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            p = s.getsockname()[1]
        # Try to verify P+1 is also free.
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", p + 1))
            return p
        except OSError:
            continue
    return p  # best effort


# ---------------------------------------------------------------------------
# ForkedReplica handle
# ---------------------------------------------------------------------------


class ForkedReplica:
    """Handle to a running ``beava fork`` subprocess.

    Construct via :func:`beava.fork`. Supports context-manager semantics
    (``with bv.fork(...) as f:``) which guarantees :meth:`stop` fires.
    """

    def __init__(
        self,
        *,
        proc: subprocess.Popen,
        local_port: int,
        token: str,
        seed_file: Path | None,
        stdout_log: Path | None,
        stderr_log: Path | None,
        streams: list,
        pipelines: list,
    ) -> None:
        self._proc = proc
        self._local_port = local_port
        self._token = token
        self._seed_file = seed_file
        self._stdout_log = stdout_log
        self._stderr_log = stderr_log
        self._streams = list(streams)
        self._pipelines = list(pipelines)
        self._stopped = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def local_port(self) -> int:
        return self._local_port

    @property
    def local_url(self) -> str:
        return f"http://127.0.0.1:{self._local_port}"

    @property
    def pid(self) -> int:
        return self._proc.pid

    @property
    def log_path(self) -> Path | None:
        """Path to the fork's stderr log (for debugging)."""
        return self._stderr_log

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def _resolve_name(self, pipeline_or_stream: Any) -> str:
        name = getattr(pipeline_or_stream, "_beava_stream_name", None)
        if isinstance(name, str) and name:
            return name
        # Raw string support.
        if isinstance(pipeline_or_stream, str):
            return pipeline_or_stream
        raise ForkValidationError(
            f"can't resolve feature name from {pipeline_or_stream!r}; "
            f"pass a @bv.stream / @bv.table descriptor or a name string"
        )

    def get(
        self,
        pipeline_or_stream: Any,
        *,
        key: str,
    ) -> dict | None:
        """Fetch the computed-features map for ``key``.

        ``pipeline_or_stream`` selects which feature-name subset to return:

        * A ``@bv.table`` descriptor → returns just that table's feature
          fields (e.g. ``{"count": 3, "total": 60.0}``).
        * A ``@bv.stream`` descriptor or a string → returns the full
          ``computed_features`` map for the key (the ``v0`` key-space is
          flat so there's no stream-scoped subset to filter to).

        Returns ``None`` if the replica has never seen ``key`` or has no
        feature values for it yet.
        """
        self._ensure_alive()
        url = f"{self.local_url}/debug/key/{key}"
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {self._token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 404:
                    return None
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

        features = body.get("computed_features") if isinstance(body, dict) else None
        if not isinstance(features, dict) or not features:
            return None

        # If the caller passed a @bv.table descriptor, return only the fields
        # declared in that table's schema (so scientists get exactly the
        # columns they authored). For @bv.stream or raw string, return the
        # full dict — the v0 key-space is flat.
        kind = getattr(pipeline_or_stream, "_beava_kind", None)
        if kind == "table":
            schema = getattr(pipeline_or_stream, "_schema", None)
            key_fields = getattr(pipeline_or_stream, "_key", None) or []
            if isinstance(schema, dict):
                wanted = [
                    f for f in schema.keys() if f not in key_fields
                ]
                projected = {
                    f: features[f] for f in wanted if f in features
                }
                # If none of the wanted fields are present yet, None.
                if not projected:
                    return None
                return projected
        return dict(features)

    def inspect(self, *keys: str) -> dict[str, dict | None]:
        """Batch-query ``.get(None, key=k)`` over ``keys``.

        Returns a dict mapping each key to its full ``computed_features``
        map (or ``None``). No projection — scientists can slice after.
        """
        self._ensure_alive()
        out: dict[str, dict | None] = {}
        for k in keys:
            out[k] = self.get(k, key=k) if False else self._raw_features(k)
        return out

    def extract_history(self) -> dict[str, dict[str, dict]]:
        """Fetch the historical extraction registry from this fork.

        Phase 44-01. Returns the snapshots captured during ``--extract-at``
        replay, keyed by ISO-8601 timestamp (server-formatted) and then by
        entity key. Empty dict when ``extract_at=`` was not passed or no
        snapshot has landed yet.

        Example::

            with bv.fork(..., extract_at=[t1, t2, t3]) as fork:
                history = fork.extract_history()
                # {"2026-03-01T10:00:00Z": {"u1": {"count": 1, "total": 10.0}},
                #  "2026-03-15T10:00:00Z": {...}, ...}
        """
        self._ensure_alive()
        url = f"{self.local_url}/extracts"
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {self._token}"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        if not isinstance(body, dict):
            return {}
        extracts = body.get("extracts")
        if not isinstance(extracts, dict):
            return {}
        # Defensive copy — return plain dicts not shared with the server
        # response object.
        return {k: dict(v) if isinstance(v, dict) else {} for k, v in extracts.items()}

    def _raw_features(self, key: str) -> dict | None:
        url = f"{self.local_url}/debug/key/{key}"
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {self._token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 404:
                    return None
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise
        feats = body.get("computed_features") if isinstance(body, dict) else None
        if isinstance(feats, dict) and feats:
            return dict(feats)
        return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _ensure_alive(self) -> None:
        if self._stopped:
            raise ForkError("ForkedReplica has been stopped")
        rc = self._proc.poll()
        if rc is not None:
            tail = _tail_log(self._stderr_log, 20)
            raise ForkSubprocessError(
                f"beava fork subprocess exited (code {rc}); "
                f"stderr tail:\n{tail}"
            )

    def stop(self) -> None:
        """Terminate the subprocess and clean up temp files. Idempotent."""
        if self._stopped:
            return
        self._stopped = True
        proc = self._proc
        try:
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        proc.kill()
                    except ProcessLookupError:
                        pass
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
            # Drain any pipes so we don't leak fds.
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    try:
                        stream.close()
                    except Exception:
                        pass
        finally:
            for path in (self._seed_file, self._stdout_log, self._stderr_log):
                if path is not None:
                    try:
                        os.unlink(path)
                    except OSError:
                        pass

    def __enter__(self) -> "ForkedReplica":
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def __del__(self) -> None:  # pragma: no cover - best-effort cleanup
        try:
            self.stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Helpers (module-level for testability)
# ---------------------------------------------------------------------------


def _tail_log(path: Path | None, n: int = 50) -> str:
    if path is None:
        return "<no log>"
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as e:
        return f"<log read error: {e}>"
    return "\n".join(lines[-n:])


def _resolve_binary(binary_path: str | None) -> str:
    """Find the ``beava`` binary. ``binary_path`` takes precedence, else PATH."""
    if binary_path is not None:
        p = Path(binary_path)
        if not p.exists():
            raise ForkValidationError(
                f"binary_path {binary_path!r} does not exist"
            )
        return str(p)
    found = shutil.which("beava")
    if found:
        return found
    # Last resort: look at the workspace target dir (useful during dev).
    here = Path(__file__).resolve()
    for cand in (
        here.parents[2] / "target" / "release" / "beava",
        here.parents[2] / "target" / "debug" / "beava",
    ):
        if cand.exists():
            return str(cand)
    raise ForkValidationError(
        "couldn't find a `beava` binary on PATH. Set binary_path=... or "
        "build the workspace (`cargo build --release`)."
    )


def _format_since(since: str | int) -> str:
    """``beava fork --since`` takes ISO-8601 UTC or u64 ms. Accept either."""
    if isinstance(since, (int,)):
        return str(since)
    if isinstance(since, str) and since:
        return since
    raise ForkValidationError(f"since must be str or int, got {since!r}")


def _format_extract_at_entry(entry: Any) -> str:
    """Serialize a single ``extract_at`` entry for the ``--extract-at`` flag.

    Accepts ``datetime`` (serialized as ISO-8601 UTC with trailing ``Z``),
    ``int`` (unix milliseconds, stringified), or ``str`` (passed through).
    """
    if isinstance(entry, datetime):
        # Normalize to UTC; if naive, assume UTC (scientist demo convention).
        if entry.tzinfo is None:
            entry = entry.replace(tzinfo=timezone.utc)
        else:
            entry = entry.astimezone(timezone.utc)
        # isoformat emits '+00:00'; the Rust parser expects trailing 'Z'.
        iso = entry.replace(tzinfo=None).isoformat()
        # Drop microsecond precision to keep things at ms (server parses
        # up to 3 fractional digits anyway).
        if "." in iso:
            head, frac = iso.split(".", 1)
            iso = f"{head}.{frac[:3]}"
        return iso + "Z"
    if isinstance(entry, bool):
        # bool is a subclass of int; reject explicitly.
        raise ForkValidationError(
            f"extract_at entries must be datetime/int/str; got bool {entry!r}"
        )
    if isinstance(entry, int):
        return str(entry)
    if isinstance(entry, str) and entry:
        return entry
    raise ForkValidationError(
        f"extract_at entries must be datetime/int/str; got {entry!r}"
    )


def _format_extract_at(entries: list) -> str:
    """Serialize a list of extract_at entries to the comma-separated CLI form."""
    if not isinstance(entries, (list, tuple)) or not entries:
        raise ForkValidationError(
            "extract_at must be a non-empty list of datetime/int/str entries"
        )
    return ",".join(_format_extract_at_entry(e) for e in entries)


def _poll_ready(
    local_port: int,
    ready_timeout: float,
    proc: subprocess.Popen,
    stderr_log: Path | None,
) -> None:
    """Poll ``/debug/ready`` until 200 or raise :class:`ForkTimeoutError`."""
    deadline = time.monotonic() + ready_timeout
    last_err = ""
    url = f"http://127.0.0.1:{local_port}/debug/ready"
    while time.monotonic() < deadline:
        rc = proc.poll()
        if rc is not None:
            tail = _tail_log(stderr_log)
            raise ForkSubprocessError(
                f"beava fork exited early (code {rc}) before /debug/ready "
                f"became reachable.\nstderr tail:\n{tail}"
            )
        try:
            with urllib.request.urlopen(url, timeout=0.5) as resp:
                if resp.status == 200:
                    body = json.loads(resp.read().decode("utf-8"))
                    if body.get("ready") is True:
                        return
        except Exception as e:
            last_err = repr(e)
        time.sleep(0.2)

    tail = _tail_log(stderr_log)
    raise ForkTimeoutError(
        f"beava fork /debug/ready did not return 200 on :{local_port} "
        f"within {ready_timeout}s (last error: {last_err}).\n"
        f"stderr tail:\n{tail}"
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def fork(
    remote: str,
    streams: list,
    *,
    keys: list[str] | None = None,
    key_prefix: str | None = None,
    since: str | int = "1970-01-01T00:00:00Z",
    token: str | None = None,
    pipelines: list | None = None,
    extract_at: list | None = None,
    local_port: int | None = None,
    binary_path: str | None = None,
    ready_timeout: float = 30.0,
    env: dict[str, str] | None = None,
) -> ForkedReplica:
    """Spawn a scoped local replica of a remote Beava cluster.

    See module docstring for the full scientist workflow.

    Args:
        remote: Upstream cluster as ``"HOST:PORT"``.
        streams: Non-empty list of ``@bv.stream`` class descriptors to replicate.
        keys: Exact keys to replicate. Mutex with ``key_prefix``.
        key_prefix: Prefix filter. Mutex with ``keys``.
        since: ISO-8601 UTC string or u64 millisecond epoch. Default: epoch 0.
        token: Replica admin token. Falls back to ``BEAVA_REPLICA_TOKEN`` env.
        pipelines: Optional ``@bv.table`` / derivation descriptors to register
            on the fork (via the ``--pipeline-file`` seed). Transitive upstream
            streams are also auto-registered via ``_collect_registrations``.
        extract_at: Phase 44-01. Optional list of extraction timestamps
            (``datetime``, ISO-8601 string, or unix-ms int). During historical
            replay, the fork captures per-scope-key feature state as it
            crosses each timestamp. Query via :meth:`ForkedReplica.extract_history`
            after catchup. Default: ``None`` (no historical extraction).
        local_port: HTTP port for the fork (also uses TCP on port + 1).
            Auto-allocated if None.
        binary_path: Path to the ``beava`` binary. Defaults to ``beava`` on PATH.
        ready_timeout: Seconds to wait for ``/debug/ready`` before failing.
        env: Extra env vars for the subprocess (merged onto ``os.environ``).

    Returns:
        :class:`ForkedReplica` — use as a context manager for clean shutdown.
    """
    stream_names, resolved_token = _validate_fork_args(
        remote=remote,
        streams=streams,
        keys=keys,
        key_prefix=key_prefix,
        token=token,
        pipelines=pipelines,
        ready_timeout=ready_timeout,
    )

    binary = _resolve_binary(binary_path)
    port = local_port if local_port is not None else _pick_free_port()
    if not isinstance(port, int) or port <= 0:
        raise ForkValidationError(f"local_port must be > 0, got {port!r}")

    # Serialise pipelines + streams to a REGISTER-bundle seed file.
    seed_file: Path | None = None
    bundle = _build_register_bundle(list(streams), list(pipelines or []))
    if bundle:
        seed_file = _write_seed_file(bundle)

    # stdout / stderr log files — caller can read via ForkedReplica.log_path.
    stdout_log = Path(
        tempfile.NamedTemporaryFile(
            prefix="beava-fork-stdout-", suffix=".log", delete=False
        ).name
    )
    stderr_log = Path(
        tempfile.NamedTemporaryFile(
            prefix="beava-fork-stderr-", suffix=".log", delete=False
        ).name
    )

    argv = [
        binary,
        "fork",
        "--remote", remote,
        "--streams", ",".join(stream_names),
        "--token", resolved_token,
        "--local-port", str(port),
        "--since", _format_since(since),
    ]
    if keys is not None:
        argv += ["--keys", ",".join(keys)]
    if key_prefix is not None:
        argv += ["--key-prefix", key_prefix]
    if seed_file is not None:
        argv += ["--pipeline-file", str(seed_file)]
    if extract_at is not None:
        # Phase 44-01: accept datetime/int/str entries; serialise to the
        # comma-separated wire format the `beava fork --extract-at` flag
        # (and the underlying `--replica-extract-at` flag) consume.
        argv += ["--extract-at", _format_extract_at(extract_at)]

    sub_env = os.environ.copy()
    # Admin-token must be set so the fork's /debug/* routes accept the
    # same bearer token the scientist used above.
    sub_env.setdefault("BEAVA_ADMIN_TOKEN", resolved_token)
    if env is not None:
        sub_env.update(env)

    try:
        stdout_fp = open(stdout_log, "wb")
        stderr_fp = open(stderr_log, "wb")
        proc = subprocess.Popen(
            argv,
            env=sub_env,
            stdout=stdout_fp,
            stderr=stderr_fp,
        )
    except FileNotFoundError as e:
        for p in (seed_file, stdout_log, stderr_log):
            if p is not None:
                try:
                    os.unlink(p)
                except OSError:
                    pass
        raise ForkSubprocessError(f"failed to spawn {binary!r}: {e}") from e

    replica = ForkedReplica(
        proc=proc,
        local_port=port,
        token=resolved_token,
        seed_file=seed_file,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
        streams=list(streams),
        pipelines=list(pipelines or []),
    )

    try:
        _poll_ready(port, ready_timeout, proc, stderr_log)
    except BaseException:
        replica.stop()
        raise

    return replica


__all__ = [
    "fork",
    "ForkedReplica",
    "ForkError",
    "ForkValidationError",
    "ForkTimeoutError",
    "ForkSubprocessError",
]
