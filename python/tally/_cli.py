"""Plan 25-03: ``tally`` CLI entry point.

A thin HTTP client over the admin-gated ``/debug/config-recommendations``
endpoint that pretty-prints recommendations grouped by decorator target,
so operators can copy the suggested line straight into their pipeline
source.

Installed as a console script via ``pyproject.toml``::

    [project.scripts]
    tally = "tally._cli:main"

Uses only the Python standard library (argparse, urllib, json, sys) —
no third-party dependencies so the CLI works anywhere the SDK does.

Subcommands:
    suggest-config      Fetch /debug/config-recommendations and print.

Exit codes:
    0   success (may still print "no recommendations")
    1   network / HTTP / decode error
    2   argparse usage error (stdlib default)
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import urllib.error
import urllib.request
from importlib.resources import files as _resource_files
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


# ---------------------------------------------------------------------------
# suggest-config
# ---------------------------------------------------------------------------


def _fetch_recommendations(
    host: str,
    port: int,
    token: Optional[str],
    timeout: float = 5.0,
) -> Dict[str, Any]:
    """GET /debug/config-recommendations and return the parsed JSON.

    Raises urllib.error.URLError / ConnectionError on network failure and
    json.JSONDecodeError on malformed responses — the caller translates
    those into a user-friendly exit-1 message.
    """
    url = f"http://{host}:{port}/debug/config-recommendations"
    headers: Dict[str, str] = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    return json.loads(body)


def _group_by_target(recs: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group recommendations by the prefix of ``knob`` before the first dot.

    ``UserProfile.ttl`` → ``UserProfile``. Preserves input order within a
    group so the server-side deterministic ordering flows through to
    stdout.
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in recs:
        knob = r.get("knob", "")
        target = knob.split(".", 1)[0] if knob else "(unknown)"
        groups.setdefault(target, []).append(r)
    return groups


def _format_confidence(conf: Any) -> str:
    """Format a confidence float as ``0.72`` or ``-`` when absent."""
    try:
        return f"{float(conf):.2f}"
    except (TypeError, ValueError):
        return "-"


def cmd_suggest_config(args: argparse.Namespace) -> int:
    """Handler for ``tally suggest-config``."""
    try:
        data = _fetch_recommendations(
            host=args.host,
            port=args.port,
            token=args.token,
            timeout=args.timeout,
        )
    except urllib.error.HTTPError as e:
        print(
            f"tally: HTTP {e.code} from {args.host}:{args.port}: {e.reason}",
            file=sys.stderr,
        )
        return 1
    except urllib.error.URLError as e:
        print(
            f"tally: could not reach {args.host}:{args.port}: {e.reason}",
            file=sys.stderr,
        )
        return 1
    except (ConnectionError, OSError) as e:
        print(
            f"tally: could not reach {args.host}:{args.port}: {e}",
            file=sys.stderr,
        )
        return 1
    except json.JSONDecodeError as e:
        print(
            f"tally: malformed JSON from server: {e}",
            file=sys.stderr,
        )
        return 1

    recs = data.get("recommendations", []) or []
    if not recs:
        print("No configuration recommendations at this time.")
        return 0

    groups = _group_by_target(recs)
    first_group = True
    for target in sorted(groups.keys()):
        if not first_group:
            print()
        first_group = False
        print(f"{target}:")
        for r in groups[target]:
            knob = r.get("knob", "(unknown)")
            current = r.get("current", "?")
            suggested = r.get("suggested", "?")
            conf = _format_confidence(r.get("confidence"))
            reason = r.get("reason", "")
            copy_paste = r.get("copy_paste", "")
            print(f"  {knob}: {current} -> {suggested}  (confidence={conf})")
            if reason:
                print(f"    reason: {reason}")
            if copy_paste:
                print(f"    {copy_paste}")
    return 0


# ---------------------------------------------------------------------------
# install-skill
# ---------------------------------------------------------------------------


def _read_bundled_skill() -> str:
    """Return the bundled SKILL.md text shipped inside the wheel.

    The file lives at ``tally/_skill/SKILL.md`` inside the installed package
    (force-included by hatchling from ``.agents/skills/tally/SKILL.md`` at
    build time). Raises FileNotFoundError if the wheel was built without the
    skill bundle (e.g. editable install from a stripped checkout).
    """
    return (
        _resource_files("tally").joinpath("_skill/SKILL.md").read_text(encoding="utf-8")
    )


def _find_git_root(start: Path) -> Optional[Path]:
    """Walk up from ``start`` until a ``.git`` directory is found."""
    for p in [start, *start.parents]:
        if (p / ".git").exists():
            return p
    return None


def cmd_install_skill(args: argparse.Namespace) -> int:
    """Handler for ``tally install-skill``."""
    try:
        content = _read_bundled_skill()
    except FileNotFoundError:
        print(
            "tally: bundled SKILL.md not found in package. Reinstall with "
            "`pip install --upgrade tally`, or check that the build included "
            "the .agents/skills/tally/SKILL.md bundle.",
            file=sys.stderr,
        )
        return 1

    if args.repo:
        root = _find_git_root(Path.cwd())
        if root is None:
            print(
                "tally: --repo requires running inside a git repo; no .git "
                "found walking up from the current directory.",
                file=sys.stderr,
            )
            return 1
        dest_dir = root / ".agents" / "skills" / "tally"
        scope = f"repo ({root})"
    else:
        dest_dir = Path.home() / ".agents" / "skills" / "tally"
        scope = "user ($HOME)"

    dest = dest_dir / "SKILL.md"
    if dest.exists() and not args.force:
        print(
            f"tally: {dest} already exists. Use --force to overwrite.",
            file=sys.stderr,
        )
        return 1

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest.write_text(content, encoding="utf-8")

    # Back-compat symlink for Claude Code (older versions still scan
    # ~/.claude/skills/ rather than .agents/skills/). Only meaningful for
    # user-level installs — skip for --repo to keep that scope clean.
    claude_link_msg = ""
    if not args.repo:
        claude_dir = Path.home() / ".claude" / "skills"
        claude_link = claude_dir / "tally"
        try:
            claude_dir.mkdir(parents=True, exist_ok=True)
            if claude_link.is_symlink() or claude_link.exists():
                if args.force:
                    if claude_link.is_symlink() or claude_link.is_file():
                        claude_link.unlink()
                    else:
                        shutil.rmtree(claude_link)
                else:
                    claude_link_msg = (
                        f"  (left existing {claude_link} alone; pass --force to replace)"
                    )
            if not claude_link.exists():
                try:
                    claude_link.symlink_to(dest_dir, target_is_directory=True)
                    claude_link_msg = f"  (symlink: {claude_link} -> {dest_dir})"
                except OSError:
                    # Windows or restricted FS: fall back to a flat copy so
                    # Claude Code still discovers the skill.
                    claude_link.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(dest, claude_link / "SKILL.md")
                    claude_link_msg = f"  (copy: {claude_link}/SKILL.md)"
        except OSError as e:
            claude_link_msg = f"  (could not set up Claude back-compat link: {e})"

    print(f"Installed Tally skill [{scope}] at {dest}")
    if claude_link_msg:
        print(claude_link_msg)
    print()
    print("Try it:")
    print("  Claude Code:  /tally")
    print("  Cursor:       @tally   (Agent mode, or just describe the task)")
    print("  Codex CLI:    /skills tally")
    print()
    print("Run `/tally` with no args for a guided walk-through, or jump to a")
    print("specific mode: feature | debug | plan | estimate | bench | tune.")
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tally",
        description="Tally operator CLI — manage and inspect a running server.",
    )
    sub = p.add_subparsers(dest="cmd", required=True, metavar="<command>")

    sc = sub.add_parser(
        "suggest-config",
        help="Print TTL/history_ttl recommendations from a running server.",
        description=(
            "Fetch recommendations from /debug/config-recommendations on the "
            "admin HTTP port and pretty-print them grouped by decorator "
            "target. Defaults to localhost:6401."
        ),
    )
    sc.add_argument(
        "--host",
        default="localhost",
        help="admin-API host (default: localhost)",
    )
    sc.add_argument(
        "--port",
        type=int,
        default=6401,
        help="admin-API port (default: 6401)",
    )
    sc.add_argument(
        "--token",
        default=None,
        help="bearer token for non-loopback access (default: none)",
    )
    sc.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP request timeout in seconds (default: 5.0)",
    )
    sc.set_defaults(func=cmd_suggest_config)

    sk = sub.add_parser(
        "install-skill",
        help="Install the Tally AI-editor skill for Claude Code, Cursor, and Codex.",
        description=(
            "Copy the bundled SKILL.md to ~/.agents/skills/tally/ (user-level, "
            "default) or ./.agents/skills/tally/ in the current git repo "
            "(--repo). User-level installs also create a back-compat "
            "~/.claude/skills/tally symlink. After install, type `/tally` in "
            "Claude Code or `@tally` in Cursor Agent mode."
        ),
    )
    sk.add_argument(
        "--repo",
        action="store_true",
        help="install into the current git repo instead of $HOME",
    )
    sk.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing SKILL.md (and replace the Claude symlink)",
    )
    sk.set_defaults(func=cmd_install_skill)

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover - module-as-script guard
    sys.exit(main())
