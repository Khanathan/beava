#!/bin/sh
# beava installer — one command, server binary + Python SDK + nothing else.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/beava-dev/beava/main/scripts/install.sh | sh
#
# What it does:
#   1. Detect host platform (Darwin/Linux × arm64/x86_64).
#   2. Find the matching wheel asset on the latest beava GitHub Release.
#   3. `pip install --user` it.
#
# The wheel ships the Rust `beava` binary inside (maturin bin-mode) so
# after install, `beava` is on `~/.local/bin/` and `import beava` works
# in Python. No Rust toolchain needed on the user's box — we did the
# cargo build in CI.
#
# Why GitHub Releases (not PyPI) — beava is pre-PyPI-publish; the
# release-wheels.yml workflow uploads platform wheels to GH Releases
# on every tag. When PyPI publishing lands, this script keeps working
# (the wheels stay on GH) and `pip install beava` becomes the
# preferred form.

set -eu

REPO="${BEAVA_REPO:-beava-dev/beava}"
RELEASE_TAG="${BEAVA_VERSION:-latest}"

# ─── platform detection ───────────────────────────────────────────────
os="$(uname -s)"
arch="$(uname -m)"
case "${os}-${arch}" in
  Darwin-arm64)               wheel_tag="macosx_11_0_arm64" ;;
  Darwin-x86_64)              wheel_tag="macosx_10_12_x86_64" ;;
  Linux-x86_64)               wheel_tag="manylinux_2_17_x86_64.manylinux2014_x86_64" ;;
  Linux-aarch64|Linux-arm64)  wheel_tag="manylinux_2_17_aarch64.manylinux2014_aarch64" ;;
  *)
    printf >&2 "beava installer: unsupported platform %s-%s\n" "$os" "$arch"
    printf >&2 "  Supported: Darwin-arm64, Darwin-x86_64, Linux-x86_64, Linux-aarch64\n"
    printf >&2 "  Or run the server in Docker:\n"
    printf >&2 "    docker run -p 8080:8080 beavadev/beava:edge\n"
    exit 1
    ;;
esac

# ─── prerequisites ────────────────────────────────────────────────────
have() { command -v "$1" >/dev/null 2>&1; }

PIP=""
if have pip3; then PIP=pip3
elif have pip; then PIP=pip
elif have python3; then PIP="python3 -m pip"
else
  printf >&2 "beava installer: pip not found.\n"
  printf >&2 "  Install Python 3.10+ first (https://python.org), then re-run.\n"
  exit 1
fi

if ! have curl; then
  printf >&2 "beava installer: curl not found. Install curl, then re-run.\n"
  exit 1
fi

# ─── locate wheel asset ───────────────────────────────────────────────
# Resolve "latest" through the API. For an explicit version
# (BEAVA_VERSION=v0.0.0), hit the tagged-release endpoint instead.
if [ "$RELEASE_TAG" = "latest" ]; then
  api="https://api.github.com/repos/${REPO}/releases/latest"
else
  api="https://api.github.com/repos/${REPO}/releases/tags/${RELEASE_TAG}"
fi

# Pull just the browser_download_url lines and pattern-match the
# wheel for our platform. Avoids a jq dependency.
url=$(curl -fsSL "$api" \
  | grep -oE '"browser_download_url"[^"]*"[^"]+\.whl"' \
  | sed -E 's/.*"(https[^"]+)".*/\1/' \
  | grep -- "-${wheel_tag}\.whl$" \
  | head -n 1 || true)

if [ -z "$url" ]; then
  printf >&2 "beava installer: no wheel asset matching %s on release %s\n" "$wheel_tag" "$RELEASE_TAG"
  printf >&2 "  Inspected: %s\n" "$api"
  printf >&2 "  This may mean the release for this platform isn't published yet.\n"
  printf >&2 "  Workaround: docker run -p 8080:8080 beavadev/beava:edge\n"
  exit 1
fi

# ─── install ──────────────────────────────────────────────────────────
printf "beava installer: downloading and installing\n"
printf "  platform : %s-%s\n" "$os" "$arch"
printf "  wheel    : %s\n" "$(basename "$url")"

# Detect whether the active Python is inside a virtualenv / conda env.
# `pip install --user` is rejected inside venvs ("User site-packages
# are not visible in this virtualenv"), so we install into the env
# itself when one is active. Outside any env, `--user` keeps the
# install isolated to ~/.local rather than touching system site-packages
# (and avoids PEP 668 on system-managed Pythons).
in_env=""
if [ -n "${VIRTUAL_ENV:-}" ] || [ -n "${CONDA_PREFIX:-}" ]; then
  in_env=1
elif command -v python3 >/dev/null 2>&1; then
  if python3 -c 'import sys; raise SystemExit(0 if sys.prefix != sys.base_prefix else 1)' 2>/dev/null; then
    in_env=1
  fi
fi

if [ -n "$in_env" ]; then
  printf "  target   : active Python env (\$VIRTUAL_ENV / \$CONDA_PREFIX)\n\n"
  PIP_USER_FLAG=""
else
  printf "  target   : --user (~/.local on Linux, ~/Library/Python/<ver> on macOS)\n\n"
  PIP_USER_FLAG="--user"
fi

# Run pip; capture combined output so we can detect PEP 668 on failure
# without losing the user-visible log line.
log=$(mktemp 2>/dev/null) || log="/tmp/beava-install.$$.log"
trap 'rm -f "$log"' EXIT

# shellcheck disable=SC2086 # PIP_USER_FLAG intentionally word-split when set
if $PIP install $PIP_USER_FLAG --upgrade "$url" >"$log" 2>&1; then
  cat "$log"
elif grep -q 'externally-managed-environment' "$log"; then
  cat "$log"
  if [ -n "${BEAVA_NO_BREAK_SYSTEM:-}" ]; then
    printf >&2 "\nbeava installer: pip refused due to PEP 668 (system-managed Python).\n"
    printf >&2 "  BEAVA_NO_BREAK_SYSTEM is set — not retrying. Either:\n"
    printf >&2 "    1. Unset BEAVA_NO_BREAK_SYSTEM and re-run, or\n"
    printf >&2 "    2. Install into a venv:\n"
    printf >&2 "         python3 -m venv ~/.beava-venv\n"
    printf >&2 "         source ~/.beava-venv/bin/activate\n"
    printf >&2 "         curl -fsSL .../scripts/install.sh | sh\n"
    exit 1
  fi
  printf "\nbeava installer: pip refused (PEP 668 system-managed Python).\n"
  printf "  Retrying with --break-system-packages (still scoped to %s).\n" \
    "$([ -n "$PIP_USER_FLAG" ] && echo "--user / ~/.local" || echo "the active env")"
  printf "  Set BEAVA_NO_BREAK_SYSTEM=1 to opt out next time.\n\n"
  # shellcheck disable=SC2086
  $PIP install $PIP_USER_FLAG --break-system-packages --upgrade "$url" || exit 1
else
  cat "$log" >&2
  exit 1
fi

# ─── PATH auto-add (idempotent) ──────────────────────────────────────
# When installing outside an env, append the user-scripts dir to PATH
# in the user's shell rc. Mirrors what bun / pixi / rustup / uv do.
# Inside a venv/conda env the env's bin/ is already on PATH from
# `activate`, so this is skipped.
#
# Skip entirely with BEAVA_NO_PATH_UPDATE=1.
if [ -z "$in_env" ] && [ -z "${BEAVA_NO_PATH_UPDATE:-}" ]; then
  py_cmd=""
  if have python3; then py_cmd=python3
  elif have python; then py_cmd=python
  fi

  user_bin=""
  if [ -n "$py_cmd" ]; then
    user_bin=$($py_cmd -c 'import sysconfig; print(sysconfig.get_paths(scheme="posix_user")["scripts"])' 2>/dev/null || true)
  fi

  if [ -n "$user_bin" ] && ! echo ":$PATH:" | grep -q ":$user_bin:"; then
    case "${SHELL:-}" in
      */zsh)  rc="$HOME/.zshrc" ;;
      */bash)
        # bash on macOS reads .bash_profile; on Linux reads .bashrc for
        # interactive non-login shells (the common terminal case).
        if [ "$os" = "Darwin" ]; then rc="$HOME/.bash_profile"; else rc="$HOME/.bashrc"; fi
        ;;
      */fish) rc="$HOME/.config/fish/config.fish" ;;
      *)      rc="$HOME/.profile" ;;
    esac

    if [ -f "$rc" ] && grep -qF '# added by beava installer' "$rc"; then
      : # already added in a prior install run — don't duplicate
    else
      mkdir -p "$(dirname "$rc")"
      {
        printf '\n# added by beava installer\n'
        case "${SHELL:-}" in
          */fish) printf 'set -gx PATH %s $PATH\n' "$user_bin" ;;
          *)      printf 'export PATH="%s:$PATH"\n' "$user_bin" ;;
        esac
      } >> "$rc"
      printf "\nbeava installer: added %s to PATH in %s\n" "$user_bin" "$rc"
      printf "  Open a new shell, or run:  source %s\n" "$rc"
    fi
  fi
fi

# ─── post-install message ─────────────────────────────────────────────
printf "\n"
printf "beava installed.\n"
printf "  Try it:    beava --help\n"
printf "  Quickstart: https://beava.dev/docs/quickstart\n"
printf "\n"
if ! have beava; then
  printf "Note: %s may not be on \$PATH yet for this shell.\n" "${user_bin:-the Python user-scripts dir}"
  printf "  Open a new shell, or add it manually to your shell rc:\n"
  printf "    export PATH=\"%s:\$PATH\"\n" "${user_bin:-\$HOME/.local/bin}"
fi
