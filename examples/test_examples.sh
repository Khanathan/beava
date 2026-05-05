#!/usr/bin/env bash
# Examples smoke test.
# Runs all 9 vertical demo files (3 langs x 3 verticals) against
# language-local mock backends.
#
# Missing toolchains are HARD FAILURES (exit 2), not silent skips.
# Reason: silent skips would weaken integration-regression coverage;
# CI/dev environments must have all 3 toolchains installed to verify
# all 9 demos.
#
# Exit codes:
#   0 -- all 9 demos passed
#   1 -- one or more demo failures
#   2 -- missing toolchain (npx for TS, go for Go); install + retry

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Pre-flight: assert all toolchains are present (hard-fail on missing).
missing_tools=()
command -v python3 >/dev/null 2>&1 || missing_tools+=("python3")
command -v npx     >/dev/null 2>&1 || missing_tools+=("npx (Node.js)")
command -v go      >/dev/null 2>&1 || missing_tools+=("go")

if [ ${#missing_tools[@]} -gt 0 ]; then
    echo "ERROR: required toolchains missing from PATH:" >&2
    for t in "${missing_tools[@]}"; do
        echo "  - $t" >&2
    done
    echo "" >&2
    echo "Install missing toolchains or run on CI environment with all 3 toolchains." >&2
    echo "Cannot verify the 9 vertical demos without all 3 languages -- aborting." >&2
    exit 2
fi

failed=0

run() {
    local label="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        echo "PASS  $label"
    else
        echo "FAIL  $label"
        failed=$((failed + 1))
    fi
}

# Python (3 demos)
for f in adtech fraud ecommerce; do
    (cd "$REPO_ROOT/examples/python" && run "python/${f}.py" python3 "${f}.py") || true
done

# TypeScript (3 demos)
for f in adtech fraud ecommerce; do
    run "typescript/${f}.ts" npx --yes tsx "$REPO_ROOT/examples/typescript/${f}.ts" || true
done

# Go (3 demos)
for f in adtech fraud ecommerce; do
    run "go/${f}.go" go run "$REPO_ROOT/examples/go/${f}.go" || true
done

if [ $failed -eq 0 ]; then
    echo ""
    echo "OK -- all 9 demos passed (3 langs x 3 verticals)"
    exit 0
else
    echo ""
    echo "FAIL -- $failed demo(s) failed"
    exit 1
fi
