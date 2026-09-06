#!/usr/bin/env bash
# Static analysis + tests for the Rust replayer lambda. Runs against the host
# target (fast), so it is suitable for the per-build lint gate in CI.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE="$ROOT/lambda"

cd "$CRATE"

cargo fmt --check
cargo clippy --all-targets -- -D warnings -D clippy::pedantic -D clippy::nursery
cargo test

echo "Lambda static analysis and tests passed."
