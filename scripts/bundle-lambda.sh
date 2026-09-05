#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="aarch64-unknown-linux-gnu"
CRATE="lambda"
BIN="sqs-replayer-lambda"
OUT="assets/lambda"

cd "$ROOT"

cargo zigbuild --manifest-path "$CRATE/Cargo.toml" --release --target "$TARGET"

mkdir -p "$OUT"
cp "$CRATE/target/$TARGET/release/$BIN" "$OUT/bootstrap"
chmod +x "$OUT/bootstrap"

echo "Bundled Rust lambda to $OUT/bootstrap"