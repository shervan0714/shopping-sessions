#!/bin/bash
# Regenerate pgo/merged.profdata, the profile the Dockerfile builds with.
# Usage: scripts/pgo.sh DATA_DIR [DATA_DIR ...]   (e.g. a 20M practice set, the dev set)
#
# The profile must come from the same compiler as the Docker build (rustc
# 1.85.1, LLVM 19) and the same source: rerun this after every source change.
# A stale profile is still safe (LLVM ignores functions whose shape changed),
# only less useful. Needs: rustup toolchain install 1.85.1 -c llvm-tools-preview
set -eu
HERE=$(cd "$(dirname "$0")/.." && pwd)
TC=1.85.1
T=x86_64-unknown-linux-gnu
PROF=$(mktemp -d)
WORK=$(mktemp -d)
trap 'rm -rf "$PROF" "$WORK"' EXIT
cd "$HERE"
# Same flags as the Dockerfile's main build, plus instrumentation.
RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static -Cprofile-generate=$PROF" \
  CARGO_TARGET_DIR="$WORK/target" cargo +$TC build --release --locked --target $T
BIN="$WORK/target/$T/release/shopping-sessions"
for d in "$@"; do
  for n in 8 3; do
    rm -rf "$WORK/out"
    NCPU=$n "$BIN" "$d" "$WORK/out" 2>/dev/null
  done
done
# The fixtures exercise the rare paths (bad lines, odd IDs, edge timestamps).
for f in fixtures/*/; do
  in="$f/input"; [ -d "$in" ] || in="$f/data"
  rm -rf "$WORK/out"; "$BIN" "$in" "$WORK/out" 2>/dev/null || true
done
PROFDATA=$(ls "$(rustc +$TC --print sysroot)"/lib/rustlib/*/bin/llvm-profdata)
mkdir -p pgo
"$PROFDATA" merge -o pgo/merged.profdata "$PROF"
ls -l pgo/merged.profdata
