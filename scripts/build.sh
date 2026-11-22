#!/bin/sh
# Release build with profile-guided optimization, all inside `docker build`
# (so the profile always matches this compiler and this source).
#
#   scripts/build.sh OUT_BINARY        (TARGET_CPU=cascadelake by default)
#
# 1. build the published generator (tools/generator) and make a 2M-event
#    training set; 2. build an instrumented binary and run it on that set
#    (NCPU 8 and 3) and on every fixture (rare paths); 3. merge the profile
#    and build the final binary with it. If the training run cannot execute
#    here (e.g. a build host without the target CPU's features) or
#    llvm-profdata is missing, the final build is the same minus PGO.
set -eu
OUT=$1
CPU=${TARGET_CPU:-cascadelake}
ARCH=$(uname -m)
T="$ARCH-unknown-linux-gnu"
BASE="-C target-feature=+crt-static"
if [ "$ARCH" = x86_64 ]; then BASE="-C target-cpu=$CPU $BASE"; fi
W=${PGO_WORK:-/tmp/pgo-work}
rm -rf "$W"; mkdir -p "$W"

echo "== generator + training data"
cargo build --release --locked --manifest-path tools/generator/Cargo.toml --target-dir "$W/gen-target"
"$W/gen-target/release/generate" --seed 7 --events 2000000 --clicks 500000 --output "$W/train" >/dev/null

echo "== instrumented build ($BASE)"
RUSTFLAGS="$BASE -Cprofile-generate=$W/raw" cargo build --release --locked --target "$T" --target-dir "$W/target"
B="$W/target/$T/release/shopping-sessions"
trained=0
for n in 8 3; do
  rm -rf "$W/out"
  if NCPU=$n "$B" "$W/train" "$W/out" 2>/dev/null; then trained=1; fi
done
for f in fixtures/*/; do
  [ -d "$f/input" ] || continue
  rm -rf "$W/out"; "$B" "$f/input" "$W/out" >/dev/null 2>&1 || true
done

PGO=""
PD=$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/llvm-profdata 2>/dev/null | head -n 1 || true)
if [ "$trained" = 1 ] && [ -n "$PD" ] && "$PD" merge -o "$W/merged.profdata" "$W/raw"; then
  PGO="-Cprofile-use=$W/merged.profdata -Cllvm-args=-no-pgo-warn-mismatch"
  echo "== final build with PGO"
else
  echo "== final build WITHOUT PGO (trained=$trained, llvm-profdata='${PD}')"
fi
RUSTFLAGS="$BASE $PGO" cargo build --release --locked --target "$T"
cp "target/$T/release/shopping-sessions" "$OUT"
strip "$OUT" 2>/dev/null || true
rm -rf "$W"
