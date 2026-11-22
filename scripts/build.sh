#!/bin/sh
# Release build with profile-guided optimization, all inside `docker build`
# (so the profile always matches this compiler and this source).
#
#   scripts/build.sh OUT_BINARY        (TARGET_CPU=cascadelake by default)
#
# 1. build the published generator (tools/generator) and make a 2M-event
#    training set; if under 5 % of its event IDs are long (a generator from
#    before Nov 21), lengthen ~1 in 9 user, event and click IDs by a 32-char
#    hex suffix, so the profile trains the exotic-ID path the scorer's data
#    takes (~12 % of IDs); 2. build an instrumented binary and run it on that set
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
# Share of event IDs over 10 bytes, in percent.
long=$(awk -F, 'FNR > 1 { t++; v = $1; gsub(/"/, "", v); if (length(v) > 10) l++ } END { print t ? int(100 * l / t) : 0 }' "$W"/train/events/*.csv)
if [ "$long" -lt 5 ]; then
  echo "== training IDs: ${long}% long; lengthening IDs ending in 0-3 (new generator's hex suffix)"
  for f in "$W"/train/events/*.csv "$W"/train/clicks/*.csv "$W"/train/links/*.csv; do
    [ -f "$f" ] || continue
    awk -F, -v OFS=, 'FNR == 1 { print; next } {
        for (i = 1; i <= 2; i++) {
          v = $i; q = 0
          if (v ~ /^".*"$/) { q = 1; v = substr(v, 2, length(v) - 2) }
          if (v ~ /[0-3]$/) v = v "5f3a9c0e7b2d4a6f8e1c3b5d7a9f0e2c"
          $i = q ? "\"" v "\"" : v
        }
        print
      }' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
  done
else
  echo "== training IDs: ${long}% long (generator already writes long IDs)"
fi

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
