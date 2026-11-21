#!/bin/bash
# Cross-check the Rust binary against scripts/reference.py on generated data.
# Usage: scripts/cross_check.sh BINARY SEED:EVENTS [SEED:EVENTS ...]
# Needs `generate` on PATH, python3, and a python with polars for compare.py.
set -u
BIN=$1; shift
HERE=$(cd "$(dirname "$0")" && pwd)
PY=${PY:-python3}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
fail=0
for spec in "$@"; do
  seed=${spec%%:*}; ev=${spec##*:}
  rm -rf "$WORK"/*
  generate --seed "$seed" --events "$ev" --clicks $((ev / 4)) --output "$WORK/d" >/dev/null
  "$BIN" "$WORK/d" "$WORK/o" 2>/dev/null || { echo "seed $seed: binary failed"; fail=1; continue; }
  python3 "$HERE/reference.py" "$WORK/d" "$WORK/r"
  res=$("$PY" "$HERE/compare.py" "$WORK/o" "$WORK/r" | tail -1)
  echo "seed $seed events $ev: $res  window $(tr -d '\n ' < "$WORK/d/manifest.json")"
  [ "$res" = MATCH ] || fail=1
done
exit $fail
