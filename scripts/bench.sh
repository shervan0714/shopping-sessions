#!/bin/bash
# Time the binary on a dataset, cold (page cache dropped, needs root) and warm.
# Usage: scripts/bench.sh BINARY DATA_DIR [NCPU]
set -u
BIN=$1; DATA=$2; N=${3:-8}
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT
run() {
  rm -rf "$OUT"/*
  NCPU=$N /usr/bin/time -f "wall %e s, user %U s, sys %S s, max RSS %M KB" "$BIN" "$DATA" "$OUT" 2>&1 | grep -v '^\s*$'
}
if [ -w /proc/sys/vm/drop_caches ]; then
  sync; echo 3 > /proc/sys/vm/drop_caches
  echo "== cold cache"; run
fi
echo "== warm cache"; run
du -sh "$OUT"/*
