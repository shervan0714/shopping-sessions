#!/bin/bash
# Alternating A/B benchmark: run A and B in turn ROUNDS times on DATA and
# print the median of each stage (from the binary's stderr timeline) and of
# the total. Alternating cancels most drift of a shared machine.
#
#   scripts/ab.sh BIN_A BIN_B DATA [ROUNDS=9] [NCPU=8]
#   A_ENV="SS_INPUT=pread" B_ENV="" scripts/ab.sh BIN BIN DATA   # same binary, two settings
#
# Pin to the scorer's shape when the machine is bigger: taskset -c 0-7 scripts/ab.sh ...
set -u
A=$1; B=$2; D=$3; R=${4:-9}; N=${5:-8}
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT
for i in $(seq "$R"); do
  for x in A B; do
    if [ $x = A ]; then bin=$A; e=${A_ENV:-}; else bin=$B; e=${B_ENV:-}; fi
    rm -rf "$OUT"/*
    line=$(env $e NCPU="$N" "$bin" "$D" "$OUT" 2>&1 | grep -E '^\[' | sed -E 's/^\[ *([0-9.]+)s\] ([a-z]+).*/\2=\1/' | tr '\n' ' ')
    echo "$x $line"
  done
done | python3 -c '
import sys, statistics as st
runs = {"A": [], "B": []}
for l in sys.stdin:
    x, *kv = l.split()
    if kv:
        runs[x].append([(k.split("=")[0], float(k.split("=")[1])) for k in kv])
for x in "AB":
    if not runs[x]:
        print(x, "no successful runs"); continue
    names = [k for k, _ in runs[x][0]]
    d = {k: [] for k in names}
    for r in runs[x]:
        prev = 0.0
        for k, t in r:
            d.setdefault(k, []).append(t - prev); prev = t
    tot = [r[-1][1] for r in runs[x]]
    print(x, " ".join(f"{k}={st.median(v):.3f}" for k, v in d.items() if k != "done"),
          f"| total median {st.median(tot):.3f} (min {min(tot):.3f}, n={len(tot)})")
'
