#!/bin/sh
# Byte-length distribution of user_id, event_id and click_id in a dataset,
# and the share that misses the packed 64-bit fast path (src/idkey.rs: over
# 10 bytes, or any byte outside [0-9A-Za-z_]). Run it on data from the
# current public generator after it changes.
#
#   scripts/id_lengths.sh DATA_DIR
set -eu
d=${1:?usage: id_lengths.sh DATA_DIR}
report() {
  name=$1; shift
  # Field 1 of each data row (header skipped), surrounding quotes removed.
  awk -F, -v name="$name" -v col="$1" 'FNR == 1 { next } {
      v = $col; gsub(/^"|"$/, "", v); n = length(v); c[n]++; t++
      if (n > 10 || v ~ /[^0-9A-Za-z_]/) slow++
    } END {
      printf "%s: %d values, %.1f%% exotic (slow path)\n", name, t, t ? 100 * slow / t : 0
      for (n in c) printf "  %3d bytes: %d\n", n, c[n] | "sort -n"
      close("sort -n")
    }' "$2"/*.csv
}
report event_id 1 "$d/events"
report user_id 2 "$d/events"
report click_id 1 "$d/clicks"
