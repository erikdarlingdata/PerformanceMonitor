#!/usr/bin/env bash
# #9320: a sorted monthly merge at the 512MB limit on 2 threads (as shipped before) against 1 thread, on generated
# files shaped like field ones: highly compressible numeric rows across 6 servers, one file per table. Each merge is
# its own process, so the peak working set is that merge's alone. Run from the repo root. One summary line per merge,
# each starting "Total:" so a wrapper that only echoes result lines shows it.
# PERFMON_COLS, WAIT_COLS and LOG_COLS set the numeric column count of each shape (to match an on-disk size).
set -u
cd "$(dirname "$0")"
dotnet build -c Release -nologo -v q 2>&1 | grep -E " error |Warn|Elapsed" | head -5
DLL=bin/Release/net10.0/CompactionRepro.dll

seed() {
  s=$(mktemp)
  dotnet $DLL "$@" --profile-only --keep > "$s" 2>&1
  grep "Temp dir retained" "$s" | sed 's/.*: //' | tr -d '\r'
  grep -q "Temp dir retained" "$s" || tail -n 6 "$s" | cut -c1-200 | sed 's/^/Total: seed failed: /' >&2
  rm -f "$s"
}

field() { grep -E "$1" "$2" | head -1 | sed -E "s/^ *$3: *//" | tr -s ' '; }

run() {
  label=$1; dir=$2; shift 2
  out=$(mktemp)
  dotnet $DLL --merge-files "$dir" "$@" > "$out" 2>&1
  echo "Total: $label | wall $(field 'Wall time' "$out" 'Wall time') | peak $(field 'Peak WS' "$out" 'Peak WS') | mixed groups $(field 'mixing' "$out" 'Row groups mixing servers') | rows $(field 'Row count' "$out" 'Row count') | fallbacks $(grep -c 'sort fallback' "$out") | $(field 'Failure' "$out" 'Failure')"
  rm -f "$out"
}

shape() {
  table=$1; rows=$2; cols=$3
  d=$(seed --seed-files 1 --seed-rows "$rows" --seed-servers 6 --seed-numeric-cols "$cols" --seed-text-cols 0 --table "$table")
  echo "Total: $table seeded: $rows rows, $(du -sk "$d" | cut -f1) KB on disk"
  for t in 2 1; do run "$table $t thread(s), 512MB" "$d" --table "$table" --threads $t --memory-limit 512MB; done
  rm -rf "$d"
}

shape perfmon_stats 1162302 "${PERFMON_COLS:-6}"
shape wait_stats 2078794 "${WAIT_COLS:-6}"
shape collection_log 447949 "${LOG_COLS:-12}"
