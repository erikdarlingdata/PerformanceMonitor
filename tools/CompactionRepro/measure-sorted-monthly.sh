#!/usr/bin/env bash
# #9258: wall time, peak working set and row groups that mix servers for a monthly merge, unsorted at the 200 MB
# budget against sorted (512MB limit) at several budgets, on two seeded monthly shapes. Run from the repo root.
# Each merge is its own process, so the peak is that merge's alone: the seeds are written once, by their own runs.
# One summary line per merge, each starting "Total:" so a wrapper that only echoes result lines shows it.
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
  echo "Total: $label | $(grep -m1 'batch(es)' "$out" | sed -E 's/^ +//') | wall $(field 'Wall time' "$out" 'Wall time') | peak $(field 'Peak WS' "$out" 'Peak WS') | mixed groups $(field 'mixing' "$out" 'Row groups mixing servers') | rows $(field 'Row count' "$out" 'Row count') | $(field 'Spill on' "$out" 'Spill on disk') | fallbacks $(grep -c 'sort fallback' "$out") | $(field 'Failure' "$out" 'Failure')"
  rm -f "$out"
}

A=$(seed --seed-files 22 --seed-rows 150000 --seed-numeric-cols 50 --seed-text-cols 10 --seed-text-bytes 24 --table index_object_stats)
echo "Total: shape A (index_object_stats: 60 payload columns, 50 numeric): $(du -sm "$A" | cut -f1) MB on disk"
run "A unsorted 200MB, 4GB limit" "$A" --table unsorted_probe
for mb in 32 64 100; do run "A sorted ${mb}MB, 512MB limit" "$A" --table index_object_stats --max-batch-mb $mb; done

B=$(seed --seed-files 22 --seed-rows 60000 --seed-numeric-cols 30 --seed-text-cols 7 --seed-text-bytes 200 --table procedure_stats)
echo "Total: shape B (procedure_stats-like: 37 payload columns, 7 text of 200 bytes): $(du -sm "$B" | cut -f1) MB on disk"
run "B unsorted 200MB, 4GB limit" "$B" --table unsorted_probe
for mb in 32 64 100; do run "B sorted ${mb}MB, 512MB limit" "$B" --table procedure_stats --max-batch-mb $mb; done

rm -rf "$A" "$B"
