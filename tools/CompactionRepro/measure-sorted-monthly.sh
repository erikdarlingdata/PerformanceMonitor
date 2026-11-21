#!/usr/bin/env bash
# #9258: wall time, peak working set and row groups that mix servers for a monthly merge, unsorted at the 200 MB
# budget against sorted (512MB limit) at several budgets, on two seeded monthly shapes. Run from the repo root.
# Each merge is its own process, so the peak is that merge's alone: the seeds are written once, by their own runs.
set -u
cd "$(dirname "$0")"
dotnet build -c Release -nologo -v q 2>&1 | grep -E " error |Warn|Elapsed" | head -5
DLL=bin/Release/net10.0/CompactionRepro.dll

seed() {
  dotnet $DLL "$@" --profile-only --keep 2>&1 | grep "Temp dir retained" | sed 's/.*: //' | tr -d '\r'
}

run() {
  label=$1; dir=$2; shift 2
  echo "== $label"
  dotnet $DLL --merge-files "$dir" "$@" 2>&1 | grep -E "batch\(es\)|Wall time|Peak WS|Output:|mixing|Row count|sort fallback|Spill on|Status|Failure"
}

A=$(seed --seed-files 22 --seed-rows 150000 --seed-numeric-cols 50 --seed-text-cols 10 --seed-text-bytes 24 --table index_object_stats)
echo "shape A (index_object_stats: 60 payload columns, 50 numeric): $(du -sm "$A" | cut -f1) MB on disk in $A"
run "A unsorted 200MB, 4GB limit" "$A" --table unsorted_probe
for mb in 32 64 100; do run "A sorted ${mb}MB, 512MB limit" "$A" --table index_object_stats --max-batch-mb $mb; done

B=$(seed --seed-files 22 --seed-rows 60000 --seed-numeric-cols 30 --seed-text-cols 7 --seed-text-bytes 200 --table procedure_stats)
echo "shape B (procedure_stats-like: 37 payload columns, 7 text of 200 bytes): $(du -sm "$B" | cut -f1) MB on disk in $B"
run "B unsorted 200MB, 4GB limit" "$B" --table unsorted_probe
for mb in 32 64 100; do run "B sorted ${mb}MB, 512MB limit" "$B" --table procedure_stats --max-batch-mb $mb; done

rm -rf "$A" "$B"
