#!/usr/bin/env bash
# #9320: the memory limit at which a sorted monthly merge runs out of memory, on 2 threads against 1 thread, for one
# generated field-shaped file (perfmon_stats-like: 1.16M rows, 6 servers, nearly constant numeric runs). Run from the
# repo root. One line per merge, each starting "Total:" so a wrapper that only echoes result lines shows it.
set -u
cd "$(dirname "$0")"
dotnet build -c Release -nologo -v q 2>&1 | grep -E " error " | head -3
DLL=bin/Release/net10.0/CompactionRepro.dll
D=$(dotnet $DLL --seed-files 1 --seed-rows 1162302 --seed-servers 6 --seed-numeric-cols 12 --seed-text-cols 0 --seed-noise 2 --table perfmon_stats --profile-only --keep 2>&1 | grep "Temp dir retained" | sed 's/.*: //' | tr -d '\r')
out=$(mktemp)
for lim in ${LIMITS:-128 160 192 224 256 320}; do
  for t in 2 1; do
    dotnet $DLL --merge-files "$D" --table perfmon_stats --threads $t --memory-limit ${lim}MB > "$out" 2>&1
    echo "Total: limit ${lim}MB, $t thread(s) | $(grep -m1 'Peak WS' "$out" | sed -E 's/ \(baseline.*//' | tr -s ' ') | sort fallbacks $(grep -c 'sort fallback' "$out") | $(grep -m1 Failure "$out" | cut -c1-80)"
  done
done
rm -f "$out"
rm -rf "$D"
