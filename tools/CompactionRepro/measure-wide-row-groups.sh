#!/usr/bin/env bash
# #9455: the largest row-group size a sorted merge finishes at, for two seeded shapes with long text, at the production
# sorted-merge memory limit and thread count (the MergeBatchToFile defaults: no --memory-limit, no --threads). Also the
# ROW_GROUP_SIZE_BYTES check under an ORDER BY. Run from anywhere. One line per run, each starting "Total:" so a wrapper
# that only echoes result lines shows it. A size "fits" when no sorted-at-2,048 or unsorted rerun line appears.
#   DUCKDB=1.5.6.1  the DuckDB the build runs on (default: the one Lite ships)
#   SIZES="..."     row-group sizes to try
set -u
cd "$(dirname "$0")"
DUCKDB=${DUCKDB:-1.5.6.1}
dotnet build -c Release -nologo -v q -p:ReproDuckDbVersion=$DUCKDB 2>&1 | grep -E " error " | head -3
DLL=bin/Release/net10.0/CompactionRepro.dll
SIZES=${SIZES:-2048 32768 65536 98304 122880}
BOUNDS=${BOUNDS:-0 24 32 48 64 96 128}

# shape name | table | rows | numeric cols | text bytes
for shape in "long-statement-text|query_stats|358000|45|500" "longer-statement-text|query_stats|358000|45|900" "long-event-xml|system_health_events|248000|6|930"; do
  IFS='|' read -r name tbl rows ncols tbytes <<< "$shape"
  D=$(dotnet $DLL --seed-files 1 --seed-rows $rows --seed-servers 5 --seed-numeric-cols $ncols --seed-text-cols 1 --seed-text-bytes $tbytes --seed-text-distinct $rows --seed-noise 2 --table $tbl --profile-only --keep 2>&1 | grep "Temp dir retained" | sed 's/.*: //' | tr -d '\r')
  echo "Total: shape $name, table $tbl, $rows rows, DuckDB $DUCKDB, input $(du -k $D/seed_000.parquet | cut -f1) KB on disk"
  for size in $SIZES; do
    out=$(mktemp)
    dotnet $DLL --merge-files "$D" --table $tbl --row-group-size $size > "$out" 2>&1
    echo "Total: $name rows/group $size | $(grep -m1 'Peak WS' "$out" | sed -E 's/ \(baseline.*//' | tr -s ' ') | smaller-group reruns $(grep -c 'sorted at 2048\|sort fallback' "$out") | $(grep -m1 'Output row groups' "$out" | tr -s ' ') | $(grep -m1 Failure "$out" | cut -c1-90)"
    rm -f "$out"
  done
  for mb in $BOUNDS; do
    dotnet $DLL --merge-files "$D" --table $tbl --probe-bytes-bound $mb 2>&1 | grep -E "^Total:|rror" | head -3
  done
  rm -rf "$D"
done
