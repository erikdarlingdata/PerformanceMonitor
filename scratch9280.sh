#!/bin/bash
# #9280 scratch rig driver: seed, then cold and warm explains per compression setting. Run on NADAGAME through the wrapper. Removed in the last commit.
set -u
: "${ID:?}"
PGBIN="/c/GitHub/worktrees/rigs/$ID/pgsql/bin"
DATA='C:\GitHub\worktrees\rigs\'"$ID"'\data'
dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q 2>&1 | grep -E "error CS|warning CS|Build succeeded|Build FAILED" | head -n 10
cd Darling/Darling.Tests/bin/Debug/net10.0-windows || exit 1
: > /c/runner/logs/m9280-plans.txt
run() { ./Darling.Tests.exe -class "*Scratch9280Rig*" 2>&1 | grep -E "S9280|Total:|\[FAIL\]|Exception|Error" | head -n 30; }
restart() { "$PGBIN/pg_ctl.exe" restart -D "$DATA" -w -t 180 > "/c/runner/logs/m9280-restart-$ID.txt" 2>&1; }
if [ "${S9280_SKIP_SEED:-0}" != "1" ]; then S9280=seed run; fi
for TAG in base segnames ordnames; do
  export S9280_TAG=$TAG
  if [ "$TAG" = "segnames" ]; then S9280=recompress S9280_SEG='server_id, database_name, object_name' S9280_ORD='collection_time DESC' run; export S9280_QS=0; fi
  if [ "$TAG" = "ordnames" ]; then S9280=recompress S9280_SEG='server_id' S9280_ORD='database_name, object_name, collection_time DESC' run; export S9280_QS=0; fi
  restart
  S9280=cold run
  S9280=warm run
done
