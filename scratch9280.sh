#!/bin/bash
# #9280 scratch rig driver: seed, restart PostgreSQL, cold explain, warm explain. Run on NADAGAME through the wrapper. Removed in the last commit.
set -u
: "${ID:?}"
VARIANT="${S9280_VARIANT:-dev}"
export S9280_VARIANT="$VARIANT"
PGBIN="/c/GitHub/worktrees/rigs/$ID/pgsql/bin"
DATA='C:\GitHub\worktrees\rigs\'"$ID"'\data'
dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q 2>&1 | grep -E "error CS|warning CS|Build succeeded|Build FAILED" | head -n 10
cd Darling/Darling.Tests/bin/Debug/net10.0-windows || exit 1
: > /c/runner/logs/m9280-plans.txt
if [ "${S9280_SKIP_SEED:-0}" != "1" ]; then
  S9280=seed ./Darling.Tests.exe -class "*Scratch9280Rig*" 2>&1 | grep -E "S9280|Total:|\[FAIL\]|Exception|Error" | head -n 40
fi
"$PGBIN/pg_ctl.exe" restart -D "$DATA" -w -t 180 2>&1 | tail -n 2
S9280=cold ./Darling.Tests.exe -class "*Scratch9280Rig*" 2>&1 | grep -E "S9280|Total:|\[FAIL\]|Exception|Error" | head -n 20
S9280=warm ./Darling.Tests.exe -class "*Scratch9280Rig*" 2>&1 | grep -E "S9280|Total:|\[FAIL\]|Exception|Error" | head -n 20
