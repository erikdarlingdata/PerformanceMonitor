#!/bin/bash
# #9280 r4a scratch driver. Run on NADAGAME through the wrapper. Removed before the push.
set -u
: "${ID:?}"
PGBIN="/c/GitHub/worktrees/rigs/$ID/pgsql/bin"
DATA='C:\GitHub\worktrees\rigs\'"$ID"'\data'
dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q 2>&1 | grep -E "error CS|warning CS|Build succeeded|Build FAILED" | head -n 10
cd Darling/Darling.Tests/bin/Debug/net10.0-windows || exit 1
: > /c/runner/logs/m9280r4a-plans.txt
run() { ./Darling.Tests.exe -class "*Scratch9280Rig*" 2>&1 | grep -E "S9280|\[FAIL\]|Exception|Error" | head -n 30; }
restart() { "$PGBIN/pg_ctl.exe" restart -D "$DATA" -w -t 180 -l "/c/runner/logs/m9280r4a-restart-$ID.txt" > /dev/null 2>&1; }
./Darling.Tests.exe -class "*TopProceduresHourlyMid*" 2>&1 | grep -E "\[FAIL\]|Total:|Exception|Assert" | head -n 30
S9280=seed run
for V in dev seek; do
  export S9280_VARIANT=$V
  restart; S9280=cold run
  S9280=warm run
done
echo "plans: C:/runner/logs/m9280r4a-plans.txt"
