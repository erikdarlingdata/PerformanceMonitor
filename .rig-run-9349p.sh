R=$(pwd)
B=$R/Darling/Darling.Tests/bin/Debug/net10.0
F=Darling/PerformanceMonitor.Darling.Service/DarlingRetention.cs
build() { dotnet build $R/Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q 2>&1 | grep -E "error|Build succeeded" | head -3; }
build
cd $B*
./Darling.Tests.exe -method "*TempPlan9350*" > /tmp/o1.txt 2>&1
echo "Total: PLAN $(grep -o 'PLAN9350.*' /tmp/o1.txt | head -1 | cut -c1-3000)"
cd $R
sed -i 's/AND   r.database_name = d.database_name";/AND   r.database_name = d.database_name AND false";/' $F
git diff --stat | tail -1
build
cd $B*
./Darling.Tests.exe -class "*ObjectUsageChainRunRetentionLiveTests*" > /tmp/o3.txt 2>&1
echo "Total: REVERT-DEADPAIRS"; grep -E "Total:" /tmp/o3.txt | head -1; grep -E "\[FAIL\]" /tmp/o3.txt | cut -c1-300 | head
cd $R
git checkout $F
sed -i 's/utcNow.AddDays(-retentionDays) - await ReadRawChunkIntervalAsync(postgres, timescaleAvailable, logger, cancellationToken)/utcNow.AddDays(-retentionDays)/' $F
git diff --stat | tail -1
build
cd $B*
./Darling.Tests.exe -class "*ObjectUsageChainRunRetentionLiveTests*" > /tmp/o2.txt 2>&1
echo "Total: REVERT-MARGIN"; grep -E "Total:" /tmp/o2.txt | head -1; grep -E "\[FAIL\]" /tmp/o2.txt | cut -c1-300 | head
