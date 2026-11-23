R=$(pwd)
cd $R/Darling/Darling.Tests/bin/Debug/net10.0*
dotnet build $R/Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q 2>&1 | grep -E "error|Build succeeded" | head -3
./Darling.Tests.exe -method "*TempPlan9350*" > /tmp/o1.txt 2>&1
echo "Total: PLAN $(grep -o 'PLAN9350.*' /tmp/o1.txt | head -1 | cut -c1-2800)"
cd $R
sed -i 's/utcNow.AddDays(-retentionDays) - await ReadRawChunkIntervalAsync(postgres, timescaleAvailable, logger, cancellationToken)/utcNow.AddDays(-retentionDays)/' Darling/PerformanceMonitor.Darling.Service/DarlingRetention.cs
git diff --stat | tail -1
dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q 2>&1 | grep -E "error|Build succeeded" | head -3
cd $R/Darling/Darling.Tests/bin/Debug/net10.0*
./Darling.Tests.exe -class "*ObjectUsageChainRunRetentionLiveTests*" > /tmp/o2.txt 2>&1
grep -E "Total:" /tmp/o2.txt | head -1
grep -E "\[FAIL\]" /tmp/o2.txt | cut -c1-300 | head
