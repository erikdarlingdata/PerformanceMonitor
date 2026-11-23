R=$(pwd)
B=$R/Darling/Darling.Tests/bin/Debug/net10.0
build() { dotnet build $R/Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q 2>&1 | grep -E "error|Build succeeded" | head -3; }
build
cd $B*
./Darling.Tests.exe -class "*ObjectUsageChainRunRetentionLiveTests*" > /tmp/o3.txt 2>&1
grep -E "Total:" /tmp/o3.txt | head -1
grep -E "\[FAIL\]" -A7 /tmp/o3.txt | grep -v "^--" | cut -c1-190 | sed 's/^/Total: F /' | head -40
grep -o 'PLAN9350.*' /tmp/o3.txt | head -1 | sed 's/ || /\n/g' | cut -c1-190 | sed 's/^/Total: P /' | head -30
