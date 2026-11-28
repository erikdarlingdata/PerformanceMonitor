dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q > b.log 2>&1; echo "Total: Build: $(grep -E "(Warning|Error)[(]s[)]" b.log | paste -sd " ")"; grep -E "error " b.log | sort -u | cut -c1-230 | sed "s/^/Total: /" | head -6
cd Darling/Darling.Tests/bin/Debug/net10.0-windows
./Darling.Tests.exe -class "*ComposeFleetRawWindowLiveTests*" > o.log 2>&1
grep -E "Total:|\[FAIL\]" o.log | head -4
grep -E "DIAG9943 Fleet (server|pages)" diag9943.txt | cut -c1-250 | sed 's/^ */Total: /'
grep -E "Scan|Workers (Planned|Launched)|Execution Time|^Limit|Gather|ChunkAppend" diag9943.txt | cut -c1-200 | sed 's/^ */Total: /' | head -40
