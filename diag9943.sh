dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q > b.log 2>&1; echo "Total: Build: $(grep -E "(Warning|Error)[(]s[)]" b.log | paste -sd " ")"; grep -E "error " b.log | sort -u | cut -c1-230 | sed "s/^/Total: /" | head -6
cd Darling/Darling.Tests/bin/Debug/net10.0-windows
./Darling.Tests.exe -class "*ComposeFleetRawWindowLiveTests*" -class "*DailySummaryCreditedPairsLiveTests*" -class "*TopCpuQueriesTextLiveTests*" > o.log 2>&1
grep -E "Total:|\[FAIL\]" o.log | head -8
grep -E "DIAG9943 (Fleet|Daily noindex)|Scan on|Scan using|Join|Seq Scan|Parallel|Heap Fetches|Execution Time|^ *Buffers: shared hit=[0-9]+$" diag9943.txt | cut -c1-170 | sed 's/^ */Total: /' | awk '!seen[$0]++' | head -50
