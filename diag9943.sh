dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q > b.log 2>&1; echo "Total: Build: $(grep -E "(Warning|Error)[(]s[)]" b.log | paste -sd " ")"; grep -E "error " b.log | sort -u | cut -c1-230 | sed "s/^/Total: /" | head -6
cd Darling/Darling.Tests/bin/Debug/net10.0-windows
./Darling.Tests.exe -class "*ComposeFleetRawWindowLiveTests*" -class "*DailySummaryCreditedPairsLiveTests*" > o.log 2>&1
grep -E "Total:|\[FAIL\]" o.log | head -4
grep -A1 -E "pages, the statement" o.log | cut -c1-200 | sed 's/^ */Total: /' | head -3
grep -E "DIAG9943 Fleet" diag9943.txt | cut -c1-200 | sed 's/^ */Total: /'
awk '/DIAG9943 Fleet pages/{f=1} /DIAG9943 Daily/{f=0} f' diag9943.txt | grep -E "Scan|Workers|Join|Execution Time|Buffers: shared hit=[0-9]+$" | cut -c1-200 | sed 's/^ */Total: /' | head -24
awk '/DIAG9943 Daily noindexscan/{f=1} f' diag9943.txt | grep -E "Scan|Execution Time|Join" | cut -c1-200 | sed 's/^ */Total: /' | head -24
