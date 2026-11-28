dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q > b.log 2>&1; echo "Total: Build: $(grep -E "(Warning|Error)[(]s[)]" b.log | paste -sd " ")"; grep -E "error " b.log | sort -u | cut -c1-230 | sed "s/^/Total: /" | head -6
cd Darling/Darling.Tests/bin/Debug/net10.0-windows
./Darling.Tests.exe -class "*ComposeFleetRawWindowLiveTests*" > o.log 2>&1
grep -E "Total:|\[FAIL\]" o.log | head -4
grep -E "DIAG9943 Fleet (server|pages)" diag9943.txt | cut -c1-250 | sed 's/^ */Total: /'
awk '/DIAG9943 Fleet pages=7975/{f=1} /DIAG9943 Fleet pages=10474/{f=0} f' diag9943.txt | grep -v "^ *Buffers" | grep -E "Scan|Workers|Execution|Index Cond|Filter|Heap|Aggregate" | cut -c1-210 | sed "s/^ */Total: /" | head -40
