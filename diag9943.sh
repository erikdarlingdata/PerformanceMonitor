dotnet build Darling/Darling.Tests/Darling.Tests.csproj -c Debug --nologo -v q > b.log 2>&1; echo "Total: Build: $(grep -E "(Warning|Error)[(]s[)]" b.log | paste -sd " ")"; grep -E "error " b.log | sort -u | cut -c1-230 | sed "s/^/Total: /" | head -6
cd Darling/Darling.Tests/bin/Debug/net10.0-windows
./Darling.Tests.exe -class "*ComposeFleetRawWindowLiveTests*" -class "*DailySummaryCreditedPairsLiveTests*" > o.log 2>&1
grep -E "Total:|\[FAIL\]" o.log | head -4
grep -A4 "Fleet.*\[FAIL\]" o.log | cut -c1-250 | sed "s/^ */Total: /"
