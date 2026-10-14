<#
.SYNOPSIS
    #5794 load test: measures a Darling store copy that inflate-query-store.sql has inflated.

.DESCRIPTION
    Each step prints a Markdown table that pastes into the issue. The steps:

      Reads        (a) The day-scan reads through the real surfaces, with 1-day and 7-day windows: the four MCP Query
                   Store tools, the same reads through the web dashboard's /api/read routes (the Query Store page),
                   and a Custom Views panel (-CustomViewSpec). Each window ends at the server's newest Query Store
                   row (as_of), not at now: the copy gets no new rows, so a window that ends at now empties as the
                   test goes on. Prints the time each call took, as the client saw it, then the slow-read rows the
                   service wrote for them.
      ReadLatency  (a) The service's own read times for the window that started at -Since, beside the read budgets.
                   The service writes them once an hour, so run this step after that hour has closed.
      Jobs         (b) The hourly work that grows with N: the three Query Store builders (plan regression daily totals,
                   Query Store top daily summary, compose rollup) and the store object convergence pass, which holds
                   the Query Store partition upkeep, read from the service log (-ServiceLog); the daily retention
                   purge from the collection log; and the TimescaleDB aggregate refresh and compression jobs.
      Writes       (c) The Query Store collectors' write time per cycle, from the collection log and get_collector_cost.
      Compression  (d) One closed day of the wide table in a compressed hypertable: size and scan time, plain and
                   compressed.

    Run it against a COPY. Never point it at a store that monitors production servers.

.EXAMPLE
    ./measure.ps1 -Psql 'C:\pg\bin\psql.exe' -Store 'host=127.0.0.1 port=5432 dbname=darling user=darling' -Steps Jobs,Compression

.EXAMPLE
    ./measure.ps1 -Psql psql -Store '...' -Steps Reads -ServerName SQL01 -McpToken (Get-Content mcp.token) -WebToken (Get-Content web.token) -CustomViewSpec panel.json
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Psql,
    [Parameter(Mandatory)] [string] $Store,
    [ValidateSet('Reads', 'ReadLatency', 'Jobs', 'Writes', 'Compression')]
    [string[]] $Steps = @('Jobs', 'Compression'),
    [string] $ServerName,
    [string] $McpUrl = 'http://localhost:5152/',
    [string] $McpToken,
    [string] $WebUrl = 'http://localhost:5153',
    [string] $WebToken,
    [string] $CustomViewSpec,
    [string] $Since,
    [string] $Day,
    [int] $Hours = 2,
    [string] $ServiceLog
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot

# Runs one of the step files and turns psql's unaligned output (header line, then rows, '|' between columns) into a
# Markdown table. Each step file returns exactly one result set.
function Invoke-StoreStep {
    param([string] $File, [hashtable] $Variables = @{})

    $arguments = @('-X', '-q', '-A', '-F', '|', '-P', 'footer=off', '-v', 'ON_ERROR_STOP=1', '-d', $Store)
    foreach ($key in $Variables.Keys) {
        $arguments += @('-v', "$key=$($Variables[$key])")
    }
    $arguments += @('-f', (Join-Path $here $File))
    $lines = & $Psql @arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "psql failed on ${File}: $($lines -join [Environment]::NewLine)"
    }

    $rows = @($lines | ForEach-Object { "$_" } | Where-Object { $_ -match '\|' -and $_ -notmatch '^(NOTICE|WARNING):' })
    if ($rows.Count -eq 0) {
        return '(no rows)'
    }
    $header = $rows[0].Split('|')
    $out = @(('| ' + ($header -join ' | ') + ' |'), ('|' + ((@('---') * $header.Count) -join '|') + '|'))
    foreach ($row in $rows | Select-Object -Skip 1) {
        $out += '| ' + ($row.Split('|') -join ' | ') + ' |'
    }
    return $out -join [Environment]::NewLine
}

$script:mcpSession = $null
$script:mcpId = 0

# One JSON-RPC call to the MCP endpoint (streamable HTTP). The answer comes back as JSON or as server-sent events.
function Invoke-Mcp {
    param([string] $Method, [hashtable] $Params, [switch] $Notification)

    $headers = @{ Accept = 'application/json, text/event-stream' }
    if ($McpToken) { $headers['Authorization'] = "Bearer $McpToken" }
    if ($script:mcpSession) { $headers['Mcp-Session-Id'] = $script:mcpSession }

    $body = @{ jsonrpc = '2.0'; method = $Method }
    if ($Params) { $body['params'] = $Params }
    if (-not $Notification) { $script:mcpId++; $body['id'] = $script:mcpId }

    $response = Invoke-WebRequest -Uri $McpUrl -Method Post -Headers $headers -ContentType 'application/json' `
        -Body ($body | ConvertTo-Json -Depth 20 -Compress) -SkipHttpErrorCheck
    if ($response.Headers['Mcp-Session-Id']) { $script:mcpSession = @($response.Headers['Mcp-Session-Id'])[0] }
    if ($response.StatusCode -ge 400) { throw "MCP $Method returned HTTP $($response.StatusCode)" }
    if ($Notification) { return $null }

    $text = [string] $response.Content
    if ($text -match '(?m)^data:') {
        $text = (($text -split "`n") | Where-Object { $_ -like 'data:*' } | Select-Object -Last 1).Substring(5)
    }
    return $text | ConvertFrom-Json
}

# The status words a Darling read puts on the wire when it did not answer the question: a name that resolves to nothing
# is "invalid" (or "not_found"), a caught exception or a statement timeout is "error". "unavailable", "precondition"
# and "not_collected" say that the read had no data to read. A read like that returns in milliseconds, so the step must
# count it as a failure, not as a fast read (#5794 review rounds 1 and 2).
$script:failureStatuses = @('error', 'invalid', 'not_found', 'ambiguous', 'conflict', 'timeout', 'unavailable', 'precondition', 'not_collected')

# The reason a tool text or a web body is a failed read, or $null when the read answered. The text is the read's JSON,
# or a bare sentence for a store fault (the registry read failing).
function Get-ReadFailure {
    param([string] $Text)

    if ([string]::IsNullOrWhiteSpace($Text)) { return 'empty answer' }
    $trimmed = $Text.TrimStart()
    if (-not ($trimmed.StartsWith('{') -or $trimmed.StartsWith('['))) {
        if ($trimmed -match '^(Could not |Error)') { return ($trimmed -split "`n")[0] }
        return $null
    }
    try {
        $json = $trimmed | ConvertFrom-Json
    }
    catch {
        return $null
    }
    if ($json -is [System.Management.Automation.PSCustomObject] -and $json.PSObject.Properties['status']) {
        $status = [string] $json.status
        if ($script:failureStatuses -contains $status) {
            $message = if ($json.PSObject.Properties['message']) { [string] $json.message } else { '' }
            # The read guard's text is "The store took too long to answer this read".
            if ($message -match 'timeout|canceling statement|took too long') { $status = 'timeout' }
            return "${status}: $message"
        }
    }
    return $null
}

# Throws when a tools/call answer is a failed read: a JSON-RPC error, isError, or a failure status word in the tool text.
# isError is a bool? that the server may leave out, and Set-StrictMode throws on a missing property, so it is tested
# for first.
function Assert-ToolAnswer {
    param($Answer)

    if ($Answer.PSObject.Properties['error'] -and $Answer.error) { throw "JSON-RPC error: $($Answer.error.message)" }
    if (-not $Answer.PSObject.Properties['result']) { throw 'the answer has no result' }
    $result = $Answer.result
    if ($result.PSObject.Properties['isError'] -and $result.isError) { throw 'the tool returned isError' }
    $text = ''
    if ($result.PSObject.Properties['content'] -and @($result.content).Count -gt 0 -and $result.content[0].PSObject.Properties['text']) {
        $text = [string] $result.content[0].text
    }
    $failure = Get-ReadFailure -Text $text
    if ($failure) { throw $failure }
}

function Measure-Call {
    param([string] $Surface, [string] $Read, [string] $Window, [string] $AsOf, [scriptblock] $Call)

    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    $outcome = 'ok'
    try {
        & $Call
    }
    catch {
        $outcome = 'failed: ' + ($_.Exception.Message -replace '\|', '/')
    }
    $watch.Stop()
    [pscustomobject]@{ Surface = $Surface; Read = $Read; Window = $Window; AsOf = $AsOf; Ms = $watch.ElapsedMilliseconds; Outcome = $outcome }
}

function Step-Reads {
    if (-not $ServerName) { throw 'The Reads step needs -ServerName: a real server in the copy.' }
    $since = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')
    $results = @()
    $serverLiteral = "'" + ($ServerName -replace "'", "''") + "'"

    # The anchor of every window (#5794 review round 2): one second after the server's newest wide row. The copy gets
    # no new Query Store rows, so a window that ends at now holds fewer rows each day of the test, and a read of an
    # empty window is fast. first_execution_time is the column the reads filter on, and its index makes this max cheap.
    $anchorRow = & $Psql -X -q -A -t -F '|' -v ON_ERROR_STOP=1 -d $Store -c @"
SELECT to_char(a.t, 'YYYY-MM-DD') || 'T' || to_char(a.t, 'HH24:MI:SS') || 'Z', to_char(a.t, 'YYYY-MM-DD HH24:MI:SS')
FROM
(
    SELECT date_trunc('second', max(w.first_execution_time)) + interval '1 second' AS t
    FROM collect.query_store_interval_wide AS w
    WHERE w.server_id = (SELECT s.server_id FROM collect.servers AS s WHERE s.server_name = $serverLiteral)
) AS a
WHERE a.t IS NOT NULL;
"@
    $anchorParts = "$anchorRow".Trim().Split('|')
    $asOf = $null
    if ($anchorParts.Count -eq 2) {
        $asOf = $anchorParts[0]
        $anchorTime = [datetime]::ParseExact($anchorParts[1], 'yyyy-MM-dd HH:mm:ss', [Globalization.CultureInfo]::InvariantCulture)
        Write-Host "Every window ends at as_of = $asOf, one second after the newest Query Store row of $ServerName."
        # The service purges wide rows older than 9 days (QueryStoreIntervalPartitions.WideHorizonDays), and the copy
        # gets no new ones, so a window that starts before that edge has lost rows to the purge.
        $purgeEdge = (Get-Date).ToUniversalTime().AddDays(-9)
        foreach ($hours in 24, 168) {
            if ($anchorTime.AddHours(-$hours) -lt $purgeEdge) {
                Write-Host "WARNING: the ${hours}h window starts before the purge edge ($($purgeEdge.ToString('yyyy-MM-dd HH:mm')) UTC). The service has removed rows from it, so its reads are faster than on a whole window. Restore a newer copy."
            }
        }
    }
    else {
        Write-Host "WARNING: $ServerName has no Query Store rows in the copy. The windows end at now, and the reads have nothing to read."
    }
    $pickWindow = if ($asOf) {
        "w.first_execution_time >= timestamp '$($anchorParts[1])' - interval '1 day' AND w.first_execution_time < timestamp '$($anchorParts[1])'"
    }
    else {
        "w.first_execution_time >= (now() AT TIME ZONE 'UTC') - interval '1 day'"
    }

    # A query to read history for: the busiest query of the server in the day before the anchor.
    $pick = & $Psql -X -q -A -t -F '|' -v ON_ERROR_STOP=1 -d $Store -c @"
SELECT w.database_name, w.query_id
FROM collect.query_store_interval_wide AS w
JOIN collect.servers AS s ON s.server_id = w.server_id
WHERE s.server_name = $serverLiteral
AND   $pickWindow
ORDER BY w.execution_count DESC NULLS LAST
LIMIT 1;
"@
    $pickParts = "$pick".Trim().Split('|')

    # A failed handshake does not stop the step: each read below then fails on its own and is listed as failed.
    try {
        $null = Invoke-Mcp -Method 'initialize' -Params @{
            protocolVersion = '2025-03-26'
            capabilities = @{}
            clientInfo = @{ name = 'darling-loadtest'; version = '1.0' }
        }
        $null = Invoke-Mcp -Method 'notifications/initialized' -Notification
    }
    catch {
        Write-Host "The MCP handshake failed: $($_.Exception.Message)"
    }

    $tools = @(
        @{ name = 'get_query_store_top'; arguments = @{ server_name = $ServerName } },
        @{ name = 'get_query_store_regressions'; arguments = @{ server_name = $ServerName } },
        @{ name = 'get_query_store_duration_trend'; arguments = @{ server_name = $ServerName } }
    )
    if ($pickParts.Count -eq 2) {
        $tools += @{ name = 'get_query_store_query_history'; arguments = @{ server_name = $ServerName; database_name = $pickParts[0]; query_id = [long] $pickParts[1] } }
    }
    else {
        Write-Host "get_query_store_query_history is skipped: $ServerName has no Query Store query in the day before the anchor."
    }

    foreach ($hours in 24, 168) {
        foreach ($tool in $tools) {
            $arguments = $tool.arguments.Clone()
            $arguments['hours_back'] = $hours
            if ($asOf) { $arguments['as_of'] = $asOf }
            $results += Measure-Call -Surface 'MCP' -Read $tool.name -Window "${hours}h" -AsOf $asOf -Call {
                $answer = Invoke-Mcp -Method 'tools/call' -Params @{ name = $tool.name; arguments = $arguments }
                Assert-ToolAnswer -Answer $answer
            }
        }
    }

    if ($WebToken) {
        $session = $null
        try {
            $null = Invoke-WebRequest -Uri ($WebUrl.TrimEnd('/') + '/auth/login') -Method Post -Body @{ token = $WebToken } `
                -SessionVariable session -SkipHttpErrorCheck -MaximumRedirection 0
        }
        catch {
            # As for MCP: each web read below then fails on its own and is listed as failed.
            Write-Host "The web dashboard sign-in failed: $($_.Exception.Message)"
        }
        foreach ($hours in 24, 168) {
            foreach ($tool in $tools | Select-Object -First 3) {
                $query = "server_name=$([uri]::EscapeDataString($ServerName))&hours_back=$hours"
                if ($asOf) { $query += "&as_of=$([uri]::EscapeDataString($asOf))" }
                $results += Measure-Call -Surface 'Web' -Read $tool.name -Window "${hours}h" -AsOf $asOf -Call {
                    $page = Invoke-WebRequest -Uri ($WebUrl.TrimEnd('/') + "/api/read/$($tool.name)?$query") -WebSession $session -SkipHttpErrorCheck
                    if ($page.StatusCode -ge 400) { throw "HTTP $($page.StatusCode)" }
                    $failure = Get-ReadFailure -Text ([string] $page.Content)
                    if ($failure) { throw $failure }
                }
            }
        }
    }
    else {
        Write-Host 'No -WebToken: the web dashboard reads are skipped.'
    }

    if ($CustomViewSpec) {
        $spec = Get-Content -Raw -LiteralPath $CustomViewSpec
        $specWindow = 'as the spec says'
        $specAsOf = ''
        # The panel's window ends at the same anchor: windowStart and windowEnd win over 'hours'. A spec that sets its own
        # window keeps it.
        $specObject = $spec | ConvertFrom-Json -AsHashtable
        if ($asOf -and -not $specObject.ContainsKey('windowStart') -and -not $specObject.ContainsKey('windowEnd')) {
            $specHours = if ($specObject.ContainsKey('hours')) { [int] $specObject['hours'] } else { 24 }
            $specObject['windowEnd'] = $asOf
            $specObject['windowStart'] = $anchorTime.AddHours(-$specHours).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", [Globalization.CultureInfo]::InvariantCulture)
            $spec = $specObject | ConvertTo-Json -Depth 50 -Compress
            $specWindow = "${specHours}h"
            $specAsOf = $asOf
        }
        $results += Measure-Call -Surface 'Compose' -Read 'run_custom_view_panel' -Window $specWindow -AsOf $specAsOf -Call {
            $answer = Invoke-Mcp -Method 'tools/call' -Params @{ name = 'run_custom_view_panel'; arguments = @{ spec = $spec } }
            Assert-ToolAnswer -Answer $answer
        }
    }
    else {
        Write-Host 'No -CustomViewSpec: the Custom Views panel read is skipped.'
    }

    Write-Host ''
    Write-Host "Reads, as the client timed them (started $since UTC; pass -Since '$since' to the ReadLatency step):"
    Write-Host '| surface | read | window | as_of | client_ms | outcome |'
    Write-Host '|---|---|---|---|---|---|'
    foreach ($r in $results) {
        $shownAsOf = if ($r.AsOf) { $r.AsOf } else { 'now' }
        Write-Host "| $($r.Surface) | $($r.Read) | $($r.Window) | $shownAsOf | $($r.Ms) | $($r.Outcome) |"
    }

    Start-Sleep -Seconds 5
    Write-Host ''
    Write-Host 'Slow reads the service wrote (5 s or more, or a timeout or an error):'
    Write-Host (Invoke-StoreStep -File 'measure-slow-reads.sql' -Variables @{ since = $since })
}

# The hourly work that grows with N (#5794 review round 1). The Query Store interval tables are plain partitioned
# tables, so no TimescaleDB job reads them: the work that scans them is the service's own hourly builders, and each
# writes one summary line per tick that ends with its time in ms. The store object convergence pass, which holds the
# Query Store partition upkeep, writes one too. The service log's time stamps are the service host's local time.
function Step-Jobs {
    Write-Host ''
    Write-Host "The service's hourly work, from the service log (newest $Hours hour(s)):"
    if (-not $ServiceLog) {
        Write-Host 'No -ServiceLog: the hourly builders are not read. Pass the newest darling-service_yyyyMMdd.log file of the service.'
    }
    elseif (-not (Test-Path -LiteralPath $ServiceLog)) {
        Write-Host "No file at $ServiceLog."
    }
    else {
        $pattern = '^(?<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[\w+\] \[[^\]]*\] ' +
            '(?<job>PLAN_REGRESSION daily totals|Query Store top daily summary|Query Store compose rollup|Store object convergence): ' +
            '(?<summary>.*), (?<ms>\d+) ms\s*$'
        $since = (Get-Date).AddHours(-$Hours)
        $runs = @(Get-Content -LiteralPath $ServiceLog | ForEach-Object {
            $match = [regex]::Match($_, $pattern)
            if ($match.Success) {
                $at = [datetime]::ParseExact($match.Groups['ts'].Value, 'yyyy-MM-dd HH:mm:ss.fff', [Globalization.CultureInfo]::InvariantCulture)
                if ($at -ge $since) {
                    [pscustomobject]@{ At = $at; Job = $match.Groups['job'].Value; Summary = $match.Groups['summary'].Value; Ms = [long] $match.Groups['ms'].Value }
                }
            }
        })
        if ($runs.Count -eq 0) {
            Write-Host '(no builder lines in the window: the log is from a build without the elapsed time, or the service ran less than an hour)'
        }
        else {
            Write-Host '| logged_at (service local time) | job | summary | ms |'
            Write-Host '|---|---|---|---|'
            foreach ($r in $runs) { Write-Host "| $($r.At.ToString('yyyy-MM-dd HH:mm:ss')) | $($r.Job) | $($r.Summary -replace '\|', '/') | $($r.Ms) |" }
            $newest = $runs | Group-Object Job | ForEach-Object { $_.Group | Sort-Object At | Select-Object -Last 1 }
            $total = ($newest | Measure-Object Ms -Sum).Sum
            Write-Host ''
            Write-Host "hourly_total_ms (the newest run of each job): $total. It must stay well under one hour (3600000 ms). A 'deferred' count above 0 that stays from tick to tick is a backlog that does not clear."
        }
        # A job that throws writes no summary line, so the total above leaves it out (#5794 review round 2).
        $jobs = @('PLAN_REGRESSION daily totals', 'Query Store top daily summary', 'Query Store compose rollup', 'Store object convergence')
        $logged = @($runs | ForEach-Object { $_.Job })
        $missing = @($jobs | Where-Object { $logged -notcontains $_ })
        if ($runs.Count -gt 0 -and $missing.Count -gt 0) {
            Write-Host "No line in the window for: $($missing -join ', '). hourly_total_ms leaves these out. A job that fails writes no summary line: look for its error in the log."
        }
    }

    Write-Host ''
    Write-Host 'The daily retention purge, from the collection log:'
    Write-Host (Invoke-StoreStep -File 'measure-purge.sql')

    Write-Host ''
    Write-Host 'The TimescaleDB jobs (these do not read the Query Store interval tables):'
    Write-Host (Invoke-StoreStep -File 'measure-jobs.sql')
}

foreach ($step in $Steps) {
    Write-Host ''
    Write-Host "## $step"
    switch ($step) {
        'Reads' { Step-Reads }
        'ReadLatency' {
            if (-not $Since) { throw 'The ReadLatency step needs -Since, the UTC time the Reads step printed.' }
            Write-Host (Invoke-StoreStep -File 'measure-read-latency.sql' -Variables @{ since = $Since })
        }
        'Jobs' { Step-Jobs }
        'Writes' {
            Write-Host (Invoke-StoreStep -File 'measure-writes.sql' -Variables @{ hours = $Hours })
            if ($McpToken -or $McpUrl) {
                try {
                    if (-not $script:mcpSession) {
                        $null = Invoke-Mcp -Method 'initialize' -Params @{ protocolVersion = '2025-03-26'; capabilities = @{}; clientInfo = @{ name = 'darling-loadtest'; version = '1.0' } }
                        $null = Invoke-Mcp -Method 'notifications/initialized' -Notification
                    }
                    $answer = Invoke-Mcp -Method 'tools/call' -Params @{ name = 'get_collector_cost'; arguments = @{ days_back = 1; collector_name = 'query_store' } }
                    Write-Host ''
                    Write-Host 'get_collector_cost (query_store, 1 day):'
                    Write-Host '```json'
                    Write-Host ($answer.result.content[0].text)
                    Write-Host '```'
                }
                catch {
                    Write-Host "get_collector_cost was not read: $($_.Exception.Message)"
                }
            }
        }
        'Compression' {
            $variables = @{}
            if ($Day) { $variables['day'] = $Day }
            Write-Host (Invoke-StoreStep -File 'measure-compression.sql' -Variables $variables)
        }
    }
}
