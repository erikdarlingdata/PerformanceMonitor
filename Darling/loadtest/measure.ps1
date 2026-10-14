<#
.SYNOPSIS
    #5794 load test: measures a Darling store copy that inflate-query-store.sql has inflated.

.DESCRIPTION
    Each step prints a Markdown table that pastes into the issue. The steps:

      Reads        (a) The day-scan reads through the real surfaces, with 1-day and 7-day windows: the four MCP Query
                   Store tools, the same reads through the web dashboard's /api/read routes (the Query Store page),
                   and a Custom Views panel (-CustomViewSpec). Prints the time each call took, as the client saw it,
                   then the slow-read rows the service wrote for them.
      ReadLatency  (a) The service's own read times for the window that started at -Since, beside the read budgets.
                   The service writes them once an hour, so run this step after that hour has closed.
      Jobs         (b) The continuous aggregate refresh jobs and the compression jobs.
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
    [int] $Hours = 2
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

function Measure-Call {
    param([string] $Surface, [string] $Read, [string] $Window, [scriptblock] $Call)

    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    $outcome = 'ok'
    try {
        & $Call
    }
    catch {
        $outcome = 'failed: ' + ($_.Exception.Message -replace '\|', '/')
    }
    $watch.Stop()
    [pscustomobject]@{ Surface = $Surface; Read = $Read; Window = $Window; Ms = $watch.ElapsedMilliseconds; Outcome = $outcome }
}

function Step-Reads {
    if (-not $ServerName) { throw 'The Reads step needs -ServerName: a real server in the copy.' }
    $since = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')
    $results = @()

    # A query to read history for: the busiest query of the server in the last day.
    $pick = & $Psql -X -q -A -t -F '|' -v ON_ERROR_STOP=1 -d $Store -c @"
SELECT w.database_name, w.query_id
FROM collect.query_store_interval_wide AS w
JOIN collect.servers AS s ON s.server_id = w.server_id
WHERE s.server_name = '$($ServerName -replace "'", "''")'
AND   w.first_execution_time >= (now() AT TIME ZONE 'UTC') - interval '1 day'
ORDER BY w.execution_count DESC NULLS LAST
LIMIT 1;
"@
    $pickParts = "$pick".Trim().Split('|')

    $null = Invoke-Mcp -Method 'initialize' -Params @{
        protocolVersion = '2025-03-26'
        capabilities = @{}
        clientInfo = @{ name = 'darling-loadtest'; version = '1.0' }
    }
    $null = Invoke-Mcp -Method 'notifications/initialized' -Notification

    $tools = @(
        @{ name = 'get_query_store_top'; arguments = @{ server_name = $ServerName } },
        @{ name = 'get_query_store_regressions'; arguments = @{ server_name = $ServerName } },
        @{ name = 'get_query_store_duration_trend'; arguments = @{ server_name = $ServerName } }
    )
    if ($pickParts.Count -eq 2) {
        $tools += @{ name = 'get_query_store_query_history'; arguments = @{ server_name = $ServerName; database_name = $pickParts[0]; query_id = [long] $pickParts[1] } }
    }

    foreach ($hours in 24, 168) {
        foreach ($tool in $tools) {
            $arguments = $tool.arguments.Clone()
            $arguments['hours_back'] = $hours
            $results += Measure-Call -Surface 'MCP' -Read $tool.name -Window "${hours}h" -Call {
                $answer = Invoke-Mcp -Method 'tools/call' -Params @{ name = $tool.name; arguments = $arguments }
                if ($answer.PSObject.Properties['error'] -and $answer.error) { throw $answer.error.message }
                if ($answer.result.isError) { throw 'the tool returned isError' }
            }
        }
    }

    if ($WebToken) {
        $session = $null
        $null = Invoke-WebRequest -Uri ($WebUrl.TrimEnd('/') + '/auth/login') -Method Post -Body @{ token = $WebToken } `
            -SessionVariable session -SkipHttpErrorCheck -MaximumRedirection 0
        foreach ($hours in 24, 168) {
            foreach ($tool in $tools | Select-Object -First 3) {
                $query = "server_name=$([uri]::EscapeDataString($ServerName))&hours_back=$hours"
                $results += Measure-Call -Surface 'Web' -Read $tool.name -Window "${hours}h" -Call {
                    $page = Invoke-WebRequest -Uri ($WebUrl.TrimEnd('/') + "/api/read/$($tool.name)?$query") -WebSession $session -SkipHttpErrorCheck
                    if ($page.StatusCode -ge 400) { throw "HTTP $($page.StatusCode)" }
                }
            }
        }
    }
    else {
        Write-Host 'No -WebToken: the web dashboard reads are skipped.'
    }

    if ($CustomViewSpec) {
        $spec = Get-Content -Raw -LiteralPath $CustomViewSpec
        $results += Measure-Call -Surface 'Compose' -Read 'run_custom_view_panel' -Window 'as the spec says' -Call {
            $answer = Invoke-Mcp -Method 'tools/call' -Params @{ name = 'run_custom_view_panel'; arguments = @{ spec = $spec } }
            if ($answer.result.isError) { throw 'the tool returned isError' }
        }
    }
    else {
        Write-Host 'No -CustomViewSpec: the Custom Views panel read is skipped.'
    }

    Write-Host ''
    Write-Host "Reads, as the client timed them (started $since UTC; pass -Since '$since' to the ReadLatency step):"
    Write-Host '| surface | read | window | client_ms | outcome |'
    Write-Host '|---|---|---|---|---|'
    foreach ($r in $results) { Write-Host "| $($r.Surface) | $($r.Read) | $($r.Window) | $($r.Ms) | $($r.Outcome) |" }

    Start-Sleep -Seconds 5
    Write-Host ''
    Write-Host 'Slow reads the service wrote (5 s or more, or a timeout or an error):'
    Write-Host (Invoke-StoreStep -File 'measure-slow-reads.sql' -Variables @{ since = $since })
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
        'Jobs' { Write-Host (Invoke-StoreStep -File 'measure-jobs.sql') }
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
