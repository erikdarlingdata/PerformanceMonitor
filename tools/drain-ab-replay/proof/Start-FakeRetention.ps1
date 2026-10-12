<#
.SYNOPSIS
    Stands in for the Darling service's retention drain on a THROWAWAY rig, so Invoke-DrainAbReplay.ps1's phase
    detection can be proven. Never point this at a real store.

.DESCRIPTION
    Deletes old rows, a batch at a time, from the tables named in -Tables, one table after another, with a pause
    between batches and a longer one between tables. Each DELETE holds still for -BatchSeconds (a sleep inside the
    statement) so a once-a-second sampler sees it. The connection reports the service's application name.
#>
param(
    [Parameter(Mandatory)] [string] $DbHost,
    [int] $Port = 5432,
    [Parameter(Mandatory)] [string] $User,
    [Parameter(Mandatory)] [string] $Database,
    [string[]] $Tables = @('wait_stats', 'perfmon_stats', 'file_io_stats'),
    [int] $OlderThanDays = 10,
    [int] $BatchRows = 4000,
    [double] $BatchSeconds = 1.5,
    [double] $PauseBetweenBatches = 3,
    [double] $PauseBetweenTables = 25,
    [string] $AppName = 'PerformanceMonitorDarling-Service',
    [string] $PsqlPath = 'psql',
    [string] $WorkFolder = [System.IO.Path]::GetTempPath()
)
$ErrorActionPreference = 'Stop'
$inv = [System.Globalization.CultureInfo]::InvariantCulture
$sb = [System.Text.StringBuilder]::new()
[void]$sb.AppendLine('SET search_path = collect;')
$first = $true
foreach ($t in $Tables) {
    if (-not $first) { [void]$sb.AppendLine("SELECT pg_sleep($($PauseBetweenTables.ToString($inv)));") }
    $first = $false
    # Each batch is one statement that starts with DELETE FROM <table>, like the service's retention DELETEs.
    for ($i = 0; $i -lt 8; $i++) {
        [void]$sb.AppendLine("DELETE FROM $t WHERE ctid IN (SELECT ctid FROM $t WHERE collection_time < (now() AT TIME ZONE 'UTC') - interval '$OlderThanDays days' LIMIT $BatchRows) AND (SELECT pg_sleep($($BatchSeconds.ToString($inv)))) IS NOT NULL;")
        [void]$sb.AppendLine("SELECT pg_sleep($($PauseBetweenBatches.ToString($inv)));")
    }
}
$file = Join-Path $WorkFolder ('fake-retention-' + [guid]::NewGuid().ToString('N') + '.sql')
Set-Content -LiteralPath $file -Value $sb.ToString() -Encoding utf8
try {
    $uri = 'postgresql://{0}@{1}:{2}/{3}?application_name={4}' -f [uri]::EscapeDataString($User), $DbHost, $Port, [uri]::EscapeDataString($Database), [uri]::EscapeDataString($AppName)
    & $PsqlPath -X -q -d $uri -f $file
}
finally { Remove-Item -LiteralPath $file -ErrorAction SilentlyContinue }

