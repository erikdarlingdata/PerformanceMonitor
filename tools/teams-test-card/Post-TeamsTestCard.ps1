#Requires -Version 7.0
<#
.SYNOPSIS
Posts the two sample Teams alert cards in this folder to a Teams webhook, to check by eye how Teams shows them.

.DESCRIPTION
The samples are the alert builder's own output for two made-up alerts. Lite.Tests/TeamsTestCardSampleTests.cs builds the same
two alerts and fails when a file here differs from what the builder produces.

  builtin-alert.teams.json   A critical alert on the made-up server SQL-TEST-01, with a triage button that opens a page on
                             example.com.
  external-alert.teams.json  An external alert whose detail has three lines: a plain one, an indented one, and one that
                             starts with "- ".

The script asks for the webhook URL of a Teams workflow, posts each sample as the file's exact bytes, and prints the HTTP
status and the response body of each post. Nothing else is printed.

The URL is a secret: anyone who has it can post to that chat or channel. The script reads it masked, keeps it in memory only,
never prints it, and writes no file, transcript or log. A request that gets no HTTP response prints the error's type and not
its message, because a connection error's message can name the host.

Post into a chat or channel you control, and delete the workflow after the check. What to look at in Teams:

  1. Both cards arrived.
  2. Title and severity: the first card is a red CRITICAL "Blocking Detected", the second an INFO "[External: sample-source]
     Nightly report".
  3. The second card's detail is three lines as written, the third starting with "- ".
  4. The first card has an "Open triage page" button and it opens the example.com link.

.EXAMPLE
pwsh ./tools/teams-test-card/Post-TeamsTestCard.ps1
#>
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# A caller's -Verbose or -Debug would make Invoke-RestMethod print the request line, which holds the URL.
$VerbosePreference = 'SilentlyContinue'
$DebugPreference = 'SilentlyContinue'
$ProgressPreference = 'SilentlyContinue'

$samples = 'builtin-alert.teams.json', 'external-alert.teams.json'
foreach ($name in $samples)
{
    if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot $name) -PathType Leaf))
    {
        Write-Host "$name is not next to this script. Nothing was posted."
        exit 1
    }
}

$secureUrl = Read-Host -Prompt 'Teams webhook URL (input is hidden)' -AsSecureString
$uri = $null
$isUrl = [System.Uri]::TryCreate([System.Net.NetworkCredential]::new('', $secureUrl).Password.Trim(), [System.UriKind]::Absolute, [ref]$uri)
if (-not $isUrl -or $uri.Scheme -ne 'https')
{
    Write-Host 'That is not an https URL. Nothing was posted.'
    exit 1
}

$allAccepted = $true
foreach ($name in $samples)
{
    $body = [System.IO.File]::ReadAllBytes((Join-Path $PSScriptRoot $name))
    try
    {
        $status = 0
        $response = Invoke-RestMethod -Uri $uri -Method Post -ContentType 'application/json' -Body $body `
            -StatusCodeVariable status -SkipHttpErrorCheck -Verbose:$false -Debug:$false

        $text =
            if ($null -eq $response -or ($response -is [string] -and $response.Length -eq 0)) { '(empty)' }
            elseif ($response -is [string]) { $response }
            else { $response | ConvertTo-Json -Depth 20 -Compress }

        Write-Host "${name}: HTTP $status"
        Write-Host "  response body: $text"
        if ($status -lt 200 -or $status -gt 299) { $allAccepted = $false }
    }
    catch
    {
        Write-Host "${name}: no HTTP response ($($_.Exception.GetType().FullName))"
        $allAccepted = $false
    }
}

$uri = $null
$secureUrl.Dispose()
if (-not $allAccepted) { exit 1 }
