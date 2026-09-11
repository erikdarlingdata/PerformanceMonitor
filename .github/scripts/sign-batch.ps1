<#
.SYNOPSIS
  Signs a batch of files through SignPath. Written to be called BY `vpk pack --signTemplate` (#3288).

.DESCRIPTION
  Velopack generates the portable launcher stub, Update.exe and Setup.exe AFTER packing, so they
  cannot be signed by the pre-pack rounds and cannot be signed afterwards either: re-signing a
  file inside a .nupkg changes that package's bytes, which invalidates the SHA256 and Size that
  releases.<channel>.json records and that delta packages patch against. Signing therefore has to
  happen inside packing, which is what `--signTemplate` is for.

  Measured behaviour of `vpk pack` 1.2.0 that this script depends on:

    * It offers 35 files for the Lite payload, not ~700, because it SKIPS files that already
      carry a signature -- Microsoft's framework binaries never reach here.
    * With `--signParallel 50` it invokes this script exactly twice per product: once with the
      34 pack-directory files, once with Setup.exe alone in its own later phase.
    * It does NOT check that this script actually signed anything. A template that exits 0
      without signing leaves packing to complete and the release to publish unsigned binaries,
      which is precisely how #3288 shipped on two releases. So this script verifies its own
      outcome before returning success, and that verification is the point of it.

.PARAMETER Files
  The files to sign, passed positionally by vpk in place of {{file...}}.

.NOTES
  Required environment:
    SIGNPATH_API_TOKEN     CI user token
    SIGNPATH_ORG_ID        organization id
    SIGNPATH_PROJECT_SLUG  project slug
    SIGNPATH_POLICY_SLUG   signing policy slug
    SIGNPATH_CONFIG_SLUG   artifact configuration slug for a flat batch of PE files
  Optional:
    SIGNPATH_TIMEOUT_SECONDS  default 3600. Submit-SigningRequest's own default is 600, which
                              is not enough when a human has to approve the request -- every
                              Foundation-tier request does.
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $Files
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Fail([string] $message) {
    Write-Host "sign-batch: FAIL: $message"
    exit 1
}

# The artifact configuration for this batch declares min-matches="0" on both suffixes, because a
# batch may be all-exe (the Setup.exe invocation) or mixed (the pack directory). That removes the
# loud failure the per-product configurations get from min-matches="1", so an EMPTY submission
# would succeed and sign nothing. Refusing it here is the only thing standing in for that.
if (-not $Files -or $Files.Count -eq 0) {
    Fail 'no files were passed. An empty batch would be signed successfully and sign nothing.'
}

$missing = $Files | Where-Object { -not (Test-Path -LiteralPath $_ -PathType Leaf) }
if ($missing) { Fail "these paths do not exist: $($missing -join ', ')" }

foreach ($name in 'SIGNPATH_API_TOKEN', 'SIGNPATH_ORG_ID', 'SIGNPATH_PROJECT_SLUG',
                  'SIGNPATH_POLICY_SLUG', 'SIGNPATH_CONFIG_SLUG') {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))) {
        Fail "$name is not set."
    }
}

$timeout = 3600
if (-not [string]::IsNullOrWhiteSpace($env:SIGNPATH_TIMEOUT_SECONDS)) {
    $timeout = [int] $env:SIGNPATH_TIMEOUT_SECONDS
}

Write-Host "sign-batch: $($Files.Count) file(s)"
foreach ($f in $Files) { Write-Host "  $(Split-Path -Leaf $f)" }

$work = Join-Path ([System.IO.Path]::GetTempPath()) ("signbatch-" + [guid]::NewGuid().ToString('N'))
# A subdirectory, not the zip root: the artifact configuration matches `**/*.exe`, and whether
# `**/` also matches zero path segments is not documented. One segment removes the question.
$staged = Join-Path $work 'batch'
New-Item -ItemType Directory -Force -Path $staged | Out-Null

# vpk passes one directory's files per invocation so base names are unique in practice, but a
# collision would silently drop a file from the batch and leave it unsigned, so it is checked
# rather than assumed.
$map = @{}
foreach ($f in $Files) {
    $leaf = Split-Path -Leaf $f
    if ($map.ContainsKey($leaf)) {
        Fail "two files in one batch share the name '$leaf': '$($map[$leaf])' and '$f'."
    }
    $map[$leaf] = (Resolve-Path -LiteralPath $f).Path
    Copy-Item -LiteralPath $f -Destination (Join-Path $staged $leaf) -Force
}

$inputZip  = Join-Path $work 'batch.zip'
$outputZip = Join-Path $work 'batch-signed.zip'
Compress-Archive -Path (Join-Path $staged '*') -DestinationPath $inputZip -Force

if (-not (Get-Module -ListAvailable -Name SignPath)) {
    Install-Module -Name SignPath -Force -Scope CurrentUser -AllowClobber | Out-Null
}
Import-Module SignPath

Write-Host "sign-batch: submitting to SignPath (timeout ${timeout}s; a Foundation-tier request waits for manual approval)"
Submit-SigningRequest `
    -InputArtifactPath $inputZip `
    -OutputArtifactPath $outputZip `
    -OrganizationId $env:SIGNPATH_ORG_ID `
    -ApiToken $env:SIGNPATH_API_TOKEN `
    -ProjectSlug $env:SIGNPATH_PROJECT_SLUG `
    -SigningPolicySlug $env:SIGNPATH_POLICY_SLUG `
    -ArtifactConfigurationSlug $env:SIGNPATH_CONFIG_SLUG `
    -WaitForCompletion `
    -WaitForCompletionTimeoutInSeconds $timeout

if (-not (Test-Path -LiteralPath $outputZip)) { Fail 'SignPath returned no signed artifact.' }

$expanded = Join-Path $work 'signed'
Expand-Archive -LiteralPath $outputZip -DestinationPath $expanded -Force

# Copy back over the ORIGINAL paths, because those are what vpk goes on to package.
$restored = @()
foreach ($leaf in $map.Keys) {
    $signed = Get-ChildItem -LiteralPath $expanded -Recurse -File |
              Where-Object { $_.Name -eq $leaf } | Select-Object -First 1
    if (-not $signed) { Fail "SignPath's response does not contain '$leaf'." }
    Copy-Item -LiteralPath $signed.FullName -Destination $map[$leaf] -Force
    $restored += $map[$leaf]
}

# THE POST-CONDITION, and the reason this script cannot just trust its own exit code: vpk does
# not check, so if this is wrong nothing downstream notices until a user is blocked by Windows.
# Delegated to the reader that found #3288, whose PE parse is covered by six mutations.
$verifier = Join-Path $PSScriptRoot 'verify_release_signatures.py'
if (-not (Test-Path -LiteralPath $verifier)) { Fail "verifier not found beside this script: $verifier" }
& python3 $verifier --verify-files @restored
if ($LASTEXITCODE -ne 0) {
    Fail "signing reported success but $($restored.Count) file(s) did not all come back signed."
}

Remove-Item -Recurse -Force $work -ErrorAction SilentlyContinue
Write-Host "sign-batch: $($restored.Count) file(s) signed and verified"
exit 0
