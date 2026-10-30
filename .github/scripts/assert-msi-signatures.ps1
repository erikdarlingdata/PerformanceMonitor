<#
  #6480: the release job's backstop for the Darling MSI. Every *.msi in the release folder must carry a Valid
  Authenticode signature whose signer is the SignPath Foundation certificate, the same test build.yml applies to the
  Darling zip's three scripts. No *.msi at all passes (the MSI ships only when the DARLING_MSI_SIGNING variable is
  'true'); an unsigned one, one signed by anyone else, or one whose signature is not Valid fails.

  Exit code 0 when every MSI passes (or there is none), 1 otherwise. Prints each MSI's status, signer, thumbprint and
  timestamper for the log. It writes plain text and no ::error:: annotation, so the proving run in darling-msi.yml
  can feed it files it expects to fail without marking the run; build.yml adds the annotation.

  Windows only: Get-AuthenticodeSignature is the part that checks the chain, the digest and the timestamp.
#>
param(
    [Parameter(Mandatory = $true)][string]$ReleasesDir
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $ReleasesDir -PathType Container)) {
    Write-Host "ERROR: '$ReleasesDir' is not a folder."
    exit 1
}

$msis = @(Get-ChildItem -LiteralPath $ReleasesDir -Filter '*.msi' -File)
if ($msis.Count -eq 0) {
    Write-Host "No .msi in $ReleasesDir; nothing to check."
    exit 0
}

$failed = 0
foreach ($msi in $msis) {
    $signature = Get-AuthenticodeSignature -LiteralPath $msi.FullName
    $subject = "$($signature.SignerCertificate.Subject)"
    $stamper = if ($signature.TimeStamperCertificate) { $signature.TimeStamperCertificate.Subject } else { '(none)' }
    Write-Host "$($msi.Name): $($signature.Status) ($($signature.StatusMessage))"
    Write-Host "  signer: $(if ($subject) { $subject } else { '(none)' })"
    Write-Host "  thumbprint: $(if ($signature.SignerCertificate) { $signature.SignerCertificate.Thumbprint } else { '(none)' })"
    Write-Host "  timestamper: $stamper"
    if ($signature.Status -ne 'Valid') {
        Write-Host "  FAIL: the signature is not Valid"
        $failed++
    } elseif ($subject -notmatch '(^|,\s*)O=SignPath Foundation(,|$)') {
        Write-Host "  FAIL: not signed by the SignPath Foundation certificate"
        $failed++
    }
}

if ($failed) {
    Write-Host "ERROR: $failed of $($msis.Count) .msi file(s) do not carry a Valid Authenticode signature by the SignPath Foundation certificate (#6480)."
    exit 1
}
Write-Host "All $($msis.Count) .msi file(s) are Valid and signed by the SignPath Foundation certificate."
exit 0
