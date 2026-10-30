/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.DarlingInstallerConfigStepTests;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The MSI package's hooks in the install scripts (#6480). Each probe pulls the function out of the script as it ships and runs it
/// under <c>powershell.exe</c> (Windows PowerShell 5.1) with stand-in facts: no script is run whole, no service, group, ACL, firewall
/// rule or registry value is touched. How the phases are stitched into the script's steps is proved by the package's own install
/// legs on a Windows runner, not here.
/// </summary>
public sealed class DarlingMsiPhaseScriptTests
{
    private static string Install => ReadRepoFile("Darling", "tools", "install-darling.ps1");
    private static string Uninstall => ReadRepoFile("Darling", "tools", "uninstall-darling.ps1");
    private static string Upgrade => ReadRepoFile("Darling", "tools", "upgrade-darling.ps1");

    /// <summary>
    /// A machine the MSI installed (marker value <c>msi</c>) is refused by the installer, the upgrade script and a plain uninstall, with next steps that
    /// name the script; no marker, or any other value, is not refused. The registry read is a stand-in scriptblock.
    /// </summary>
    [Theory]
    [InlineData("install-darling.ps1")]
    [InlineData("upgrade-darling.ps1")]
    [InlineData("uninstall-darling.ps1")]
    public void TheMarkerRefusal_FiresOnMsi_AndOnNothingElse(string scriptName)
    {
        var script = scriptName switch { "install-darling.ps1" => Install, "upgrade-darling.ps1" => Upgrade, _ => Uninstall };
        var lines = RunWindowsPowerShell(
            ExtractFunction(script, "Get-DarlingMsiInstallRefusal") + "\n"
            + $"$refusal = Get-DarlingMsiInstallRefusal '{scriptName}' {{ 'msi' }}\n"
            + "'msi-refused=' + [bool]$refusal\n"
            + $"'names-script=' + ($refusal -match '{scriptName}')\n"
            + "'says-newer-msi=' + ($refusal -match 'newer MSI')\n"
            + $"'absent=' + ($null -eq (Get-DarlingMsiInstallRefusal '{scriptName}' {{ $null }}))\n"
            + $"'other=' + ($null -eq (Get-DarlingMsiInstallRefusal '{scriptName}' {{ 'script' }}))\n");

        Assert.Equal(
            ["msi-refused=True", "names-script=True", "says-newer-msi=True", "absent=True", "other=True"],
            lines);
    }

    /// <summary>
    /// Under -MsiPhase the package has already created the service, so a fresh MSI install has a service record and must still take the
    /// fresh path (which refuses a bad location and an unedited sample instead of asking). Only -MsiUpgrade makes it an upgrade. By hand
    /// nothing changes: the service record decides.
    /// </summary>
    [Fact]
    public void AFreshMsiInstall_WithAServiceRecord_TakesTheFreshPath_AndAskingIsNeverDone()
    {
        var lines = RunWindowsPowerShell(
            ExtractFunction(Install, "Test-DarlingIsUpgrade") + "\n"
            + ExtractFunction(Install, "Read-DarlingAnswer") + "\n"
            + "$record = New-Object psobject -Property @{ Status = 'Stopped' }\n"
            + "'msi-fresh-with-record=' + (Test-DarlingIsUpgrade 'PreStart' $false $record)\n"
            + "'msi-upgrade=' + (Test-DarlingIsUpgrade 'PreStart' $true $record)\n"
            + "'msi-poststart-fresh=' + (Test-DarlingIsUpgrade 'PostStart' $false $record)\n"
            + "'hand-with-record=' + (Test-DarlingIsUpgrade '' $false $record)\n"
            + "'hand-without=' + (Test-DarlingIsUpgrade '' $false $null)\n"
            // Under -MsiPhase the reader must not be called: this one fails the probe if it is.
            + "$answer = Read-DarlingAnswer 'PreStart' 'Install anyway? [y/N]' { throw 'asked a question under -MsiPhase' } 6>$null\n"
            + "'msi-answer=' + $answer\n"
            + "'hand-answer=' + (Read-DarlingAnswer '' 'Install anyway? [y/N]' { param($text) 'n' })\n");

        Assert.Equal(
            [
                "msi-fresh-with-record=False", "msi-upgrade=True", "msi-poststart-fresh=False",
                "hand-with-record=True", "hand-without=False", "msi-answer=y", "hand-answer=n"
            ],
            lines);
    }

    /// <summary>
    /// The ready wait (#6470) ends a start that did not finish with exit 1 by hand, but under -MsiPhase a nonzero exit rolls the package
    /// back, after the store may already have migrated. There the same problem and remedy are a warning and the exit is 0. Both phases
    /// are probed; PreStart never reaches the wait (it exits before step 5), so only PostStart matters in practice.
    /// </summary>
    [Fact]
    public void ANotReadyStart_ExitsOneByHand_AndZeroWithAWarningUnderMsiPhase()
    {
        var lines = RunWindowsPowerShell(
            ExtractFunction(Install, "Write-DarlingNotReadyVerdict") + "\n"
            + "$problem = 'The service logged a startup failure and is not collecting.'\n"
            + "$remedy = 'Fix what the lines above say, then restart it.'\n"
            + "$hand = Write-DarlingNotReadyVerdict $problem $remedy '' 6>&1 | Out-String\n"
            + "$handCode = (Write-DarlingNotReadyVerdict $problem $remedy '' 6>$null)\n"
            + "$msiCode = (Write-DarlingNotReadyVerdict $problem $remedy 'PostStart' 6>$null)\n"
            + "$msi = Write-DarlingNotReadyVerdict $problem $remedy 'PostStart' 6>&1 | Out-String\n"
            + "'hand-exit=' + $handCode\n"
            + "'hand-says-problem-and-remedy=' + (($hand -match 'startup failure') -and ($hand -match 'restart it'))\n"
            + "'msi-exit=' + $msiCode\n"
            + "'msi-warns=' + ($msi -match 'WARNING')\n"
            + "'msi-says-problem-and-remedy=' + (($msi -match 'startup failure') -and ($msi -match 'restart it'))\n");

        Assert.Equal(
            ["hand-exit=1", "hand-says-problem-and-remedy=True", "msi-exit=0", "msi-warns=True", "msi-says-problem-and-remedy=True"],
            lines);
    }

    /// <summary>
    /// -WebDashboard (the MSI's WEBDASHBOARD=1) seeds web.enabled and an encrypted web token in the minimal darling.json the installer
    /// writes: the web host refuses to start without a token, so web.enabled alone gave a dashboard that never started. The service's own
    /// loader and token check accept the file, tokenless loopback stays off, and a second run leaves the first token alone. Without the
    /// switch the web block holds only allowAdminWrites (a new install keeps the web admin writes on).
    /// </summary>
    [Fact]
    public void WebDashboard_SeedsAnEncryptedWebToken_ThatTheWebHostAccepts_AndOtherwiseNothingChanges()
    {
        var folder = Directory.CreateTempSubdirectory("darling-msi-config-").FullName;
        try
        {
            var withWeb = Path.Combine(folder, "with-web.json");
            var without = Path.Combine(folder, "without.json");
            var lines = RunWindowsPowerShell(
                ExtractFunction(Install, "New-DarlingWebEncryptedToken") + "\n"
                + ExtractFunction(Install, "New-DarlingMinimalConfigText") + "\n"
                + ExtractFunction(Install, "Write-DarlingMinimalConfig") + "\n"
                + ExtractFunction(Install, "Initialize-DarlingMinimalConfig") + "\n"
                + $"'first-wrote=' + (Initialize-DarlingMinimalConfig '{withWeb}' $true)\n"
                + GrantTestAccountRead(withWeb)
                + $"$firstText = [System.IO.File]::ReadAllText('{withWeb}')\n"
                + $"'second-wrote=' + (Initialize-DarlingMinimalConfig '{withWeb}' $true)\n"
                + $"'second-keeps-file=' + ($firstText -ceq [System.IO.File]::ReadAllText('{withWeb}'))\n"
                + $"'plain-wrote=' + (Initialize-DarlingMinimalConfig '{without}' $false)\n"
                + GrantTestAccountRead(without));
            Assert.Equal(["first-wrote=True", "second-wrote=False", "second-keeps-file=True", "plain-wrote=True"], lines);

            var seeded = DarlingConfig.Parse(File.ReadAllText(withWeb));
            Assert.True(seeded.Web.Enabled);
            Assert.True(seeded.Web.AllowAdminWrites);
            Assert.False(seeded.Web.AllowTokenlessLoopback);
            Assert.Empty(seeded.Servers);
            Assert.True(seeded.Postgres.Managed);
            Assert.Empty(seeded.Validate());
            Assert.False(string.IsNullOrWhiteSpace(seeded.Web.Network?.EncryptedToken));
            Assert.True(string.IsNullOrWhiteSpace(seeded.Web.Network?.Token));

            // The service decrypts it with its own DPAPI entropy and the web host starts on it: no refusal, no missing-token line.
            var resolved = DarlingWebHostService.ResolveLoopbackOnlyToken(
                seeded.Web.Network, seeded.Web.AllowTokenlessLoopback, NullLogger.Instance);
            Assert.False(resolved.Refuse);
            Assert.False(resolved.MissingToken);
            Assert.Null(resolved.TokenProblem);
            Assert.Equal(32, resolved.Token.Length);
            Assert.Equal(resolved.Token, DarlingSecrets.Unprotect(seeded.Web.Network!.EncryptedToken!));

            var plain = DarlingConfig.Parse(File.ReadAllText(without));
            // A new install keeps web admin writes on even when the dashboard is switched on later: the key is written, the dashboard stays off.
            Assert.False(plain.Web.Enabled);
            Assert.True(plain.Web.AllowAdminWrites);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>
    /// The minimal darling.json is created with a protected DACL holding SYSTEM and Administrators only, so the encrypted web token in it is
    /// never readable through the install tree's inherited Users read, even when the run stops before step 4b rebuilds the ACL.
    /// </summary>
    [Fact]
    public void TheMinimalConfig_IsCreatedProtected_WithNoUsersOrEveryoneEntry()
    {
        var folder = Directory.CreateTempSubdirectory("darling-msi-acl-").FullName;
        try
        {
            var path = Path.Combine(folder, "darling.json");
            var lines = RunWindowsPowerShell(
                ExtractFunction(Install, "New-DarlingWebEncryptedToken") + "\n"
                + ExtractFunction(Install, "New-DarlingMinimalConfigText") + "\n"
                + ExtractFunction(Install, "Write-DarlingMinimalConfig") + "\n"
                + $"Write-DarlingMinimalConfig '{path}' $true\n"
                + $"$acl = Get-Acl -LiteralPath '{path}'\n"
                + "$sids = @($acl.Access | ForEach-Object { $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value } | Sort-Object)\n"
                + "'protected=' + $acl.AreAccessRulesProtected\n"
                + "'sids=' + ($sids -join ';')\n"
                + "'broad=' + @($sids | Where-Object { $_ -in 'S-1-5-32-545', 'S-1-1-0', 'S-1-5-11', 'S-1-5-4' }).Count\n");
            Assert.Equal(["protected=True", "sids=S-1-5-18;S-1-5-32-544", "broad=0"], lines);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    /// <summary>
    /// The closing summary treats the dashboard as on only when the "web" block itself says enabled true. A web block with enabled false
    /// followed by another block's enabled true (alerts, analysis, mcp) is off, and a web block with a nested network object still counts.
    /// </summary>
    [Fact]
    public void TheWebEnabledCheck_ReadsOnlyTheWebBlock()
    {
        var lines = RunWindowsPowerShell(
            ExtractFunction(Install, "Test-DarlingWebEnabledText") + "\n"
            + "'off-then-other-on=' + (Test-DarlingWebEnabledText '{ \"web\": { \"enabled\": false }, \"alerts\": { \"enabled\": true } }')\n"
            + "'off-with-network-then-other-on=' + (Test-DarlingWebEnabledText '{ \"web\": { \"network\": { \"port\": 1 }, \"enabled\": false }, \"mcp\": { \"enabled\": true } }')\n"
            + "'on=' + (Test-DarlingWebEnabledText '{ \"web\": { \"enabled\": true }, \"servers\": [] }')\n"
            + "'on-after-network=' + (Test-DarlingWebEnabledText '{ \"web\": { \"network\": { \"port\": 1 }, \"enabled\": true } }')\n"
            + "'no-web=' + (Test-DarlingWebEnabledText '{ \"mcp\": { \"enabled\": true } }')\n");
        Assert.Equal(
            ["off-then-other-on=False", "off-with-network-then-other-on=False", "on=True", "on-after-network=True", "no-web=False"],
            lines);
    }

    /// <summary>
    /// The store is deleted only on a given -ConfirmPurge, or on the typed word DELETE by hand. An -MsiUninstall run that was not given
    /// -ConfirmPurge keeps it and never asks.
    /// </summary>
    [Fact]
    public void MsiUninstall_KeepsTheStore_UnlessPurgeIsConfirmed()
    {
        var lines = RunWindowsPowerShell(
            ExtractFunction(Uninstall, "Test-DarlingPurgeConfirmed") + "\n"
            + "'msi-no-confirm=' + (Test-DarlingPurgeConfirmed $false $true { throw 'asked a question under -MsiUninstall' })\n"
            + "'msi-confirm=' + (Test-DarlingPurgeConfirmed $true $true { throw 'asked although confirmed' })\n"
            + "'hand-typed-delete=' + (Test-DarlingPurgeConfirmed $false $false { 'DELETE' })\n"
            + "'hand-typed-other=' + (Test-DarlingPurgeConfirmed $false $false { 'delete' })\n");

        Assert.Equal(
            ["msi-no-confirm=False", "msi-confirm=True", "hand-typed-delete=True", "hand-typed-other=False"],
            lines);
    }
}
