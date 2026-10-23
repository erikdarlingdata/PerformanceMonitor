/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6469: the installer offers to restart itself elevated instead of stopping, stops with a clear message when it
/// cannot ask, prints the one-line extract command when it refuses a folder, and ships a <c>.cmd</c> launcher. The
/// script's own functions run under Windows PowerShell 5.1; the launcher runs in cmd.exe on a branch that never
/// asks for elevation.
/// </summary>
public sealed class DarlingInstallLauncherTests
{
    private static string Script => ReadRepoFileLf("Darling", "tools", "install-darling.ps1");

    private static string Functions(params string[] names) =>
        string.Join("\n", names.Select(name => DarlingInstallerConfigStepTests.ExtractFunction(Script, name)));

    /// <summary>Not elevated and able to answer: offer the restart. Not elevated and not able to answer: stop.
    /// Elevated: carry on whatever the session.</summary>
    [Fact]
    public void TheElevationAction_OffersARestartOnlyWhenTheSessionCanAnswer()
    {
        var lines = Run(Functions("Get-DarlingElevationAction") + """

            "admin,interactive=" + (Get-DarlingElevationAction $true $true)
            "admin,batch=" + (Get-DarlingElevationAction $true $false)
            "user,interactive=" + (Get-DarlingElevationAction $false $true)
            "user,batch=" + (Get-DarlingElevationAction $false $false)
            """);

        Assert.Equal(
            new[] { "admin,interactive=Continue", "admin,batch=Continue", "user,interactive=Offer", "user,batch=Stop" },
            lines);
    }

    /// <summary>-NonInteractive (and its abbreviations), redirected input and a non-interactive process each mean the
    /// installer cannot ask, so it stops rather than hangs on a prompt.</summary>
    [Fact]
    public void ASessionThatCannotAnswerIsNotInteractive()
    {
        var lines = Run(Functions("Test-DarlingInteractiveSession") + """

            "plain=" + (Test-DarlingInteractiveSession @('powershell.exe', '-NoProfile', '-File', 'x.ps1') $true $false)
            "noninteractive=" + (Test-DarlingInteractiveSession @('powershell.exe', '-NonInteractive', '-File', 'x.ps1') $true $false)
            "abbreviated=" + (Test-DarlingInteractiveSession @('powershell.exe', '-noni', '-File', 'x.ps1') $true $false)
            "redirected=" + (Test-DarlingInteractiveSession @('powershell.exe', '-File', 'x.ps1') $true $true)
            "service=" + (Test-DarlingInteractiveSession @('powershell.exe', '-File', 'x.ps1') $false $false)
            """);

        Assert.Equal(
            new[] { "plain=True", "noninteractive=False", "abbreviated=False", "redirected=False", "service=False" },
            lines);
    }

    /// <summary>The elevated restart runs the same script with the policy bypassed for that process and keeps the
    /// switches the person passed, and only those.</summary>
    [Fact]
    public void TheElevatedRestart_BypassesThePolicyForThatProcess_AndKeepsTheSwitches()
    {
        var lines = Run(Functions("Get-DarlingElevatedArguments") + """

            $bound = @{ Network = [switch]$true; SkipPreflight = [switch]$true; NoShortcuts = [switch]$false }
            (Get-DarlingElevatedArguments 'C:\Program Files\PerformanceMonitorDarling\install-darling.ps1' $bound) -join '|'
            """);

        Assert.Single(lines);
        Assert.Equal(
            "-NoProfile|-ExecutionPolicy|Bypass|-NoExit|-File|\"C:\\Program Files\\PerformanceMonitorDarling\\install-darling.ps1\"|-Network|-SkipPreflight",
            lines[0]);
    }

    /// <summary>-AddOperator is the script's one valued parameter. The elevated restart must carry it, or the elevated
    /// run never adds that account (#6469 review). The script below binds real parameters, so the
    /// <c>$PSBoundParameters</c> to <c>[hashtable]</c> coercion the installer relies on runs too.</summary>
    [Fact]
    public void TheElevatedRestart_KeepsAValuedParameter_FromTheRealBoundParameters()
    {
        var lines = Run(Functions("Get-DarlingElevatedArguments") + """

            function Invoke-Bound([switch]$Network, [string]$AddOperator, [string]$Unset) {
                (Get-DarlingElevatedArguments 'C:\x\install-darling.ps1' $PSBoundParameters) -join '|'
            }
            Invoke-Bound -AddOperator 'CONTOSO\alice' -Network
            """);

        Assert.Single(lines);
        Assert.Equal(
            "-NoProfile|-ExecutionPolicy|Bypass|-NoExit|-File|\"C:\\x\\install-darling.ps1\"|-AddOperator|\"CONTOSO\\alice\"|-Network",
            lines[0]);
    }

    /// <summary>The refusal of an extract folder prints a command that extracts the zip beside it, or says where the
    /// zip path goes when no zip sits beside the folder.</summary>
    [Fact]
    public void TheExtractCommand_NamesTheZipBesideTheFolder_OrASlotForIt()
    {
        var parent = Path.Combine(Path.GetTempPath(), "darling-launcher-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(parent);
        try
        {
            var withZip = Path.Combine(parent, "PerformanceMonitorDarling-9.9.9");
            var withoutZip = Path.Combine(parent, "elsewhere o'dear");
            var apostropheZip = Path.Combine(parent, "o'dear-1.0");
            File.WriteAllBytes(withZip + ".zip", new byte[] { 0x50, 0x4B });
            File.WriteAllBytes(apostropheZip + ".zip", new byte[] { 0x50, 0x4B });

            var lines = Run(Functions("Get-DarlingExtractCommand") + $$"""

                (Get-DarlingExtractCommand '{{withZip}}\')
                (Get-DarlingExtractCommand '{{withoutZip.Replace("'", "''", StringComparison.Ordinal)}}')
                (Get-DarlingExtractCommand '{{apostropheZip.Replace("'", "''", StringComparison.Ordinal)}}')
                """);

            var programFiles = Environment.GetEnvironmentVariable("ProgramFiles") + "\\PerformanceMonitorDarling";
            Assert.Equal(3, lines.Count);
            Assert.Equal($"Expand-Archive -LiteralPath '{withZip}.zip' -DestinationPath '{programFiles}'", lines[0]);
            Assert.Equal($"Expand-Archive -LiteralPath '<path to the Darling zip you downloaded>' -DestinationPath '{programFiles}'", lines[1]);
            Assert.Equal($"Expand-Archive -LiteralPath '{apostropheZip.Replace("'", "''", StringComparison.Ordinal)}.zip' -DestinationPath '{programFiles}'", lines[2]);
        }
        finally
        {
            Directory.Delete(parent, recursive: true);
        }
    }

    /// <summary>Run beside no script, the launcher says so and stops with exit code 1 instead of closing at once. This
    /// branch comes before the elevation check, so the test never raises a Windows prompt.</summary>
    [Fact]
    public void TheLauncher_WithNoScriptBesideIt_SaysSoAndStops()
    {
        var folder = Path.Combine(Path.GetTempPath(), "darling-launcher-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(folder);
        try
        {
            var launcher = Path.Combine(folder, "Install-Darling.cmd");
            File.Copy(PathTo("Darling", "tools", "Install-Darling.cmd"), launcher);

            var startInfo = new ProcessStartInfo("cmd.exe", $"/d /c \"\"{launcher}\" -Network\"")
            {
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
            using var process = Process.Start(startInfo)!;
            process.StandardInput.Close();
            var stdout = process.StandardOutput.ReadToEnd();
            Assert.True(process.WaitForExit(60_000), "the launcher did not exit");

            Assert.Equal(1, process.ExitCode);
            Assert.Contains("install-darling.ps1 was not found beside this file", stdout, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    private static List<string> Run(string script) => DarlingInstallerConfigStepTests.RunWindowsPowerShell(script);
}
