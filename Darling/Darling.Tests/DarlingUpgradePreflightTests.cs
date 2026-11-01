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
using System.Security.Principal;
using System.Text;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6982: <c>upgrade-darling.ps1</c> tries the backup-folder step on a throwaway folder BEFORE it stops the service, and
/// <c>-Check</c> runs the checks that come before the stop and that pre-flight, then exits without changing anything.
/// #6920 failed in that step under PowerShell 7 after the service was already stopped.
///
/// <para>The pre-flight function is lifted out of the script and run in scratch folders under both hosts. The whole script
/// is run too, with its elevation test replaced and the service calls (<c>Get-Service</c>, <c>Get-CimInstance</c> for the
/// service, <c>Stop-Service</c>) stood in for by functions, against a temp install root: no service is touched, and a
/// recorded stop is how a test knows the script went past the pre-flight. Every test runs under Windows PowerShell 5.1 and
/// PowerShell 7 (pwsh.exe, skipped where it is not on PATH).</para>
///
/// <para>The success paths need an elevated token, as the script itself does: the throwaway folder is limited to SYSTEM and
/// Administrators, and only an elevated administrator can remove it afterwards. They are skipped on an unelevated machine
/// and run on the hosted Windows runners, which are elevated.</para>
/// </summary>
public sealed class DarlingUpgradePreflightTests
{
    private const string ElevationTest =
        "if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {";

    private static string Script() =>
        ReadRepoFile("Darling", "tools", "upgrade-darling.ps1").Replace("\r\n", "\n", StringComparison.Ordinal);

    private static bool IsElevated()
    {
        if (!OperatingSystem.IsWindows())
        {
            return false;
        }

        using var identity = WindowsIdentity.GetCurrent();
        return new WindowsPrincipal(identity).IsInRole(WindowsBuiltInRole.Administrator);
    }

    private static bool SkipUnlessRunnable(string host, bool needsElevation)
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("DACLs and Windows PowerShell are Windows'.");
            return true;
        }

        if (host == "pwsh" && !OnPath("pwsh.exe"))
        {
            Assert.Skip("PowerShell 7 (pwsh.exe) is not on PATH on this machine.");
            return true;
        }

        if (needsElevation && !IsElevated())
        {
            Assert.Skip("The success path removes a folder limited to SYSTEM and Administrators, which needs an elevated token.");
            return true;
        }

        return false;
    }

    private static bool OnPath(string exe) =>
        (Environment.GetEnvironmentVariable("PATH") ?? string.Empty)
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Any(dir => File.Exists(Path.Combine(dir.Trim('"'), exe)));

    private static string ExtractFunction(string script, string name)
    {
        var start = script.IndexOf("function " + name + "(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"upgrade-darling.ps1 no longer defines {name}");

        var open = script.IndexOf('{', start);
        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{')
            {
                depth++;
            }
            else if (script[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return script.Substring(start, i - start + 1);
                }
            }
        }

        throw new InvalidOperationException($"{name} has unbalanced braces.");
    }

    /// <summary>Runs <paramref name="ps1"/> in <paramref name="host"/> and returns its stdout lines.</summary>
    private static List<string> Run(string ps1, string host, string extraArguments = "")
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-6982-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, ps1, new UTF8Encoding(false));
        try
        {
            using var process = Process.Start(WindowsPowerShellStartInfo.Create(host, $"-NoProfile -ExecutionPolicy Bypass -File \"{path}\" {extraArguments}"));
            Assert.NotNull(process);

            var stdoutTask = process!.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();

            var exited = process.WaitForExit(150_000);
            if (!exited)
            {
                try { process.Kill(entireProcessTree: true); }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or System.ComponentModel.Win32Exception)
                {
                    /* Already gone between the timeout and the kill. */
                }
            }

            var stdout = stdoutTask.GetAwaiter().GetResult();
            var stderr = stderrTask.GetAwaiter().GetResult();
            Assert.True(exited, $"{host} did not exit within 150 seconds. Output so far:\n{stdout}\n{stderr}");
            Assert.True(string.IsNullOrWhiteSpace(stderr), $"{host} reported an error:\n{stderr}\nOutput:\n{stdout}");

            return stdout.Split('\n').Select(line => line.Trim()).Where(line => line.Length > 0).ToList();
        }
        finally
        {
            try { File.Delete(path); }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                /* A temp file that cannot be removed is not a test failure. */
            }
        }
    }

    // The function under test, with the two it calls, run against a scratch install root that holds one file. 'fail' makes
    // Set-Acl throw, which is what a shell that cannot restrict a folder looks like to the step.
    private static string FunctionProbe(string mode)
    {
        var script = Script();
        var probe = new StringBuilder();
        foreach (var name in new[] { "Get-DarlingFileNameCount", "Protect-DarlingRollbackBackupFolder", "Test-DarlingUpgradePreflight" })
        {
            probe.AppendLine(ExtractFunction(script, name));
        }

        probe.AppendLine($$"""
            $ErrorActionPreference = 'Stop'
            $mode = '{{mode}}'
            if ($mode -eq 'fail') { function Set-Acl { throw 'stubbed failure' } }
            $root = Join-Path $env:TEMP ('pm6982-fn-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            New-Item -ItemType Directory -Path $root | Out-Null
            Set-Content -LiteralPath (Join-Path $root 'PerformanceMonitor.Darling.Service.exe') -Value 'x'
            try {
                $before = (Get-ChildItem -LiteralPath $root -Force | ForEach-Object { $_.Name }) -join ';'
                'edition=' + $PSVersionTable.PSEdition
                $problem = Test-DarlingUpgradePreflight $root
                'problem=' + $(if ($problem) { $problem } else { 'none' })
                $after = (Get-ChildItem -LiteralPath $root -Force | ForEach-Object { $_.Name }) -join ';'
                'unchanged=' + ($before -eq $after)
                'left=' + $after
            }
            finally { Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue }
            """);
        return probe.ToString();
    }

    [Theory]
    [InlineData("pwsh")]
    [InlineData("powershell.exe")]
    public void ThePreflight_RestrictsAThrowawayFolder_ThenRemovesIt_InBothShells(string host)
    {
        if (SkipUnlessRunnable(host, needsElevation: true))
        {
            return;
        }

        var all = string.Join("\n", Run(FunctionProbe("ok"), host));

        Assert.Contains(host == "pwsh" ? "edition=Core" : "edition=Desktop", all, StringComparison.Ordinal);
        Assert.Contains("problem=none", all, StringComparison.Ordinal);
        Assert.Contains("unchanged=True", all, StringComparison.Ordinal);
        Assert.Contains("left=PerformanceMonitor.Darling.Service.exe", all, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pwsh")]
    [InlineData("powershell.exe")]
    public void ThePreflight_NamesTheReason_WhenTheRestrictStepFails_AndLeavesNothingBehind(string host)
    {
        if (SkipUnlessRunnable(host, needsElevation: false))
        {
            return;
        }

        var all = string.Join("\n", Run(FunctionProbe("fail"), host));

        Assert.Contains("could not be limited to SYSTEM and Administrators (stubbed failure)", all, StringComparison.Ordinal);
        Assert.Contains("unchanged=True", all, StringComparison.Ordinal);
    }

    // The whole script, against a temp install root. The wrapper stands in for the service and records a Stop-Service call.
    // The script's own copy goes in a folder of its own, so it is not running from the install root.
    private static string ScriptWrapper(string mode, string scriptArguments, string patchedScriptPath, string setup = "", string sourceArgument = "$src")
    {
        return $$"""
            $ErrorActionPreference = 'Stop'
            $mode = '{{mode}}'
            $patched = '{{patchedScriptPath}}'
            $base = Join-Path $env:TEMP ('pm6982-run-' + [guid]::NewGuid().ToString('N').Substring(0, 8))
            $root = Join-Path $base 'install'; $src = Join-Path $base 'source'
            New-Item -ItemType Directory -Path $root, $src | Out-Null
            Set-Content -LiteralPath (Join-Path $root 'PerformanceMonitor.Darling.Service.exe') -Value 'x'
            Set-Content -LiteralPath (Join-Path $src 'PerformanceMonitor.Darling.Service.exe') -Value 'y'
            Set-Content -LiteralPath (Join-Path $root 'darling.json') -Value '{}'
            function Get-Service { [pscustomobject]@{ Name = 'PerformanceMonitor Darling'; Status = 'Running' } }
            function Get-CimInstance {
                param([string]$ClassName, [string]$Filter, $ErrorAction)
                if ($ClassName -eq 'Win32_Service') { return [pscustomobject]@{ Name = 'PerformanceMonitor Darling'; StartName = 'LocalSystem'; PathName = '"' + (Join-Path $root 'PerformanceMonitor.Darling.Service.exe') + '"'; ProcessId = 0 } }
                & (Get-Command Get-CimInstance -CommandType Cmdlet) @PSBoundParameters
            }
            function Stop-Service { Add-Content -LiteralPath (Join-Path $base 'stop-called.txt') -Value 'stop' }
            if ($mode -eq 'fail') { function Set-Acl { throw 'stubbed failure' } }
            $listing = { (Get-ChildItem -LiteralPath $root -Force -Recurse | ForEach-Object { $_.FullName.Substring($root.Length) + '|' + $_.Length }) -join ';' }
            {{setup}}
            try {
                $before = & $listing
                & $patched {{scriptArguments}} -Source {{sourceArgument}} -InstallRoot $root -AcceptWritableExtraction -SkipHashCheck
                'exit=' + $LASTEXITCODE
                'unchanged=' + ($before -eq (& $listing))
                'stopCalled=' + (Test-Path -LiteralPath (Join-Path $base 'stop-called.txt'))
            }
            finally { Remove-Item -LiteralPath $base -Recurse -Force -ErrorAction SilentlyContinue }
            """;
    }

    /// <summary>Runs the whole script, with its elevation test replaced, in <paramref name="host"/> over a temp install root.</summary>
    private static List<string> RunScript(string host, string mode, string scriptArguments, string setup = "", string sourceArgument = "$src")
    {
        var script = Script();
        Assert.Contains(ElevationTest, script, StringComparison.Ordinal);
        var folder = Directory.CreateTempSubdirectory("darling-6982-script-").FullName;
        try
        {
            var patched = Path.Combine(folder, "upgrade-darling.ps1");
            File.WriteAllText(patched, script.Replace(ElevationTest, "if ($false) {", StringComparison.Ordinal), new UTF8Encoding(false));
            return Run(ScriptWrapper(mode, scriptArguments, patched, setup, sourceArgument), host);
        }
        finally
        {
            Directory.Delete(folder, recursive: true);
        }
    }

    [Theory]
    [InlineData("pwsh")]
    [InlineData("powershell.exe")]
    public void Check_RunsThePreflight_ChangesNothing_AndNeverStopsTheService(string host)
    {
        if (SkipUnlessRunnable(host, needsElevation: true))
        {
            return;
        }

        var all = string.Join("\n", RunScript(host, "ok", "-Check"));

        Assert.Contains(host == "pwsh" ? "(Core edition)" : "(Desktop edition)", all, StringComparison.Ordinal);
        Assert.Contains("Pre-flight passed", all, StringComparison.Ordinal);
        Assert.Contains("-Check passed", all, StringComparison.Ordinal);
        Assert.Contains("exit=0", all, StringComparison.Ordinal);
        Assert.Contains("unchanged=True", all, StringComparison.Ordinal);
        Assert.Contains("stopCalled=False", all, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pwsh", "-Check")]
    [InlineData("powershell.exe", "-Check")]
    [InlineData("pwsh", "")]
    [InlineData("powershell.exe", "")]
    public void AFailingPreflight_StopsBeforeTheServiceStop_AndNamesTheReason(string host, string scriptArguments)
    {
        if (SkipUnlessRunnable(host, needsElevation: false))
        {
            return;
        }

        var all = string.Join("\n", RunScript(host, "fail", scriptArguments));

        Assert.Contains("The pre-flight failed under PowerShell", all, StringComparison.Ordinal);
        Assert.Contains("could not be limited to SYSTEM and Administrators (stubbed failure)", all, StringComparison.Ordinal);
        Assert.Contains("the service is still running and the install is untouched", all, StringComparison.Ordinal);
        Assert.Contains("exit=1", all, StringComparison.Ordinal);
        Assert.Contains("unchanged=True", all, StringComparison.Ordinal);
        Assert.Contains("stopCalled=False", all, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pwsh", "-PruneOnly")]
    [InlineData("powershell.exe", "-PruneOnly")]
    [InlineData("pwsh", "-ListRollbacks")]
    [InlineData("powershell.exe", "-ListRollbacks")]
    public void Check_CannotBeCombinedWithAnActingSwitch(string host, string other)
    {
        if (SkipUnlessRunnable(host, needsElevation: false))
        {
            return;
        }

        var all = string.Join("\n", RunScript(host, "ok", "-Check " + other));

        Assert.Contains("cannot be combined with -PruneOnly or -ListRollbacks", all, StringComparison.Ordinal);
        Assert.Contains("exit=1", all, StringComparison.Ordinal);
        Assert.Contains("unchanged=True", all, StringComparison.Ordinal);
        Assert.Contains("stopCalled=False", all, StringComparison.Ordinal);
    }

    // #7063: a build with no hostfxr.dll ships no .NET runtime. Over an install that has one, it leaves the old runtime in
    // place and the service cannot find a framework. The refusal comes before the service stop, whichever form the source takes.
    private const string InstallWithRuntime =
        "Set-Content -LiteralPath (Join-Path $root 'hostfxr.dll') -Value 'old runtime'";

    private const string SourceWithRuntime =
        "Set-Content -LiteralPath (Join-Path $src 'hostfxr.dll') -Value 'new runtime'";

    // A zip of the source folder, plus a nested viewer folder hostfxr.dll that must not count as the service's runtime.
    private const string SourceZip =
        "$zsrc = Join-Path $base 'zipsrc'; New-Item -ItemType Directory -Path $zsrc | Out-Null; " +
        "Copy-Item -LiteralPath (Join-Path $src 'PerformanceMonitor.Darling.Service.exe') -Destination $zsrc; " +
        "New-Item -ItemType Directory -Path (Join-Path $zsrc 'viewer') | Out-Null; " +
        "Set-Content -LiteralPath (Join-Path $zsrc 'viewer\\hostfxr.dll') -Value 'viewer runtime'; " +
        "ZIPEXTRA " +
        "$zip = Join-Path $base 'PerformanceMonitorDarling-9.9.9.zip'; Compress-Archive -Path (Join-Path $zsrc '*') -DestinationPath $zip";

    private const string RefusalText = "framework-dependent build";

    [Theory]
    [InlineData("pwsh", "-Check")]
    [InlineData("powershell.exe", "-Check")]
    [InlineData("pwsh", "")]
    [InlineData("powershell.exe", "")]
    public void AFolderWithNoRuntime_OverAnInstallThatHasOne_IsRefused_BeforeTheServiceStop(string host, string scriptArguments)
    {
        if (SkipUnlessRunnable(host, needsElevation: false))
        {
            return;
        }

        var all = string.Join("\n", RunScript(host, "ok", scriptArguments, InstallWithRuntime));

        Assert.Contains(RefusalText, all, StringComparison.Ordinal);
        Assert.Contains("self-contained install", all, StringComparison.Ordinal);
        Assert.Contains("--runtime win-x64 --self-contained", all, StringComparison.Ordinal);
        Assert.Contains("NOTHING has been stopped or copied", all, StringComparison.Ordinal);
        Assert.Contains("exit=1", all, StringComparison.Ordinal);
        Assert.Contains("unchanged=True", all, StringComparison.Ordinal);
        Assert.Contains("stopCalled=False", all, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pwsh")]
    [InlineData("powershell.exe")]
    public void AZipWithNoRuntimeAtItsRoot_OverAnInstallThatHasOne_IsRefused_BeforeTheServiceStop(string host)
    {
        if (SkipUnlessRunnable(host, needsElevation: true))
        {
            return;
        }

        var setup = InstallWithRuntime + "; " + SourceZip.Replace("ZIPEXTRA ", string.Empty, StringComparison.Ordinal);
        var all = string.Join("\n", RunScript(host, "ok", "-Check", setup, "$zip"));

        Assert.Contains(RefusalText, all, StringComparison.Ordinal);
        Assert.Contains("exit=1", all, StringComparison.Ordinal);
        Assert.Contains("unchanged=True", all, StringComparison.Ordinal);
        Assert.Contains("stopCalled=False", all, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pwsh")]
    [InlineData("powershell.exe")]
    public void AnUnreadableZip_FailsBeforeTheServiceStop_WithItsOwnMessage(string host)
    {
        if (SkipUnlessRunnable(host, needsElevation: true))
        {
            return;
        }

        // #7063: the file has a .zip name, but its bytes are not a zip, so the runtime check cannot open it.
        // It must refuse with its own message, before the service stop.
        var setup = InstallWithRuntime + "; " + "$zip = Join-Path $base 'PerformanceMonitorDarling-9.9.9.zip'; Set-Content -LiteralPath $zip -Value 'not a zip'";
        var all = string.Join("\n", RunScript(host, "ok", "-Check", setup, "$zip"));

        Assert.Contains("could not be read to check whether it carries a .NET runtime", all, StringComparison.Ordinal);
        Assert.Contains("exit=1", all, StringComparison.Ordinal);
        Assert.Contains("unchanged=True", all, StringComparison.Ordinal);
        Assert.Contains("stopCalled=False", all, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pwsh", true)]
    [InlineData("powershell.exe", true)]
    [InlineData("pwsh", false)]
    [InlineData("powershell.exe", false)]
    public void ASelfContainedFolder_OverAnInstallWithOrWithoutARuntime_PassesTheCheck(string host, bool installHasRuntime)
    {
        if (SkipUnlessRunnable(host, needsElevation: true))
        {
            return;
        }

        // installHasRuntime: self-contained over self-contained. Otherwise self-contained over framework-dependent, the reverse direction.
        var setup = SourceWithRuntime + (installHasRuntime ? "; " + InstallWithRuntime : string.Empty);
        var all = string.Join("\n", RunScript(host, "ok", "-Check", setup));

        Assert.DoesNotContain(RefusalText, all, StringComparison.Ordinal);
        Assert.Contains("-Check passed", all, StringComparison.Ordinal);
        Assert.Contains("exit=0", all, StringComparison.Ordinal);
        Assert.Contains("stopCalled=False", all, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("pwsh")]
    [InlineData("powershell.exe")]
    public void AZipWithARuntimeAtItsRoot_OverAnInstallThatHasOne_PassesTheCheck(string host)
    {
        if (SkipUnlessRunnable(host, needsElevation: true))
        {
            return;
        }

        var zipExtra = "Set-Content -LiteralPath (Join-Path $zsrc 'hostfxr.dll') -Value 'new runtime'; ";
        var setup = InstallWithRuntime + "; " + SourceZip.Replace("ZIPEXTRA ", zipExtra, StringComparison.Ordinal);
        var all = string.Join("\n", RunScript(host, "ok", "-Check", setup, "$zip"));

        Assert.DoesNotContain(RefusalText, all, StringComparison.Ordinal);
        Assert.Contains("-Check passed", all, StringComparison.Ordinal);
        Assert.Contains("exit=0", all, StringComparison.Ordinal);
        Assert.Contains("stopCalled=False", all, StringComparison.Ordinal);
    }
}
