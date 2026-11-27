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
using System.Text;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #9432: <c>upgrade-darling.ps1</c> writes <c>upgrade-run.json</c> beside the install at the start of a run, rewrites it at the end
/// with the result, and rewrites it to verified under <c>-MarkVerified</c>. The script needs an elevated shell and a registered service,
/// so the functions that build and write the record are extracted and run under PowerShell against a temp folder, the way the other
/// tests of this script run its functions. The end-to-end run of the script is not covered here: it is covered by the upgrade-shape
/// check in CI, and the finally block that calls <c>Close-DarlingUpgradeRun</c> has no behaviour test of its own.
/// </summary>
public sealed class DarlingUpgradeRunRecordTests
{
    private static readonly string[] FunctionNames =
    {
        "Get-DarlingBuildVersion", "New-DarlingUpgradeRun", "Complete-DarlingUpgradeRun", "Write-DarlingUpgradeRunFile",
        "Read-DarlingUpgradeRunFile", "Set-DarlingUpgradeRunVerified", "Close-DarlingUpgradeRun",
    };

    private static string? PwshPath
    {
        get
        {
            if (OperatingSystem.IsWindows())
            {
                var windowsPowerShell = Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.System), "WindowsPowerShell", "v1.0", "powershell.exe");
                if (File.Exists(windowsPowerShell)) { return windowsPowerShell; }

                var pwsh7 = Path.Combine(Environment.GetEnvironmentVariable("ProgramFiles") ?? @"C:\Program Files", "PowerShell", "7", "pwsh.exe");
                return File.Exists(pwsh7) ? pwsh7 : null;
            }

            return File.Exists("/opt/homebrew/bin/pwsh") ? "/opt/homebrew/bin/pwsh"
                : File.Exists("/usr/local/bin/pwsh") ? "/usr/local/bin/pwsh"
                : null;
        }
    }

    /// <summary>Runs the extracted functions plus <paramref name="body"/> under PowerShell and returns what the body wrote to the output.</summary>
    private static string Run(string folder, string body, params string[] extraFunctions)
    {
        var pwsh = PwshPath;
        Assert.SkipUnless(pwsh is not null, "neither Windows PowerShell 5.1 nor pwsh is installed on this machine.");
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var text = new StringBuilder();
        text.AppendLine("$ErrorActionPreference = 'Stop'");
        foreach (var name in FunctionNames.Concat(extraFunctions)) { text.AppendLine(Extract(script, name)); }
        text.AppendLine("$root = '" + folder.Replace("'", "''") + "'");
        text.AppendLine(body);
        var driver = Path.Combine(folder, "driver.ps1");
        File.WriteAllText(driver, text.ToString(), new UTF8Encoding(true));

        /* #9432: the shared helper drops the PSModulePath a PowerShell 7 parent (CI's step shell) hands down, which
           otherwise stops Windows PowerShell 5.1 loading Microsoft.PowerShell.Security, so Get-Acl and Set-Acl fail. */
        using var process = Process.Start(WindowsPowerShellStartInfo.Create(pwsh!, $"-NoProfile -ExecutionPolicy Bypass -File \"{driver}\""))!;
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        Assert.True(process.WaitForExit(120_000), "PowerShell did not finish");
        Assert.True(process.ExitCode == 0, $"PowerShell failed: {error}{output}");
        return output;
    }

    private static string Extract(string script, string name)
    {
        var start = script.IndexOf("function " + name, StringComparison.Ordinal);
        Assert.True(start >= 0, $"upgrade-darling.ps1 no longer defines {name} (#9432)");
        var open = script.IndexOf('{', start);
        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{') { depth++; }
            else if (script[i] == '}' && --depth == 0) { return script.Substring(start, i - start + 1); }
        }

        Assert.Fail($"unbalanced braces while extracting {name}");
        return string.Empty;
    }

    private static string NewFolder() => Directory.CreateTempSubdirectory("darling-9432-ps-").FullName;

    private static void Cleanup(string folder)
    {
        try { Directory.Delete(folder, recursive: true); } catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
    }

    private static JsonElement ReadFile(string folder)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(folder, "upgrade-run.json")));
        return document.RootElement.Clone();
    }

    [Fact]
    public void TheStartFile_HoldsTheRunIdStateVersionsAndStartTime_AndNoLeftoverTempFile()
    {
        var folder = NewFolder();
        try
        {
            Run(folder, "$run = New-DarlingUpgradeRun '3.14.0' '3.15.0' ([datetime]::UtcNow); $w = Write-DarlingUpgradeRunFile $root $run; if (-not $w.Ok) { throw $w.Reason }");
            var file = ReadFile(folder);
            Assert.True(Guid.TryParse(file.GetProperty("run_id").GetString(), out _));
            Assert.Equal("in_progress", file.GetProperty("state").GetString());
            Assert.Equal("3.14.0", file.GetProperty("tool_version").GetString());
            Assert.Equal("3.15.0", file.GetProperty("target_build").GetString());
            Assert.True(DateTime.TryParse(file.GetProperty("started_utc").GetString(), out _));
            Assert.Equal(JsonValueKind.Null, file.GetProperty("ended_utc").ValueKind);
            Assert.Equal(new[] { "upgrade-run.json" }, Directory.GetFiles(folder).Select(Path.GetFileName).Where(n => n != "driver.ps1").ToArray());
        }
        finally { Cleanup(folder); }
    }

    [Fact]
    public void TheEndRewrite_OnSuccess_KeepsTheRunId_AndCarriesTheResult()
    {
        var folder = NewFolder();
        try
        {
            Run(folder, "$run = New-DarlingUpgradeRun 'a' 'b' ([datetime]::UtcNow); Write-DarlingUpgradeRunFile $root $run | Out-Null; Close-DarlingUpgradeRun $root $run $true 'clear' 'C:\\x\\_rollback_manual_1' '' | Out-Null");
            var file = ReadFile(folder);
            Assert.Equal("succeeded", file.GetProperty("state").GetString());
            Assert.Equal(0, file.GetProperty("exit_code").GetInt32());
            Assert.Equal("clear", file.GetProperty("stop_check").GetString());
            Assert.Equal("C:\\x\\_rollback_manual_1", file.GetProperty("rollback_folder").GetString());
            Assert.Equal(JsonValueKind.Null, file.GetProperty("first_error").ValueKind);
            Assert.NotEqual(JsonValueKind.Null, file.GetProperty("ended_utc").ValueKind);
        }
        finally { Cleanup(folder); }
    }

    [Fact]
    public void TheEndRewrite_OnFailure_CarriesTheExitCode_TheFirstError_AndTheStopCheck()
    {
        var folder = NewFolder();
        try
        {
            Run(folder, "$run = New-DarlingUpgradeRun 'a' 'b' ([datetime]::UtcNow); Write-DarlingUpgradeRunFile $root $run | Out-Null; Close-DarlingUpgradeRun $root $run $false 'blocked: postgres (pid 4)' '' 'the copy failed twice' | Out-Null");
            var file = ReadFile(folder);
            Assert.Equal("failed", file.GetProperty("state").GetString());
            Assert.Equal(1, file.GetProperty("exit_code").GetInt32());
            Assert.Equal("the copy failed twice", file.GetProperty("first_error").GetString());
            Assert.Equal("blocked: postgres (pid 4)", file.GetProperty("stop_check").GetString());
            Assert.Equal(JsonValueKind.Null, file.GetProperty("rollback_folder").ValueKind);
        }
        finally { Cleanup(folder); }
    }

    [Fact]
    public void MarkVerified_RewritesASucceededRun_ButNotAFailedOrUnfinishedOne_AndHoldsNoSecret()
    {
        var folder = NewFolder();
        try
        {
            var body = "$run = New-DarlingUpgradeRun 'a' 'b' ([datetime]::UtcNow); Write-DarlingUpgradeRunFile $root $run | Out-Null; "
                + "$early = Set-DarlingUpgradeRunVerified $root ([datetime]::UtcNow); 'early=' + $early.Ok; "
                + "Close-DarlingUpgradeRun $root $run $false '' '' 'boom' | Out-Null; $bad = Set-DarlingUpgradeRunVerified $root ([datetime]::UtcNow); 'failed=' + $bad.Ok; "
                + "Close-DarlingUpgradeRun $root $run $true 'clear' '' '' | Out-Null; $ok = Set-DarlingUpgradeRunVerified $root ([datetime]::UtcNow); 'ok=' + $ok.Ok; "
                + "$again = Set-DarlingUpgradeRunVerified $root ([datetime]::UtcNow); 'again=' + $again.Ok";
            var output = Run(folder, body);
            Assert.Contains("early=False", output, StringComparison.Ordinal);
            Assert.Contains("failed=False", output, StringComparison.Ordinal);
            Assert.Contains("ok=True", output, StringComparison.Ordinal);
            Assert.Contains("again=True", output, StringComparison.Ordinal);
            var file = ReadFile(folder);
            Assert.Equal("verified", file.GetProperty("state").GetString());
            Assert.NotEqual(JsonValueKind.Null, file.GetProperty("verified_utc").ValueKind);
            var names = file.EnumerateObject().Select(p => p.Name).ToHashSet();
            Assert.DoesNotContain(names, n => n.Contains("password", StringComparison.OrdinalIgnoreCase)
                || n.Contains("token", StringComparison.OrdinalIgnoreCase) || n.Contains("connection", StringComparison.OrdinalIgnoreCase));
        }
        finally { Cleanup(folder); }
    }

    [Fact]
    public void MarkVerified_WithNoFile_ChangesNothing_AndSaysSo()
    {
        var folder = NewFolder();
        try
        {
            var output = Run(folder, "$r = Set-DarlingUpgradeRunVerified $root ([datetime]::UtcNow); 'ok=' + $r.Ok");
            Assert.Contains("ok=False", output, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(folder, "upgrade-run.json")));
        }
        finally { Cleanup(folder); }
    }

    [Fact]
    public void AWriteThatFails_LeavesNoTempFile_AndTheOldFileUntouched()
    {
        var folder = NewFolder();
        try
        {
            Run(folder, "$run = New-DarlingUpgradeRun 'a' 'b' ([datetime]::UtcNow); Write-DarlingUpgradeRunFile $root $run | Out-Null");
            var before = File.ReadAllText(Path.Combine(folder, "upgrade-run.json"));
            /* A write that cannot finish: the run is not a record the JSON converter can write a file from, so the temp file never gets made; and a
               held lock on the target makes the replace itself fail. Either way no partial file may remain and the old file must stay whole. */
            using (new FileStream(Path.Combine(folder, "upgrade-run.json"), FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var output = Run(folder, "$run = New-DarlingUpgradeRun 'a' 'b' ([datetime]::UtcNow); $run['state'] = 'succeeded'; $w = Write-DarlingUpgradeRunFile $root $run; 'ok=' + $w.Ok");
                if (OperatingSystem.IsWindows()) { Assert.Contains("ok=False", output, StringComparison.Ordinal); }
            }

            Assert.Empty(Directory.GetFiles(folder, "*.tmp"));
            if (OperatingSystem.IsWindows()) { Assert.Equal(before, File.ReadAllText(Path.Combine(folder, "upgrade-run.json"))); }
        }
        finally { Cleanup(folder); }
    }

    /// <summary>
    /// #9850: the in_progress record is written while the service runs, over the file a previous run left. A holder that lets writers in but
    /// not deleters (no FileShare.Delete) makes File.Replace throw, and the record must still land, in place. The old code failed this case
    /// with a sharing violation and left the previous run's record in the file.
    /// </summary>
    [Fact]
    public void TheInProgressWrite_Lands_WhenTheOldFileIsHeldWithoutDeleteSharing()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "File.Replace and share modes are Windows behaviour.");
        var folder = NewFolder();
        try
        {
            Run(folder, "$run = New-DarlingUpgradeRun 'a' 'old-target' ([datetime]::UtcNow); $run['state'] = 'succeeded'; Write-DarlingUpgradeRunFile $root $run | Out-Null");
            using (new FileStream(Path.Combine(folder, "upgrade-run.json"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var output = Run(folder, "$run = New-DarlingUpgradeRun 'a' 'new-target' ([datetime]::UtcNow); $w = Write-DarlingUpgradeRunFile $root $run; 'ok=' + $w.Ok + ' reason=[' + $w.Reason + ']'");
                Assert.Contains("ok=True", output, StringComparison.Ordinal);
            }

            var file = ReadFile(folder);
            Assert.Equal("in_progress", file.GetProperty("state").GetString());
            Assert.Equal("new-target", file.GetProperty("target_build").GetString());
            Assert.Empty(Directory.GetFiles(folder, "*.tmp"));
        }
        finally { Cleanup(folder); }
    }

    /// <summary>#9850: the service's own pickup opens the file with ReadWrite and Delete sharing. The in_progress write must land with that handle open.</summary>
    [Fact]
    public void TheInProgressWrite_Lands_WhenTheOldFileIsHeldTheWayTheServiceHoldsIt()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "File.Replace and share modes are Windows behaviour.");
        var folder = NewFolder();
        try
        {
            Run(folder, "$run = New-DarlingUpgradeRun 'a' 'old-target' ([datetime]::UtcNow); $run['state'] = 'succeeded'; Write-DarlingUpgradeRunFile $root $run | Out-Null");
            using (new FileStream(Path.Combine(folder, "upgrade-run.json"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
            {
                var output = Run(folder, "$run = New-DarlingUpgradeRun 'a' 'new-target' ([datetime]::UtcNow); $w = Write-DarlingUpgradeRunFile $root $run; 'ok=' + $w.Ok");
                Assert.Contains("ok=True", output, StringComparison.Ordinal);
            }

            var file = ReadFile(folder);
            Assert.Equal("in_progress", file.GetProperty("state").GetString());
            Assert.Equal("new-target", file.GetProperty("target_build").GetString());
        }
        finally { Cleanup(folder); }
    }

    /// <summary>
    /// #9850: a write that fails with nothing able to get past the holder (a reader that lets in no writer and no deleter) leaves the old file whole, and
    /// the warning for the in_progress record names that record and does not tell the operator the store will not show the run. The final record's
    /// warning does say so.
    /// </summary>
    [Fact]
    public void AFailedWrite_NamesTheRecordThatIsMissing_AndOnlyTheFinalOneSaysTheStoreWillNotShowTheRun()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "share modes are Windows behaviour.");
        var folder = NewFolder();
        try
        {
            Run(folder, "$run = New-DarlingUpgradeRun 'a' 'old-target' ([datetime]::UtcNow); Write-DarlingUpgradeRunFile $root $run | Out-Null");
            string output;
            using (new FileStream(Path.Combine(folder, "upgrade-run.json"), FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                output = Run(folder,
                    "$run = New-DarlingUpgradeRun 'a' 'new-target' ([datetime]::UtcNow); $w = Write-DarlingUpgradeRunFile $root $run; 'ok=' + $w.Ok; "
                    + "'start=[' + (Get-DarlingUpgradeRunWriteWarning 'in_progress' $w.Reason) + ']'; 'end=[' + (Get-DarlingUpgradeRunWriteWarning 'final' $w.Reason) + ']'",
                    "Get-DarlingUpgradeRunWriteWarning");
            }

            Assert.Contains("ok=False", output, StringComparison.Ordinal);
            var lines = output.Split('\n').Select(l => l.Trim()).ToArray();
            var start = lines.Single(l => l.StartsWith("start=[", StringComparison.Ordinal));
            var end = lines.Single(l => l.StartsWith("end=[", StringComparison.Ordinal));
            Assert.Contains("in_progress", start, StringComparison.Ordinal);
            Assert.DoesNotContain("will not show", start, StringComparison.Ordinal);
            Assert.Contains("final", end, StringComparison.Ordinal);
            Assert.Contains("will not show", end, StringComparison.Ordinal);
            Assert.Equal("old-target", ReadFile(folder).GetProperty("target_build").GetString());
        }
        finally { Cleanup(folder); }
    }

    private static readonly string[] ServiceWrittenListFunctions =
    {
        "Get-DarlingServiceWrittenNames", "Test-DarlingServiceWrittenFileName", "Test-PathIsAtOrUnder", "Get-DarlingFileNameCount",
        "Initialize-DarlingNativeOwner", "Test-DarlingServiceWrittenPath", "Get-DarlingServiceWrittenRootFiles", "Get-DarlingCanonicalPath",
        "Get-DarlingSegmentsBelowRoot",
    };

    /// <summary>
    /// The review of #9493: the service account (NT SERVICE account, never LocalSystem) holds an explicit Modify grant on upgrade-run.json,
    /// and the pre-lock walk of a re-run named it as a stranger's write grant, so a run that failed before the lock blocked the documented
    /// re-run. A real SID that is not this account (Network Service) stands for the service: granting on a file this account owns needs no
    /// admin. The control run, with no service SID passed, shows the walk does see the grant.
    /// </summary>
    [Fact]
    public void TheServicesGrantOnTheRunFile_IsNotAStrangersGrant_ToThePreLockWalk()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the grant is an NTFS ACL.");
        var folder = NewFolder();
        try
        {
            var body = "$run = New-DarlingUpgradeRun 'a' 'b' ([datetime]::UtcNow); $w = Write-DarlingUpgradeRunFile $root $run '*S-1-5-20'; 'warning=[' + $w.Warning + ']'; "
                + "$wk = [System.Security.Principal.WellKnownSidType]; $service = New-Object System.Security.Principal.SecurityIdentifier('S-1-5-20'); "
                + "$trusted = @([System.Security.Principal.WindowsIdentity]::GetCurrent().User, (New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)), (New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null))); "
                + "$rule = @((Get-Acl -LiteralPath (Join-Path $root 'upgrade-run.json')).GetAccessRules($true, $false, [System.Security.Principal.SecurityIdentifier]) | Where-Object { $_.IdentityReference -eq $service }); 'granted=' + ($rule.Count -gt 0); "
                + "$control = @(Get-UntrustedWriteGrantees $root $trusted -Recurse | Where-Object { $_ -like '*upgrade-run.json*' }); 'control=' + $control.Count; "
                + "$walk = @(Get-UntrustedWriteGrantees $root $trusted -Recurse -ServiceSids @($service) | Where-Object { $_ -like '*upgrade-run.json*' }); 'walk=' + $walk.Count; "
                + "'trustedPath=' + (Test-DarlingServiceWrittenPath $root (Join-Path $root 'upgrade-run.json') @() (Get-Item -LiteralPath (Join-Path $root 'upgrade-run.json')))";
            var output = Run(folder, body, ServiceWrittenListFunctions.Append("Get-UntrustedWriteGrantees").ToArray());
            Assert.Contains("granted=True", output, StringComparison.Ordinal);
            Assert.Contains("control=1", output, StringComparison.Ordinal);
            Assert.Contains("walk=0", output, StringComparison.Ordinal);
            Assert.Contains("trustedPath=True", output, StringComparison.Ordinal);
        }
        finally { Cleanup(folder); }
    }

    /// <summary>
    /// The review of #9493: the install-tree lock kept resetting the run file's explicit grant, and File.Replace keeps the DACL of the file it
    /// replaces, so after the lock the service could never delete the file. With the file on the service-written list the lock keeps the
    /// grant, and a rewrite after the lock (the run's end, or -MarkVerified) keeps it too. The lock needs an elevated token for its owner step:
    /// without one it reports the file as not owned by the service, which this test ignores, and still sets the permissions it asserts on.
    /// </summary>
    [Fact]
    public void AfterTheInstallTreeLock_AndARewrite_TheServiceStillHoldsModifyOnTheRunFile()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the grant is an NTFS ACL.");
        var folder = NewFolder();
        try
        {
            var body = "$run = New-DarlingUpgradeRun 'a' 'b' ([datetime]::UtcNow); Write-DarlingUpgradeRunFile $root $run 'NT AUTHORITY\\NETWORK SERVICE' | Out-Null; "
                + "$service = New-Object System.Security.Principal.SecurityIdentifier('S-1-5-20'); $file = Join-Path $root 'upgrade-run.json'; "
                + "function Get-ServiceModify { @((Get-Acl -LiteralPath $file).GetAccessRules($true, $false, [System.Security.Principal.SecurityIdentifier]) | Where-Object { $_.IdentityReference -eq $service -and ([int]$_.FileSystemRights -band 0x10000) -ne 0 }).Count -gt 0 }; "
                + "'before=' + (Get-ServiceModify); "
                + "$open = @(Lock-DarlingInstallTree $root 'NT AUTHORITY\\NETWORK SERVICE'); 'lockRan=' + ($open.Count -ge 0); "
                + "'afterLock=' + (Get-ServiceModify); "
                + "Close-DarlingUpgradeRun $root $run $true 'clear' '' '' 'NT AUTHORITY\\NETWORK SERVICE' | Out-Null; "
                + "'afterRewrite=' + (Get-ServiceModify); "
                + "'state=' + (Read-DarlingUpgradeRunFile $root)['state']";
            var output = Run(folder, body, ServiceWrittenListFunctions.Append("Lock-DarlingInstallTree").ToArray());
            Assert.Contains("before=True", output, StringComparison.Ordinal);
            Assert.Contains("afterLock=True", output, StringComparison.Ordinal);
            Assert.Contains("afterRewrite=True", output, StringComparison.Ordinal);
            Assert.Contains("state=succeeded", output, StringComparison.Ordinal);
        }
        finally
        {
            try { _ = System.Diagnostics.Process.Start(new ProcessStartInfo("icacls.exe", $"\"{folder}\" /reset /T /C /Q") { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true })?.WaitForExit(30_000); }
            catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException) { }
            Cleanup(folder);
        }
    }
}
