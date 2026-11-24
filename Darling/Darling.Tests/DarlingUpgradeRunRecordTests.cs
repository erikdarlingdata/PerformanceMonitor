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
    private static string Run(string folder, string body)
    {
        var pwsh = PwshPath;
        Assert.SkipUnless(pwsh is not null, "neither Windows PowerShell 5.1 nor pwsh is installed on this machine.");
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));
        var text = new StringBuilder();
        text.AppendLine("$ErrorActionPreference = 'Stop'");
        foreach (var name in FunctionNames) { text.AppendLine(Extract(script, name)); }
        text.AppendLine("$root = '" + folder.Replace("'", "''") + "'");
        text.AppendLine(body);
        var driver = Path.Combine(folder, "driver.ps1");
        File.WriteAllText(driver, text.ToString(), new UTF8Encoding(true));

        using var process = Process.Start(new ProcessStartInfo(pwsh!, $"-NoProfile -ExecutionPolicy Bypass -File \"{driver}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        })!;
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
}
