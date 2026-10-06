/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5413: a SUCCESSFUL scripted <c>--encrypt-password</c> must not write to the error stream.
///
/// <para>Windows PowerShell 5.1 turns every line a native command writes to a captured or redirected error
/// stream into an error record, so under <c>$ErrorActionPreference = 'Stop'</c> a run that succeeded still
/// stopped the script. The verb used to write its <c>Password: </c> prompt and its "Paste the line above"
/// hint to stderr unconditionally. The prompt now appears only when input comes from a person, the hint only
/// when output goes to a console, and the error paths (including the #2097 empty-read guidance) are
/// unchanged.</para>
///
/// <para>The verb is inline in <c>Program.cs</c>, so these tests run the built service executable as a
/// process, the way the install script and an operator's script do.</para>
/// </summary>
public class DarlingEncryptPasswordStreamTests
{
    private const string DummyInput = "dummy-input-for-the-test";

    private sealed record RunResult(int ExitCode, string StdOut, string StdErr);

    /// <summary>
    /// The service's own build output folder, found from the test output folder. The test output
    /// folder's copy of the executable is not runnable on its own: it lacks the DPAPI assembly the service ships with.
    /// </summary>
    private static string ServiceExePath()
    {
        var tfmDir = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        var configDir = tfmDir.Parent!;
        var testProjectDir = configDir.Parent!.Parent!;
        return Path.Combine(
            testProjectDir.Parent!.FullName, "PerformanceMonitor.Darling.Service", "bin", configDir.Name, "net10.0",
            "PerformanceMonitor.Darling.Service.exe");
    }

    private static ProcessStartInfo ServiceStartInfo()
    {
        var exe = ServiceExePath();
        Assert.True(File.Exists(exe), $"The service build output was not found at {exe}.");
        return new ProcessStartInfo(exe);
    }

    private static async Task<RunResult> RunEncryptPassword(string? stdin)
    {
        var info = ServiceStartInfo();
        info.ArgumentList.Add("--encrypt-password");
        info.RedirectStandardInput = true;
        info.RedirectStandardOutput = true;
        info.RedirectStandardError = true;
        info.UseShellExecute = false;
        info.CreateNoWindow = true;

        using var process = Process.Start(info);
        Assert.NotNull(process);

        if (stdin is not null)
        {
            process!.StandardInput.WriteLine(stdin);
        }

        process!.StandardInput.Close();

        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        Assert.True(process.WaitForExit(60_000), "--encrypt-password did not exit within 60 seconds.");
        return new RunResult(process.ExitCode, await stdoutTask, await stderrTask);
    }

    [Fact]
    public async Task EncryptPassword_ScriptedSuccess_WritesNothingToStderr_AndOnlyTheBlobToStdout()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var result = await RunEncryptPassword(DummyInput);

        Assert.Equal(0, result.ExitCode);
        Assert.Equal(string.Empty, result.StdErr);

        var lines = result.StdOut.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var blob = Assert.Single(lines);
        Assert.StartsWith("AQAAA", blob, StringComparison.Ordinal);
        Assert.Equal(DummyInput, PerformanceMonitor.Darling.Service.DarlingSecrets.Unprotect(blob));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task EncryptPassword_EmptyRead_StillExitsOne_WithErrorLineAndStdoutGuidance(string? stdin)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var result = await RunEncryptPassword(stdin);

        /* #2097 unchanged: the error line stays on stderr, the guidance rides stdout, and the exit code is 1. */
        Assert.Equal(1, result.ExitCode);
        Assert.Contains("No password read from stdin.", result.StdErr, StringComparison.Ordinal);
        Assert.DoesNotContain("Password: ", result.StdErr, StringComparison.Ordinal);
        Assert.Contains("No interactive input received.", result.StdOut, StringComparison.Ordinal);
    }

    /// <summary>
    /// The failing shape from the issue, run under <c>powershell.exe</c> (Windows PowerShell 5.1): the
    /// password piped in, the call captured with <c>2&gt;&amp;1</c>, <c>$ErrorActionPreference = 'Stop'</c>.
    /// Any stderr line becomes a terminating error, so the script never reaches its last line.
    /// </summary>
    [Fact]
    public async Task EncryptPassword_UnderWindowsPowerShell51_WithStopAndMergedStreams_Succeeds()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var exe = ServiceExePath();
        Assert.True(File.Exists(exe), $"The service build output was not found at {exe}.");

        var script = new StringBuilder();
        script.AppendLine("$ErrorActionPreference = 'Stop'");
        script.AppendLine($"$blob = '{DummyInput}' | & '{exe}' --encrypt-password 2>&1");
        script.AppendLine("if ($LASTEXITCODE -ne 0) { throw \"exit code $LASTEXITCODE\" }");
        script.AppendLine("Write-Output \"BLOB=$blob\"");

        var path = Path.Combine(Path.GetTempPath(), $"darling-5413-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script.ToString());
        try
        {
            using var process = Process.Start(new ProcessStartInfo("powershell.exe", $"-NoProfile -ExecutionPolicy Bypass -File \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
            Assert.NotNull(process);

            var stdoutTask = process!.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();
            Assert.True(process.WaitForExit(120_000), "powershell.exe did not exit within 120 seconds.");
            var stdout = await stdoutTask;
            var stderr = await stderrTask;

            Assert.True(string.IsNullOrWhiteSpace(stderr), $"The script stopped on an error record:\n{stderr}");
            Assert.Equal(0, process.ExitCode);
            Assert.Contains("BLOB=AQAAA", stdout, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
