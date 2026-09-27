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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4466: <c>Get-Service</c> reporting <c>Stopped</c> is the SCM's own state machine reaching that state,
/// not proof that every process the service spawned has actually finished exiting - a postmaster, or
/// another child, can still be a moment from unwinding when phase two of <c>upgrade-darling.ps1</c>'s stop
/// guard takes its snapshot of what is still holding the install tree. Reported live: an upgrade refused
/// with "the service is stopped, but processes are still running out of the install tree" against a
/// process that was already on its way out, costing a re-run that changed nothing.
///
/// <para><b>The fix.</b> <c>Wait-DarlingInstallTreeClear</c> polls
/// <c>Get-DarlingProcessesUnderPath</c> every ~2 seconds until it comes back empty or a bound (120 seconds
/// by default) runs out, between "Service is stopped." and phase two's refusal. On empty, phase two
/// proceeds exactly as before; on expiry, phase two refuses with the SAME message it always has -
/// <c>-SkipStopGuard</c> skips the wait along with the refusal, unchanged.</para>
///
/// <para><b>Why these tests are split the way they are.</b> The ORDERING - the wait sits between "Service
/// is stopped." and the refusal, and is skipped entirely under <c>-SkipStopGuard</c> - and the UNCHANGED
/// refusal text are pinned structurally, the same way <c>DarlingDeployRollbackRetentionTests</c> pins this
/// script's other invariants it cannot compile. Whether the wait actually WAITS is not visible to any
/// source-parsing assertion, so <c>Wait-DarlingInstallTreeClear</c> is extracted and run under
/// <c>pwsh</c> against a real child process, the same idiom <c>DarlingInstallLocationTests</c> and
/// <c>DarlingDeployRollbackRetentionTests</c> use to run this script's other extracted functions - except
/// this script targets Windows PowerShell 5.1 for those, and this fix has nothing PowerShell-5.1-specific
/// in it, so it runs under the <c>pwsh</c> installed on this Mac instead. It is skipped cleanly where
/// <c>pwsh</c> is not on PATH.</para>
/// </summary>
public sealed class DarlingUpgradeInstallTreeWaitTests
{
    private static string DeployScript => ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));

    private static string? PwshPath =>
        File.Exists("/opt/homebrew/bin/pwsh") ? "/opt/homebrew/bin/pwsh"
        : File.Exists("/usr/local/bin/pwsh") ? "/usr/local/bin/pwsh"
        : null;

    /// <summary>
    /// The wait sits AFTER "Service is stopped." and BEFORE phase two's refusal, and the refusal's own
    /// message text - "Do NOT kill anything under...pg-runtime" among it - is unchanged. Getting the
    /// ordering backwards (waiting before the service is even confirmed stopped, or refusing before the
    /// wait gets a chance to run) reproduces the exact defect #4466 reports.
    /// </summary>
    [Fact]
    public void TheDeployScript_WaitsAfterTheServiceStops_AndBeforeThePhaseTwoRefusal_WithTheRefusalTextUnchanged()
    {
        var script = DeployScript;

        var stoppedGood = script.IndexOf("Good \"Service is stopped.\"", StringComparison.Ordinal);
        var waitCall = script.IndexOf("Wait-DarlingInstallTreeClear $InstallRoot", StringComparison.Ordinal);
        var stillHolding = script.IndexOf("$stillHolding = Get-DarlingProcessesUnderPath $InstallRoot", StringComparison.Ordinal);
        var refusal = script.IndexOf(
            "Fail \"Nothing has been copied, so the install is intact - but the service is now STOPPED. " +
            "Either close these and re-run (safe, and it will reuse the backup it is about to take), or " +
            "abandon the upgrade with: Start-Service '$serviceName'. Do NOT kill anything under " +
            "$InstallRoot\\pg-runtime - that is the bundled PostgreSQL and killing it takes the store down; " +
            "give a postmaster that outlived the stop a few seconds and re-run.\"",
            StringComparison.Ordinal);

        Assert.True(stoppedGood >= 0, "upgrade-darling.ps1 no longer reports 'Service is stopped.' (#4466)");
        Assert.True(waitCall >= 0, "upgrade-darling.ps1 no longer calls Wait-DarlingInstallTreeClear before phase two (#4466)");
        Assert.True(stillHolding >= 0, "upgrade-darling.ps1 no longer re-checks for holders after the stop (#2525/#4466)");
        Assert.True(refusal >= 0, "upgrade-darling.ps1's phase-two refusal message changed - the 'Do NOT kill' wording must stay as it ships (#4466)");

        Assert.True(stoppedGood < waitCall, "the wait must start AFTER 'Service is stopped.' is reported, not before");
        Assert.True(waitCall < stillHolding, "the wait must run BEFORE the unfiltered holder snapshot phase two acts on, or it waits on stale data");
        Assert.True(stillHolding < refusal, "the holder snapshot must be taken before the refusal that reads it");

        /* -SkipStopGuard skips the wait along with the refusal it exists to avoid - unchanged behavior.
           Looked for in the text BETWEEN 'Service is stopped.' and the call itself: the wait is guarded by
           an `if (-not $SkipStopGuard) { ... }` wrapper that opens a few lines above the call, not on the
           line immediately before it. */
        var guardBlock = script.Substring(stoppedGood, waitCall - stoppedGood);
        Assert.Contains("$SkipStopGuard", guardBlock, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>Wait-DarlingInstallTreeClear</c> is extracted and run against a real child process copied under a
    /// temp "install root", under <c>pwsh</c>. With a generous bound it waits for the process to exit and
    /// then returns empty; with a bound shorter than the process's lifetime it returns that process instead
    /// - the exact split between "wait, then proceed" and "wait, then refuse" phase two now depends on.
    /// </summary>
    [Fact]
    public void WaitDarlingInstallTreeClear_ReturnsEmptyAfterTheProcessExits_ButReturnsItWhenTheBoundIsTooShort()
    {
        var pwsh = PwshPath;
        Assert.SkipUnless(pwsh is not null, "pwsh is not installed on this machine.");

        var root = Directory.CreateTempSubdirectory("darling-4466-wait-");
        /* On macOS /tmp is a symlink to /private/tmp: Get-DarlingProcessesUnderPath resolves ITS root with
           GetFullPath (which does not follow symlinks), while a started process's own .Path comes back
           already fully resolved by the OS - so a root string that keeps the /tmp spelling can never match
           what the process reports and the wait would see a false empty. Windows has no such symlink in
           the temp path this ever runs against; this is purely a Mac-test-harness wrinkle, not anything
           about the product code. */
        var resolvedRoot = ResolveRealPath(root.FullName);
        try
        {
            var sleeperSource = Path.Combine(root.FullName, "sleeper.c");
            var sleeperExe = Path.Combine(root.FullName, "sleeper");
            File.WriteAllText(sleeperSource,
                "#include <stdlib.h>\n#include <unistd.h>\n" +
                "int main(int argc, char **argv) { sleep(argc > 1 ? atoi(argv[1]) : 1); return 0; }\n");

            Compile(sleeperSource, sleeperExe);

            /* Bound (10s) generous next to the process's own lifetime (2s): it must wait, then clear. */
            using (var proc = StartUnder(sleeperExe, "2"))
            {
                var cleared = RunWait(pwsh!, resolvedRoot, waitSeconds: 10, pollSeconds: 1);
                Assert.Empty(cleared);
                proc.WaitForExit(5_000);
            }

            /* Bound (1s) shorter than the process's own lifetime (30s): it must refuse to wait forever and
               return the still-running process instead. Killed in a finally so the test leaves nothing
               behind even if an assertion above it fails first. */
            Process? holder = null;
            try
            {
                holder = StartUnder(sleeperExe, "30");
                var stillHolding = RunWait(pwsh!, resolvedRoot, waitSeconds: 1, pollSeconds: 1);
                Assert.NotEmpty(stillHolding);
                Assert.Contains(stillHolding, name => name.Contains("sleeper", StringComparison.OrdinalIgnoreCase));
            }
            finally
            {
                if (holder is { HasExited: false })
                {
                    holder.Kill();
                    holder.WaitForExit(5_000);
                }
                holder?.Dispose();
            }
        }
        finally
        {
            try { Directory.Delete(root.FullName, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>Resolves every symlink in <paramref name="path"/>'s ancestry, so this test's temp root
    /// matches what a started process's own <c>.Path</c> reports on macOS - see the comment where this is
    /// called for why that matters. A no-op on a path with nothing to resolve, which covers Windows.</summary>
    private static string ResolveRealPath(string path)
    {
        if (!OperatingSystem.IsMacOS() && !OperatingSystem.IsLinux()) { return path; }

        using var readlink = Process.Start(new ProcessStartInfo("readlink", $"-f \"{path}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        });
        if (readlink is null) { return path; }

        var resolved = readlink.StandardOutput.ReadToEnd().Trim();
        readlink.WaitForExit(5_000);
        return readlink.ExitCode == 0 && resolved.Length > 0 ? resolved : path;
    }

    /// <summary>Compiles a tiny sleeper with the system C compiler - the same one <c>clang</c>/<c>cc</c>
    /// every macOS dev box ships, so a copied signed binary (which macOS refuses to run from a new path)
    /// never enters the picture.</summary>
    private static void Compile(string source, string outputExe)
    {
        using var cc = Process.Start(new ProcessStartInfo("cc", $"-O0 -o \"{outputExe}\" \"{source}\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        });
        Assert.NotNull(cc);
        var stderr = cc!.StandardError.ReadToEnd();
        cc.WaitForExit(30_000);
        Assert.True(cc.ExitCode == 0, $"could not compile the test's sleeper helper: {stderr}");
    }

    private static Process StartUnder(string exePath, string sleepSeconds)
    {
        var psi = new ProcessStartInfo(exePath, sleepSeconds)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        var proc = Process.Start(psi);
        Assert.NotNull(proc);
        return proc!;
    }

    /// <summary>Runs the REAL, extracted <c>Wait-DarlingInstallTreeClear</c> under <c>pwsh</c> against a
    /// real root, and returns the process names it reports still holding it - empty when it cleared.
    ///
    /// <para>The holder-check it calls, <c>Get-DarlingProcessesUnderPath</c>, builds its match prefix with
    /// a hardcoded backslash (<c>[IO.Path]::GetFullPath($root).TrimEnd('\') + '\'</c>) - correct for the
    /// Windows paths it only ever runs against in production, but it can never match a macOS process's
    /// forward-slash <c>.Path</c>, so it is not what is under test here. A portable stand-in with the same
    /// shape (full-path prefix match, skip processes whose <c>.Path</c> throws or is empty) is substituted
    /// for it - defined after the real extraction, so it is the one <c>Wait-DarlingInstallTreeClear</c>
    /// actually calls. That helper's own Windows behavior is exercised by
    /// <c>DarlingDeployRollbackRetentionTests</c> against real Windows PowerShell; what is exercised here is
    /// the real fix - the poll loop and the bound - which has nothing OS-specific in it.</para></summary>
    private static string[] RunWait(string pwsh, string root, int waitSeconds, int pollSeconds)
    {
        var script = new StringBuilder();
        script.AppendLine("function Get-DarlingProcessesUnderPath([string]$root) {");
        script.AppendLine("    $hits = @()");
        script.AppendLine("    if ([string]::IsNullOrWhiteSpace($root)) { return @($hits) }");
        script.AppendLine("    $prefix = [IO.Path]::GetFullPath($root).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar");
        script.AppendLine("    foreach ($process in @(Get-Process -ErrorAction SilentlyContinue)) {");
        script.AppendLine("        $path = $null");
        script.AppendLine("        try { $path = $process.Path } catch { continue }");
        script.AppendLine("        if ([string]::IsNullOrEmpty($path)) { continue }");
        script.AppendLine("        if ($path.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) { $hits += $process }");
        script.AppendLine("    }");
        script.AppendLine("    return @($hits)");
        script.AppendLine("}");
        script.AppendLine(ExtractFunction(DeployScript, "Note"));
        script.AppendLine(ExtractFunction(DeployScript, "Wait-DarlingInstallTreeClear"));
        script.AppendLine($"$result = @(Wait-DarlingInstallTreeClear '{root}' {waitSeconds} {pollSeconds})");
        /* A marker line, since Write-Host (what Note uses) lands on stdout, not just the host, once pwsh's
           stdout is redirected rather than connected to a terminal - so the "Still exiting..." line the
           wait itself prints would otherwise land in this test's parsed result right alongside the actual
           answer. */
        script.AppendLine("'---RESULT---'");
        script.AppendLine("$result | ForEach-Object { $_.ProcessName }");

        var path = Path.Combine(Path.GetTempPath(), $"darling-4466-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script.ToString());
        try
        {
            using var process = Process.Start(new ProcessStartInfo(pwsh, $"-NoProfile -File \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            });
            Assert.NotNull(process);

            var stdoutTask = process!.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();
            /* Waits comfortably longer than either bound this test drives it with, so the process's own
               poll loop is what decides the outcome, not a race against this timeout. */
            Assert.True(process.WaitForExit(60_000), "pwsh did not exit within 60s running the extracted wait function.");

            var stdout = stdoutTask.GetAwaiter().GetResult();
            var stderr = stderrTask.GetAwaiter().GetResult();
            Assert.True(string.IsNullOrWhiteSpace(stderr), $"pwsh reported an error running Wait-DarlingInstallTreeClear:\n{stderr}");

            var lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var markerIndex = Array.IndexOf(lines, "---RESULT---");
            Assert.True(markerIndex >= 0, $"pwsh's output did not contain the result marker:\n{stdout}");
            return lines[(markerIndex + 1)..];
        }
        finally
        {
            try { File.Delete(path); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>Returns the body of the function named <paramref name="name"/> out of <paramref
    /// name="script"/>, by brace matching - the same idiom <c>DarlingInstallLocationTests</c> uses to lift
    /// a function out of the sibling installer script.</summary>
    private static string ExtractFunction(string script, string name)
    {
        var start = script.IndexOf("function " + name, StringComparison.Ordinal);
        Assert.True(start >= 0, $"upgrade-darling.ps1 no longer defines {name} (#4466)");

        var open = script.IndexOf('{', start);
        Assert.True(open >= 0, $"expected an opening brace after 'function {name}'");

        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{') { depth++; }
            else if (script[i] == '}')
            {
                depth--;
                if (depth == 0) { return script.Substring(start, i - start + 1); }
            }
        }

        Assert.Fail($"unbalanced braces while extracting {name} from upgrade-darling.ps1");
        return string.Empty;
    }
}
