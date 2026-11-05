/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7688: on SQL Server on Linux the Scheduler Issues read leaves Other-Process CPU and Idle out (idle reads 0 there, so the figures were
/// 100 minus the SQL figure on every minute) and answers <c>system_cpu_note</c> with the reason. The System Events tab's Scheduler Issues
/// card shows that note above the grid the way the system_health card does, and the two cleared columns draw the page's null mark, not a
/// 100 or a 0. The shipped page runs under Node (<c>scheduler-issues-note-harness.mjs</c>); Node is skipped when it is not installed.
/// </summary>
public sealed class SchedulerIssuesNoteBehaviourTests
{
    [Fact]
    public async Task TheSchedulerIssuesCard_ShowsTheHostCpuNote_AndNoFigureInTheClearedColumns()
    {
        var result = await RunHarnessAsync();

        Assert.True(result.GetProperty("found").GetBoolean());
        Assert.Equal([SystemHealthHostCpu.SchedulerLinuxNote], result.GetProperty("notices").EnumerateArray().Select(n => n.GetString()).ToArray());
        Assert.Equal("94", result.GetProperty("sqlCpu").GetString());
        Assert.Equal("—", result.GetProperty("otherProcess").GetString());
        Assert.Equal("—", result.GetProperty("idle").GetString());
    }

    private static async Task<JsonElement> RunHarnessAsync()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.Environment["HARNESS_INPUT"] = JsonSerializer.Serialize(new { note = SystemHealthHostCpu.SchedulerLinuxNote });
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "scheduler-issues-note-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = (await proc.StandardOutput.ReadToEndAsync()).Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the Scheduler Issues note harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the Scheduler Issues note harness failed: " + await error);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }
}
