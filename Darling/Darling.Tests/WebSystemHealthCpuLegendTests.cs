/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7593: on SQL Server on Linux the system_health CPU chart says System CPU is not shown, so its legend and the Entries grid do not
/// list it either. The shipped System Events tab runs under Node (<c>system-health-cpu-legend-harness.mjs</c>); Node is skipped when
/// it is not installed.
/// </summary>
public sealed class WebSystemHealthCpuLegendTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "system-health-cpu-legend-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

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
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the system_health CPU legend harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the system_health CPU legend harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void LinuxServer_NoteSaysSystemCpuIsNotShown_LegendAndGridLeaveItOut()
    {
        var run = Run("linux");
        var cpu = run.GetProperty("cpuText").GetString()!;
        Assert.Contains("System CPU is not shown", cpu);
        var legend = run.GetProperty("legend").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(new[] { "SQL CPU %" }, legend);
        var headers = run.GetProperty("headers").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Contains("SQL CPU %", headers);
        Assert.DoesNotContain("System CPU %", headers);
    }

    [Fact]
    public void WindowsServer_NoNote_LegendAndGridKeepSystemCpu()
    {
        var run = Run("windows");
        var legend = run.GetProperty("legend").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(new[] { "SQL CPU %", "System CPU %" }, legend);
        var headers = run.GetProperty("headers").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Contains("System CPU %", headers);
    }
}
