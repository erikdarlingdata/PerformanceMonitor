/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7243: the Agent Alerts "Last fired (server clock)" cell on the web Configuration tab showed the stored ISO stamp. It must show the
/// grids' date-time format, and with no zone shift, because the stamp is the monitored server's own clock. Runs the shipped
/// <c>sql-config.js</c> under Node (<c>web-server-clock-harness.mjs</c>) in a zone five hours behind UTC. Skipped when Node is not installed.
/// </summary>
public sealed class WebServerClockColumnBehaviourTests
{
    [Fact]
    public async System.Threading.Tasks.Task TheLastFiredCell_ShowsTheServerClockInTheDisplayFormat_WithoutAZoneShift_AndSortsByTheStamp()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-server-clock-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.Environment["TZ"] = "America/New_York";

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped column script cannot be run.");
            return;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the server clock harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the server clock harness failed: " + await error);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            var r = doc.RootElement;
            Assert.Equal("12/7/2025, 1:32:48 PM", r.GetProperty("shown").GetString());
            Assert.Equal("—", r.GetProperty("empty").GetString());
            Assert.True(r.GetProperty("sortOrdered").GetBoolean());
        }
    }
}
