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
/// Three small fixes on the web viewer's SQL Server Overview tiles (#7485): the Daily Summary "Total wait" tile names its unit as the
/// table form does, the Server Properties "Service objective" tile shows only on an Azure SQL Database, and a tile value with no space in
/// it (<c>SOS_SCHEDULER_YIELD</c>) can wrap instead of running past its tile. The first two run the shipped <c>panels.js</c> under Node
/// (<c>web-overview-tiles-harness.mjs</c>), skipped when Node is not installed. Layout needs a browser, so no test here covers the wrap.
/// </summary>
public sealed class WebOverviewTilesBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-overview-tiles-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped tile script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the overview tiles harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the overview tiles harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Tiles(JsonElement root, string name) => root.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void DailySummary_TotalWaitTile_NamesItsUnit()
    {
        var root = Run();
        var tiles = Tiles(root, "daily");
        Assert.Contains("Total wait (s)=" + root.GetProperty("expectedTotal").GetString(), tiles);
        Assert.DoesNotContain(tiles, t => t.StartsWith("Total wait=", StringComparison.Ordinal));
    }

    [Fact]
    public void ServerProperties_ServiceObjectiveTile_ShowsOnlyOnAnAzureSqlDatabase()
    {
        var root = Run();
        Assert.Contains("Service objective=GP_S_Gen5_1", Tiles(root, "azureDb"));
        Assert.Contains("vCores=1", Tiles(root, "azureDb"));
        Assert.DoesNotContain(Tiles(root, "onPrem"), t => t.StartsWith("Service objective=", StringComparison.Ordinal));
        Assert.DoesNotContain(Tiles(root, "onPrem"), t => t.StartsWith("vCores=", StringComparison.Ordinal));
    }
}
