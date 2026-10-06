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
/// The window select on the FinOps Storage Growth tab, run from the shipped page script under Node
/// (<c>finops-storage-growth-window-harness.mjs</c>): the databases level has no select and reads 24 hours; the objects level
/// has a 7/30/90-day select that reads days x 24 hours (720 by default), a pick re-reads, the pick survives the 60 s rebuild for
/// the same server, and another server starts at 30 days. Node is skipped when it is not installed.
/// </summary>
public sealed class FinOpsStorageGrowthWindowBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-storage-growth-window-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the storage growth window harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the storage growth window harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Hours(JsonElement step) => step.GetProperty("reads").EnumerateArray().Select(e => e.GetProperty("hours").GetString()!).ToArray();

    private static string?[] Limits(JsonElement step) => step.GetProperty("reads").EnumerateArray().Select(e => e.GetProperty("limit").GetString()).ToArray();

    [Fact]
    public void TheDatabasesLevelHasNoSelectAndReadsTwentyFourHours()
    {
        var d = Run().GetProperty("databases");
        Assert.Equal(new[] { "24" }, Hours(d));
        Assert.Equal(0, d.GetProperty("selects").GetInt32());
    }

    [Fact]
    public void TheDatabasesLevelAsksForFiveHundredDatabases_AndTheObjectsLevelForTwenty()
    {
        var run = Run();
        // #5238: the databases list is the desktop's every-database grid, so the tab asks for the most the service takes at this level.
        Assert.Equal(new[] { "500" }, Limits(run.GetProperty("databases")));
        Assert.Equal(new[] { "500" }, Limits(run.GetProperty("other")));
        Assert.Equal(new[] { "20" }, Limits(run.GetProperty("objects")));
    }

    [Fact]
    public void TheObjectsLevelDefaultsToThirtyDays_AndEachPickSendsDaysTimes24()
    {
        var run = Run();
        var objects = run.GetProperty("objects");
        Assert.Equal(new[] { "720" }, Hours(objects));
        Assert.Equal("7,30,90", string.Join(",", objects.GetProperty("options").EnumerateArray().Select(e => e.GetString())));
        Assert.Equal("30", objects.GetProperty("value").GetString());

        Assert.Equal(new[] { "2160" }, Hours(run.GetProperty("picked90")));
        Assert.Equal("90", run.GetProperty("picked90").GetProperty("value").GetString());
        Assert.Equal(new[] { "168" }, Hours(run.GetProperty("picked7")));
        Assert.Equal("7", run.GetProperty("picked7").GetProperty("value").GetString());
    }

    [Fact]
    public void ThePickSurvivesARebuildForTheSameServer_AndAnotherServerStartsAtThirtyDays()
    {
        var run = Run();
        var rebuilt = run.GetProperty("rebuilt");
        Assert.Equal(new[] { "168" }, Hours(rebuilt));
        Assert.Equal("7", rebuilt.GetProperty("value").GetString());

        Assert.Equal(new[] { "24" }, Hours(run.GetProperty("other")));
        var otherObjects = run.GetProperty("otherObjects");
        Assert.Equal(new[] { "720" }, Hours(otherObjects));
        Assert.Equal("30", otherObjects.GetProperty("value").GetString());
    }
}
