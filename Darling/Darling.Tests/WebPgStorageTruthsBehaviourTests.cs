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
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7551: the Storage tab does not state a missing estimate as 0 B, and a cut grid says how many rows it lists out of how many the window held.
/// These run the shipped <c>pg-storage.js</c> and <c>panels.js</c> under Node (<c>web-pg-storage-harness.mjs</c>); skipped when Node is not installed.
/// </summary>
public sealed class WebPgStorageTruthsBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-pg-storage-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped panel script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the storage harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the storage harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ATableReadWithNoPublishedEstimate_ShowsTheStateAndNotZeroBytes()
    {
        var tiles = Run().GetProperty("noEstimate").GetProperty("tiles");
        Assert.Equal("No estimate published", tiles.GetProperty("Est. bloat (trusted rows)").GetString());
        Assert.Equal("140", tiles.GetProperty("Estimates suppressed").GetString());
    }

    [Fact]
    public void ATableReadWhoseEstimatesSumToZero_StillShowsZeroBytes()
    {
        Assert.Equal("0 B", Run().GetProperty("realZero").GetProperty("tiles").GetProperty("Est. bloat (trusted rows)").GetString());
    }

    [Fact]
    public void ACutGrid_SaysHowManyRowsItListsOutOfTheWindowsTotal()
    {
        var r = Run();
        Assert.Contains("lists 25 of the 140 tables the window held", r.GetProperty("noEstimate").GetProperty("notices")[0].GetString()!, StringComparison.Ordinal);
        Assert.Contains("lists 25 of the 312 indexes the window held", r.GetProperty("usageCut").GetProperty("notices")[0].GetString()!, StringComparison.Ordinal);
        Assert.Contains("The window held more indexes than the grid below lists", r.GetProperty("usageCutNoCount").GetProperty("notices")[0].GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEstimatedIndexRow_ShowsItsEstimateAndItsBasis_NextToTheReclaimableFigure()
    {
        var b = Run().GetProperty("indexBloat");
        Assert.True(b.GetProperty("hasEstPct").GetBoolean(), "the grid has no Est. Bloat % column, so a 0 B reclaimable figure has nothing beside it");
        Assert.Equal("Estimated", b.GetProperty("basisEstimated").GetString());
        Assert.Equal("Measured", b.GetProperty("basisMeasured").GetString());
        Assert.Equal("—", b.GetProperty("basisNone").GetString());
    }
}
