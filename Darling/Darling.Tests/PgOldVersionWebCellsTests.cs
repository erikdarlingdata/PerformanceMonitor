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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6836: a PostgreSQL 10 to 13 target does not report some figures (the statement WAL counters, the sampler's query id)
/// and the store holds NULL for them. This runs the shipped Activity and Waits tabs against answers that carry those nulls
/// and checks what the grids draw: the not-applicable dash for a null, and the real value for a zero, so "not reported"
/// never reads as "none".
/// </summary>
public sealed class PgOldVersionWebCellsTests
{
    private static JsonElement RunHarness()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-pg-old-version-cells-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the old-version cells harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the old-version cells harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string Cell(JsonElement grid, int row, string header) =>
        grid.GetProperty("rows")[row].GetProperty(header).GetString()!;

    [Fact]
    public void TheTopQueriesGrid_DrawsADashForUnreportedWal_AndTheNumberForAReportedOne()
    {
        var grid = RunHarness().GetProperty("topQueries");

        Assert.True(grid.GetProperty("present").GetBoolean());
        Assert.Equal("—", Cell(grid, 0, "WAL (bytes)"));
        Assert.Equal("0", Cell(grid, 1, "WAL (bytes)"));
        /* The digits only: the grouping separator follows the machine's locale. */
        Assert.Equal("2048", new string(Cell(grid, 2, "WAL (bytes)").Where(char.IsDigit).ToArray()));
    }

    [Fact]
    public void TheSampledWaitsGrid_DrawsADashForAnUnreportedQueryId_AndKeepsZeroForNoStatement()
    {
        var grid = RunHarness().GetProperty("sampledWaits");

        Assert.True(grid.GetProperty("present").GetBoolean());
        Assert.Equal("—", Cell(grid, 0, "Query ID"));
        Assert.Equal("0", Cell(grid, 1, "Query ID"));
        Assert.Equal("555", Cell(grid, 2, "Query ID"));
    }
}
