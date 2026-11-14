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
/// #8787: the Store Health page on the Darling web. These run the shipped <c>store-health.js</c> with <c>panels.js</c> and <c>util.js</c> under Node
/// (<c>store-health-harness.mjs</c>) against a scripted read; skipped when Node is not installed.
/// </summary>
public sealed class StoreHealthPageBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "store-health-harness.mjs"));
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
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the store health harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the store health harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[][] Rows(JsonElement rows) =>
        rows.EnumerateArray().Select(r => r.EnumerateArray().Select(c => c.GetString()!).ToArray()).ToArray();

    private static string[] Names(JsonElement asks, string noSection) =>
        asks.EnumerateArray().Select(a => a.GetProperty("section").ValueKind == JsonValueKind.Null ? noSection : a.GetProperty("section").GetString()!).ToArray();

    [Fact]
    public void ThePage_ReadsTablesAndIndexesAsTwoReads_AndNeverAsksForAll()
    {
        var r = Result.Value;
        Assert.Equal(new[] { "tables", "indexes", "readiness" }, Names(r.GetProperty("first").GetProperty("asks"), "readiness"));
        Assert.Equal(0, r.GetProperty("neverAll").GetInt32());
        Assert.Equal(new[] { "tables", "indexes", "readiness" }, Names(r.GetProperty("refresh").GetProperty("asks"), "readiness"));
    }

    [Fact]
    public void TheTablesGrid_DrawsBeforeTheIndexesReadReturns_AndTheIndexesSectionSaysLoading()
    {
        var first = Result.Value.GetProperty("first");
        Assert.Equal(4, Rows(first.GetProperty("tableRows")).Length);
        Assert.Contains("Loading indexes", first.GetProperty("indexesWhileHeld").GetString()!, StringComparison.Ordinal);
        Assert.Contains("Loading partition index coverage", first.GetProperty("coverageWhileHeld").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void ATableWithNoEstimate_ShowsTheReasonPhrase_AndNeverZero()
    {
        var rows = Rows(Result.Value.GetProperty("first").GetProperty("tableRows"));
        var fresh = rows.Single(r => r[0] == "collect.fresh_table");
        Assert.Equal("Never analyzed", fresh[7]);
        Assert.Equal("—", fresh[8]);
        var candidate = rows.Single(r => r[0] == "collect.store_metrics");
        Assert.Equal("Rewrite candidate", candidate[2]);
        Assert.Equal("75%", candidate[7]);
    }

    [Fact]
    public void TheHeader_ShowsTheCountsTheReasonsAndTheReadinessLine_AndTheNoteStatesItIsAnEstimate()
    {
        var first = Result.Value.GetProperty("first");
        var tiles = first.GetProperty("tiles").EnumerateArray().Select(t => t.GetString()!).ToArray();
        Assert.Contains(tiles, t => t.Contains("Rewrite candidate", StringComparison.Ordinal) && !t.Contains("Rewrite candidates", StringComparison.Ordinal) && t.StartsWith("1", StringComparison.Ordinal));
        Assert.Contains("Never analyzed: 1", first.GetProperty("headerText").GetString()!, StringComparison.Ordinal);
        Assert.Contains("Not safe for maintenance now.", first.GetProperty("readiness").GetString()!, StringComparison.Ordinal);
        Assert.Contains("estimate", first.GetProperty("note")[0].GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIndexesGrid_ListsFlaggedIndexesFirst_AndCoverageShowsOnlyIncompleteParents()
    {
        var after = Result.Value.GetProperty("afterIndexes");
        var rows = Rows(after.GetProperty("indexRows"));
        Assert.Equal("Invalid", rows[0][0]);
        Assert.Equal("Larger than its table", rows[1][0]);
        Assert.Contains(rows, r => r.Any(c => c.Contains("Table not analyzed", StringComparison.Ordinal)));
        Assert.DoesNotContain(rows, r => r.Any(c => c.Contains("reltuples", StringComparison.Ordinal)));
        var coverage = Rows(after.GetProperty("coverageRows"));
        Assert.Single(coverage);
        Assert.Equal("collect.parts_2", coverage[0][6]);
    }

    [Fact]
    public void WhenEveryPartitionHasEveryIndex_TheCoverageSectionIsOneLine()
    {
        var clean = Result.Value.GetProperty("cleanCoverage");
        Assert.Equal(0, clean.GetProperty("headings").GetInt32());
        Assert.Contains("Every partition", clean.GetProperty("text").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public void Measure_AsksForTheRowsTable_LocksEveryButtonWhileItRuns_AndShowsTheResult()
    {
        var m = Result.Value.GetProperty("measure");
        var asks = m.GetProperty("asks").EnumerateArray().ToArray();
        Assert.Single(asks);
        Assert.Equal("collect.store_metrics", asks[0].GetProperty("table").GetString());
        Assert.Contains("has run for", m.GetProperty("panelWhileRunning").GetString()!, StringComparison.Ordinal);
        Assert.All(m.GetProperty("disabledWhileRunning").EnumerateArray(), b => Assert.True(b.GetBoolean()));
        Assert.All(m.GetProperty("disabledAfter").EnumerateArray(), b => Assert.False(b.GetBoolean()));
        var panel = m.GetProperty("panelText").GetString()!;
        Assert.Contains("700 of 1,000", panel, StringComparison.Ordinal);
        Assert.Contains("Can VACUUM shrink the file?", panel, StringComparison.Ordinal);
        Assert.Contains("used by live rows", panel, StringComparison.Ordinal);
        Assert.Equal(4, m.GetProperty("histogramRows").GetArrayLength());
    }
}
