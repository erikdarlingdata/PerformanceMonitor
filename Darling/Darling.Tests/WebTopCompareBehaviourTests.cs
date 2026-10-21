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
/// #6300: the web Compare picker on the Top Queries, Top Procedures and Query Store grids. Runs the shipped
/// <c>pages/top-compare.js</c> under Node (skipped without Node): how each choice shifts the window, how rows match, the deltas,
/// NEW and GONE, the sort, and that an earlier window with no data or a failed read draws the plain grid, never every row NEW.
/// </summary>
public sealed class WebTopCompareBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "top-compare-harness.mjs"));
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
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the top-compare harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the top-compare harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void EachChoice_ShiftsTheWindowAsTheViewerDoes_AndNamesItAsAsOf()
    {
        var shifts = Run().GetProperty("shifts");

        Assert.Equal(0, shifts.GetProperty("off").GetInt32());
        Assert.Equal(1, shifts.GetProperty("yesterday").GetInt32());
        Assert.Equal(7, shifts.GetProperty("lastweek").GetInt32());
        Assert.Equal(7, shifts.GetProperty("sameday").GetInt32());
        Assert.Equal("2026-10-20T12:00:00.000Z", shifts.GetProperty("yesterdayAsOf").GetString());
        Assert.Equal("2026-10-14T12:00:00.000Z", shifts.GetProperty("lastweekAsOf").GetString());
    }

    [Fact]
    public void Rows_MatchOnDatabaseAndHash_FlagNewAndGone_AndSortNewFirstGoneLast()
    {
        var r = Run();
        var rows = r.GetProperty("queries").EnumerateArray().ToArray();

        // 0xS in database d exists only now (NEW); 0xS in "other" only before (GONE): the database is part of the key.
        Assert.Equal(new[] { "NEW", "NEW", "", "", "GONE", "GONE" }, rows.Select(x => x.GetProperty("status").GetString()).ToArray());
        Assert.Equal(new[] { "select 0xN", "select 0xS" }, rows.Take(2).Select(x => x.GetProperty("name").GetString()).ToArray());
        // Duration delta, biggest first among the rows present in both: +20% before -50%.
        Assert.Equal(new[] { "select 0xA", "select 0xB" }, rows.Skip(2).Take(2).Select(x => x.GetProperty("name").GetString()).ToArray());
        Assert.Equal(20, rows[2].GetProperty("dur").GetDouble(), 6);
        Assert.Equal(-50, rows[2].GetProperty("cpu").GetDouble(), 6);
        Assert.Equal(-50, rows[3].GetProperty("dur").GetDouble(), 6);
        // A NEW row has no baseline, so no delta; it is not coloured.
        Assert.Equal(JsonValueKind.Null, rows[0].GetProperty("dur").ValueKind);
        // Colours follow the Viewer's thresholds: duration and CPU red above +25 and green below -25.
        Assert.Equal(JsonValueKind.Null, rows[2].GetProperty("durSev").ValueKind);
        Assert.Equal("Healthy", rows[2].GetProperty("cpuSev").GetString());
        // No logical reads figure rides on any row (Erik's ruling).
        Assert.True(r.GetProperty("noReads").GetBoolean());
    }

    [Fact]
    public void ManyRowsOfOneKey_BecomeOneRow_WithExecutionWeightedAverages()
    {
        var merged = Run().GetProperty("merged");

        Assert.Equal(1, merged.GetProperty("count").GetInt32());
        Assert.Equal(40, merged.GetProperty("execs").GetInt32());
        Assert.Equal(175, merged.GetProperty("avg").GetDouble(), 6); // (10*100 + 30*200) / 40
        Assert.Equal("", merged.GetProperty("status").GetString());
    }

    [Fact]
    public void Procedures_MatchOnName_AndQueryStore_OnHashAcrossPlans()
    {
        var r = Run();
        var procs = r.GetProperty("procedures").EnumerateArray().ToArray();
        var qs = r.GetProperty("querystore").EnumerateArray().ToArray();

        Assert.Equal(new[] { "dbo.p", "dbo.gone" }, procs.Select(x => x.GetProperty("name").GetString()).ToArray());
        Assert.Equal("GONE", procs[1].GetProperty("status").GetString());
        Assert.Equal(100, procs[0].GetProperty("dur").GetDouble(), 6);
        // Query Store: the same hash under a different plan id is the same row, not one NEW and one GONE.
        Assert.Single(qs);
        Assert.Equal("", qs[0].GetProperty("status").GetString());
        Assert.Equal(50, qs[0].GetProperty("dur").GetDouble(), 6);
    }

    [Fact]
    public void AnEarlierWindowWithNoData_OrAFailedRead_DrawsThePlainGrid_NeverEveryRowNew()
    {
        var plans = Run().GetProperty("plans");

        Assert.Equal("delta", plans.GetProperty("ok").GetProperty("mode").GetString());
        Assert.Equal(6, plans.GetProperty("ok").GetProperty("rows").GetInt32());
        foreach (var name in new[] { "baselineEmpty", "baselineError", "baselineNoExecutions", "baselineNoRows" })
        {
            var plan = plans.GetProperty(name);
            Assert.Equal("plain", plan.GetProperty("mode").GetString());
            Assert.StartsWith("No data for the comparison window", plan.GetProperty("notice").GetString());
        }

        // #6353: this window being the empty or unread one is said by its own card; the comparison notice would blame the wrong window.
        foreach (var name in new[] { "currentError", "currentEmpty", "bothEmpty" })
        {
            var plan = plans.GetProperty(name);
            Assert.Equal("plain", plan.GetProperty("mode").GetString());
            Assert.Equal(JsonValueKind.Null, plan.GetProperty("notice").ValueKind);
        }
    }

    [Fact]
    public void DeltaText_FollowsTheViewer()
    {
        var f = Run().GetProperty("formats");

        Assert.Equal("+12.3%", f.GetProperty("up").GetString());
        Assert.Equal("-4.0%", f.GetProperty("down").GetString());
        Assert.Equal("—", f.GetProperty("none").GetString());
        Assert.Equal(JsonValueKind.Null, f.GetProperty("zeroBase").ValueKind);
    }
}
