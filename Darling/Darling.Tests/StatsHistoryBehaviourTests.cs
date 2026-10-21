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
/// The History button on the web Top Queries and Top Procedures grids (#6303), from the shipped <c>stats-history.js</c> run under Node
/// (<c>web-stats-history-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class StatsHistoryBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-stats-history-harness.mjs"));
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
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the stats history harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the stats history harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void AProcedureRow_ReadsThatProceduresTrend_AndDrawsOneMetricAtATimeWithoutLogicalReads()
    {
        var r = Run("procOpen");
        Assert.Equal("/api/read/get_procedure_duration_trend", r.GetProperty("path").GetString());
        var q = r.GetProperty("query");
        Assert.Equal("srv-a", q.GetProperty("server").GetString());
        Assert.Equal("Orders", q.GetProperty("database_name").GetString());
        Assert.Equal("dbo.usp_Ship", q.GetProperty("procedure_name").GetString());
        Assert.Equal("24", q.GetProperty("hours").GetString());
        Assert.Equal(1, r.GetProperty("fetches").GetInt32());
        Assert.Equal(new[] { "Avg Duration (ms)", "Avg CPU (ms)", "Duration (ms/s)", "Executions (per second)" },
            r.GetProperty("options").EnumerateArray().Select(o => o.GetString()));
        Assert.DoesNotContain("reads", r.GetProperty("options").GetRawText(), System.StringComparison.OrdinalIgnoreCase);

        var chart = r.GetProperty("chart")[0];
        Assert.Equal(new[] { "avg_elapsed_ms" }, chart.GetProperty("series").EnumerateArray().Select(s => s.GetString()));
        var points = chart.GetProperty("points").EnumerateArray().ToArray();
        // 200 ms/s over 4 executions/s is 50 ms an execution; an unrated point and a point with no executions have no average, and a
        // gap is not a zero.
        Assert.Equal(50, points[0].GetProperty("avg_elapsed_ms").GetDouble());
        Assert.Equal(JsonValueKind.Null, points[1].GetProperty("avg_elapsed_ms").ValueKind);
        Assert.Equal(JsonValueKind.Null, points[2].GetProperty("avg_elapsed_ms").ValueKind);
        // The read's avg_cpu_ms is drawn as it came; a point without it (no executions) is a gap, not a 0.
        Assert.Equal(12.5, points[0].GetProperty("avg_cpu_ms").GetDouble());
        Assert.Equal(JsonValueKind.Null, points[1].GetProperty("avg_cpu_ms").ValueKind);
        Assert.Equal(JsonValueKind.Null, points[2].GetProperty("avg_cpu_ms").ValueKind);

        // Picking another metric redraws from the data already read, with no second read.
        Assert.Equal(new[] { "executions_per_second" }, r.GetProperty("afterPick").GetProperty("series").EnumerateArray().Select(s => s.GetString()));
        Assert.Equal(1, r.GetProperty("fetchesAfterPick").GetInt32());
        Assert.True(r.GetProperty("closed").GetBoolean());
    }

    [Fact]
    public void AQueryRow_ReadsThatQuerysTrend_ByHashAndDatabase()
    {
        var r = Run("queryOpen");
        Assert.Equal("/api/read/get_query_trend", r.GetProperty("path").GetString());
        var q = r.GetProperty("query");
        Assert.Equal("0xABC", q.GetProperty("query_hash").GetString());
        Assert.Equal("Orders", q.GetProperty("database_name").GetString());
        Assert.Equal(new[] { "avg_cpu_ms", "avg_elapsed_ms" }, r.GetProperty("chart")[0].GetProperty("series").EnumerateArray().Select(s => s.GetString()));
        Assert.Equal(0, r.GetProperty("selects").GetInt32());
    }

    [Fact]
    public void AnOpenPanel_SurvivesARebuild_AndAChangedWindowReadsAgain()
    {
        var r = Run("rebuild");
        Assert.True(r.GetProperty("open").GetBoolean());
        Assert.Equal(0, r.GetProperty("refetched").GetInt32());
        Assert.Equal(1, r.GetProperty("refetchedForNewWindow").GetInt32());
    }

    [Fact]
    public void AnEmptyWindow_ShowsTheReadsOwnWords_AndDrawsNoChart()
    {
        var r = Run("empty");
        Assert.Equal(new[] { "No stored-procedure samples in the window." }, r.GetProperty("empty").EnumerateArray().Select(s => s.GetString()));
        Assert.Equal(0, r.GetProperty("charts").GetInt32());
    }

    [Fact]
    public void TheNotice_SaysTheBucketWidth_AndNeverTheMcpParameterAdvice()
    {
        // #6359: the read's aggregate_note says "pass bucket_minutes (1-1440) for another width, or narrow hours_back" (a browser
        // reader can do neither). The panel states the width from the read's bucket fields instead.
        var r = Run("mcpAdviceNote");
        Assert.Equal(new[] { "Each point covers 1 hour." }, r.GetProperty("notices").EnumerateArray().Select(s => s.GetString()));
        Assert.Equal(new[] { "Each point is one collection." }, r.GetProperty("queryNotices").EnumerateArray().Select(s => s.GetString()));
        Assert.Equal(
            new string?[] { "covers 1 minute", "covers 10 minutes", "covers 1 hour", "covers 6 hours", "covers 1 day", "is one collection", null },
            r.GetProperty("widths").EnumerateArray().Select(s => s.GetString()));
    }

    [Fact]
    public void AtPast5Minutes_HistoryReadsTheWindowTheGridRankedOver_AndSaysItIsWider()
    {
        // #6359: the grid ranks over the whole hour; the row's History used to be cut to the 5 picked minutes and said nothing was recorded.
        var r = Run("fiveMinuteRange");
        Assert.Equal("1", r.GetProperty("query").GetProperty("hours").GetString());
        Assert.Equal("2026-03-04T08:00:00Z", r.GetProperty("query").GetProperty("as_of").GetString());
        Assert.Empty(r.GetProperty("empty").EnumerateArray());
        Assert.Equal(2, r.GetProperty("points").GetInt32());
        var notice = Assert.Single(r.GetProperty("notices").EnumerateArray()).GetString()!;
        Assert.Contains("the window the grid ranked over", notice);
        Assert.Contains("wider than the range you picked", notice);
        // Nothing to say when the window is the picked one (a range of whole hours, or a preset).
        Assert.Equal(JsonValueKind.Null, r.GetProperty("wholeHourNote").ValueKind);
        Assert.Equal(JsonValueKind.Null, r.GetProperty("presetNote").ValueKind);
    }

    [Fact]
    public void ARowWithoutTheKeysTheReadNeeds_GetsADash()
    {
        var r = Run("noKey");
        Assert.Equal("—", r.GetProperty("noName").GetString());
        Assert.Equal("—", r.GetProperty("noDb").GetString());
        Assert.Equal("—", r.GetProperty("noHash").GetString());
    }
}
