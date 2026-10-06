/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5244: the desktop viewer's blocking charts name the collector that answered, the same way Lite's do (<c>BlockingSourceLabel</c> is
/// shared). The trend and severity reads tag each point with the arm that answered, and the Trends tab's Blocking Incidents chart and
/// the Blocking Stats tab's two duration charts put it on the axis only when they draw rows. The Blocking Severity read also follows
/// the database filter now, like the Trends tab's count: the SQL binds <c>$5</c> on both arms, and the tab passes the chosen names to
/// the read and to its data-start probe.
/// </summary>
public sealed class ViewerBlockingSourceLabelTests
{
    private static readonly DateTime T0 = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static string Tab => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Blocking.cs").ReplaceLineEndings("\n");

    private static List<BlockingTrendPoint> Trend(string? source) => new() { new BlockingTrendPoint(T0, 2, source), new BlockingTrendPoint(T0.AddMinutes(1), 1, source) };

    private static List<BlockingDurationStatsPoint> Stats(string? source) => new() { new BlockingDurationStatsPoint(T0, 2, 500, 300, 250, source) };

    [Fact]
    public void TrendChart_XeOnly_DmvOnly_AndEmpty()
    {
        Assert.Equal("Blocking Incidents (blocked-process-report)", BlockingSourceLabel.For("Blocking Incidents", Trend("blocked-process-report").Select(d => d.Source)));
        Assert.Equal("Blocking Incidents (DMV snapshot)", BlockingSourceLabel.For("Blocking Incidents", Trend("DMV snapshot").Select(d => d.Source)));
        Assert.Equal("Blocking Incidents", BlockingSourceLabel.For("Blocking Incidents", new List<BlockingTrendPoint>().Select(d => d.Source)));
    }

    [Fact]
    public void StatsCharts_XeOnly_DmvOnly_AndEmpty()
    {
        Assert.Equal("Block Duration (ms) (blocked-process-report)", BlockingSourceLabel.For("Block Duration (ms)", Stats("blocked-process-report").Select(d => d.Source)));
        Assert.Equal("Total Block Duration (ms) (DMV snapshot)", BlockingSourceLabel.For("Total Block Duration (ms)", Stats("DMV snapshot").Select(d => d.Source)));
        Assert.Equal("Total Block Duration (ms)", BlockingSourceLabel.For("Total Block Duration (ms)", new List<BlockingDurationStatsPoint>().Select(d => d.Source)));
    }

    [Fact]
    public void RowsWithNoSource_KeepThePlainLabel_AsTheDeadlockTrendDoes()
    {
        /* The deadlock trend shares the point type and never sets a source. */
        Assert.Equal("Deadlocks", BlockingSourceLabel.For("Deadlocks", Trend(null).Select(d => d.Source)));
    }

    [Fact]
    public void TheTags_AreTheOnesTheSqlTextsEmit()
    {
        foreach (var sql in new[] { ViewerDataService.BlockingTrendSql, ViewerDataService.BlockingDurationStatsSql })
        {
            Assert.Contains("'" + BlockingSourceLabel.BlockedProcessReport + "' AS source", sql, StringComparison.Ordinal);
            Assert.Contains("'" + BlockingSourceLabel.DmvSnapshot + "' AS source", sql, StringComparison.Ordinal);
            Assert.Contains("source FROM bpr", sql, StringComparison.Ordinal);
            Assert.Contains("source FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheCharts_PutTheLabelOnTheAxisOnlyWhenTheyDrawRows()
    {
        var trend = Between(Tab, "private void RenderBlockingTrendChart(", "private void RenderDeadlockTrendChart(");
        var duration = Between(Tab, "private void RenderBlockingDurationChart(", "private void RenderBlockingTotalDurationChart(");
        var total = Between(Tab, "private void RenderBlockingTotalDurationChart(", "private void RenderDeadlockWaitChart(");

        Assert.Single(Occurrences(trend, "BlockingSourceLabel.For(\"Blocking Incidents\", data.Select(d => d.Source))"));
        Assert.Single(Occurrences(duration, "BlockingSourceLabel.For(\"Block Duration (ms)\", data.Select(d => d.Source))"));
        Assert.Single(Occurrences(total, "BlockingSourceLabel.For(\"Total Block Duration (ms)\", data.Select(d => d.Source))"));
        /* The zero-row branch returns before it: nothing answered, so nothing is named. */
        foreach (var chart in new[] { trend, duration, total })
            Assert.DoesNotContain("BlockingSourceLabel", chart[..chart.IndexOf("return;", StringComparison.Ordinal)], StringComparison.Ordinal);
    }

    [Fact]
    public void TheSeverityRead_TakesTheDatabaseFilter_OnBothArms()
    {
        var sql = ViewerDataService.BlockingDurationStatsSql;
        Assert.Equal(2, Occurrences(sql, "AND   ($5::text[] IS NULL OR database_name = ANY($5))").Count());

        var read = Between(ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.BlockingStats.cs").ReplaceLineEndings("\n"),
            "public async Task<List<BlockingDurationStatsPoint>> GetBlockingDurationStatsAsync(", "The deadlock graphs in the window");
        Assert.Contains("IReadOnlyList<string>? databaseNames = null", read, StringComparison.Ordinal);
        Assert.Contains("DatabaseFilterParameter(databaseNames)", read, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBlockingStatsTab_PassesTheFilter_ToTheSeverityReadAndItsProbe()
    {
        var stats = Between(Tab, "case BlockingStatsSubTabIndex:", "break;");
        Assert.Contains("GetBlockingDurationStatsAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter)", stats, StringComparison.Ordinal);
        Assert.Contains("GetBlockingChartDataStartAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter)", stats, StringComparison.Ordinal);
    }

    private static IEnumerable<int> Occurrences(string text, string needle)
    {
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            yield return i;
    }

    private static string Between(string text, string from, string to)
    {
        var start = text.IndexOf(from, StringComparison.Ordinal);
        Assert.True(start >= 0, from);
        var end = text.IndexOf(to, start + from.Length, StringComparison.Ordinal);
        Assert.True(end > start, to);
        return text[start..end];
    }
}
