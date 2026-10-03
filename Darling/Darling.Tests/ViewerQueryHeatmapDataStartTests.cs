/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's Query Heatmap says where its data starts (#4966). The read bins <c>query_stats</c> by collection time and
/// the chart draws one column per 5-minute bin that holds a row, so a stretch with no row has no column and the chart cannot tell
/// a server that did not exist yet from a quiet one: the chart needs the notice. The notice names the earlier of the coverage
/// start and the first column drawn. The read has no row cap and no rollup route, which is what the pins below hold in place.
/// </summary>
/* The banner's time text reads the process-wide display mode, which these tests set (restored in Dispose); one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerQueryHeatmapDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    // ── The read and the probe ──

    /* The read is the raw table, with no rollup route and no cap. A rollup route added later must move the probe to that rollup's
       floor (TryForRollup), and a LIMIT added later must be wired to the cap rule: either fails here first. */
    [Theory]
    [InlineData(HeatmapMetric.Duration)]
    [InlineData(HeatmapMetric.Cpu)]
    [InlineData(HeatmapMetric.LogicalReads)]
    [InlineData(HeatmapMetric.LogicalWrites)]
    [InlineData(HeatmapMetric.ExecutionCount)]
    public void TheRead_IsTheRawTable_WithNoRollupRoute_AndNoRowCap(HeatmapMetric metric)
    {
        var sql = ViewerDataService.BuildQueryHeatmapSql(metric);

        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"query_stats_\w+", sql);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheProbe_GoesThroughTheSharedFloor_OverTheRawCollectorTable()
    {
        var source = ViewerFile("ViewerDataService.QueryHeatmap.cs");

        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"query_stats\")", source, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.GetForServerAsync(", source, StringComparison.Ordinal);
    }

    // ── The tab: the probe beside the read, the banner from the columns drawn ──

    [Fact]
    public void TheHeatmapRead_StartsItsProbeBesideIt_AndNamesTheFirstColumnItDraws_WithNoCap()
    {
        var read = MethodBody(ViewerFile("ViewerServerTab.QueryHeatmap.cs"), @"private async Task ReadAndDrawQueryHeatmapAsync\(");

        Assert.Equal(1, Matches(read, @"var dataStartTask = _dataService\.GetQueryHeatmapDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(read,
            @"await ShowEventDataStartAsync\(QueryHeatmapTruncationBanner,\s*dataStartTask,\s*""Query Heatmap"",\s*startUtc,\s*result\.TimeBuckets\.Select\(t => \(DateTime\?\)t\)\);"));
    }

    /* A census over every server-tab file: the one read of the heatmap is paired with its probe and its banner call, and both
       entry points (the range load, the metric change) go through it, so a read added beside it without them fails here. */
    [Fact]
    public void EveryTabRead_OfTheHeatmap_HasItsProbe_AndBothEntryPointsGoThroughIt()
    {
        var tabs = string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));
        var tab = ViewerFile("ViewerServerTab.QueryHeatmap.cs");

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetQueryHeatmapAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetQueryHeatmapDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"ShowEventDataStartAsync\(QueryHeatmapTruncationBanner,"));
        Assert.Equal(2, Matches(tab, @"await ReadAndDrawQueryHeatmapAsync\("));
        Assert.DoesNotContain("await dataStartTask", tab, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBanner_SitsAboveTheChart_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var banner = Regex.Match(xaml, @"<TextBlock Grid\.Row=""1"" x:Name=""QueryHeatmapTruncationBanner""[^>]*/>");
        Assert.True(banner.Success, "QueryHeatmapTruncationBanner is not declared in ViewerServerTab.xaml, in the row above the chart");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.Matches(@"<scottplot:WpfPlot Grid\.Row=""2"" x:Name=""QueryHeatmapChart""", xaml);
    }

    // ── What the banner shows ──

    /* The banner raised for the probe's answer and the first columns the chart draws, read off the control; null when it is
       hidden. Seeded visible, so a no-op cannot pass as a hidden banner. */
    private static string? BannerFor(DateTime? coverageStartUtc, params DateTime[] columns)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

            ViewerServerTab.ShowEventDataStartAsync(
                banner, Task.FromResult(coverageStartUtc), "Query Heatmap", RangeStart, columns.Select(t => (DateTime?)t)).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    private static DateTime At(int days, int hours = 0, int minutes = 0) => RangeStart.AddDays(days).AddHours(hours).AddMinutes(minutes);

    /* Rows start inside the range: the first column is where the first row came, after the coverage began. The notice reads back the coverage start. */
    [Fact]
    public void ColumnsThatStartInsideTheRange_RaiseTheNotice_AtTheCoverageStart()
    {
        Assert.Equal("Showing since 2026-09-04 00:00", BannerFor(At(3), At(3), At(3, 6)));
    }

    /* A quiet start: the store covered the whole range (coverage 20 days before it), and the first column is 5 hours in. No notice. */
    [Fact]
    public void AQuietStart_RaisesNoNotice()
    {
        Assert.Null(BannerFor(At(-20), At(0, 5), At(2)));
    }

    /* A column is a 5-minute bin, stamped at its start, so the first column can start up to 4 minutes before the first row the
       probe found: the notice names the column, never a time later than a column on screen. */
    [Fact]
    public void AColumnThatStartsBeforeTheCoverage_IsNamedInsteadOfIt()
    {
        Assert.Equal("Showing since 2026-09-04 00:00", BannerFor(At(3, 0, 3), At(3), At(3, 6)));
    }

    /* No column at all (the range holds no row) with the coverage starting inside the range: the chart is empty, and the notice says
       from when the store has data. A probe with no answer names nothing. */
    [Fact]
    public void AnEmptyChart_NamesTheCoverage_AndAFailedProbeNamesNothing()
    {
        Assert.Equal("Showing since 2026-09-04 00:00", BannerFor(At(3)));
        Assert.Null(BannerFor(null));
    }

    /* WPF objects require STA; same shape as ViewerLongQueriesDataStartTests. */
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
