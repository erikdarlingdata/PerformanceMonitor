/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's Query Store Regressions grid says where its data starts (#4966). The read compares a RECENT window (the
/// toolbar's range) with a BASELINE of the 7 days before it, so the notice keys on the earlier window: a server added two days ago
/// covers a one-day range whole but has one day of baseline instead of seven, and a notice keyed on the range alone would say
/// nothing. The grid ranks its top 50 by added duration, not by time, so the cap rule does not apply.
/// </summary>
/* The banner's time text reads the process-wide display mode, which these tests set (restored in Dispose); one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerQueryStoreRegressionsDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc);

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    // ── The earlier window and the probe ──

    [Fact]
    public void TheBaselineStart_IsSevenDaysBeforeTheWindow_AndTheReadAndTheProbeTakeItFromOneHelper()
    {
        Assert.Equal(7, ViewerDataService.BaselineLookbackDays);
        Assert.Equal(RangeStart.AddDays(-7), ViewerDataService.QueryStoreRegressionsBaselineStart(RangeStart));

        var source = ViewerFile("ViewerDataService.QueryStoreRegressions.cs");
        Assert.Equal(1, Matches(source, @"var baselineStartUtc = QueryStoreRegressionsBaselineStart\(startUtc\);"));
        Assert.Equal(1, Matches(source, @"serverId,\s*QueryStoreRegressionsBaselineStart\(startUtc\),\s*endUtc,"));
    }

    [Fact]
    public void TheProbe_GoesThroughTheSharedFloor_OverTheRawCollectorTable_FromTheBaselineStart()
    {
        var source = ViewerFile("ViewerDataService.QueryStoreRegressions.cs");

        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"query_store_stats\")", source, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.GetForServerAsync(", source, StringComparison.Ordinal);
        /* Both arms read the raw table, so there is no rollup floor to ask: the baseline arm starts at $5 and the recent arm at $2. */
        var sql = ViewerDataService.QueryStoreRegressionsSql;
        Assert.Equal(2, Matches(sql, @"FROM query_store_stats\b"));
        Assert.DoesNotMatch(@"query_store_stats_\w+", sql);
        Assert.Contains("collection_time >= $5", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
    }

    /* The top 50 are ranked by added duration, not by time, so the rows say nothing about how far back the comparison reached and
       the cap rule (newest-first rows) does not apply. A re-ranking by time would have to wire the rule. */
    [Fact]
    public void TheRanking_IsByCost_SoTheCapRuleDoesNotApply()
    {
        var sql = ViewerDataService.QueryStoreRegressionsSql;

        Assert.Contains("ORDER BY additional_duration_ms DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 50", sql, StringComparison.Ordinal);
    }

    // ── The tab: the probe beside the read, the banner keyed on the baseline window ──

    [Fact]
    public void QueryStoreRegressions_AsksAboutTheBaselineWindow_AndComparesWithItsStart()
    {
        var load = MethodBody(ViewerFile("ViewerServerTab.Queries.cs"), @"private async Task LoadQueryStoreRegressionsAsync\(");

        Assert.Equal(1, Matches(load, @"var baselineStartUtc = ViewerDataService\.QueryStoreRegressionsBaselineStart\(startUtc\);"));
        Assert.Equal(1, Matches(load, @"var floorTask = _dataService\.GetQueryStoreRegressionsDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load,
            @"UpdateTruncationBanner\(QueryStoreRegressionsTruncationBanner,\s*await DataStartOrNullAsync\(floorTask,\s*""Query Store Regressions""\),\s*baselineStartUtc\);"));
    }

    [Fact]
    public void EveryTabRead_OfTheGrid_HasItsProbe()
    {
        var tabs = string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));

        Assert.Equal(1, Matches(tabs, @"_dataService\.GetQueryStoreRegressionsAsync\("));
        Assert.Equal(1, Matches(tabs, @"_dataService\.GetQueryStoreRegressionsDataStartAsync\("));
        Assert.Equal(1, Matches(tabs, @"UpdateTruncationBanner\(QueryStoreRegressionsTruncationBanner,"));
    }

    [Fact]
    public void TheBanner_SitsAboveTheGrid_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        var banner = Regex.Match(xaml, @"<TextBlock Grid\.Row=""1"" x:Name=""QueryStoreRegressionsTruncationBanner""[^>]*/>");
        Assert.True(banner.Success, "QueryStoreRegressionsTruncationBanner is not declared in ViewerServerTab.xaml, in the row above the grid");
        Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
        Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        Assert.Matches(@"<DataGrid Grid\.Row=""2"" x:Name=""QueryStoreRegressionsGrid""", xaml);
    }

    // ── What the banner shows ──

    /* The banner raised for the probe's answer against a requested start, read off the control; null when it is hidden. Seeded
       visible, so a no-op cannot pass as a hidden banner. */
    private static string? BannerFor(DateTime? coverageStartUtc, DateTime requestedStartUtc)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

            ViewerServerTab.UpdateTruncationBanner(banner, coverageStartUtc, requestedStartUtc);

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    private static DateTime Baseline => ViewerDataService.QueryStoreRegressionsBaselineStart(RangeStart);

    /* A server added two days before the range starts: the recent window is covered whole, but the baseline holds 5 of its 7
       days. The notice names where the data starts; keyed on the range alone it would say nothing. */
    [Fact]
    public void ABaselineThatStartsInsideItsWindow_RaisesTheNotice_ThoughTheRecentWindowIsCovered()
    {
        var coverage = RangeStart.AddDays(-2);

        Assert.Equal("Showing since 2026-09-08 00:00", BannerFor(coverage, Baseline));
        Assert.Null(BannerFor(coverage, RangeStart));
    }

    /* Coverage inside the recent window: the baseline is empty and the grid shows nothing, and the notice says since when. */
    [Fact]
    public void CoverageThatStartsInsideTheRecentWindow_RaisesTheNotice()
    {
        Assert.Equal("Showing since 2026-09-11 00:00", BannerFor(RangeStart.AddDays(1), Baseline));
    }

    /* A quiet start: the store covered the whole baseline (coverage 20 days before the range). No notice. */
    [Fact]
    public void AQuietStart_RaisesNoNotice()
    {
        Assert.Null(BannerFor(RangeStart.AddDays(-20), Baseline));
        /* A probe with no answer names nothing. */
        Assert.Null(BannerFor(null, Baseline));
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
