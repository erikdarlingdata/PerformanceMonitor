/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's server tab says where the data starts on the two surfaces that read the 7-day tables over
/// the tab's range: Active Queries (query_snapshots) and Blocking's Current Waits (waiting_tasks). A 30-day custom
/// range used to draw 7 days on both with nothing to say so. Each asks the shared probe
/// (<see cref="PerformanceMonitor.Darling.Storage.DataWindowFloor"/>) where the server's coverage starts for the
/// range (the later of its first collection and the table's retention edge, moved earlier by a row in the range,
/// so a quiet start is not a cut) and shows the Queries tab's "Showing since" banner when that is after the
/// range's start. The Queries tab's three raw-table grids (Top Queries, Top Procedures, Query Store) ask their own
/// window-floor probe the same way, and every one of these probes is awaited through the same catch, so a probe that
/// throws costs its banner and nothing after it.
/// </summary>
/* The banner's time text reads the process-wide display mode, which the culture pin sets (restored in Dispose); one
   shared collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerDataStartBannerTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Count(string source, string text) => Regex.Matches(source, Regex.Escape(text)).Count;

    [Fact]
    public void ActiveQueries_ShowsWhereTheSnapshotsStart_OnEveryReadOfTheGrid()
    {
        var tab = ViewerFile("ViewerServerTab.ActiveQueries.cs");

        /* The range load, the deep-link load, the slicer re-read and the window drill (the heatmap, the charts and the
           Overview drills) each draw the grid, so each sets the banner. The window drill is the fourth (3 -> 4, #4953): it
           loads its own narrow window and used to leave the banner of the last range read standing. */
        Assert.Equal(4, Count(tab, "_dataService.GetQuerySnapshotsDataStartAsync("));
        Assert.Equal(4, Count(tab, "UpdateTruncationBanner(QuerySnapshotsTruncationBanner, "));
        Assert.Contains("x:Name=\"QuerySnapshotsTruncationBanner\"", ViewerFile("ViewerServerTab.xaml"), StringComparison.Ordinal);
    }

    [Fact]
    public void CurrentWaits_ShowsWhereTheWaitingTasksStart()
    {
        var tab = ViewerFile("ViewerServerTab.Blocking.cs");

        Assert.Contains("_dataService.GetWaitingTasksDataStartAsync(", tab, StringComparison.Ordinal);
        Assert.Contains("UpdateTruncationBanner(CurrentWaitsTruncationBanner, ", tab, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"CurrentWaitsTruncationBanner\"", ViewerFile("ViewerServerTab.xaml"), StringComparison.Ordinal);
    }

    [Fact]
    public void BothReads_GoThroughTheSharedProbe()
    {
        var snapshots = ViewerFile("ViewerDataService.QuerySnapshots.cs");
        Assert.Contains("DataWindowFloor.GetForServerAsync(", snapshots, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"query_snapshots\")", snapshots, StringComparison.Ordinal);

        var waits = ViewerFile("ViewerDataService.BlockingTrends.cs");
        Assert.Contains("DataWindowFloor.GetForServerAsync(", waits, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"waiting_tasks\")", waits, StringComparison.Ordinal);
    }

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    /* Each read asks its probe about the window it draws and compares the answer with that window's start: the toolbar's
       range on a load, the deep-link's own narrow window (pending.FromUtc, not the toolbar's start the method also
       receives), the slicer's selection on a drag. */
    [Fact]
    public void ActiveQueries_RangeLoad_AsksAboutTheToolbarWindow_AndComparesWithItsStart()
    {
        var tab = ViewerFile("ViewerServerTab.ActiveQueries.cs");

        Assert.Equal(1, Matches(tab, @"var dataStartTask = _dataService\.GetQuerySnapshotsDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(tab,
            @"UpdateTruncationBanner\(QuerySnapshotsTruncationBanner,\s*await DataStartOrNullAsync\(dataStartTask,\s*""Active Queries""\),\s*startUtc\);\s*await LoadActiveQueriesSlicerAsync\(startUtc,\s*endUtc\);"));
    }

    [Fact]
    public void ActiveQueries_DeepLink_AsksAboutThePendingWindow_AndComparesWithItsStart()
    {
        var tab = ViewerFile("ViewerServerTab.ActiveQueries.cs");

        Assert.Equal(1, Matches(tab, @"var pendingDataStartTask = _dataService\.GetQuerySnapshotsDataStartAsync\(_server\.ServerId,\s*pending\.FromUtc,\s*pending\.ToUtc\);"));
        Assert.Equal(1, Matches(tab,
            @"UpdateTruncationBanner\(QuerySnapshotsTruncationBanner,\s*await DataStartOrNullAsync\(pendingDataStartTask,\s*""Active Queries""\),\s*pending\.FromUtc\);"));
    }

    [Fact]
    public void ActiveQueries_SlicerDrag_AsksAboutTheSelection_AndComparesWithItsStart()
    {
        var tab = ViewerFile("ViewerServerTab.ActiveQueries.cs");

        Assert.Equal(1, Matches(tab, @"var dataStartTask = _dataService\.GetQuerySnapshotsDataStartAsync\(_server\.ServerId,\s*e\.StartUtc,\s*e\.EndUtc\);"));
        Assert.Equal(1, Matches(tab,
            @"UpdateTruncationBanner\(QuerySnapshotsTruncationBanner,\s*await DataStartOrNullAsync\(dataStartTask,\s*""Active Queries""\),\s*e\.StartUtc\);"));
    }

    /* A window drill (the heatmap, a resource or trend chart, an Overview drill, or the deep link that reuses it) loads the
       grid for its own narrow window, so it asks the probe about THAT window (fromUtc/toUtc, not the toolbar's range) and
       compares the answer with its start, after the rows are bound and before the slicer loads, as the deep-link branch
       of the range load does. Without it the banner of the last range read stays up: it can name a cut the drill window
       does not have, or miss one it does. */
    [Fact]
    public void ActiveQueries_WindowDrill_AsksAboutTheDrillWindow_AndComparesWithItsStart()
    {
        var tab = ViewerFile("ViewerServerTab.ActiveQueries.cs");
        var drill = Regex.Match(tab, @"private async Task NavigateToActiveQueriesForWindowAsync\(.*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(drill.Success, "NavigateToActiveQueriesForWindowAsync was not found in ViewerServerTab.ActiveQueries.cs");
        var body = drill.Value;

        Assert.Equal(1, Matches(body,
            @"var dataStartTask = _dataService\.GetQuerySnapshotsDataStartAsync\(_server\.ServerId,\s*fromUtc,\s*toUtc\);\s*var dataReadTask = _dataService\.GetLatestQuerySnapshotsAsync\(_server\.ServerId,\s*fromUtc,\s*toUtc,"));
        /* #5034: the read is awaited beside its probe, so a probe that fails after the read threw is watched. The rows come from the same read. */
        Assert.Equal(1, Matches(body,
            @"await AwaitReadWatchingProbeAsync\(dataReadTask,\s*dataStartTask,\s*""Active Queries""\);\s*var \(totalCount, snapshots\) = dataReadTask\.Result;"));
        Assert.Equal(1, Matches(body,
            @"_querySnapshotsFilterMgr!\.UpdateData\(snapshots\);[\s\S]*?UpdateTruncationBanner\(QuerySnapshotsTruncationBanner,\s*await DataStartOrNullAsync\(dataStartTask,\s*""Active Queries""\),\s*fromUtc\);\s*await LoadActiveQueriesSlicerAsync\(fromUtc\.AddHours\(-1\),\s*toUtc\.AddHours\(1\)\);"));
    }

    /* "Latest Snapshot" swaps the grid for the newest stored batch, so the "Showing since" banner the last range read
       raised no longer describes it. The handler collapses the banner and clears its text as the batch is bound, and not
       before, so a read that fails, which leaves the range rows in the grid, keeps the banner that still describes them.
       It is a direct hide and not a fifth UpdateTruncationBanner call: that call asks a probe about a range, and this
       read has none (the count pin above stays at four). The next range read raises the banner again through those
       four calls. */
    [Fact]
    public void LatestSnapshot_HidesTheActiveQueriesBanner_AsTheBatchIsBound_AndNotWhenTheReadFails()
    {
        var tab = ViewerFile("ViewerServerTab.ActiveQueries.cs");
        var handler = Regex.Match(tab, @"private async void LatestSnapshot_Click\(.*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(handler.Success, "LatestSnapshot_Click was not found in ViewerServerTab.ActiveQueries.cs");
        var body = handler.Value;

        var bound = body.IndexOf("_querySnapshotsFilterMgr!.UpdateData(rows);", StringComparison.Ordinal);
        var failed = body.IndexOf("catch (Exception ex)", StringComparison.Ordinal);
        Assert.True(bound >= 0, "LatestSnapshot_Click no longer binds the batch through _querySnapshotsFilterMgr.UpdateData(rows)");
        Assert.True(failed > bound, "LatestSnapshot_Click no longer has its failure arm after the rows are bound");

        var collapses = Regex.Matches(body, @"QuerySnapshotsTruncationBanner\.Visibility\s*=\s*Visibility\.Collapsed;");
        var clears = Regex.Matches(body, @"QuerySnapshotsTruncationBanner\.Text\s*=\s*(?:""""|string\.Empty);");
        Assert.True(collapses.Count == 1 && clears.Count == 1,
            "LatestSnapshot_Click must collapse the Active Queries banner and clear its text, once each (found " + collapses.Count + " and "
            + clears.Count + "): the newest batch replaces the range rows the banner described");
        Assert.True(collapses[0].Index > bound && collapses[0].Index < failed && clears[0].Index > bound && clears[0].Index < failed,
            "the banner must come down after the batch is bound and before the failure arm, so a failed read keeps it");
    }

    [Fact]
    public void CurrentWaits_AsksAboutTheToolbarWindow_AndComparesWithItsStart()
    {
        var tab = ViewerFile("ViewerServerTab.Blocking.cs");

        Assert.Equal(1, Matches(tab, @"var dataStartTask = _dataService\.GetWaitingTasksDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(tab,
            @"UpdateTruncationBanner\(CurrentWaitsTruncationBanner,\s*await DataStartOrNullAsync\(dataStartTask,\s*""Current Waits""\),\s*startUtc\);"));
    }

    /* The Queries tab's three raw-table grids (Top Queries, Top Procedures, Query Store) start their window-floor probe
       beside the grid read, ask it about the window the grid draws, and compare the answer with that window's start: the
       toolbar's range on a load (startUtc), the slicer's selection on a drag (e.StartUtc). Both await the probe through the
       same catch as the two surfaces above, so a probe that throws costs the grid its banner and not the slicer and the
       comparison loads that follow it. The tail is what the call passes after the start: the hourly-tier suffix on Top
       Queries and Top Procedures, the interval-table plan on a Query Store load, nothing on a Query Store slicer drag. */
    private const string HourlyTail = @",\s*tier == ""hourly"" \? HourlyTierSuffix : null";
    private const string WidePlanTail = @",\s*widePlan: widePlan";

    [Theory]
    [InlineData("GetQueryStatsWindowFloorAsync", "QueryStats", "Query Stats", HourlyTail)]
    [InlineData("GetProcedureStatsWindowFloorAsync", "ProcStats", "Procedure Stats", HourlyTail)]
    [InlineData("GetQueryStoreWindowFloorAsync", "QueryStore", "Query Store", WidePlanTail)]
    public void QueriesTab_RangeLoad_AsksAboutTheToolbarWindow_AndComparesWithItsStart(string probe, string grid, string surface, string tail)
    {
        var tab = ViewerFile("ViewerServerTab.Queries.cs");

        Assert.Equal(1, Matches(tab, $@"var floorTask = _dataService\.{probe}\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(tab,
            $@"UpdateTruncationBanner\({grid}TruncationBanner,\s*await DataStartOrNullAsync\(floorTask,\s*""{surface}""\),\s*startUtc{tail}\);\s*await Load{grid}SlicerAsync\(startUtc,\s*endUtc\);\s*await Refresh{grid}ComparisonAsync\(startUtc,\s*endUtc\);"));
    }

    [Theory]
    [InlineData("GetQueryStatsWindowFloorAsync", "QueryStats", "Query Stats", HourlyTail)]
    [InlineData("GetProcedureStatsWindowFloorAsync", "ProcStats", "Procedure Stats", HourlyTail)]
    [InlineData("GetQueryStoreWindowFloorAsync", "QueryStore", "Query Store", "")]
    public void QueriesTab_SlicerDrag_AsksAboutTheSelection_AndComparesWithItsStart(string probe, string grid, string surface, string tail)
    {
        var tab = ViewerFile("ViewerServerTab.Queries.cs");

        Assert.Equal(1, Matches(tab, $@"var floorTask = _dataService\.{probe}\(_server\.ServerId,\s*e\.StartUtc,\s*e\.EndUtc\);"));
        Assert.Equal(1, Matches(tab,
            $@"UpdateTruncationBanner\({grid}TruncationBanner,\s*await DataStartOrNullAsync\(floorTask,\s*""{surface}""\),\s*e\.StartUtc{tail}\);\s*await Refresh{grid}ComparisonAsync\(e\.StartUtc,\s*e\.EndUtc\);"));
    }

    /* No read awaits its probe bare (a probe that throws would unwind the load past the slicer, or the rest of the tab),
       and the default log is the viewer's own. On the Queries tab every banner call goes through the catch, so a grid read
       added later without it fails here instead of shipping bare. */
    [Fact]
    public void NoLoadAwaitsItsProbeBare_AndTheDefaultLogIsTheViewersOwn()
    {
        var queries = ViewerFile("ViewerServerTab.Queries.cs");

        foreach (var source in new[] { ViewerFile("ViewerServerTab.ActiveQueries.cs"), ViewerFile("ViewerServerTab.Blocking.cs"), queries })
        {
            Assert.DoesNotContain("await dataStartTask", source, StringComparison.Ordinal);
            Assert.DoesNotContain("await pendingDataStartTask", source, StringComparison.Ordinal);
            Assert.DoesNotContain("await floorTask", source, StringComparison.Ordinal);
        }

        /* Six banner calls: a range load and a slicer drag on each of the three grids. The Plan Corrections grid's banner goes
           through ShowEventDataStartAsync, because it passes the cap rule's inputs rather than the probe's answer alone;
           ViewerPlanCorrectionsDataStartTests pins that call. The Query Store Regressions grid's goes through
           ShowQueryStoreRegressionsDataStartAsync (#4966), because it compares the answer with the baseline window's start rather
           than the range's; ViewerQueryStoreRegressionsDataStartTests pins that call and the step's own banner call. */
        const string bannerCall = @"UpdateTruncationBanner\(\w+TruncationBanner,";
        Assert.Equal(6, Matches(queries, bannerCall));
        Assert.Equal(6, Matches(queries, bannerCall + @"\s*await DataStartOrNullAsync\(floorTask,"));

        Assert.Contains("(warn ?? ViewerLogger.Warn)(", queries, StringComparison.Ordinal);
    }

    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    /* A probe that throws costs the surface its banner and nothing else: the helper answers null instead of throwing (the
       load goes on to the slicer), the banner drawn from that answer is hidden (seeded visible, so a no-op cannot pass),
       and the failure is logged with the surface and the cause. */
    [Fact]
    public void AProbeThatThrows_HidesTheBanner_IsLogged_AndLetsTheLoadGoOn()
    {
        OnStaThread(() =>
        {
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "Showing since 2026-09-04 00:00:00" };
            var logged = new List<(string Source, string Message)>();
            var probe = Task.FromException<DateTime?>(new InvalidOperationException("the store went away"));

            var floor = ViewerServerTab.DataStartOrNullAsync(probe, "Active Queries", (source, message) => logged.Add((source, message)))
                .GetAwaiter().GetResult();
            ViewerServerTab.UpdateTruncationBanner(banner, floor, RangeStart);

            Assert.Null(floor);
            Assert.Equal(Visibility.Collapsed, banner.Visibility);
            var line = Assert.Single(logged);
            Assert.Equal("ViewerServerTab", line.Source);
            Assert.Contains("Active Queries", line.Message, StringComparison.Ordinal);
            Assert.Contains("the store went away", line.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void AProbeThatAnswers_PassesTheAnswerOn_AndLogsNothing()
    {
        OnStaThread(() =>
        {
            var banner = new TextBlock();
            var logged = new List<(string Source, string Message)>();
            var answer = RangeStart.AddDays(3);

            var floor = ViewerServerTab.DataStartOrNullAsync(Task.FromResult<DateTime?>(answer), "Current Waits", (source, message) => logged.Add((source, message)))
                .GetAwaiter().GetResult();
            ViewerServerTab.UpdateTruncationBanner(banner, floor, RangeStart);

            Assert.Equal(answer, floor);
            Assert.Empty(logged);
            Assert.Equal(Visibility.Visible, banner.Visibility);
        });
    }

    /* The banner names its instants on the invariant culture whatever the machine's: under a Thai default (Buddhist
       calendar, 2026 prints as 2569) or a Finnish one (a "." time separator) it still reads Gregorian "yyyy-MM-dd HH:mm:ss",
       as Lite's banner and the web's notice do. Every branch of the shared banner is read. */
    [Theory]
    [InlineData("th-TH")]
    [InlineData("fi-FI")]
    public void TheBanner_NamesItsInstants_OnTheInvariantCulture(string cultureName)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            OnStaThread(() =>
            {
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(cultureName);
                /* The premise: this culture's own formatting does not give the invariant text, so the pin is not vacuous. */
                Assert.NotEqual("2026-09-04 00:00:00", RangeStart.AddDays(3).ToString("yyyy-MM-dd HH:mm:ss"));

                var banner = new TextBlock();

                ViewerServerTab.UpdateTruncationBanner(banner, RangeStart.AddDays(3), RangeStart);
                Assert.Equal("Showing since 2026-09-04 00:00:00", banner.Text);

                /* The seconds are the instant's own, not a rounded minute. */
                ViewerServerTab.UpdateTruncationBanner(banner, RangeStart.AddDays(3).AddSeconds(42), RangeStart);
                Assert.Equal("Showing since 2026-09-04 00:00:42", banner.Text);

                ViewerServerTab.UpdateTruncationBanner(banner, null, RangeStart, " (hourly)");
                Assert.Equal("Showing 2026-09-01 00:00:00 (hourly)", banner.Text);

                var wideStart = RangeStart.AddDays(1);
                var plan = new QueryStoreIntervalWide.WideReadPlan(
                    true, RangeStart.AddDays(4), wideStart, wideStart, QueryStoreIntervalWide.WideStartBound.FilledSince);
                ViewerServerTab.UpdateTruncationBanner(banner, RangeStart.AddDays(4), RangeStart, widePlan: plan);
                Assert.StartsWith("Showing since 2026-09-02 00:00:00", banner.Text, StringComparison.Ordinal);
                Assert.Contains(" · slicer since 2026-09-05 00:00:00", banner.Text, StringComparison.Ordinal);
            });
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
        }
    }

    /* WPF objects require STA; same shape as RawWindowFloorViewerPortTests. */
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
