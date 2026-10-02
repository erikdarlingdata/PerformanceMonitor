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
/// range's start.
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

        /* The range load, the deep-link load, and the slicer re-read each draw the grid, so each sets the banner. */
        Assert.Equal(3, Count(tab, "_dataService.GetQuerySnapshotsDataStartAsync("));
        Assert.Equal(3, Count(tab, "UpdateTruncationBanner(QuerySnapshotsTruncationBanner, "));
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

    [Fact]
    public void CurrentWaits_AsksAboutTheToolbarWindow_AndComparesWithItsStart()
    {
        var tab = ViewerFile("ViewerServerTab.Blocking.cs");

        Assert.Equal(1, Matches(tab, @"var dataStartTask = _dataService\.GetWaitingTasksDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(tab,
            @"UpdateTruncationBanner\(CurrentWaitsTruncationBanner,\s*await DataStartOrNullAsync\(dataStartTask,\s*""Current Waits""\),\s*startUtc\);"));
    }

    /* No read awaits its probe bare (a probe that throws would unwind the load past the slicer, or the rest of the tab),
       and the default log is the viewer's own. */
    [Fact]
    public void NoLoadAwaitsItsProbeBare_AndTheDefaultLogIsTheViewersOwn()
    {
        foreach (var source in new[] { ViewerFile("ViewerServerTab.ActiveQueries.cs"), ViewerFile("ViewerServerTab.Blocking.cs") })
        {
            Assert.DoesNotContain("await dataStartTask", source, StringComparison.Ordinal);
            Assert.DoesNotContain("await pendingDataStartTask", source, StringComparison.Ordinal);
        }

        Assert.Contains("(warn ?? ViewerLogger.Warn)(", ViewerFile("ViewerServerTab.Queries.cs"), StringComparison.Ordinal);
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
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "Showing since 2026-09-04 00:00" };
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
       calendar, 2026 prints as 2569) or a Finnish one (a "." time separator) it still reads Gregorian "yyyy-MM-dd HH:mm",
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
                Assert.NotEqual("2026-09-04 00:00", RangeStart.AddDays(3).ToString("yyyy-MM-dd HH:mm"));

                var banner = new TextBlock();

                ViewerServerTab.UpdateTruncationBanner(banner, RangeStart.AddDays(3), RangeStart);
                Assert.Equal("Showing since 2026-09-04 00:00", banner.Text);

                ViewerServerTab.UpdateTruncationBanner(banner, null, RangeStart, " (hourly)");
                Assert.Equal("Showing 2026-09-01 00:00 (hourly)", banner.Text);

                var wideStart = RangeStart.AddDays(1);
                var plan = new QueryStoreIntervalWide.WideReadPlan(
                    true, RangeStart.AddDays(4), wideStart, wideStart, QueryStoreIntervalWide.WideStartBound.FilledSince);
                ViewerServerTab.UpdateTruncationBanner(banner, RangeStart.AddDays(4), RangeStart, widePlan: plan);
                Assert.StartsWith("Showing since 2026-09-02 00:00", banner.Text, StringComparison.Ordinal);
                Assert.Contains(" · slicer since 2026-09-05 00:00", banner.Text, StringComparison.Ordinal);
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
