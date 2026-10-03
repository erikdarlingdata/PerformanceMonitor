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
/// The desktop viewer's event grids say where their data starts (#4966). Blocked process reports and deadlocks filter
/// on the event's own time, and a server's first collection stores the server's event history, so a row can carry an
/// event time from before the server was added. The notice names the earlier of the collector's coverage start (the
/// shared <see cref="PerformanceMonitor.Darling.Storage.DataWindowFloor"/> probe) and the earliest event the grid
/// shows, so it never names a time later than a row on screen.
/// </summary>
/* The banner's time text reads the process-wide display mode, which the culture pin sets (restored in Dispose); one
   shared collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerEventDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static string MethodBody(string source, string signaturePattern)
    {
        var match = Regex.Match(source, signaturePattern + @".*?\r?\n    \}\r?\n", RegexOptions.Singleline);
        Assert.True(match.Success, $"{signaturePattern} was not found");
        return match.Value;
    }

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    // ── The rule: the earlier of the coverage start and the earliest row shown ──

    [Fact]
    public void TheNotice_NamesTheCoverageStart_WhenNoEarlierRowIsShown()
    {
        var coverage = RangeStart.AddDays(3);

        Assert.Equal(coverage, ViewerEventDataStart.Of(coverage, null));
        Assert.Equal(coverage, ViewerEventDataStart.Of(coverage, coverage.AddHours(2)));
    }

    [Fact]
    public void TheNotice_NamesTheHistoryStart_WhenTheGridShowsEventsFromBeforeTheCoverage()
    {
        var coverage = RangeStart.AddDays(3);
        var history = RangeStart.AddDays(1);

        Assert.Equal(history, ViewerEventDataStart.Of(coverage, history));
    }

    [Fact]
    public void ADeadProbe_NamesNothing_WhenTheReadStayedUnderItsCap()
    {
        Assert.Null(ViewerEventDataStart.Of(null, RangeStart.AddDays(2), probeFailed: true));
        Assert.Null(ViewerEventDataStart.Of(null, null, probeFailed: true));
        /* Whatever coverage the dead probe seems to hold, a failure names nothing. */
        Assert.Null(ViewerEventDataStart.Of(RangeStart.AddDays(3), RangeStart.AddDays(2), probeFailed: true));
    }

    /* The range ends before the server's first collection: the probe found no coverage in it, yet the grid lists the history
       that collection stored. Without coverage to compare, the notice names the earliest event shown. */
    [Fact]
    public void AProbeWithNoCoverage_NamesTheEarliestEventShown()
    {
        var earliest = RangeStart.AddDays(1);

        Assert.Equal(earliest, ViewerEventDataStart.Of(null, earliest));
        Assert.Equal(earliest, ViewerEventDataStart.Of(null, earliest, readHitCap: false, probeFailed: false));
        /* Nothing in the range and nothing shown: an empty grid already says so. */
        Assert.Null(ViewerEventDataStart.Of(null, null));
    }

    [Fact]
    public void TheEarliestShown_SkipsRowsWithoutAnEventTime()
    {
        Assert.Null(ViewerEventDataStart.EarliestOf([]));
        Assert.Null(ViewerEventDataStart.EarliestOf([null, null]));
        Assert.Equal(RangeStart.AddHours(5), ViewerEventDataStart.EarliestOf([RangeStart.AddHours(9), null, RangeStart.AddHours(5), RangeStart.AddHours(7)]));
    }

    // ── A read that hit its row cap names its oldest row ──

    /* The grid keeps the newest N rows, so it reaches back no further than the oldest it returned, even where the store
       covers the whole range. */
    [Fact]
    public void AReadThatHitsItsCap_NamesItsOldestRow_WhateverTheStoreCovers()
    {
        var oldest = RangeStart.AddDays(3);

        Assert.Equal(oldest, ViewerEventDataStart.Of(RangeStart.AddDays(-20), oldest, readHitCap: true));
        Assert.Equal(oldest, ViewerEventDataStart.Of(RangeStart, oldest, readHitCap: true));
        Assert.Equal(oldest, ViewerEventDataStart.Of(oldest.AddDays(2), oldest, readHitCap: true));
        /* The notice comes from the rows, so a probe with no answer does not hide it. */
        Assert.Equal(oldest, ViewerEventDataStart.Of(null, oldest, readHitCap: true));
    }

    [Fact]
    public void AReadUnderItsCap_KeepsTheResultItHad()
    {
        var coverage = RangeStart.AddDays(3);

        Assert.Equal(coverage, ViewerEventDataStart.Of(coverage, coverage.AddHours(2), readHitCap: false));
        Assert.Equal(RangeStart.AddDays(1), ViewerEventDataStart.Of(coverage, RangeStart.AddDays(1), readHitCap: false));
        Assert.Null(ViewerEventDataStart.Of(null, RangeStart.AddDays(2), readHitCap: false, probeFailed: true));
        /* A full page with no dated row has nothing to name, so it falls back to the coverage. */
        Assert.Equal(coverage, ViewerEventDataStart.Of(coverage, null, readHitCap: true));
    }

    [Fact]
    public void TheCapIsHit_WhenTheReadReturnedItsFullPage()
    {
        Assert.True(ViewerEventDataStart.ReadHitCap(200, 200));
        Assert.True(ViewerEventDataStart.ReadHitCap(201, 200));
        Assert.False(ViewerEventDataStart.ReadHitCap(199, 200));
        Assert.False(ViewerEventDataStart.ReadHitCap(0, 50));
        Assert.False(ViewerEventDataStart.ReadHitCap(5000, null));
    }

    [Fact]
    public void TheCaps_AreTheLimitsOfTheReads()
    {
        Assert.Equal(200, ViewerDataService.BlockedProcessReportsRowCap);
        Assert.Equal(50, ViewerDataService.DeadlocksRowCap);
        Assert.Contains($"LIMIT {ViewerDataService.BlockedProcessReportsRowCap}", ViewerDataService.BlockedProcessReportsSql, StringComparison.Ordinal);
        Assert.Contains($"LIMIT {ViewerDataService.DeadlocksRowCap}", ViewerDataService.RecentDeadlocksSql, StringComparison.Ordinal);
    }

    /* A full page whose oldest row came 3 days after the range started raises the notice at that row, though the store covers
       the range (coverage 20 days before it). Under the cap the same rows raise none. */
    [Fact]
    public void AReadThatHitsItsCap_RaisesTheNotice_AtItsOldestRow()
    {
        Assert.Equal("Showing since 2026-09-04 00:00:00", CappedBannerFor(RangeStart.AddDays(-20), RangeStart.AddDays(3), shownRows: 3, rowCap: 3));
    }

    [Fact]
    public void AReadUnderItsCap_RaisesNoNotice_WhenTheStoreCoversTheRange()
    {
        Assert.Null(CappedBannerFor(RangeStart.AddDays(-20), RangeStart.AddDays(3), shownRows: 2, rowCap: 3));
    }

    private static string? CappedBannerFor(DateTime coverageStartUtc, DateTime oldestShownUtc, int shownRows, int rowCap)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

            ViewerServerTab.ShowEventDataStartAsync(
                banner, Task.FromResult<DateTime?>(coverageStartUtc), "Deadlocks", RangeStart,
                Enumerable.Range(0, shownRows).Select(i => (DateTime?)oldestShownUtc.AddHours(6 * i)), rowCap).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    // ── What the banner shows ──

    /* Rows start inside the range and the history is none: the notice reads back the coverage start. */
    [Fact]
    public void RowsThatStartInsideTheRange_RaiseTheNotice_AtTheCoverageStart()
    {
        Assert.Equal("Showing since 2026-09-04 00:00:00", BannerFor(RangeStart.AddDays(3), RangeStart.AddDays(3).AddHours(6)));
    }

    /* History that reaches before the coverage start gives a notice at the history's start, not at the later coverage. */
    [Fact]
    public void HistoryThatReachesBeforeTheCoverage_RaisesTheNotice_AtTheHistorysStart()
    {
        Assert.Equal("Showing since 2026-09-02 00:00:00", BannerFor(RangeStart.AddDays(3), RangeStart.AddDays(1)));
    }

    /* History that reaches the range start gives no notice, though the coverage starts days later. */
    [Fact]
    public void HistoryThatReachesTheRangeStart_RaisesNoNotice()
    {
        Assert.Null(BannerFor(RangeStart.AddDays(3), RangeStart.AddMinutes(30)));
        Assert.Null(BannerFor(RangeStart.AddDays(3), RangeStart));
    }

    /* A quiet start: the store covered the whole range (coverage at or before its start), but the first event comes late. */
    [Fact]
    public void AQuietStart_RaisesNoNotice()
    {
        Assert.Null(BannerFor(RangeStart.AddDays(-20), RangeStart.AddHours(5)));
        Assert.Null(BannerFor(RangeStart, RangeStart.AddHours(5)));
    }

    /* A probe that throws is hidden, not guessed: the grid's own first row must not stand in for the coverage. */
    [Fact]
    public void AProbeThatThrows_RaisesNoNotice_AndIsLogged()
    {
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "Showing since 2026-09-04 00:00:00" };

            ViewerServerTab.ShowEventDataStartAsync(
                banner, Task.FromException<DateTime?>(new InvalidOperationException("the store went away")), "Deadlocks", RangeStart,
                [RangeStart.AddDays(2)]).GetAwaiter().GetResult();

            Assert.Equal(Visibility.Collapsed, banner.Visibility);
        });
    }

    /* A server first collected on the range's last day, with events back to a day into the range: the range ends before the
       first collection, so the probe finds no coverage in it, and the grid still lists the history that collection stored.
       The grid starts a day late, and the notice says where. */
    [Fact]
    public void ARangeThatEndsBeforeTheFirstCollection_RaisesTheNotice_AtTheEarliestEventShown()
    {
        Assert.Equal(
            "Showing since 2026-09-02 00:00",
            BannerOf(Task.FromResult<DateTime?>(null), RangeStart, [RangeStart.AddDays(2), RangeStart.AddDays(1)]));
    }

    [Fact]
    public void ARangeThatEndsBeforeTheFirstCollection_RaisesNoNotice_WhenTheHistoryReachesTheRangeStart()
    {
        Assert.Null(BannerOf(Task.FromResult<DateTime?>(null), RangeStart, [RangeStart.AddMinutes(30), RangeStart.AddDays(2)]));
        Assert.Null(BannerOf(Task.FromResult<DateTime?>(null), RangeStart, [RangeStart]));
        /* Nothing in the range and nothing shown: an empty grid already says so. */
        Assert.Null(BannerOf(Task.FromResult<DateTime?>(null), RangeStart, []));
    }

    /* No coverage and a failed probe are different answers: the first names the history, the second names nothing. */
    [Fact]
    public void AProbeThatFoundNoCoverage_RaisesTheNotice_WhereAProbeThatThrowsStaysHidden()
    {
        DateTime?[] shown = [RangeStart.AddDays(1)];

        Assert.Equal("Showing since 2026-09-02 00:00", BannerOf(Task.FromResult<DateTime?>(null), RangeStart, shown));
        Assert.Null(BannerOf(Task.FromException<DateTime?>(new InvalidOperationException("the store went away")), RangeStart, shown));
    }

    // ── A grid merged from several reads: one of them filled its own cap ──

    /* The merge drops the rows another read already holds and the rows that repeat a pair within a minute, so a read that
       stopped at its LIMIT can leave the merged list under the grid's cap. */
    [Fact]
    public void ACappedSource_NamesItsOldestRow_WhenTheMergedListStaysUnderTheCap()
    {
        var oldest = RangeStart.AddDays(3);
        DateTime?[] capped = [oldest.AddHours(2), oldest.AddHours(1), oldest];
        DateTime?[] under = [RangeStart.AddDays(1)];

        Assert.Equal(oldest, ViewerEventDataStart.CappedSourceStart(3, under, capped));
        Assert.Equal(oldest, ViewerEventDataStart.CappedSourceStart(3, capped, under));
        Assert.Null(ViewerEventDataStart.CappedSourceStart(4, under, capped));
        Assert.Null(ViewerEventDataStart.CappedSourceStart(3, under, Array.Empty<DateTime?>()));
    }

    [Fact]
    public void WhenBothSourcesFillTheirCap_TheLaterOldestRowBoundsTheGrid()
    {
        DateTime?[] xe = [RangeStart.AddDays(6), RangeStart.AddDays(5)];
        DateTime?[] dmv = [RangeStart.AddDays(4), RangeStart.AddDays(2)];

        Assert.Equal(RangeStart.AddDays(5), ViewerEventDataStart.CappedSourceStart(2, xe, dmv));
        /* A row with no event time names nothing, and does not count as the oldest. */
        Assert.Equal(RangeStart.AddDays(2), ViewerEventDataStart.CappedSourceStart(2, new DateTime?[] { null, RangeStart.AddDays(2) }));
        Assert.Null(ViewerEventDataStart.CappedSourceStart(2, new DateTime?[] { null, null }));
    }

    [Fact]
    public void ACappedSource_BoundsTheNotice_WhateverTheStoreCovers()
    {
        var oldest = RangeStart.AddDays(3);

        Assert.Equal(oldest, ViewerEventDataStart.Of(RangeStart.AddDays(-20), RangeStart.AddDays(1), cappedSourceOldestUtc: oldest));
        /* It comes from the rows, so a probe with no answer, or a dead one, does not hide it. */
        Assert.Equal(oldest, ViewerEventDataStart.Of(null, RangeStart.AddDays(1), cappedSourceOldestUtc: oldest));
        Assert.Equal(oldest, ViewerEventDataStart.Of(null, RangeStart.AddDays(1), probeFailed: true, cappedSourceOldestUtc: oldest));
        /* Beside a merged list that also filled its cap, the later of the two oldest rows is where the grid is complete from. */
        Assert.Equal(oldest, ViewerEventDataStart.Of(null, RangeStart.AddDays(2), readHitCap: true, cappedSourceOldestUtc: oldest));
        Assert.Equal(oldest.AddDays(1), ViewerEventDataStart.Of(null, oldest.AddDays(1), readHitCap: true, cappedSourceOldestUtc: oldest));
        /* No capped source: the rule it had. */
        Assert.Equal(RangeStart.AddDays(-20), ViewerEventDataStart.Of(RangeStart.AddDays(-20), RangeStart.AddDays(1), cappedSourceOldestUtc: null));
    }

    /* Two rows are under the cap of 3, so the merged list names nothing; the DMV read behind it filled its cap 3 days in. */
    [Fact]
    public void ACappedSource_RaisesTheNotice_AtItsOldestRow_WhereTheMergedListStaysUnderTheCap()
    {
        var oldest = RangeStart.AddDays(3);
        var covered = Task.FromResult<DateTime?>(RangeStart.AddDays(-20));
        DateTime?[] shown = [RangeStart.AddDays(1), oldest];

        Assert.Null(BannerOf(covered, RangeStart, shown, rowCap: 3));
        Assert.Equal("Showing since 2026-09-04 00:00", BannerOf(covered, RangeStart, shown, rowCap: 3, cappedSourceOldestUtc: oldest));
    }

    /* A capped read dropped rows for certain, so its verdict has no slack, as the merged list's own cap has none. */
    [Fact]
    public void ACappedSource_GetsNoSlack_OnARangeOfAnHourOrLess()
    {
        var oldest = RangeStart.AddMinutes(30);

        Assert.Equal(
            "Showing since 2026-09-01 00:30",
            BannerOf(Task.FromResult<DateTime?>(null), RangeStart, [oldest], rowCap: 3, cappedSourceOldestUtc: oldest));
    }

    /* The banner raised for any probe answer, through the shared function; null when it is hidden. */
    private static string? BannerOf(
        Task<DateTime?> probe, DateTime startUtc, IEnumerable<DateTime?> shownUtc, int? rowCap = null, DateTime? cappedSourceOldestUtc = null)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            /* Seeded visible, so a no-op cannot pass as a hidden banner. */
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

            ViewerServerTab.ShowEventDataStartAsync(banner, probe, "Deadlocks", startUtc, shownUtc, rowCap, cappedSourceOldestUtc).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    /* The banner raised for the probe's answer and the grid's earliest row, read off the control; null when it is hidden. */
    private static string? BannerFor(DateTime coverageStartUtc, DateTime earliestShownUtc)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            /* Seeded visible, so a no-op cannot pass as a hidden banner. */
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

            ViewerServerTab.ShowEventDataStartAsync(
                banner, Task.FromResult<DateTime?>(coverageStartUtc), "Blocked Process Reports", RangeStart,
                [earliestShownUtc.AddDays(2), earliestShownUtc]).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    // ── Every read path of the two grids asks the probe ──

    private static string TabSources() =>
        string.Join("\n", Directory.GetFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "ViewerServerTab*.cs").Select(File.ReadAllText));

    [Fact]
    public void BlockedProcessReports_AsksAboutTheWindowItDraws_OnTheRangeLoad_AndOnTheSlicerDrag()
    {
        var tab = ViewerFile("ViewerServerTab.Blocking.cs");

        var load = MethodBody(tab, @"private async Task LoadBlockedProcessReportsAsync\(");
        Assert.Equal(1, Matches(load, @"var dataStartTask = _dataService\.GetBlockedProcessReportsDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load,
            @"await ShowEventDataStartAsync\(BlockedProcessReportsTruncationBanner,\s*dataStartTask,\s*""Blocked Process Reports"",\s*startUtc,\s*rows\.Select\(r => r\.EventTime\),\s*ViewerDataService\.BlockedProcessReportsRowCap,\s*read\.CappedSourceStartUtc\);"));

        var drag = MethodBody(tab, @"private async void OnBlockingSlicerChanged\(");
        Assert.Equal(1, Matches(drag, @"var dataStartTask = _dataService\.GetBlockedProcessReportsDataStartAsync\(_server\.ServerId,\s*e\.StartUtc,\s*e\.EndUtc\);"));
        Assert.Equal(1, Matches(drag,
            @"await ShowEventDataStartAsync\(BlockedProcessReportsTruncationBanner,\s*dataStartTask,\s*""Blocked Process Reports"",\s*e\.StartUtc,\s*rows\.Select\(r => r\.EventTime\),\s*ViewerDataService\.BlockedProcessReportsRowCap,\s*read\.CappedSourceStartUtc\);"));
    }

    [Fact]
    public void Deadlocks_AsksAboutTheWindowItDraws_OnTheRangeLoad_AndOnTheSlicerDrag()
    {
        var tab = ViewerFile("ViewerServerTab.Blocking.cs");

        var load = MethodBody(tab, @"private async Task LoadDeadlocksAsync\(");
        Assert.Equal(1, Matches(load, @"var dataStartTask = _dataService\.GetDeadlocksDataStartAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load,
            @"await ShowEventDataStartAsync\(DeadlocksTruncationBanner,\s*dataStartTask,\s*""Deadlocks"",\s*startUtc,\s*rows\.Select\(r => r\.DeadlockTime\),\s*ViewerDataService\.DeadlocksRowCap\);"));

        var drag = MethodBody(tab, @"private async void OnDeadlockSlicerChanged\(");
        Assert.Equal(1, Matches(drag, @"var dataStartTask = _dataService\.GetDeadlocksDataStartAsync\(_server\.ServerId,\s*e\.StartUtc,\s*e\.EndUtc\);"));
        Assert.Equal(1, Matches(drag,
            @"await ShowEventDataStartAsync\(DeadlocksTruncationBanner,\s*dataStartTask,\s*""Deadlocks"",\s*e\.StartUtc,\s*rows\.Select\(r => r\.DeadlockTime\),\s*ViewerDataService\.DeadlocksRowCap\);"));
    }

    /* A census over every server-tab file: each read of either grid's rows is paired with its probe, so a third read path
       added later without one fails here instead of shipping with a stale banner. */
    [Fact]
    public void EveryTabRead_OfTheTwoGrids_HasItsProbe()
    {
        var tabs = TabSources();

        /* The blocked process reports read goes through the call that also reports a source read that filled its cap. */
        Assert.Equal(2, Matches(tabs, @"_dataService\.ReadRecentBlockedProcessReportsAsync\("));
        Assert.Equal(0, Matches(tabs, @"_dataService\.GetRecentBlockedProcessReportsAsync\("));
        Assert.Equal(2, Matches(tabs, @"_dataService\.GetBlockedProcessReportsDataStartAsync\("));
        Assert.Equal(2, Matches(tabs, @"_dataService\.GetRecentDeadlocksAsync\("));
        Assert.Equal(2, Matches(tabs, @"_dataService\.GetDeadlocksDataStartAsync\("));

        /* No probe is awaited bare: a probe that throws costs its banner and nothing after it. */
        Assert.DoesNotContain("await dataStartTask", ViewerFile("ViewerServerTab.Blocking.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheBanners_SitAboveTheirGrids_InTheExistingStyle()
    {
        var xaml = ViewerFile("ViewerServerTab.xaml");

        foreach (var name in new[] { "BlockedProcessReportsTruncationBanner", "DeadlocksTruncationBanner" })
        {
            var banner = Regex.Match(xaml, @"<TextBlock x:Name=""" + name + @"""[^>]*/>");
            Assert.True(banner.Success, name + " is not declared in ViewerServerTab.xaml");
            Assert.Contains("Visibility=\"Collapsed\"", banner.Value, StringComparison.Ordinal);
            Assert.Contains("Background=\"#22FFAA00\"", banner.Value, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheProbes_GoThroughTheSharedFloor_OverTheCollectorTheGridIsNamedFor()
    {
        var blocking = ViewerFile("ViewerDataService.Blocking.cs");
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"blocked_process_reports\")", blocking, StringComparison.Ordinal);
        /* The always-on DMV snapshots stand in for the XE collector, so the coverage is read from both. */
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"dmv_blocking_snapshots\")", blocking, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.GetForServerAsync(", blocking, StringComparison.Ordinal);

        var deadlock = ViewerFile("ViewerDataService.Deadlock.cs");
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"deadlocks\")", deadlock, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.GetForServerAsync(", deadlock, StringComparison.Ordinal);
    }

    /* WPF objects require STA; same shape as ViewerDataStartBannerTests. */
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
