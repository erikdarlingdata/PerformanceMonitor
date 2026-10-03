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
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The "Showing since" notice on a range of an hour or less (#4966). The 90-minute slack belongs to the coverage probe alone, where it
/// absorbs a first collection that lands a little after the window starts: a window no longer than the slack can never get a coverage
/// note, so its probe starts no query. A read that filled its row cap dropped rows for certain, so its verdict has no slack: on a short
/// range the banner shows whenever the oldest row shown is later than the window's start.
/// </summary>
/* The banner's time text reads the process-wide display mode, which these tests set (restored in Dispose); one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerShortWindowDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    private const int Cap = ViewerDataService.PlanCorrectionsRowCap;

    /* A store nothing listens on: a probe that starts a query against it fails, one that does not returns at once. */
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    private static DateTime At(int minutes) => RangeStart.AddMinutes(minutes);

    /* A page of `count` rows 10 seconds apart from `oldest`: 200 of them span 33 minutes, so a page that starts inside the first
       half hour of a one-hour range stays inside it. */
    private static IEnumerable<DateTime> Page(DateTime oldest, int count) => Enumerable.Range(0, count).Select(i => oldest.AddSeconds(10 * i));

    // ── A capped verdict gets no slack ──

    /* A capped read on a one-hour range whose oldest row came 20 minutes in: the grid reaches back no further, so the banner names that
       row. Before, the slack hid it, and on a range this short no row could ever sit past it. */
    [Fact]
    public void ACappedRead_OnAOneHourRange_NamesItsOldestRow_WhenItIsLaterThanTheStart()
    {
        Assert.Equal("Showing since 2026-09-01 00:20:00", BannerFor(coverageStartUtc: null, Page(At(20), Cap), Cap));
        /* The same rows with a coverage answer that reaches the range start: the cap is what names the row. */
        Assert.Equal("Showing since 2026-09-01 00:20:00", BannerFor(At(-600), Page(At(20), Cap), Cap));
    }

    /* A capped read whose oldest row is at the window's start reaches the start: no banner. */
    [Fact]
    public void ACappedRead_WhoseOldestRowIsAtTheStart_ShowsNoBanner()
    {
        Assert.Null(BannerFor(coverageStartUtc: null, Page(At(0), Cap), Cap));
        Assert.Null(BannerFor(At(-600), Page(At(0), Cap), Cap));
    }

    /* The note names its time to the second. A capped read has no slack, so a read whose oldest row came 42 seconds into the range's
       first minute shows a banner, and a note in whole minutes would print the range's own start minute for it (10:15 for a range from
       10:15:00), which reads as if the grid started at the range's start. */
    [Fact]
    public void ACappedRead_WhoseOldestRowIsSecondsPastTheStart_NamesTheSecond_NotTheStartsMinute()
    {
        var rangeStart = new DateTime(2026, 9, 1, 10, 15, 0, DateTimeKind.Utc);
        var oldest = rangeStart.AddSeconds(42);

        Assert.Equal("Showing since 2026-09-01 10:15:42", BannerFor(coverageStartUtc: null, Page(oldest, Cap), Cap, rangeStart));
        /* Under a coverage answer that reaches the start, the cap still names the row, to the second. */
        Assert.Equal("Showing since 2026-09-01 10:15:42", BannerFor(rangeStart.AddHours(-10), Page(oldest, Cap), Cap, rangeStart));
    }

    /* A read under its cap keeps its verdict, and the slack stays on the coverage probe: a first collection 85 minutes in is
       absorbed, one 95 minutes in is named. */
    [Fact]
    public void TheSlack_StaysOnTheCoverageProbe()
    {
        Assert.Null(BannerFor(At(85), Page(At(86), 3), Cap));
        Assert.Equal("Showing since 2026-09-01 01:35:00", BannerFor(At(95), Page(At(96), 3), Cap));
        /* A page one row under the cap is not capped. */
        Assert.Null(BannerFor(coverageStartUtc: null, Page(At(20), Cap - 1), Cap));
    }

    // ── A window no longer than the slack starts no probe ──

    /* Every window of an hour up to the slack itself returns at once, with no query against a store nothing listens on; one minute over
       the slack starts the query, which fails there. */
    [Theory]
    [InlineData(60)]
    [InlineData(90)]
    public async Task AWindowNoLongerThanTheSlack_StartsNoProbe_AgainstNoStore(int minutes)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var source = NpgsqlDataSource.Create(UnreachableStore);

        var answer = await DataWindowFloor.GetForServerAsync(
            source, DataWindowFloor.Source.ForCollectorTable("plan_correction"), 1, RangeStart, RangeStart.AddMinutes(minutes), 3, ct);

        Assert.Null(answer);
    }

    [Fact]
    public async Task AWindowOneMinuteOverTheSlack_StartsItsProbe_AgainstNoStore()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var source = NpgsqlDataSource.Create(UnreachableStore);

        await Assert.ThrowsAnyAsync<Exception>(() => DataWindowFloor.GetForServerAsync(
            source, DataWindowFloor.Source.ForCollectorTable("plan_correction"), 1, RangeStart, RangeStart.AddMinutes(91), 3, ct));
    }

    /* The tab's own probes go through the shared form, so each skips a one-hour window; the Regressions probe asks about the baseline
       window, 7 days before the range, so a one-hour range still starts its probe. */
    [Fact]
    public async Task TheTabsProbes_SkipAOneHourWindow_AgainstNoStore()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);
        var end = RangeStart.AddHours(1);

        Assert.Null(await viewer.GetPlanCorrectionsDataStartAsync(1, RangeStart, end, ct));
        Assert.Null(await viewer.GetQueryHeatmapDataStartAsync(1, RangeStart, end, ct));
        Assert.Null(await viewer.GetBlockedProcessReportsDataStartAsync(1, RangeStart, end, ct));
        Assert.Null(await viewer.GetDeadlocksDataStartAsync(1, RangeStart, end, ct));
        Assert.Null(await viewer.GetLongQueriesDataStartAsync(1, RangeStart, end, ct));
        Assert.Null(await viewer.GetSystemHealthEventsDataStartAsync(1, RangeStart, end, ct));
        await Assert.ThrowsAnyAsync<Exception>(() => viewer.GetQueryStoreRegressionsDataStartAsync(1, RangeStart, end, ct));
    }

    // ── The banner, read off the real control ──

    /* The banner raised for the probe's answer and the rows the grid shows on the range that starts at startUtc (RangeStart when
       omitted), read off the control; null when it is hidden. Seeded visible, so a no-op cannot pass as a hidden banner. */
    private static string? BannerFor(DateTime? coverageStartUtc, IEnumerable<DateTime> shownTimes, int? rowCap, DateTime? startUtc = null)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

            ViewerServerTab.ShowEventDataStartAsync(
                banner, Task.FromResult(coverageStartUtc), "Short Window", startUtc ?? RangeStart, shownTimes.Select(t => (DateTime?)t), rowCap).GetAwaiter().GetResult();

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    /* WPF objects require STA; same shape as ViewerPlanCorrectionsDataStartTests. */
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
