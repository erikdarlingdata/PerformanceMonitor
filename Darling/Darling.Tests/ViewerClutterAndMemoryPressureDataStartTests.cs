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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// What the Query Store Clutter panel and the Memory Pressure Events chart of the desktop viewer say about where their data starts
/// (#4966). The panel's read cost and plan churn are aggregates over the window, so the coverage rule applies: a range that reaches
/// before the server's coverage names where it starts, and a covered range with a quiet start names nothing. The chart filters on
/// each event's own time, so the event-time rule applies too: the note names the earlier of the coverage and the earliest event the
/// chart draws. A window of an hour or less starts no probe, and a probe that fails costs the note and nothing else.
/// </summary>
/* The banner's time text reads the process-wide display mode, which these tests set (restored in Dispose); one shared
   collection serializes it with every other class that flips the viewer's time statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerClutterAndMemoryPressureDataStartTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime RangeStart = new(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc);

    /* A store nothing listens on: a probe that starts a query against it fails, one that does not returns at once. */
    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=nobody;Database=nothing;Timeout=3;Pooling=false";

    private static DateTime At(int days, int hours = 0, int minutes = 0) => RangeStart.AddDays(days).AddHours(hours).AddMinutes(minutes);

    // ── Query Store Clutter ──

    /* The banner the tab's own step (ShowQueryStoreClutterDataStartAsync) raises for the probe's answer on the range that starts at
       RangeStart, read off the control; null when it is hidden. Seeded visible, so a no-op cannot pass as a hidden banner. */
    private static string? ClutterBannerFor(Task<DateTime?> probe) =>
        ReadBanner(banner => ViewerServerTab.ShowQueryStoreClutterDataStartAsync(banner, probe, RangeStart).GetAwaiter().GetResult());

    /* A server added 2 days before the range's end: the range reaches before its coverage, and the note says where the data starts. */
    [Fact]
    public void Clutter_ARangePastTheCoverage_NamesWhereTheCoverageStarts()
    {
        Assert.Equal("Showing since 2026-09-14 06:30:15", ClutterBannerFor(Task.FromResult<DateTime?>(At(4, 6, 30).AddSeconds(15))));
    }

    /* A quiet start: the store covered the whole range (coverage 20 days before it), so no note, whenever the first row came. A probe
       with no answer (the window holds no row and no logged run) names nothing either. */
    [Fact]
    public void Clutter_AQuietStart_RaisesNoNotice()
    {
        Assert.Null(ClutterBannerFor(Task.FromResult<DateTime?>(At(-20))));
        Assert.Null(ClutterBannerFor(Task.FromResult<DateTime?>(null)));
        /* A first collection that lands inside the slack after the window starts is absorbed. */
        Assert.Null(ClutterBannerFor(Task.FromResult<DateTime?>(At(0, 1, 20))));
    }

    /* A probe that throws costs this note and nothing else: the step answers a hidden banner (seeded visible) instead of throwing. */
    [Fact]
    public void Clutter_AProbeThatThrows_HidesTheBanner_InsteadOfThrowing()
    {
        Assert.Null(ClutterBannerFor(Task.FromException<DateTime?>(new InvalidOperationException("the store went away"))));
    }

    /* A window of an hour or less starts no probe: it returns at once against a store nothing listens on, and a window one minute
       over the 90-minute slack starts the query, which fails there. */
    [Theory]
    [InlineData(60)]
    [InlineData(90)]
    public async Task Clutter_AWindowNoLongerThanTheSlack_MakesNoProbeCall(int minutes)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);

        Assert.Null(await viewer.GetQueryStoreClutterDataStartAsync(1, RangeStart, RangeStart.AddMinutes(minutes), ct));
    }

    [Fact]
    public async Task Clutter_AWindowOneMinuteOverTheSlack_StartsItsProbe()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);

        await Assert.ThrowsAnyAsync<Exception>(() => viewer.GetQueryStoreClutterDataStartAsync(1, RangeStart, RangeStart.AddMinutes(91), ct));
    }

    // ── Memory Pressure Events ──

    private static MemoryPressureEventRow Event(DateTime sampleTimeUtc, int process = 2, int system = 0) => new(sampleTimeUtc, "RESOURCE_MEMPHYSICAL_LOW", process, system);

    /* The banner the tab's own step (ShowMemoryPressureEventsDataStartAsync) raises for the probe's answer and the events the read
       returned, on the range that starts at RangeStart; null when it is hidden. */
    private static string? MemoryBannerFor(Task<DateTime?> probe, params MemoryPressureEventRow[] rows) =>
        ReadBanner(banner => ViewerServerTab.ShowMemoryPressureEventsDataStartAsync(banner, probe, RangeStart, rows).GetAwaiter().GetResult());

    private static Task<DateTime?> Answer(DateTime? coverageStartUtc) => Task.FromResult(coverageStartUtc);

    /* Events start inside the range, after the coverage began: the note reads back the coverage start. */
    [Fact]
    public void Memory_ARangePastTheCoverage_NamesWhereTheCoverageStarts()
    {
        Assert.Equal("Showing since 2026-09-13 00:00:00", MemoryBannerFor(Answer(At(3)), Event(At(3, 4)), Event(At(5))));
    }

    /* A quiet start: the store covered the whole range (coverage 20 days before it) and the first event came 5 hours in. The chart
       draws nothing before it, and the note stays down. */
    [Fact]
    public void Memory_AQuietStart_RaisesNoNotice()
    {
        Assert.Null(MemoryBannerFor(Answer(At(-20)), Event(At(0, 5)), Event(At(2))));
        Assert.Null(MemoryBannerFor(Answer(At(-20))));
    }

    /* An event carries its own time, and a server's first collection stores the ring buffer's history, so the chart can draw an event
       from before the coverage the probe found: the note names that event, never a time later than a bar on the chart. */
    [Fact]
    public void Memory_AnEventStampedBeforeTheServersFirstCollection_IsTheTimeNamed()
    {
        Assert.Equal("Showing since 2026-09-12 18:20:05", MemoryBannerFor(Answer(At(3)), Event(At(2, 18, 20).AddSeconds(5)), Event(At(3, 4))));
        /* The same event under a range that ends before the coverage began (no answer): the chart still draws it. */
        Assert.Equal("Showing since 2026-09-12 18:20:05", MemoryBannerFor(Answer(null), Event(At(2, 18, 20).AddSeconds(5))));
    }

    /* A sample the chart does not draw (both indicators under 2) is not an event: it names no time. */
    [Fact]
    public void Memory_ASampleTheChartDoesNotDraw_NamesNoTime()
    {
        var rows = new[] { Event(At(-1, 12), process: 1, system: 0), Event(At(0, 3), process: 0, system: 1), Event(At(4), process: 2, system: 0) };

        Assert.Equal(new[] { At(4) }, ViewerServerTab.PressureRowsDrawn(rows).Select(r => r.SampleTime).ToArray());
        /* Coverage starts 3 days in; the undrawn samples came before it and are not named. */
        Assert.Equal("Showing since 2026-09-13 00:00:00", MemoryBannerFor(Answer(At(3)), rows));
        /* A system indicator alone draws the event. */
        Assert.Equal("Showing since 2026-09-11 08:00:00", MemoryBannerFor(Answer(At(3)), Event(At(1, 8), process: 0, system: 2)));
    }

    /* A probe that throws costs the note and not the chart: the step answers a hidden banner instead of throwing, and the rows the
       chart draws are unchanged by it. A failed probe names nothing, and the chart's own first event does not stand in for coverage. */
    [Fact]
    public void Memory_AProbeThatThrows_HidesTheBanner_AndTheRowsStillDraw()
    {
        var rows = new[] { Event(At(2)), Event(At(3)) };

        Assert.Null(MemoryBannerFor(Task.FromException<DateTime?>(new InvalidOperationException("the store went away")), rows));
        Assert.Equal(2, ViewerServerTab.PressureRowsDrawn(rows).Count);
    }

    /* A window of an hour or less starts no probe, and one minute over the slack starts it. */
    [Theory]
    [InlineData(60)]
    [InlineData(90)]
    public async Task Memory_AWindowNoLongerThanTheSlack_MakesNoProbeCall(int minutes)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);

        Assert.Null(await viewer.GetMemoryPressureEventsDataStartAsync(1, RangeStart, RangeStart.AddMinutes(minutes), ct));
    }

    [Fact]
    public async Task Memory_AWindowOneMinuteOverTheSlack_StartsItsProbe()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var viewer = new ViewerDataService(UnreachableStore);

        await Assert.ThrowsAnyAsync<Exception>(() => viewer.GetMemoryPressureEventsDataStartAsync(1, RangeStart, RangeStart.AddMinutes(91), ct));
    }

    // ── The named source ──

    /* The source probes the payload's own stamp, over the collector's schedule edge, and never walks to the table's oldest row: the
       table is sparse, and a walk would turn a quiet start into a false note. */
    [Fact]
    public void TheNamedSource_ProbesSampleTime_OverTheSchedulesEdge()
    {
        var source = DataWindowFloor.Source.ForMemoryPressureEvents();

        Assert.Equal("memory_pressure_events", source.Relation);
        Assert.Equal("sample_time", source.TimeColumn);
        Assert.Equal("memory_pressure_events", source.CollectorName);
        Assert.False(source.EndExclusive);
        Assert.Null(source.LowerBoundUtc);
        Assert.Equal(CollectorScheduleDefaults.All["memory_pressure_events"].RetentionDays, source.RetentionDefaultDays);

        var sql = DataWindowFloor.FloorSql([source], DataWindowFloor.Scope.ServerId);
        Assert.Contains("FROM collect.memory_pressure_events AS f", sql, StringComparison.Ordinal);
        Assert.Contains("f.sample_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST($3 - make_interval(days =>", sql, StringComparison.Ordinal);
        Assert.Contains("lower(o.collector_name) = 'memory_pressure_events'", sql, StringComparison.Ordinal);
        Assert.Contains("c.collector_name = 'memory_pressure_events'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AS h WHERE", sql, StringComparison.Ordinal);
    }

    // ── The banner, read off the real control ──

    private static string? ReadBanner(Action<TextBlock> raise)
    {
        string? text = null;
        OnStaThread(() =>
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            var banner = new TextBlock { Visibility = Visibility.Visible, Text = "stale" };

            raise(banner);

            text = banner.Visibility == Visibility.Visible ? banner.Text : null;
        });
        return text;
    }

    /* WPF objects require STA; same shape as ViewerQueryHeatmapDataStartTests. */
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
