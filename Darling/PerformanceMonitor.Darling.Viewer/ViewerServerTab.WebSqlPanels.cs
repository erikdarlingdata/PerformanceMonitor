/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One Resource Semaphore Pressure grid row (#6388): a <see cref="MemoryGrantWindowRow"/> with its instants
/// shown in the display zone. The raw instants stay as the sort keys.</summary>
public sealed record ResourceSemaphorePressureGridRow(
    short? ResourceSemaphoreId, int PoolId, long Snapshots, long PeakWaiters, DateTime PeakWaitersAt, double PeakGrantedMb,
    DateTime PeakGrantedAt, double MinAvailableMb, long TimeoutsInWindow, long ForcedGrantsInWindow, DateTime FirstSnapshotAt,
    DateTime LastSnapshotAt)
{
    /// <summary>Null on a peak of 0: the shared read's DISTINCT ON picks the newest snapshot when nothing waited, an arbitrary row
    /// and not an event, so the web nulls it too (<c>peak_waiters_at</c>, <c>peak_granted_at</c>). The grid shows a dash.</summary>
    public string? PeakWaitersAtLocal => PeakWaiters > 0 ? HistoryTime.CollectionLocal(PeakWaitersAt) : null;

    public string? PeakGrantedAtLocal => PeakGrantedMb > 0 ? HistoryTime.CollectionLocal(PeakGrantedAt) : null;

    public string FirstSnapshotLocal => HistoryTime.CollectionLocal(FirstSnapshotAt);

    public string LastSnapshotLocal => HistoryTime.CollectionLocal(LastSnapshotAt);

    public static ResourceSemaphorePressureGridRow From(MemoryGrantWindowRow row) => new(
        row.ResourceSemaphoreId, row.PoolId, row.SnapshotsInWindow, row.PeakWaiterCount, row.PeakWaitersAt, row.PeakGrantedMemoryMb,
        row.PeakGrantedAt, row.MinAvailableMemoryMb, row.TimeoutErrorsInWindow, row.ForcedGrantsInWindow, row.FirstSnapshotAt,
        row.LastSnapshotAt);
}

/// <summary>One Resource Semaphore (newest snapshot) grid row (#6388): a <see cref="ResourceSemaphoreRow"/> whose two delta cells
/// are null, and show a dash, when the stored interval is the 0 marker (a restart, a counter reset or a first sighting), the
/// way the latch and spinlock snapshot grids do. The web's <c>get_resource_semaphore</c> nulls its interval and says
/// <c>interval_known: false</c> on the same marker; the fabricated (0, 0) must not read as "no timeouts this interval".</summary>
public sealed record ResourceSemaphoreSnapshotGridRow(ResourceSemaphoreRow Row)
{
    public int PoolId => Row.PoolId;

    public short ResourceSemaphoreId => Row.ResourceSemaphoreId;

    public double TargetMemoryMb => Row.TargetMemoryMb;

    public double MaxTargetMemoryMb => Row.MaxTargetMemoryMb;

    public double TotalMemoryMb => Row.TotalMemoryMb;

    public double AvailableMemoryMb => Row.AvailableMemoryMb;

    public double GrantedMemoryMb => Row.GrantedMemoryMb;

    public double UsedMemoryMb => Row.UsedMemoryMb;

    public int GranteeCount => Row.GranteeCount;

    public int WaiterCount => Row.WaiterCount;

    public long TimeoutErrorCount => Row.TimeoutErrorCount;

    public long ForcedGrantCount => Row.ForcedGrantCount;

    public long? TimeoutErrorCountDelta => DeltaSeriesShaping.ReadableDelta(Row.TimeoutErrorCountDelta, Row.SampleIntervalSeconds);

    public long? ForcedGrantCountDelta => DeltaSeriesShaping.ReadableDelta(Row.ForcedGrantCountDelta, Row.SampleIntervalSeconds);

    public int? SampleIntervalSeconds => Row.SampleIntervalSeconds;

    /// <summary>The Interval (sec) cell - see <see cref="DeltaSeriesShaping.IntervalDisplay"/>.</summary>
    public string IntervalDisplay => DeltaSeriesShaping.IntervalDisplay(Row.SampleIntervalSeconds);
}

/// <summary>One Extension Install Census grid row (#6388): an extension somebody created on the server, with the databases it is
/// created in (at the server's default version or older), how many databases reported, and the server's database count.</summary>
public sealed record PgExtensionCensusGridRow(
    string ExtensionName, long DatabasesCreated, long DatabasesInstalled, long DatabasesOutdated, long DatabasesReporting, long DatabasesTotal)
{
    public static PgExtensionCensusGridRow From(DarlingPgExtensionAvailabilityReader.PgExtensionInstallCensusRow row) => new(
        row.ExtensionName ?? "", row.DatabasesInstalled + row.DatabasesOutdated, row.DatabasesInstalled, row.DatabasesOutdated,
        row.DatabasesReporting, row.DatabasesTotal);
}

/// <summary>
/// #6388: the panels the web had and the Viewer lacked. Resource Semaphore Pressure and Resource Semaphore sit on a new
/// Memory &gt; Resource Semaphore sub-tab (the web puts them on its Memory Grants tab), Plan Cache by Type under the Plan Cache
/// chart, and Sessions by Application under the Session Stats chart. Each grid shows the rows the shared store read returns
/// (<see cref="ViewerDataService"/>'s <c>WebSqlPanels</c> reads), over the toolbar's window where the web's panel has one.
/// An empty grid says so in the neighbours' words, and the engine-gap note replaces them where the collector does not run on
/// this server.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>Loads the Resource Semaphore sub-tab: the window aggregate and the newest snapshot, read concurrently.</summary>
    private async Task LoadResourceSemaphoreAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        using var readFanOut = ViewerReadFanOut.Of(2);
        var windowTask = _dataService.GetResourceSemaphoreWindowAsync(_server.ServerId, startUtc, endUtc);
        var latestTask = _dataService.GetResourceSemaphoreLatestAsync(_server.ServerId, startUtc, endUtc);
        await Task.WhenAll(windowTask, latestTask);

        /* The two reads are done: release so the engine-gap probes below, a store read each, are not priced at this width. */
        readFanOut.Release();

        var window = windowTask.Result.ConvertAll(ResourceSemaphorePressureGridRow.From);
        var latest = latestTask.Result;
        ResourceSemaphorePressureGrid.ItemsSource = window;
        ResourceSemaphoreGrid.ItemsSource = latest.ConvertAll(r => new ResourceSemaphoreSnapshotGridRow(r));
        ResourceSemaphoreCollectedText.Text = HistoryTime.SnapshotCollected(latest.Count > 0 ? latest[0].CollectionTime : null, "--");

        await ShowEngineGapAsync(ResourceSemaphorePressureNoDataMessage, "memory_grant_stats", window.Count, keepsOwnEmptyText: true);
        await ShowEngineGapAsync(ResourceSemaphoreNoDataMessage, "memory_grant_stats", latest.Count, keepsOwnEmptyText: true);
    }

    /// <summary>Fills the Plan Cache by Type grid from the newest snapshot in the window.</summary>
    private async Task RenderPlanCacheByTypeAsync(List<PlanCacheGroupRow> rows)
    {
        PlanCacheByTypeGrid.ItemsSource = rows;
        await ShowEngineGapAsync(PlanCacheByTypeNoDataMessage, "plan_cache_stats", rows.Count, keepsOwnEmptyText: true);
    }

    /// <summary>Fills the Sessions by Application grid from the newest per-application snapshot.</summary>
    private async Task RenderSessionsByApplicationAsync(List<SessionStatRow> rows)
    {
        SessionsByApplicationGrid.ItemsSource = rows;
        /* The read has no window: it is the newest snapshot of all, so the panel says which instant that is (#3637). */
        SessionsByApplicationCollectedText.Text = HistoryTime.SnapshotCollected(rows.Count > 0 ? rows[0].CollectionTime : null, "--");
        await ShowEngineGapAsync(SessionsByApplicationNoDataMessage, "session_stats", rows.Count, keepsOwnEmptyText: true);
    }
}
