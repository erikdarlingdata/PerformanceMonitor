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
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One Resource Semaphore Pressure grid row (#6388): a <see cref="MemoryGrantWindowRow"/> with its instants
/// shown in the display zone. The raw instants stay as the sort keys.</summary>
public sealed record ResourceSemaphorePressureGridRow(
    short? ResourceSemaphoreId, int PoolId, long Snapshots, long PeakWaiters, DateTime PeakWaitersAt, double PeakGrantedMb,
    DateTime PeakGrantedAt, double MinAvailableMb, long TimeoutsInWindow, long ForcedGrantsInWindow, DateTime FirstSnapshotAt,
    DateTime LastSnapshotAt)
{
    public string PeakWaitersAtLocal => HistoryTime.CollectionLocal(PeakWaitersAt);

    public string PeakGrantedAtLocal => HistoryTime.CollectionLocal(PeakGrantedAt);

    public string FirstSnapshotLocal => HistoryTime.CollectionLocal(FirstSnapshotAt);

    public string LastSnapshotLocal => HistoryTime.CollectionLocal(LastSnapshotAt);

    public static ResourceSemaphorePressureGridRow From(MemoryGrantWindowRow row) => new(
        row.ResourceSemaphoreId, row.PoolId, row.SnapshotsInWindow, row.PeakWaiterCount, row.PeakWaitersAt, row.PeakGrantedMemoryMb,
        row.PeakGrantedAt, row.MinAvailableMemoryMb, row.TimeoutErrorsInWindow, row.ForcedGrantsInWindow, row.FirstSnapshotAt,
        row.LastSnapshotAt);
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

        var window = windowTask.Result.ConvertAll(ResourceSemaphorePressureGridRow.From);
        var latest = latestTask.Result;
        ResourceSemaphorePressureGrid.ItemsSource = window;
        ResourceSemaphoreGrid.ItemsSource = latest;
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
        await ShowEngineGapAsync(SessionsByApplicationNoDataMessage, "session_stats", rows.Count, keepsOwnEmptyText: true);
    }
}
