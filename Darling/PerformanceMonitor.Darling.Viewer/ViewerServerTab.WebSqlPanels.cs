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

    /// <summary>Fills the Extension Install Census under the Extensions grid: one row per extension created anywhere on the server,
    /// from the same shared read as the web's <c>install_census</c>. The scalars-only row a server with no stored rows produces
    /// carries the denominators and is left out of the grid, as in the web.</summary>
    private async Task LoadPgExtensionInstallCensusAsync(DateTime startUtc, DateTime endUtc)
    {
        var census = await _dataService.GetPgExtensionInstallCensusAsync(_server.ServerId, startUtc, endUtc);
        var rows = census.Where(r => r.ExtensionName is not null).Select(PgExtensionCensusGridRow.From).ToList();
        PgExtensionCensusGrid.ItemsSource = rows;
        PgExtensionCensusNote.Text = rows.Count == 0
            ? "Extension Install Census: nothing is created on this server beyond what PostgreSQL installs itself, or the inventory has not been collected yet. "
              + "This census counts only extensions somebody created; the available-but-absent ones are the actionable rows in the grid above."
            : $"Extension Install Census: {rows.Count} extension(s) created on this server, one row each, counted across all {census[0].DatabasesTotal:N0} database(s). "
              + "No row limit touches it.";
    }
}
