/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #6388: the SQL Server panels the web had and the Viewer lacked, each fed by the SAME store read the web endpoint's
/// MCP tool calls (the read moved to <c>PerformanceMonitor.Darling.Storage</c> so both hosts run one text). The Viewer
/// passes its own interactive command deadline, except for the Extension Install Census, whose reader pins the MCP read
/// deadline itself; the rows come back as the shared read's own records, which the grids bind to directly or wrap for display
/// (the Resource Semaphore grids null what the web nulls).
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Resource Semaphore: the newest snapshot in the window, per (pool, semaphore) -
    /// <c>get_resource_semaphore</c>'s <c>grants[]</c>.</summary>
    public Task<List<ResourceSemaphoreRow>> GetResourceSemaphoreLatestAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingResourceSemaphoreReader.GetLatestAsync(
            _dataSource, serverId, startUtc, endUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>Resource Semaphore Pressure: every snapshot in the window aggregated per (pool, semaphore) -
    /// <c>get_resource_semaphore</c>'s <c>window[]</c>.</summary>
    public Task<List<MemoryGrantWindowRow>> GetResourceSemaphoreWindowAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingResourceSemaphoreReader.GetWindowAsync(
            _dataSource, serverId, startUtc, endUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>Plan Cache by Type: every (cacheobjtype, objtype) group at the newest collection in the window -
    /// <c>get_plan_cache_bloat</c>'s <c>cache_types</c>.</summary>
    public Task<List<PlanCacheGroupRow>> GetPlanCacheByTypeAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingPlanCacheBloatReader.GetAsync(
            _dataSource, serverId, startUtc, endUtc, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>Sessions by Application: every program_name at the newest per-application session snapshot, most
    /// connections first - <c>get_session_stats</c>'s <c>applications</c>. A moment, not the window.</summary>
    public Task<List<SessionStatRow>> GetSessionsByApplicationAsync(
        int serverId, CancellationToken cancellationToken = default) =>
        DarlingSessionStatsReader.GetLatestAsync(
            _dataSource, serverId, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);

    /// <summary>Table &amp; Index Sizes: the largest tables at the newest daily index snapshot with 7-day and 30-day growth -
    /// <c>get_table_index_sizes</c>'s <c>tables</c>. A moment, not the window. Asks for one row past the page so the grid can say
    /// the list was cut.</summary>
    public Task<List<TableSizeGrowthRow>> GetTableSizesAsync(
        int serverId, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var now = DateTime.UtcNow;
        return DarlingTableSizeGrowthReader.GetAsync(
            _dataSource, serverId, now.AddDays(-7), now.AddDays(-30), DarlingTableSizeGrowthReader.TopTables + 1,
            DatabaseFilter.Of(databaseNames), ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
    }

    /// <summary>Extension Install Census (PostgreSQL targets): one row per extension somebody created, over the population
    /// no row limit touches - <c>get_pg_extensions</c>'s <c>install_census</c>, server-wide.</summary>
    public Task<List<DarlingPgExtensionAvailabilityReader.PgExtensionInstallCensusRow>> GetPgExtensionInstallCensusAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingPgExtensionAvailabilityReader.GetInstallCensusAsync(_dataSource, serverId, startUtc, endUtc, null, cancellationToken);
}
