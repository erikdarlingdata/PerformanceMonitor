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
/// passes its own interactive command deadline; the rows come back as the shared read's own records, which the grids bind
/// to directly.
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
}
