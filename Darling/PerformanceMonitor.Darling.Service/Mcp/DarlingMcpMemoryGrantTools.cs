/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The memory-grant MCP tools — get_resource_semaphore, get_memory_grants — served over Darling's Postgres
/// store through <see cref="DarlingMemoryGrantReader"/> (STORED reads, no live monitored-server hit). The two
/// names are two lenses on the one collector table, so Darling hosts BOTH: get_resource_semaphore is the
/// Dashboard's semaphore/ceiling shape (per resource semaphore, with the workspace-memory target/max-target
/// ceiling); get_memory_grants is Lite's per-pool grant-detail shape. A client familiar with either SKU finds
/// its tool.
///
/// <para><b>#3541 A10 — the window is read, and the snapshot says when it was taken.</b> Each tool serves two
/// things under one <c>hours_back</c> / <c>as_of</c> window: <c>grants</c>, the NEWEST snapshot in the window
/// (stamped once as <c>captured_at</c>, with <c>age_seconds</c> against the window's end), and <c>window</c>,
/// the per-semaphore / per-pool aggregate over EVERY snapshot in it — peak waiters and when, peak grant, the
/// available-workspace floor, and the summed timeout / forced-grant deltas. Before this the tools accepted
/// <c>hours_back</c> and read only the latest row, so a three-hour-old grant storm was invisible behind a
/// calm snapshot while the parameter read as a window. Dropping the parameter was the other honest shape;
/// reading the window was chosen because "was there grant pressure in the last N hours" is the question an
/// agent brings to this tool, and the store answers it in one bounded scan.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpMemoryGrantTools
{
    [McpServerTool(Name = "get_resource_semaphore"), Description("Resource semaphore stats: granted/available memory vs target/max-target ceiling, waiters, timeout/forced pressure, window ends at as_of. TWO READS, per semaphore+pool: grants[] is the NEWEST snapshot (moment: captured_at/age_seconds); window[] aggregates EVERY snapshot (peak waiters+when, peak/min memory, summed timeout/forced deltas). Read window[] for 'was there pressure', grants[] for 'is there pressure now'. No rows: unavailable (not_collected first). sample_interval_seconds/interval_known null/false on a restart-marker or pre-column row; its zero deltas aren't 'no timeouts'. <<GUIDE>> Gets resource semaphore statistics showing granted vs available workspace memory against the target/max-target ceiling, waiter counts, and timeout/forced grant pressure indicators. High waiter counts or rising timeout/forced deltas indicate memory grant pressure affecting query performance. TWO READS UNDER ONE WINDOW: grants[] is the NEWEST snapshot in the window (one row per resource semaphore and pool), stamped once as captured_at with age_seconds against the window's end - it is a moment, not the window. window[] aggregates EVERY snapshot in the window per (resource_semaphore_id, pool_id): peak_waiter_count and peak_waiters_at (the most sessions ever seen waiting for a grant and when), peak_granted_memory_mb, min_available_memory_mb, and timeout_errors_in_window / forced_grants_in_window (the SUM of the per-interval deltas across the window). Each grants[] row also carries sample_interval_seconds, the measured seconds its two deltas accrued over; it is null with interval_known false when the row is a restart marker (no delta was knowable, so the zero deltas beside it are not 'no timeouts') or predates the column. interval_seconds is the interval these deltas cover, null when it can't be known - the same name the latch and spinlock tools use; sample_interval_seconds carries the same value and is kept for existing readers. " + McpToolGuideTopics.MemoryGrantWindowReadOrder)]
    public static async Task<string> GetResourceSemaphore(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24. window[] aggregates every snapshot in these hours; grants[] is the newest snapshot in them.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var windowStart = now.AddHours(-hours_back);
            var rows = await DarlingMemoryGrantReader.GetResourceSemaphoreLatestAsync(
                postgres, resolved.ServerId, windowStart, now, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_grant_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No memory grant data available.");

            /* Same window bounds as the latest read, so the window's last_snapshot_at IS captured_at and the
               two halves describe one span of the same rows (#3541 A10). */
            var window = await DarlingMemoryGrantReader.GetResourceSemaphoreWindowAsync(
                postgres, resolved.ServerId, windowStart, now, cancellationToken);

            var grants = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                resource_semaphore_id = r.ResourceSemaphoreId,
                pool_id = r.PoolId,
                target_memory_mb = r.TargetMemoryMb,
                max_target_memory_mb = r.MaxTargetMemoryMb,
                total_memory_mb = r.TotalMemoryMb,
                available_memory_mb = r.AvailableMemoryMb,
                granted_memory_mb = r.GrantedMemoryMb,
                used_memory_mb = r.UsedMemoryMb,
                grantee_count = r.GranteeCount,
                waiter_count = r.WaiterCount,
                timeout_error_count = r.TimeoutErrorCount,
                forced_grant_count = r.ForcedGrantCount,
                timeout_error_count_delta = r.TimeoutErrorCountDelta,
                forced_grant_count_delta = r.ForcedGrantCountDelta,
                /* #3540 (V128): the interval the deltas accrued over, the way the perfmon and file-I/O tools
                   hand it over. A stored 0 is the calculator's no-delta-knowable marker (a restart, not a
                   quiet semaphore) and is reported as null rather than 0 — 0 seconds is not a measurement;
                   a pre-V128 row that never recorded one is null too. interval_known states the one thing
                   both nulls have in common: the two *_delta zeros beside them are not "none this interval". */
                sample_interval_seconds = r.SampleIntervalSeconds is > 0 ? r.SampleIntervalSeconds : null,
                /* #3653 item 17: the same fact under the name get_latch_stats/get_spinlock_stats use, beside
                   the original sample_interval_seconds rather than in place of it. */
                interval_seconds = r.SampleIntervalSeconds is > 0 ? r.SampleIntervalSeconds : null,
                interval_known = r.SampleIntervalSeconds is > 0
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                window_start = windowStart.ToString("o"),
                window_end = now.ToString("o"),
                /* Every grants[] row shares this stamp by construction (the read is WHERE collection_time =
                   MAX(...) in the window); it is published once, as the snapshot's own clock. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = LatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, now),
                grants,
                window = window.Select(WindowShape)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_resource_semaphore", ex);
        }
    }

    [McpServerTool(Name = "get_memory_grants"), Description("Gets resource semaphore stats: granted vs available workspace memory per resource pool, waiters, timeout/forced grant deltas, over a window ending at as_of (default 1h). TWO READS: grants[] is the NEWEST snapshot in the window (a moment: captured_at/age_seconds), one row per pool; window[] aggregates EVERY snapshot in it, per pool (peak waiters+when, peak/min memory, summed timeout/forced deltas). Read window[] for 'was there pressure', grants[] for 'is there pressure now'. No rows: unavailable (or not_collected first). <<GUIDE>> Gets resource semaphore statistics showing granted vs available workspace memory per resource pool, waiter counts, and timeout/forced grant deltas. High waiter counts or rising timeout deltas indicate memory grant pressure affecting query performance. TWO READS UNDER ONE WINDOW: grants[] is the NEWEST snapshot in the window (one row per pool, summed across its semaphores), stamped once as captured_at with age_seconds against the window's end - it is a moment, not the window. window[] aggregates EVERY snapshot in the window per pool: peak_waiter_count and peak_waiters_at (the most sessions ever seen waiting on the pool at one instant and when), peak_granted_memory_mb, min_available_memory_mb, and timeout_errors_in_window / forced_grants_in_window (the SUM of the per-interval deltas across the window). " + McpToolGuideTopics.MemoryGrantWindowReadOrder)]
    public static async Task<string> GetMemoryGrants(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1. window[] aggregates every snapshot in these hours; grants[] is the newest snapshot in them.")] int hours_back = 1,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var windowStart = now.AddHours(-hours_back);
            var rows = await DarlingMemoryGrantReader.GetMemoryGrantsLatestAsync(
                postgres, resolved.ServerId, windowStart, now, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_grant_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No memory grant data available.");

            var window = await DarlingMemoryGrantReader.GetMemoryGrantsWindowAsync(
                postgres, resolved.ServerId, windowStart, now, cancellationToken);

            var grants = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                pool_id = r.PoolId,
                available_memory_mb = Math.Round(r.AvailableMemoryMb, 2),
                granted_memory_mb = Math.Round(r.GrantedMemoryMb, 2),
                used_memory_mb = Math.Round(r.UsedMemoryMb, 2),
                grantee_count = r.GranteeCount,
                waiter_count = r.WaiterCount,
                timeout_error_count_delta = r.TimeoutErrorCountDelta,
                forced_grant_count_delta = r.ForcedGrantCountDelta
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                window_start = windowStart.ToString("o"),
                window_end = now.ToString("o"),
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = LatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, now),
                grants,
                window = window.Select(WindowShape)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_memory_grants", ex);
        }
    }

    /// <summary>
    /// The window half's payload shape, shared by both lenses so the same key set describes a semaphore's
    /// window and a pool's window (the pool lens carries a null <c>resource_semaphore_id</c>, which
    /// <see cref="McpHelpers.JsonOptions"/> writes rather than drops — the key is present on both so a
    /// caller can read one shape).
    /// </summary>
    private static object WindowShape(DarlingMemoryGrantReader.MemoryGrantWindowRow w) => new
    {
        resource_semaphore_id = w.ResourceSemaphoreId,
        pool_id = w.PoolId,
        snapshots_in_window = w.SnapshotsInWindow,
        first_snapshot_at = w.FirstSnapshotAt.ToString("o"),
        last_snapshot_at = w.LastSnapshotAt.ToString("o"),
        peak_waiter_count = w.PeakWaiterCount,
        peak_waiters_at = w.PeakWaitersAt.ToString("o"),
        peak_granted_memory_mb = Math.Round(w.PeakGrantedMemoryMb, 2),
        min_available_memory_mb = Math.Round(w.MinAvailableMemoryMb, 2),
        timeout_errors_in_window = w.TimeoutErrorsInWindow,
        forced_grants_in_window = w.ForcedGrantsInWindow
    };

    [McpServerTool(Name = "get_memory_pressure_events"), Description("Gets memory pressure notification events (memory_notification: RESOURCE_MEMPHYSICAL_LOW, RESOURCE_MEMVIRTUAL_LOW, RESOURCE_MEMPHYSICAL_HIGH, RESOURCE_MEM_STEADY) from RING_BUFFER_RESOURCE_MONITOR (sp_pressuredetector's source), over a sample_time window ending at as_of (default 24h), oldest first. memory_indicators_process/_system: 0-1 normal, 2 medium (Resource Monitor trims caches, cuts grants), 3+ severe (aggressive eviction); process is this instance, system is the whole box. Empty: none in the window. not_collected: Azure SQL DB (no ring buffer) or a non-SQL Server target.\n<<GUIDE>> Gets memory pressure notifications from the RING_BUFFER_RESOURCE_MONITOR ring buffer (same source as sp_pressuredetector). Returns RESOURCE_MEMPHYSICAL_LOW, RESOURCE_MEMVIRTUAL_LOW, RESOURCE_MEMPHYSICAL_HIGH, and RESOURCE_MEM_STEADY notifications with indicator values.\n\nIndicator scale (applies to both memory_indicators_process and memory_indicators_system):\n  0-1 = normal, no pressure\n  2   = medium pressure (SQL Server's Resource Monitor starts trimming caches and reducing grants)\n  3+  = severe pressure (aggressive buffer pool / plan cache eviction)\n\nmemory_indicators_process = SQL Server process itself is under memory pressure (workload-induced).\nmemory_indicators_system  = Windows is signaling low memory system-wide (could be other tenants on the box).\n\nNot available on Azure SQL DB (ring buffer not exposed).")]
    public static async Task<string> GetMemoryPressureEvents(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingMemoryGrantReader.GetMemoryPressureEventsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_pressure_events", cancellationToken)
                    ?? McpHelpers.Status("empty", "No memory pressure events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                events = rows.Select(r => new
                {
                    sample_time = r.SampleTime.ToString("o"),
                    memory_notification = r.MemoryNotification,
                    memory_indicators_process = r.MemoryIndicatorsProcess,
                    memory_indicators_system = r.MemoryIndicatorsSystem
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_memory_pressure_events", ex);
        }
    }
}
