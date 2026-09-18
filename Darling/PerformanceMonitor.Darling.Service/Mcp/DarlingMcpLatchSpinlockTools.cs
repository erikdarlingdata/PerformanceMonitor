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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The latch / spinlock contention MCP tools — get_latch_stats, get_spinlock_stats — served over Darling's
/// Postgres store. Each tool body mirrors the Dashboard's <c>McpLatchSpinlockTools</c> field-for-field (Lite
/// has since ported both names as LATEST-SNAPSHOT reads over its own store — <c>McpLatchSpinlockTools</c> in
/// <c>Lite/Mcp</c> — so the two SKUs share the names but not the shape: Darling aggregates the window, Lite
/// serves the newest snapshot in it). Reads flow through <see cref="DarlingLatchSpinlockReader"/> — STORED reads (no live monitored-server hit),
/// windowed on <c>hours_back</c>.
///
/// <para>
/// The Dashboard's per-class <c>severity</c> / <c>description</c> / <c>recommendation</c> (latch) and
/// <c>description</c> (spinlock) are the Dashboard view's own CASE derivations, not collected columns; they are
/// reproduced verbatim in <see cref="DarlingLatchSpinlockReader"/> so these tools serve the full Dashboard
/// result shape. The per-second rate is derived in SQL from the per-class LAG interval, because Darling's
/// delta collectors store no <c>sample_interval_seconds</c>.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpLatchSpinlockTools
{
    [McpServerTool(Name = "get_latch_stats"), Description("Gets top latch contention by class. Shows latch waits, wait time, and per-second rates. High LATCH_EX on ACCESS_METHODS_DATASET_PARENT or FGCB_ADD_REMOVE indicates TempDB allocation contention. TWO CLOCKS PER ROW, NAMED: total_delta_* SUM every collection in the window; severity, waits_per_second and wait_ms_per_second are banded/derived from the LATEST interval only - the severity_banded_from block names that interval (its delta wait, the seconds it accrued over, and the collection it ended at, which is also latest_collection_time). A LOW severity beside a large window total is a class that was hot earlier in the window and is quiet now, not a contradiction.")]
    public static async Task<string> GetLatchStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24,
        [Description("Number of top latch classes to return. Default 10.")] int top = 10,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingLatchSpinlockReader.GetLatchStatsTopNAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, top);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "latch_stats")
                    ?? McpHelpers.Status("unavailable", "No latch statistics available in the requested time range.");

            var latches = rows.Select(r => new
            {
                latch_class = r.LatchClass,
                total_delta_wait_time_ms = r.TotalDeltaWaitTimeMs,
                total_delta_waiting_requests = r.TotalDeltaWaitingRequests,
                avg_wait_ms_per_request = r.TotalDeltaWaitingRequests > 0
                    ? Math.Round((double)r.TotalDeltaWaitTimeMs / r.TotalDeltaWaitingRequests, 2)
                    : (double?)null,
                /* null when the latest interval was unknowable (#3540) — a restart, not a quiet latch. */
                waits_per_second = r.WaitsPerSecond is double waits ? Math.Round(waits, 2) : (double?)null,
                wait_ms_per_second = r.WaitMsPerSecond is double waitMs ? Math.Round(waitMs, 2) : (double?)null,
                severity = DarlingLatchSpinlockReader.LatchSeverity(r.LatestDeltaWaitTimeMs),
                /* #3541 A10: the band above is a function of ONE interval's delta, published beside window
                   totals it is not a function of. Naming the interval — its delta, its length, its end — is
                   what lets a reader tell "LOW now, 40 s of waits over the day" from "LOW all day". */
                severity_banded_from = new
                {
                    delta_wait_time_ms = r.LatestDeltaWaitTimeMs,
                    interval_seconds = r.LatestIntervalSeconds is double seconds ? Math.Round(seconds, 0) : (double?)null,
                    captured_at = r.LatestCollectionTime.ToString("o")
                },
                description = DarlingLatchSpinlockReader.LatchDescription(r.LatchClass),
                recommendation = DarlingLatchSpinlockReader.LatchRecommendation(r.LatchClass),
                latest_collection_time = r.LatestCollectionTime.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                latch_count = rows.Count,
                latches
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_latch_stats", ex);
        }
    }

    [McpServerTool(Name = "get_spinlock_stats"), Description("Gets top spinlock contention. Shows collisions, spins, backoffs, and per-second rates. High spinlock contention indicates CPU-bound internal contention that doesn't appear in wait stats.")]
    public static async Task<string> GetSpinlockStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24,
        [Description("Number of top spinlocks to return. Default 10.")] int top = 10,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingLatchSpinlockReader.GetSpinlockStatsTopNAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, top);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "spinlock_stats")
                    ?? McpHelpers.Status("unavailable", "No spinlock statistics available in the requested time range.");

            var spinlocks = rows.Select(r => new
            {
                spinlock_name = r.SpinlockName,
                total_delta_collisions = r.TotalDeltaCollisions,
                total_delta_spins = r.TotalDeltaSpins,
                total_delta_backoffs = r.TotalDeltaBackoffs,
                spins_per_collision = r.TotalDeltaCollisions > 0
                    ? Math.Round((double)r.TotalDeltaSpins / r.TotalDeltaCollisions, 1)
                    : (double?)null,
                /* null when the latest interval was unknowable (#3540) — a restart, not a quiet spinlock. */
                collisions_per_second = r.CollisionsPerSecond is double collisions ? Math.Round(collisions, 2) : (double?)null,
                spins_per_second = r.SpinsPerSecond is double spins ? Math.Round(spins, 2) : (double?)null,
                description = DarlingLatchSpinlockReader.SpinlockDescription(r.SpinlockName),
                latest_collection_time = r.LatestCollectionTime.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                spinlock_count = rows.Count,
                spinlocks
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_spinlock_stats", ex);
        }
    }
}
