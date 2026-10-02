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
/// result shape. The per-second rate is derived in SQL from the latest interval's STORED
/// <c>sample_interval_seconds</c> (V127, #3595 — both delta collectors have stamped it since; the per-class
/// LAG interval is the fallback for a pre-V127 row only), and is null when that interval was unknowable.
/// </para>
///
/// <para>
/// <b>The unknowable row is spelled one way on both SKUs (#3653 A16).</b> A restart / first-sample
/// collection stores <c>sample_interval_seconds = 0</c> beside deltas of 0 that were never measured
/// (#3540's marker). Both tools on both SKUs publish that row as: the per-second rates null,
/// <c>interval_seconds</c> null beside them (the why — a bare null rate cannot say whether the interval was
/// unknowable or the class was quiet), and the latest delta null rather than the fabricated 0 (#3642's rule,
/// the shape both viewers' snapshot grids render since #3702 and Lite's twins publish since the same PR).
/// Darling's rows are window AGGREGATES and Lite's are the newest snapshot, so the rows as a whole are not the
/// same shape and never were; the KEYS that spell this one fact are, and
/// <c>McpPageContractTests.TheSameToolName_SpellsTheUnknowableRowTheSameWay_OnBothSkus</c> holds them so.
/// The <c>severity</c> band is a function of the latest delta, so it is null on the same row — "LOW" banded
/// from a 0 nobody measured is the same lie in a word.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpLatchSpinlockTools
{
    [McpServerTool(Name = "get_latch_stats"), Description("Latch contention by class. Darling sums waits over the whole hours_back window and returns the top classes by that total. Lite: LATEST IS A TIME, only the newest snapshot within hours_back, paged to limit, heaviest last-interval wait first. interval_seconds, both per-second rates and (on Darling) severity come from the LATEST interval only; on a restart or first sample (interval unknowable, stored 0) they are null, never 0 or LOW. Zero rows: not_collected on a non-SQL Server target, else unavailable, never empty. <<GUIDE>> Gets top latch contention by class. Shows latch waits, wait time, and per-second rates. ACCESS_METHODS_DATASET_PARENT synchronizes child dataset access to the parent dataset during parallel operations, and FGCB_ADD_REMOVE synchronizes filegroup add, drop, grow and shrink file operations. TWO CLOCKS PER ROW, NAMED: total_delta_* SUM every collection in the window; severity, waits_per_second and wait_ms_per_second are banded/derived from the LATEST interval only - interval_seconds is the seconds that interval accrued over, and the severity_banded_from block names it in full (its delta wait, the same seconds, and the collection it ended at, which is also latest_collection_time). A LOW severity beside a large window total is a class that was hot earlier in the window and is quiet now, not a contradiction. When the latest interval was unknowable (a restart or first sample, stored as 0), interval_seconds, both per-second rates, severity and severity_banded_from.delta_wait_time_ms are all null - unknowable, never a quiet 0.00 or a LOW banded from a zero nobody measured; the window totals beside them still stand.")]
    public static async Task<string> GetLatchStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24,
        [Description("Number of top latch classes to return. Default 10.")] int top = 10,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingLatchSpinlockReader.GetLatchStatsTopNAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "latch_stats", cancellationToken)
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
                /* #3653 A16: the seconds the two rates divide by, beside them — the one why-key for a null rate
                   on both tools and both SKUs. Also restated inside severity_banded_from below, the way that
                   block restates captured_at beside latest_collection_time: the block is the band's
                   provenance in one place, this is the rates' denominator in its place. */
                interval_seconds = r.LatestIntervalSeconds is double intervalSeconds ? Math.Round(intervalSeconds, 0) : (double?)null,
                /* #3653 A16: no band from an unknowable interval. The reader stores the marker's delta as 0 and
                   LatchSeverity(0) reads LOW — a quiet latch, asserted from a number nobody measured. Null
                   here is what the shared row already says on Lite and on both viewers' grids (#3702). */
                severity = r.LatestIntervalSeconds is null
                    ? null
                    : DarlingLatchSpinlockReader.LatchSeverity(r.LatestDeltaWaitTimeMs),
                /* #3541 A10: the band above is a function of ONE interval's delta, published beside window
                   totals it is not a function of. Naming the interval — its delta, its length, its end — is
                   what lets a reader tell "LOW now, 40 s of waits over the day" from "LOW all day". The delta
                   is null on the unknowable row for the same reason the band is (#3642: the stored 0 was the
                   calculator's marker, not a measurement). */
                severity_banded_from = new
                {
                    delta_wait_time_ms = r.LatestIntervalSeconds is null ? (long?)null : r.LatestDeltaWaitTimeMs,
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_latch_stats", ex);
        }
    }

    [McpServerTool(Name = "get_spinlock_stats"), Description("Gets spinlock contention: collisions, spins, backoffs, per-second rates. High contention is CPU-bound, not in wait stats. Darling sums every collection in hours_back, top N by total collisions. Lite: LATEST IS A TIME, only the newest snapshot within hours_back, cumulative counters plus last-interval deltas, bounded by limit (truncated flags more). interval_seconds and the rates reflect the latest interval, never a window total; null - never 0 - when unknowable (restart/first sample); totals/counters still stand. No rows: unavailable (or not_collected first). <<GUIDE>> Gets top spinlock contention. Shows collisions, spins, backoffs, and per-second rates. High spinlock contention indicates CPU-bound internal contention that doesn't appear in wait stats. total_delta_* SUM every collection in the window; collisions_per_second and spins_per_second are derived from the LATEST interval only, and interval_seconds is the seconds that interval accrued over. When the latest interval was unknowable (a restart or first sample, stored as 0), interval_seconds and both per-second rates are null - unknowable, never a quiet 0.00; the window totals beside them still stand.")]
    public static async Task<string> GetSpinlockStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24,
        [Description("Number of top spinlocks to return. Default 10.")] int top = 10,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(top);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingLatchSpinlockReader.GetSpinlockStatsTopNAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, top, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "spinlock_stats", cancellationToken)
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
                /* #3653 A16: the seconds those rates divide by, beside them — null when unknowable, the same
                   why-key the latch row and both of Lite's twins carry. */
                interval_seconds = r.LatestIntervalSeconds is double intervalSeconds ? Math.Round(intervalSeconds, 0) : (double?)null,
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_spinlock_stats", ex);
        }
    }
}
