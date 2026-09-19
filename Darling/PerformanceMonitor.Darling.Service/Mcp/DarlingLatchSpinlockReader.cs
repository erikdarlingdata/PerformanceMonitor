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
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the latch / spinlock contention MCP tools
/// (<see cref="DarlingMcpLatchSpinlockTools"/>) — the SAME collected data the Dashboard's
/// <c>McpLatchSpinlockTools</c> and the viewer's <c>ViewerDataService.LatchSpinlock</c> read, adapted here
/// so the MCP host never references the WPF viewer project. Both are STORED reads (no live monitored-server
/// hit) keyed by <c>server_id</c> and windowed on the naive-UTC <c>collection_time</c>.
///
/// <para>
/// Darling's <c>latch_stats</c> / <c>spinlock_stats</c> collectors are cumulative-counter delta collectors
/// (like wait_stats) that store the last interval's <c>delta_*</c> beside the measured
/// <c>sample_interval_seconds</c> it accrued over (V127, #3540 — before which the per-second rate could only
/// be derived from the per-class <c>LAG</c> interval, the truncate-then-diff epoch idiom the viewer's trend
/// reads use, and that derivation remains the fallback for pre-V127 rows). Each read aggregates the window
/// per class into one row (the top N by total delta wait time / total delta collisions), carrying the latest
/// interval's per-second rate and last delta; the rate is null when the latest interval was unknowable (a
/// stored 0: first sighting, counter reset, gap past the policy) rather than the 0.00 a restart used to read
/// as. The Dashboard's <c>severity</c> / <c>latch_description</c>
/// / <c>recommendation</c> and <c>spinlock_description</c> are NOT stored columns — they are the Dashboard
/// view's own CASE derivations (deterministic functions of <c>latch_class</c> / <c>spinlock_name</c> and the
/// latest delta), reproduced VERBATIM as pure static helpers here so the tools serve the FULL Dashboard result
/// shape without inventing any data. Every SQL string is a public const so Darling.Tests can pin the dialect
/// + columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingLatchSpinlockReader
{
    /* ─────────────────────────── result rows ─────────────────────────── */

    /// <summary>One latch class aggregated over the window: the summed deltas plus the latest interval's
    /// per-second rate and last delta wait (the severity input). <see cref="LatestIntervalSeconds"/> is the
    /// span that last delta accrued over (#3541 A10 — the interval the severity band was computed from, published
    /// beside the window totals so the two are distinguishable); null when the interval was unknowable.</summary>
    public sealed record LatchStatRow(
        string LatchClass, long TotalDeltaWaitTimeMs, long TotalDeltaWaitingRequests,
        double? WaitsPerSecond, double? WaitMsPerSecond, long LatestDeltaWaitTimeMs, DateTime LatestCollectionTime,
        double? LatestIntervalSeconds);

    /// <summary>One spinlock aggregated over the window: the summed deltas plus the latest interval's
    /// per-second collision/spin rates. <see cref="LatestIntervalSeconds"/> is the span those rates divide by
    /// (#3653 A16 — the latch row has carried it since #3541 A10; the spinlock row gains it so a null rate on
    /// EITHER tool sits beside the same why-key on both SKUs); null when the interval was unknowable.</summary>
    public sealed record SpinlockStatRow(
        string SpinlockName, long TotalDeltaCollisions, long TotalDeltaSpins, long TotalDeltaBackoffs,
        double? CollisionsPerSecond, double? SpinsPerSecond, DateTime LatestCollectionTime,
        double? LatestIntervalSeconds);

    /* ─────────────────────────── latch stats (top N over the window) ─────────────────────────── */

    /// <summary>
    /// The top-N latch classes over the window, one row per class — mirroring the Dashboard's
    /// <c>get_latch_stats</c> per-class aggregation (SUM of the last interval's deltas, top by total delta
    /// wait time). The per-second rate comes from the latest interval's stored <c>sample_interval_seconds</c>
    /// (the per-class <c>LAG</c> interval only for pre-V127 rows), the same idiom the viewer's
    /// <c>LatchTrendSql</c> uses; null when that interval was unknowable. <c>latest_delta_wait_time_ms</c>
    /// feeds the reproduced severity CASE. Runs on the
    /// <c>v_latch_stats</c> passthrough view. $1 server_id, $2 window start, $3 window end (naive UTC), $4 top.
    /// </summary>
    public const string LatchStatsTopNSql = """
        WITH windowed AS
        (
            SELECT
                latch_class,
                collection_time,
                delta_waiting_requests_count,
                delta_wait_time_ms,
                /* #3540: the STORED interval where the row has one; 0 (no delta knowable) becomes NULL through NULLIF
                   so the latest-interval rates below are NULL — reported as null, never 0.00 — when the newest
                   collection was a restart. NULL (a pre-V127 row) falls back to the LAG. */
                CASE WHEN sample_interval_seconds IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY latch_class ORDER BY collection_time))))
                     ELSE NULLIF(sample_interval_seconds, 0)
                END AS interval_seconds
            FROM v_latch_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        agg AS
        (
            SELECT
                latch_class,
                CAST(SUM(delta_wait_time_ms) AS bigint) AS total_delta_wait_time_ms,
                CAST(SUM(delta_waiting_requests_count) AS bigint) AS total_delta_waiting_requests,
                MAX(collection_time) AS latest_collection_time
            FROM windowed
            GROUP BY latch_class
        ),
        latest AS
        (
            SELECT DISTINCT ON (latch_class)
                latch_class,
                delta_wait_time_ms AS latest_delta_wait_time_ms,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_waiting_requests_count AS double precision) / interval_seconds END AS waits_per_second,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS double precision) / interval_seconds END AS wait_ms_per_second,
                /* #3541 A10: the span the severity-banded delta accrued over, so the tool can publish the
                   interval the band came from beside the window totals it does NOT come from. */
                CASE WHEN interval_seconds > 0 THEN CAST(interval_seconds AS double precision) END AS latest_interval_seconds
            FROM windowed
            ORDER BY latch_class, collection_time DESC
        )
        SELECT
            a.latch_class,
            a.total_delta_wait_time_ms,
            a.total_delta_waiting_requests,
            l.waits_per_second,
            l.wait_ms_per_second,
            l.latest_delta_wait_time_ms,
            a.latest_collection_time,
            l.latest_interval_seconds
        FROM agg AS a
        JOIN latest AS l ON l.latch_class = a.latch_class
        ORDER BY a.total_delta_wait_time_ms DESC
        LIMIT $4
        """;

    public static async Task<List<LatchStatRow>> GetLatchStatsTopNAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, CancellationToken cancellationToken = default)
    {
        var rows = new List<LatchStatRow>();
        await using var command = postgres.CreateCommand(LatchStatsTopNSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, top);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new LatchStatRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetDouble(3),
                reader.IsDBNull(4) ? null : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.GetDateTime(6),
                reader.IsDBNull(7) ? null : reader.GetDouble(7)));
        }

        return rows;
    }

    /* ─────────────────────────── spinlock stats (top N over the window) ─────────────────────────── */

    /// <summary>
    /// The top-N spinlocks over the window, one row per spinlock — the collision analog of
    /// <see cref="LatchStatsTopNSql"/>, mirroring the Dashboard's <c>get_spinlock_stats</c> per-name
    /// aggregation (top by total delta collisions). Per-second collision/spin rates from the latest interval's
    /// stored <c>sample_interval_seconds</c> (per-name <c>LAG</c> for pre-V127 rows), null when unknowable,
    /// with that interval itself as <c>latest_interval_seconds</c> — the latch query's own line (#3541 A10),
    /// so a caller reading a null <c>collisions_per_second</c> sees WHY beside it rather than inferring it
    /// (#3653 A16). Runs on <c>v_spinlock_stats</c>. $1 server_id, $2 start, $3 end (naive UTC), $4 top.
    /// </summary>
    public const string SpinlockStatsTopNSql = """
        WITH windowed AS
        (
            SELECT
                spinlock_name,
                collection_time,
                delta_collisions,
                delta_spins,
                delta_backoffs,
                /* #3540: the STORED interval where the row has one; 0 (no delta knowable) becomes NULL through NULLIF
                   so the latest-interval rates below are NULL — reported as null, never 0.00 — when the newest
                   collection was a restart. NULL (a pre-V127 row) falls back to the LAG. */
                CASE WHEN sample_interval_seconds IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY spinlock_name ORDER BY collection_time))))
                     ELSE NULLIF(sample_interval_seconds, 0)
                END AS interval_seconds
            FROM v_spinlock_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        agg AS
        (
            SELECT
                spinlock_name,
                CAST(SUM(delta_collisions) AS bigint) AS total_delta_collisions,
                CAST(SUM(delta_spins) AS bigint) AS total_delta_spins,
                CAST(SUM(delta_backoffs) AS bigint) AS total_delta_backoffs,
                MAX(collection_time) AS latest_collection_time
            FROM windowed
            GROUP BY spinlock_name
        ),
        latest AS
        (
            SELECT DISTINCT ON (spinlock_name)
                spinlock_name,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_collisions AS double precision) / interval_seconds END AS collisions_per_second,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_spins AS double precision) / interval_seconds END AS spins_per_second,
                /* #3653 A16: the span the two rates above divide by, published beside them as interval_seconds
                   (null when unknowable) — the same line LatchStatsTopNSql carries for the severity block. */
                CASE WHEN interval_seconds > 0 THEN CAST(interval_seconds AS double precision) END AS latest_interval_seconds
            FROM windowed
            ORDER BY spinlock_name, collection_time DESC
        )
        SELECT
            a.spinlock_name,
            a.total_delta_collisions,
            a.total_delta_spins,
            a.total_delta_backoffs,
            l.collisions_per_second,
            l.spins_per_second,
            a.latest_collection_time,
            l.latest_interval_seconds
        FROM agg AS a
        JOIN latest AS l ON l.spinlock_name = a.spinlock_name
        ORDER BY a.total_delta_collisions DESC
        LIMIT $4
        """;

    public static async Task<List<SpinlockStatRow>> GetSpinlockStatsTopNAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, CancellationToken cancellationToken = default)
    {
        var rows = new List<SpinlockStatRow>();
        await using var command = postgres.CreateCommand(SpinlockStatsTopNSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, top);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new SpinlockStatRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetDouble(4),
                reader.IsDBNull(5) ? null : reader.GetDouble(5),
                reader.GetDateTime(6),
                reader.IsDBNull(7) ? null : reader.GetDouble(7)));
        }

        return rows;
    }

    /* ─────────────────────────── the Dashboard's CASE enrichment, reproduced ─────────────────────────── */
    /* severity / latch_description / recommendation / spinlock_description are NOT stored columns — they are
       the Dashboard view's own CASE derivations (see DatabaseService.ResourceMetrics.LatchSpinlock.cs).
       Reproduced verbatim so Darling serves the full Dashboard result shape without inventing any data. */

    /// <summary>The Dashboard's latch severity band (from the last interval's delta wait time).</summary>
    public static string LatchSeverity(long latestDeltaWaitTimeMs) =>
        latestDeltaWaitTimeMs > 10000 ? "HIGH" :
        latestDeltaWaitTimeMs > 5000 ? "MEDIUM" :
        "LOW";

    /// <summary>The Dashboard's latch-class description (exact-name CASE).</summary>
    public static string LatchDescription(string latchClass) =>
        latchClass switch
        {
            "BUFFER" => "Synchronize short term access to database pages.",
            "BUFFER_POOL_GROW" => "Buffer pool grow operations.",
            "DATABASE_CHECKPOINT" => "Serialize checkpoints within a database.",
            "FCB" => "Synchronize access to the file control block.",
            "FGCB_ADD_REMOVE" => "Synchronize file add/drop/grow/shrink operations.",
            "LOG_MANAGER" => "Transaction log manager synchronization.",
            _ => "Internal SQL Server synchronization.",
        };

    /// <summary>The Dashboard's latch-class recommendation (prefix / membership CASE).</summary>
    public static string LatchRecommendation(string latchClass)
    {
        if (latchClass.StartsWith("PAGEIOLATCH", StringComparison.Ordinal)) return "I/O bottleneck - check disk latency, add memory";
        if (latchClass.StartsWith("PAGELATCH", StringComparison.Ordinal)) return "Page contention - check for hot pages, tempdb issues";
        if (latchClass == "BUFFER") return "Buffer pool contention - check for memory pressure";
        if (latchClass.StartsWith("ACCESS_METHODS", StringComparison.Ordinal)) return "Index/heap access contention";
        if (latchClass.StartsWith("ALLOC", StringComparison.Ordinal)) return "Allocation contention - consider pre-sizing files";
        if (latchClass is "LOG_MANAGER" or "LOGCACHE_ACCESS") return "Log contention - check log disk";
        return "Review latch class documentation";
    }

    /// <summary>The Dashboard's spinlock description (exact-name CASE).</summary>
    public static string SpinlockDescription(string spinlockName) =>
        spinlockName switch
        {
            "BACKUP_CTX" => "Page I/O during backup - high spins during checkpoint/lazywriter.",
            "DBTABLE" => "In-memory data structure access for database properties.",
            "DP_LIST" => "Dirty page list with indirect checkpoint enabled.",
            "LOCK_HASH" => "Lock manager hash table access.",
            "LOCK_RW_SECURITY_CACHE" => "Security token and access check cache.",
            "SOS_CACHESTORE" => "Various in-memory caches (plan cache, temp tables).",
            _ => "Internal use only.",
        };
}
