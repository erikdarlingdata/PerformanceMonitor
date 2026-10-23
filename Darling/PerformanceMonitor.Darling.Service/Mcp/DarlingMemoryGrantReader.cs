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
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the memory-grant MCP tools (<see cref="DarlingMcpMemoryGrantTools"/>) — the SAME
/// <c>memory_grant_stats</c> data the Dashboard's <c>get_resource_semaphore</c>, Lite's <c>get_memory_grants</c>,
/// and the viewer's <c>ViewerDataService.Memory</c> read, adapted here so the MCP host never references the WPF
/// viewer project. Both are STORED reads (no live monitored-server hit) of the LATEST snapshot in the window,
/// on the <c>v_memory_grant_stats</c> passthrough view.
///
/// <para>
/// get_resource_semaphore is the semaphore/ceiling lens: one row per (resource_semaphore_id, pool_id) with the
/// full workspace-memory sizing — <c>target</c> / <c>max_target</c> (the hard ceiling) / <c>total</c> /
/// <c>available</c> / <c>granted</c> / <c>used</c> plus grantee/waiter/timeout/forced counts and deltas. It
/// carries <c>max_target_memory_mb</c> (present in the store, the Dashboard tool omitted it) and, since V128
/// (#3540), the Dashboard's <c>sample_interval_seconds</c> too — the measured seconds the two deltas accrued
/// over, which this tool dropped while the collector stored none; <c>0</c> is the calculator's "no delta
/// knowable" marker (a restart, not a quiet semaphore) and the tool reports it as <c>null</c>, and a pre-V128
/// row that never recorded one reads as <c>null</c> with <c>interval_known = false</c>. get_memory_grants
/// is Lite's pool-detail lens: the sizing + activity SUMMED per pool (Lite's <c>GetMemoryGrantChartDataAsync</c>
/// shape). Every SQL string is a public const so Darling.Tests can pin the dialect + columns without a live Postgres.
/// </para>
///
/// <para><b>#3541 A10: the window is READ, not merely searched.</b> Both tools accept <c>hours_back</c>, and
/// until this change the only thing it did was bound the search for the newest snapshot — a grant storm three
/// hours ago (waiters in the dozens, timeouts climbing) was invisible behind a calm latest row while the
/// parameter read as a window. The <c>*WindowSql</c> reads below aggregate the SAME rows the latest read picks
/// its snapshot from, per semaphore or per pool: the peak waiter count and WHEN it peaked, the peak grant, the
/// floor of available workspace, and the SUM of the per-interval timeout / forced-grant deltas across every
/// snapshot in the window. Summing the deltas is the whole reason the collector stores them; it needs no
/// interval arithmetic (the "naked family" #3540 rung that is still to land stores the interval the deltas
/// accrued over, which is a RATE question, not this one) and a restart's fabricated 0 delta adds 0. The
/// latest snapshot is served BESIDE the window figures, stamped, so a caller can tell "calm now" from "calm
/// all window".</para>
/// </summary>
internal static class DarlingMemoryGrantReader
{
    /* ─────────────────────────── result rows ─────────────────────────── */

    /// <summary>One resource pool at the latest snapshot (summed across its semaphores) — Lite's grant lens.</summary>
    public sealed record MemoryGrantRow(
        DateTime CollectionTime, int PoolId, double AvailableMemoryMb, double GrantedMemoryMb, double UsedMemoryMb,
        long GranteeCount, long WaiterCount, long TimeoutErrorCountDelta, long ForcedGrantCountDelta);

    /// <summary>One point of get_memory_grants' <c>chart: true</c> series (#6299): one resource pool at one bucket of the
    /// window. <see cref="CollectionTime"/> is the bucket's start, or the one collection's own time when every bucket of the
    /// answer holds exactly one collection (the viewer's #4349 rule, see <see cref="GetMemoryGrantChartAsync"/>).
    /// <see cref="Snapshots"/> is how many collections the point averages. A NULL column reads as 0, as the viewer's chart does.</summary>
    public sealed record MemoryGrantChartRow(
        DateTime CollectionTime, int PoolId, double AvailableMemoryMb, double GrantedMemoryMb, double UsedMemoryMb,
        double TargetMemoryMb, double MaxTargetMemoryMb, long GranteeCount, long WaiterCount,
        long TimeoutErrorCountDelta, long ForcedGrantCountDelta, long Snapshots);

    /* ─────────────────────────── resource semaphore (latest snapshot, per semaphore) ─────────────────────────── */

    /* ─────────────────────────── memory grants (latest snapshot, per pool) ─────────────────────────── */

    /// <summary>
    /// The latest snapshot's per-pool grant detail — Lite's <c>get_memory_grants</c> shape: sizing + activity
    /// SUMMED per pool_id (Lite's <c>GetMemoryGrantChartDataAsync</c> aggregation) at the most recent
    /// collection in the window. The MB SUMs CAST to double precision; the count SUMs CAST to bigint (a
    /// Postgres <c>SUM(integer)</c> widens to bigint), matching the viewer. $1 server_id, $2 start, $3 end (naive UTC).
    /// </summary>
    public const string MemoryGrantsLatestSql = """
        WITH latest AS
        (
            SELECT MAX(collection_time) AS mx
            FROM v_memory_grant_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        )
        SELECT
            collection_time,
            pool_id,
            CAST(SUM(available_memory_mb) AS double precision) AS available_memory_mb,
            CAST(SUM(granted_memory_mb) AS double precision) AS granted_memory_mb,
            CAST(SUM(used_memory_mb) AS double precision) AS used_memory_mb,
            CAST(SUM(grantee_count) AS bigint) AS grantee_count,
            CAST(SUM(waiter_count) AS bigint) AS waiter_count,
            CAST(SUM(timeout_error_count_delta) AS bigint) AS timeout_error_count_delta,
            CAST(SUM(forced_grant_count_delta) AS bigint) AS forced_grant_count_delta
        FROM v_memory_grant_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT mx FROM latest)
        GROUP BY collection_time, pool_id
        ORDER BY pool_id
        """;

    public static async Task<List<MemoryGrantRow>> GetMemoryGrantsLatestAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<MemoryGrantRow>();
        await using var command = postgres.CreateCommand(MemoryGrantsLatestSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new MemoryGrantRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8)));
        }

        return rows;
    }

    /// <summary>
    /// The per-collection series behind the viewer's two Memory Grants charts (#6299), through the viewer's own SQL text
    /// (<see cref="PerformanceMonitor.Darling.Storage.TrendBucketSql.MemoryGrantChartDataSql"/>): one point per bucket per pool across the WHOLE
    /// window, never the newest snapshot's rows. Zero handling is the viewer's: a NULL column reads as 0, and a collection with no
    /// grants is a real point of zeros. A window with no collection returns no row at all, so the caller says "no data"
    /// rather than drawing a line of zeros. Like the viewer (<c>ViewerDataService.GetMemoryGrantChartDataAsync</c>), when EVERY
    /// bucket returned holds exactly one collection each point is stamped at that collection's own time instead of the bucket grid
    /// (#4349), so a sparse window does not draw its dots on a grid the collector never ran on. $4 is the bucket width in minutes.
    /// </summary>
    public static async Task<List<MemoryGrantChartRow>> GetMemoryGrantChartAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int bucketMinutes, CancellationToken cancellationToken = default)
    {
        var rows = new List<MemoryGrantChartRow>();
        var firstTimes = new List<DateTime>();
        var everyBucketSingleton = true;
        await using var command = postgres.CreateCommand(PerformanceMonitor.Darling.Storage.TrendBucketSql.MemoryGrantChartDataSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var snapshots = reader.GetInt64(12);
            if (snapshots != 1)
            {
                everyBucketSingleton = false;
            }

            firstTimes.Add(reader.GetDateTime(11));
            rows.Add(new MemoryGrantChartRow(
                reader.GetDateTime(1),
                reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
                reader.IsDBNull(10) ? 0 : reader.GetDouble(10),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                snapshots));
        }

        return everyBucketSingleton
            ? rows.Select((r, i) => r with { CollectionTime = firstTimes[i] }).ToList()
            : rows;
    }

    /* ─────────────────────────── the window, per semaphore / per pool (#3541 A10) ─────────────────────────── */

    /// <summary>
    /// Every snapshot in the window aggregated per pool — the window half of get_memory_grants. The pool lens
    /// SUMs across a pool's semaphores at each snapshot first (the same per-snapshot SUM
    /// <see cref="MemoryGrantsLatestSql"/> serves), THEN takes the window's peak / floor / total over those
    /// per-snapshot pool figures, so "peak waiters" is the most sessions waiting on the pool at any one
    /// instant, not the largest single semaphore's count. $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string MemoryGrantsWindowSql = """
        WITH per_snapshot AS
        (
            SELECT
                collection_time,
                pool_id,
                SUM(waiter_count) AS waiter_count,
                SUM(granted_memory_mb) AS granted_memory_mb,
                SUM(available_memory_mb) AS available_memory_mb,
                SUM(timeout_error_count_delta) AS timeout_error_count_delta,
                SUM(forced_grant_count_delta) AS forced_grant_count_delta
            FROM v_memory_grant_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time, pool_id
        ),
        agg AS
        (
            SELECT
                pool_id,
                COUNT(*) AS snapshots_in_window,
                MIN(collection_time) AS first_snapshot_at,
                MAX(collection_time) AS last_snapshot_at,
                CAST(MAX(waiter_count) AS bigint) AS peak_waiter_count,
                CAST(MAX(granted_memory_mb) AS double precision) AS peak_granted_memory_mb,
                CAST(MIN(available_memory_mb) AS double precision) AS min_available_memory_mb,
                CAST(SUM(timeout_error_count_delta) AS bigint) AS timeout_errors_in_window,
                CAST(SUM(forced_grant_count_delta) AS bigint) AS forced_grants_in_window
            FROM per_snapshot
            GROUP BY pool_id
        ),
        peak AS
        (
            SELECT DISTINCT ON (pool_id)
                pool_id,
                collection_time AS peak_waiters_at
            FROM per_snapshot
            ORDER BY pool_id, waiter_count DESC, collection_time DESC
        ),
        peak_grant AS
        (
            SELECT DISTINCT ON (pool_id)
                pool_id,
                collection_time AS peak_granted_at
            FROM per_snapshot
            ORDER BY pool_id, granted_memory_mb DESC NULLS LAST, collection_time DESC
        )
        SELECT
            CAST(NULL AS smallint) AS resource_semaphore_id,
            a.pool_id,
            a.snapshots_in_window,
            a.first_snapshot_at,
            a.last_snapshot_at,
            a.peak_waiter_count,
            p.peak_waiters_at,
            a.peak_granted_memory_mb,
            a.min_available_memory_mb,
            a.timeout_errors_in_window,
            a.forced_grants_in_window,
            pg.peak_granted_at
        FROM agg AS a
        JOIN peak AS p ON p.pool_id = a.pool_id
        JOIN peak_grant AS pg ON pg.pool_id = a.pool_id
        ORDER BY a.pool_id
        """;

    /* ─────────────────────────── resource semaphore: moved to Storage (#6388) ─────────────────────────── */

    /// <summary>The latest-snapshot read, now <see cref="DarlingResourceSemaphoreReader.LatestSql"/> in Storage so the Viewer
    /// reads the same text (#6388).</summary>
    public const string ResourceSemaphoreLatestSql = DarlingResourceSemaphoreReader.LatestSql;

    /// <summary>The window read, now <see cref="DarlingResourceSemaphoreReader.WindowSql"/> in Storage (#6388).</summary>
    public const string ResourceSemaphoreWindowSql = DarlingResourceSemaphoreReader.WindowSql;

    public static Task<List<ResourceSemaphoreRow>> GetResourceSemaphoreLatestAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingResourceSemaphoreReader.GetLatestAsync(postgres, serverId, startUtc, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);

    public static Task<List<MemoryGrantWindowRow>> GetResourceSemaphoreWindowAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingResourceSemaphoreReader.GetWindowAsync(postgres, serverId, startUtc, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);

    public static Task<List<MemoryGrantWindowRow>> GetMemoryGrantsWindowAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default) =>
        DarlingResourceSemaphoreReader.ReadWindowAsync(postgres, MemoryGrantsWindowSql, serverId, startUtc, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);


    /* ─────────────────────────── memory pressure events (RING_BUFFER_RESOURCE_MONITOR) ─────────────────────────── */

    /// <summary>One RING_BUFFER_RESOURCE_MONITOR sample — the sample time plus the SQL Server (process) and OS
    /// (system) memory-pressure indicators. Mirror of the viewer's <c>MemoryPressureEventRow</c>.</summary>
    public sealed record MemoryPressureEventRow(
        DateTime SampleTime, string MemoryNotification, int MemoryIndicatorsProcess, int MemoryIndicatorsSystem);

    /// <summary>
    /// The RING_BUFFER_RESOURCE_MONITOR memory-pressure samples over the window — the viewer's
    /// <c>MemoryPressureEventsSql</c> (Lite's <c>GetMemoryPressureEventsAsync</c> ported to Postgres). Windowed
    /// on <c>sample_time</c> (the payload's own clock, which the <c>memory_pressure_events</c> index keys on),
    /// not collection_time, on the <c>v_memory_pressure_events</c> passthrough view. $1 server_id, $2 window
    /// start, $3 window end (naive UTC).
    /// </summary>
    public const string MemoryPressureEventsSql = """
        SELECT
            sample_time,
            memory_notification,
            memory_indicators_process,
            memory_indicators_system
        FROM v_memory_pressure_events
        WHERE server_id = $1
        AND   sample_time >= $2
        AND   sample_time <= $3
        ORDER BY sample_time
        """;

    public static async Task<List<MemoryPressureEventRow>> GetMemoryPressureEventsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<MemoryPressureEventRow>();
        await using var command = postgres.CreateCommand(MemoryPressureEventsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new MemoryPressureEventRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3)));
        }

        return rows;
    }
}
