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
/// carries <c>max_target_memory_mb</c> (present in the store, the Dashboard tool omitted it) and drops the
/// Dashboard's <c>sample_interval_seconds</c> (Darling's delta collector stores no sample interval). get_memory_grants
/// is Lite's pool-detail lens: the sizing + activity SUMMED per pool (Lite's <c>GetMemoryGrantChartDataAsync</c>
/// shape). Every SQL string is a public const so Darling.Tests can pin the dialect + columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingMemoryGrantReader
{
    /* ─────────────────────────── result rows ─────────────────────────── */

    /// <summary>One resource semaphore at the latest snapshot — the workspace-memory ceiling lens.</summary>
    public sealed record ResourceSemaphoreRow(
        DateTime CollectionTime, short ResourceSemaphoreId, int PoolId, double TargetMemoryMb, double MaxTargetMemoryMb,
        double TotalMemoryMb, double AvailableMemoryMb, double GrantedMemoryMb, double UsedMemoryMb,
        int GranteeCount, int WaiterCount, long TimeoutErrorCount, long ForcedGrantCount,
        long TimeoutErrorCountDelta, long ForcedGrantCountDelta);

    /// <summary>One resource pool at the latest snapshot (summed across its semaphores) — Lite's grant lens.</summary>
    public sealed record MemoryGrantRow(
        DateTime CollectionTime, int PoolId, double AvailableMemoryMb, double GrantedMemoryMb, double UsedMemoryMb,
        long GranteeCount, long WaiterCount, long TimeoutErrorCountDelta, long ForcedGrantCountDelta);

    /* ─────────────────────────── resource semaphore (latest snapshot, per semaphore) ─────────────────────────── */

    /// <summary>
    /// The latest snapshot's per-(semaphore, pool) rows — the Dashboard's <c>get_resource_semaphore</c> shape
    /// over Darling's store (plus <c>max_target_memory_mb</c> from the store; minus the Dashboard's
    /// unstored <c>sample_interval_seconds</c>). MB columns are <c>numeric(18,2)</c> → double precision.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string ResourceSemaphoreLatestSql = """
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
            resource_semaphore_id,
            pool_id,
            CAST(target_memory_mb AS double precision) AS target_memory_mb,
            CAST(max_target_memory_mb AS double precision) AS max_target_memory_mb,
            CAST(total_memory_mb AS double precision) AS total_memory_mb,
            CAST(available_memory_mb AS double precision) AS available_memory_mb,
            CAST(granted_memory_mb AS double precision) AS granted_memory_mb,
            CAST(used_memory_mb AS double precision) AS used_memory_mb,
            grantee_count,
            waiter_count,
            timeout_error_count,
            forced_grant_count,
            timeout_error_count_delta,
            forced_grant_count_delta
        FROM v_memory_grant_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT mx FROM latest)
        ORDER BY resource_semaphore_id, pool_id
        """;

    public static async Task<List<ResourceSemaphoreRow>> GetResourceSemaphoreLatestAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<ResourceSemaphoreRow>();
        await using var command = postgres.CreateCommand(ResourceSemaphoreLatestSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ResourceSemaphoreRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? (short)0 : reader.GetInt16(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                reader.IsDBNull(6) ? 0 : reader.GetDouble(6),
                reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                reader.IsDBNull(14) ? 0 : reader.GetInt64(14)));
        }

        return rows;
    }

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
