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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One resource semaphore at the latest snapshot — the workspace-memory ceiling lens.</summary>
/// <param name="SampleIntervalSeconds">#3540 (V128): the measured seconds the two deltas accrued over;
/// <c>0</c> is the calculator's "no delta knowable" marker (first sighting, counter reset, a gap past the
/// policy — a restart, not a quiet semaphore); <c>null</c> is a pre-V128 row that never recorded one.</param>
public sealed record ResourceSemaphoreRow(
    DateTime CollectionTime, short ResourceSemaphoreId, int PoolId, double TargetMemoryMb, double MaxTargetMemoryMb,
    double TotalMemoryMb, double AvailableMemoryMb, double GrantedMemoryMb, double UsedMemoryMb,
    int GranteeCount, int WaiterCount, long TimeoutErrorCount, long ForcedGrantCount,
    long TimeoutErrorCountDelta, long ForcedGrantCountDelta, int? SampleIntervalSeconds)
{
    /// <summary>True when the row's deltas are the calculator's (0, 0) marker: no delta was knowable, so
    /// the two <c>*_delta</c> zeros beside it are not "no timeouts this interval".</summary>
    public bool IsUnknowable => SampleIntervalSeconds == 0;
}

/// <summary>
/// One (resource_semaphore_id, pool_id) — or, for the pool lens, one pool with <see cref="ResourceSemaphoreId"/>
/// null — aggregated over EVERY snapshot in the window (#3541 A10). <see cref="PeakWaiterCount"/> /
/// <see cref="PeakWaitersAt"/> is the storm detector: the most sessions ever seen waiting for a grant in the
/// window and the snapshot it happened at. <see cref="PeakGrantedAt"/> is the snapshot of the largest
/// granted memory (<see cref="PeakGrantedMemoryMb"/>), a different moment when nothing waited then (#5644 W9). The two <c>*InWindow</c> figures are SUMs of the stored per-interval
/// deltas — how many grants timed out / were forced across the whole window, not just the last interval.
/// </summary>
public sealed record MemoryGrantWindowRow(
    short? ResourceSemaphoreId, int PoolId, long SnapshotsInWindow, DateTime FirstSnapshotAt, DateTime LastSnapshotAt,
    long PeakWaiterCount, DateTime PeakWaitersAt, double PeakGrantedMemoryMb, double MinAvailableMemoryMb,
    long TimeoutErrorsInWindow, long ForcedGrantsInWindow, DateTime PeakGrantedAt);

/// <summary>
/// The resource-semaphore reads the web's <c>get_resource_semaphore</c> endpoint (the MCP tool) and the Viewer's
/// Memory &gt; Resource Semaphore sub-tab share (#6388), moved here from the Service project's
/// <c>DarlingMemoryGrantReader</c> unchanged except that the command deadline is passed in: the MCP host passes
/// <c>McpCommandDeadlines.ReadSeconds</c>, the Viewer its interactive deadline. <see cref="ReadWindowAsync"/> is
/// the materialiser the memory-grants pool lens' window read still uses in the Service project.
/// </summary>
public static class DarlingResourceSemaphoreReader
{
    /// <summary>
    /// The latest snapshot's per-(semaphore, pool) rows — the Dashboard's <c>get_resource_semaphore</c> shape
    /// over Darling's store (plus <c>max_target_memory_mb</c> from the store; plus, since V128, the
    /// Dashboard's <c>sample_interval_seconds</c>, trailing so every existing ordinal is stable). MB columns
    /// are <c>numeric(18,2)</c> → double precision. $1 server_id, $2 window start, $3 window end (naive UTC).
    ///
    /// <para><b>Row order is pool first, then semaphore — the same order Lite's twin read
    /// (<c>LocalDataService.GetResourceSemaphoreSnapshotAsync</c>) and the pool-lens reads in both SKUs
    /// already use (#3653 A15/A16).</b> This read and <see cref="WindowSql"/> ordered semaphore
    /// first while Lite ordered pool first, so <c>get_resource_semaphore</c> returned the same rows in a
    /// different order on the two SKUs — the one place the twins disagreed on this tool, and a difference an
    /// agent reading <c>grants[0]</c> as "the default pool's regular semaphore" would never see. Pool first
    /// groups a pool's two semaphores (0 regular, 1 small-query) together, which is how the DMV is read.
    /// Pinned cross-SKU by <c>DarlingMcpMemoryGrantToolsTests</c>.</para>
    /// </summary>
    public const string LatestSql = """
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
            forced_grant_count_delta,
            sample_interval_seconds
        FROM v_memory_grant_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT mx FROM latest)
        ORDER BY pool_id, resource_semaphore_id
        """;

    public static async Task<List<ResourceSemaphoreRow>> GetLatestAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var rows = new List<ResourceSemaphoreRow>();
        await using var command = postgres.CreateCommand(LatestSql);
        command.CommandTimeout = commandTimeoutSeconds;
        StoreReadParameters.AddWindow(command, serverId, startUtc, endUtc);
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
                reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                /* NULL stays NULL: a pre-V128 row never recorded its interval, and that is a different
                   statement from the 0 the calculator writes when no delta was knowable. */
                reader.IsDBNull(15) ? null : reader.GetInt32(15)));
        }

        return rows;
    }

    /// <summary>
    /// Every snapshot in the window aggregated per (resource_semaphore_id, pool_id) — the window half of
    /// get_resource_semaphore. The peak's instant comes from a <c>DISTINCT ON</c> over the same windowed rows
    /// (highest waiter_count first, newest first on a tie, so a storm that plateaued reports its latest
    /// snapshot); the two <c>*_in_window</c> figures SUM the stored per-interval deltas. MB aggregates CAST to
    /// double precision, count aggregates to bigint, like the latest reads. $1 server_id, $2 window start,
    /// $3 window end (naive UTC).
    /// </summary>
    public const string WindowSql = """
        WITH windowed AS
        (
            SELECT
                collection_time,
                resource_semaphore_id,
                pool_id,
                waiter_count,
                granted_memory_mb,
                available_memory_mb,
                timeout_error_count_delta,
                forced_grant_count_delta
            FROM v_memory_grant_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        agg AS
        (
            SELECT
                resource_semaphore_id,
                pool_id,
                COUNT(*) AS snapshots_in_window,
                MIN(collection_time) AS first_snapshot_at,
                MAX(collection_time) AS last_snapshot_at,
                CAST(MAX(waiter_count) AS bigint) AS peak_waiter_count,
                CAST(MAX(granted_memory_mb) AS double precision) AS peak_granted_memory_mb,
                CAST(MIN(available_memory_mb) AS double precision) AS min_available_memory_mb,
                CAST(SUM(timeout_error_count_delta) AS bigint) AS timeout_errors_in_window,
                CAST(SUM(forced_grant_count_delta) AS bigint) AS forced_grants_in_window
            FROM windowed
            GROUP BY resource_semaphore_id, pool_id
        ),
        peak AS
        (
            SELECT DISTINCT ON (resource_semaphore_id, pool_id)
                resource_semaphore_id,
                pool_id,
                collection_time AS peak_waiters_at
            FROM windowed
            ORDER BY resource_semaphore_id, pool_id, waiter_count DESC, collection_time DESC
        ),
        peak_grant AS
        (
            SELECT DISTINCT ON (resource_semaphore_id, pool_id)
                resource_semaphore_id,
                pool_id,
                collection_time AS peak_granted_at
            FROM windowed
            ORDER BY resource_semaphore_id, pool_id, granted_memory_mb DESC NULLS LAST, collection_time DESC
        )
        SELECT
            a.resource_semaphore_id,
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
        JOIN peak AS p
          ON p.resource_semaphore_id = a.resource_semaphore_id
         AND p.pool_id = a.pool_id
        JOIN peak_grant AS pg
          ON pg.resource_semaphore_id = a.resource_semaphore_id
         AND pg.pool_id = a.pool_id
        ORDER BY a.pool_id, a.resource_semaphore_id
        """;

    public static Task<List<MemoryGrantWindowRow>> GetWindowAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int commandTimeoutSeconds, CancellationToken cancellationToken = default) =>
        ReadWindowAsync(postgres, WindowSql, serverId, startUtc, endUtc, commandTimeoutSeconds, cancellationToken);

    /// <summary>The two window reads project the SAME twelve columns in the same order (the pool lens fills
    /// <c>resource_semaphore_id</c> with a typed NULL), so one materialiser serves both.</summary>
    public static async Task<List<MemoryGrantWindowRow>> ReadWindowAsync(
        NpgsqlDataSource postgres, string sql, int serverId, DateTime startUtc, DateTime endUtc, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var rows = new List<MemoryGrantWindowRow>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = commandTimeoutSeconds;
        StoreReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new MemoryGrantWindowRow(
                reader.IsDBNull(0) ? null : reader.GetInt16(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.GetInt64(2),
                reader.GetDateTime(3),
                reader.GetDateTime(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.GetDateTime(6),
                reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.GetDateTime(11)));
        }

        return rows;
    }
}
