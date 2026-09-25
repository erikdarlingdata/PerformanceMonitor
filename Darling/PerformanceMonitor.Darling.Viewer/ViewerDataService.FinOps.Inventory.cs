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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Server Inventory reads. Lite queries each target LIVE (<c>GetServerPropertiesLiveAsync</c>) — the
/// headless viewer can't reach targets, so the inventory rows come from the COLLECTED
/// <c>server_properties</c> table (Darling collects it; <c>PgFactCollector.Config.ServerPropertiesSql</c>
/// reads it the same way) joined to the <c>servers</c> registry, with the collected metrics
/// (<see cref="ServerMetricsSql"/>) overlaid per server by the loader. Column parity vs Lite's live query:
/// the collected table carries edition/version/level/CPU/memory/sockets/cores/HADR/clustered, so those
/// surface; it does NOT carry sqlserver_start_time / host OS / AG replica role, so those Lite columns are
/// omitted (nothing stubbed). SQL kept in <c>public const</c> so tests pin it.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Collected 24h-CPU / storage / idle-DB / provisioning metrics for EVERY server, in one round trip (#4227).
    /// $1 cpu cutoff (24h), $2 idle cutoff (7d). Used to be one call per server (<c>GetServerMetricsAsync</c>,
    /// 43 round trips on a 43-server fleet); the five CTEs group naturally by <c>server_id</c>, so they read the
    /// fleet at once instead — 24h CPU and grants via <c>GROUP BY server_id</c>, latest memory and size snapshots
    /// via a per-server <c>LATERAL … LIMIT 1</c> descent (the <c>ServerFreshnessSql</c> shape, #3895).
    ///
    /// <para><b>The idle-database check</b> used to be a raw 7-day <c>v_query_stats</c> scan per server — the
    /// #4227 cost. <c>active_dbs</c> below is the RAW shape (this constant, unrouted): every database with any
    /// execution in <c>[$2, now)</c>. <see cref="ServerMetricsSqlFor"/> routes it instead through the stitched
    /// rollup (<c>query_stats_db_hourly</c> / <c>_interval_hourly</c>, #3653) for every WHOLE hour, plus raw for
    /// only the two edges the rollup cannot answer: the partial leading hour the bucket grain can't slice, and
    /// the trailing range the aggregate — <c>materialized_only = true</c> (#4186 froze the legacy; NEVER read it
    /// alone) — has not materialized yet. The raw cost then stays in proportion to those two edges rather than
    /// the whole window, and the check is exact rather than "whatever raw retention still holds" (#4227's other
    /// finding: a 4-day raw retention silently narrowed a 7-day claim).</para>
    /// </summary>
    public const string ServerMetricsSql = @"
WITH cpu_24h AS (
    SELECT
        server_id,
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct
    FROM v_cpu_utilization_stats
    WHERE collection_time >= $1
    GROUP BY server_id
),
/* Only the worker counts are consumed now: memory_ratio used to feed this read's own CASE, and that
   CASE was the #2246 bug. The verdict comes from ProvisioningVerdict, so the division would be dead. */
mem_latest AS (
    SELECT
        s.server_id,
        latest.max_workers_count,
        latest.current_workers_count
    FROM servers s
    CROSS JOIN LATERAL (
        SELECT max_workers_count, current_workers_count
        FROM v_memory_stats
        WHERE server_id = s.server_id
        ORDER BY collection_time DESC
        LIMIT 1
    ) AS latest
),
/* Same workspace-memory pressure signals as the drill-down read, so the INVENTORY GRID cannot classify a
   server by a rule the drill-down no longer uses (#2246 — this grid is the screen the field report was
   looking at). */
grants AS (
    SELECT
        server_id,
        MAX(waiter_count) AS max_grant_waiters,
        SUM(COALESCE(timeout_error_count_delta, 0)) AS grant_timeouts,
        SUM(COALESCE(forced_grant_count_delta, 0)) AS forced_grants,
        MAX(100.0 * granted_memory_mb / NULLIF(target_memory_mb, 0)) AS grant_utilization_pct
    FROM v_memory_grant_stats
    WHERE collection_time >= $1
    GROUP BY server_id
),
/* The latest size-snapshot TIME per server (LATERAL LIMIT 1, indexed descent), shared by storage_totals
   (sums every database at that instant) and latest_dbs (the idle check's known-databases universe) so
   neither re-derives it. */
size_latest AS (
    SELECT
        s.server_id,
        latest_time.collection_time
    FROM servers s
    CROSS JOIN LATERAL (
        SELECT collection_time
        FROM v_database_size_stats
        WHERE server_id = s.server_id
        ORDER BY collection_time DESC
        LIMIT 1
    ) AS latest_time
),
storage_totals AS (
    SELECT
        sl.server_id,
        SUM(vd.total_size_mb) / 1024.0 AS total_storage_gb
    FROM size_latest sl
    JOIN v_database_size_stats vd
      ON vd.server_id = sl.server_id
     AND vd.collection_time = sl.collection_time
    GROUP BY sl.server_id
),
latest_dbs AS (
    SELECT DISTINCT
        sl.server_id,
        vd.database_name
    FROM size_latest sl
    JOIN v_database_size_stats vd
      ON vd.server_id = sl.server_id
     AND vd.collection_time = sl.collection_time
    WHERE vd.database_name NOT IN ('master', 'model', 'msdb', 'tempdb', 'PerformanceMonitor')
),
active_dbs AS (
    SELECT DISTINCT server_id, database_name
    FROM v_query_stats
    WHERE collection_time >= $2
    AND   delta_execution_count > 0
),
idle_dbs AS (
    SELECT server_id, COUNT(*) AS idle_db_count
    FROM (
        SELECT server_id, database_name FROM latest_dbs
        EXCEPT
        SELECT server_id, database_name FROM active_dbs
    ) AS idle
    GROUP BY server_id
)
SELECT
    s.server_id,
    c.avg_cpu_pct,
    st.total_storage_gb,
    id.idle_db_count,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    COALESCE(m.max_workers_count, 0),
    COALESCE(m.current_workers_count, 0),
    COALESCE(g.max_grant_waiters, 0),
    COALESCE(g.grant_timeouts, 0),
    COALESCE(g.forced_grants, 0),
    COALESCE(g.grant_utilization_pct, 0)
FROM servers s
LEFT JOIN cpu_24h c ON c.server_id = s.server_id
LEFT JOIN mem_latest m ON m.server_id = s.server_id
LEFT JOIN storage_totals st ON st.server_id = s.server_id
LEFT JOIN idle_dbs id ON id.server_id = s.server_id
LEFT JOIN grants g ON g.server_id = s.server_id
WHERE s.server_id <> 0";

    /// <summary>The RAW <c>active_dbs</c> CTE in <see cref="ServerMetricsSql"/>, verbatim — the anchor
    /// <see cref="ServerMetricsSqlFor"/> replaces to route the idle check through the rollup.</summary>
    private const string IdleActiveDbsRawCte = @"active_dbs AS (
    SELECT DISTINCT server_id, database_name
    FROM v_query_stats
    WHERE collection_time >= $2
    AND   delta_execution_count > 0
),";

    /// <summary>
    /// The routed <c>active_dbs</c>: every WHOLE hour from <paramref name="relationSql"/> (#3653's stitched
    /// legacy/successor, already split at the successor's own floor) from <paramref name="ceilingHourUtc"/>,
    /// UNION the raw leading edge <c>[$2, ceilingHour)</c> the bucket grain can't slice, UNION the raw trailing
    /// edge <c>[watermark, now)</c> the aggregate has not materialized yet (<see cref="RollupMaterializationWatermark"/>).
    /// Three <c>UNION</c>s rather than one combined predicate because each arm reads a different relation; a
    /// database counts active if ANY of the three shows an execution (#4227's boolean rule).
    /// </summary>
    private static string IdleActiveDbsForCagg(string relationSql, DateTime ceilingHourUtc, DateTime watermarkUtc)
    {
        var ceiling = $"TIMESTAMP '{ceilingHourUtc:yyyy-MM-dd HH:mm:ss.ffffff}'";
        var watermark = $"TIMESTAMP '{watermarkUtc:yyyy-MM-dd HH:mm:ss.ffffff}'";

        return $@"active_dbs AS (
    SELECT DISTINCT server_id, database_name
    FROM {relationSql}
    WHERE bucket >= {ceiling}
    AND   execution_count_sum > 0

    UNION

    SELECT DISTINCT server_id, database_name
    FROM v_query_stats
    WHERE collection_time >= $2
    AND   collection_time < {ceiling}
    AND   delta_execution_count > 0

    UNION

    SELECT DISTINCT server_id, database_name
    FROM v_query_stats
    WHERE collection_time >= {watermark}
    AND   delta_execution_count > 0
),";
    }

    /// <summary>
    /// <see cref="ServerMetricsSql"/> with the idle check routed through the hourly stitched rollup
    /// (<paramref name="coverage"/>) for everything at or after <paramref name="watermarkUtc"/>'s bucket ceiling
    /// back to <c>ceilingHour(idleCutoffUtc)</c>, raw for the two edges outside that. Callers only take this path
    /// once <see cref="RollupMaterializationWatermark.GetAsync"/> has answered a real watermark for the
    /// database-grain hourly family — with no watermark (nothing materialized, or the family doesn't exist on
    /// this store) <see cref="ServerMetricsSql"/> unrouted is the correct read, not this one.
    /// </summary>
    public static string ServerMetricsSqlFor(RollupCoverage coverage, DateTime idleCutoffUtc, DateTime watermarkUtc)
    {
        ArgumentNullException.ThrowIfNull(coverage);

        var floorHour = TimescaleSupport.AlignDown(idleCutoffUtc, TimescaleSupport.HourlyBucket);
        var ceilingHour = floorHour == idleCutoffUtc ? floorHour : floorHour + TimescaleSupport.HourlyBucket;

        var relationSql = coverage.StitchedRelationSql(
            TimescaleSupport.QueryStatsDbHourlyView, "f", ceilingHour, RollupCoverage.StitchTier.Hourly);

        return RouteOrThrow(
            ServerMetricsSql,
            IdleActiveDbsRawCte,
            IdleActiveDbsForCagg(relationSql, ceilingHour, watermarkUtc),
            "server inventory idle databases");
    }

    /// <summary>One fleet server's overlay metrics — <see cref="GetServerMetricsAsync"/>'s per-row result.</summary>
    public readonly record struct ServerMetricsRow(decimal? AvgCpuPct, decimal? StorageTotalGb, int? IdleDbCount, string? ProvisioningStatus);

    /// <summary>
    /// Every server's overlay metrics, ONE round trip (#4227). Routes the idle check through the rollup when
    /// the database-grain hourly family exists here AND has a real materialization watermark (the successor's
    /// when it exists, else the legacy's — whichever is the LIVE one); falls back to <see cref="ServerMetricsSql"/>
    /// unrouted — the exact same predicate the fleet used to run per server — when neither holds, which is
    /// always correct, just the slower path (a plain-PostgreSQL store, or a fresh TimescaleDB store that has not
    /// materialized anything yet).
    /// </summary>
    public async Task<Dictionary<int, ServerMetricsRow>> GetServerMetricsAsync(CancellationToken cancellationToken = default)
    {
        var cpuCutoff = DateTime.UtcNow.AddHours(-24);
        var idleCutoff = DateTime.UtcNow.AddDays(-7);

        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);

        DateTime? watermark = null;
        if (rollups.DbGrainHourly)
        {
            var liveRelation = rollups.DbGrainIntervalHourly
                ? TimescaleSupport.QueryStatsDbIntervalHourlyView
                : TimescaleSupport.QueryStatsDbHourlyView;
            watermark = await RollupMaterializationWatermark.GetAsync(
                _dataSource, liveRelation, TimescaleSupport.HourlyBucket,
                ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        }

        var sql = watermark is DateTime mark
            ? ServerMetricsSqlFor(coverage, idleCutoff, mark)
            : ServerMetricsSql;

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cpuCutoff, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(idleCutoff, DateTimeKind.Unspecified) });

        var results = new Dictionary<int, ServerMetricsRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* The verdict is computed HERE rather than as a SQL CASE, so this grid and the drill-down
               cannot disagree — they now call the same predicate. The old inline CASE was copies 5 and 6
               of the ratio bug, on the screen the field report was actually looking at (#2246). */
            var status = ProvisioningVerdict.Evaluate(
                avgCpuPercent: reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
                maxCpuPercent: reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                p95CpuPercent: reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)),
                maxGrantWaiters: reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                grantTimeouts: reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                forcedGrants: reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                grantUtilizationPercent: reader.IsDBNull(11) ? 0m : Convert.ToDecimal(reader.GetValue(11)),
                maxWorkers: reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                currentWorkers: reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)));

            results[reader.GetInt32(0)] = new ServerMetricsRow(
                reader.IsDBNull(1) ? null : Convert.ToDecimal(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3)),
                status);
        }

        return results;
    }

    /// <summary>
    /// #1591: the Hardware Note for one inventory row, or null when hardware is present.
    ///
    /// <para>PURE so the rule is testable without a store — the codebase's pattern for read-path decisions. Keyed
    /// on cpu_count AND physical_memory_mb both being absent, because they come from the same guarded
    /// <c>sys.dm_os_sys_info</c> read in the collector: either that read succeeded and both are populated, or it
    /// was denied and both are NULL. Requiring both guards against annotating a server over a single odd NULL.</para>
    ///
    /// <para>Without this the grid renders the <c>IsDBNull ? 0</c> coalesce as a literal 0, which reads as "this
    /// server has no CPUs and no memory" rather than "we were not allowed to look".</para>
    /// </summary>
    internal static string? HardwareNoteFor(bool cpuCountIsNull, bool physicalMemoryIsNull) =>
        cpuCountIsNull && physicalMemoryIsNull
            ? "Hardware inventory (CPU, memory, sockets) unavailable - the monitoring login likely lacks VIEW SERVER STATE (VIEW DATABASE STATE on Azure SQL DB) on this server."
            : null;

    /// <summary>
    /// Latest collected properties per server joined to the registry — the Server Inventory base rows (metrics
    /// overlaid per row by the loader). DISTINCT ON keeps each server's newest server_properties row.
    /// </summary>
    public const string ServerInventorySql = @"
SELECT
    sp.server_id,
    COALESCE(s.display_name, s.server_name) AS server_name,
    sp.edition,
    sp.product_version,
    sp.product_level,
    sp.product_update_level,
    sp.engine_edition,
    sp.cpu_count,
    sp.physical_memory_mb,
    sp.socket_count,
    sp.cores_per_socket,
    sp.is_hadr_enabled,
    sp.is_clustered,
    sp.collection_time,
    sp.sqlserver_start_time,
    sp.host_os_version,
    sp.ag_replica_role,
    s.is_enabled,
    COALESCE(s.monthly_cost_usd, 0) AS monthly_cost_usd,
    /* #2359: real collection freshness, which is NOT what sp.collection_time means. server_properties
       has FrequencyMinutes 0 - collected once on server load - so its timestamp is the last service
       start, not a heartbeat. Appended LAST so no existing reader ordinal moves. */
    (SELECT MAX(cl.collection_time) FROM v_collection_log cl WHERE cl.server_id = sp.server_id) AS last_collection
FROM (
    SELECT DISTINCT ON (server_id)
        server_id, edition, product_version, product_level, product_update_level,
        engine_edition, cpu_count, physical_memory_mb, socket_count, cores_per_socket,
        is_hadr_enabled, is_clustered, collection_time,
        sqlserver_start_time, host_os_version, ag_replica_role
    FROM server_properties
    ORDER BY server_id, collection_time DESC
) sp
JOIN servers s ON s.server_id = sp.server_id
ORDER BY s.is_enabled DESC, server_name";

    public async Task<List<ServerPropertyRow>> GetServerInventoryAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerInventorySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;

        var items = new List<ServerPropertyRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var version = reader.IsDBNull(3) ? "" : reader.GetString(3);
            var level = reader.IsDBNull(4) ? "" : reader.GetString(4);
            var updateLevel = reader.IsDBNull(5) ? null : reader.GetString(5);
            var versionDisplay = !string.IsNullOrEmpty(updateLevel)
                ? $"{version} - {updateLevel}"
                : $"{version} - {level}";

            items.Add(new ServerPropertyRow
            {
                ServerId = reader.GetInt32(0),
                ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                Edition = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ProductVersion = versionDisplay,
                EngineEdition = reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                CpuCount = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7)),
                PhysicalMemoryMb = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                HardwareUnavailableReason = HardwareNoteFor(reader.IsDBNull(7), reader.IsDBNull(8)),
                SocketCount = reader.IsDBNull(9) ? null : Convert.ToInt32(reader.GetValue(9)),
                CoresPerSocket = reader.IsDBNull(10) ? null : Convert.ToInt32(reader.GetValue(10)),
                IsHadrEnabled = reader.IsDBNull(11) ? null : reader.GetBoolean(11),
                IsClustered = reader.IsDBNull(12) ? null : reader.GetBoolean(12),
                /* #2359: this is the CONFIG SNAPSHOT time, not a freshness heartbeat. Named for what it
                   is so nobody reads a days-old value as a stale metric again. */
                InventoryAsOf = reader.IsDBNull(13) ? null : ViewerTimeHelper.ForDisplay(reader.GetDateTime(13)),
                /* sqlserver_start_time is the server's LOCAL clock — read verbatim, shown as-is like Lite
                   (UptimeDisplay = Now - start). host OS + AG role are the collected guarded values. */
                SqlServerStartTime = reader.IsDBNull(14) ? null : reader.GetDateTime(14),
                HostOsVersion = reader.IsDBNull(15) ? "" : reader.GetString(15),
                AgReplicaRole = reader.IsDBNull(16) ? "Standalone" : reader.GetString(16),
                /* #2359: is_enabled sits at 17, so monthly_cost_usd moved to 18. A registered-but-disabled
                   server keeps its final collection_time forever, and without this flag that timestamp reads
                   as a stale metric rather than as the date monitoring stopped. */
                IsEnabled = reader.IsDBNull(17) || reader.GetBoolean(17),
                MonthlyCost = reader.IsDBNull(18) ? 0m : Convert.ToDecimal(reader.GetValue(18)),
                LastCollected = reader.IsDBNull(19) ? null : ViewerTimeHelper.ForDisplay(reader.GetDateTime(19))
            });
        }
        return items;
    }
}
