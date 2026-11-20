/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Metrics;

/// <summary>One server-set reading: the metric, its non-server labels (fixed values only) and the value.</summary>
internal sealed record ServerSetValue(int ServerId, string Metric, IReadOnlyList<KeyValuePair<string, string>> Labels, double Value);

/// <summary>
/// #9013: the metrics port's server set. Each statement reads the NEWEST collection per enabled SQL Server from one
/// collect table, inside that table's freshness bound, so a stopped collector's family goes absent instead of
/// repeating its last reading.
///
/// <para><b>One probe per server, never a correlated MAX</b> (#3895, #8013). Driven from the registry, each server
/// gets its own <c>ORDER BY collection_time DESC LIMIT 1</c> on <c>idx_&lt;table&gt;_time</c>, bounded below by the
/// freshness cutoff so a server with no fresh row costs one short descent, then the aggregate reads the rows AT that
/// instant by equality. A PostgreSQL target never writes these tables and is skipped by
/// <see cref="DarlingFleetReader.SqlServerCollectedTargetSql"/>.</para>
///
/// <para><b>Row shape.</b> Every statement returns <c>(server_id, metric, label1_key, label1_value, label2_key,
/// label2_value, value)</c>, label columns NULL when unused (the disk family is the only one with two), so one row loop
/// serves all ten. A NULL value is "no sample" and is dropped here, never written as 0. Label values are literals or
/// CASE results over fixed strings: no text from a monitored server is ever selected out of a statement.</para>
/// </summary>
internal static class DarlingMetricsServerSetReader
{
    /* #9013: a collection is fresh when it is newer than the larger of the floor below and three of the collector's
       intervals on that server, so a collector an operator scheduled every 15 minutes or slower keeps its series
       between samples instead of losing them while collection is healthy. The floors are the bounds for the shipped
       cadences: 15 minutes for the one-minute collectors, 30 for the five-minute session summary, 3 hours for the
       hourly database sizes. */
    internal const int IntervalMultiplier = 3;
    internal const int OneMinuteFloorMinutes = 15;
    internal const int FiveMinuteFloorMinutes = 30;
    internal const int HourlyFloorMinutes = 180;

    /* The statements are static readonly rather than const: the cutoffs come from CollectorFreshnessSql and the wait
       category CASE from WaitCategorySql, neither of which is a compile-time constant. */

    private static readonly string Target = $@"WHERE s.is_enabled
AND   {DarlingFleetReader.SqlServerCollectedTargetSql}";

    /// <summary>SQL and other-process CPU of the newest ring-buffer sample. One poll writes up to 60 samples under one
    /// <c>collection_time</c>, so the sample order breaks the tie, as <c>FleetCpuSql</c> does.</summary>
    internal static readonly string CpuSql = $@"
SELECT s.server_id, v.metric, NULL::text AS label1_key, NULL::text AS label1_value, NULL::text AS label2_key, NULL::text AS label2_value, v.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT sqlserver_cpu_utilization, other_process_cpu_utilization
    FROM v_cpu_utilization_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("cpu_utilization", OneMinuteFloorMinutes)}
    ORDER BY collection_time DESC, sample_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    VALUES
        ('darling_cpu_sql_percent', CAST(latest.sqlserver_cpu_utilization AS double precision)),
        ('darling_cpu_other_percent', CAST(latest.other_process_cpu_utilization AS double precision))
) AS v (metric, value)
{Target}";

    /// <summary>Runnable tasks of the newest collection, summed over its rows: an older store can split one collection
    /// across several rows (#5710), as <see cref="CpuSchedulerCollectionSql.CombinedColumns"/> combines them.</summary>
    internal static readonly string SchedulerSql = $@"
SELECT s.server_id, 'darling_scheduler_runnable_tasks' AS metric, NULL::text AS label1_key, NULL::text AS label1_value, NULL::text AS label2_key, NULL::text AS label2_value, agg.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT collection_time
    FROM v_cpu_scheduler_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("cpu_scheduler_stats", OneMinuteFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    SELECT CAST(SUM(c.total_runnable_tasks_count) AS double precision) AS value
    FROM v_cpu_scheduler_stats AS c
    WHERE c.server_id = s.server_id
    AND   c.collection_time = latest.collection_time
) AS agg
{Target}";

    /// <summary>
    /// The perfmon counters of the newest collection. Per (counter, object), <c>_Total</c> alone when the object has
    /// one, else the sum of its instances; the objects are then summed and the interval is the MAX over every row,
    /// the rules of <see cref="DarlingTrendReader.PerfmonTrendSql"/> (a summed interval would divide by the instance
    /// count). A rate whose interval is 0 or NULL is NULL, so it is absent. <c>Processes blocked</c> is a gauge and
    /// reads <c>cntr_value</c>.
    /// <para>No <c>is_artifact</c> filter: the trend read's artifact needs both a previous and a NEXT collection
    /// (<see cref="WaitStatisticsArtifact.ArtifactPredicateSql"/>), so on the newest collection it is never true, and
    /// it only ever matches the Wait Statistics object, which carries none of these counters.</para>
    /// </summary>
    internal static readonly string PerfmonSql = $@"
SELECT s.server_id, agg.metric, NULL::text AS label1_key, NULL::text AS label1_value, NULL::text AS label2_key, NULL::text AS label2_value, agg.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT collection_time
    FROM v_perfmon_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("perfmon_stats", OneMinuteFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    SELECT
        m.metric,
        CASE
            WHEN m.is_gauge THEN CAST(SUM(o.cntr_value) AS double precision)
            WHEN MAX(o.interval_seconds) > 0 THEN CAST(SUM(o.delta_cntr_value) AS double precision) / MAX(o.interval_seconds)
        END AS value
    FROM
    (
        SELECT
            p.counter_name,
            CASE WHEN bool_or(p.instance_name = '_Total')
                 THEN SUM(p.cntr_value) FILTER (WHERE p.instance_name = '_Total')
                 ELSE SUM(p.cntr_value) END AS cntr_value,
            CASE WHEN bool_or(p.instance_name = '_Total')
                 THEN SUM(p.delta_cntr_value) FILTER (WHERE p.instance_name = '_Total')
                 ELSE SUM(p.delta_cntr_value) END AS delta_cntr_value,
            NULLIF(MAX(p.sample_interval_seconds), 0) AS interval_seconds
        FROM v_perfmon_stats AS p
        WHERE p.server_id = s.server_id
        AND   p.collection_time = latest.collection_time
        AND   p.counter_name IN
              ('Batch Requests/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec', 'Transactions/sec',
               'Lock Waits/sec', 'Lazy writes/sec', 'Page reads/sec', 'Page writes/sec', 'Free list stalls/sec',
               'Log Flushes/sec', 'Log Bytes Flushed/sec', 'Processes blocked')
        GROUP BY p.counter_name, p.object_name
    ) AS o
    JOIN
    (
        VALUES
            ('Batch Requests/sec', 'darling_batch_requests_per_second', false),
            ('SQL Compilations/sec', 'darling_compilations_per_second', false),
            ('SQL Re-Compilations/sec', 'darling_recompilations_per_second', false),
            ('Transactions/sec', 'darling_transactions_per_second', false),
            ('Lock Waits/sec', 'darling_lock_waits_per_second', false),
            ('Lazy writes/sec', 'darling_lazy_writes_per_second', false),
            ('Page reads/sec', 'darling_page_reads_per_second', false),
            ('Page writes/sec', 'darling_page_writes_per_second', false),
            ('Free list stalls/sec', 'darling_free_list_stalls_per_second', false),
            ('Log Flushes/sec', 'darling_log_flushes_per_second', false),
            ('Log Bytes Flushed/sec', 'darling_log_bytes_flushed_per_second', false),
            ('Processes blocked', 'darling_processes_blocked', true)
    ) AS m (counter_name, metric, is_gauge)
      ON m.counter_name = o.counter_name
    GROUP BY m.metric, m.is_gauge
) AS agg
{Target}";

    /// <summary>Sessions by state from the newest summary (a 5-minute collector, so the 30-minute floor). The
    /// <c>top_*</c> columns name an application and a host and are never selected.</summary>
    internal static readonly string SessionsSql = $@"
SELECT s.server_id, 'darling_sessions' AS metric, 'state' AS label1_key, v.state AS label1_value, NULL::text AS label2_key, NULL::text AS label2_value, v.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT running_sessions, sleeping_sessions, background_sessions, dormant_sessions
    FROM v_session_summary_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("session_summary_stats", FiveMinuteFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    VALUES
        ('running', CAST(latest.running_sessions AS double precision)),
        ('sleeping', CAST(latest.sleeping_sessions AS double precision)),
        ('background', CAST(latest.background_sessions AS double precision)),
        ('dormant', CAST(latest.dormant_sessions AS double precision))
) AS v (state, value)
{Target}";

    /// <summary>The four memory gauges of the newest memory snapshot.</summary>
    internal static readonly string MemorySql = $@"
SELECT s.server_id, v.metric, NULL::text AS label1_key, NULL::text AS label1_value, NULL::text AS label2_key, NULL::text AS label2_value, v.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT total_server_memory_mb, target_server_memory_mb, plan_cache_mb, available_physical_memory_mb
    FROM v_memory_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("memory_stats", OneMinuteFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    VALUES
        ('darling_memory_total_server_mb', CAST(latest.total_server_memory_mb AS double precision)),
        ('darling_memory_target_server_mb', CAST(latest.target_server_memory_mb AS double precision)),
        ('darling_memory_plan_cache_mb', CAST(latest.plan_cache_mb AS double precision)),
        ('darling_memory_os_available_mb', CAST(latest.available_physical_memory_mb AS double precision))
) AS v (metric, value)
{Target}";

    /// <summary>Grant waiters and granted MB on the regular semaphore, summed over pools, as
    /// <see cref="DarlingFleetReader.FleetMemoryPressureSql"/> reads them (#7676).</summary>
    internal static readonly string GrantsSql = $@"
SELECT s.server_id, v.metric, NULL::text AS label1_key, NULL::text AS label1_value, NULL::text AS label2_key, NULL::text AS label2_value, v.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT collection_time
    FROM v_memory_grant_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("memory_grant_stats", OneMinuteFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    SELECT
        CAST(SUM(m.waiter_count) AS double precision) AS waiter_count,
        CAST(SUM(m.granted_memory_mb) AS double precision) AS granted_memory_mb
    FROM v_memory_grant_stats AS m
    WHERE m.server_id = s.server_id
    AND   m.collection_time = latest.collection_time
    AND   COALESCE(m.resource_semaphore_id, 0) = 0
) AS agg
CROSS JOIN LATERAL
(
    VALUES
        ('darling_memory_grants_pending', agg.waiter_count),
        ('darling_memory_granted_mb', agg.granted_memory_mb)
) AS v (metric, value)
{Target}";

    /// <summary>File IO of the newest collection by (file_type, scope): LOG files are <c>log</c> and everything else
    /// (ROWS, FILESTREAM, FULLTEXT) <c>data</c>; tempdb is <c>tempdb</c> and every other database <c>user</c>. Only rows
    /// with a known interval count. The database, file and physical names are read inside the statement and never
    /// selected out of it.</summary>
    internal static readonly string FileIoSql = $@"
SELECT s.server_id, v.metric, 'file_type' AS label1_key, g.file_type AS label1_value, 'scope' AS label2_key, g.scope AS label2_value, v.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT collection_time
    FROM v_file_io_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("file_io_stats", OneMinuteFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    SELECT
        CASE WHEN f.file_type = 'LOG' THEN 'log' ELSE 'data' END AS file_type,
        CASE WHEN f.database_name = 'tempdb' THEN 'tempdb' ELSE 'user' END AS scope,
        CAST(SUM(f.delta_stall_read_ms) AS double precision) / NULLIF(SUM(f.delta_reads), 0) AS read_latency_ms,
        CAST(SUM(f.delta_stall_write_ms) AS double precision) / NULLIF(SUM(f.delta_writes), 0) AS write_latency_ms,
        CAST(SUM(f.delta_read_bytes) AS double precision) / 1048576 / NULLIF(MAX(f.sample_interval_seconds), 0) AS read_mb_per_second,
        CAST(SUM(f.delta_write_bytes) AS double precision) / 1048576 / NULLIF(MAX(f.sample_interval_seconds), 0) AS write_mb_per_second,
        CAST(SUM(f.delta_reads) AS double precision) / NULLIF(MAX(f.sample_interval_seconds), 0) AS reads_per_second,
        CAST(SUM(f.delta_writes) AS double precision) / NULLIF(MAX(f.sample_interval_seconds), 0) AS writes_per_second
    FROM v_file_io_stats AS f
    WHERE f.server_id = s.server_id
    AND   f.collection_time = latest.collection_time
    AND   f.sample_interval_seconds > 0
    GROUP BY 1, 2
) AS g
CROSS JOIN LATERAL
(
    VALUES
        ('darling_disk_read_latency_ms', g.read_latency_ms),
        ('darling_disk_write_latency_ms', g.write_latency_ms),
        ('darling_disk_read_mb_per_second', g.read_mb_per_second),
        ('darling_disk_write_mb_per_second', g.write_mb_per_second),
        ('darling_disk_reads_per_second', g.reads_per_second),
        ('darling_disk_writes_per_second', g.writes_per_second)
) AS v (metric, value)
{Target}";

    /// <summary>The lowest free share across the server's volumes in the newest size collection (an hourly collector,
    /// so the 3-hour floor). Every file on a volume repeats the volume's figures, so the rows are first folded to one
    /// per <see cref="VolumeKey.Sql"/> (#7704: a Linux volume has no mount point name).</summary>
    internal static readonly string VolumeSql = $@"
SELECT s.server_id, 'darling_disk_volume_min_free_percent' AS metric, NULL::text AS label1_key, NULL::text AS label1_value, NULL::text AS label2_key, NULL::text AS label2_value, agg.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT collection_time
    FROM v_database_size_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("database_size_stats", HourlyFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    SELECT MIN(vol.free_percent) AS value
    FROM
    (
        SELECT MIN(CAST(d.volume_free_mb AS double precision) / CAST(d.volume_total_mb AS double precision) * 100) AS free_percent
        FROM
        (
            SELECT {VolumeKey.Sql} AS volume_key, volume_free_mb, volume_total_mb
            FROM v_database_size_stats
            WHERE server_id = s.server_id
            AND   collection_time = latest.collection_time
            AND   volume_total_mb > 0
        ) AS d
        GROUP BY d.volume_key
    ) AS vol
) AS agg
{Target}";

    /// <summary>Wait time per category per second and the signal share, from the newest wait collection. Only rows with
    /// a known interval and some wait time count; the category is <see cref="WaitCategorySql"/>'s, lowercased, over the
    /// rtrimmed wait type (older rows carry a trailing space).</summary>
    internal static readonly string WaitsSql = $@"
SELECT s.server_id, agg.metric, agg.label1_key, agg.label1_value, NULL::text AS label2_key, NULL::text AS label2_value, agg.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT collection_time
    FROM v_wait_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("wait_stats", OneMinuteFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    WITH w AS
    (
        SELECT
            lower({WaitCategorySql.CaseExpression("rtrim(wait_type)")}) AS category,
            delta_wait_time_ms,
            delta_signal_wait_time_ms,
            sample_interval_seconds
        FROM v_wait_stats
        WHERE server_id = s.server_id
        AND   collection_time = latest.collection_time
        AND   sample_interval_seconds > 0
        AND   delta_wait_time_ms > 0
    )
    SELECT
        'darling_wait_ms_per_second' AS metric,
        'category' AS label1_key,
        w.category AS label1_value,
        CAST(SUM(w.delta_wait_time_ms) AS double precision) / NULLIF(MAX(w.sample_interval_seconds), 0) AS value
    FROM w
    GROUP BY w.category
    UNION ALL
    SELECT
        'darling_signal_wait_percent',
        NULL::text,
        NULL::text,
        CAST(SUM(w.delta_signal_wait_time_ms) AS double precision) / NULLIF(SUM(w.delta_wait_time_ms), 0) * 100
    FROM w
) AS agg
{Target}";

    /// <summary>tempdb space by kind, unallocated space and log use of the newest snapshot. The <c>holder_*</c> and
    /// <c>top_session_*</c> columns name sessions, logins and programs and are never selected.</summary>
    internal static readonly string TempdbSql = $@"
SELECT s.server_id, v.metric, v.label1_key, v.label1_value, NULL::text AS label2_key, NULL::text AS label2_value, v.value
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb, unallocated_mb, log_used_percent
    FROM v_tempdb_stats
    WHERE server_id = s.server_id
    AND   collection_time >= {Fresh("tempdb_stats", OneMinuteFloorMinutes)}
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    VALUES
        ('darling_tempdb_used_mb', 'kind', 'user_objects', CAST(latest.user_object_reserved_mb AS double precision)),
        ('darling_tempdb_used_mb', 'kind', 'internal_objects', CAST(latest.internal_object_reserved_mb AS double precision)),
        ('darling_tempdb_used_mb', 'kind', 'version_store', CAST(latest.version_store_reserved_mb AS double precision)),
        ('darling_tempdb_unallocated_mb', NULL, NULL, CAST(latest.unallocated_mb AS double precision)),
        ('darling_tempdb_log_used_percent', NULL, NULL, CAST(latest.log_used_percent AS double precision))
) AS v (metric, label1_key, label1_value, value)
{Target}";

    private static readonly string[] Statements =
    {
        CpuSql, SchedulerSql, PerfmonSql, SessionsSql, MemorySql, GrantsSql, FileIoSql, VolumeSql, WaitsSql, TempdbSql,
    };

    /// <summary>Runs the ten statements in order, one after another, on <paramref name="connection"/>.
    /// <paramref name="nowUtc"/> is the store's clock (naive UTC) the freshness bounds are measured from.</summary>
    internal static async Task<IReadOnlyList<ServerSetValue>> ReadAsync(NpgsqlConnection connection, DateTime nowUtc, CancellationToken ct)
    {
        var now = DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified);
        var values = new List<ServerSetValue>();
        foreach (var sql in Statements)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now });
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                if (reader.IsDBNull(6))
                {
                    continue;
                }

                var labels = new List<KeyValuePair<string, string>>(2);
                AddLabel(reader, 2, labels);
                AddLabel(reader, 4, labels);
                values.Add(new ServerSetValue(reader.GetInt32(0), reader.GetString(1), labels, reader.GetDouble(6)));
            }
        }

        return values;
    }

    private static void AddLabel(NpgsqlDataReader reader, int keyOrdinal, List<KeyValuePair<string, string>> labels)
    {
        if (reader.IsDBNull(keyOrdinal))
        {
            return;
        }

        labels.Add(new(reader.GetString(keyOrdinal), reader.GetString(keyOrdinal + 1)));
    }

    /// <summary>The freshness cutoff for one statement's collector, measured from <c>$1</c> for the LATERAL's server.</summary>
    private static string Fresh(string collectorName, int floorMinutes) =>
        CollectorFreshnessSql.Cutoff(
            collectorName, IntervalMultiplier, "s.server_id", "$1", floorMinutes.ToString(CultureInfo.InvariantCulture));
}
