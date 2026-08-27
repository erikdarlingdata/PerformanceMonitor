/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Validates that a database name exists on the server and returns the properly quoted name.
    /// This prevents SQL injection via malicious database names.
    /// </summary>
    private static async Task<string?> GetValidatedDatabaseNameAsync(SqlConnection connection, string databaseName)
    {
        using var command = new SqlCommand(@"
SELECT
    quoted_name = QUOTENAME(d.name)
FROM sys.databases AS d
WHERE d.name = @database_name;", connection);
        command.Parameters.Add(new SqlParameter("@database_name", SqlDbType.NVarChar, 128) { Value = databaseName });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }

    /// <summary>
    /// Gets top queries by CPU for a server over a time period.
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetQueryStatsSlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
    date_trunc('hour', collection_time) AS bucket,
    COUNT(DISTINCT query_hash) AS query_count,
    COALESCE(SUM(delta_worker_time), 0) / 1000.0 AS total_cpu_ms,
    COALESCE(SUM(delta_elapsed_time), 0) / 1000.0 AS total_elapsed_ms,
    COALESCE(SUM(delta_logical_reads), 0) AS total_reads,
    COALESCE(SUM(delta_logical_writes), 0) AS total_writes,
    COALESCE(SUM(delta_physical_reads), 0) AS total_physical_reads
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
GROUP BY date_trunc('hour', collection_time)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                TotalCpu = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalWrites = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TotalLogicalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalPhysicalReads = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                Value = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)), // default: total CPU
            });
        }
        return items;
    }

    public async Task<List<QueryStatsRow>> GetTopQueriesByCpuAsync(int serverId, int hoursBack = 24, int top = 50, DateTime? fromDate = null, DateTime? toDate = null, int utcOffsetMinutes = 0, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetTopQueriesByCpuAsync", "v_query_stats top N by CPU");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 6, out var dbValues);

        command.CommandText = @"
WITH ranked AS (
    SELECT
        database_name,
        query_hash,
        MAX(last_execution_time) AS last_execution_time,
        MAX(creation_time) AS creation_time,
        SUM(delta_execution_count) AS total_executions,
        SUM(delta_worker_time) AS total_cpu_us,
        SUM(delta_elapsed_time) AS total_elapsed_us,
        SUM(delta_logical_reads) AS total_reads,
        SUM(delta_rows) AS total_rows,
        SUM(delta_logical_writes) AS total_writes,
        SUM(delta_physical_reads) AS total_physical_reads,
        SUM(delta_spills) AS total_spills,
        MIN(min_dop) AS min_dop,
        MAX(max_dop) AS max_dop,
        MIN(min_worker_time) AS min_worker_time,
        MAX(max_worker_time) AS max_worker_time,
        MIN(min_elapsed_time) AS min_elapsed_time,
        MAX(max_elapsed_time) AS max_elapsed_time,
        MIN(min_physical_reads) AS min_physical_reads,
        MAX(max_physical_reads) AS max_physical_reads,
        MIN(min_rows) AS min_rows,
        MAX(max_rows) AS max_rows,
        MIN(min_grant_kb) AS min_grant_kb,
        MAX(max_grant_kb) AS max_grant_kb,
        MIN(min_spills) AS min_spills,
        MAX(max_spills) AS max_spills,
        MAX(query_plan_hash) AS query_plan_hash,
        MAX(sql_handle) AS sql_handle,
        MAX(plan_handle) AS plan_handle,
        MIN(min_used_grant_kb) AS min_used_grant_kb,
        MAX(max_used_grant_kb) AS max_used_grant_kb,
        MIN(min_ideal_grant_kb) AS min_ideal_grant_kb,
        MAX(max_ideal_grant_kb) AS max_ideal_grant_kb,
        MIN(min_reserved_threads) AS min_reserved_threads,
        MAX(max_reserved_threads) AS max_reserved_threads,
        MIN(min_used_threads) AS min_used_threads,
        MAX(max_used_threads) AS max_used_threads,
        MAX(total_clr_time) AS total_clr_time,
        MAX(plan_generation_num) AS plan_generation_num,
        MAX(CAST(delta_worker_time AS DOUBLE PRECISION) / NULLIF(sample_interval_seconds, 0) / 1000.0) AS worker_time_per_second,
        /* #2012: distinct statement texts merged into this group. query_hash is a SHAPE hash, so
           ad-hoc literal variants collapse - > 1 means the representative text below labels a blend
           (stage 2 folded host_object_name into the key, so INSERT...EXEC statements hosted by
           DIFFERENT procs no longer merge; only ad-hoc blends and pre-upgrade NULL-host history
           can still count > 1). DuckDB's 64-bit hash() stands in for Darling's #1767 content
           digest: comparing fixed-size hashes instead of arbitrarily long batch texts (a review
           note on the twin's asymmetry); a same-group 64-bit collision is negligible for a
           display count. */
        COUNT(DISTINCT hash(query_text)) AS distinct_texts,
        host_object_name
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   last_execution_time >= $2 + $5 * INTERVAL '1' MINUTE" + dbClause + @"
    GROUP BY database_name, query_hash, host_object_name
    HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
    ORDER BY SUM(delta_elapsed_time) DESC
    LIMIT $4 + 5
),
module AS (
    /* #1568 module attribution: one procedure_stats identity per sql_handle (latest collection_time
       wins) so a statement whose sql_handle matches a cached procedure/function/trigger inherits its
       db.schema.object and the query_stats row never fans out. Both stores persist the SAME normalized
       CONVERT(varchar(130), ..., 1) handle text, so the join key lines up; unmatched -> ad hoc. */
    SELECT
        sql_handle,
        object_name,
        schema_name,
        database_name
    FROM
    (
        SELECT
            sql_handle,
            object_name,
            schema_name,
            database_name,
            ROW_NUMBER() OVER (PARTITION BY sql_handle ORDER BY collection_time DESC) AS rn
        FROM v_procedure_stats
        WHERE server_id = $1
        AND   sql_handle IS NOT NULL
        AND   sql_handle <> ''
    ) ranked_modules
    WHERE rn = 1
)
SELECT
    r.*,
    t.query_text,
    t.query_plan_xml AS query_plan,
    m.object_name AS module_object_name,
    m.schema_name AS module_schema_name,
    m.database_name AS module_database_name
FROM ranked r
LEFT JOIN LATERAL (
    SELECT query_text, query_plan_xml
    FROM v_query_stats
    WHERE server_id = $1
    AND   query_hash = r.query_hash
    AND   database_name = r.database_name
    /* #2012 stage 2: the representative text must come from THIS group's own rows - without the
       host constraint a hash shared across host objects could label one caller's row with
       another caller's text (NOT DISTINCT FROM so ad-hoc NULL hosts still match ad-hoc rows). */
    AND   host_object_name IS NOT DISTINCT FROM r.host_object_name
    AND   query_text IS NOT NULL
    ORDER BY collection_time DESC
    LIMIT 1
) t ON TRUE
LEFT JOIN module m ON m.sql_handle = r.sql_handle
WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
ORDER BY r.total_elapsed_us DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = top });
        command.Parameters.Add(new DuckDBParameter { Value = utcOffsetMinutes });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QueryStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStatsRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                LastExecutionTime = reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                CreationTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                TotalExecutions = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                TotalCpuUs = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TotalElapsedUs = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                TotalLogicalReads = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                TotalRows = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                TotalLogicalWrites = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalPhysicalReads = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                TotalSpills = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                MinDop = reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
                MaxDop = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                MinCpuUs = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                MaxCpuUs = reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                MinElapsedUs = reader.IsDBNull(16) ? 0 : reader.GetInt64(16),
                MaxElapsedUs = reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                MinPhysicalReads = reader.IsDBNull(18) ? 0 : reader.GetInt64(18),
                MaxPhysicalReads = reader.IsDBNull(19) ? 0 : reader.GetInt64(19),
                MinRows = reader.IsDBNull(20) ? 0 : reader.GetInt64(20),
                MaxRows = reader.IsDBNull(21) ? 0 : reader.GetInt64(21),
                MinGrantKb = reader.IsDBNull(22) ? 0 : reader.GetInt64(22),
                MaxGrantKb = reader.IsDBNull(23) ? 0 : reader.GetInt64(23),
                MinSpills = reader.IsDBNull(24) ? 0 : reader.GetInt64(24),
                MaxSpills = reader.IsDBNull(25) ? 0 : reader.GetInt64(25),
                QueryPlanHash = reader.IsDBNull(26) ? "" : reader.GetString(26),
                SqlHandle = reader.IsDBNull(27) ? "" : reader.GetString(27),
                PlanHandle = reader.IsDBNull(28) ? "" : reader.GetString(28),
                MinUsedGrantKb = reader.IsDBNull(29) ? 0 : reader.GetInt64(29),
                MaxUsedGrantKb = reader.IsDBNull(30) ? 0 : reader.GetInt64(30),
                MinIdealGrantKb = reader.IsDBNull(31) ? 0 : reader.GetInt64(31),
                MaxIdealGrantKb = reader.IsDBNull(32) ? 0 : reader.GetInt64(32),
                MinReservedThreads = reader.IsDBNull(33) ? 0 : reader.GetInt64(33),
                MaxReservedThreads = reader.IsDBNull(34) ? 0 : reader.GetInt64(34),
                MinUsedThreads = reader.IsDBNull(35) ? 0 : reader.GetInt64(35),
                MaxUsedThreads = reader.IsDBNull(36) ? 0 : reader.GetInt64(36),
                TotalClrUs = reader.IsDBNull(37) ? 0 : reader.GetInt64(37),
                PlanGenerationNum = reader.IsDBNull(38) ? 0 : reader.GetInt64(38),
                WorkerTimePerSecond = reader.IsDBNull(39) ? 0 : ToDouble(reader.GetValue(39)),
                DistinctTexts = reader.IsDBNull(40) ? 0 : reader.GetInt64(40),
                HostObjectName = reader.IsDBNull(41) ? null : reader.GetString(41),
                QueryText = reader.IsDBNull(42) ? "" : reader.GetString(42),
                QueryPlan = reader.IsDBNull(43) ? null : reader.GetString(43),
                ModuleObjectName = reader.IsDBNull(44) ? "" : reader.GetString(44),
                ModuleSchemaName = reader.IsDBNull(45) ? "" : reader.GetString(45),
                ModuleDatabaseName = reader.IsDBNull(46) ? "" : reader.GetString(46)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets query stats comparison between a current time range and a baseline range.
    /// Returns delta percentages for duration, CPU, reads, and execution count.
    /// </summary>
    public async Task<List<QueryStatsComparisonItem>> GetQueryStatsComparisonAsync(
        int serverId,
        DateTime currentStart, DateTime currentEnd,
        DateTime baselineStart, DateTime baselineEnd,
        IReadOnlyList<string>? databaseNames = null)
    {
        using var _q = TimeQuery("GetQueryStatsComparisonAsync", "v_query_stats comparison");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var dbClause = BuildDbInClause(databaseNames, "database_name", 6, out var dbValues);

        command.CommandText = @"
WITH top_current AS (
    SELECT query_hash, database_name
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2 AND collection_time <= $3" + dbClause + @"
    AND   delta_execution_count > 0
    GROUP BY query_hash, database_name
    ORDER BY SUM(delta_execution_count) DESC
    LIMIT 100
),
top_baseline AS (
    SELECT query_hash, database_name
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $4 AND collection_time <= $5" + dbClause + @"
    AND   delta_execution_count > 0
    GROUP BY query_hash, database_name
    ORDER BY SUM(delta_execution_count) DESC
    LIMIT 100
),
top_hashes AS (
    SELECT DISTINCT query_hash, database_name
    FROM (
        SELECT * FROM top_current
        UNION ALL
        SELECT * FROM top_baseline
    ) combined
),
current_period AS (
    SELECT th.database_name, th.query_hash,
           SUM(qs.delta_execution_count) AS exec_count,
           SUM(qs.delta_elapsed_time)::DOUBLE PRECISION / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(qs.delta_worker_time)::DOUBLE PRECISION / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(qs.delta_physical_reads)::DOUBLE PRECISION / NULLIF(SUM(qs.delta_execution_count), 0) AS avg_reads,
           MAX(qs.query_text) AS query_text
    FROM top_hashes th
    INNER JOIN v_query_stats qs
      ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
      AND qs.database_name IS NOT DISTINCT FROM th.database_name
    WHERE qs.server_id = $1
    AND   qs.collection_time >= $2 AND qs.collection_time <= $3
    AND   qs.delta_execution_count > 0
    GROUP BY th.database_name, th.query_hash
),
baseline_period AS (
    SELECT th.database_name, th.query_hash,
           SUM(qs.delta_execution_count) AS exec_count,
           SUM(qs.delta_elapsed_time)::DOUBLE PRECISION / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(qs.delta_worker_time)::DOUBLE PRECISION / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(qs.delta_physical_reads)::DOUBLE PRECISION / NULLIF(SUM(qs.delta_execution_count), 0) AS avg_reads,
           MAX(qs.query_text) AS query_text
    FROM top_hashes th
    INNER JOIN v_query_stats qs
      ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
      AND qs.database_name IS NOT DISTINCT FROM th.database_name
    WHERE qs.server_id = $1
    AND   qs.collection_time >= $4 AND qs.collection_time <= $5
    AND   qs.delta_execution_count > 0
    GROUP BY th.database_name, th.query_hash
)
SELECT COALESCE(c.database_name, b.database_name) AS database_name,
       COALESCE(c.query_hash, b.query_hash) AS query_hash,
       COALESCE(c.query_text, b.query_text) AS query_text,
       c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
       b.exec_count AS baseline_exec_count,
       b.avg_duration_ms AS baseline_avg_duration_ms,
       b.avg_cpu_ms AS baseline_avg_cpu_ms,
       b.avg_reads AS baseline_avg_reads
FROM current_period c
FULL OUTER JOIN baseline_period b
  ON  c.database_name IS NOT DISTINCT FROM b.database_name
  AND c.query_hash IS NOT DISTINCT FROM b.query_hash;";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = currentStart });
        command.Parameters.Add(new DuckDBParameter { Value = currentEnd });
        command.Parameters.Add(new DuckDBParameter { Value = baselineStart });
        command.Parameters.Add(new DuckDBParameter { Value = baselineEnd });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QueryStatsComparisonItem>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStatsComparisonItem
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ExecutionCount = reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                AvgDurationMs = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                AvgCpuMs = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AvgReads = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                BaselineExecutionCount = reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
                BaselineAvgDurationMs = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                BaselineAvgCpuMs = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
                BaselineAvgReads = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10)),
            });
        }

        return items;
    }

    /// <summary>
    /// Gets collection-level history for a specific query hash (for drilldown).
    /// </summary>
    public async Task<List<QueryStatsHistoryRow>> GetQueryStatsHistoryAsync(int serverId, string databaseName, string queryHash, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        command.CommandText = @"
SELECT
    collection_time,
    delta_execution_count,
    delta_worker_time,
    delta_elapsed_time,
    delta_logical_reads,
    delta_logical_writes,
    delta_physical_reads,
    delta_rows,
    delta_spills,
    min_dop,
    max_dop,
    min_worker_time,
    max_worker_time,
    min_elapsed_time,
    max_elapsed_time,
    query_plan_hash,
    min_grant_kb,
    max_grant_kb,
    min_used_grant_kb,
    max_used_grant_kb,
    min_ideal_grant_kb,
    max_ideal_grant_kb,
    min_reserved_threads,
    max_reserved_threads,
    min_used_threads,
    max_used_threads,
    min_physical_reads,
    max_physical_reads,
    min_rows,
    max_rows,
    min_spills,
    max_spills,
    total_clr_time,
    creation_time,
    last_execution_time,
    execution_count,
    total_worker_time,
    total_elapsed_time,
    total_logical_reads,
    total_logical_writes,
    total_physical_reads,
    total_rows,
    total_spills,
    sql_handle,
    plan_handle,
    query_hash,
    sample_interval_seconds
FROM v_query_stats
WHERE server_id = $1
AND   database_name = $2
AND   query_hash = $3
AND   collection_time >= $4
AND   collection_time <= $5
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = queryHash });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<QueryStatsHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryStatsHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeltaExecutions = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                DeltaCpuUs = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                DeltaElapsedUs = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                DeltaLogicalReads = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                DeltaLogicalWrites = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                DeltaPhysicalReads = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                DeltaRows = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                DeltaSpills = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                MinDop = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                MaxDop = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                MinCpuUs = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                MaxCpuUs = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                MinElapsedUs = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                MaxElapsedUs = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                QueryPlanHash = reader.IsDBNull(15) ? "" : reader.GetString(15),
                MinGrantKb = reader.IsDBNull(16) ? 0 : reader.GetInt64(16),
                MaxGrantKb = reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                MinUsedGrantKb = reader.IsDBNull(18) ? 0 : reader.GetInt64(18),
                MaxUsedGrantKb = reader.IsDBNull(19) ? 0 : reader.GetInt64(19),
                MinIdealGrantKb = reader.IsDBNull(20) ? 0 : reader.GetInt64(20),
                MaxIdealGrantKb = reader.IsDBNull(21) ? 0 : reader.GetInt64(21),
                MinReservedThreads = reader.IsDBNull(22) ? 0 : reader.GetInt64(22),
                MaxReservedThreads = reader.IsDBNull(23) ? 0 : reader.GetInt64(23),
                MinUsedThreads = reader.IsDBNull(24) ? 0 : reader.GetInt64(24),
                MaxUsedThreads = reader.IsDBNull(25) ? 0 : reader.GetInt64(25),
                MinPhysicalReads = reader.IsDBNull(26) ? 0 : reader.GetInt64(26),
                MaxPhysicalReads = reader.IsDBNull(27) ? 0 : reader.GetInt64(27),
                MinRows = reader.IsDBNull(28) ? 0 : reader.GetInt64(28),
                MaxRows = reader.IsDBNull(29) ? 0 : reader.GetInt64(29),
                MinSpills = reader.IsDBNull(30) ? 0 : reader.GetInt64(30),
                MaxSpills = reader.IsDBNull(31) ? 0 : reader.GetInt64(31),
                TotalClrTimeUs = reader.IsDBNull(32) ? 0 : reader.GetInt64(32),
                CreationTime = reader.IsDBNull(33) ? (DateTime?)null : reader.GetDateTime(33),
                LastExecutionTime = reader.IsDBNull(34) ? (DateTime?)null : reader.GetDateTime(34),
                TotalExecutions = reader.IsDBNull(35) ? 0 : reader.GetInt64(35),
                TotalCpuUs = reader.IsDBNull(36) ? 0 : reader.GetInt64(36),
                TotalElapsedUs = reader.IsDBNull(37) ? 0 : reader.GetInt64(37),
                TotalLogicalReads = reader.IsDBNull(38) ? 0 : reader.GetInt64(38),
                TotalLogicalWrites = reader.IsDBNull(39) ? 0 : reader.GetInt64(39),
                TotalPhysicalReads = reader.IsDBNull(40) ? 0 : reader.GetInt64(40),
                TotalRows = reader.IsDBNull(41) ? 0 : reader.GetInt64(41),
                TotalSpills = reader.IsDBNull(42) ? 0 : reader.GetInt64(42),
                SqlHandle = reader.IsDBNull(43) ? "" : reader.GetString(43),
                PlanHandle = reader.IsDBNull(44) ? "" : reader.GetString(44),
                QueryHash = reader.IsDBNull(45) ? "" : reader.GetString(45),
                SampleIntervalSeconds = reader.IsDBNull(46) ? (int?)null : reader.GetInt32(46)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets collection-level history for a specific procedure (for drilldown).
    /// </summary>
    public async Task<List<ProcedureStatsHistoryRow>> GetProcedureStatsHistoryAsync(int serverId, string databaseName, string schemaName, string objectName, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        command.CommandText = @"
SELECT
    collection_time,
    delta_execution_count,
    delta_worker_time,
    delta_elapsed_time,
    delta_logical_reads,
    delta_logical_writes,
    delta_physical_reads,
    min_worker_time,
    max_worker_time,
    min_elapsed_time,
    max_elapsed_time,
    total_spills,
    min_logical_reads,
    max_logical_reads,
    min_physical_reads,
    max_physical_reads,
    min_logical_writes,
    max_logical_writes,
    min_spills,
    max_spills,
    sql_handle,
    plan_handle,
    cached_time,
    last_execution_time,
    object_type,
    execution_count,
    total_worker_time,
    total_elapsed_time,
    total_logical_reads,
    total_physical_reads,
    total_logical_writes,
    delta_spills,
    CAST(extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS BIGINT) AS sample_interval_seconds
FROM v_procedure_stats
WHERE server_id = $1
AND   database_name = $2
AND   schema_name = $3
AND   object_name = $4
AND   collection_time >= $5
AND   collection_time <= $6
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = schemaName });
        command.Parameters.Add(new DuckDBParameter { Value = objectName });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<ProcedureStatsHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ProcedureStatsHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeltaExecutions = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                DeltaCpuUs = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                DeltaElapsedUs = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                DeltaLogicalReads = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                DeltaLogicalWrites = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                DeltaPhysicalReads = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                MinWorkerTimeUs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                MaxWorkerTimeUs = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                MinElapsedTimeUs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                MaxElapsedTimeUs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                TotalSpills = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                MinLogicalReads = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                MaxLogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                MinPhysicalReads = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                MaxPhysicalReads = reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                MinLogicalWrites = reader.IsDBNull(16) ? 0 : reader.GetInt64(16),
                MaxLogicalWrites = reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                MinSpills = reader.IsDBNull(18) ? 0 : reader.GetInt64(18),
                MaxSpills = reader.IsDBNull(19) ? 0 : reader.GetInt64(19),
                SqlHandle = reader.IsDBNull(20) ? "" : reader.GetString(20),
                PlanHandle = reader.IsDBNull(21) ? "" : reader.GetString(21),
                CachedTime = reader.IsDBNull(22) ? (DateTime?)null : reader.GetDateTime(22),
                LastExecutionTime = reader.IsDBNull(23) ? (DateTime?)null : reader.GetDateTime(23),
                ObjectType = reader.IsDBNull(24) ? "" : reader.GetString(24),
                TotalExecutions = reader.IsDBNull(25) ? 0 : reader.GetInt64(25),
                TotalCpuUs = reader.IsDBNull(26) ? 0 : reader.GetInt64(26),
                TotalElapsedUs = reader.IsDBNull(27) ? 0 : reader.GetInt64(27),
                TotalLogicalReads = reader.IsDBNull(28) ? 0 : reader.GetInt64(28),
                TotalPhysicalReads = reader.IsDBNull(29) ? 0 : reader.GetInt64(29),
                TotalLogicalWrites = reader.IsDBNull(30) ? 0 : reader.GetInt64(30),
                DeltaSpills = reader.IsDBNull(31) ? 0 : reader.GetInt64(31),
                SampleIntervalSeconds = reader.IsDBNull(32) ? (int?)null : Convert.ToInt32(reader.GetValue(32))
            });
        }

        return items;
    }

    /// <summary>
    /// Looks up a cached query plan from DuckDB by server_id and query_hash.
    /// Returns the most recently collected plan XML, or null if not found.
    /// </summary>
    public async Task<string?> GetCachedQueryPlanAsync(int serverId, string queryHash)
    {
        const string query = @"
SELECT query_plan_xml
FROM v_query_stats
WHERE server_id = $1
AND   query_hash = $2
AND   query_plan_xml IS NOT NULL
AND   query_plan_xml <> ''
ORDER BY collection_time DESC
LIMIT 1";

        using var connection = await OpenConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = query;
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = queryHash });
        var result = await cmd.ExecuteScalarAsync();
        return result as string;
    }

    public async Task<string?> GetCachedProcedurePlanAsync(int serverId, string planHandle)
    {
        const string query = @"
SELECT query_plan_xml
FROM v_query_stats
WHERE server_id = $1
AND   plan_handle = $2
AND   query_plan_xml IS NOT NULL
AND   query_plan_xml <> ''
ORDER BY collection_time DESC
LIMIT 1";

        using var connection = await OpenConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = query;
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = planHandle });
        var result = await cmd.ExecuteScalarAsync();
        return result as string;
    }

    /// <summary>
    /// Fetches a query plan on-demand from the remote server by query hash.
    /// </summary>
    public static async Task<string?> FetchQueryPlanOnDemandAsync(string connectionString, string queryHash)
    {
        const string query = @"
SELECT TOP (1)
    query_plan_text = tqp.query_plan
FROM sys.dm_exec_query_stats AS qs
OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) AS tqp
WHERE CONVERT(varchar(64), qs.query_hash, 1) = @query_hash
AND   tqp.query_plan IS NOT NULL
ORDER BY
    qs.total_elapsed_time DESC
OPTION(RECOMPILE);";

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new SqlParameter("@query_hash", SqlDbType.VarChar, 64) { Value = queryHash });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }

    /// <summary>
    /// Fetches a procedure plan on-demand from the remote server by object name.
    /// Uses three-part naming with sp_executesql for Azure SQL DB compatibility.
    /// </summary>
    public static async Task<string?> FetchProcedurePlanOnDemandAsync(string connectionString, string databaseName, string schemaName, string objectName)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        /*
        Validate the database name exists and get the QUOTENAME'd version
        to prevent SQL injection via malicious database names
        */
        var quotedDbName = await GetValidatedDatabaseNameAsync(connection, databaseName);
        if (quotedDbName == null)
        {
            return null;
        }

        /*
        Use three-part naming (database.sys.sp_executesql) instead of USE statement
        for Azure SQL DB compatibility
        */
        var query = $@"
EXECUTE {quotedDbName}.sys.sp_executesql
    N'
SELECT TOP (1)
    query_plan_text = tqp.query_plan
FROM sys.dm_exec_procedure_stats AS ps
OUTER APPLY sys.dm_exec_text_query_plan(ps.plan_handle, 0, -1) AS tqp
WHERE ps.database_id = DB_ID()
AND   OBJECT_NAME(ps.object_id, ps.database_id) COLLATE DATABASE_DEFAULT = @object_name COLLATE DATABASE_DEFAULT
AND   OBJECT_SCHEMA_NAME(ps.object_id, ps.database_id) COLLATE DATABASE_DEFAULT = @schema_name COLLATE DATABASE_DEFAULT
AND   tqp.query_plan IS NOT NULL
ORDER BY
    ps.total_elapsed_time DESC
OPTION(RECOMPILE);',
    N'@object_name sysname, @schema_name sysname',
    @object_name,
    @schema_name;";

        using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new SqlParameter("@object_name", SqlDbType.NVarChar, 128) { Value = objectName });
        command.Parameters.Add(new SqlParameter("@schema_name", SqlDbType.NVarChar, 128) { Value = schemaName });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }

    /// <summary>
    /// Fetches a query plan on-demand by sql_handle + statement offsets.
    /// Used for Blocked Process Reports, where query_hash is not present in the
    /// XE event payload — only the sql_handle and offsets from executionStack frames.
    /// </summary>
    public static async Task<string?> FetchPlanBySqlHandleAsync(
        string connectionString,
        string databaseName,
        string sqlHandleHex,
        int statementStartOffset,
        int statementEndOffset)
    {
        if (string.IsNullOrWhiteSpace(sqlHandleHex)) return null;
        var handleBytes = HexStringToBytes(sqlHandleHex);
        if (handleBytes == null || handleBytes.Length == 0) return null;

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var quotedDbName = await GetValidatedDatabaseNameAsync(connection, databaseName)
                           ?? "[master]";

        var query = $@"
EXECUTE {quotedDbName}.sys.sp_executesql
    N'
SELECT TOP (1)
    query_plan_text = tqp.query_plan
FROM sys.dm_exec_query_stats AS qs
OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) AS tqp
WHERE qs.sql_handle = @h
AND   qs.statement_start_offset = @stmt_start
AND   qs.statement_end_offset = @stmt_end
AND   tqp.query_plan IS NOT NULL
ORDER BY
    qs.last_execution_time DESC
OPTION(RECOMPILE);',
    N'@h varbinary(64), @stmt_start int, @stmt_end int',
    @h, @stmt_start, @stmt_end;";

        using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
        command.Parameters.Add(new SqlParameter("@h", SqlDbType.VarBinary, 64) { Value = handleBytes });
        command.Parameters.Add(new SqlParameter("@stmt_start", SqlDbType.Int) { Value = statementStartOffset });
        command.Parameters.Add(new SqlParameter("@stmt_end", SqlDbType.Int) { Value = statementEndOffset });
        var result = await command.ExecuteScalarAsync();
        return result as string;
    }

    private static byte[]? HexStringToBytes(string hex)
    {
        var start = hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? 2 : 0;
        var len = hex.Length - start;
        if (len <= 0 || (len % 2) != 0) return null;
        var bytes = new byte[len / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            if (!byte.TryParse(hex.AsSpan(start + i * 2, 2),
                               System.Globalization.NumberStyles.HexNumber,
                               System.Globalization.CultureInfo.InvariantCulture,
                               out bytes[i]))
            {
                return null;
            }
        }
        return bytes;
    }

    /// <summary>
    /// Gets top procedures by CPU for a server.
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetProcStatsSlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
    date_trunc('hour', collection_time) AS bucket,
    COUNT(DISTINCT object_name) AS proc_count,
    COALESCE(SUM(delta_worker_time), 0) / 1000.0 AS total_cpu_ms,
    COALESCE(SUM(delta_elapsed_time), 0) / 1000.0 AS total_elapsed_ms,
    COALESCE(SUM(delta_logical_reads), 0) AS total_reads,
    COALESCE(SUM(delta_logical_writes), 0) AS total_writes,
    COALESCE(SUM(delta_physical_reads), 0) AS total_physical_reads
FROM v_procedure_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
GROUP BY date_trunc('hour', collection_time)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                TotalCpu = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalWrites = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TotalLogicalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                Value = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
            });
        }
        return items;
    }

    public async Task<List<ProcedureStatsRow>> GetTopProceduresByCpuAsync(int serverId, int hoursBack = 24, int top = 50, DateTime? fromDate = null, DateTime? toDate = null, int utcOffsetMinutes = 0, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetTopProceduresByCpuAsync", "v_procedure_stats top N by CPU");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 6, out var dbValues);

        command.CommandText = @"
SELECT
    database_name,
    schema_name,
    object_name,
    object_type,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_us,
    SUM(delta_elapsed_time) AS total_elapsed_us,
    SUM(delta_logical_reads) AS total_reads,
    SUM(delta_logical_writes) AS total_writes,
    SUM(delta_physical_reads) AS total_physical_reads,
    MIN(min_worker_time) AS min_worker_time,
    MAX(max_worker_time) AS max_worker_time,
    MIN(min_elapsed_time) AS min_elapsed_time,
    MAX(max_elapsed_time) AS max_elapsed_time,
    MIN(min_logical_reads) AS min_logical_reads,
    MAX(max_logical_reads) AS max_logical_reads,
    MIN(min_physical_reads) AS min_physical_reads,
    MAX(max_physical_reads) AS max_physical_reads,
    MIN(min_logical_writes) AS min_logical_writes,
    MAX(max_logical_writes) AS max_logical_writes,
    SUM(delta_spills) AS total_spills,
    MIN(min_spills) AS min_spills,
    MAX(max_spills) AS max_spills,
    MAX(cached_time) AS cached_time,
    MAX(last_execution_time) AS last_execution_time,
    MAX(sql_handle) AS sql_handle,
    MAX(plan_handle) AS plan_handle,
    CAST(SUM(delta_spills) AS DOUBLE PRECISION) / NULLIF(SUM(delta_execution_count), 0) AS avg_spills
FROM v_procedure_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   last_execution_time >= $2 + $5 * INTERVAL '1' MINUTE" + dbClause + @"
GROUP BY database_name, schema_name, object_name, object_type
HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
ORDER BY SUM(delta_elapsed_time) DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = top });
        command.Parameters.Add(new DuckDBParameter { Value = utcOffsetMinutes });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<ProcedureStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ProcedureStatsRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ObjectName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ObjectType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                TotalExecutions = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                TotalCpuUs = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TotalElapsedUs = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                TotalLogicalReads = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                TotalLogicalWrites = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                TotalPhysicalReads = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                MinWorkerTimeUs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                MaxWorkerTimeUs = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                MinElapsedTimeUs = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                MaxElapsedTimeUs = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                MinLogicalReads = reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                MaxLogicalReads = reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                MinPhysicalReads = reader.IsDBNull(16) ? 0 : reader.GetInt64(16),
                MaxPhysicalReads = reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                MinLogicalWrites = reader.IsDBNull(18) ? 0 : reader.GetInt64(18),
                MaxLogicalWrites = reader.IsDBNull(19) ? 0 : reader.GetInt64(19),
                TotalSpills = reader.IsDBNull(20) ? 0 : reader.GetInt64(20),
                MinSpills = reader.IsDBNull(21) ? 0 : reader.GetInt64(21),
                MaxSpills = reader.IsDBNull(22) ? 0 : reader.GetInt64(22),
                CachedTime = reader.IsDBNull(23) ? (DateTime?)null : reader.GetDateTime(23),
                LastExecutionTime = reader.IsDBNull(24) ? (DateTime?)null : reader.GetDateTime(24),
                SqlHandle = reader.IsDBNull(25) ? "" : reader.GetString(25),
                PlanHandle = reader.IsDBNull(26) ? "" : reader.GetString(26),
                AvgSpills = reader.IsDBNull(27) ? 0 : ToDouble(reader.GetValue(27))
            });
        }

        return items;
    }

    /// <summary>
    /// Gets procedure stats comparison between a current time range and a baseline range.
    /// </summary>
    public async Task<List<ProcedureStatsComparisonItem>> GetProcedureStatsComparisonAsync(
        int serverId,
        DateTime currentStart, DateTime currentEnd,
        DateTime baselineStart, DateTime baselineEnd,
        IReadOnlyList<string>? databaseNames = null)
    {
        using var _q = TimeQuery("GetProcedureStatsComparisonAsync", "v_procedure_stats comparison");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var dbClause = BuildDbInClause(databaseNames, "database_name", 6, out var dbValues);

        command.CommandText = @"
WITH top_current AS (
    SELECT database_name, schema_name, object_name
    FROM v_procedure_stats
    WHERE server_id = $1
    AND   collection_time >= $2 AND collection_time <= $3" + dbClause + @"
    AND   delta_execution_count > 0
    GROUP BY database_name, schema_name, object_name
    ORDER BY SUM(delta_execution_count) DESC
    LIMIT 100
),
top_baseline AS (
    SELECT database_name, schema_name, object_name
    FROM v_procedure_stats
    WHERE server_id = $1
    AND   collection_time >= $4 AND collection_time <= $5" + dbClause + @"
    AND   delta_execution_count > 0
    GROUP BY database_name, schema_name, object_name
    ORDER BY SUM(delta_execution_count) DESC
    LIMIT 100
),
top_procs AS (
    SELECT DISTINCT database_name, schema_name, object_name
    FROM (
        SELECT * FROM top_current
        UNION ALL
        SELECT * FROM top_baseline
    ) combined
),
current_period AS (
    SELECT tp.database_name, tp.schema_name, tp.object_name,
           SUM(ps.delta_execution_count) AS exec_count,
           SUM(ps.delta_elapsed_time)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(ps.delta_worker_time)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(ps.delta_physical_reads)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) AS avg_reads,
           MAX(ps.sql_handle) AS sql_handle
    FROM top_procs tp
    INNER JOIN v_procedure_stats ps
      ON  ps.database_name IS NOT DISTINCT FROM tp.database_name
      AND ps.schema_name IS NOT DISTINCT FROM tp.schema_name
      AND ps.object_name IS NOT DISTINCT FROM tp.object_name
    WHERE ps.server_id = $1
    AND   ps.collection_time >= $2 AND ps.collection_time <= $3
    AND   ps.delta_execution_count > 0
    GROUP BY tp.database_name, tp.schema_name, tp.object_name
),
baseline_period AS (
    SELECT tp.database_name, tp.schema_name, tp.object_name,
           SUM(ps.delta_execution_count) AS exec_count,
           SUM(ps.delta_elapsed_time)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(ps.delta_worker_time)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(ps.delta_physical_reads)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) AS avg_reads,
           MAX(ps.sql_handle) AS sql_handle
    FROM top_procs tp
    INNER JOIN v_procedure_stats ps
      ON  ps.database_name IS NOT DISTINCT FROM tp.database_name
      AND ps.schema_name IS NOT DISTINCT FROM tp.schema_name
      AND ps.object_name IS NOT DISTINCT FROM tp.object_name
    WHERE ps.server_id = $1
    AND   ps.collection_time >= $4 AND ps.collection_time <= $5
    AND   ps.delta_execution_count > 0
    GROUP BY tp.database_name, tp.schema_name, tp.object_name
)
SELECT COALESCE(c.database_name, b.database_name) AS database_name,
       COALESCE(c.schema_name, b.schema_name) AS schema_name,
       COALESCE(c.object_name, b.object_name) AS object_name,
       c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
       b.exec_count AS baseline_exec_count,
       b.avg_duration_ms AS baseline_avg_duration_ms,
       b.avg_cpu_ms AS baseline_avg_cpu_ms,
       b.avg_reads AS baseline_avg_reads,
       t.query_text
FROM current_period c
FULL OUTER JOIN baseline_period b
  ON  c.database_name IS NOT DISTINCT FROM b.database_name
  AND c.schema_name IS NOT DISTINCT FROM b.schema_name
  AND c.object_name IS NOT DISTINCT FROM b.object_name
/* #1981: a REPRESENTATIVE statement of the procedure, resolved through the same normalized
   sql_handle join the #1568 module attribution relies on (both stores persist the identical
   CONVERT(varchar(130), ..., 1) text). procedure_stats captures no text of its own, so this is
   the latest captured statement from inside the module - parity with the other two comparison
   grids' text columns, labeled a statement rather than the definition. */
LEFT JOIN LATERAL (
    SELECT qs.query_text
    FROM v_query_stats qs
    WHERE qs.server_id = $1
    AND   qs.sql_handle = COALESCE(c.sql_handle, b.sql_handle)
    AND   qs.query_text IS NOT NULL
    ORDER BY qs.collection_time DESC
    LIMIT 1
) t ON TRUE;";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = currentStart });
        command.Parameters.Add(new DuckDBParameter { Value = currentEnd });
        command.Parameters.Add(new DuckDBParameter { Value = baselineStart });
        command.Parameters.Add(new DuckDBParameter { Value = baselineEnd });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<ProcedureStatsComparisonItem>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new ProcedureStatsComparisonItem
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ObjectName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ExecutionCount = reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                AvgDurationMs = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                AvgCpuMs = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AvgReads = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                BaselineExecutionCount = reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
                BaselineAvgDurationMs = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8)),
                BaselineAvgCpuMs = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
                BaselineAvgReads = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10)),
                QueryText = reader.IsDBNull(11) ? "" : reader.GetString(11),
            });
        }

        return items;
    }

    /// <summary>
    /// Gets query duration trend — total elapsed time per collection snapshot.
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetQueryDurationTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
        SUM(delta_execution_count) AS total_executions,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3" + dbClause + @"
    GROUP BY collection_time
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,
    CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
FROM raw
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QueryTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : (long)ToDouble(reader.GetValue(2)),
                ExecutionsPerSecond = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))
            });
        }
        return items;
    }

    /// <summary>
    /// Whether this server has EVER recorded a query-stats sample, ignoring any window.
    /// <para>Lets an empty query-duration trend say WHICH kind of nothing it found — see
    /// <c>LocalDataService.HasAnyMemoryStatAsync</c> for the reasoning. Reads <c>v_query_stats</c>, the
    /// same source <see cref="GetQueryDurationTrendAsync"/> reads here. Darling's twin probes the BASE
    /// <c>query_stats</c> table instead, because ITS duration trend does — on a Darling store
    /// <c>v_query_stats</c> is the payload-resolving view rather than a passthrough. Each probe follows
    /// its own read; the SENTENCES the two SKUs return are identical, which is what the caller sees.</para>
    /// </summary>
    public async Task<bool> HasAnyQueryStatAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_query_stats
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets procedure duration trend — elapsed time per second per collection snapshot.
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetProcedureDurationTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
        SUM(delta_execution_count) AS total_executions,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
    FROM v_procedure_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3" + dbClause + @"
    GROUP BY collection_time
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,
    CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
FROM raw
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QueryTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : (long)ToDouble(reader.GetValue(2)),
                ExecutionsPerSecond = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets execution count trend — executions per second per collection snapshot from query_stats.
    /// </summary>
    public async Task<List<QueryTrendPoint>> GetExecutionCountTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        SUM(delta_execution_count) AS total_executions,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3" + dbClause + @"
    GROUP BY collection_time
)
SELECT
    collection_time,
    CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
FROM raw
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QueryTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1))
            });
        }
        return items;
    }

    /// <summary>The same probe over <c>v_procedure_stats</c>, the source
    /// <see cref="GetProcedureDurationTrendAsync"/> reads. See <see cref="HasAnyQueryStatAsync"/>.</summary>
    public Task<bool> HasAnyProcedureStatAsync(int serverId) => HasAnyRowAsync("v_procedure_stats", serverId);

    /// <summary>
    /// The same probe over <c>v_query_store_stats</c>. See <see cref="HasAnyQueryStatAsync"/>.
    /// <para>Worth the most of the trends' probes: zero rows here has a cause the others do not, which is that
    /// Query Store can be OFF on every database. A server with no Query Store data is not a server with no
    /// slow queries.</para>
    /// </summary>
    public Task<bool> HasAnyQueryStoreStatAsync(int serverId) => HasAnyRowAsync("v_query_store_stats", serverId);

    /*
        The view name is interpolated because DuckDB cannot parameterize a FROM target. Every caller is one
        of the two literals above -- no caller-supplied string reaches this -- and the server id stays a
        bound parameter.
    */
    private async Task<bool> HasAnyRowAsync(string viewName, int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = $"SELECT 1 FROM {viewName} WHERE server_id = $1 LIMIT 1";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    private static readonly double[] HeatmapBucketThresholds = { 0, 1, 10, 100, 1000, 10000, 100000, 1000000 };

    private static readonly Dictionary<HeatmapMetric, string[]> BucketLabelsMap = new()
    {
        [HeatmapMetric.Duration] = new[] { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" },
        [HeatmapMetric.Cpu] = new[] { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" },
        [HeatmapMetric.LogicalReads] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
        [HeatmapMetric.LogicalWrites] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
        [HeatmapMetric.ExecutionCount] = new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" }
    };

    private static string GetMetricColumn(HeatmapMetric metric) => metric switch
    {
        HeatmapMetric.Duration => "(delta_elapsed_time / 1000.0) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.Cpu => "(delta_worker_time / 1000.0) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.LogicalReads => "CAST(delta_logical_reads AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.LogicalWrites => "CAST(delta_logical_writes AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)",
        HeatmapMetric.ExecutionCount => "CAST(delta_execution_count AS DOUBLE PRECISION)",
        _ => "(delta_elapsed_time / 1000.0) / NULLIF(delta_execution_count, 0)"
    };

    public async Task<HeatmapResult> GetQueryHeatmapAsync(int serverId, HeatmapMetric metric, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        var metricExpr = GetMetricColumn(metric);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        AppLogger.Info("Heatmap", $"GetQueryHeatmapAsync: serverId={serverId}, metric={metric}, hoursBack={hoursBack}, start={startTime:O}, end={endTime:O}");

        command.CommandText = $@"
WITH per_query AS (
    SELECT
        time_bucket(INTERVAL '5 minutes', collection_time) AS time_bin,
        {metricExpr} AS metric_value,
        query_hash,
        LEFT(query_text, 120) AS query_preview,
        delta_execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   delta_execution_count > 0
    AND   {metricExpr} IS NOT NULL{dbClause}
)
SELECT
    time_bin,
    CASE
        WHEN metric_value < 1 THEN 0
        WHEN metric_value < 10 THEN 1
        WHEN metric_value < 100 THEN 2
        WHEN metric_value < 1000 THEN 3
        WHEN metric_value < 10000 THEN 4
        WHEN metric_value < 100000 THEN 5
        ELSE 6
    END AS bucket_index,
    COUNT(*) AS query_count,
    ARG_MAX(query_hash, delta_execution_count) AS top_query_hash,
    ARG_MAX(query_preview, delta_execution_count) AS top_query_text
FROM per_query
GROUP BY time_bin, bucket_index
ORDER BY time_bin, bucket_index";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var rawCells = new List<HeatmapCell>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rawCells.Add(new HeatmapCell
            {
                TimeBucket = reader.GetDateTime(0),
                BucketIndex = (int)ToDouble(reader.GetValue(1)),
                Count = (long)ToDouble(reader.GetValue(2)),
                TopQueryHash = reader.IsDBNull(3) ? "" : reader.GetString(3),
                TopQueryText = reader.IsDBNull(4) ? "" : reader.GetString(4)
            });
        }

        if (rawCells.Count == 0)
            return new HeatmapResult();

        var times = rawCells.Select(c => c.TimeBucket).Distinct().OrderBy(t => t).ToArray();
        var timeIndex = new Dictionary<DateTime, int>();
        for (int i = 0; i < times.Length; i++) timeIndex[times[i]] = i;

        int numBuckets = 7;
        var intensities = new double[numBuckets, times.Length];
        var cellDetails = new HeatmapCell[numBuckets, times.Length];

        foreach (var cell in rawCells)
        {
            if (!timeIndex.TryGetValue(cell.TimeBucket, out int col)) continue;
            int row = Math.Clamp(cell.BucketIndex, 0, numBuckets - 1);
            intensities[row, col] = cell.Count;
            cellDetails[row, col] = cell;
        }

        return new HeatmapResult
        {
            Intensities = intensities,
            TimeBuckets = times,
            BucketLabels = BucketLabelsMap[metric],
            CellDetails = cellDetails
        };
    }
}

public enum HeatmapMetric
{
    Duration,
    Cpu,
    LogicalReads,
    LogicalWrites,
    ExecutionCount
}

public class HeatmapCell
{
    public DateTime TimeBucket { get; set; }
    public int BucketIndex { get; set; }
    public long Count { get; set; }
    public string TopQueryHash { get; set; } = "";
    public string TopQueryText { get; set; } = "";
}

public class HeatmapResult
{
    public double[,] Intensities { get; set; } = new double[0, 0];
    public DateTime[] TimeBuckets { get; set; } = Array.Empty<DateTime>();
    public string[] BucketLabels { get; set; } = Array.Empty<string>();
    public HeatmapCell[,] CellDetails { get; set; } = new HeatmapCell[0, 0];
}

public class QueryTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double Value { get; set; }
    public long ExecutionCount { get; set; }

    /// <summary>
    /// The SAME quantity as <see cref="ExecutionCount"/> - executions per second - without the truncation.
    /// <para>The long shipped first and the charts have always plotted it; on a server doing three
    /// executions a second that rounds harmlessly, and on a quiet one doing 0.4 it reports ZERO, which reads
    /// as an idle server rather than a slow one. Kept alongside rather than replacing it so nothing reading
    /// the long breaks. Darling's twin is <c>QueryDurationTrendPoint.ExecutionsPerSecond</c>.</para>
    /// </summary>
    public double ExecutionsPerSecond { get; set; }
}

public class QueryStatsRow
{
    /// <summary>Gates "Get Actual Plan (re-run)" — see <see cref="QuerySnapshotRow.CanGetActualPlan"/>.</summary>
    public bool CanGetActualPlan => !string.IsNullOrEmpty(QueryText);

    public string DatabaseName { get; set; } = "";
    public string QueryHash { get; set; } = "";
    public DateTime? LastExecutionTime { get; set; }
    public DateTime? CreationTime { get; set; }
    public string LastExecutionTimeLocal => Services.ServerTimeHelper.FormatServerTime(LastExecutionTime);
    public string CreationTimeLocal => Services.ServerTimeHelper.FormatServerTime(CreationTime);
    public long TotalExecutions { get; set; }
    public long TotalCpuUs { get; set; }
    public long TotalElapsedUs { get; set; }
    public long TotalLogicalReads { get; set; }
    public long TotalRows { get; set; }
    public long TotalLogicalWrites { get; set; }
    public long TotalPhysicalReads { get; set; }
    public long TotalSpills { get; set; }
    public int MinDop { get; set; }
    public int MaxDop { get; set; }
    public long MinCpuUs { get; set; }
    public long MaxCpuUs { get; set; }
    public long MinElapsedUs { get; set; }
    public long MaxElapsedUs { get; set; }
    public long MinPhysicalReads { get; set; }
    public long MaxPhysicalReads { get; set; }
    public long MinRows { get; set; }
    public long MaxRows { get; set; }
    public long MinGrantKb { get; set; }
    public long MaxGrantKb { get; set; }
    public long MinUsedGrantKb { get; set; }
    public long MaxUsedGrantKb { get; set; }
    public long MinIdealGrantKb { get; set; }
    public long MaxIdealGrantKb { get; set; }
    public long MinReservedThreads { get; set; }
    public long MaxReservedThreads { get; set; }
    public long MinUsedThreads { get; set; }
    public long MaxUsedThreads { get; set; }
    public long TotalClrUs { get; set; }
    public long MinSpills { get; set; }
    public long MaxSpills { get; set; }
    public long PlanGenerationNum { get; set; }
    public double WorkerTimePerSecond { get; set; }
    public string QueryPlanHash { get; set; } = "";
    public string SqlHandle { get; set; } = "";
    public string PlanHandle { get; set; } = "";
    public string QueryText { get; set; } = "";

    /// <summary>#2012: distinct statement texts merged into this group; > 1 means
    /// <see cref="QueryText"/> is one representative of a blend (ad-hoc literal variants, or
    /// pre-stage-2 history where <see cref="HostObjectName"/> hadn't split INSERT...EXEC
    /// callers yet).</summary>
    public long DistinctTexts { get; set; }

    /// <summary>#2012 stage 2: the statement's hosting module (<c>schema.object</c>) captured at
    /// collection from dm_exec_sql_text.objectid; null for ad-hoc/prepared text and for rows
    /// collected before the column existed. Part of the group key, so same-hash statements hosted
    /// by different procs (INSERT...EXEC) land in separate rows.</summary>
    public string? HostObjectName { get; set; }
    public string? QueryPlan { get; set; }
    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlan);

    /* #1568 module attribution (read-time stitch of query_stats.sql_handle -> procedure_stats): the
       matched procedure/function/trigger's identity, or empty when this statement is ad hoc. */
    public string ModuleObjectName { get; set; } = "";
    public string ModuleSchemaName { get; set; } = "";
    public string ModuleDatabaseName { get; set; } = "";

    /// <summary>Grid "Module" column: the collection-time <see cref="HostObjectName"/> when present
    /// (#2012 stage 2 — authoritative, resolved on the monitored server), else the #1568 sql_handle
    /// stitch's <c>database.schema.object</c> for older rows, else the literal <c>ad hoc</c>.</summary>
    public string ModuleName =>
        !string.IsNullOrEmpty(HostObjectName)
            ? $"{DatabaseName}.{HostObjectName}"
            : string.IsNullOrEmpty(ModuleObjectName)
                ? "ad hoc"
                : $"{ModuleDatabaseName}.{ModuleSchemaName}.{ModuleObjectName}";

    public double TotalCpuMs => TotalCpuUs / 1000.0;
    public double TotalElapsedMs => TotalElapsedUs / 1000.0;
    public double AvgCpuMs => TotalExecutions > 0 ? TotalCpuMs / TotalExecutions : 0;
    public double AvgElapsedMs => TotalExecutions > 0 ? TotalElapsedMs / TotalExecutions : 0;
    public double AvgReads => TotalExecutions > 0 ? (double)TotalLogicalReads / TotalExecutions : 0;
    public double MinCpuMs => MinCpuUs / 1000.0;
    public double MaxCpuMs => MaxCpuUs / 1000.0;
    public double MinElapsedMs => MinElapsedUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedUs / 1000.0;
    // total_clr_time is stored in microseconds (like worker/elapsed time)
    public double TotalClrMs => TotalClrUs / 1000.0;
}

public class ProcedureStatsRow
{
    public string DatabaseName { get; set; } = "";
    public string SchemaName { get; set; } = "";
    public string ObjectName { get; set; } = "";
    public string ObjectType { get; set; } = "";
    public long TotalExecutions { get; set; }
    public long TotalCpuUs { get; set; }
    public long TotalElapsedUs { get; set; }
    public long TotalLogicalReads { get; set; }
    public long TotalLogicalWrites { get; set; }
    public long TotalPhysicalReads { get; set; }
    public long MinWorkerTimeUs { get; set; }
    public long MaxWorkerTimeUs { get; set; }
    public long MinElapsedTimeUs { get; set; }
    public long MaxElapsedTimeUs { get; set; }
    public long MinLogicalReads { get; set; }
    public long MaxLogicalReads { get; set; }
    public long MinPhysicalReads { get; set; }
    public long MaxPhysicalReads { get; set; }
    public long MinLogicalWrites { get; set; }
    public long MaxLogicalWrites { get; set; }
    public long TotalSpills { get; set; }
    public double AvgSpills { get; set; }
    public long MinSpills { get; set; }
    public long MaxSpills { get; set; }
    public DateTime? CachedTime { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public string SqlHandle { get; set; } = "";
    public string PlanHandle { get; set; } = "";
    public string FullName => string.IsNullOrEmpty(SchemaName) ? ObjectName : $"{SchemaName}.{ObjectName}";
    public double TotalCpuMs => TotalCpuUs / 1000.0;
    public double TotalElapsedMs => TotalElapsedUs / 1000.0;
    public double AvgCpuMs => TotalExecutions > 0 ? TotalCpuMs / TotalExecutions : 0;
    public double AvgElapsedMs => TotalExecutions > 0 ? TotalElapsedMs / TotalExecutions : 0;
    public double AvgReads => TotalExecutions > 0 ? (double)TotalLogicalReads / TotalExecutions : 0;
    public double MinCpuMs => MinWorkerTimeUs / 1000.0;
    public double MaxCpuMs => MaxWorkerTimeUs / 1000.0;
    public double MinElapsedMs => MinElapsedTimeUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedTimeUs / 1000.0;
    public string CachedTimeFormatted => Services.ServerTimeHelper.FormatServerTime(CachedTime);
    public string LastExecutionTimeLocal => Services.ServerTimeHelper.FormatServerTime(LastExecutionTime);
}

public class QueryStatsHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long DeltaExecutions { get; set; }
    public long DeltaCpuUs { get; set; }
    public long DeltaElapsedUs { get; set; }
    public long DeltaLogicalReads { get; set; }
    public long DeltaLogicalWrites { get; set; }
    public long DeltaPhysicalReads { get; set; }
    public long DeltaRows { get; set; }
    public long DeltaSpills { get; set; }
    public int MinDop { get; set; }
    public int MaxDop { get; set; }
    public long MinCpuUs { get; set; }
    public long MaxCpuUs { get; set; }
    public long MinElapsedUs { get; set; }
    public long MaxElapsedUs { get; set; }
    public long MinGrantKb { get; set; }
    public long MaxGrantKb { get; set; }
    public long MinUsedGrantKb { get; set; }
    public long MaxUsedGrantKb { get; set; }
    public long MinIdealGrantKb { get; set; }
    public long MaxIdealGrantKb { get; set; }
    public long MinReservedThreads { get; set; }
    public long MaxReservedThreads { get; set; }
    public long MinUsedThreads { get; set; }
    public long MaxUsedThreads { get; set; }
    public long MinPhysicalReads { get; set; }
    public long MaxPhysicalReads { get; set; }
    public long MinRows { get; set; }
    public long MaxRows { get; set; }
    public long MinSpills { get; set; }
    public long MaxSpills { get; set; }
    public long TotalClrTimeUs { get; set; }
    public string QueryPlanHash { get; set; } = "";
    public DateTime? CreationTime { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public long TotalExecutions { get; set; }
    public long TotalCpuUs { get; set; }
    public long TotalElapsedUs { get; set; }
    public long TotalLogicalReads { get; set; }
    public long TotalLogicalWrites { get; set; }
    public long TotalPhysicalReads { get; set; }
    public long TotalRows { get; set; }
    public long TotalSpills { get; set; }
    public string SqlHandle { get; set; } = "";
    public string PlanHandle { get; set; } = "";
    public string QueryHash { get; set; } = "";
    public int? SampleIntervalSeconds { get; set; }
    public double DeltaCpuMs => DeltaCpuUs / 1000.0;
    public double DeltaElapsedMs => DeltaElapsedUs / 1000.0;
    public double AvgCpuMs => DeltaExecutions > 0 ? DeltaCpuMs / DeltaExecutions : 0;
    public double AvgElapsedMs => DeltaExecutions > 0 ? DeltaElapsedMs / DeltaExecutions : 0;
    public double AvgReads => DeltaExecutions > 0 ? (double)DeltaLogicalReads / DeltaExecutions : 0;
    public double AvgPhysicalReads => DeltaExecutions > 0 ? (double)DeltaPhysicalReads / DeltaExecutions : 0;
    public double AvgWrites => DeltaExecutions > 0 ? (double)DeltaLogicalWrites / DeltaExecutions : 0;
    public double AvgRows => DeltaExecutions > 0 ? (double)DeltaRows / DeltaExecutions : 0;
    public double MinCpuMs => MinCpuUs / 1000.0;
    public double MaxCpuMs => MaxCpuUs / 1000.0;
    public double MinElapsedMs => MinElapsedUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedUs / 1000.0;
    public double TotalClrMs => TotalClrTimeUs / 1000.0;
    public double TotalCpuMs => TotalCpuUs / 1000.0;
    public double TotalElapsedMs => TotalElapsedUs / 1000.0;
    public string CollectionTimeLocal => ServerTimeHelper.FormatServerTime(CollectionTime);
    public string CreationTimeLocal => ServerTimeHelper.FormatServerTime(CreationTime);
    public string LastExecutionTimeLocal => ServerTimeHelper.FormatServerTime(LastExecutionTime);
}

public class ProcedureStatsHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public long DeltaExecutions { get; set; }
    public long DeltaCpuUs { get; set; }
    public long DeltaElapsedUs { get; set; }
    public long DeltaLogicalReads { get; set; }
    public long DeltaLogicalWrites { get; set; }
    public long DeltaPhysicalReads { get; set; }
    public long MinWorkerTimeUs { get; set; }
    public long MaxWorkerTimeUs { get; set; }
    public long MinElapsedTimeUs { get; set; }
    public long MaxElapsedTimeUs { get; set; }
    public long TotalSpills { get; set; }
    public long MinLogicalReads { get; set; }
    public long MaxLogicalReads { get; set; }
    public long MinPhysicalReads { get; set; }
    public long MaxPhysicalReads { get; set; }
    public long MinLogicalWrites { get; set; }
    public long MaxLogicalWrites { get; set; }
    public long MinSpills { get; set; }
    public long MaxSpills { get; set; }
    public string SqlHandle { get; set; } = "";
    public string PlanHandle { get; set; } = "";
    public DateTime? CachedTime { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public string ObjectType { get; set; } = "";
    public long TotalExecutions { get; set; }
    public long TotalCpuUs { get; set; }
    public long TotalElapsedUs { get; set; }
    public long TotalLogicalReads { get; set; }
    public long TotalPhysicalReads { get; set; }
    public long TotalLogicalWrites { get; set; }
    public long DeltaSpills { get; set; }
    public int? SampleIntervalSeconds { get; set; }
    public double DeltaCpuMs => DeltaCpuUs / 1000.0;
    public double DeltaElapsedMs => DeltaElapsedUs / 1000.0;
    public double AvgCpuMs => DeltaExecutions > 0 ? DeltaCpuMs / DeltaExecutions : 0;
    public double AvgElapsedMs => DeltaExecutions > 0 ? DeltaElapsedMs / DeltaExecutions : 0;
    public double AvgReads => DeltaExecutions > 0 ? (double)DeltaLogicalReads / DeltaExecutions : 0;
    public double AvgPhysicalReads => DeltaExecutions > 0 ? (double)DeltaPhysicalReads / DeltaExecutions : 0;
    public double AvgWrites => DeltaExecutions > 0 ? (double)DeltaLogicalWrites / DeltaExecutions : 0;
    public double AvgSpills => DeltaExecutions > 0 ? (double)DeltaSpills / DeltaExecutions : 0;
    public double MinCpuMs => MinWorkerTimeUs / 1000.0;
    public double MaxCpuMs => MaxWorkerTimeUs / 1000.0;
    public double MinElapsedMs => MinElapsedTimeUs / 1000.0;
    public double MaxElapsedMs => MaxElapsedTimeUs / 1000.0;
    public double TotalCpuMs => TotalCpuUs / 1000.0;
    public double TotalElapsedMs => TotalElapsedUs / 1000.0;
    public string CollectionTimeLocal => ServerTimeHelper.FormatServerTime(CollectionTime);
    public string CachedTimeLocal => ServerTimeHelper.FormatServerTime(CachedTime);
    public string LastExecutionTimeLocal => ServerTimeHelper.FormatServerTime(LastExecutionTime);
}
