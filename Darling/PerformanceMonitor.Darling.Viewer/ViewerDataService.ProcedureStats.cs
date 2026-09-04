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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One Top-Procedures-by-Duration grid row — the viewer copy of Lite's <c>ProcedureStatsRow</c>
/// (LocalDataService.QueryStats.cs). A (database, schema, object) group's summed deltas + min/max
/// spreads over the window. <see cref="FullName"/> composes schema.object like Lite;
/// <see cref="CachedTime"/>/<see cref="LastExecutionTime"/> are the server's local wall clock
/// (dm_exec_procedure_stats), shown raw. Procedure stats carry no DOP / rows columns (the DMV does
/// not expose them), matching Lite's row.
/// </summary>
public sealed class ViewerProcedureStatsRow
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
    /// <summary>Whether the collector stored an execution plan for this object in the window — gates the
    /// grid's per-row "Query Plan" Download button (mirrors Lite's Top Procedures HasQueryPlan gating).</summary>
    public bool HasQueryPlan { get; set; }

    /// <summary>True when this row carries a plan_handle — gates the grid's "Fetch Live Plan" context item.</summary>
    public bool HasLivePlanHandle => !string.IsNullOrEmpty(PlanHandle);
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
    public string CachedTimeFormatted => ViewerDataService.FormatServerClock(CachedTime);
    public string LastExecutionTimeLocal => ViewerDataService.FormatServerClock(LastExecutionTime);
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Top-Procedures-by-Duration read — Lite's <c>GetTopProceduresByCpuAsync</c> ported to Postgres
    /// against the base <c>procedure_stats</c> table (there is no <c>v_procedure_stats</c> view; the
    /// existing viewer reads use base tables anyway). Group by (database, schema, object, type), sum the
    /// deltas + carry min/max spreads, rank by summed <c>delta_elapsed_time</c> descending (Lite's grid
    /// default), and cap at top — no LATERAL / WAITFOR trim (procedures aren't ad-hoc text). Viewer
    /// deviations: summed bigints CAST back to bigint for GetInt64; Lite's staleness filter +
    /// utcOffsetMinutes param dropped.
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 top.
    /// </summary>
    public const string TopProceduresSql = """
        SELECT
            database_name,
            schema_name,
            object_name,
            object_type,
            CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
            CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
            CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
            CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
            CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
            CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
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
            CAST(SUM(delta_spills) AS bigint) AS total_spills,
            MIN(min_spills) AS min_spills,
            MAX(max_spills) AS max_spills,
            MAX(cached_time) AS cached_time,
            MAX(last_execution_time) AS last_execution_time,
            MAX(sql_handle) AS sql_handle,
            MAX(plan_handle) AS plan_handle,
            CAST(SUM(delta_spills) AS double precision) / NULLIF(SUM(delta_execution_count), 0) AS avg_spills,
            /* Whether the collector captured a plan for this object anywhere in the window — gates the grid's
               per-row "Query Plan" Download button, matching the same filter GetProcedureStatsPlanXmlAsync
               fetches on. A digest is enough to answer it: since #1767 the plan itself lives in
               query_plan_dim and the row carries only the key, so joining the dimension just to prove
               presence would buy nothing this test cannot see from the fact row. */
            bool_or(query_plan_xml IS NOT NULL OR query_plan_digest IS NOT NULL) AS has_query_plan
        FROM procedure_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($5::text[] IS NULL OR database_name = ANY($5))
        GROUP BY database_name, schema_name, object_name, object_type
        HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
        ORDER BY SUM(delta_elapsed_time) DESC
        LIMIT $4
        """;

    /// <summary>
    /// The top procedure-stats groups for one server over [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>], pre-sorted by total elapsed time descending (the grid's default sort).
    /// </summary>
    public async Task<List<ViewerProcedureStatsRow>> GetTopProceduresByCpuAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int top = TopQueriesPageSize, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerProcedureStatsRow>();

        await using var command = _dataSource.CreateCommand(TopProceduresSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = top });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerProcedureStatsRow
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
                CachedTime = reader.IsDBNull(23) ? null : reader.GetDateTime(23),
                LastExecutionTime = reader.IsDBNull(24) ? null : reader.GetDateTime(24),
                SqlHandle = reader.IsDBNull(25) ? "" : reader.GetString(25),
                PlanHandle = reader.IsDBNull(26) ? "" : reader.GetString(26),
                AvgSpills = reader.IsDBNull(27) ? 0 : Convert.ToDouble(reader.GetValue(27)),
                HasQueryPlan = !reader.IsDBNull(28) && reader.GetBoolean(28),
            });
        }

        return rows;
    }

    /// <summary>
    /// Top-Procedures comparison — Lite's <c>GetProcedureStatsComparisonAsync</c> ported. Same
    /// top-100-union / FULL OUTER JOIN shape as the query-stats comparison, keyed on
    /// (database, schema, object). Returns the shared <see cref="ProcedureStatsComparisonItem"/>.
    /// $1 server_id, $2/$3 current window, $4/$5 baseline window (naive UTC).
    /// </summary>
    public const string ProcedureStatsComparisonSql = """
        WITH top_current AS (
            SELECT database_name, schema_name, object_name
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $2 AND collection_time <= $3
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
            AND   delta_execution_count > 0
            GROUP BY database_name, schema_name, object_name
            ORDER BY SUM(delta_execution_count) DESC
            LIMIT 100
        ),
        top_baseline AS (
            SELECT database_name, schema_name, object_name
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $4 AND collection_time <= $5
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
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
            ) AS combined
        ),
        current_period AS (
            SELECT tp.database_name, tp.schema_name, tp.object_name,
                   SUM(ps.delta_execution_count) AS exec_count,
                   SUM(ps.delta_elapsed_time)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
                   SUM(ps.delta_worker_time)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
                   SUM(ps.delta_physical_reads)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) AS avg_reads,
                   MAX(ps.sql_handle) AS sql_handle
            FROM top_procs tp
            INNER JOIN procedure_stats ps
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
                   SUM(ps.delta_elapsed_time)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
                   SUM(ps.delta_worker_time)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
                   SUM(ps.delta_physical_reads)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) AS avg_reads,
                   MAX(ps.sql_handle) AS sql_handle
            FROM top_procs tp
            INNER JOIN procedure_stats ps
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
          ON  COALESCE(c.database_name, '') = COALESCE(b.database_name, '')
          AND COALESCE(c.schema_name, '') = COALESCE(b.schema_name, '')
          AND COALESCE(c.object_name, '') = COALESCE(b.object_name, '')
        /* #1981: a REPRESENTATIVE statement of the procedure via the same normalized sql_handle
           join #1568's module attribution relies on (both stores persist the identical
           CONVERT(varchar(130), ..., 1) text). procedure_stats captures no text of its own, so
           this is the latest captured statement from inside the module — parity with the other
           two comparison grids, labeled a statement rather than the definition. v_query_stats
           resolves the #1767 payload dimension. */
        LEFT JOIN LATERAL (
            SELECT qs.query_text
            FROM v_query_stats qs
            WHERE qs.server_id = $1
            AND   qs.sql_handle = COALESCE(c.sql_handle, b.sql_handle)
            AND   qs.query_text IS NOT NULL
            ORDER BY qs.collection_time DESC
            LIMIT 1
        ) t ON TRUE
        """;

    /// <summary>Top-Procedures current-vs-baseline comparison rows (shared .Ui item; delta % + NEW/GONE badges).</summary>
    public async Task<List<ProcedureStatsComparisonItem>> GetProcedureStatsComparisonAsync(
        int serverId,
        DateTime currentStart, DateTime currentEnd,
        DateTime baselineStart, DateTime baselineEnd,
        IReadOnlyList<string>? databaseNames = null,
        CancellationToken cancellationToken = default)
    {
        var items = new List<ProcedureStatsComparisonItem>();

        await using var command = _dataSource.CreateCommand(ProcedureStatsComparisonSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddComparisonParameters(command, serverId, currentStart, currentEnd, baselineStart, baselineEnd);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ProcedureStatsComparisonItem
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                SchemaName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ObjectName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ExecutionCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                AvgDurationMs = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                AvgCpuMs = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                AvgReads = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                BaselineExecutionCount = reader.IsDBNull(7) ? 0 : Convert.ToInt64(reader.GetValue(7)),
                BaselineAvgDurationMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
                BaselineAvgCpuMs = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9)),
                BaselineAvgReads = reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10)),
                QueryText = reader.IsDBNull(11) ? "" : reader.GetString(11),
            });
        }

        return items;
    }

    /// <summary>
    /// Hourly procedure-stats buckets for the Top-Procedures slicer — Lite's
    /// <c>GetProcStatsSlicerDataAsync</c> ported (same bucket shape as the query-stats slicer, so it
    /// shares <see cref="ReadQueryStatsSlicerAsync"/>). The distinct-count is over object_name.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string ProcStatsSlicerSql = """
        SELECT
            date_trunc('hour', collection_time) AS bucket,
            COUNT(DISTINCT object_name) AS proc_count,
            COALESCE(SUM(delta_worker_time), 0) / 1000.0 AS total_cpu_ms,
            COALESCE(SUM(delta_elapsed_time), 0) / 1000.0 AS total_elapsed_ms,
            COALESCE(SUM(delta_logical_reads), 0) AS total_reads,
            COALESCE(SUM(delta_logical_writes), 0) AS total_writes,
            COALESCE(SUM(delta_physical_reads), 0) AS total_physical_reads
        FROM procedure_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        GROUP BY date_trunc('hour', collection_time)
        ORDER BY bucket
        """;

    /// <summary>Hourly procedure-stats slicer buckets over [<paramref name="startUtc"/>, <paramref name="endUtc"/>].</summary>
    public async Task<List<TimeSliceBucket>> GetProcStatsSlicerDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
        => await ReadQueryStatsSlicerAsync(ProcStatsSlicerSql, serverId, startUtc, endUtc, databaseNames, cancellationToken);
}
