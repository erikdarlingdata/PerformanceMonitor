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
/// One Query-Store-by-Duration grid row — the viewer copy of Lite's <c>QueryStoreRow</c>
/// (LocalDataService.QueryStore.cs). A (database, query_id, plan_id, query_hash) group's interval
/// averages + min/max spreads over the window, with the latest captured query text.
/// Duration / CPU / CLR are converted to ms (from us) in SQL; memory-grant pages are converted to MB
/// in SQL; reads/writes/rows/tempdb pages / log bytes stay raw. The View-Plan surface is deferred, so
/// unlike Lite there is no QueryPlanText/HasQueryPlan (the collected query_plan_text is not read).
/// </summary>
public sealed class ViewerQueryStoreRow
{
    public string DatabaseName { get; set; } = "";
    public long QueryId { get; set; }
    public long PlanId { get; set; }

    /// <summary>
    /// The replica role SQL Server 2022+ attributed these runtime stats to ('Primary', 'Secondary',
    /// 'Geo Secondary', 'Geo HA Secondary'), a GROUP BY key so an AG's shared Query Store does not
    /// blend replicas into one row. NULL/blank = the server did not attribute it (pre-2022, or a 2022
    /// standalone) — expected, and deliberately not coalesced.
    /// </summary>
    public string? ReplicaRole { get; set; }
    public string QueryHash { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string ModuleName { get; set; } = "";

    /// <summary>Gates "View Stored Plan" in the QueryStorePlanContextMenu — Query Store rows carry a stored plan.</summary>
    public bool HasStoredPlan => true;

    /// <summary>Gates "Get Actual Plan (re-run)" — the service re-executes by QueryId (BuildActualPlanArgsForQueryStore).</summary>
    public bool CanGetActualPlan => QueryId != 0;
    public long TotalExecutions { get; set; }
    public double AvgDurationMs { get; set; }
    public double AvgCpuTimeMs { get; set; }
    public double AvgLogicalReads { get; set; }
    public double AvgLogicalWrites { get; set; }
    public double AvgPhysicalReads { get; set; }
    public double AvgRowcount { get; set; }
    public long MinDop { get; set; }
    public long MaxDop { get; set; }
    public DateTime? LastExecutionTime { get; set; }
    public string QueryPlanHash { get; set; } = "";
    public bool IsForcedPlan { get; set; }
    public string PlanForcingType { get; set; } = "";
    public string ExecutionTypeDesc { get; set; } = "";
    public DateTime? FirstExecutionTime { get; set; }
    public double AvgClrTimeMs { get; set; }
    public double AvgTempdbSpaceUsed { get; set; }
    public double AvgLogBytesUsed { get; set; }
    public string PlanType { get; set; } = "";
    public long ForceFailureCount { get; set; }
    public string LastForceFailureReason { get; set; } = "";
    public int CompatibilityLevel { get; set; }
    public double MinDurationMs { get; set; }
    public double MaxDurationMs { get; set; }
    public double MinCpuTimeMs { get; set; }
    public double MaxCpuTimeMs { get; set; }
    public double MinLogicalReads { get; set; }
    public double MaxLogicalReads { get; set; }
    public double MinLogicalWrites { get; set; }
    public double MaxLogicalWrites { get; set; }
    public double MinPhysicalReads { get; set; }
    public double MaxPhysicalReads { get; set; }
    public double MinClrTimeMs { get; set; }
    public double MaxClrTimeMs { get; set; }
    public double MinRowcount { get; set; }
    public double MaxRowcount { get; set; }
    public double MinLogBytesUsed { get; set; }
    public double MaxLogBytesUsed { get; set; }
    public double MinTempdbSpaceUsed { get; set; }
    public double MaxTempdbSpaceUsed { get; set; }
    public double AvgMemoryMb { get; set; }
    public double MinMemoryMb { get; set; }
    public double MaxMemoryMb { get; set; }
    public double AvgNumPhysicalIoReads { get; set; }
    public double MinNumPhysicalIoReads { get; set; }
    public double MaxNumPhysicalIoReads { get; set; }
    public string FirstExecutionTimeLocal => ViewerDataService.FormatServerClock(FirstExecutionTime);
    public string LastExecutionTimeLocal => ViewerDataService.FormatServerClock(LastExecutionTime);
    public double TotalCpuMs => TotalExecutions * AvgCpuTimeMs;
    public double TotalDurationMs => TotalExecutions * AvgDurationMs;
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Query-Store-by-Duration read — Lite's <c>GetQueryStoreTopQueriesAsync</c> ported to Postgres
    /// against the base <c>query_store_stats</c> table (the store name; Lite reads its
    /// <c>v_query_store_stats</c> view). Group by (database, query_id, plan_id, query_hash), average the
    /// per-interval metrics + carry min/max spreads, rank by <c>SUM(execution_count) *
    /// AVG(avg_duration_us)</c> (total duration) descending, over-fetch by 5 for the WAITFOR trim, and cap
    /// at top. Viewer deviations: <c>SUM(execution_count)</c> CAST to bigint; the query_plan_text column
    /// is not selected (plan surfacing deferred); the forced-plan flag folds through Postgres
    /// <c>bool_or(is_forced_plan)</c> (Postgres has no <c>MAX(boolean)</c> aggregate, unlike DuckDB).
    /// Every avg/min/max metric column is stored as bigint, so each is CAST to double precision before
    /// the AVG/scale arithmetic (matching Lite's DuckDB casts).
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 top.
    /// </summary>
    public const string QueryStoreTopSql = """
        WITH deduped AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
               per-Query-Store-interval snapshots, and the collector re-fetches the OPEN interval every
               cycle as its last_execution_time advances, so the SAME interval (same first_execution_time)
               is stored repeatedly with a growing execution_count. SUM(execution_count) over the raw rows
               reports 10 + 25 + 40 for an interval that reached 40, and AVG(avg_*) becomes an avg-of-avgs
               weighted by how many times each interval happened to be re-collected. Keep the LATEST
               snapshot per interval. Matches the dedup convention in PgFactCollector.QueryPerf and
               PgDrillDownCollector.Queries, and Lite's GetQueryStoreTopQueriesAsync.

               replica_role and execution_type_desc are in the partition because the aggregate below is
               grouped (or MAXed) on them: the dedup key must be at least as fine as the read's own row
               identity, or dedup would silently drop a row the grid must show rather than de-duplicate one.

               The execution_count tie-break is the #1907 residual, and it is NOT decoration. collection_time
               alone was not a total order on rows collected before that fix: Query Store hands back the
               flushed and the still-in-memory slice of ONE interval as two ADDITIVE rows, the collector
               stored both, and they then shared every partition column above AND collection_time. ROW_NUMBER
               kept whichever the engine emitted first, so a grid could show an in-memory sliver of 8 where
               the interval's total was 94, and show a different one on the next run. The collector now
               combines the slices before storing, so rows collected after #1907 cannot tie at all and this
               clause never fires on them. It is here for the rows already in the store, which cannot be
               rewritten: it deterministically picks the FLUSHED slice, the one holding the bulk of the
               interval's work, instead of flapping. Closest-available, not correct — the correct value is the
               SUM of the slices, which no read-side rule can express (#1912). This applies to EVERY dedup
               site in both apps: Darling.Tests/QueryStoreSliceTieBreakSourceTests pins all 12 of Darling's
               across the 8 files that hold them, and Lite.Tests/QueryStoreSliceTieBreakSourceTests pins its
               7, so dropping one fails loudly and names the file. Both enumerate their files rather than
               globbing, so MOVING a read has to be re-declared instead of silently shrinking the guard. */
            SELECT
                *,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
        ),
        ranked AS (
            SELECT
                database_name,
                query_id,
                plan_id,
                query_hash,
                /* A GROUP BY key, not MAX(): with Query Store for secondary replicas (2022+) the primary
                   holds ONE shared Query Store carrying every replica's rows, so grouping without it would
                   average primary and secondary workload into a single blended row — the conflation this
                   column exists to expose. Grouping splits them into one row per replica role instead. On a
                   standalone/non-AG server every row shares one value (NULL, or 'Primary' on 2025), so the
                   grouping is a no-op and the grid is unchanged. Twins Lite's QueryStore reader. */
                replica_role,
                MAX(module_name) AS module_name,
                CAST(SUM(execution_count) AS bigint) AS total_executions,
                AVG(CAST(avg_duration_us AS double precision)) / 1000.0 AS avg_duration_ms,
                AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0 AS avg_cpu_time_ms,
                AVG(CAST(avg_logical_io_reads AS double precision)) AS avg_logical_reads,
                AVG(CAST(avg_logical_io_writes AS double precision)) AS avg_logical_writes,
                AVG(CAST(avg_physical_io_reads AS double precision)) AS avg_physical_reads,
                AVG(CAST(avg_rowcount AS double precision)) AS avg_rowcount,
                MIN(min_dop) AS min_dop,
                MAX(max_dop) AS max_dop,
                MAX(last_execution_time) AS last_execution_time,
                MAX(query_plan_hash) AS query_plan_hash,
                bool_or(is_forced_plan) AS is_forced_plan,
                MAX(plan_forcing_type) AS plan_forcing_type,
                MAX(execution_type_desc) AS execution_type_desc,
                MIN(first_execution_time) AS first_execution_time,
                AVG(CAST(avg_clr_time_us AS double precision)) / 1000.0 AS avg_clr_time_ms,
                AVG(CAST(avg_tempdb_space_used AS double precision)) AS avg_tempdb_space_used,
                AVG(CAST(avg_log_bytes_used AS double precision)) AS avg_log_bytes_used,
                MAX(plan_type) AS plan_type,
                MAX(force_failure_count) AS force_failure_count,
                MAX(last_force_failure_reason) AS last_force_failure_reason,
                MAX(compatibility_level) AS compatibility_level,
                MIN(CAST(min_duration_us AS double precision)) / 1000.0 AS min_duration_ms,
                MAX(CAST(max_duration_us AS double precision)) / 1000.0 AS max_duration_ms,
                MIN(CAST(min_cpu_time_us AS double precision)) / 1000.0 AS min_cpu_time_ms,
                MAX(CAST(max_cpu_time_us AS double precision)) / 1000.0 AS max_cpu_time_ms,
                MIN(CAST(min_logical_io_reads AS double precision)) AS min_logical_reads,
                MAX(CAST(max_logical_io_reads AS double precision)) AS max_logical_reads,
                MIN(CAST(min_logical_io_writes AS double precision)) AS min_logical_writes,
                MAX(CAST(max_logical_io_writes AS double precision)) AS max_logical_writes,
                MIN(CAST(min_physical_io_reads AS double precision)) AS min_physical_reads,
                MAX(CAST(max_physical_io_reads AS double precision)) AS max_physical_reads,
                MIN(CAST(min_clr_time_us AS double precision)) / 1000.0 AS min_clr_time_ms,
                MAX(CAST(max_clr_time_us AS double precision)) / 1000.0 AS max_clr_time_ms,
                MIN(CAST(min_rowcount AS double precision)) AS min_rowcount,
                MAX(CAST(max_rowcount AS double precision)) AS max_rowcount,
                MIN(CAST(min_log_bytes_used AS double precision)) AS min_log_bytes_used,
                MAX(CAST(max_log_bytes_used AS double precision)) AS max_log_bytes_used,
                MIN(CAST(min_tempdb_space_used AS double precision)) AS min_tempdb_space_used,
                MAX(CAST(max_tempdb_space_used AS double precision)) AS max_tempdb_space_used,
                AVG(CAST(avg_query_max_used_memory AS double precision)) * 8.0 / 1024.0 AS avg_memory_mb,
                MIN(CAST(min_query_max_used_memory AS double precision)) * 8.0 / 1024.0 AS min_memory_mb,
                MAX(CAST(max_query_max_used_memory AS double precision)) * 8.0 / 1024.0 AS max_memory_mb,
                AVG(CAST(avg_num_physical_io_reads AS double precision)) AS avg_num_physical_io_reads,
                MIN(CAST(min_num_physical_io_reads AS double precision)) AS min_num_physical_io_reads,
                MAX(CAST(max_num_physical_io_reads AS double precision)) AS max_num_physical_io_reads
            FROM deduped
            WHERE rn = 1
            GROUP BY database_name, query_id, plan_id, query_hash, replica_role
            ORDER BY SUM(execution_count) * AVG(CAST(avg_duration_us AS double precision)) DESC
            LIMIT $4 + 5
        )
        SELECT
            r.database_name,
            r.query_id,
            r.plan_id,
            r.query_hash,
            t.query_text,
            r.module_name,
            r.total_executions,
            r.avg_duration_ms,
            r.avg_cpu_time_ms,
            r.avg_logical_reads,
            r.avg_logical_writes,
            r.avg_physical_reads,
            r.avg_rowcount,
            r.min_dop,
            r.max_dop,
            r.last_execution_time,
            r.query_plan_hash,
            r.is_forced_plan,
            r.plan_forcing_type,
            r.execution_type_desc,
            r.first_execution_time,
            r.avg_clr_time_ms,
            r.avg_tempdb_space_used,
            r.avg_log_bytes_used,
            r.plan_type,
            r.force_failure_count,
            r.last_force_failure_reason,
            r.compatibility_level,
            r.min_duration_ms,
            r.max_duration_ms,
            r.min_cpu_time_ms,
            r.max_cpu_time_ms,
            r.min_logical_reads,
            r.max_logical_reads,
            r.min_logical_writes,
            r.max_logical_writes,
            r.min_physical_reads,
            r.max_physical_reads,
            r.min_clr_time_ms,
            r.max_clr_time_ms,
            r.min_rowcount,
            r.max_rowcount,
            r.min_log_bytes_used,
            r.max_log_bytes_used,
            r.min_tempdb_space_used,
            r.max_tempdb_space_used,
            r.avg_memory_mb,
            r.min_memory_mb,
            r.max_memory_mb,
            r.avg_num_physical_io_reads,
            r.min_num_physical_io_reads,
            r.max_num_physical_io_reads,
            r.replica_role
        FROM ranked AS r
        /* #2150: resolve the text ONCE, inside the lateral, so everything downstream still reads a single
           t.query_text — the projection above and the WAITFOR self-exclusion below both get the resolved
           value without either having to know where it came from.
           First arm: collect.query_store_text, one row per (server, database, query_id), which is where the
           collector lands text once the separate fetch is on. Second arm: the newest inline query_text on
           the fact row, which is where text lived BEFORE the cutover — it stays because dropping it would
           blank the text on all existing history. */
        LEFT JOIN LATERAL (
            SELECT COALESCE(
                       (
                           SELECT x.query_sql_text
                           FROM query_store_text AS x
                           WHERE x.server_id = $1
                           AND   x.database_name = r.database_name
                           AND   x.query_id = r.query_id
                       ),
                       (
                           SELECT s.query_text
                           FROM query_store_stats AS s
                           WHERE s.server_id = $1
                           AND   s.query_id = r.query_id
                           AND   s.database_name = r.database_name
                           AND   s.query_text IS NOT NULL
                           ORDER BY s.collection_time DESC
                           LIMIT 1
                       )
                   ) AS query_text
        ) AS t ON TRUE
        WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
        ORDER BY r.total_executions * r.avg_duration_ms DESC
        LIMIT $4
        """;

    /// <summary>
    /// The top Query Store queries for one server over [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>], pre-sorted by total duration descending (the grid's default sort).
    /// </summary>
    public async Task<List<ViewerQueryStoreRow>> GetQueryStoreTopQueriesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int top = TopQueriesPageSize, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerQueryStoreRow>();

        await using var command = _dataSource.CreateCommand(QueryStoreTopSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = top });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerQueryStoreRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                PlanId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                QueryHash = reader.IsDBNull(3) ? "" : reader.GetString(3),
                QueryText = reader.IsDBNull(4) ? "" : reader.GetString(4),
                ModuleName = reader.IsDBNull(5) ? "" : reader.GetString(5),
                TotalExecutions = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                AvgDurationMs = reader.IsDBNull(7) ? 0 : Convert.ToDouble(reader.GetValue(7)),
                AvgCpuTimeMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
                AvgLogicalReads = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9)),
                AvgLogicalWrites = reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10)),
                AvgPhysicalReads = reader.IsDBNull(11) ? 0 : Convert.ToDouble(reader.GetValue(11)),
                AvgRowcount = reader.IsDBNull(12) ? 0 : Convert.ToDouble(reader.GetValue(12)),
                MinDop = reader.IsDBNull(13) ? 0 : Convert.ToInt64(reader.GetValue(13)),
                MaxDop = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14)),
                LastExecutionTime = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                QueryPlanHash = reader.IsDBNull(16) ? "" : reader.GetString(16),
                IsForcedPlan = !reader.IsDBNull(17) && reader.GetBoolean(17),
                PlanForcingType = reader.IsDBNull(18) ? "" : reader.GetString(18),
                ExecutionTypeDesc = reader.IsDBNull(19) ? "" : reader.GetString(19),
                FirstExecutionTime = reader.IsDBNull(20) ? null : reader.GetDateTime(20),
                AvgClrTimeMs = reader.IsDBNull(21) ? 0 : Convert.ToDouble(reader.GetValue(21)),
                AvgTempdbSpaceUsed = reader.IsDBNull(22) ? 0 : Convert.ToDouble(reader.GetValue(22)),
                AvgLogBytesUsed = reader.IsDBNull(23) ? 0 : Convert.ToDouble(reader.GetValue(23)),
                PlanType = reader.IsDBNull(24) ? "" : reader.GetString(24),
                ForceFailureCount = reader.IsDBNull(25) ? 0 : Convert.ToInt64(reader.GetValue(25)),
                LastForceFailureReason = reader.IsDBNull(26) ? "" : reader.GetString(26),
                CompatibilityLevel = reader.IsDBNull(27) ? 0 : Convert.ToInt32(reader.GetValue(27)),
                MinDurationMs = reader.IsDBNull(28) ? 0 : Convert.ToDouble(reader.GetValue(28)),
                MaxDurationMs = reader.IsDBNull(29) ? 0 : Convert.ToDouble(reader.GetValue(29)),
                MinCpuTimeMs = reader.IsDBNull(30) ? 0 : Convert.ToDouble(reader.GetValue(30)),
                MaxCpuTimeMs = reader.IsDBNull(31) ? 0 : Convert.ToDouble(reader.GetValue(31)),
                MinLogicalReads = reader.IsDBNull(32) ? 0 : Convert.ToDouble(reader.GetValue(32)),
                MaxLogicalReads = reader.IsDBNull(33) ? 0 : Convert.ToDouble(reader.GetValue(33)),
                MinLogicalWrites = reader.IsDBNull(34) ? 0 : Convert.ToDouble(reader.GetValue(34)),
                MaxLogicalWrites = reader.IsDBNull(35) ? 0 : Convert.ToDouble(reader.GetValue(35)),
                MinPhysicalReads = reader.IsDBNull(36) ? 0 : Convert.ToDouble(reader.GetValue(36)),
                MaxPhysicalReads = reader.IsDBNull(37) ? 0 : Convert.ToDouble(reader.GetValue(37)),
                MinClrTimeMs = reader.IsDBNull(38) ? 0 : Convert.ToDouble(reader.GetValue(38)),
                MaxClrTimeMs = reader.IsDBNull(39) ? 0 : Convert.ToDouble(reader.GetValue(39)),
                MinRowcount = reader.IsDBNull(40) ? 0 : Convert.ToDouble(reader.GetValue(40)),
                MaxRowcount = reader.IsDBNull(41) ? 0 : Convert.ToDouble(reader.GetValue(41)),
                MinLogBytesUsed = reader.IsDBNull(42) ? 0 : Convert.ToDouble(reader.GetValue(42)),
                MaxLogBytesUsed = reader.IsDBNull(43) ? 0 : Convert.ToDouble(reader.GetValue(43)),
                MinTempdbSpaceUsed = reader.IsDBNull(44) ? 0 : Convert.ToDouble(reader.GetValue(44)),
                MaxTempdbSpaceUsed = reader.IsDBNull(45) ? 0 : Convert.ToDouble(reader.GetValue(45)),
                AvgMemoryMb = reader.IsDBNull(46) ? 0 : Convert.ToDouble(reader.GetValue(46)),
                MinMemoryMb = reader.IsDBNull(47) ? 0 : Convert.ToDouble(reader.GetValue(47)),
                MaxMemoryMb = reader.IsDBNull(48) ? 0 : Convert.ToDouble(reader.GetValue(48)),
                AvgNumPhysicalIoReads = reader.IsDBNull(49) ? 0 : Convert.ToDouble(reader.GetValue(49)),
                MinNumPhysicalIoReads = reader.IsDBNull(50) ? 0 : Convert.ToDouble(reader.GetValue(50)),
                MaxNumPhysicalIoReads = reader.IsDBNull(51) ? 0 : Convert.ToDouble(reader.GetValue(51)),
                ReplicaRole = reader.IsDBNull(52) ? null : reader.GetString(52),
            });
        }

        return rows;
    }

    /// <summary>
    /// Query-Store comparison — Lite's <c>GetQueryStoreComparisonAsync</c> ported. Uses execution-count
    /// weighted averages (<c>SUM(execution_count * avg_metric) / SUM(execution_count)</c>) so periods with
    /// uneven interval counts aggregate correctly, top-100-union + FULL OUTER JOIN keyed on
    /// (database, query_hash). Returns the shared <see cref="QueryStatsComparisonItem"/>.
    /// $1 server_id, $2/$3 current window, $4/$5 baseline window (naive UTC).
    /// </summary>
    public const string QueryStoreComparisonSql = """
        WITH deduped_current AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. The rows are CUMULATIVE per-interval
               snapshots and the collector re-fetches the OPEN interval every cycle, so the SAME interval
               (same first_execution_time) is stored repeatedly with a growing execution_count. Keep the
               LATEST snapshot per interval before aggregating.

               The execution-count weighting below does NOT rescue this on its own: the repeated snapshots
               of one interval carry DIFFERENT (growing) weights AND different avg_* values, so an open
               interval is weighted by the triangular sum of its own growth. That bias is stronger in the
               recent window than in the baseline window (recent windows hold more still-open intervals),
               which skews the very delta this comparison exists to compute. One deduped CTE per window,
               so both arms are treated identically. */
            SELECT
                database_name,
                /* #2150: query_id is projected (it was already a partition key) so the period CTEs can
                   resolve text from collect.query_store_text, which is keyed on it. */
                query_id,
                query_hash,
                query_text,
                execution_count,
                avg_duration_us,
                avg_cpu_time_us,
                avg_logical_io_reads,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $2 AND collection_time <= $3
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
        ),
        deduped_baseline AS (
            SELECT
                database_name,
                /* #2150: query_id is projected (it was already a partition key) so the period CTEs can
                   resolve text from collect.query_store_text, which is keyed on it. */
                query_id,
                query_hash,
                query_text,
                execution_count,
                avg_duration_us,
                avg_cpu_time_us,
                avg_logical_io_reads,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $4 AND collection_time <= $5
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
        ),
        top_current AS (
            SELECT database_name, query_hash
            FROM deduped_current
            WHERE rn = 1
            AND   execution_count > 0
            GROUP BY database_name, query_hash
            ORDER BY SUM(execution_count) DESC
            LIMIT 100
        ),
        top_baseline AS (
            SELECT database_name, query_hash
            FROM deduped_baseline
            WHERE rn = 1
            AND   execution_count > 0
            GROUP BY database_name, query_hash
            ORDER BY SUM(execution_count) DESC
            LIMIT 100
        ),
        top_hashes AS (
            SELECT DISTINCT database_name, query_hash
            FROM (
                SELECT * FROM top_current
                UNION ALL
                SELECT * FROM top_baseline
            ) AS combined
        ),
        current_period AS (
            SELECT th.database_name, th.query_hash,
                   SUM(qs.execution_count) AS exec_count,
                   SUM(qs.execution_count * qs.avg_duration_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
                   SUM(qs.execution_count * qs.avg_cpu_time_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
                   SUM(qs.execution_count * qs.avg_logical_io_reads::double precision) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
                   /* #2150: this comparison groups by query_hash, but text is stored per query_id, so the
                      side table is joined on the finer key and MAX still picks one member's text for the
                      group — the same arbitrary-but-deterministic choice MAX(qs.query_text) made before.
                      The join cannot fan out (query_store_text is one row per server/database/query_id, by
                      primary key), so the execution-count SUMs above are unaffected. The COALESCE keeps
                      pre-cutover rows, whose text is still inline, reading exactly as they used to. */
                   MAX(COALESCE(x.query_sql_text, qs.query_text)) AS query_text
            FROM top_hashes th
            INNER JOIN deduped_current qs
              ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
              AND qs.database_name IS NOT DISTINCT FROM th.database_name
            LEFT JOIN query_store_text AS x
              ON  x.server_id = $1
              AND x.database_name = qs.database_name
              AND x.query_id = qs.query_id
            WHERE qs.rn = 1
            AND   qs.execution_count > 0
            GROUP BY th.database_name, th.query_hash
        ),
        baseline_period AS (
            SELECT th.database_name, th.query_hash,
                   SUM(qs.execution_count) AS exec_count,
                   SUM(qs.execution_count * qs.avg_duration_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
                   SUM(qs.execution_count * qs.avg_cpu_time_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
                   SUM(qs.execution_count * qs.avg_logical_io_reads::double precision) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
                   /* #2150 — same resolution as current_period above. Both arms need it because the final
                      projection takes COALESCE(c.query_text, b.query_text): converting only one arm would
                      leave a GONE row (present in baseline only) with no text to fall back to. */
                   MAX(COALESCE(x.query_sql_text, qs.query_text)) AS query_text
            FROM top_hashes th
            INNER JOIN deduped_baseline qs
              ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
              AND qs.database_name IS NOT DISTINCT FROM th.database_name
            LEFT JOIN query_store_text AS x
              ON  x.server_id = $1
              AND x.database_name = qs.database_name
              AND x.query_id = qs.query_id
            WHERE qs.rn = 1
            AND   qs.execution_count > 0
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
          ON  COALESCE(c.database_name, '') = COALESCE(b.database_name, '')
          AND COALESCE(c.query_hash, '') = COALESCE(b.query_hash, '')
        """;

    /// <summary>Query-Store current-vs-baseline comparison rows (shared .Ui item; delta % + NEW/GONE badges).</summary>
    public async Task<List<QueryStatsComparisonItem>> GetQueryStoreComparisonAsync(
        int serverId,
        DateTime currentStart, DateTime currentEnd,
        DateTime baselineStart, DateTime baselineEnd,
        IReadOnlyList<string>? databaseNames = null,
        CancellationToken cancellationToken = default)
    {
        var items = new List<QueryStatsComparisonItem>();

        await using var command = _dataSource.CreateCommand(QueryStoreComparisonSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddComparisonParameters(command, serverId, currentStart, currentEnd, baselineStart, baselineEnd);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new QueryStatsComparisonItem
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                ExecutionCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                AvgDurationMs = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                AvgCpuMs = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                AvgReads = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                BaselineExecutionCount = reader.IsDBNull(7) ? 0 : Convert.ToInt64(reader.GetValue(7)),
                BaselineAvgDurationMs = reader.IsDBNull(8) ? 0 : Convert.ToDouble(reader.GetValue(8)),
                BaselineAvgCpuMs = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9)),
                BaselineAvgReads = reader.IsDBNull(10) ? 0 : Convert.ToDouble(reader.GetValue(10)),
            });
        }

        return items;
    }

    /// <summary>
    /// Hourly Query Store buckets for the slicer — Lite's <c>GetQueryStoreSlicerDataAsync</c> ported.
    /// Query Store rows are already per-interval averages, so the bucket totals weight each by
    /// <c>execution_count</c> (<c>SUM(avg_metric * execution_count)</c>). Same 7-column shape as the
    /// query/procedure slicers, so it shares <see cref="ReadQueryStatsSlicerAsync"/>.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string QueryStoreSlicerSql = """
        WITH deduped AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. The rows are CUMULATIVE per-interval
               snapshots and the collector re-fetches the OPEN interval every cycle, so summing the raw
               rows counts one interval's work once per collection. The bucket is an HOUR and the cadence
               is ~5 minutes, so an open interval is summed up to ~12 times per bar — and a live store
               showed 496 collections of a single interval inside one hour bucket. Keep the LATEST
               snapshot per interval, then bucket, so the bars sum to the true total across the window.

               runtime_stats_interval_id is the REAL interval identity (tier 2), with first_execution_time
               kept beside it as the tier-1 proxy for rows collected before it existed — both in the key
               rather than a COALESCE, because the id is NULL on exactly the pre-tier-2 generation and
               non-NULL on exactly the post-tier-2 one, so the two can never collide and a legacy-only
               window degrades to precisely tier 1's key. Twins Lite's GetQueryStoreSlicerDataAsync. */
            SELECT
                collection_time,
                interval_start_time_utc,
                query_id,
                execution_count,
                avg_cpu_time_us,
                avg_duration_us,
                avg_logical_io_reads,
                avg_logical_io_writes,
                avg_physical_io_reads,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            /* The window is filtered on the SAME expression the bars are keyed on (#1892). It used to filter
               on collection_time while bucketing on the interval start, and once those stopped being the
               same instant (#1841 tier 2) the two disagreed at both edges: an interval that started before
               the window but closed inside it drew an extra bar to the LEFT of the requested range, and the
               window's own last interval -- whose closing fetch lands after the range ends -- was dropped
               entirely, which is the collection lag #1841 set out to remove showing up one layer down. */
            AND   COALESCE(interval_start_time_utc, collection_time) >= $2
            AND   COALESCE(interval_start_time_utc, collection_time) <= $3
            /* Chunk-exclusion bounds, NOT filters: neither can exclude a row the predicate above keeps in
               any realistic store. They matter because query_store_stats is a hypertable partitioned on
               collection_time -- without bounds on that column TimescaleDB must open and decompress every
               chunk from the window through the present rather than the ones the window overlaps. Same
               shape and reasoning as the drill-down collector's last_execution_time bound.

               The FLOOR is free: an interval is always collected after it starts, so
               interval_start_time_utc <= collection_time, and therefore COALESCE(...) >= $2 already implies
               collection_time >= $2. The extra day is slack against clock skew between the monitored
               server's interval clock and ours.

               The CEILING is deliberately enormous rather than tight, because tight is unsafe here. A row's
               collection_time exceeds its interval start by the interval's own length -- at most 1 day,
               since Query Store's INTERVAL_LENGTH_MINUTES accepts only 1/5/10/15/30/60/1440 -- plus however
               long the collector was behind, which nothing bounds. So 30 days is 1 day of engine maximum
               and 29 of collector-outage allowance. A month-long outage that then back-collects an interval
               straddling an old window's edge could still omit that one bar; the data stays in the store,
               and the alternative (no ceiling) makes every historical window scan to the present. */
            AND   collection_time >= $2 - interval '1 day'
            AND   collection_time <= $3 + interval '30 days'
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        )
        SELECT
            /* The bar sits in the hour the work RAN, not the hour it was last COLLECTED (#1841 tier 2).
               Query Store's default interval is 60 minutes and the closing fetch lands in the cycle AFTER
               the interval ends, so a collection_time bucket was reliably one bar late.
               interval_start_time_utc is the interval's own start boundary, converted to UTC at collection
               (the same clock as collection_time), so this is a placement fix and not a timezone trade.
               Legacy rows have no interval start and keep collection_time placement, so a window spanning
               the upgrade renders correctly on both sides and the lag disappears as those rows age out. */
            date_trunc('hour', COALESCE(interval_start_time_utc, collection_time)) AS bucket,
            COUNT(DISTINCT query_id) AS query_count,
            COALESCE(SUM(CAST(avg_cpu_time_us AS double precision) * execution_count), 0) / 1000.0 AS total_cpu_ms,
            COALESCE(SUM(CAST(avg_duration_us AS double precision) * execution_count), 0) / 1000.0 AS total_duration_ms,
            COALESCE(SUM(CAST(avg_logical_io_reads AS double precision) * execution_count), 0) AS total_reads,
            COALESCE(SUM(CAST(avg_logical_io_writes AS double precision) * execution_count), 0) AS total_writes,
            COALESCE(SUM(CAST(avg_physical_io_reads AS double precision) * execution_count), 0) AS total_physical_reads
        FROM deduped
        WHERE rn = 1
        GROUP BY date_trunc('hour', COALESCE(interval_start_time_utc, collection_time))
        ORDER BY bucket
        """;

    /// <summary>Hourly Query Store slicer buckets over [<paramref name="startUtc"/>, <paramref name="endUtc"/>].</summary>
    public async Task<List<TimeSliceBucket>> GetQueryStoreSlicerDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
        => await ReadQueryStatsSlicerAsync(QueryStoreSlicerSql, serverId, startUtc, endUtc, databaseNames, cancellationToken);
}
