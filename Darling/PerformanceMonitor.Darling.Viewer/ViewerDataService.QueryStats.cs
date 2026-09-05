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
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One Top-Queries-by-Duration grid row — the viewer copy of Lite's <c>QueryStatsRow</c>
/// (LocalDataService.QueryStats.cs). A (database, query_hash, host_object) group's summed deltas +
/// min/max spreads over the window, with the latest captured query text. Numeric members stay in the
/// store's units (microseconds for CPU/duration, KB for grants); the display properties convert,
/// mirroring Lite's grid. <see cref="LastExecutionTime"/>/<see cref="CreationTime"/> are the SQL
/// SERVER's local wall clock (dm_exec_query_stats), NOT UTC — shown raw (no naive-UTC-to-local
/// conversion, matching the absorbed top-50 grid this replaces). <see cref="HasQueryPlan"/> is a cheap
/// presence flag from the read (bool_or over the group) that gates the grid's Query Plan column; the full
/// query_plan_xml is fetched on demand by <see cref="ViewerDataService.GetQueryStatsPlanXmlAsync"/>, so a
/// multi-KB plan never rides in every grid row (Lite's HasQueryPlan gating pattern, sourced from the store).
/// </summary>
public sealed class ViewerQueryStatsRow
{
    public string DatabaseName { get; set; } = "";
    public string QueryHash { get; set; } = "";
    public DateTime? LastExecutionTime { get; set; }
    public DateTime? CreationTime { get; set; }
    public string LastExecutionTimeLocal => ViewerDataService.FormatServerClock(LastExecutionTime);
    public string CreationTimeLocal => ViewerDataService.FormatServerClock(CreationTime);
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
    /// <summary>True when the collector captured a query_plan_xml for this (database, query_hash) —
    /// gates the grid's Query Plan column (the full plan is fetched on demand, not carried here).</summary>
    public bool HasQueryPlan { get; set; }

    /// <summary>#2012 stage 2: the statement's hosting module (<c>schema.object</c>) captured at
    /// collection from dm_exec_sql_text.objectid; null for ad-hoc/prepared text and for rows
    /// collected before the column existed. Part of the group key, so same-hash statements hosted
    /// by different procs (INSERT...EXEC) land in separate rows.</summary>
    public string? HostObjectName { get; set; }

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

    /// <summary>True when this row carries a plan_handle — gates the grid's "Fetch Live Plan" context item (the
    /// live-cache fetch is keyed on plan_handle, distinct from the stored query_plan_xml "View Plan" opens).</summary>
    public bool HasLivePlanHandle => !string.IsNullOrEmpty(PlanHandle);

    /// <summary>True when this row can have its ACTUAL plan captured — it carries the query text plus the
    /// stored-row key (query_hash) the service re-executes by. Gates the grid's "Get Actual Plan" context item;
    /// only Top Queries rows define it, so the Query Store / Top Procedures rows that SHARE this menu fall back
    /// to disabled (their FallbackValue). Mirrors Lite, whose "Get Actual Plan" lives on the query-stats grid.</summary>
    public bool CanGetActualPlan => !string.IsNullOrEmpty(QueryText) && !string.IsNullOrEmpty(QueryHash);
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

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Top-Queries-by-Duration read — Lite's <c>GetTopQueriesByCpuAsync</c> (despite the name it
    /// ranks by summed <c>delta_elapsed_time</c>, matching the grid's default TotalElapsedMs-descending
    /// sort) ported to Postgres against <c>v_query_stats</c> (the view resolves the #1767 payload
    /// dimensions, so the text/plan columns read the same as they did inline). Group by
    /// (database_name, query_hash, host_object_name) over the window (#2012 stage 2 — the host object
    /// splits same-hash statements hosted by different procs, e.g. INSERT...EXEC callers; SQL GROUP BY
    /// treats NULLs as equal so ad-hoc rows still collapse), sum the deltas + carry the min/max spreads,
    /// fetch each group's latest non-null query text via a LATERAL join constrained to the group's own
    /// host object, drop WAITFOR shells (over-fetch by 5
    /// exactly like Lite's <c>LIMIT top + 5</c> → filter → <c>LIMIT top</c>), and cap at top.
    /// Viewer deviations from Lite: (1) Postgres <c>SUM(bigint)</c> is numeric, so each summed aggregate
    /// is CAST back to bigint for the typed GetInt64 readers (MIN/MAX of bigint stay bigint, no cast);
    /// (2) Lite's server-local <c>last_execution_time</c> staleness filter and its utcOffsetMinutes
    /// parameter are dropped (the viewer has no per-server UTC offset; the delta HAVING already excludes
    /// plans that never ran in the window); (3) the LATERAL fetches only query_text; the collected
    /// query_plan_xml is not carried per row (it can be multi-KB) — instead bool_or(query_plan_xml IS NOT
    /// NULL) rides back as has_query_plan to gate the grid's Query Plan column, and the plan itself is
    /// fetched on demand by <see cref="GetQueryStatsPlanXmlAsync"/>.
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 top.
    /// </summary>
    public const string TopQueriesSql = """
        WITH ranked AS (
            SELECT
                database_name,
                query_hash,
                MAX(last_execution_time) AS last_execution_time,
                MAX(creation_time) AS creation_time,
                CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
                CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
                CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
                CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
                CAST(SUM(delta_rows) AS bigint) AS total_rows,
                CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
                CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
                CAST(SUM(delta_spills) AS bigint) AS total_spills,
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
                MAX(CAST(delta_worker_time AS double precision) / NULLIF(sample_interval_seconds, 0) / 1000.0) AS worker_time_per_second,
                /* A digest means the plan lives in query_plan_dim (#1767), so presence is testable
                   without resolving it. Deliberately the base table here, not v_query_stats: this
                   CTE aggregates the whole window and needs only a presence flag, and reading the
                   resolving view would make Postgres join the plan dimension per row to evaluate
                   it (it can drop an unreferenced unique join, but the COALESCE references it).
                   The LATERAL below, which needs the actual text, does read the view. */
                bool_or(query_plan_xml IS NOT NULL OR query_plan_digest IS NOT NULL) AS has_query_plan,
                host_object_name
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
            GROUP BY database_name, query_hash, host_object_name
            HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
            ORDER BY SUM(delta_elapsed_time) DESC
            LIMIT $4 + 5
        ),
        module AS (
            /* #1568 module attribution: one procedure_stats identity per sql_handle (latest
               collection_time wins) so a statement whose sql_handle matches a cached
               procedure/function/trigger inherits its db.schema.object and the query_stats row never
               fans out. Both stores persist the SAME normalized CONVERT(varchar(130), ..., 1) handle
               text, so the join key lines up; unmatched -> ad hoc. */
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
                FROM procedure_stats
                WHERE server_id = $1
                AND   sql_handle IS NOT NULL
                AND   sql_handle <> ''
            ) AS ranked_modules
            WHERE rn = 1
        )
        SELECT
            r.database_name,
            r.query_hash,
            r.last_execution_time,
            r.creation_time,
            r.total_executions,
            r.total_cpu_us,
            r.total_elapsed_us,
            r.total_reads,
            r.total_rows,
            r.total_writes,
            r.total_physical_reads,
            r.total_spills,
            r.min_dop,
            r.max_dop,
            r.min_worker_time,
            r.max_worker_time,
            r.min_elapsed_time,
            r.max_elapsed_time,
            r.min_physical_reads,
            r.max_physical_reads,
            r.min_rows,
            r.max_rows,
            r.min_grant_kb,
            r.max_grant_kb,
            r.min_spills,
            r.max_spills,
            r.query_plan_hash,
            r.sql_handle,
            r.plan_handle,
            r.min_used_grant_kb,
            r.max_used_grant_kb,
            r.min_ideal_grant_kb,
            r.max_ideal_grant_kb,
            r.min_reserved_threads,
            r.max_reserved_threads,
            r.min_used_threads,
            r.max_used_threads,
            r.total_clr_time,
            r.plan_generation_num,
            r.worker_time_per_second,
            t.query_text,
            r.has_query_plan,
            m.object_name AS module_object_name,
            m.schema_name AS module_schema_name,
            m.database_name AS module_database_name,
            r.host_object_name
        FROM ranked AS r
        LEFT JOIN LATERAL (
            SELECT query_text
            FROM v_query_stats
            WHERE server_id = $1
            AND   query_hash = r.query_hash
            AND   database_name = r.database_name
            /* #2012 stage 2: the representative text must come from THIS group's own rows — without
               the host constraint a hash shared across host objects could label one caller's row
               with another caller's text (NOT DISTINCT FROM so ad-hoc NULL hosts still match). */
            AND   host_object_name IS NOT DISTINCT FROM r.host_object_name
            AND   query_text IS NOT NULL
            ORDER BY collection_time DESC
            LIMIT 1
        ) AS t ON TRUE
        LEFT JOIN module AS m ON m.sql_handle = r.sql_handle
        WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
        ORDER BY r.total_elapsed_us DESC
        LIMIT $4
        """;

    /// <summary>Grid cell preview cap for query/SQL text (single line).</summary>
    public const int CellTextCap = 160;

    /// <summary>Tooltip cap for query/SQL text — enough to read, not a 40 KB balloon.</summary>
    public const int TooltipTextCap = 4000;

    /// <summary>Default page size for the Top-Queries / Top-Procedures / Query-Store grids (Lite's top-50).</summary>
    public const int TopQueriesPageSize = 50;

    /// <summary>
    /// The top query-stats groups for one server over [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>], pre-sorted by total elapsed time descending (the grid's default sort).
    /// </summary>
    public async Task<List<ViewerQueryStatsRow>> GetTopQueriesByCpuAsync(
        int serverId, DateTime startUtc, DateTime endUtc, int top = TopQueriesPageSize, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerQueryStatsRow>();

        await using var command = _dataSource.CreateCommand(TopQueriesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerQueryStatsRow
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
                MinDop = reader.IsDBNull(12) ? 0 : (int)reader.GetInt64(12),
                MaxDop = reader.IsDBNull(13) ? 0 : (int)reader.GetInt64(13),
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
                WorkerTimePerSecond = reader.IsDBNull(39) ? 0 : reader.GetDouble(39),
                QueryText = reader.IsDBNull(40) ? "" : reader.GetString(40),
                HasQueryPlan = !reader.IsDBNull(41) && reader.GetBoolean(41),
                ModuleObjectName = reader.IsDBNull(42) ? "" : reader.GetString(42),
                ModuleSchemaName = reader.IsDBNull(43) ? "" : reader.GetString(43),
                ModuleDatabaseName = reader.IsDBNull(44) ? "" : reader.GetString(44),
                HostObjectName = reader.IsDBNull(45) ? null : reader.GetString(45),
            });
        }

        return rows;
    }

    /// <summary>
    /// Top-Queries comparison — Lite's <c>GetQueryStatsComparisonAsync</c> ported. Unions the top-100
    /// (by summed executions) hashes of the current and baseline windows, aggregates each period's
    /// per-execution averages, and FULL OUTER JOINs the two so NEW / GONE queries surface. Returns the
    /// shared <see cref="QueryStatsComparisonItem"/> the .Ui comparison grid binds (delta % + badges).
    /// PG dialect deviations from Lite's DuckDB: <c>::double precision</c> casts on the summed bigints
    /// before the per-execution division; the CTE INNER JOINs keep Lite's null-safe
    /// <c>IS NOT DISTINCT FROM</c>, but the outer FULL JOIN uses <c>COALESCE(key,'')</c> equality —
    /// Postgres only FULL-JOINs on merge/hash-joinable conditions and <c>IS NOT DISTINCT FROM</c> is
    /// neither (query_hash / database_name are never the empty string, so the sentinel is collision-free).
    /// $1 server_id, $2/$3 current window, $4/$5 baseline window (naive UTC).
    /// </summary>
    public const string QueryStatsComparisonSql = """
        WITH top_current AS (
            SELECT query_hash, database_name
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2 AND collection_time <= $3
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
            AND   delta_execution_count > 0
            GROUP BY query_hash, database_name
            ORDER BY SUM(delta_execution_count) DESC
            LIMIT 100
        ),
        top_baseline AS (
            SELECT query_hash, database_name
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $4 AND collection_time <= $5
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
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
            ) AS combined
        ),
        current_period AS (
            SELECT th.database_name, th.query_hash,
                   SUM(qs.delta_execution_count) AS exec_count,
                   SUM(qs.delta_elapsed_time)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
                   SUM(qs.delta_worker_time)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
                   SUM(qs.delta_physical_reads)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) AS avg_reads,
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
                   SUM(qs.delta_elapsed_time)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
                   SUM(qs.delta_worker_time)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
                   SUM(qs.delta_physical_reads)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) AS avg_reads,
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
          ON  COALESCE(c.database_name, '') = COALESCE(b.database_name, '')
          AND COALESCE(c.query_hash, '') = COALESCE(b.query_hash, '')
        """;

    /// <summary>Top-Queries current-vs-baseline comparison rows (shared .Ui item; delta % + NEW/GONE badges).</summary>
    public async Task<List<QueryStatsComparisonItem>> GetQueryStatsComparisonAsync(
        int serverId,
        DateTime currentStart, DateTime currentEnd,
        DateTime baselineStart, DateTime baselineEnd,
        IReadOnlyList<string>? databaseNames = null,
        CancellationToken cancellationToken = default)
    {
        var items = new List<QueryStatsComparisonItem>();

        await using var command = _dataSource.CreateCommand(QueryStatsComparisonSql);
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
    /// Hourly query-stats buckets for the Top-Queries slicer — Lite's <c>GetQueryStatsSlicerDataAsync</c>
    /// ported. Buckets by <c>date_trunc('hour', collection_time)</c> into the shared
    /// <see cref="TimeSliceBucket"/>; the columns feed the slicer's sort-driven metric overlay
    /// (CPU / duration / reads / writes / physical reads). PG dialect: <c>COUNT(DISTINCT …)</c> is bigint
    /// and <c>SUM(…)/1000.0</c> is numeric, so the reads go through <c>Convert</c> tolerantly.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string QueryStatsSlicerSql = """
        SELECT
            date_trunc('hour', collection_time) AS bucket,
            COUNT(DISTINCT query_hash) AS query_count,
            COALESCE(SUM(delta_worker_time), 0) / 1000.0 AS total_cpu_ms,
            COALESCE(SUM(delta_elapsed_time), 0) / 1000.0 AS total_elapsed_ms,
            COALESCE(SUM(delta_logical_reads), 0) AS total_reads,
            COALESCE(SUM(delta_logical_writes), 0) AS total_writes,
            COALESCE(SUM(delta_physical_reads), 0) AS total_physical_reads
        FROM query_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        GROUP BY date_trunc('hour', collection_time)
        ORDER BY bucket
        """;

    /// <summary>Hourly query-stats slicer buckets over [<paramref name="startUtc"/>, <paramref name="endUtc"/>].</summary>
    public async Task<List<TimeSliceBucket>> GetQueryStatsSlicerDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
        => await ReadQueryStatsSlicerAsync(QueryStatsSlicerSql, serverId, startUtc, endUtc, databaseNames, cancellationToken);

    /// <summary>
    /// Shared reader for the query-stats / procedure-stats hourly slicer buckets (same column shape:
    /// bucket, count, cpu_ms, elapsed_ms, reads, writes, physical_reads). The default slicer metric is
    /// total CPU (Value = TotalCpu), matching Lite's initial "Total CPU (ms)" label.
    /// </summary>
    internal async Task<List<TimeSliceBucket>> ReadQueryStatsSlicerAsync(
        string sql, int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken)
    {
        var items = new List<TimeSliceBucket>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var cpu = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2));
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                TotalCpu = cpu,
                TotalElapsed = reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                TotalLogicalReads = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                TotalWrites = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                TotalPhysicalReads = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                Value = cpu,
            });
        }

        return items;
    }

    // ── Shared parameter helpers for the Queries reads (serverId + naive-UTC window) ──

    /// <summary>$1 server_id, $2 start, $3 end (naive UTC — Kind=Unspecified maps to the timestamp columns).</summary>
    internal static void AddServerWindowParameters(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
    }

    /// <summary>$1 server_id, $2/$3 current window, $4/$5 baseline window (all naive UTC).</summary>
    internal static void AddComparisonParameters(
        NpgsqlCommand command, int serverId,
        DateTime currentStart, DateTime currentEnd, DateTime baselineStart, DateTime baselineEnd)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(currentStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(currentEnd, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineEnd, DateTimeKind.Unspecified) });
    }

    // ── Query / SQL text display helpers (shared by the Queries + Blocking grids) ──

    /// <summary>
    /// Formats a SQL-server-local wall-clock time (dm_exec_* last_execution_time / creation_time /
    /// cached_time — NOT naive UTC) for the grid: shown raw, no timezone conversion. Empty when null.
    /// </summary>
    public static string FormatServerClock(DateTime? serverLocal)
        => serverLocal.HasValue ? serverLocal.Value.ToString("yyyy-MM-dd HH:mm:ss") : "";

    /// <summary>
    /// Collapses whitespace runs (query text arrives with its original formatting) to a single
    /// space and caps at <see cref="CellTextCap"/> with an ellipsis — the one-line grid preview.
    /// </summary>
    public static string TruncateForCell(string? text)
        => Truncate(CollapseWhitespace(text), CellTextCap);

    /// <summary>Caps the raw text at <see cref="TooltipTextCap"/> with an ellipsis, keeping line breaks.</summary>
    public static string TruncateForTooltip(string? text)
        => Truncate(text ?? "", TooltipTextCap);

    private static string Truncate(string text, int cap)
        => text.Length <= cap ? text : text.Substring(0, cap) + "…";

    private static string CollapseWhitespace(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return "";
        }

        var sb = new System.Text.StringBuilder(Math.Min(text.Length, CellTextCap + 16));
        var inWhitespace = false;
        foreach (var c in text)
        {
            if (char.IsWhiteSpace(c))
            {
                inWhitespace = true;
                continue;
            }

            if (inWhitespace && sb.Length > 0)
            {
                sb.Append(' ');
            }

            inWhitespace = false;
            sb.Append(c);

            if (sb.Length > CellTextCap)
            {
                break; /* preview cap — no need to walk a multi-KB statement */
            }
        }

        return sb.ToString();
    }
}
