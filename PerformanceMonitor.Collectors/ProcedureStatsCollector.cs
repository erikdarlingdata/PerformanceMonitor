/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Procedure/trigger/function statistics from sys.dm_exec_procedure_stats and friends.
/// Extracted verbatim from Lite's RemoteCollectorService.ProcedureStats.cs: the standard path is
/// dynamic SQL (spills columns don't exist in dm_exec_function_stats, so @spills_cols splices per
/// branch, and the exclusion filter interpolates as double-escaped literals because @sql is itself
/// a quoted string); Azure SQL DB skips plan_attributes (it reports dbid=1 for all plans) and has
/// no trigger/function branches. Seven delta groups keyed on plan_handle (falling back to
/// db.schema.object) to prevent cross-plan contamination.
/// </summary>
public sealed class ProcedureStatsCollector : CollectorDefinitionBase<ProcedureStatsCollector.Row>
{
    public static ProcedureStatsCollector Instance { get; } = new();

    private ProcedureStatsCollector()
    {
    }

    public readonly record struct Row(
        string DatabaseName,
        string SchemaName,
        string ObjectName,
        string ObjectType,
        DateTime? CachedTime,
        DateTime? LastExecutionTime,
        long ExecutionCount,
        long TotalWorkerTime,
        long TotalElapsedTime,
        long TotalLogicalReads,
        long TotalPhysicalReads,
        long TotalLogicalWrites,
        long MinWorkerTime,
        long MaxWorkerTime,
        long MinElapsedTime,
        long MaxElapsedTime,
        long MinLogicalReads,
        long MaxLogicalReads,
        long MinPhysicalReads,
        long MaxPhysicalReads,
        long MinLogicalWrites,
        long MaxLogicalWrites,
        long TotalSpills,
        long MinSpills,
        long MaxSpills,
        string? SqlHandle,
        string? PlanHandle,
        string? QueryPlanXml);

    private const string StandardQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @spills_cols nvarchar(400) = N'total_spills = ISNULL(s.total_spills, 0), min_spills = ISNULL(s.min_spills, 0), max_spills = ISNULL(s.max_spills, 0),',
    @fn_spills_cols nvarchar(400) = N'total_spills = CONVERT(bigint, 0), min_spills = CONVERT(bigint, 0), max_spills = CONVERT(bigint, 0),',
    @sql nvarchar(max);

SET @sql = CAST(N'
SELECT /* PerformanceMonitorLite */ TOP (150)
    ranked.*/*PLAN_SELECT*/
FROM
(
SELECT TOP (150) * FROM (
SELECT
    database_name = d.name,
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N''PROCEDURE'',
    cached_time = s.cached_time,
    last_execution_time = s.last_execution_time,
    execution_count = s.execution_count,
    total_worker_time = s.total_worker_time,
    total_elapsed_time = s.total_elapsed_time,
    total_logical_reads = s.total_logical_reads,
    total_physical_reads = s.total_physical_reads,
    total_logical_writes = s.total_logical_writes,
    min_worker_time = s.min_worker_time,
    max_worker_time = s.max_worker_time,
    min_elapsed_time = s.min_elapsed_time,
    max_elapsed_time = s.max_elapsed_time,
    min_logical_reads = s.min_logical_reads,
    max_logical_reads = s.max_logical_reads,
    min_physical_reads = s.min_physical_reads,
    max_physical_reads = s.max_physical_reads,
    min_logical_writes = s.min_logical_writes,
    max_logical_writes = s.max_logical_writes,
    ' AS nvarchar(max)) + @spills_cols + N'
    sql_handle = CONVERT(varchar(130), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(130), s.plan_handle, 1)
FROM sys.dm_exec_procedure_stats AS s
CROSS APPLY
(
    SELECT
        dbid = CONVERT(integer, pa.value)
    FROM sys.dm_exec_plan_attributes(s.plan_handle) AS pa
    WHERE pa.attribute = N''dbid''
) AS pa
INNER JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE d.state = 0
AND   pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))
AND   s.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
/*EXCLUSION_FILTER*/

UNION ALL

SELECT
    database_name = d.name,
    schema_name = ISNULL(OBJECT_SCHEMA_NAME(s.object_id, s.database_id), N''dbo''),
    object_name = COALESCE(
        OBJECT_NAME(s.object_id, s.database_id),
        CASE
            WHEN CHARINDEX(N''CREATE TRIGGER'', st.text) > 0
            THEN LTRIM(RTRIM(REPLACE(REPLACE(
                SUBSTRING(
                    st.text,
                    CHARINDEX(N''CREATE TRIGGER'', st.text) + 15,
                    CHARINDEX(N'' ON '', st.text + N'' ON '', CHARINDEX(N''CREATE TRIGGER'', st.text) + 15) - CHARINDEX(N''CREATE TRIGGER'', st.text) - 15
                ), N''['', N''''), N'']'', N'''')))
            ELSE N''trigger_'' + CONVERT(nvarchar(20), s.object_id)
        END
    ),
    object_type = N''TRIGGER'',
    cached_time = s.cached_time,
    last_execution_time = s.last_execution_time,
    execution_count = s.execution_count,
    total_worker_time = s.total_worker_time,
    total_elapsed_time = s.total_elapsed_time,
    total_logical_reads = s.total_logical_reads,
    total_physical_reads = s.total_physical_reads,
    total_logical_writes = s.total_logical_writes,
    min_worker_time = s.min_worker_time,
    max_worker_time = s.max_worker_time,
    min_elapsed_time = s.min_elapsed_time,
    max_elapsed_time = s.max_elapsed_time,
    min_logical_reads = s.min_logical_reads,
    max_logical_reads = s.max_logical_reads,
    min_physical_reads = s.min_physical_reads,
    max_physical_reads = s.max_physical_reads,
    min_logical_writes = s.min_logical_writes,
    max_logical_writes = s.max_logical_writes,
    ' + @spills_cols + CAST(N'
    sql_handle = CONVERT(varchar(130), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(130), s.plan_handle, 1)
FROM sys.dm_exec_trigger_stats AS s
CROSS APPLY sys.dm_exec_sql_text(s.sql_handle) AS st
CROSS APPLY
(
    SELECT
        dbid = CONVERT(integer, pa.value)
    FROM sys.dm_exec_plan_attributes(s.plan_handle) AS pa
    WHERE pa.attribute = N''dbid''
) AS pa
INNER JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE d.state = 0
AND   pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))
AND   s.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
/*EXCLUSION_FILTER*/

UNION ALL

SELECT
    database_name = d.name,
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N''FUNCTION'',
    cached_time = s.cached_time,
    last_execution_time = s.last_execution_time,
    execution_count = s.execution_count,
    total_worker_time = s.total_worker_time,
    total_elapsed_time = s.total_elapsed_time,
    total_logical_reads = s.total_logical_reads,
    total_physical_reads = s.total_physical_reads,
    total_logical_writes = s.total_logical_writes,
    min_worker_time = s.min_worker_time,
    max_worker_time = s.max_worker_time,
    min_elapsed_time = s.min_elapsed_time,
    max_elapsed_time = s.max_elapsed_time,
    min_logical_reads = s.min_logical_reads,
    max_logical_reads = s.max_logical_reads,
    min_physical_reads = s.min_physical_reads,
    max_physical_reads = s.max_physical_reads,
    min_logical_writes = s.min_logical_writes,
    max_logical_writes = s.max_logical_writes,
    ' AS nvarchar(max)) + @fn_spills_cols + CAST(N'
    sql_handle = CONVERT(varchar(130), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(130), s.plan_handle, 1)
FROM sys.dm_exec_function_stats AS s
CROSS APPLY
(
    SELECT
        dbid = CONVERT(integer, pa.value)
    FROM sys.dm_exec_plan_attributes(s.plan_handle) AS pa
    WHERE pa.attribute = N''dbid''
) AS pa
INNER JOIN sys.databases AS d
  ON pa.dbid = d.database_id
WHERE d.state = 0
AND   pa.dbid NOT IN (1, 3, 4, 32761, 32767, ISNULL(DB_ID(N''PerformanceMonitor''), 0))
AND   s.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
/*EXCLUSION_FILTER*/
) AS combined
ORDER BY total_elapsed_time DESC
) AS ranked/*PLAN_APPLY*/
ORDER BY ranked.total_elapsed_time DESC
OPTION(RECOMPILE);' AS nvarchar(max));

EXECUTE sys.sp_executesql @sql;";

    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */ TOP (150)
    ranked.*/*PLAN_SELECT*/
FROM
(
SELECT TOP (150)
    database_name = DB_NAME(),
    schema_name = OBJECT_SCHEMA_NAME(s.object_id, s.database_id),
    object_name = OBJECT_NAME(s.object_id, s.database_id),
    object_type = N'PROCEDURE',
    cached_time = s.cached_time,
    last_execution_time = s.last_execution_time,
    execution_count = s.execution_count,
    total_worker_time = s.total_worker_time,
    total_elapsed_time = s.total_elapsed_time,
    total_logical_reads = s.total_logical_reads,
    total_physical_reads = s.total_physical_reads,
    total_logical_writes = s.total_logical_writes,
    min_worker_time = s.min_worker_time,
    max_worker_time = s.max_worker_time,
    min_elapsed_time = s.min_elapsed_time,
    max_elapsed_time = s.max_elapsed_time,
    min_logical_reads = s.min_logical_reads,
    max_logical_reads = s.max_logical_reads,
    min_physical_reads = s.min_physical_reads,
    max_physical_reads = s.max_physical_reads,
    min_logical_writes = s.min_logical_writes,
    max_logical_writes = s.max_logical_writes,
    total_spills = ISNULL(s.total_spills, 0),
    min_spills = ISNULL(s.min_spills, 0),
    max_spills = ISNULL(s.max_spills, 0),
    sql_handle = CONVERT(varchar(130), s.sql_handle, 1),
    plan_handle = CONVERT(varchar(130), s.plan_handle, 1)
FROM sys.dm_exec_procedure_stats AS s
WHERE s.database_id = DB_ID()
AND   s.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
/*EXCLUSION_FILTER*/
ORDER BY s.total_elapsed_time DESC
) AS ranked/*PLAN_APPLY*/
ORDER BY ranked.total_elapsed_time DESC
OPTION(RECOMPILE);";

    /* Execution-plan capture — spliced into every branch (procedure/trigger/function) and the Azure
       variant only when the host sets CapturePlanXml (Darling); when off the placeholders erase to
       nothing, so Lite's SQL is byte-identical to the no-plan form. Mirrors the full Dashboard's
       @collect_plan path in install/10_collect_procedure_stats.sql. Unlike query_stats, the three
       module-level DMVs (sys.dm_exec_procedure_stats / _trigger_stats / _function_stats) expose NO
       statement_start_offset/statement_end_offset — they aggregate at the whole-object grain — so we
       pass literal 0, -1 (documented "beginning-of-batch through end-of-batch") to get the entire
       module's plan. Still the TEXT DMV (not sys.dm_exec_query_plan) so large/deep plans that overflow
       the xml type still return. dm_exec_text_query_plan(plan_handle, 0, -1) returns a single row, so
       the OUTER APPLY never multiplies rows and never drops one (NULL plan for an aged-out handle). */
    /* #1959: the apply used to sit INSIDE each branch, below the TOP - so a burst window rendered a
       module-grain plan (the big ones) for every candidate the TOP then discarded, which is the same
       defect the field investigation measured at 81% of query_stats' runtime. The render now happens
       ONCE, outside the ranked derived table, against at most 150 survivors: free when candidates fit
       under the TOP (the daytime case the field measured as noise) and bounded when they do not (the
       25-second overnight tails). The handle round-trips through CONVERT(varbinary(64), ..., 1) from
       the varchar(130) the payload already carries, so no extra column threads through the branches
       and the stored shape is untouched. Placement inside the shell keeps the fragment identical for
       the standard (dynamic SQL) and Azure variants. */
    private const string PlanSelectFragment = @",
    query_plan_xml = tqp.query_plan";

    private const string PlanApplyFragment = @"
OUTER APPLY sys.dm_exec_text_query_plan(CONVERT(varbinary(64), ranked.plan_handle, 1), 0, -1) AS tqp";

    public override string Name => "procedure_stats";

    public override string TargetTable => "procedure_stats";

    /// <summary>
    /// #1833: on Azure SQL Database this collector must run per database, like query_stats and the
    /// other database-scoped collectors already do. Without the override it ran once on the server
    /// entry's own connection — whose catalog defaults to master when the Database field is blank —
    /// and the Azure variant's <c>WHERE s.database_id = DB_ID()</c> then filtered to master's
    /// procedures: zero user rows, logged SUCCESS, and an empty Top Procedures grid that looked
    /// healthy. The per-database connection makes <c>DB_ID()</c> each user database in turn, which
    /// is exactly what that predicate was written for.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /// <summary>
    /// #2673: bound the per-server wall-clock (execute + DRAIN), so a large procedure cache can't make this
    /// collector run minutes on a monitored server — the profile that makes us stick out in that server's
    /// own monitoring. The 60s per-command timeout covers only execution, not the drain. Measured tail
    /// before the bound: 176s on one prod server (avg 6s). On on-prem/RDS the whole collection is one
    /// server-scoped item; on Azure SQL DB it applies per database. A cycle that blows the budget ships
    /// nothing and retries next — eventual consistency, the same tolerance the query_store fetch relies on.
    /// </summary>
    public override TimeSpan? PerItemWallClockBudget => TimeSpan.FromSeconds(120);

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var planSelect = context.CapturePlanXml ? PlanSelectFragment : "";
        var planApply = context.CapturePlanXml ? PlanApplyFragment : "";

        if (context.Target.IsAzureSqlDb)
        {
            /* Azure: single-database scope; the exclusion token is left in place unreplaced in the
               original (no clause is spliced on Azure) — reproduce exactly. Plan placeholders still
               erase (flag off) or splice (Darling) here just like the standard variant. */
            return new CollectorQuery(
                AzureSqlDbQueryText
                    .Replace("/*PLAN_SELECT*/", planSelect, StringComparison.Ordinal)
                    .Replace("/*PLAN_APPLY*/", planApply, StringComparison.Ordinal));
        }

        /* Standard query is dynamic SQL (built into @sql then passed to sp_executesql), so the
           exclusion filter is interpolated as literal N'...' values rather than parameter bindings —
           doubled escaping because @sql is itself a single-quoted T-SQL string. The plan fragments
           carry no single quotes, so they splice straight into the nested dynamic SQL body. */
        var exclusionClause = DatabaseExclusionFilter.BuildLiteralClause(
            context.ExcludedDatabases, "d.name", forNestedDynamicSql: true);

        return new CollectorQuery(
            StandardQueryText
                .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal)
                .Replace("/*PLAN_SELECT*/", planSelect, StringComparison.Ordinal)
                .Replace("/*PLAN_APPLY*/", planApply, StringComparison.Ordinal));
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("object_name", CollectorColumnType.Varchar),
        new CollectorColumn("object_type", CollectorColumnType.Varchar),
        new CollectorColumn("cached_time", CollectorColumnType.Timestamp),
        new CollectorColumn("last_execution_time", CollectorColumnType.Timestamp),
        new CollectorColumn("execution_count", CollectorColumnType.BigInt),
        new CollectorColumn("total_worker_time", CollectorColumnType.BigInt),
        new CollectorColumn("total_elapsed_time", CollectorColumnType.BigInt),
        new CollectorColumn("total_logical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("total_physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("total_logical_writes", CollectorColumnType.BigInt),
        new CollectorColumn("min_worker_time", CollectorColumnType.BigInt),
        new CollectorColumn("max_worker_time", CollectorColumnType.BigInt),
        new CollectorColumn("min_elapsed_time", CollectorColumnType.BigInt),
        new CollectorColumn("max_elapsed_time", CollectorColumnType.BigInt),
        new CollectorColumn("min_logical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("max_logical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("min_physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("max_physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("min_logical_writes", CollectorColumnType.BigInt),
        new CollectorColumn("max_logical_writes", CollectorColumnType.BigInt),
        new CollectorColumn("total_spills", CollectorColumnType.BigInt),
        new CollectorColumn("min_spills", CollectorColumnType.BigInt),
        new CollectorColumn("max_spills", CollectorColumnType.BigInt),
        new CollectorColumn("sql_handle", CollectorColumnType.Varchar),
        new CollectorColumn("plan_handle", CollectorColumnType.Varchar),
        new CollectorColumn("delta_execution_count", CollectorColumnType.BigInt),
        new CollectorColumn("delta_worker_time", CollectorColumnType.BigInt),
        new CollectorColumn("delta_elapsed_time", CollectorColumnType.BigInt),
        new CollectorColumn("delta_logical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("delta_logical_writes", CollectorColumnType.BigInt),
        new CollectorColumn("delta_physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("delta_spills", CollectorColumnType.BigInt),
        /* Trailing plan column (appended, not grouped with the raw handles) so a store already at the
           pre-plan shape gets it via one ADD COLUMN — fresh (V1) and upgraded (V6) Darling stores keep
           an identical physical order. NULL on Lite (flag off) and for any procedure whose plan aged out. */
        new CollectorColumn("query_plan_xml", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.GetInt64(8),
                reader.GetInt64(9),
                reader.GetInt64(10),
                reader.GetInt64(11),
                reader.IsDBNull(12) ? 0L : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0L : reader.GetInt64(13),
                reader.IsDBNull(14) ? 0L : reader.GetInt64(14),
                reader.IsDBNull(15) ? 0L : reader.GetInt64(15),
                reader.IsDBNull(16) ? 0L : reader.GetInt64(16),
                reader.IsDBNull(17) ? 0L : reader.GetInt64(17),
                reader.IsDBNull(18) ? 0L : reader.GetInt64(18),
                reader.IsDBNull(19) ? 0L : reader.GetInt64(19),
                reader.IsDBNull(20) ? 0L : reader.GetInt64(20),
                reader.IsDBNull(21) ? 0L : reader.GetInt64(21),
                reader.GetInt64(22),
                reader.GetInt64(23),
                reader.GetInt64(24),
                reader.IsDBNull(25) ? null : reader.GetString(25),
                reader.IsDBNull(26) ? null : reader.GetString(26),
                /* query_plan_xml is the trailing column present only when CapturePlanXml spliced it
                   into every branch's SELECT (ordinal 27); the short-circuit skips it entirely when off. */
                context.CapturePlanXml && !reader.IsDBNull(27) ? reader.GetString(27) : null));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Delta key: plan_handle to prevent cross-contamination when multiple plans exist for the
           same object; the db.schema.object fallback and the seven group names are the parity contract. */
        var deltaKey = row.PlanHandle ?? $"{row.DatabaseName}.{row.SchemaName}.{row.ObjectName}";
        var deltaExec = context.Deltas.CalculateDelta(context.ServerId, "proc_stats_exec", deltaKey, row.ExecutionCount, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWorker = context.Deltas.CalculateDelta(context.ServerId, "proc_stats_worker", deltaKey, row.TotalWorkerTime, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaElapsed = context.Deltas.CalculateDelta(context.ServerId, "proc_stats_elapsed", deltaKey, row.TotalElapsedTime, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaReads = context.Deltas.CalculateDelta(context.ServerId, "proc_stats_reads", deltaKey, row.TotalLogicalReads, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWrites = context.Deltas.CalculateDelta(context.ServerId, "proc_stats_writes", deltaKey, row.TotalLogicalWrites, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaPhysReads = context.Deltas.CalculateDelta(context.ServerId, "proc_stats_phys_reads", deltaKey, row.TotalPhysicalReads, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaSpills = context.Deltas.CalculateDelta(context.ServerId, "proc_stats_spills", deltaKey, row.TotalSpills, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        writer
            .Value(row.DatabaseName)
            .Value(row.SchemaName)
            .Value(row.ObjectName)
            .Value(row.ObjectType)
            .Value(row.CachedTime)
            .Value(row.LastExecutionTime)
            .Value(row.ExecutionCount)
            .Value(row.TotalWorkerTime)
            .Value(row.TotalElapsedTime)
            .Value(row.TotalLogicalReads)
            .Value(row.TotalPhysicalReads)
            .Value(row.TotalLogicalWrites)
            .Value(row.MinWorkerTime)
            .Value(row.MaxWorkerTime)
            .Value(row.MinElapsedTime)
            .Value(row.MaxElapsedTime)
            .Value(row.MinLogicalReads)
            .Value(row.MaxLogicalReads)
            .Value(row.MinPhysicalReads)
            .Value(row.MaxPhysicalReads)
            .Value(row.MinLogicalWrites)
            .Value(row.MaxLogicalWrites)
            .Value(row.TotalSpills)
            .Value(row.MinSpills)
            .Value(row.MaxSpills)
            .Value(row.SqlHandle)
            .Value(row.PlanHandle)
            .Value(deltaExec)
            .Value(deltaWorker)
            .Value(deltaElapsed)
            .Value(deltaReads)
            .Value(deltaWrites)
            .Value(deltaPhysReads)
            .Value(deltaSpills)
            .Value(row.QueryPlanXml);          /* null unless CapturePlanXml captured it (Darling) */
    }
}
