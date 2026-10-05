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
using System.Globalization;
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
        string? QueryPlanXml,

        /* #3392: DATALENGTH of this module's plan XML as measured on the monitored server, or null when the
           host captures no plans or the handle aged out before the plan apply ran. Carries the SIZE whether
           or not QueryPlanXml carries the CONTENT, which is what tells a plan omitted for size apart from a
           plan that was never there. */
        long? QueryPlanXmlBytes)
    {
        /// <summary>
        /// #5158: the digest the host's store already holds for this module's plan, set by a host that defers
        /// the plan fetch and recognizes the plan from an earlier commit. When set, the writer stores the
        /// digest instead of the content (<see cref="ICollectorRowWriter.PayloadOrDigest"/>). Null by
        /// default, so a row writes its <see cref="QueryPlanXml"/> as it always has. Init-only rather than
        /// positional so existing <c>new Row(...)</c> call sites are untouched. A known identity whose plan is
        /// over the capture cap has no digest; the host sets <see cref="QueryPlanXmlBytes"/> from its cache for
        /// it, as <c>query_stats</c> does, so the size column and the oversized backlog both see it. The host
        /// must clear this digest when it measures a new size for the row.
        /// </summary>
        public string? KnownPlanDigest { get; init; }

        /// <summary>#5158 identity fingerprint, read only when the plan fetch is deferred: how many statements
        /// <c>sys.dm_exec_query_stats</c> holds for this module's plan handle. Zero (with a null max and sum)
        /// means no statements are visible for the handle: it aged out or none has run. The wiring must not
        /// cache an identity keyed on a count of zero.</summary>
        public long? PlanStatementCount { get; init; }

        /// <summary>#5158 identity fingerprint: the latest statement <c>creation_time</c> for the plan handle.</summary>
        public DateTime? PlanLastStatementCompile { get; init; }

        /// <summary>#5158 identity fingerprint: the sum of <c>plan_generation_num</c> over the plan handle's statements.</summary>
        public long? PlanGenerationSum { get; init; }
    }

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
    /* The DATALENGTH guard bounds the SIZE of one module-grain plan, on top of #1959's bound on how
       MANY rows render one — see QueryPlanXmlCaptureLimits. A whole-procedure plan is exactly the
       shape most likely to cross it: this DMV aggregates at the module grain, so one heavy stored
       proc's plan can dwarf a single statement's. tqp.query_plan is read twice (DATALENGTH, then the
       value) — one materialized OUTER APPLY column read twice, not a second TVF invocation.

       query_plan_xml_bytes selects that same DATALENGTH a THIRD time, free for the same reason. It is never
       gated by the cap — a row over the cap reports its size and a NULL plan, the only pairing that
       distinguishes "omitted for size" from "the handle aged out". #3392's backlog is keyed on that
       distinction. Both plan columns sit INSIDE this fragment, so a host with CapturePlanXml off (Lite)
       emits neither and its SQL stays byte-identical to the no-plan form. */
    private static readonly string PlanSelectFragment = @",
    query_plan_xml = CASE WHEN DATALENGTH(tqp.query_plan) > " + QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes + @" THEN NULL ELSE tqp.query_plan END,
    query_plan_xml_bytes = DATALENGTH(tqp.query_plan)";

    /// <summary>
    /// The plan fetch's start offset for this collector: the documented "beginning of batch" literal. The
    /// three module-level DMVs aggregate at the whole-object grain and expose no
    /// <c>statement_start_offset</c>, so there is nothing per-row to pass.
    ///
    /// <para>Named rather than inlined because #3392's deferred fetch has to pass the SAME pair to get the
    /// SAME document back, and two copies of a literal are how those quietly stop agreeing.</para>
    /// </summary>
    public const int ModuleStatementStartOffset = 0;

    /// <summary>The plan fetch's end offset: the documented "end of batch" literal. See
    /// <see cref="ModuleStatementStartOffset"/> for why it is named.</summary>
    public const int ModuleStatementEndOffset = -1;

    private static readonly string PlanApplyFragment = @"
OUTER APPLY sys.dm_exec_text_query_plan(CONVERT(varbinary(64), ranked.plan_handle, 1), "
        + ModuleStatementStartOffset + ", " + ModuleStatementEndOffset + @") AS tqp";

    /// <summary>
    /// #5158: the identity columns a deferred-fetch host reads INSTEAD of the plan columns, at ordinals 27-29
    /// (not payload columns). A module's plan XML changes in place when one statement recompiles, while
    /// <c>plan_handle</c> and <c>cached_time</c> stay put (measured: adding cached_time to the key changed the
    /// distinct count by zero), so the handle alone cannot say whether a cached digest is still current.
    /// These three aggregates over the module's statements move when any statement recompiles or first compiles.
    /// </summary>
    private const string PlanIdentitySelectFragment = @",
    plan_statement_count = pfp.plan_statement_count,
    plan_last_statement_compile = pfp.plan_last_statement_compile,
    plan_generation_sum = pfp.plan_generation_sum";

    /* PROVISIONAL (#5158): a correlated OUTER APPLY over sys.dm_exec_query_stats, one per surviving row, outside
       the ranked derived table like the plan apply (at most 150). dm_exec_query_stats cannot seek on
       plan_handle, so what this costs on a large cache is UNMEASURED; a grouped single walk or
       dm_exec_cached_plans.size_in_bytes are the alternatives if it proves too slow. No single quotes, because
       the standard query nests this text inside dynamic SQL. */
    private const string PlanIdentityApplyFragment = @"
OUTER APPLY
(
    SELECT
        plan_statement_count = COUNT_BIG(*),
        plan_last_statement_compile = MAX(qs.creation_time),
        plan_generation_sum = SUM(CONVERT(bigint, qs.plan_generation_num))
    FROM sys.dm_exec_query_stats AS qs
    WHERE qs.plan_handle = CONVERT(varbinary(64), ranked.plan_handle, 1)
) AS pfp";

    /// <summary>
    /// True when the main query carries the plan columns itself: the host captures plans and has not deferred
    /// the fetch to <see cref="BuildPlanFetchQuery"/> (#5158). One definition for the query text and the read.
    /// </summary>
    private static bool InlinePlanCapture(CollectorContext context) =>
        context.CapturePlanXml && !context.DeferPlanXmlFetch;

    /// <summary>True when the host captures plans and defers the fetch, so the main query carries the identity columns.</summary>
    private static bool DeferredPlanIdentity(CollectorContext context) =>
        context.CapturePlanXml && context.DeferPlanXmlFetch;

    /// <summary>
    /// #5158: the second target query for a host that defers the plan fetch. Takes plan handles, not
    /// <see cref="QueryStatsCollector.PlanFetchKey"/>s, so the offsets are always
    /// (<see cref="ModuleStatementStartOffset"/>, <see cref="ModuleStatementEndOffset"/>): a caller cannot pass
    /// a statement grain this collector does not have. It is query_stats' fetch query for those keys, one SQL.
    /// Read the result with <see cref="QueryStatsCollector.ReadPlanFetchAsync"/>.
    /// </summary>
    public static CollectorQuery BuildPlanFetchQuery(CollectorContext context, IReadOnlyList<byte[]> planHandles)
    {
        ArgumentNullException.ThrowIfNull(planHandles);

        var keys = new List<QueryStatsCollector.PlanFetchKey>(planHandles.Count);
        foreach (var handle in planHandles)
        {
            keys.Add(new QueryStatsCollector.PlanFetchKey(handle, ModuleStatementStartOffset, ModuleStatementEndOffset));
        }

        return QueryStatsCollector.BuildPlanFetchQuery(context, keys);
    }

    /// <summary>
    /// Parses the shipped <c>plan_handle</c> text (<c>0x</c> and 2 to 128 hex digits) to 1 to 64 bytes. False
    /// for null, empty, a bare <c>0x</c>, an odd digit count, more than 64 bytes or a non-hex character, so
    /// such a row is never fetched and ships no plan, as an aged-out handle does.
    /// </summary>
    public static bool TryParsePlanHandle(string? hex, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        if (hex is null || hex.Length < 4 || hex.Length > 130 || (hex.Length & 1) != 0
            || hex[0] != '0' || (hex[1] != 'x' && hex[1] != 'X'))
        {
            return false;
        }

        try
        {
            bytes = Convert.FromHexString(hex.AsSpan(2));
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

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
        var planSelect = InlinePlanCapture(context)
            ? PlanSelectFragment
            : DeferredPlanIdentity(context) ? PlanIdentitySelectFragment : "";
        var planApply = InlinePlanCapture(context)
            ? PlanApplyFragment
            : DeferredPlanIdentity(context) ? PlanIdentityApplyFragment : "";

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
        /* #3392, appended after it for the same reason. BigInt because DATALENGTH over an nvarchar(max)
           expression returns bigint, and a module-grain plan is measured in megabytes on the tail this
           exists to describe. */
        new CollectorColumn("query_plan_xml_bytes", CollectorColumnType.BigInt),
        /* Appended (Darling V128 / Lite v61, #3540): the measured seconds the row's seven deltas accrued
           over, or 0 when no delta was knowable. Appended at the END because both stores' writers are
           positional — the same rule GoldenCollectorSchema's header states for every column a numbered
           migration adds by ALTER TABLE. The same integer perfmon_stats and query_stats have carried from
           the start and the four V127 families gained, so one NULLIF idiom reads all ten. */
        new CollectorColumn("sample_interval_seconds", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        var inlinePlan = InlinePlanCapture(context);
        var deferredIdentity = DeferredPlanIdentity(context);

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
                inlinePlan && !reader.IsDBNull(27) ? reader.GetString(27) : null,
                /* #3392: the plan's measured size rides the same splice at ordinal 28, so the same
                   short-circuit covers it. Convert rather than GetInt64: DATALENGTH's return type widens to
                   bigint only for the max types, and a provider that hands back an Int32 here would throw on
                   a strict accessor. */
                inlinePlan && !reader.IsDBNull(28)
                    ? Convert.ToInt64(reader.GetValue(28), CultureInfo.InvariantCulture)
                    : null)
            {
                /* #5158: a deferred-fetch host reads the identity fingerprint at 27-29 in place of the plan. */
                PlanStatementCount = deferredIdentity && !reader.IsDBNull(27)
                    ? Convert.ToInt64(reader.GetValue(27), CultureInfo.InvariantCulture) : null,
                PlanLastStatementCompile = deferredIdentity && !reader.IsDBNull(28) ? reader.GetDateTime(28) : null,
                PlanGenerationSum = deferredIdentity && !reader.IsDBNull(29)
                    ? Convert.ToInt64(reader.GetValue(29), CultureInfo.InvariantCulture) : null,
            });
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Delta key: plan_handle to prevent cross-contamination when multiple plans exist for the
           same object; the db.schema.object fallback and the seven group names are the parity contract.

           The interval is stored beside the deltas (#3540, Darling V128 / Lite v61). The calculator
           reports (delta 0, interval 0) when no delta is knowable — first sighting, counter reset, a gap
           past the policy — and (0, n) when the interval was genuinely idle, and that pairing is the ONLY
           way a reader can tell the two apart. This collector took the bare long and discarded the
           interval at the write, so a restart's fabricated zero survived as a measured one and the
           procedure duration trend LAG-divided it into a confident 0.00 ms/sec. This family also has the
           highest first-sighting rate of the ten: a TOP (150) that churns readmits plans that fell out,
           and every readmission is a first sighting whose 0 used to read as "ran zero times".

           One interval per ROW, the minimum over the row's seven groups — the V127 rule
           (WaitStatsCollector). The groups share a key and a collection time, so first-sighting,
           gap-policy and seeding decisions are identical across them and the intervals agree in every
           case but an independent single-counter reset, which the module DMVs never do (a plan eviction
           resets all seven together). Taking the minimum makes the stored pair mean "every delta in this
           row is knowable", so a reader never divides a reset counter's 0 by a sibling's real interval
           and reads it as idle. */
        var deltaKey = row.PlanHandle ?? $"{row.DatabaseName}.{row.SchemaName}.{row.ObjectName}";
        var deltaExec = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "proc_stats_exec", deltaKey, row.ExecutionCount, out var execInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWorker = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "proc_stats_worker", deltaKey, row.TotalWorkerTime, out var workerInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaElapsed = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "proc_stats_elapsed", deltaKey, row.TotalElapsedTime, out var elapsedInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaReads = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "proc_stats_reads", deltaKey, row.TotalLogicalReads, out var readsInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaWrites = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "proc_stats_writes", deltaKey, row.TotalLogicalWrites, out var writesInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaPhysReads = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "proc_stats_phys_reads", deltaKey, row.TotalPhysicalReads, out var physReadsInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaSpills = context.Deltas.CalculateDeltaWithInterval(context.ServerId, "proc_stats_spills", deltaKey, row.TotalSpills, out var spillsInterval, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var sampleIntervalSeconds = Math.Min(execInterval, Math.Min(workerInterval, Math.Min(elapsedInterval, Math.Min(readsInterval, Math.Min(writesInterval, Math.Min(physReadsInterval, spillsInterval))))));

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
            .PayloadOrDigest(row.QueryPlanXml, row.KnownPlanDigest) /* null unless CapturePlanXml captured it (Darling); #5158: or the digest of a plan the store holds */
            .Value(row.QueryPlanXmlBytes) /* #3392: measured size, never gated by the cap; a host that defers the fetch sets it from its cache for a known identity */
            .Value(sampleIntervalSeconds);     /* sample_interval_seconds INTEGER — measured, 0 = unknowable */
    }

    /// <summary>
    /// #3392: this row is a backlog candidate when its plan measured over the cap and both cache handles are
    /// present. The offsets handed back are <see cref="ModuleStatementStartOffset"/> /
    /// <see cref="ModuleStatementEndOffset"/> — the same pair <see cref="PlanApplyFragment"/> passes — so the
    /// deferred fetch re-issues the identical module-grain call rather than a plausible-looking variant of it.
    /// </summary>
    public override OversizedPlanObservation? DescribeOversizedPlan(Row row)
    {
        if (!QueryPlanXmlCaptureLimits.ExceedsCaptureCap(row.QueryPlanXmlBytes)
            || row.PlanHandle is null
            || row.SqlHandle is null)
        {
            return null;
        }

        /* No query_hash: sys.dm_exec_procedure_stats has none, and this collector's readers key on the
           sql_handle and on the object name, both of which are recoverable without one. */
        return new OversizedPlanObservation(
            row.PlanHandle,
            row.SqlHandle,
            ModuleStatementStartOffset,
            ModuleStatementEndOffset,
            row.DatabaseName,
            null,
            row.QueryPlanXmlBytes!.Value);
    }
}
