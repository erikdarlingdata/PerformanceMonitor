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
/// Query statistics from sys.dm_exec_query_stats. Extracted verbatim from Lite's
/// RemoteCollectorService.QueryStats.cs — the standard path joins plan_attributes for the
/// database (with the exclusion splice); Azure SQL DB skips plan_attributes (it reports dbid=1
/// for all plans) and runs per database (RunsPerDatabase). Deltas are keyed on the FULL
/// dm_exec_query_stats row identity (sql_handle:start:end:plan_handle) — keying on plan_handle
/// alone cross-contaminated multi-statement plans (one plan_handle, many statements at different
/// offsets), yielding garbage resource deltas with zero execution delta. The worker-time delta
/// also captures the collection interval (CalculateDeltaWithInterval) so the display can derive
/// peak CPU-ms per wall-clock second.
/// </summary>
public sealed class QueryStatsCollector : CollectorDefinitionBase<QueryStatsCollector.Row>
{
    public static QueryStatsCollector Instance { get; } = new();

    private QueryStatsCollector()
    {
    }

    public sealed class Row
    {
        public string? DatabaseName { get; set; }
        public string QueryHash { get; set; } = "";
        public string? QueryPlanHash { get; set; }
        public DateTime? CreationTime { get; set; }
        public DateTime? LastExecutionTime { get; set; }
        public long ExecutionCount { get; set; }
        public long TotalWorkerTime { get; set; }
        public long TotalElapsedTime { get; set; }
        public long TotalLogicalReads { get; set; }
        public long TotalLogicalWrites { get; set; }
        public long TotalPhysicalReads { get; set; }
        public long TotalClrTime { get; set; }
        public long TotalRows { get; set; }
        public long TotalSpills { get; set; }
        public long MinWorkerTime { get; set; }
        public long MaxWorkerTime { get; set; }
        public long MinElapsedTime { get; set; }
        public long MaxElapsedTime { get; set; }
        public long MinPhysicalReads { get; set; }
        public long MaxPhysicalReads { get; set; }
        public long MinRows { get; set; }
        public long MaxRows { get; set; }
        public long MinDop { get; set; }
        public long MaxDop { get; set; }
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
        public long MinSpills { get; set; }
        public long MaxSpills { get; set; }
        public string? SqlHandle { get; set; }
        public string? PlanHandle { get; set; }
        public string? QueryText { get; set; }
        public string? QueryPlanXml { get; set; }

        /// <summary>
        /// <c>DATALENGTH</c> of this statement's plan XML as measured on the monitored server, or null when
        /// the host captures no plans or the handle aged out before the plan apply ran (#3392). Carries the
        /// SIZE whether or not <see cref="QueryPlanXml"/> carries the CONTENT, which is what lets a reader
        /// tell a plan omitted for size from a plan that was never there.
        /// </summary>
        public long? QueryPlanXmlBytes { get; set; }

        /// <summary>
        /// #5158: the digest the host's store already holds for this row's plan, set by a host that defers
        /// the plan fetch and recognizes the plan from an earlier commit. When set, the writer stores the
        /// digest instead of the content (<see cref="ICollectorRowWriter.PayloadOrDigest"/>). Null by
        /// default, so a row writes its <see cref="QueryPlanXml"/> as it always has.
        /// </summary>
        public string? KnownPlanDigest { get; set; }

        public long PlanGenerationNum { get; set; }

        /// <summary>
        /// <c>sys.dm_exec_query_stats.statement_start_offset</c> / <c>statement_end_offset</c>: the statement's
        /// position inside its batch text, in BYTES of the <c>nvarchar</c> text, so a character slice divides
        /// by two (the <c>SUBSTRING(st.text, (start / 2) + 1, …)</c> in the SELECT above). <c>-1</c> as the end
        /// offset means "to the end of the batch"; <c>(0, -1)</c> is the whole batch. Half of the delta key
        /// (<c>sql_handle:start:end:plan_handle</c>) — a multi-statement batch shares one sql_handle and one
        /// plan_handle across its statements, and only the offsets tell them apart. Stored since Darling
        /// V128 / Lite v61 (#3540) exactly as read, <c>-1</c> included, so the restart seed can rebuild the
        /// key from the store.
        /// </summary>
        public int StatementStartOffset { get; set; }

        /// <summary>See <see cref="StatementStartOffset"/>.</summary>
        public int StatementEndOffset { get; set; }

        /// <summary>The statement's host object (schema.name) from <c>sys.dm_exec_sql_text.objectid</c>;
        /// NULL for ad-hoc/prepared text (#2012 stage 2 — splits INSERT...EXEC callers sharing a hash).</summary>
        public string? HostObjectName { get; set; }

        /// <summary>
        /// #2235: seconds since this plan was compiled, as measured on the monitored server. Feeds the
        /// delta calculator's series-age rule and is NOT stored — a recompile presents a new
        /// <c>plan_handle</c> and therefore a new delta key, and without this the first sighting of that
        /// key reports 0, which on a churning instance is most of the server's CPU.
        /// </summary>
        public int? CompileAgeSeconds { get; set; }
    }

    private const string SelectColumnsText = @"
    query_hash = CONVERT(varchar(64), qs.query_hash, 1),
    query_plan_hash = CONVERT(varchar(64), qs.query_plan_hash, 1),
    creation_time = qs.creation_time,
    last_execution_time = qs.last_execution_time,
    execution_count = qs.execution_count,
    total_worker_time = qs.total_worker_time,
    total_elapsed_time = qs.total_elapsed_time,
    total_logical_reads = qs.total_logical_reads,
    total_logical_writes = qs.total_logical_writes,
    total_physical_reads = qs.total_physical_reads,
    total_clr_time = qs.total_clr_time,
    total_rows = qs.total_rows,
    total_spills = qs.total_spills,
    min_worker_time = qs.min_worker_time,
    max_worker_time = qs.max_worker_time,
    min_elapsed_time = qs.min_elapsed_time,
    max_elapsed_time = qs.max_elapsed_time,
    min_physical_reads = qs.min_physical_reads,
    max_physical_reads = qs.max_physical_reads,
    min_rows = qs.min_rows,
    max_rows = qs.max_rows,
    min_dop = qs.min_dop,
    max_dop = qs.max_dop,
    min_grant_kb = qs.min_grant_kb,
    max_grant_kb = qs.max_grant_kb,
    min_used_grant_kb = qs.min_used_grant_kb,
    max_used_grant_kb = qs.max_used_grant_kb,
    min_ideal_grant_kb = qs.min_ideal_grant_kb,
    max_ideal_grant_kb = qs.max_ideal_grant_kb,
    min_reserved_threads = qs.min_reserved_threads,
    max_reserved_threads = qs.max_reserved_threads,
    min_used_threads = qs.min_used_threads,
    max_used_threads = qs.max_used_threads,
    min_spills = qs.min_spills,
    max_spills = qs.max_spills,
    sql_handle = CONVERT(varchar(130), qs.sql_handle, 1),
    plan_handle = CONVERT(varchar(130), qs.plan_handle, 1),
    query_text =
        CASE
            WHEN qs.statement_start_offset = 0
            AND  qs.statement_end_offset = -1
            THEN st.text
            ELSE
                SUBSTRING
                (
                    st.text,
                    (qs.statement_start_offset / 2) + 1,
                    (
                        CASE
                            WHEN qs.statement_end_offset = -1
                            THEN DATALENGTH(st.text)
                            ELSE qs.statement_end_offset
                        END - qs.statement_start_offset
                    ) / 2 + 1
                )
        END,
    plan_generation_num = qs.plan_generation_num,
    statement_start_offset = qs.statement_start_offset,
    statement_end_offset = qs.statement_end_offset,
    host_object_name =
        CASE
            WHEN st.objectid IS NOT NULL
            THEN ISNULL
                 (
                     OBJECT_SCHEMA_NAME(st.objectid, st.dbid) + N'.' + OBJECT_NAME(st.objectid, st.dbid),
                     N'Unknown'
                 )
        END,
    /* #2235: how long ago this plan was compiled, so the delta calculator can tell a key that is new
       TO US from a counter that is new to the WORLD. Sent as an AGE rather than creation_time itself
       because creation_time is in the monitored server's local time while collection times are UTC —
       comparing them client-side is a timezone bug on every server that is not UTC. DATEDIFF is
       evaluated where both clocks are the same one. Not stored: PayloadColumns is unchanged, this
       exists only to inform the delta. */
    compile_age_seconds = DATEDIFF(SECOND, qs.creation_time, GETDATE())";

    /* #1959: rank on the CHEAP DMV columns inside the derived table FIRST, and run the text apply, the
       NOT LIKE self-filter, and the (Darling-only) plan-XML render against the survivors ONLY. The optimizer
       does not defer the applies past the TOP on its own - a field plan showed dm_exec_text_query_plan
       executing 2,434 times to keep 200 rows, 81% of an entire sweep, with 30-second timeout MISSES on
       big-cache boxes - so the deferral is made structural here rather than hoped for. The derived table is
       deliberately aliased qs so SelectColumnsText and the plan fragments splice unchanged.

       The inner TOP is 300, not 200: the self-filter runs post-ranking now, so the inner set needs headroom
       for collector-self rows that ranking admits and the filter then removes. The expensive applies still
       run at most ~200-300 times either way; measured 6.0x on the reporting fleet's apex box (median
       7,736 ms -> 1,286 ms), 99.2% row-identity parity with the residual explained by cache churn. */
    private const string StandardQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */ TOP (200)
    database_name = qs.database_name," + SelectColumnsText + @"/*PLAN_SELECT*/
FROM
(
    SELECT TOP (300)
        database_name = d.name,
        qs.*
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY
    (
        SELECT
            dbid = CONVERT(integer, pa.value)
        FROM sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
        WHERE pa.attribute = N'dbid'
    ) AS pa
    INNER JOIN sys.databases AS d
      ON pa.dbid = d.database_id
    WHERE pa.dbid NOT IN (1, 2, 3, 4, 32761, 32767, ISNULL(DB_ID(N'PerformanceMonitor'), 0))
    AND   qs.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
    /*EXCLUSION_FILTER*/
    ORDER BY
        qs.total_elapsed_time DESC
) AS qs
OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st/*PLAN_APPLY*/
WHERE st.text NOT LIKE N'%PerformanceMonitorLite%'
ORDER BY
    qs.total_elapsed_time DESC
OPTION(RECOMPILE);";

    /* #1959: same rank-first shape as StandardQueryText, minus the dbid apply (Azure SQL DB scopes the
       DMV to the connected database). See the standard variant's comment for the mechanism and numbers. */
    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */ TOP (200)
    database_name = DB_NAME()," + SelectColumnsText + @"/*PLAN_SELECT*/
FROM
(
    SELECT TOP (300)
        qs.*
    FROM sys.dm_exec_query_stats AS qs
    WHERE qs.last_execution_time >= DATEADD(MINUTE, -10, GETDATE())
    ORDER BY
        qs.total_elapsed_time DESC
) AS qs
OUTER APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st/*PLAN_APPLY*/
WHERE st.text NOT LIKE N'%PerformanceMonitorLite%'
ORDER BY
    qs.total_elapsed_time DESC
OPTION(RECOMPILE);";

    /* Execution-plan capture — spliced into both variants only when the host sets CapturePlanXml
       (Darling); when off the placeholders erase to nothing, so Lite's SQL is byte-identical to the
       no-plan form. Mirrors the full Dashboard's @collect_plan path in
       install/08_collect_query_stats.sql: the STATEMENT-level plan from sys.dm_exec_text_query_plan
       keyed on the same plan_handle + statement offsets (the text DMV, not dm_exec_query_plan, so
       large/deep plans that overflow the xml type still return).

       The DATALENGTH guard is the size bound this comment used to say did not exist: see
       QueryPlanXmlCaptureLimits for why 200 rows still let one oversized plan dominate the cycle's
       drain time, and why the answer is a per-row cap rather than the running-budget shape
       QueryStoreCollector uses. tqp.query_plan is read twice (DATALENGTH, then the value) — that is
       one materialized OUTER APPLY column read twice, not a second invocation of the TVF.

       query_plan_xml_bytes selects that same DATALENGTH a THIRD time, and it is free for the same reason the
       second read is: one materialized OUTER APPLY column, read again. It is never gated by the cap — a row
       over the cap reports its size and a NULL plan, which is the only pairing that distinguishes "omitted
       for size" from "the handle aged out". #3392's backlog is keyed on that distinction.

       Both plan columns sit INSIDE this fragment, so a host with CapturePlanXml off (Lite) emits neither and
       its SQL stays byte-identical to the no-plan form. */
    private static readonly string PlanSelectFragment = @",
    query_plan_xml = CASE WHEN DATALENGTH(tqp.query_plan) > " + QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes + @" THEN NULL ELSE tqp.query_plan END,
    query_plan_xml_bytes = DATALENGTH(tqp.query_plan)";

    private const string PlanApplyFragment = @"
OUTER APPLY
    sys.dm_exec_text_query_plan
    (
        qs.plan_handle,
        qs.statement_start_offset,
        qs.statement_end_offset
    ) AS tqp";

    /// <summary>
    /// One statement plan the host wants rendered by <see cref="BuildPlanFetchQuery"/>: the same
    /// <c>plan_handle</c> and statement offsets <see cref="PlanApplyFragment"/> hands to
    /// <c>sys.dm_exec_text_query_plan</c>.
    /// </summary>
    public readonly record struct PlanFetchKey(byte[] PlanHandle, int StartOffset, int EndOffset);

    /// <summary>
    /// The most keys one <see cref="BuildPlanFetchQuery"/> call accepts. The inline <c>VALUES</c> list has no
    /// engine limit as a derived table, so this is a bound on the statement a host can ask a monitored
    /// server to compile, not a limit of the engine: a host with more misses splits its call.
    /// </summary>
    public const int MaxPlanFetchKeys = 1000;

    /// <summary>
    /// #5158: the second target query for a host that sets <see cref="CollectorContext.DeferPlanXmlFetch"/>.
    /// The main query ships no plan, and this renders plan XML for only the keys the host does not yet
    /// have committed. Row <c>ord</c> is the key's position in <paramref name="keys"/>, so the host maps a
    /// result back to its row without reading the handle back.
    ///
    /// WHY A SECOND QUERY AND NOT A NOT EXISTS IN THE MAIN ONE. The inline form renders and ships about 200
    /// plans every run whether or not the store already holds them: on one large store that was 54 GB/h
    /// rendered and 29 GB/h shipped against 1.1 GB/h of distinct plans (47x), and it is why the slowest
    /// runs are bound by draining plan text. A NOT EXISTS over the TVF inside the main query does not defer
    /// it: #1959 found the optimizer does not reliably hold the TVF back until after the filter, and the
    /// monitored server cannot take a table-valued parameter or a temp type from us either. The host's own
    /// commits already say which plans are in the store, so it filters first and sends the survivors here.
    ///
    /// The handles are inlined as hex literals, rendered host-side from bytes the same way
    /// <c>QueryStoreCollector</c> inlines its id list: nothing in the text is operator input. The cap CASE,
    /// the bytes column and the comment on both are the inline fragment's, unchanged: a plan over the cap
    /// reports its size and a NULL plan, and a handle that aged out between the two queries returns no
    /// plan and no size, which is the pairing the inline form produces for the same case.
    /// </summary>
    /// <exception cref="InvalidOperationException">The host captures no plans, or <paramref name="keys"/> is empty.</exception>
    public static CollectorQuery BuildPlanFetchQuery(CollectorContext context, IReadOnlyList<PlanFetchKey> keys)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.CapturePlanXml)
        {
            throw new InvalidOperationException(
                "A plan fetch needs CapturePlanXml: a host that captures no plans must not issue one.");
        }

        /* An empty key list means nothing is missing, the steady state in which NO target query should run.
           `VALUES ()` is a syntax error anyway, and returning a no-op query would hide the caller's missing skip. */
        if (keys is null || keys.Count == 0)
        {
            throw new InvalidOperationException(
                "The plan fetch key list must be non-empty; an empty list means no fetch should be issued at all.");
        }

        if (keys.Count > MaxPlanFetchKeys)
        {
            throw new InvalidOperationException(
                "The plan fetch key list holds " + keys.Count.ToString(CultureInfo.InvariantCulture)
                + " keys; one call takes at most " + MaxPlanFetchKeys.ToString(CultureInfo.InvariantCulture)
                + ", so the caller must split it.");
        }

        var values = new System.Text.StringBuilder();
        for (var ord = 0; ord < keys.Count; ord++)
        {
            var key = keys[ord];
            /* plan_handle is varbinary(64). A null or empty handle would render `0x`, which is not a
               usable literal, and anything longer than 64 bytes is not a plan handle. */
            if (key.PlanHandle is null || key.PlanHandle.Length == 0 || key.PlanHandle.Length > 64)
            {
                throw new InvalidOperationException(
                    "Plan fetch key " + ord.ToString(CultureInfo.InvariantCulture)
                    + " has no valid plan_handle; it must be 1 to 64 bytes.");
            }

            if (ord > 0)
            {
                values.Append(",\n        ");
            }

            values
                .Append('(')
                .Append(ord.ToString(CultureInfo.InvariantCulture))
                .Append(", 0x").Append(Convert.ToHexString(key.PlanHandle))
                .Append(", ").Append(key.StartOffset.ToString(CultureInfo.InvariantCulture))
                .Append(", ").Append(key.EndOffset.ToString(CultureInfo.InvariantCulture))
                .Append(')');
        }

        /* The cap CASE and the bytes column mirror PlanSelectFragment exactly (same constant), and
           tqp.query_plan is read twice there for the same reason: one materialized OUTER APPLY column. */
        var text = @"SELECT
    k.ord,
    query_plan_xml = CASE WHEN DATALENGTH(tqp.query_plan) > " + QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes + @" THEN NULL ELSE tqp.query_plan END,
    query_plan_xml_bytes = DATALENGTH(tqp.query_plan)
FROM
(
    VALUES
        " + values + @"
) AS k (ord, plan_handle, s, e)
OUTER APPLY
    sys.dm_exec_text_query_plan
    (
        k.plan_handle,
        k.s,
        k.e
    ) AS tqp
OPTION(RECOMPILE);";

        return new CollectorQuery(text);
    }

    /// <summary>
    /// Reads <see cref="BuildPlanFetchQuery"/>'s result into <c>ord</c> -> (plan, size). A key whose plan
    /// aged out maps to (null, null); a plan over the cap maps to (null, size).
    /// </summary>
    public static async ValueTask<Dictionary<int, (string? PlanXml, long? Bytes)>> ReadPlanFetchAsync(
        DbDataReader reader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var results = new Dictionary<int, (string? PlanXml, long? Bytes)>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var ord = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            /* Convert, not GetInt64: the same DATALENGTH-width reasoning as the inline read of this column. */
            results[ord] = (
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture));
        }

        return results;
    }

    /// <summary>
    /// True when the main query carries the plan columns itself: the host captures plans and has not
    /// deferred the fetch to <see cref="BuildPlanFetchQuery"/> (#5158). One definition for the query text
    /// and the read, so the ordinals cannot disagree with the SELECT.
    /// </summary>
    private static bool InlinePlanCapture(CollectorContext context) =>
        context.CapturePlanXml && !context.DeferPlanXmlFetch;

    public override string Name => "query_stats";

    public override string TargetTable => "query_stats";

    /// <summary>
    /// On-prem/RDS require SQL Server 2016 (v13) or newer; older boxes lack columns this query reads.
    /// Azure SQL DB / Managed Instance report a low ProductMajorVersion yet fully support the DMV, so they
    /// are never version-gated, and an unknown version (0) is assumed newest — matching the exact condition
    /// Lite used in IsCollectorSupported. Gated here in the shared AppliesTo so Lite and Darling skip
    /// identically on a pre-2016 target.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target.SqlMajorVersion == 0 || target.SqlMajorVersion >= 13 || target.IsAzureSqlDb || target.IsAzureManagedInstance;

    /// <summary>Azure SQL DB scopes dm_exec_query_stats to the connected database.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /// <summary>
    /// #2673: bound the per-server wall-clock (execute + DRAIN) so this collector can't run minutes on a
    /// monitored server (measured tail 168s on one prod box, avg 1.9s) — the 60s command timeout bounds only
    /// execution, not the drain of a large dm_exec_query_stats result. Server-scoped on on-prem/RDS, per
    /// database on Azure SQL DB; a cycle that blows the budget ships nothing and retries next cycle.
    /// </summary>
    public override TimeSpan? PerItemWallClockBudget => TimeSpan.FromSeconds(120);

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var inlinePlan = InlinePlanCapture(context);
        var planSelect = inlinePlan ? PlanSelectFragment : "";
        var planApply = inlinePlan ? PlanApplyFragment : "";

        if (context.Target.IsAzureSqlDb)
        {
            return new CollectorQuery(
                AzureSqlDbQueryText
                    .Replace("/*PLAN_SELECT*/", planSelect, StringComparison.Ordinal)
                    .Replace("/*PLAN_APPLY*/", planApply, StringComparison.Ordinal));
        }

        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        return new CollectorQuery(
            StandardQueryText
                .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal)
                .Replace("/*PLAN_SELECT*/", planSelect, StringComparison.Ordinal)
                .Replace("/*PLAN_APPLY*/", planApply, StringComparison.Ordinal),
            exclusionParameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("query_hash", CollectorColumnType.Varchar),
        new CollectorColumn("query_plan_hash", CollectorColumnType.Varchar),
        new CollectorColumn("creation_time", CollectorColumnType.Timestamp),
        new CollectorColumn("last_execution_time", CollectorColumnType.Timestamp),
        new CollectorColumn("execution_count", CollectorColumnType.BigInt),
        new CollectorColumn("total_worker_time", CollectorColumnType.BigInt),
        new CollectorColumn("total_elapsed_time", CollectorColumnType.BigInt),
        new CollectorColumn("total_logical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("total_logical_writes", CollectorColumnType.BigInt),
        new CollectorColumn("total_physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("total_clr_time", CollectorColumnType.BigInt),
        new CollectorColumn("total_rows", CollectorColumnType.BigInt),
        new CollectorColumn("total_spills", CollectorColumnType.BigInt),
        new CollectorColumn("min_worker_time", CollectorColumnType.BigInt),
        new CollectorColumn("max_worker_time", CollectorColumnType.BigInt),
        new CollectorColumn("min_elapsed_time", CollectorColumnType.BigInt),
        new CollectorColumn("max_elapsed_time", CollectorColumnType.BigInt),
        new CollectorColumn("min_physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("max_physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("min_rows", CollectorColumnType.BigInt),
        new CollectorColumn("max_rows", CollectorColumnType.BigInt),
        new CollectorColumn("min_dop", CollectorColumnType.BigInt),
        new CollectorColumn("max_dop", CollectorColumnType.BigInt),
        new CollectorColumn("min_grant_kb", CollectorColumnType.BigInt),
        new CollectorColumn("max_grant_kb", CollectorColumnType.BigInt),
        new CollectorColumn("min_used_grant_kb", CollectorColumnType.BigInt),
        new CollectorColumn("max_used_grant_kb", CollectorColumnType.BigInt),
        new CollectorColumn("min_ideal_grant_kb", CollectorColumnType.BigInt),
        new CollectorColumn("max_ideal_grant_kb", CollectorColumnType.BigInt),
        new CollectorColumn("min_reserved_threads", CollectorColumnType.BigInt),
        new CollectorColumn("max_reserved_threads", CollectorColumnType.BigInt),
        new CollectorColumn("min_used_threads", CollectorColumnType.BigInt),
        new CollectorColumn("max_used_threads", CollectorColumnType.BigInt),
        new CollectorColumn("min_spills", CollectorColumnType.BigInt),
        new CollectorColumn("max_spills", CollectorColumnType.BigInt),
        new CollectorColumn("query_text", CollectorColumnType.Varchar),
        new CollectorColumn("query_plan_xml", CollectorColumnType.Varchar),
        new CollectorColumn("sql_handle", CollectorColumnType.Varchar),
        new CollectorColumn("plan_handle", CollectorColumnType.Varchar),
        new CollectorColumn("delta_execution_count", CollectorColumnType.BigInt),
        new CollectorColumn("delta_worker_time", CollectorColumnType.BigInt),
        new CollectorColumn("delta_elapsed_time", CollectorColumnType.BigInt),
        new CollectorColumn("delta_logical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("delta_logical_writes", CollectorColumnType.BigInt),
        new CollectorColumn("delta_physical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("delta_rows", CollectorColumnType.BigInt),
        new CollectorColumn("delta_spills", CollectorColumnType.BigInt),
        new CollectorColumn("plan_generation_num", CollectorColumnType.BigInt),
        new CollectorColumn("sample_interval_seconds", CollectorColumnType.Integer),
        /* #2012 stage 2 — appended LAST so every existing store column keeps its position; the
           store-side ALTER ADDs land at the end to match. NULL for ad-hoc/prepared statements. */
        new CollectorColumn("host_object_name", CollectorColumnType.Varchar),
        /* #3392, appended after it for the same reason. BigInt because DATALENGTH over an nvarchar(max)
           expression returns bigint, and a plan XML document is measured in megabytes on the tail this
           exists to describe. */
        new CollectorColumn("query_plan_xml_bytes", CollectorColumnType.BigInt),
        /* #3540 (Darling V128 / Lite v61), appended after it for the same reason: the two statement
           offsets the delta key is made of. Until these were stored no row in query_stats could
           reproduce the key WritePayload builds, so the restart seed could restore this family's pass
           window but not one baseline, and every plan older than the restart gap re-baselined on every
           deploy. Integer, the DMV's own type. BYTE offsets into the batch's nvarchar text (Unicode, so a
           character position is offset / 2), and statement_end_offset = -1 means "to the end of the
           batch" — stored verbatim, -1 included, because the key string carries the raw values and the
           seed has to spell the same string. NULL on every pre-V128 row: the offsets were never
           recorded, and a fabricated 0/-1 would build a key nothing will ever present. */
        new CollectorColumn("statement_start_offset", CollectorColumnType.Integer),
        new CollectorColumn("statement_end_offset", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        var inlinePlan = InlinePlanCapture(context);

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row
            {
                DatabaseName = reader.IsDBNull(0) ? null : reader.GetString(0),
                QueryHash = reader.IsDBNull(1) ? "" : reader.GetString(1),
                QueryPlanHash = reader.IsDBNull(2) ? null : reader.GetString(2),
                CreationTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                LastExecutionTime = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                ExecutionCount = reader.IsDBNull(5) ? 0L : reader.GetInt64(5),
                TotalWorkerTime = reader.IsDBNull(6) ? 0L : reader.GetInt64(6),
                TotalElapsedTime = reader.IsDBNull(7) ? 0L : reader.GetInt64(7),
                TotalLogicalReads = reader.IsDBNull(8) ? 0L : reader.GetInt64(8),
                TotalLogicalWrites = reader.IsDBNull(9) ? 0L : reader.GetInt64(9),
                TotalPhysicalReads = reader.IsDBNull(10) ? 0L : reader.GetInt64(10),
                TotalClrTime = reader.IsDBNull(11) ? 0L : reader.GetInt64(11),
                TotalRows = reader.IsDBNull(12) ? 0L : reader.GetInt64(12),
                TotalSpills = reader.IsDBNull(13) ? 0L : reader.GetInt64(13),
                MinWorkerTime = reader.IsDBNull(14) ? 0L : reader.GetInt64(14),
                MaxWorkerTime = reader.IsDBNull(15) ? 0L : reader.GetInt64(15),
                MinElapsedTime = reader.IsDBNull(16) ? 0L : reader.GetInt64(16),
                MaxElapsedTime = reader.IsDBNull(17) ? 0L : reader.GetInt64(17),
                MinPhysicalReads = reader.IsDBNull(18) ? 0L : reader.GetInt64(18),
                MaxPhysicalReads = reader.IsDBNull(19) ? 0L : reader.GetInt64(19),
                MinRows = reader.IsDBNull(20) ? 0L : reader.GetInt64(20),
                MaxRows = reader.IsDBNull(21) ? 0L : reader.GetInt64(21),
                MinDop = reader.IsDBNull(22) ? 0L : Convert.ToInt64(reader.GetValue(22), CultureInfo.InvariantCulture),
                MaxDop = reader.IsDBNull(23) ? 0L : Convert.ToInt64(reader.GetValue(23), CultureInfo.InvariantCulture),
                MinGrantKb = reader.IsDBNull(24) ? 0L : reader.GetInt64(24),
                MaxGrantKb = reader.IsDBNull(25) ? 0L : reader.GetInt64(25),
                MinUsedGrantKb = reader.IsDBNull(26) ? 0L : reader.GetInt64(26),
                MaxUsedGrantKb = reader.IsDBNull(27) ? 0L : reader.GetInt64(27),
                MinIdealGrantKb = reader.IsDBNull(28) ? 0L : reader.GetInt64(28),
                MaxIdealGrantKb = reader.IsDBNull(29) ? 0L : reader.GetInt64(29),
                MinReservedThreads = reader.IsDBNull(30) ? 0L : reader.GetInt64(30),
                MaxReservedThreads = reader.IsDBNull(31) ? 0L : reader.GetInt64(31),
                MinUsedThreads = reader.IsDBNull(32) ? 0L : reader.GetInt64(32),
                MaxUsedThreads = reader.IsDBNull(33) ? 0L : reader.GetInt64(33),
                MinSpills = reader.IsDBNull(34) ? 0L : reader.GetInt64(34),
                MaxSpills = reader.IsDBNull(35) ? 0L : reader.GetInt64(35),
                SqlHandle = reader.IsDBNull(36) ? null : reader.GetString(36),
                PlanHandle = reader.IsDBNull(37) ? null : reader.GetString(37),
                QueryText = reader.IsDBNull(38) ? null : reader.GetString(38),
                PlanGenerationNum = reader.IsDBNull(39) ? 0L : reader.GetInt64(39),
                StatementStartOffset = reader.IsDBNull(40) ? 0 : reader.GetInt32(40),
                StatementEndOffset = reader.IsDBNull(41) ? 0 : reader.GetInt32(41),
                /* #2012 stage 2: the statement's HOST OBJECT (schema.name), NULL for ad-hoc/prepared
                   text — sys.dm_exec_sql_text.objectid resolved in the SELECT. This is what lets
                   readers split INSERT...EXEC callers that share a query_hash. */
                HostObjectName = reader.IsDBNull(42) ? null : reader.GetString(42),
                /* #2235: compile age sits at 43 — inside SelectColumnsText, so it is present in BOTH
                   capture modes and its ordinal is fixed. That pushes the plan XML to 44. */
                CompileAgeSeconds = reader.IsDBNull(43) ? null : reader.GetInt32(43),
                /* query_plan_xml is the trailing column present only when CapturePlanXml spliced it
                   into the SELECT (ordinal 44); the short-circuit skips it entirely when off, and also when
                   the host defers the plan fetch (#5158), which leaves the main query without it. */
                QueryPlanXml = inlinePlan && !reader.IsDBNull(44) ? reader.GetString(44) : null,
                /* #3392: the plan's measured size rides the same splice at ordinal 45, so the same
                   short-circuit covers it. Convert rather than GetInt64: DATALENGTH's return type widens to
                   bigint only for the max types, and a provider that hands back an Int32 here would throw
                   on a strict accessor — the min_dop/max_dop idiom above, for the same reason. */
                QueryPlanXmlBytes = inlinePlan && !reader.IsDBNull(45)
                    ? Convert.ToInt64(reader.GetValue(45), CultureInfo.InvariantCulture)
                    : null,
            });
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* Delta key = the dm_exec_query_stats row identity (sql_handle + offsets + plan_handle).
           Keying on plan_handle alone cross-contaminated multi-statement plans — parity contract.
           The two hosts' restart seeds (DeltaCalculator / DarlingDeltaCalculator, QueryStatsSeedSql)
           rebuild THIS string from the stored handles and offsets with the same interpolation — a null
           handle formats as empty here and there, and the raw offsets (-1 included) are spelled by the
           same int formatting — so the seeded key is the one this line presents. */
        var deltaKey = $"{row.SqlHandle}:{row.StatementStartOffset}:{row.StatementEndOffset}:{row.PlanHandle}";

        /* #2235: plan_handle is in the key above, and it changes on every recompile — so a churning plan
           presents a NEW key on nearly every sighting, and a first sighting reports 0. That silently
           discarded most of a plan-churning instance's CPU (a query Datadog measured at ~43% of the box
           read as 18 executions and 2,824 ms over 168 hours), and it was invisible because the honest
           "unknowable, not zero" path needs the SAME key to reappear lower, which a recompile never does.
           Passing the compile age lets the calculator credit a plan whose counter demonstrably STARTED
           since the previous pass, which is the recoverable half. The unrecoverable half is a plan
           compiled AND evicted between two passes: it never appears in the DMV at all, so no keying
           scheme can recover it — that ceiling is stated rather than left to be discovered.

           ALL EIGHT delta'd counters take the same rule. Crediting only some would make one row's metrics
           disagree about how much work it did, which is worse than under-reporting all of them. */
        var age = row.CompileAgeSeconds;

        /* #4428: a per-family CalculateDeltaWithSeriesAge call only ever sees its OWN counter, so a row
           whose statistics restart under this same key can be read as a reset in one family (the one that
           happened to shrink) and a giant real increment in another (a sibling that had already re-grown
           past its own pre-restart value) — the same row telling two different stories about whether it
           restarted. In the field: executions 1 -> 16 against a CPU counter that fell from 57,695,259 to
           703,943 read as "executions +15" instead of "16 executions since the restart". DecideRow peeks
           every family's cached value under deltaKey WITHOUT updating anything, so the decision is made
           once, ROW-COHERENTLY, before any of the eight per-family calls below mutate a baseline: if any
           family would reset, every counter's fate is the same — the #2235 series-age test's call on
           whether the restart falls inside the gap since we last looked, exactly as a single reset already
           decides for itself. A row with no reset is unaffected: DecideRow reports AnyReset = false and
           every per-family call below runs its ordinary path. */
        var rowReset = context.Deltas.DecideRow(
            context.ServerId,
            new (string Family, long Current)[]
            {
                ("query_stats_exec", row.ExecutionCount),
                ("query_stats_worker", row.TotalWorkerTime),
                ("query_stats_elapsed", row.TotalElapsedTime),
                ("query_stats_reads", row.TotalLogicalReads),
                ("query_stats_writes", row.TotalLogicalWrites),
                ("query_stats_phys_reads", row.TotalPhysicalReads),
                ("query_stats_rows", row.TotalRows),
                ("query_stats_spills", row.TotalSpills),
            },
            deltaKey,
            age,
            context.CollectionTime,
            CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var deltaExecCount = context.Deltas.CalculateDeltaWithSeriesAge(context.ServerId, "query_stats_exec", deltaKey, row.ExecutionCount, age, out var execIntervalSeconds, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        /* Capture the collection interval alongside the CPU delta so the display can derive
           worker_time_per_second (peak CPU-ms per wall-clock second) over the window. */
        var deltaWorkerTime = context.Deltas.CalculateDeltaWithSeriesAge(context.ServerId, "query_stats_worker", deltaKey, row.TotalWorkerTime, age, out var workerIntervalSeconds, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaElapsedTime = context.Deltas.CalculateDeltaWithSeriesAge(context.ServerId, "query_stats_elapsed", deltaKey, row.TotalElapsedTime, age, out var elapsedIntervalSeconds, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaLogicalReads = context.Deltas.CalculateDeltaWithSeriesAge(context.ServerId, "query_stats_reads", deltaKey, row.TotalLogicalReads, age, out _, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaLogicalWrites = context.Deltas.CalculateDeltaWithSeriesAge(context.ServerId, "query_stats_writes", deltaKey, row.TotalLogicalWrites, age, out _, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaPhysicalReads = context.Deltas.CalculateDeltaWithSeriesAge(context.ServerId, "query_stats_phys_reads", deltaKey, row.TotalPhysicalReads, age, out _, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaRows = context.Deltas.CalculateDeltaWithSeriesAge(context.ServerId, "query_stats_rows", deltaKey, row.TotalRows, age, out _, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var deltaSpills = context.Deltas.CalculateDeltaWithSeriesAge(context.ServerId, "query_stats_spills", deltaKey, row.TotalSpills, age, out _, collectionTime: context.CollectionTime, maxGapSeconds: CollectorDeltaCalculator.DefaultMaxGapSeconds);

        /* #4428: apply the row-coherent decision AFTER every per-family call above has run — each call
           already updated its own baseline to the current value/time (the store-forward the Add/Update
           branches always perform), so the row is ready for an ordinary delta on the NEXT pass regardless
           of which branch this row takes now. When the row restarted, override every counter together:
           credited-in-gap makes each counter's delta its own CURRENT value over the real interval (the
           restart's whole accrual, symmetric with #2235's single-family rescue); otherwise every counter
           becomes unknowable, (0, 0), rather than the mixed reset-plus-inflated-increment the per-family
           calls above would otherwise have written. */
        if (rowReset.AnyReset)
        {
            if (rowReset.CreditedInGap)
            {
                deltaExecCount = row.ExecutionCount;
                deltaWorkerTime = row.TotalWorkerTime;
                deltaElapsedTime = row.TotalElapsedTime;
                deltaLogicalReads = row.TotalLogicalReads;
                deltaLogicalWrites = row.TotalLogicalWrites;
                deltaPhysicalReads = row.TotalPhysicalReads;
                deltaRows = row.TotalRows;
                deltaSpills = row.TotalSpills;
                execIntervalSeconds = rowReset.IntervalSeconds;
                workerIntervalSeconds = rowReset.IntervalSeconds;
                elapsedIntervalSeconds = rowReset.IntervalSeconds;
            }
            else
            {
                deltaExecCount = 0;
                deltaWorkerTime = 0;
                deltaElapsedTime = 0;
                deltaLogicalReads = 0;
                deltaLogicalWrites = 0;
                deltaPhysicalReads = 0;
                deltaRows = 0;
                deltaSpills = 0;
                execIntervalSeconds = 0;
                workerIntervalSeconds = 0;
                elapsedIntervalSeconds = 0;
            }
        }

        /* #4394: the worker (CPU) counter's own interval can land at 0 — a first sighting, a plan
           reset, or a gap past the policy — while the exec-count or elapsed-time counters (same row,
           same collection pass) return a real, knowable delta. Writing CPU as a false 0 over interval 0
           in that case made the row look like it did no work, and the interval-honest filter
           (sample_interval_seconds IS DISTINCT FROM 0) then discarded its real executions and duration
           along with it. #2234 established interval 0 as the pairing for "no delta knowable"; that now
           holds per counter rather than per row — CPU can be unknowable (NULL) while exec/elapsed are
           real. A worker delta of 0 over a REAL interval (a query that ran but burned no CPU) is left
           untouched: that 0 is measured, not assumed. */
        var (resolvedWorkerDelta, sampleIntervalSeconds) = ResolveWorkerDelta(
            deltaWorkerTime,
            workerIntervalSeconds,
            execIntervalSeconds,
            elapsedIntervalSeconds);

        writer
            .Value(row.DatabaseName)
            .Value(row.QueryHash)
            .Value(row.QueryPlanHash)
            .Value(row.CreationTime)
            .Value(row.LastExecutionTime)
            .Value(row.ExecutionCount)
            .Value(row.TotalWorkerTime)
            .Value(row.TotalElapsedTime)
            .Value(row.TotalLogicalReads)
            .Value(row.TotalLogicalWrites)
            .Value(row.TotalPhysicalReads)
            .Value(row.TotalClrTime)
            .Value(row.TotalRows)
            .Value(row.TotalSpills)
            .Value(row.MinWorkerTime)
            .Value(row.MaxWorkerTime)
            .Value(row.MinElapsedTime)
            .Value(row.MaxElapsedTime)
            .Value(row.MinPhysicalReads)
            .Value(row.MaxPhysicalReads)
            .Value(row.MinRows)
            .Value(row.MaxRows)
            .Value(row.MinDop)
            .Value(row.MaxDop)
            .Value(row.MinGrantKb)
            .Value(row.MaxGrantKb)
            .Value(row.MinUsedGrantKb)
            .Value(row.MaxUsedGrantKb)
            .Value(row.MinIdealGrantKb)
            .Value(row.MaxIdealGrantKb)
            .Value(row.MinReservedThreads)
            .Value(row.MaxReservedThreads)
            .Value(row.MinUsedThreads)
            .Value(row.MaxUsedThreads)
            .Value(row.MinSpills)
            .Value(row.MaxSpills)
            .Value(row.QueryText)
            .PayloadOrDigest(row.QueryPlanXml, row.KnownPlanDigest) /* null unless CapturePlanXml captured it (Darling); #5158: or the digest of a plan the store holds */
            .Value(row.SqlHandle)
            .Value(row.PlanHandle)
            .Value(deltaExecCount)
            .Value(resolvedWorkerDelta)
            .Value(deltaElapsedTime)
            .Value(deltaLogicalReads)
            .Value(deltaLogicalWrites)
            .Value(deltaPhysicalReads)
            .Value(deltaRows)
            .Value(deltaSpills)
            .Value(row.PlanGenerationNum)
            .Value(sampleIntervalSeconds)      /* sample_interval_seconds INTEGER */
            .Value(row.HostObjectName)         /* #2012 stage 2: NULL for ad-hoc text */
            .Value(row.QueryPlanXmlBytes)      /* #3392: measured size, never gated by the cap */
            .Value(row.StatementStartOffset)   /* #3540: the delta key's offsets, raw, -1 included */
            .Value(row.StatementEndOffset);
    }

    /// <summary>
    /// #4394: resolves the worker (CPU) delta to write. The worker counter's own delta call can return
    /// interval 0 (a first sighting, a plan reset, or a gap past the policy) in the SAME collection pass
    /// where the exec-count or elapsed-time counters return a real, knowable delta. In that case CPU is
    /// unknowable rather than zero, so this returns a NULL worker delta and takes the row's
    /// sample_interval_seconds from whichever of exec/elapsed has a real interval (exec first, then
    /// elapsed). Every other case passes the worker call's own delta and interval through unchanged —
    /// in particular a worker delta of 0 over a REAL interval (a query that ran but burned no measurable
    /// CPU) is a measured 0, not an unknowable one, and keeps its own interval.
    /// </summary>
    internal static (long? WorkerDelta, int IntervalSeconds) ResolveWorkerDelta(
        long workerDelta,
        int workerIntervalSeconds,
        int execIntervalSeconds,
        int elapsedIntervalSeconds)
    {
        if (workerIntervalSeconds != 0)
        {
            return (workerDelta, workerIntervalSeconds);
        }

        if (execIntervalSeconds > 0)
        {
            return (null, execIntervalSeconds);
        }

        if (elapsedIntervalSeconds > 0)
        {
            return (null, elapsedIntervalSeconds);
        }

        return (workerDelta, workerIntervalSeconds);
    }

    /// <summary>
    /// #3392: this row is a backlog candidate when its plan measured over the cap and both cache handles are
    /// present. The offsets handed back are the SAME ones <see cref="PlanApplyFragment"/> passes, so the
    /// deferred fetch re-issues the identical statement-grain call rather than a plausible-looking variant of
    /// it — a plan_handle with the wrong offsets is a different statement's plan, which is the exact
    /// cross-contamination the delta key's own comment records.
    /// </summary>
    public override OversizedPlanObservation? DescribeOversizedPlan(Row row)
    {
        if (!QueryPlanXmlCaptureLimits.ExceedsCaptureCap(row.QueryPlanXmlBytes)
            || row.PlanHandle is null
            || row.SqlHandle is null)
        {
            return null;
        }

        return new OversizedPlanObservation(
            row.PlanHandle,
            row.SqlHandle,
            row.StatementStartOffset,
            row.StatementEndOffset,
            row.DatabaseName,
            row.QueryHash,
            row.QueryPlanXmlBytes!.Value);
    }
}
