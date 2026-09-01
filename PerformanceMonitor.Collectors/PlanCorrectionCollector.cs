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
/// Automatic plan correction (#1952): what the engine's own plan-regression detector is doing, per
/// database. Two layers in one row — the FORCE_LAST_GOOD_PLAN enablement state from
/// sys.database_automatic_tuning_options, and the live recommendation set from
/// sys.dm_db_tuning_recommendations with its details JSON shredded into typed columns and the
/// Query Store query text resolved at collection time.
///
/// <para>
/// Collecting this at all is the point: the DMV is documented as "the database engine keeps
/// recommendations only until it restarts". A restart erases the whole set, and a restart DURING a
/// verification silently unforces the plan the engine had forced — so a cycle that captured a
/// Verifying row is the only surviving record that any of it happened. That is also why this is not
/// on the FrequencyMinutes 0 (on-load) tier the other config snapshots use.
/// </para>
///
/// <para>
/// Both objects are DATABASE-scoped, so this takes the enumeration path (list databases, then
/// EXECUTE [db].sys.sp_executesql per database on one connection) exactly like
/// database_scoped_config and query_store, with the Azure SQL DB per-database-connection branch
/// those collectors also carry. Live-verified 2026-08-01 on SQL2022: the DMV binds and returns that
/// database's rows through the three-part sp_executesql call.
/// </para>
///
/// <para>
/// Every database is enumerated, NOT just the Query-Store-enabled ones. The interesting diagnostic
/// is precisely the database where someone asked for FORCE_LAST_GOOD_PLAN and did not get it:
/// desired ON / actual OFF with reason QUERY_STORE_OFF (live-verified). Filtering the enumeration by
/// Query Store would drop exactly those rows. The engine's own reason_desc is the answer to "is
/// Query Store blocking this", which is why nothing here re-probes Query Store state.
/// </para>
/// </summary>
public sealed class PlanCorrectionCollector : CollectorDefinitionBase<PlanCorrectionCollector.Row>
{
    public static PlanCorrectionCollector Instance { get; } = new();

    private PlanCorrectionCollector()
    {
    }

    public sealed class Row
    {
        public string DbName { get; set; } = "";

        /* Enablement (sys.database_automatic_tuning_options) — repeated on every row of a database. */
        public string? DesiredState { get; set; }
        public string? ActualState { get; set; }
        public string? StateReason { get; set; }
        public string? CreateIndexState { get; set; }
        public string? DropIndexState { get; set; }

        /* The recommendation (sys.dm_db_tuning_recommendations). All null on the enablement-only row. */
        public string? RecommendationName { get; set; }
        public string? RecommendationType { get; set; }
        public string? RecommendationState { get; set; }
        public string? RecommendationStateReason { get; set; }
        public string? RecommendationReason { get; set; }
        public DateTime? ValidSince { get; set; }
        public DateTime? LastRefresh { get; set; }
        public int? Score { get; set; }

        /* The query the engine is working on, resolved through Query Store at collection time. */
        public long? QueryId { get; set; }
        public string? QueryText { get; set; }
        public long? RegressedPlanId { get; set; }
        public long? LastGoodPlanId { get; set; }
        public string? LastGoodPlanForcingType { get; set; }
        public bool? LastGoodPlanIsForced { get; set; }
        public string? LastGoodPlanForceFailureReason { get; set; }

        /* The regression evidence, shredded out of details.planForceDetails. */
        public long? RegressedPlanExecutionCount { get; set; }
        public double? RegressedPlanCpuTimeAverageMs { get; set; }
        public long? RegressedPlanErrorCount { get; set; }
        public long? LastGoodPlanExecutionCount { get; set; }
        public double? LastGoodPlanCpuTimeAverageMs { get; set; }
        public long? LastGoodPlanErrorCount { get; set; }
        public double? EstimatedGainSeconds { get; set; }

        /* What the engine did, or could do, about it. */
        public bool? IsExecutableAction { get; set; }
        public bool? IsRevertableAction { get; set; }
        public string? ExecuteActionInitiatedBy { get; set; }
        public DateTime? ExecuteActionInitiatedTime { get; set; }
        public DateTime? ExecuteActionStartTime { get; set; }
        public double? ExecuteActionDurationSeconds { get; set; }
        public string? RevertActionInitiatedBy { get; set; }
        public DateTime? RevertActionInitiatedTime { get; set; }
        public DateTime? RevertActionStartTime { get; set; }
        public double? RevertActionDurationSeconds { get; set; }
        public string? ImplementationScript { get; set; }
    }

    /// <summary>
    /// The per-database read, shared by both branches: the enumeration path quote-doubles it into
    /// <c>EXECUTE [db].sys.sp_executesql</c>, and the Azure per-database branch runs it as-is on a
    /// connection already pointed at the database.
    ///
    /// <para>
    /// #2673 tuning (the sp_QuickieStore rule — stage first, never naively join the Query Store views):
    /// the recommendation set is materialised into a #temp BEFORE the Query Store views are touched.
    /// sys.dm_db_tuning_recommendations is a DMV with no statistics, so the optimizer guesses its
    /// cardinality high and then satisfies the joins to sys.query_store_plan / sys.query_store_query by
    /// SCANNING the whole plan store — catastrophic on a bloated Query Store, where one prod database's
    /// read tailed to 260s (avg ~4s). The recommendation set is invariably small (the engine keeps only
    /// a handful of live recommendations and erases them on restart), so materialising it first — with
    /// its details JSON already shredded — hands the optimizer a real row count, and the final query
    /// nested-loop SEEKS each query_id/plan_id into the QS views instead of scanning them. SET NOCOUNT
    /// ON so the SELECT INTO emits no result set and the caller's reader takes the final SELECT as the
    /// shipped rows; the #temp is scoped to this sp_executesql and dropped explicitly.
    /// </para>
    ///
    /// <para>
    /// #2759/#2760: neither statement carries OPTION(RECOMPILE) anymore. Both are static literal T-SQL
    /// with no runtime parameters — nothing here for RECOMPILE to protect against parameter sniffing —
    /// and sys.dm_db_tuning_recommendations has no statistics (see below), so a cached plan's cardinality
    /// guess is the same fixed heuristic a fresh compile would produce; there is no plan-quality benefit
    /// to buy back. Live evidence on prod-pos-use1-multi-45/Demo: 5,004ms avg duration, 4,237ms avg CPU
    /// (85%), 381 logical reads, 18 rows — a compile-cost signature, not an execution-cost one, on a
    /// query that runs per database, per server, every cycle. Caching the plan removes that repeated
    /// compile cost fleet-wide with no functional change.
    /// </para>
    ///
    /// <para>
    /// The details JSON is shredded with JSON_VALUE rather than the OPENJSON form Microsoft's own
    /// examples use, and that is not a style preference: OPENJSON is only available at database
    /// COMPATIBILITY LEVEL 130 or higher, and a compat-100 database on a 2017+ instance is perfectly
    /// legal. Verified 2026-08-01 on SQL2022 against a compat-100 database — JSON_VALUE returned the
    /// value, OPENJSON failed to parse at all ("Incorrect syntax near '$.queryId'"). The documented
    /// query would take out the whole collection for that database. For the same reason the numeric
    /// extractions use TRY_CAST, not TRY_CONVERT: TRY_CONVERT is not a recognized built-in below
    /// COMPATIBILITY LEVEL 110 (raises Msg 195), while TRY_CAST binds at compat 100 — verified on the
    /// SQL 2025 rig, TRY_CAST('123' AS bigint) returns 123 at compat 100 where TRY_CONVERT throws.
    /// (House rule: always TRY_CAST over TRY_CONVERT unless a CONVERT style code is actually needed.)
    /// </para>
    ///
    /// <para>
    /// Both spellings of the aborted/error counts are read and COALESCEd. MS Learn's column table
    /// documents <c>regressedPlanAbortedCount</c> while every example query on the same page (and on
    /// the automatic-tuning overview page) shreds <c>regressedPlanErrorCount</c>; the two cannot both
    /// be right, and picking the wrong one binds a silent NULL rather than raising anything. Reading
    /// both costs one JSON_VALUE each and cannot be wrong.
    /// </para>
    ///
    /// <para>
    /// The Query Store joins are LEFT, deliberately. Microsoft's Example 3 INNER JOINs
    /// sys.query_store_plan twice, which drops any recommendation whose regressed or recommended plan
    /// has already aged out of Query Store — the recommendation is still live and still names a real
    /// regression, so dropping it is exactly backwards for a monitoring tool. A recommendation with
    /// no resolvable text lands with a null query_text instead of vanishing.
    /// </para>
    ///
    /// <para>
    /// The enablement half is a single no-GROUP-BY aggregate so it yields exactly one row even when
    /// the view is empty, and the DMV is LEFT JOINed to it. That is what guarantees a database with
    /// zero recommendations still reports its enablement state instead of collecting nothing.
    /// </para>
    /// </summary>
    private const string PayloadBodyText = @"
SET NOCOUNT ON;

/*Stage the (invariably small) recommendation set with its details JSON already shredded, so the
  Query Store joins below seek a real row count instead of scanning the whole plan store off a
  statistics-free DMV cardinality guess. See the summary for why this is the 260s fix.*/
DROP TABLE IF EXISTS #pm_plan_correction_recs;

SELECT
    recommendation_name = dtr.name,
    recommendation_type = dtr.type,
    recommendation_state = JSON_VALUE(dtr.state, '$.currentValue'),
    recommendation_state_reason = JSON_VALUE(dtr.state, '$.reason'),
    recommendation_reason = dtr.reason,
    valid_since = dtr.valid_since,
    last_refresh = dtr.last_refresh,
    score = dtr.score,
    query_id = pfd.query_id,
    regressed_plan_id = pfd.regressed_plan_id,
    last_good_plan_id = pfd.last_good_plan_id,
    regressed_plan_execution_count = pfd.regressed_plan_execution_count,
    regressed_plan_cpu_time_average_ms = pfd.regressed_plan_cpu_time_average / 1000.0,
    regressed_plan_error_count = pfd.regressed_plan_error_count,
    last_good_plan_execution_count = pfd.last_good_plan_execution_count,
    last_good_plan_cpu_time_average_ms = pfd.last_good_plan_cpu_time_average / 1000.0,
    last_good_plan_error_count = pfd.last_good_plan_error_count,
    /*Microsoft's own Estimated Gain (sec) formula from the MS Learn automatic-tuning shredding
      examples: BOTH plans' execution counts are summed deliberately (total executions at the
      regressed rate minus the same total at the good rate). Live-captured data reproduces the
      engine's number exactly (64.84s on the repro). Computed here off the raw microsecond averages
      so the arithmetic is unchanged by the /1000 the ms columns carry.*/
    estimated_gain_seconds =
        (pfd.regressed_plan_execution_count + pfd.last_good_plan_execution_count)
        * (pfd.regressed_plan_cpu_time_average - pfd.last_good_plan_cpu_time_average)
        / 1000000.0,
    is_executable_action = dtr.is_executable_action,
    is_revertable_action = dtr.is_revertable_action,
    execute_action_initiated_by = dtr.execute_action_initiated_by,
    execute_action_initiated_time = dtr.execute_action_initiated_time,
    execute_action_start_time = dtr.execute_action_start_time,
    execute_action_duration_seconds =
        CONVERT(float, DATEDIFF(millisecond, CONVERT(time(7), '00:00:00'), dtr.execute_action_duration)) / 1000.0,
    revert_action_initiated_by = dtr.revert_action_initiated_by,
    revert_action_initiated_time = dtr.revert_action_initiated_time,
    revert_action_start_time = dtr.revert_action_start_time,
    revert_action_duration_seconds =
        CONVERT(float, DATEDIFF(millisecond, CONVERT(time(7), '00:00:00'), dtr.revert_action_duration)) / 1000.0,
    implementation_script = JSON_VALUE(dtr.details, '$.implementationDetails.script')
INTO #pm_plan_correction_recs
FROM sys.dm_db_tuning_recommendations AS dtr
CROSS APPLY
(
    SELECT
        query_id =
            TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.queryId') AS bigint),
        regressed_plan_id =
            TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.regressedPlanId') AS bigint),
        last_good_plan_id =
            TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.recommendedPlanId') AS bigint),
        regressed_plan_execution_count =
            TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.regressedPlanExecutionCount') AS bigint),
        last_good_plan_execution_count =
            TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.recommendedPlanExecutionCount') AS bigint),
        regressed_plan_cpu_time_average =
            TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.regressedPlanCpuTimeAverage') AS float),
        last_good_plan_cpu_time_average =
            TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.recommendedPlanCpuTimeAverage') AS float),
        /*Docs defect: the column table says AbortedCount, every example says ErrorCount. Read both.*/
        regressed_plan_error_count =
            COALESCE
            (
                TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.regressedPlanAbortedCount') AS bigint),
                TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.regressedPlanErrorCount') AS bigint)
            ),
        last_good_plan_error_count =
            COALESCE
            (
                TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.recommendedPlanAbortedCount') AS bigint),
                TRY_CAST(JSON_VALUE(dtr.details, '$.planForceDetails.recommendedPlanErrorCount') AS bigint)
            )
) AS pfd;

SELECT
    force_last_good_plan_desired_state = o.desired_state_desc,
    force_last_good_plan_actual_state = o.actual_state_desc,
    force_last_good_plan_reason = o.reason_desc,
    create_index_actual_state = create_index.actual_state_desc,
    drop_index_actual_state = drop_index.actual_state_desc,
    recommendation_name = r.recommendation_name,
    recommendation_type = r.recommendation_type,
    recommendation_state = r.recommendation_state,
    recommendation_state_reason = r.recommendation_state_reason,
    recommendation_reason = r.recommendation_reason,
    valid_since = r.valid_since,
    last_refresh = r.last_refresh,
    score = r.score,
    query_id = r.query_id,
    query_text = qsqt.query_sql_text,
    regressed_plan_id = r.regressed_plan_id,
    last_good_plan_id = r.last_good_plan_id,
    last_good_plan_forcing_type = qsp.plan_forcing_type_desc,
    last_good_plan_is_forced = qsp.is_forced_plan,
    last_good_plan_force_failure_reason = NULLIF(qsp.last_force_failure_reason_desc, N'NONE'),
    regressed_plan_execution_count = r.regressed_plan_execution_count,
    regressed_plan_cpu_time_average_ms = r.regressed_plan_cpu_time_average_ms,
    regressed_plan_error_count = r.regressed_plan_error_count,
    last_good_plan_execution_count = r.last_good_plan_execution_count,
    last_good_plan_cpu_time_average_ms = r.last_good_plan_cpu_time_average_ms,
    last_good_plan_error_count = r.last_good_plan_error_count,
    estimated_gain_seconds = r.estimated_gain_seconds,
    is_executable_action = r.is_executable_action,
    is_revertable_action = r.is_revertable_action,
    execute_action_initiated_by = r.execute_action_initiated_by,
    execute_action_initiated_time = r.execute_action_initiated_time,
    execute_action_start_time = r.execute_action_start_time,
    execute_action_duration_seconds = r.execute_action_duration_seconds,
    revert_action_initiated_by = r.revert_action_initiated_by,
    revert_action_initiated_time = r.revert_action_initiated_time,
    revert_action_start_time = r.revert_action_start_time,
    revert_action_duration_seconds = r.revert_action_duration_seconds,
    implementation_script = r.implementation_script
FROM
(
    /*The one-row anchor. Every enablement lookup hangs off it as an OUTER APPLY, so this query
      returns a row for the database even when the view and the DMV are both empty — that is what
      keeps a database with no recommendations reporting its enablement state.*/
    SELECT
        anchor = 1
) AS a
OUTER APPLY
(
    SELECT TOP (1)
        flgp.desired_state_desc,
        flgp.actual_state_desc,
        flgp.reason_desc
    FROM sys.database_automatic_tuning_options AS flgp
    WHERE flgp.name = N'FORCE_LAST_GOOD_PLAN'
) AS o
/*Azure SQL Database only — automatic index management does not exist on box or on Managed
  Instance, so these two are NULL everywhere else.*/
OUTER APPLY
(
    SELECT TOP (1)
        ci.actual_state_desc
    FROM sys.database_automatic_tuning_options AS ci
    WHERE ci.name = N'CREATE_INDEX'
) AS create_index
OUTER APPLY
(
    SELECT TOP (1)
        di.actual_state_desc
    FROM sys.database_automatic_tuning_options AS di
    WHERE di.name = N'DROP_INDEX'
) AS drop_index
LEFT JOIN #pm_plan_correction_recs AS r
  ON 1 = 1 /*keeps the enablement row when the engine has no recommendations*/
LEFT JOIN sys.query_store_query AS qsq
  ON qsq.query_id = r.query_id
LEFT JOIN sys.query_store_query_text AS qsqt
  ON qsqt.query_text_id = qsq.query_text_id
LEFT JOIN sys.query_store_plan AS qsp
  ON qsp.query_id = r.query_id
  AND qsp.plan_id = r.last_good_plan_id
ORDER BY r.score DESC, r.recommendation_name;

DROP TABLE #pm_plan_correction_recs;";

    /// <summary>
    /// The database list. The version predicate rides in the WHERE clause rather than an IF so the
    /// batch always returns a result set (an empty one on an older instance) — the enumeration
    /// contract's first result set must exist. <see cref="AppliesTo"/> already gates this statically;
    /// this is the same gate re-evaluated on the server, which is what covers the unknown-version case
    /// (SqlMajorVersion 0 = assume newest) that would otherwise build a query naming a DMV that is not
    /// there until 2017.
    /// </summary>
    private const string OnPremDatabaseListQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @product_major integer =
        CONVERT(integer, PARSENAME(CONVERT(sysname, SERVERPROPERTY('PRODUCTVERSION')), 4));

SELECT
    d.name
FROM sys.databases AS d
LEFT JOIN sys.dm_hadr_database_replica_states AS drs
    ON d.database_id = drs.database_id
    AND drs.is_local = 1
WHERE @product_major >= 14 /*automatic plan correction is 2017+; older instances enumerate nothing*/
AND   (d.database_id > 4 OR d.database_id = 2)
AND   d.database_id < 32761
AND   d.name <> N'PerformanceMonitor'
AND   d.state_desc = N'ONLINE'
AND   HAS_DBACCESS(d.name) = 1 /*a least-privilege login without per-db access raised 916 per db per cycle (#1823)*/
AND
(
    drs.database_id IS NULL          /*not in any AG*/
    OR drs.is_primary_replica = 1    /*primary replica*/
)
/*EXCLUSION_FILTER*/
ORDER BY d.name
OPTION(RECOMPILE);";

    public override string Name => "plan_correction";

    public override string TargetTable => "plan_correction";

    /// <summary>
    /// 2017+ on box. Azure SQL Database and Managed Instance both carry automatic plan correction
    /// (MI supports FORCE_LAST_GOOD_PLAN only — no automatic index management), so neither is
    /// version-gated. SqlMajorVersion 0 = version unknown = assume newest, the house convention; the
    /// enumeration query re-checks PRODUCTVERSION on the server for exactly that case.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target is null
        || target.SqlMajorVersion == 0
        || target.SqlMajorVersion >= 14
        || target.IsAzureSqlDb
        || target.IsAzureManagedInstance;

    /// <summary>Azure SQL DB rejects three-part names, so the host connects per database there instead.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target is not null && target.IsAzureSqlDb;

    /// <summary>
    /// #2673: bound the per-server wall-clock (execute + DRAIN). Measured the WORST single-collector tail in
    /// the fleet — 260s on one prod server (avg ~4s) — because on a bloated Query Store the recommendation
    /// read scanned the whole plan store off a statistics-free DMV cardinality guess (root cause since fixed by
    /// staging the recommendation set to a #temp — see <see cref="PayloadBodyText"/>), and the 60s command
    /// timeout covers only execution, not the drain. This budget stays as the backstop: server-scoped on
    /// on-prem/RDS (where that tail is), per database on Azure SQL DB; a cycle that blows the budget ships
    /// nothing and retries next, so this collector cannot run minutes on a monitored server even if a future
    /// shape regresses.
    /// </summary>
    public override TimeSpan? PerItemWallClockBudget => TimeSpan.FromSeconds(120);

    /// <summary>The Azure per-database branch: already connected to the database, so read it directly.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException(
                "plan_correction enumerates databases off Azure SQL DB; BuildEnumerationQuery drives the cycle.");
        }

        return new CollectorQuery(PayloadBodyText);
    }

    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        /* Azure SQL DB takes the per-database-connection path instead — no enumeration. */
        if (context.Target.IsAzureSqlDb)
        {
            return null;
        }

        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        var text = OnPremDatabaseListQueryText
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(item);

        /* [dbname].sys.sp_executesql to run in database context — both objects are database-scoped. */
        var text = $@"
EXECUTE [{item.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql
    N'{PayloadBodyText.Replace("'", "''", StringComparison.Ordinal)}'";

        return new CollectorQuery(text);
    }

    public override async ValueTask ReadItemAsync(string item, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(rows);

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadRow(reader, item));
        }
    }

    /// <summary>The Azure per-database branch's read; the connection's database names the rows.</summary>
    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(context);

        var rows = new List<Row>();
        var databaseName = context.CurrentDatabaseName ?? "";

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadRow(reader, databaseName));
        }

        return rows;
    }

    /// <summary>Ordinals follow <see cref="PayloadBodyText"/>'s SELECT list, which follows PayloadColumns.</summary>
    private static Row ReadRow(DbDataReader reader, string databaseName) =>
        new()
        {
            DbName = databaseName,
            DesiredState = NullableString(reader, 0),
            ActualState = NullableString(reader, 1),
            StateReason = NullableString(reader, 2),
            CreateIndexState = NullableString(reader, 3),
            DropIndexState = NullableString(reader, 4),
            RecommendationName = NullableString(reader, 5),
            RecommendationType = NullableString(reader, 6),
            RecommendationState = NullableString(reader, 7),
            RecommendationStateReason = NullableString(reader, 8),
            RecommendationReason = NullableString(reader, 9),
            ValidSince = NullableDateTime(reader, 10),
            LastRefresh = NullableDateTime(reader, 11),
            Score = NullableInt(reader, 12),
            QueryId = NullableLong(reader, 13),
            QueryText = NullableString(reader, 14),
            RegressedPlanId = NullableLong(reader, 15),
            LastGoodPlanId = NullableLong(reader, 16),
            LastGoodPlanForcingType = NullableString(reader, 17),
            LastGoodPlanIsForced = NullableBool(reader, 18),
            LastGoodPlanForceFailureReason = NullableString(reader, 19),
            RegressedPlanExecutionCount = NullableLong(reader, 20),
            RegressedPlanCpuTimeAverageMs = NullableDouble(reader, 21),
            RegressedPlanErrorCount = NullableLong(reader, 22),
            LastGoodPlanExecutionCount = NullableLong(reader, 23),
            LastGoodPlanCpuTimeAverageMs = NullableDouble(reader, 24),
            LastGoodPlanErrorCount = NullableLong(reader, 25),
            EstimatedGainSeconds = NullableDouble(reader, 26),
            IsExecutableAction = NullableBool(reader, 27),
            IsRevertableAction = NullableBool(reader, 28),
            ExecuteActionInitiatedBy = NullableString(reader, 29),
            ExecuteActionInitiatedTime = NullableDateTime(reader, 30),
            ExecuteActionStartTime = NullableDateTime(reader, 31),
            ExecuteActionDurationSeconds = NullableDouble(reader, 32),
            RevertActionInitiatedBy = NullableString(reader, 33),
            RevertActionInitiatedTime = NullableDateTime(reader, 34),
            RevertActionStartTime = NullableDateTime(reader, 35),
            RevertActionDurationSeconds = NullableDouble(reader, 36),
            ImplementationScript = NullableString(reader, 37),
        };

    private static string? NullableString(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal).ToString();

    private static DateTime? NullableDateTime(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);

    private static int? NullableInt(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    private static long? NullableLong(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    private static double? NullableDouble(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToDouble(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    private static bool? NullableBool(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetBoolean(ordinal);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("force_last_good_plan_desired_state", CollectorColumnType.Varchar),
        new CollectorColumn("force_last_good_plan_actual_state", CollectorColumnType.Varchar),
        new CollectorColumn("force_last_good_plan_reason", CollectorColumnType.Varchar),
        new CollectorColumn("create_index_actual_state", CollectorColumnType.Varchar),
        new CollectorColumn("drop_index_actual_state", CollectorColumnType.Varchar),
        new CollectorColumn("recommendation_name", CollectorColumnType.Varchar),
        new CollectorColumn("recommendation_type", CollectorColumnType.Varchar),
        new CollectorColumn("recommendation_state", CollectorColumnType.Varchar),
        new CollectorColumn("recommendation_state_reason", CollectorColumnType.Varchar),
        new CollectorColumn("recommendation_reason", CollectorColumnType.Varchar),
        new CollectorColumn("valid_since", CollectorColumnType.Timestamp),
        new CollectorColumn("last_refresh", CollectorColumnType.Timestamp),
        new CollectorColumn("score", CollectorColumnType.Integer),
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("query_text", CollectorColumnType.Varchar),
        new CollectorColumn("regressed_plan_id", CollectorColumnType.BigInt),
        new CollectorColumn("last_good_plan_id", CollectorColumnType.BigInt),
        new CollectorColumn("last_good_plan_forcing_type", CollectorColumnType.Varchar),
        new CollectorColumn("last_good_plan_is_forced", CollectorColumnType.Boolean),
        new CollectorColumn("last_good_plan_force_failure_reason", CollectorColumnType.Varchar),
        new CollectorColumn("regressed_plan_execution_count", CollectorColumnType.BigInt),
        new CollectorColumn("regressed_plan_cpu_time_average_ms", CollectorColumnType.Double),
        new CollectorColumn("regressed_plan_error_count", CollectorColumnType.BigInt),
        new CollectorColumn("last_good_plan_execution_count", CollectorColumnType.BigInt),
        new CollectorColumn("last_good_plan_cpu_time_average_ms", CollectorColumnType.Double),
        new CollectorColumn("last_good_plan_error_count", CollectorColumnType.BigInt),
        new CollectorColumn("estimated_gain_seconds", CollectorColumnType.Double),
        new CollectorColumn("is_executable_action", CollectorColumnType.Boolean),
        new CollectorColumn("is_revertable_action", CollectorColumnType.Boolean),
        new CollectorColumn("execute_action_initiated_by", CollectorColumnType.Varchar),
        new CollectorColumn("execute_action_initiated_time", CollectorColumnType.Timestamp),
        new CollectorColumn("execute_action_start_time", CollectorColumnType.Timestamp),
        new CollectorColumn("execute_action_duration_seconds", CollectorColumnType.Double),
        new CollectorColumn("revert_action_initiated_by", CollectorColumnType.Varchar),
        new CollectorColumn("revert_action_initiated_time", CollectorColumnType.Timestamp),
        new CollectorColumn("revert_action_start_time", CollectorColumnType.Timestamp),
        new CollectorColumn("revert_action_duration_seconds", CollectorColumnType.Double),
        new CollectorColumn("implementation_script", CollectorColumnType.Varchar),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DbName)                            /* database_name VARCHAR */
            .Value(row.DesiredState)                      /* force_last_good_plan_desired_state VARCHAR */
            .Value(row.ActualState)                       /* force_last_good_plan_actual_state VARCHAR */
            .Value(row.StateReason)                       /* force_last_good_plan_reason VARCHAR — NULL when desired = actual */
            .Value(row.CreateIndexState)                  /* create_index_actual_state VARCHAR — Azure SQL DB only */
            .Value(row.DropIndexState)                    /* drop_index_actual_state VARCHAR — Azure SQL DB only */
            .Value(row.RecommendationName)                /* recommendation_name VARCHAR — NULL on the enablement-only row */
            .Value(row.RecommendationType)                /* recommendation_type VARCHAR */
            .Value(row.RecommendationState)               /* recommendation_state VARCHAR — Active/Verifying/Success/Reverted/Expired */
            .Value(row.RecommendationStateReason)         /* recommendation_state_reason VARCHAR */
            .Value(row.RecommendationReason)              /* recommendation_reason VARCHAR */
            .Value(row.ValidSince)                        /* valid_since TIMESTAMP */
            .Value(row.LastRefresh)                       /* last_refresh TIMESTAMP */
            .Value(row.Score)                             /* score INTEGER */
            .Value(row.QueryId)                           /* query_id BIGINT */
            .Value(row.QueryText)                         /* query_text VARCHAR */
            .Value(row.RegressedPlanId)                   /* regressed_plan_id BIGINT */
            .Value(row.LastGoodPlanId)                    /* last_good_plan_id BIGINT */
            .Value(row.LastGoodPlanForcingType)           /* last_good_plan_forcing_type VARCHAR — NONE/MANUAL/AUTO */
            .Value(row.LastGoodPlanIsForced)              /* last_good_plan_is_forced BOOLEAN */
            .Value(row.LastGoodPlanForceFailureReason)    /* last_good_plan_force_failure_reason VARCHAR */
            .Value(row.RegressedPlanExecutionCount)       /* regressed_plan_execution_count BIGINT */
            .Value(row.RegressedPlanCpuTimeAverageMs)     /* regressed_plan_cpu_time_average_ms DOUBLE */
            .Value(row.RegressedPlanErrorCount)           /* regressed_plan_error_count BIGINT */
            .Value(row.LastGoodPlanExecutionCount)        /* last_good_plan_execution_count BIGINT */
            .Value(row.LastGoodPlanCpuTimeAverageMs)      /* last_good_plan_cpu_time_average_ms DOUBLE */
            .Value(row.LastGoodPlanErrorCount)            /* last_good_plan_error_count BIGINT */
            .Value(row.EstimatedGainSeconds)              /* estimated_gain_seconds DOUBLE */
            .Value(row.IsExecutableAction)                /* is_executable_action BOOLEAN */
            .Value(row.IsRevertableAction)                /* is_revertable_action BOOLEAN */
            .Value(row.ExecuteActionInitiatedBy)          /* execute_action_initiated_by VARCHAR */
            .Value(row.ExecuteActionInitiatedTime)        /* execute_action_initiated_time TIMESTAMP */
            .Value(row.ExecuteActionStartTime)            /* execute_action_start_time TIMESTAMP */
            .Value(row.ExecuteActionDurationSeconds)      /* execute_action_duration_seconds DOUBLE */
            .Value(row.RevertActionInitiatedBy)           /* revert_action_initiated_by VARCHAR */
            .Value(row.RevertActionInitiatedTime)         /* revert_action_initiated_time TIMESTAMP */
            .Value(row.RevertActionStartTime)             /* revert_action_start_time TIMESTAMP */
            .Value(row.RevertActionDurationSeconds)       /* revert_action_duration_seconds DOUBLE */
            .Value(row.ImplementationScript);             /* implementation_script VARCHAR */
    }
}
