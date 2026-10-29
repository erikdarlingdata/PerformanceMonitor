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
/// The plans Query Store has forced (#5632), per database, every pass. The <c>query_store</c> collector reads only
/// plans that RAN since its last pass (<c>WatermarkColumn => "last_execution_time"</c>), so a forced plan the engine
/// ignores, or that never runs, never reaches the store. This reads <c>sys.query_store_plan</c> itself, where
/// <c>is_forced_plan = 1</c>, a handful of rows per database, so the store can say whether a force works.
///
/// <para><b>One marker row per database per pass, with no plan</b> (every payload column but <c>database_name</c>
/// NULL), written even when the database has no forced plan. It is how a reader tells a force that was REMOVED (gone
/// from the newest capture of a database that was captured) from a database that was not captured. The precedent is
/// <see cref="PlanCorrectionCollector"/>'s enablement-only row.</para>
///
/// <para><b>Which databases.</b> The ones <see cref="QueryStoreCollector"/> collects: Query Store in a usable state
/// (<c>actual_state IN (1, 2, 4)</c> and not a readable secondary) and the same vendor-name screen. The state check
/// runs inside each database's own statement, so the enumeration is one cheap <c>sys.databases</c> read instead of a
/// probe cursor.</para>
///
/// <para><b>Load, and what it never reads (#6221).</b> Per database one statement over <c>sys.query_store_plan</c> alone with
/// <c>is_forced_plan = 1</c>, bounded by <see cref="MaxRowsPerDatabase"/>, then a second statement that takes each forced row's object id
/// from <c>sys.query_store_query</c>. The two views are never joined in one statement: on a primary that join took 20 s for 162 forced
/// plans in one database, against 0.6 s for this whole body (measured on a production primary; a read-only replica showed 0.1 s for the
/// join, because it has no in-memory Query Store data). It reads NO plan XML: not the column, not a <c>CHARINDEX</c> over it. #6171
/// searched the XML of every later plan of every forced query on every pass to find variants and plan guides (#6160). On one production
/// store with about 300 forced plans per server the pass took 17 to 86 s; removing the XML read alone did not fix it there, the view join
/// was most of the time. The variant check lives in the store now (<c>DarlingForcedPlanStateReader</c>): the Query Store collector already
/// fetches the XML of every plan that ran and stores it once, and the reader looks at the stored XML of the later plans that ran in the
/// window it judges. A plain table on every store (<see cref="StaysPlainTable"/>): the volume is a few rows per database per pass, the
/// same grid decision as the attention table (#3035, #3044, #3107).</para>
///
/// <para><b>Plan guides.</b> A <c>USE PLAN</c> plan guide on the forced query is read from <c>sys.plan_guides</c> in the same database: an
/// OBJECT-scoped guide by <c>scope_object_id</c> = the query's <c>object_id</c> (the guide may cover another statement of the same object:
/// the scope is the only match, no query text is compared). That reads no plan XML. A guide that a later plan names in its XML
/// (<c>PlanGuideName</c>) is found by the store-side reader. Only guides in the query's own database are seen here.</para>
///
/// <para><b>Variants.</b> <see cref="Row.VariantPlanIds"/> is always NULL now; the column stays (no migration), and the reader still
/// honors a value that an older collector wrote.</para>
///
/// <para><b>Version gates.</b> <c>plan_forcing_type_desc</c> is SQL Server 2017 and later (and always on Azure), read as
/// a typed NULL below that, the way <see cref="QueryStoreCollector"/> gates the same column.</para>
/// </summary>
public sealed class QueryStoreForcedPlansCollector : CollectorDefinitionBase<QueryStoreForcedPlansCollector.Row>
{
    public static QueryStoreForcedPlansCollector Instance { get; } = new();

    private QueryStoreForcedPlansCollector()
    {
    }

    public sealed class Row
    {
        public string DbName { get; set; } = "";

        /// <summary>NULL on the marker row.</summary>
        public long? PlanId { get; set; }
        public long? QueryId { get; set; }
        public string? QueryPlanHash { get; set; }
        public string? PlanForcingType { get; set; }
        public long? ForceFailureCount { get; set; }

        /// <summary>The engine's reason text; NULL when it says NONE.</summary>
        public string? LastForceFailureReason { get; set; }
        public DateTime? LastExecutionTimeUtc { get; set; }
        public DateTime? LastCompileStartTimeUtc { get; set; }

        /// <summary>
        /// #6160: the plan ids of the same query, newer than the forced plan, whose stored XML carries <c>UsePlan="1"</c> and no
        /// <c>PlanGuideName</c>, as a comma-separated list (<c>12,15</c>). Always NULL from this collector since #6221, which reads no plan
        /// XML on the monitored server; the store-side reader finds variants in the stored plans. Rows an older collector wrote still carry it.
        /// </summary>
        public string? VariantPlanIds { get; set; }

        /// <summary>#6160: the name of an OBJECT-scoped <c>USE PLAN</c> plan guide that covers the forced query (see the class remarks); NULL when none was found.</summary>
        public string? PlanGuideName { get; set; }

        /// <summary>#6160: NULL when no guide was found, else whether the guide is enabled.</summary>
        public bool? PlanGuideEnabled { get; set; }

        /// <summary>#6160: the guide's <c>scope_type_desc</c> (OBJECT, SQL or TEMPLATE); NULL when no guide was found.</summary>
        public string? PlanGuideScope { get; set; }
    }

    /// <summary>The most forced plans one database ships in one pass. A database with more forces than this is not a normal one.</summary>
    public const int MaxRowsPerDatabase = 500;

    /// <summary>The per-command deadline: a few rows per database should take milliseconds, so this is a backstop, not a budget.</summary>
    public const int CommandTimeoutSeconds = 120;

    public override string Name => "query_store_forced_plans";

    public override string TargetTable => "query_store_forced_plans";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>The wall clock for one server's whole pass, the plan_correction bound: a pass past it ships nothing and retries.</summary>
    public override TimeSpan? PerItemWallClockBudget => TimeSpan.FromSeconds(120);

    /// <summary>The plan table stays a plain table with a 30-day time-sliced DELETE (see the class remarks).</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin yet (the read is a Darling MCP tool).</summary>
    public override bool IsDarlingOnly => true;

    /// <summary>Query Store is 2016 (v13) and later; Azure SQL DB and Managed Instance are never version-gated; unknown (0) is assumed newest.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target is null
        || target.SqlMajorVersion == 0
        || target.SqlMajorVersion >= 13
        || target.IsAzureSqlDb
        || target.IsAzureManagedInstance;

    /// <summary>Azure SQL DB rejects three-part names, so the host connects per database there instead.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target is not null && target.IsAzureSqlDb;

    /// <summary>The same databases <see cref="QueryStoreCollector"/> enumerates, minus its per-database state probe (the per-database statement checks it).</summary>
    internal const string OnPremDatabaseListQueryText = @"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

" + NoDatabaseAccessNote.ScopeProjection + @"
FROM sys.databases AS d
LEFT JOIN sys.dm_hadr_database_replica_states AS drs
    ON d.database_id = drs.database_id
    AND drs.is_local = 1
WHERE d.database_id > 4
AND   d.database_id < 32761
AND   d.state_desc = N'ONLINE'
AND   d.name <> N'PerformanceMonitor'
AND   d.name NOT IN
      (
          N'master', N'model', N'msdb', N'tempdb',
          " + DatabaseScopeFilter.VendorControlledDatabaseNames + @",
          N'ReportServer', N'ReportServerTempDB',
          N'DWConfiguration', N'DWDiagnostics', N'DWQueue',
          N'DBAUtil', N'DBAUtils', N'Utility'
      )
AND
(
    drs.database_id IS NULL
    OR drs.is_primary_replica = 1
)
/*EXCLUSION_FILTER*/
ORDER BY d.name
OPTION(RECOMPILE);" + NoDatabaseAccessNote.ScopeResults;

    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Target.IsAzureSqlDb)
        {
            return null;
        }

        var (exclusionClause, exclusionParameters) = DatabaseScopeFilter.BuildEnumerationPredicate(context, "d.name");
        var text = OnPremDatabaseListQueryText
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    public override CollectorQuery? BuildEnumerationProbe(CollectorContext context)
        => new(QueryStoreCollector.ProductVersionProbeText);

    /// <summary>
    /// The per-database read, one body for both paths (the on-prem path quote-doubles it into
    /// <c>[db].sys.sp_executesql</c>, Azure runs it as-is). The eligibility gate returns NO result set for a database
    /// whose Query Store is off, in error or a readable secondary, so such a database writes nothing (not captured is
    /// not the same as no forced plans). Otherwise it returns ONE all-NULL marker row, always, then every forced plan
    /// (the marker is how a reader tells the capture happened). <c>is_forced_plan = 1</c> is the only filter.
    /// </summary>
    internal static string BuildPayloadBody(CollectorContext context)
    {
        var productVersion = context.EnumerationProbeResult is null
            ? QueryStoreCollector.DefaultProductVersion
            : Convert.ToInt32(context.EnumerationProbeResult, CultureInfo.InvariantCulture);
        var hasForcingType = productVersion > 13 || context.Target.IsAzureSqlDb || context.Target.IsAzureManagedInstance;
        var forcingTypeColumn = hasForcingType
            ? "plan_forcing_type = qsp.plan_forcing_type_desc,"
            : "plan_forcing_type = CONVERT(nvarchar(60), NULL),";

        return $@"
SET NOCOUNT ON;

IF NOT EXISTS
(
    SELECT
        1
    FROM sys.database_query_store_options
    WHERE actual_state IN (1, 2, 4)
    AND   readonly_reason & 8 = 0
)
BEGIN
    RETURN;
END;

DROP TABLE IF EXISTS #pm_forced_plans;

SELECT TOP ({MaxRowsPerDatabase})
    plan_id = qsp.plan_id,
    query_id = qsp.query_id,
    object_id = CONVERT(bigint, NULL),
    query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1),
    {forcingTypeColumn}
    force_failure_count = CONVERT(bigint, qsp.force_failure_count),
    last_force_failure_reason = NULLIF(qsp.last_force_failure_reason_desc, N'NONE'),
    last_execution_time = CONVERT(datetime2, qsp.last_execution_time AT TIME ZONE 'UTC'),
    last_compile_start_time = CONVERT(datetime2, qsp.last_compile_start_time AT TIME ZONE 'UTC')
INTO #pm_forced_plans
FROM sys.query_store_plan AS qsp
WHERE qsp.is_forced_plan = 1
ORDER BY qsp.plan_id
OPTION(RECOMPILE);

/* #6221: the object id comes from sys.query_store_query in a SECOND statement, joined to the few forced rows in the temp table. On a
   primary, joining the two Query Store views in one statement took 20 s for 162 forced plans in one database (the same rows in
   0.6 s for the whole body when split), and a read-only replica does not show it, because it has no in-memory Query Store data. */
UPDATE f
SET f.object_id = CONVERT(bigint, qsq.object_id)
FROM #pm_forced_plans AS f
JOIN sys.query_store_query AS qsq
    ON qsq.query_id = f.query_id
OPTION(RECOMPILE);

SELECT /* PerformanceMonitorLite */
    plan_id = CONVERT(bigint, NULL),
    query_id = CONVERT(bigint, NULL),
    query_plan_hash = CONVERT(varchar(64), NULL),
    plan_forcing_type = CONVERT(nvarchar(60), NULL),
    force_failure_count = CONVERT(bigint, NULL),
    last_force_failure_reason = CONVERT(nvarchar(60), NULL),
    last_execution_time = CONVERT(datetime2, NULL),
    last_compile_start_time = CONVERT(datetime2, NULL),
    variant_plan_ids = CONVERT(varchar(400), NULL),
    plan_guide_name = CONVERT(nvarchar(128), NULL),
    plan_guide_enabled = CONVERT(bit, NULL),
    plan_guide_scope = CONVERT(nvarchar(60), NULL)
UNION ALL
SELECT
    f.plan_id,
    f.query_id,
    f.query_plan_hash,
    f.plan_forcing_type,
    f.force_failure_count,
    f.last_force_failure_reason,
    f.last_execution_time,
    f.last_compile_start_time,
    variant_plan_ids = CONVERT(varchar(400), NULL),
    g.plan_guide_name,
    g.plan_guide_enabled,
    g.plan_guide_scope
FROM #pm_forced_plans AS f
OUTER APPLY
(
    SELECT TOP (1)
        plan_guide_name = pg.name,
        plan_guide_enabled = CONVERT(bit, CASE WHEN pg.is_disabled = 1 THEN 0 ELSE 1 END),
        plan_guide_scope = pg.scope_type_desc
    FROM sys.plan_guides AS pg
    WHERE pg.scope_type_desc = N'OBJECT'
    AND   f.object_id <> 0
    AND   pg.scope_object_id = f.object_id
    AND   pg.hints LIKE N'%USE PLAN%'
    ORDER BY
        CASE WHEN pg.is_disabled = 0 THEN 0 ELSE 1 END,
        pg.name
) AS g
ORDER BY
    plan_id
OPTION(RECOMPILE);

DROP TABLE IF EXISTS #pm_forced_plans;";
    }

    /// <summary>The Azure per-database branch: the connection is already in the database.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException(
                "query_store_forced_plans enumerates databases off Azure SQL DB; BuildEnumerationQuery drives the cycle.");
        }

        return new CollectorQuery(BuildPayloadBody(context));
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(context);

        var text = $@"
EXECUTE [{item.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql
    N'{BuildPayloadBody(context).Replace("'", "''", StringComparison.Ordinal)}'";

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

    /// <summary>Ordinals follow the SELECT list of <see cref="BuildPayloadBody"/>.</summary>
    private static Row ReadRow(DbDataReader reader, string databaseName) =>
        new()
        {
            DbName = databaseName,
            PlanId = NullableLong(reader, 0),
            QueryId = NullableLong(reader, 1),
            QueryPlanHash = NullableString(reader, 2),
            PlanForcingType = NullableString(reader, 3),
            ForceFailureCount = NullableLong(reader, 4),
            LastForceFailureReason = NullableString(reader, 5),
            LastExecutionTimeUtc = NullableDateTime(reader, 6),
            LastCompileStartTimeUtc = NullableDateTime(reader, 7),
            VariantPlanIds = NullableString(reader, 8),
            PlanGuideName = NullableString(reader, 9),
            PlanGuideEnabled = reader.IsDBNull(10) ? null : Convert.ToBoolean(reader.GetValue(10), CultureInfo.InvariantCulture),
            PlanGuideScope = NullableString(reader, 11),
        };

    private static string? NullableString(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal).ToString();

    private static DateTime? NullableDateTime(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);

    private static long? NullableLong(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("plan_id", CollectorColumnType.BigInt),
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("query_plan_hash", CollectorColumnType.Varchar),
        new CollectorColumn("plan_forcing_type", CollectorColumnType.Varchar),
        new CollectorColumn("force_failure_count", CollectorColumnType.BigInt),
        new CollectorColumn("last_force_failure_reason", CollectorColumnType.Varchar),
        new CollectorColumn("last_execution_time", CollectorColumnType.Timestamp),
        new CollectorColumn("last_compile_start_time", CollectorColumnType.Timestamp),
        /* V201 (#6160), appended last: the writer is a positional binary COPY. */
        new CollectorColumn("variant_plan_ids", CollectorColumnType.Varchar),
        new CollectorColumn("plan_guide_name", CollectorColumnType.Varchar),
        new CollectorColumn("plan_guide_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("plan_guide_scope", CollectorColumnType.Varchar),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DbName)                      /* database_name VARCHAR */
            .Value(row.PlanId)                      /* plan_id BIGINT, NULL on the marker row */
            .Value(row.QueryId)                     /* query_id BIGINT */
            .Value(row.QueryPlanHash)               /* query_plan_hash VARCHAR */
            .Value(row.PlanForcingType)             /* plan_forcing_type VARCHAR, NULL below SQL Server 2017 */
            .Value(row.ForceFailureCount)           /* force_failure_count BIGINT */
            .Value(row.LastForceFailureReason)      /* last_force_failure_reason VARCHAR, NULL when NONE */
            .Value(row.LastExecutionTimeUtc)        /* last_execution_time TIMESTAMP, UTC */
            .Value(row.LastCompileStartTimeUtc)     /* last_compile_start_time TIMESTAMP, UTC */
            .Value(row.VariantPlanIds)              /* variant_plan_ids VARCHAR, "12,15" form, NULL when none */
            .Value(row.PlanGuideName)               /* plan_guide_name VARCHAR, NULL when no guide covers the forced query */
            .Value(row.PlanGuideEnabled)            /* plan_guide_enabled BOOLEAN */
            .Value(row.PlanGuideScope);             /* plan_guide_scope VARCHAR: OBJECT, SQL or TEMPLATE */
    }
}
