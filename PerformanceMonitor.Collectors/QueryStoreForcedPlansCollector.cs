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
/// <para><b>Load.</b> Per database one statement over <c>sys.query_store_plan</c> with <c>is_forced_plan = 1</c>,
/// bounded by <see cref="MaxRowsPerDatabase"/>, never the plan XML. A plain table on every store
/// (<see cref="StaysPlainTable"/>): the volume is a few rows per database per pass, the same grid decision as
/// the attention table (#3035, #3044, #3107).</para>
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
    private const string OnPremDatabaseListQueryText = @"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    d.name
FROM sys.databases AS d
LEFT JOIN sys.dm_hadr_database_replica_states AS drs
    ON d.database_id = drs.database_id
    AND drs.is_local = 1
WHERE d.database_id > 4
AND   d.database_id < 32761
AND   d.state_desc = N'ONLINE'
AND   d.name <> N'PerformanceMonitor'
AND   HAS_DBACCESS(d.name) = 1
AND   d.name NOT IN
      (
          N'master', N'model', N'msdb', N'tempdb',
          N'rdsadmin', N'gcloud_cloudsqladmin',
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
OPTION(RECOMPILE);";

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
    /// not the same as no forced plans). Otherwise the LEFT JOIN from a one-row anchor returns every forced plan, or
    /// exactly one all-NULL marker row when there is none. <c>is_forced_plan = 1</c> is the only filter.
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

SELECT TOP ({MaxRowsPerDatabase}) /* PerformanceMonitorLite */
    plan_id = qsp.plan_id,
    query_id = qsp.query_id,
    query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1),
    {forcingTypeColumn}
    force_failure_count = qsp.force_failure_count,
    last_force_failure_reason = NULLIF(qsp.last_force_failure_reason_desc, N'NONE'),
    last_execution_time = CONVERT(datetime2, qsp.last_execution_time AT TIME ZONE 'UTC'),
    last_compile_start_time = CONVERT(datetime2, qsp.last_compile_start_time AT TIME ZONE 'UTC')
FROM
(
    SELECT
        anchor = 1
) AS a
LEFT JOIN sys.query_store_plan AS qsp
  ON qsp.is_forced_plan = 1
ORDER BY qsp.plan_id
OPTION(RECOMPILE);";
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
            .Value(row.LastCompileStartTimeUtc);    /* last_compile_start_time TIMESTAMP, UTC */
    }
}
