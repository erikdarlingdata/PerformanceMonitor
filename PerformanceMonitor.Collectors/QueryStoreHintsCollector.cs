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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The Query Store hints in place (#6581), per database, every pass: <c>sys.query_store_query_hints</c>, SQL Server 2022 and later and
/// Azure SQL. A hint (<c>sys.sp_query_store_set_hints</c>) changes a query's plan just as a forced plan does, and nothing else Darling
/// collects shows one being set or cleared.
///
/// <para><b>One marker row per database per pass, with no hint</b> (every payload column but <c>database_name</c> NULL), written even
/// when the database has no hint: the same shape as <see cref="QueryStoreForcedPlansCollector"/>, for the same reason. It is how a
/// reader tells a hint that was CLEARED (gone from the newest capture of a database that was captured) from a database that was not
/// captured.</para>
///
/// <para><b>Which databases</b> are the forced-plans collector's: Query Store in a usable state (<c>actual_state IN (1, 2, 4)</c> and not
/// a readable secondary), the same vendor-name screen and the same database scope. A database whose Query Store is off, in error or a
/// readable secondary, or one that has no <c>sys.query_store_query_hints</c> (a Managed Instance on an older update policy), returns NO
/// result set: it writes nothing, and nothing says it was captured.</para>
///
/// <para><b>Version gate.</b> Before SQL Server 2022 (major 16) the view does not exist, so the collector does not run there
/// (<see cref="AppliesTo"/>): no query reaches the server and no error is logged. Azure SQL Database and Managed Instance are never
/// version-gated here; a Managed Instance without the view is caught by the per-database existence check.</para>
///
/// <para><b>Load.</b> One statement per database over <c>sys.query_store_query_hints</c> alone, bounded by
/// <see cref="MaxRowsPerDatabase"/>, never joined to another Query Store view (#6221). The hint text is the one user-written string, passed
/// through the statement scrub like every collected statement. A plain table on every store (<see cref="StaysPlainTable"/>): the volume is
/// a few rows per database per pass.</para>
/// </summary>
public sealed class QueryStoreHintsCollector : CollectorDefinitionBase<QueryStoreHintsCollector.Row>
{
    public static QueryStoreHintsCollector Instance { get; } = new();

    private QueryStoreHintsCollector()
    {
    }

    public sealed class Row
    {
        public string DbName { get; set; } = "";

        /// <summary>NULL on the marker row.</summary>
        public long? QueryHintId { get; set; }
        public long? QueryId { get; set; }
        public string? QueryHintText { get; set; }

        /// <summary>The engine's reason text; NULL when it says NONE.</summary>
        public string? LastFailureReason { get; set; }
        public long? FailureCount { get; set; }

        /// <summary>The engine's <c>source_desc</c>: where the hint came from (for example USER).</summary>
        public string? Source { get; set; }
    }

    /// <summary>The most hints one database ships in one pass. A database with more hints than this is not a normal one.</summary>
    public const int MaxRowsPerDatabase = 2000;

    /// <summary>The per-command deadline: a few rows per database should take milliseconds, so this is a backstop, not a budget.</summary>
    public const int CommandTimeoutSeconds = 120;

    /// <summary>SQL Server 2022.</summary>
    public const int FirstMajorVersion = 16;

    public override string Name => "query_store_hints";

    public override string TargetTable => "query_store_hints";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>The wall clock for one server's whole pass, the forced-plans bound: a pass past it ships nothing and retries.</summary>
    public override TimeSpan? PerItemWallClockBudget => TimeSpan.FromSeconds(120);

    /// <summary>A plain table with a time-sliced DELETE, the forced-plans decision (a few rows per database per pass).</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin (ruling 10-21: new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    /// <summary>The view is SQL Server 2022 (v16) and later; Azure SQL DB and Managed Instance are not version-gated; unknown (0) is assumed newest.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target is null
        || target.SqlMajorVersion == 0
        || target.SqlMajorVersion >= FirstMajorVersion
        || target.IsAzureSqlDb
        || target.IsAzureManagedInstance;

    /// <summary>Azure SQL DB rejects three-part names, so the host connects per database there instead.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target is not null && target.IsAzureSqlDb;

    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Target.IsAzureSqlDb)
        {
            return null;
        }

        var (exclusionClause, exclusionParameters) = DatabaseScopeFilter.BuildEnumerationPredicate(context, "d.name");
        var text = QueryStoreForcedPlansCollector.OnPremDatabaseListQueryText
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    /// <summary>
    /// The per-database read, one body for both paths (the on-prem path quote-doubles it into <c>[db].sys.sp_executesql</c>, Azure runs it
    /// as-is). The eligibility gate returns NO result set for a database whose Query Store is off, in error or a readable secondary, or
    /// that has no hints view. Otherwise it returns ONE all-NULL marker row, always, then every hint.
    /// </summary>
    internal static string BuildPayloadBody() => $@"
SET NOCOUNT ON;

IF OBJECT_ID(N'sys.query_store_query_hints', N'V') IS NULL
BEGIN
    RETURN;
END;

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

SELECT /* PerformanceMonitorLite */
    query_hint_id = CONVERT(bigint, NULL),
    query_id = CONVERT(bigint, NULL),
    query_hint_text = CONVERT(nvarchar(max), NULL),
    last_query_hint_failure_reason = CONVERT(nvarchar(128), NULL),
    query_hint_failure_count = CONVERT(bigint, NULL),
    source = CONVERT(nvarchar(60), NULL)
UNION ALL
SELECT
    query_hint_id = h.query_hint_id,
    query_id = h.query_id,
    query_hint_text = h.query_hint_text,
    last_query_hint_failure_reason = NULLIF(h.last_query_hint_failure_reason_desc, N'NONE'),
    query_hint_failure_count = CONVERT(bigint, h.query_hint_failure_count),
    source = h.source_desc
FROM
(
    SELECT TOP ({MaxRowsPerDatabase})
        qh.query_hint_id,
        qh.query_id,
        qh.query_hint_text,
        qh.last_query_hint_failure_reason_desc,
        qh.query_hint_failure_count,
        qh.source_desc
    FROM sys.query_store_query_hints AS qh
    ORDER BY qh.query_id, qh.query_hint_id
) AS h
ORDER BY
    query_id,
    query_hint_id
OPTION(RECOMPILE);";

    /// <summary>The Azure per-database branch: the connection is already in the database.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException(
                "query_store_hints enumerates databases off Azure SQL DB; BuildEnumerationQuery drives the cycle.");
        }

        return new CollectorQuery(BuildPayloadBody());
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(context);

        var text = $@"
EXECUTE [{item.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql
    N'{BuildPayloadBody().Replace("'", "''", StringComparison.Ordinal)}'";

        return new CollectorQuery(text);
    }

    public override async ValueTask ReadItemAsync(string item, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(context);

        var scrub = context.BeginStatementScrub();
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadRow(reader, item, scrub));
        }
    }

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(context);

        var rows = new List<Row>();
        var databaseName = context.CurrentDatabaseName ?? "";
        var scrub = context.BeginStatementScrub();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadRow(reader, databaseName, scrub));
        }

        return rows;
    }

    /// <summary>Ordinals follow the SELECT list of <see cref="BuildPayloadBody"/>.</summary>
    private static Row ReadRow(DbDataReader reader, string databaseName, SensitiveStatements.Session scrub) =>
        new()
        {
            DbName = databaseName,
            QueryHintId = NullableLong(reader, 0),
            QueryId = NullableLong(reader, 1),
            QueryHintText = scrub.Text(NullableString(reader, 2)),
            LastFailureReason = NullableString(reader, 3),
            FailureCount = NullableLong(reader, 4),
            Source = NullableString(reader, 5),
        };

    private static string? NullableString(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal).ToString();

    private static long? NullableLong(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("query_hint_id", CollectorColumnType.BigInt),
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("query_hint_text", CollectorColumnType.Varchar),
        new CollectorColumn("last_query_hint_failure_reason", CollectorColumnType.Varchar),
        new CollectorColumn("query_hint_failure_count", CollectorColumnType.BigInt),
        new CollectorColumn("source_desc", CollectorColumnType.Varchar),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DbName)                      /* database_name VARCHAR */
            .Value(row.QueryHintId)                 /* query_hint_id BIGINT, NULL on the marker row */
            .Value(row.QueryId)                     /* query_id BIGINT */
            .Value(row.QueryHintText)               /* query_hint_text VARCHAR, through the statement scrub */
            .Value(row.LastFailureReason)           /* last_query_hint_failure_reason VARCHAR, NULL when NONE */
            .Value(row.FailureCount)                /* query_hint_failure_count BIGINT */
            .Value(row.Source);                     /* source_desc VARCHAR */
    }
}
