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
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// How full the identity columns are (#6875), per database, every six hours: <c>sys.identity_columns</c> joined to <c>sys.objects</c>
/// (user objects only), for <c>tinyint</c>, <c>smallint</c>, <c>int</c>, <c>bigint</c> and <c>decimal(p,0)</c>. An identity column that
/// runs out stops every insert into its table with an arithmetic overflow, and nothing else Darling collects warns before it does.
///
/// <para><b>Rows.</b> Per scanned database, the top <see cref="IdentityCapacityMath.MaxColumnsPerDatabase"/> columns that are more than
/// <see cref="IdentityCapacityMath.MinimumPercentKept"/> percent used, plus ONE marker row (every payload column but <c>database_name</c>
/// NULL), written even when no column is high: "scanned, nothing high" is a stored answer, where a database that was not scanned has no
/// marker and never reads as clean. The query ships the marker only when it ran with the right to see the catalog
/// (see <see cref="BuildPayloadBody"/>).</para>
///
/// <para><b>Which databases</b> are the Query Store collectors' (online, not a snapshot, not a secondary replica, not a system or vendor
/// database, inside the server's database scope), with the snapshot screen added: a snapshot repeats its source's identity values.</para>
///
/// <para><b>Load.</b> Catalog views only, READ UNCOMMITTED, a short lock timeout and a command timeout. The database prunes to its highest
/// <see cref="IdentityCapacityMath.MaxColumnsPerDatabase"/> candidates before anything crosses the wire, using float arithmetic that only
/// has to be close: the stored percent comes from <see cref="IdentityCapacityMath"/>, and <see cref="KeepTop"/> makes the final cut on
/// it. Each database has its own <see cref="PerItemWallClockBudget"/>: one that runs past it ships no rows, so no marker.</para>
///
/// <para><b>Bounded for hundreds of databases.</b> The whole pass has a <see cref="PassWallClockBudget"/> too. A pass that spends it starts no
/// further database, and the next pass resumes at the database after the last one started and wraps (<see cref="PassBudget"/>). A database a
/// pass did not reach writes nothing, marker included, so "no marker" still means "not scanned" and never "scanned clean".</para>
///
/// <para>A plain table on every store (<see cref="StaysPlainTable"/>): at most 21 rows per database per pass.</para>
/// </summary>
public sealed class IdentityCapacityCollector : CollectorDefinitionBase<IdentityCapacityCollector.Row>
{
    public static IdentityCapacityCollector Instance { get; } = new();

    private IdentityCapacityCollector()
    {
    }

    public sealed class Row
    {
        public string DbName { get; set; } = "";

        /// <summary>NULL on the marker row, as is everything below.</summary>
        public string? SchemaName { get; set; }
        public string? TableName { get; set; }
        public string? ColumnName { get; set; }
        public string? DataType { get; set; }
        public int? TypePrecision { get; set; }

        /// <summary>NULL on a marker row, and on a column with no row inserted yet.</summary>
        public BigInteger? LastValue { get; set; }
        public BigInteger? SeedValue { get; set; }
        public BigInteger? IncrementValue { get; set; }

        /// <summary>From <see cref="IdentityCapacityMath"/>; NULL on the marker row.</summary>
        public double? PercentUsed { get; set; }

        public bool IsMarker => ColumnName is null;
    }

    /// <summary>The per-command deadline: a catalog read that is not done in this long is not going to be.</summary>
    public const int CommandTimeoutSeconds = 120;

    /// <summary>The lock wait the monitored server gets before the read gives up on a database: a metadata lock held by DDL must not queue this.</summary>
    public const int LockTimeoutMilliseconds = 5000;

    /// <summary>The percent a candidate must pass in the database's float arithmetic: below the final 10, so rounding there never drops a column the exact math keeps.</summary>
    internal const string CandidatePercentFloor = "9.9";

    public override string Name => "identity_capacity";

    public override string TargetTable => "identity_capacity";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>Each database's own wall clock, the Query Store hints collector's: a database past it ships nothing, so it has no marker.</summary>
    public override TimeSpan? PerItemWallClockBudget => TimeSpan.FromSeconds(120);

    /// <summary>The whole pass's wall clock (#6875). A pass that spends it stops starting databases and the next pass resumes at the database after the last one started.</summary>
    public override TimeSpan? PassWallClockBudget => TimeSpan.FromMinutes(5);

    /// <summary>A plain table with a time-sliced DELETE, the hints decision (at most 21 rows per database per pass).</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin (ruling 10-21: new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    /// <summary>Catalog views exist on every SQL Server version and platform this supports.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>Azure SQL DB rejects three-part names, so the host connects per database there instead.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target is not null && target.IsAzureSqlDb;

    /// <summary>
    /// The system, vendor and service databases <see cref="OnPremDatabaseListQueryText"/> leaves out by name, so the reader that lists a
    /// server's not-yet-scanned databases from the database config capture skips the same ones (it must not call a database "not yet
    /// scanned" that the collector never scans). Keep the two lists equal.
    /// </summary>
    public static readonly IReadOnlyList<string> SkippedDatabaseNames = new[]
    {
        "master", "model", "msdb", "tempdb", "PerformanceMonitor", "rdsadmin", "gcloud_cloudsqladmin",
        "ReportServer", "ReportServerTempDB", "DWConfiguration", "DWDiagnostics", "DWQueue", "DBAUtil", "DBAUtils", "Utility",
    };

    /// <summary>
    /// The Query Store collectors' list (online, vendor and system databases out, secondaries out), plus the snapshot screen
    /// (<c>source_database_id IS NULL</c>). A restoring database is not ONLINE, so it is out with the offline ones.
    /// </summary>
    internal const string OnPremDatabaseListQueryText = @"
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
AND   d.source_database_id IS NULL
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

    /// <summary>
    /// The per-database read, one body for both paths (the on-prem path quote-doubles it into <c>[db].sys.sp_executesql</c>, Azure runs it
    /// as-is). Identity metadata is HIDDEN without VIEW DEFINITION (a catalog view hides rows, it does not fail), so a login without it
    /// would see no identity column anywhere and every database would read "scanned, clean". The gate returns NO result set then: no
    /// marker, and the database reads as not scanned. Otherwise ONE all-NULL marker row, then the highest candidates.
    ///
    /// <para>The candidate cut is float arithmetic in the same shape as <see cref="IdentityCapacityMath.Calculate"/> (capacity from the seed
    /// toward the end the increment moves to), with a lower bar than the final 10 percent so a rounding difference never drops a column
    /// the exact math keeps. Values cross as text: a <c>decimal(38,0)</c> value does not fit a CLR decimal.</para>
    /// </summary>
    internal static string BuildPayloadBody() => $@"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT {LockTimeoutMilliseconds};

IF HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DEFINITION') <> 1
BEGIN
    RETURN;
END;

SELECT /* PerformanceMonitorLite */
    schema_name = CONVERT(nvarchar(128), NULL),
    table_name = CONVERT(nvarchar(128), NULL),
    column_name = CONVERT(nvarchar(128), NULL),
    data_type = CONVERT(nvarchar(128), NULL),
    type_precision = CONVERT(integer, NULL),
    last_value = CONVERT(varchar(40), NULL),
    seed_value = CONVERT(varchar(40), NULL),
    increment_value = CONVERT(varchar(40), NULL)
UNION ALL
SELECT
    c.schema_name,
    c.table_name,
    c.column_name,
    c.data_type,
    c.type_precision,
    c.last_value,
    c.seed_value,
    c.increment_value
FROM
(
    SELECT TOP ({IdentityCapacityMath.MaxColumnsPerDatabase})
        schema_name = s.name,
        table_name = o.name,
        column_name = ic.name,
        data_type = TYPE_NAME(ic.system_type_id),
        type_precision = CONVERT(integer, ic.precision),
        last_value = CONVERT(varchar(40), CONVERT(decimal(38, 0), ic.last_value)),
        seed_value = CONVERT(varchar(40), CONVERT(decimal(38, 0), ic.seed_value)),
        increment_value = CONVERT(varchar(40), CONVERT(decimal(38, 0), ic.increment_value)),
        percent_used = 100 * u.used / NULLIF(u.total, 0)
    FROM sys.identity_columns AS ic
    JOIN sys.objects AS o
        ON o.object_id = ic.object_id
    JOIN sys.schemas AS s
        ON s.schema_id = o.schema_id
    CROSS APPLY
    (
        SELECT
            low = CASE ic.system_type_id
                      WHEN 48 THEN CONVERT(float, 0)
                      WHEN 52 THEN CONVERT(float, -32768)
                      WHEN 56 THEN CONVERT(float, -2147483648)
                      WHEN 127 THEN CONVERT(float, -9223372036854775808)
                      ELSE -(POWER(CONVERT(float, 10), ic.precision) - 1)
                  END,
            high = CASE ic.system_type_id
                       WHEN 48 THEN CONVERT(float, 255)
                       WHEN 52 THEN CONVERT(float, 32767)
                       WHEN 56 THEN CONVERT(float, 2147483647)
                       WHEN 127 THEN CONVERT(float, 9223372036854775807)
                       ELSE POWER(CONVERT(float, 10), ic.precision) - 1
                   END,
            seed = CONVERT(float, ic.seed_value),
            step = CONVERT(float, ic.increment_value),
            last = CONVERT(float, ic.last_value)
    ) AS b
    CROSS APPLY
    (
        SELECT
            total = (CASE WHEN b.step > 0 THEN b.high - b.seed ELSE b.seed - b.low END) / ABS(b.step) + 1,
            used = CASE
                       WHEN b.last IS NULL
                       THEN 0
                       ELSE (CASE WHEN b.step > 0 THEN b.last - b.seed ELSE b.seed - b.last END) / ABS(b.step) + 1
                   END
    ) AS u
    WHERE o.is_ms_shipped = 0
    AND   ic.system_type_id IN (48, 52, 56, 127, 106, 108)
    AND   100 * u.used / NULLIF(u.total, 0) > {CandidatePercentFloor}
    ORDER BY
        percent_used DESC,
        s.name,
        o.name,
        ic.name
) AS c
OPTION(RECOMPILE);";

    /// <summary>The Azure per-database branch: the connection is already in the database.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException(
                "identity_capacity enumerates databases off Azure SQL DB; BuildEnumerationQuery drives the cycle.");
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

        rows.AddRange(await ReadDatabaseAsync(reader, item, cancellationToken));
    }

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(context);

        return await ReadDatabaseAsync(reader, context.CurrentDatabaseName ?? "", cancellationToken);
    }

    /// <summary>
    /// One database's result set to its rows. A result set with no marker row is a database the query did not scan (no catalog right), so
    /// it yields nothing: not even a marker.
    /// </summary>
    private static async Task<List<Row>> ReadDatabaseAsync(DbDataReader reader, string databaseName, CancellationToken cancellationToken)
    {
        var sawMarker = false;
        var candidates = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = ReadRow(reader, databaseName);
            if (row.IsMarker)
            {
                sawMarker = true;
            }
            else
            {
                candidates.Add(row);
            }
        }

        var rows = new List<Row>();
        if (sawMarker)
        {
            rows.Add(new Row { DbName = databaseName });
            rows.AddRange(KeepTop(candidates));
        }

        return rows;
    }

    /// <summary>
    /// The final cut: each candidate's percent from <see cref="IdentityCapacityMath"/>, those above
    /// <see cref="IdentityCapacityMath.MinimumPercentKept"/> percent, the highest <see cref="IdentityCapacityMath.MaxColumnsPerDatabase"/>,
    /// fullest first. A candidate whose type or numbers have no capacity is dropped.
    /// </summary>
    internal static List<Row> KeepTop(IEnumerable<Row> candidates)
    {
        ArgumentNullException.ThrowIfNull(candidates);

        var kept = new List<Row>();
        foreach (var row in candidates)
        {
            if (row.DataType is null || row.SeedValue is null || row.IncrementValue is null)
            {
                continue;
            }

            var capacity = IdentityCapacityMath.Calculate(
                row.DataType, row.TypePrecision ?? 0, row.SeedValue.Value, row.IncrementValue.Value, row.LastValue);
            if (capacity is not { } c || c.PercentUsed <= IdentityCapacityMath.MinimumPercentKept)
            {
                continue;
            }

            row.PercentUsed = c.PercentUsed;
            kept.Add(row);
        }

        return kept
            .OrderByDescending(r => r.PercentUsed)
            .ThenBy(r => r.SchemaName, StringComparer.Ordinal)
            .ThenBy(r => r.TableName, StringComparer.Ordinal)
            .ThenBy(r => r.ColumnName, StringComparer.Ordinal)
            .Take(IdentityCapacityMath.MaxColumnsPerDatabase)
            .ToList();
    }

    /// <summary>Ordinals follow the SELECT list of <see cref="BuildPayloadBody"/>.</summary>
    private static Row ReadRow(DbDataReader reader, string databaseName) =>
        new()
        {
            DbName = databaseName,
            SchemaName = NullableString(reader, 0),
            TableName = NullableString(reader, 1),
            ColumnName = NullableString(reader, 2),
            DataType = NullableString(reader, 3),
            TypePrecision = reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture),
            LastValue = NullableBigInteger(reader, 5),
            SeedValue = NullableBigInteger(reader, 6),
            IncrementValue = NullableBigInteger(reader, 7),
        };

    private static string? NullableString(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal).ToString();

    private static BigInteger? NullableBigInteger(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal)
            ? null
            : BigInteger.Parse(reader.GetValue(ordinal).ToString()!, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("column_name", CollectorColumnType.Varchar),
        new CollectorColumn("data_type", CollectorColumnType.Varchar),
        new CollectorColumn("type_precision", CollectorColumnType.Integer),
        new CollectorColumn("last_value", CollectorColumnType.Decimal, 38, 0),
        new CollectorColumn("seed_value", CollectorColumnType.Decimal, 38, 0),
        new CollectorColumn("increment_value", CollectorColumnType.Decimal, 38, 0),
        new CollectorColumn("percent_used", CollectorColumnType.Double),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DbName)                      /* database_name VARCHAR */
            .Value(row.SchemaName)                  /* schema_name VARCHAR, NULL on the marker row */
            .Value(row.TableName)                   /* table_name VARCHAR */
            .Value(row.ColumnName)                  /* column_name VARCHAR */
            .Value(row.DataType)                    /* data_type VARCHAR */
            .Value(row.TypePrecision)               /* type_precision INTEGER */
            .Value(ToDecimal(row.LastValue))        /* last_value NUMERIC(38,0), NULL when no row was inserted yet */
            .Value(ToDecimal(row.SeedValue))        /* seed_value NUMERIC(38,0) */
            .Value(ToDecimal(row.IncrementValue))   /* increment_value NUMERIC(38,0) */
            .Value(row.PercentUsed);                /* percent_used DOUBLE PRECISION */
    }

    /// <summary>
    /// A CLR decimal holds about 7.9e28, below a <c>decimal(38,0)</c> maximum. A value past it (a seed chosen near the top of a wide
    /// <c>decimal</c> type) stores NULL: <c>percent_used</c> is already computed from the exact value and is what a reader sorts on.
    /// </summary>
    private static decimal? ToDecimal(BigInteger? value) =>
        value is { } v && v >= (BigInteger)decimal.MinValue && v <= (BigInteger)decimal.MaxValue ? (decimal)v : null;
}
