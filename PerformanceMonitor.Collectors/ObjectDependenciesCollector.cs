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
/// The dependency list of each database (#8784 part 2), once a day: which objects each view, module and trigger names in its definition,
/// from <c>sys.sql_expression_dependencies</c>. <c>get_object_usage</c> walks it from everything Query Store shows in use, so a view used only
/// through another view, or through an inline function, is not called unused: neither leaves its own name in any statement's text.
///
/// <para><b>Which databases</b> are <see cref="IndexObjectStatsCollector"/>'s, as for <see cref="ObjectInventoryCollector"/>: its enumeration
/// statement is reused as it is, and the host resolves this collector's #3477 scope through <see cref="DatabaseScopeFilter.ScopeOwner"/>.</para>
///
/// <para><b>Which rows.</b> Object-to-object references (<c>referencing_class</c> and <c>referenced_class</c> 1) from user objects
/// (<c>is_ms_shipped = 0</c>), one row per (referencing object, referenced name): a column-level row of a schema-bound reference adds nothing
/// to the object it is on, so the rows are DISTINCT without the minor ids. Where SQL Server bound the name (<c>referenced_id</c> is not
/// null) the bound object's own schema and name are shipped, so an unqualified reference arrives with its real schema. The rows that never
/// name a user object (XML type method artifacts, <c>sys</c>, <c>inserted</c>/<c>deleted</c>) are shipped and left out at read time by
/// <c>ObjectDependencyFilter</c>, which is one tested place for the rule.</para>
///
/// <para><b>Synonyms</b> are never the referencing object in <c>sys.sql_expression_dependencies</c>, so a statement naming a synonym would
/// reach nothing (#8784 review). Each synonym ships as one more row, type <c>SN</c>, its <c>base_object_name</c> split by <c>PARSENAME</c>,
/// so the same filter and walk carry use of the synonym on to its target, in this database or (by three-part name) another.</para>
///
/// <para><b>Load.</b> One catalog read per database per day, bounded by <see cref="MaxRowsPerDatabase"/>. A plain table on every store
/// (<see cref="StaysPlainTable"/>), kept 3 days (the schedule's retention): only the newest capture is read, and the purge keeps a database's newest list however old.</para>
/// </summary>
public sealed class ObjectDependenciesCollector : CollectorDefinitionBase<ObjectDependenciesCollector.Row>
{
    public static ObjectDependenciesCollector Instance { get; } = new();

    private ObjectDependenciesCollector()
    {
    }

    /// <summary>One reference.</summary>
    public sealed class Row
    {
        public string DatabaseName { get; set; } = "";
        public string ReferencingSchema { get; set; } = "";
        public string ReferencingName { get; set; } = "";
        public string ReferencingType { get; set; } = "";
        public string? ReferencedServer { get; set; }
        public string? ReferencedDatabase { get; set; }
        public string? ReferencedSchema { get; set; }
        public string ReferencedName { get; set; } = "";
        public bool IsBound { get; set; }
        public bool IsCallerDependent { get; set; }
        public bool IsAmbiguous { get; set; }
    }

    /// <summary>The most references one database ships in one pass. Far above a normal database's count.</summary>
    public const int MaxRowsPerDatabase = 200_000;

    /// <summary>The per-command deadline: a catalog read, so this is a backstop.</summary>
    public const int CommandTimeoutSeconds = 120;

    public override string Name => "object_dependencies";

    public override string TargetTable => "object_dependencies";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>A plain table with a time-sliced DELETE: the newest capture is the only one read.</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin (new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    /// <summary>Azure SQL DB rejects three-part names, so the host connects per database there instead.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target is not null && target.IsAzureSqlDb;

    /// <summary>The index and object stats enumeration statement, under <c>index_object_stats</c>'s scope
    /// (<see cref="DatabaseScopeFilter.ScopeOwner"/>), so the dependency list describes the same databases as the usage counters.</summary>
    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context) =>
        IndexObjectStatsCollector.Instance.BuildEnumerationQuery(context);

    /// <summary>
    /// The per-database read, one body for both paths (the on-prem path quote-doubles it into <c>[db].sys.sp_executesql</c>).
    ///
    /// <para><b>Permission.</b> <c>sys.sql_expression_dependencies</c> needs <c>SELECT</c> on the catalog view itself, which only
    /// <c>db_owner</c> has by default (<c>VIEW DEFINITION</c> does not include it; FinOps Index Analysis asks for the same grant). Without it
    /// the read would fail with Msg 229 in every database, so it is checked first and the database ships no rows. No rows reads as no list,
    /// and its views are then <c>Not tracked</c>, never called unused.</para>
    /// </summary>
    internal static string BuildPayloadBody() => $@"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

IF ISNULL(HAS_PERMS_BY_NAME(N'sys.sql_expression_dependencies', N'OBJECT', N'SELECT'), 0) = 0
BEGIN
    SELECT TOP (0)
        referencing_schema = CONVERT(sysname, NULL),
        referencing_name = CONVERT(sysname, NULL),
        referencing_type = CONVERT(nvarchar(2), NULL),
        referenced_server = CONVERT(sysname, NULL),
        referenced_database = CONVERT(sysname, NULL),
        referenced_schema = CONVERT(sysname, NULL),
        referenced_name = CONVERT(sysname, NULL),
        is_bound = CONVERT(bit, NULL),
        is_caller_dependent = CONVERT(bit, NULL),
        is_ambiguous = CONVERT(bit, NULL);

    RETURN;
END;

SELECT DISTINCT /* PerformanceMonitorLite */ TOP ({MaxRowsPerDatabase.ToString(CultureInfo.InvariantCulture)})
    x.referencing_schema,
    x.referencing_name,
    x.referencing_type,
    x.referenced_server,
    x.referenced_database,
    x.referenced_schema,
    x.referenced_name,
    x.is_bound,
    x.is_caller_dependent,
    x.is_ambiguous
FROM
(
    SELECT
        referencing_schema = s.name,
        referencing_name = o.name,
        referencing_type = CONVERT(nvarchar(2), RTRIM(o.type)),
        referenced_server = d.referenced_server_name,
        referenced_database = d.referenced_database_name,
        referenced_schema = COALESCE(rs.name, d.referenced_schema_name),
        referenced_name = COALESCE(ro.name, d.referenced_entity_name),
        is_bound = CONVERT(bit, CASE WHEN d.referenced_id IS NULL THEN 0 ELSE 1 END),
        is_caller_dependent = d.is_caller_dependent,
        is_ambiguous = d.is_ambiguous
    FROM sys.sql_expression_dependencies AS d
    JOIN sys.objects AS o
      ON o.object_id = d.referencing_id
    JOIN sys.schemas AS s
      ON s.schema_id = o.schema_id
    LEFT JOIN sys.objects AS ro
      ON ro.object_id = d.referenced_id
    LEFT JOIN sys.schemas AS rs
      ON rs.schema_id = ro.schema_id
    WHERE d.referencing_class = 1
    AND   d.referenced_class = 1
    AND   o.is_ms_shipped = 0
    AND   COALESCE(ro.name, d.referenced_entity_name) IS NOT NULL

    UNION ALL

    SELECT
        referencing_schema = s.name,
        referencing_name = sn.name,
        referencing_type = CONVERT(nvarchar(2), N'SN'),
        referenced_server = PARSENAME(sn.base_object_name, 4),
        referenced_database = PARSENAME(sn.base_object_name, 3),
        referenced_schema = PARSENAME(sn.base_object_name, 2),
        referenced_name = PARSENAME(sn.base_object_name, 1),
        is_bound = CONVERT(bit, 0),
        is_caller_dependent = CONVERT(bit, 0),
        is_ambiguous = CONVERT(bit, 0)
    FROM sys.synonyms AS sn
    JOIN sys.schemas AS s
      ON s.schema_id = sn.schema_id
    WHERE sn.is_ms_shipped = 0
    AND   PARSENAME(sn.base_object_name, 1) IS NOT NULL
) AS x
ORDER BY
    referencing_schema,
    referencing_name,
    referenced_schema,
    referenced_name
OPTION(RECOMPILE);";

    /// <summary>The Azure per-database branch: the connection is already in the database.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException(
                "object_dependencies enumerates databases off Azure SQL DB; BuildEnumerationQuery drives the cycle.");
        }

        return new CollectorQuery(BuildPayloadBody());
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(item);

        var text = $@"
EXECUTE [{item.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql
    N'{BuildPayloadBody().Replace("'", "''", StringComparison.Ordinal)}';";

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
    internal static Row ReadRow(DbDataReader reader, string databaseName) =>
        new()
        {
            DatabaseName = databaseName,
            ReferencingSchema = reader.IsDBNull(0) ? "" : reader.GetString(0),
            ReferencingName = reader.IsDBNull(1) ? "" : reader.GetString(1),
            ReferencingType = reader.IsDBNull(2) ? "" : reader.GetString(2),
            ReferencedServer = reader.IsDBNull(3) ? null : reader.GetString(3),
            ReferencedDatabase = reader.IsDBNull(4) ? null : reader.GetString(4),
            ReferencedSchema = reader.IsDBNull(5) ? null : reader.GetString(5),
            ReferencedName = reader.IsDBNull(6) ? "" : reader.GetString(6),
            IsBound = !reader.IsDBNull(7) && reader.GetBoolean(7),
            IsCallerDependent = !reader.IsDBNull(8) && reader.GetBoolean(8),
            IsAmbiguous = !reader.IsDBNull(9) && reader.GetBoolean(9),
        };

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("referencing_schema", CollectorColumnType.Varchar),
        new CollectorColumn("referencing_name", CollectorColumnType.Varchar),
        new CollectorColumn("referencing_type", CollectorColumnType.Varchar),
        new CollectorColumn("referenced_server", CollectorColumnType.Varchar),
        new CollectorColumn("referenced_database", CollectorColumnType.Varchar),
        new CollectorColumn("referenced_schema", CollectorColumnType.Varchar),
        new CollectorColumn("referenced_name", CollectorColumnType.Varchar),
        new CollectorColumn("is_bound", CollectorColumnType.Boolean),
        new CollectorColumn("is_caller_dependent", CollectorColumnType.Boolean),
        new CollectorColumn("is_ambiguous", CollectorColumnType.Boolean),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DatabaseName)        /* database_name VARCHAR */
            .Value(row.ReferencingSchema)   /* referencing_schema VARCHAR */
            .Value(row.ReferencingName)     /* referencing_name VARCHAR */
            .Value(row.ReferencingType)     /* referencing_type VARCHAR */
            .Value(row.ReferencedServer)    /* referenced_server VARCHAR */
            .Value(row.ReferencedDatabase)  /* referenced_database VARCHAR */
            .Value(row.ReferencedSchema)    /* referenced_schema VARCHAR */
            .Value(row.ReferencedName)      /* referenced_name VARCHAR */
            .Value(row.IsBound)             /* is_bound BOOLEAN */
            .Value(row.IsCallerDependent)   /* is_caller_dependent BOOLEAN */
            .Value(row.IsAmbiguous);        /* is_ambiguous BOOLEAN */
    }
}
