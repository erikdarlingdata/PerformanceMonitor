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
/// The list of procedures, functions and views in each database (#8784), once a day: name, type and create and modify dates from
/// <c>sys.objects</c>. <c>get_object_usage</c> needs it to name the modules that never ran: Query Store only shows a module that executed, so
/// without the list a procedure that runs once a year and one that was retired years ago both simply do not appear.
///
/// <para><b>Which databases</b> are <see cref="IndexObjectStatsCollector"/>'s, so the list and the usage counters always describe the same
/// databases. Its enumeration statement is reused as it is (every accessible online database, less the server's exclusions), and the #3477
/// scope it is built with is <c>index_object_stats</c>'s too: the host resolves this collector's scope through
/// <see cref="DatabaseScopeFilter.ScopeOwner"/>, because the statement alone would be filled from this collector's own scope row.</para>
///
/// <para><b>Which objects.</b> Procedures (T-SQL and CLR), scalar and table-valued functions (T-SQL and CLR) and views, user objects only
/// (<c>is_ms_shipped = 0</c>). The usage tool lists the modules and views from it (#8784 part 2). Tables are not
/// listed here: <c>index_object_stats</c> already holds every table.</para>
///
/// <para><b>Load.</b> One catalog read per database per day, bounded by <see cref="MaxRowsPerDatabase"/>. A plain table on every store
/// (<see cref="StaysPlainTable"/>), kept 30 days (the schedule's retention): only the newest capture is read, and the purge keeps a database's newest list however old.</para>
/// </summary>
public sealed class ObjectInventoryCollector : CollectorDefinitionBase<ObjectInventoryCollector.Row>
{
    public static ObjectInventoryCollector Instance { get; } = new();

    private ObjectInventoryCollector()
    {
    }

    /// <summary>One object.</summary>
    public sealed class Row
    {
        public string DatabaseName { get; set; } = "";
        public string SchemaName { get; set; } = "";
        public string ObjectName { get; set; } = "";
        public int ObjectId { get; set; }
        public string TypeDesc { get; set; } = "";
        public DateTime? CreateDate { get; set; }
        public DateTime? ModifyDate { get; set; }

        /// <summary>A natively compiled module, which Query Store does not record by default.</summary>
        public bool IsNativelyCompiled { get; set; }
    }

    /// <summary>The most objects one database ships in one pass. A database with more modules than this is not a normal one.</summary>
    public const int MaxRowsPerDatabase = 50_000;

    /// <summary>The per-command deadline: a catalog read of a few thousand rows takes milliseconds, so this is a backstop.</summary>
    public const int CommandTimeoutSeconds = 120;

    public override string Name => "object_inventory";

    public override string TargetTable => "object_inventory";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>A plain table with a time-sliced DELETE: the newest capture is the only one read.</summary>
    public override bool StaysPlainTable => true;

    /// <summary>The 5 s lock wait before a blocked read yields, the one <see cref="IndexObjectStatsCollector"/> uses.</summary>
    public const int LockTimeoutMilliseconds = IndexObjectStatsCollector.LockTimeoutMilliseconds;

    /// <summary>The body sets a 5 s lock timeout, so a 1222 behind a DDL's schema lock is a yield, not a failure (#9526).</summary>
    public override bool YieldsOnLockTimeout => true;

    /// <summary>Darling stores and reads it; Lite has no twin (ruling 10-21: new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    /// <summary>Azure SQL DB rejects three-part names, so the host connects per database there instead.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target is not null && target.IsAzureSqlDb;

    /// <summary>The index and object stats enumeration statement. The scope in <paramref name="context"/> is <c>index_object_stats</c>'s as
    /// well (<see cref="DatabaseScopeFilter.ScopeOwner"/>), so both collectors describe the same databases.</summary>
    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context) =>
        IndexObjectStatsCollector.Instance.BuildEnumerationQuery(context);

    /// <summary>
    /// The per-database read, one body for both paths (the on-prem path quote-doubles it into <c>[db].sys.sp_executesql</c>). It sets a 5 s
    /// lock timeout, so a DDL holding a schema modification lock on one module makes the database yield (error 1222) instead of waiting for
    /// the DDL to commit (#9526). <paramref name="supportsNativeCompilationColumn"/> gates <c>sys.sql_modules.uses_native_compilation</c>
    /// (SQL Server 2014+, Azure SQL DB, Managed Instance): below it the flag is a typed 0.
    /// </summary>
    internal static string BuildPayloadBody(bool supportsNativeCompilationColumn = true)
    {
        var nativelyCompiled = supportsNativeCompilationColumn
            ? "CONVERT(bit, ISNULL(m.uses_native_compilation, 0))"
            : "CONVERT(bit, 0)";

        return $@"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT {LockTimeoutMilliseconds.ToString(CultureInfo.InvariantCulture)};

SELECT /* PerformanceMonitorLite */ TOP ({MaxRowsPerDatabase.ToString(CultureInfo.InvariantCulture)})
    schema_name = s.name,
    object_name = o.name,
    object_id = o.object_id,
    type_desc = o.type_desc,
    create_date = o.create_date,
    modify_date = o.modify_date,
    /* Read from sys.sql_modules, not OBJECTPROPERTY: OBJECTPROPERTY takes a schema stability lock per row, so a DDL open on one procedure
       stalled the whole run behind it (#9526), where the catalog column does not wait. A version below SQL Server 2014 has no such column
       and no such modules, so the flag is a typed 0 there. A CLR module has no sql_modules row and reads 0. */
    is_natively_compiled = {nativelyCompiled}
FROM sys.objects AS o
JOIN sys.schemas AS s
  ON s.schema_id = o.schema_id
LEFT JOIN sys.sql_modules AS m
  ON m.object_id = o.object_id
WHERE o.is_ms_shipped = 0
AND   o.type IN (N'P', N'PC', N'FN', N'IF', N'TF', N'FS', N'FT', N'V')
ORDER BY
    o.object_id
OPTION(RECOMPILE);";
    }

    /// <summary>The Azure per-database branch: the connection is already in the database.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException(
                "object_inventory enumerates databases off Azure SQL DB; BuildEnumerationQuery drives the cycle.");
        }

        return new CollectorQuery(BuildPayloadBody(IndexObjectStatsCollector.SupportsInMemoryOltpColumns(context.Target)));
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(item);

        var text = $@"
EXECUTE [{item.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql
    N'{BuildPayloadBody(IndexObjectStatsCollector.SupportsInMemoryOltpColumns(context.Target)).Replace("'", "''", StringComparison.Ordinal)}';";

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
            SchemaName = reader.IsDBNull(0) ? "" : reader.GetString(0),
            ObjectName = reader.IsDBNull(1) ? "" : reader.GetString(1),
            ObjectId = Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
            TypeDesc = reader.IsDBNull(3) ? "" : reader.GetString(3),
            CreateDate = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
            ModifyDate = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            IsNativelyCompiled = !reader.IsDBNull(6) && reader.GetBoolean(6),
        };

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("object_name", CollectorColumnType.Varchar),
        new CollectorColumn("object_id", CollectorColumnType.Integer),
        new CollectorColumn("type_desc", CollectorColumnType.Varchar),
        new CollectorColumn("create_date", CollectorColumnType.Timestamp),
        new CollectorColumn("modify_date", CollectorColumnType.Timestamp),
        new CollectorColumn("is_natively_compiled", CollectorColumnType.Boolean),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DatabaseName)    /* database_name VARCHAR */
            .Value(row.SchemaName)      /* schema_name VARCHAR */
            .Value(row.ObjectName)      /* object_name VARCHAR */
            .Value(row.ObjectId)        /* object_id INTEGER */
            .Value(row.TypeDesc)        /* type_desc VARCHAR */
            .Value(row.CreateDate)      /* create_date TIMESTAMP, server-local */
            .Value(row.ModifyDate)      /* modify_date TIMESTAMP, server-local */
            .Value(row.IsNativelyCompiled); /* is_natively_compiled BOOLEAN */
    }
}
