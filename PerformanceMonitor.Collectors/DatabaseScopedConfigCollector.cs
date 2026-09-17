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
/// Database-scoped configurations from sys.database_scoped_configurations for each online user
/// database (on-load only). Extracted verbatim from Lite's RemoteCollectorService.ServerConfig.cs.
/// The enumeration shape: list databases first (on-prem filters to non-AG or primary-replica
/// databases the login can actually enter; Azure lists all online), then EXECUTE
/// [db].sys.sp_executesql per database on the same connection — the proven per-database idiom the
/// Query Store collector also uses. A database that fails is skipped with a warning by the host.
/// </summary>
public sealed class DatabaseScopedConfigCollector : CollectorDefinitionBase<DatabaseScopedConfigCollector.Row>
{
    public static DatabaseScopedConfigCollector Instance { get; } = new();

    private DatabaseScopedConfigCollector()
    {
    }

    public readonly record struct Row(string DbName, string ConfigName, string? Value, string? ValueForSecondary);

    private const string OnPremDatabaseListQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    d.name
FROM sys.databases AS d
LEFT JOIN sys.dm_hadr_database_replica_states AS drs
    ON d.database_id = drs.database_id
    AND drs.is_local = 1
WHERE (d.database_id > 4 OR d.database_id = 2)
AND   d.database_id < 32761
AND   d.name <> N'PerformanceMonitor'
AND   d.state_desc = N'ONLINE'
AND   HAS_DBACCESS(d.name) = 1 /*a least-privilege login without per-db access raised 916 per db per cycle (#1823); index_object_stats and database_size_stats already self-skip this way. On-prem only: from master on Azure SQL DB this returns 0 for every user database.*/
AND
(
    drs.database_id IS NULL          /*not in any AG*/
    OR drs.is_primary_replica = 1    /*primary replica*/
)
/*EXCLUSION_FILTER*/
ORDER BY d.name
OPTION(RECOMPILE);";

    private const string AzureDatabaseListQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    d.name
FROM sys.databases AS d
WHERE (d.database_id > 4 OR d.database_id = 2)
AND   d.database_id < 32761
AND   d.name <> N'PerformanceMonitor'
AND   d.state_desc = N'ONLINE'
/*EXCLUSION_FILTER*/
ORDER BY d.name
OPTION(RECOMPILE);";

    public override string Name => "database_scoped_config";

    public override string TargetTable => "database_scoped_config";

    /// <summary>The config snapshots' prefix is config_id/capture_time in Lite's schema; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "config_id";

    public override string PrefixTimeColumnName => "capture_time";

    /// <summary>Enumerating collector — the primary query is never used.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
        => throw new NotSupportedException("database_scoped_config enumerates databases; BuildEnumerationQuery drives the cycle.");

    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        /* #3477: the composed scope+exclusion predicate rides the same splice point the exclusion
           always used — the placeholder keeps its name because it marks WHERE the filter lands, and
           the composition (allow-list AND NOT excluded) is DatabaseScopeFilter's to state once. */
        var (exclusionClause, exclusionParameters) = DatabaseScopeFilter.BuildEnumerationPredicate(context, "d.name");
        var text = (context.Target.IsAzureSqlDb ? AzureDatabaseListQueryText : OnPremDatabaseListQueryText)
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        /* Use [dbname].sys.sp_executesql to run in database context (Azure SQL DB compatible) */
        var text = $@"
EXECUTE [{item.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql
    N'SELECT
         configuration_name = dsc.name,
         value = CONVERT(nvarchar(256), dsc.value),
         value_for_secondary = CONVERT(nvarchar(256), dsc.value_for_secondary)
     FROM sys.database_scoped_configurations AS dsc
     ORDER BY dsc.name
     OPTION(RECOMPILE);'";

        return new CollectorQuery(text);
    }

    public override async ValueTask ReadItemAsync(string item, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
    {
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                item,
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2)));
        }
    }

    /// <summary>Never called — enumeration drives this collector.</summary>
    public override ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
        => throw new NotSupportedException("database_scoped_config enumerates databases; ReadItemAsync drives row reads.");

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("configuration_name", CollectorColumnType.Varchar),
        new CollectorColumn("value", CollectorColumnType.Varchar),
        new CollectorColumn("value_for_secondary", CollectorColumnType.Varchar),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DbName)              /* database_name VARCHAR */
            .Value(row.ConfigName)          /* configuration_name VARCHAR */
            .Value(row.Value)               /* value VARCHAR */
            .Value(row.ValueForSecondary);  /* value_for_secondary VARCHAR */
    }
}
