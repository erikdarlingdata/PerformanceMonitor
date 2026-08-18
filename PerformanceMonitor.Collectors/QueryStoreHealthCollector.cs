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
/// Per-database Query Store health from <c>sys.database_query_store_options</c> (#2319) — the fields that
/// answer "is Query Store actually working, and how close to its cap is it", which
/// <c>database_config</c>'s single <c>is_query_store_on</c> bit cannot: <c>actual_state</c> vs
/// <c>desired_state</c> (the classic silent failure is desired READ_WRITE with actual READ_ONLY after the
/// storage cap hit — <c>readonly_reason</c> says why), current vs max storage, the cleanup mode and
/// thresholds, and the runtime-stats interval length (the grain an investigation like #2312 needs to
/// interpret per-interval cost).
///
/// <para>The enumeration shape is <see cref="DatabaseScopedConfigCollector"/>'s verbatim: list databases
/// first (on-prem filters to non-AG or primary-replica databases the login can actually enter; Azure
/// lists all online), then EXECUTE <c>[db].sys.sp_executesql</c> per database on the same connection —
/// the proven per-database idiom. A database that fails is skipped with a warning by the host.</para>
///
/// <para>The database list is deliberately NOT filtered to <c>is_query_store_on = 1</c>:
/// <c>sys.database_query_store_options</c> returns exactly one row even when Query Store is off
/// (<c>actual_state_desc = 'OFF'</c>), so every enumerated database yields one honest row and OFF is
/// recorded as OFF — an absent row means "not collected", never "off". The collector itself gates on
/// 2016+ via <see cref="AppliesTo"/> (the view does not exist before v13); WITHIN the view every
/// selected column exists from 2016 on, so there are no per-column version gates.</para>
///
/// <para>Hourly rather than the config family's on-load cadence, because unlike the scoped-config knobs
/// (which only change when an operator changes them) <c>actual_state</c>, <c>readonly_reason</c> and
/// <c>current_storage_size_mb</c> change BY THEMSELVES — the cap-hit transition to READ_ONLY is the whole
/// point of collecting this, and an on-load snapshot would miss it until the next reconnect.</para>
/// </summary>
public sealed class QueryStoreHealthCollector : CollectorDefinitionBase<QueryStoreHealthCollector.Row>
{
    public static QueryStoreHealthCollector Instance { get; } = new();

    private QueryStoreHealthCollector()
    {
    }

    public sealed class Row
    {
        public string DbName { get; set; } = "";
        public string? ActualState { get; set; }
        public string? DesiredState { get; set; }
        public int ReadonlyReason { get; set; }
        public long CurrentStorageMb { get; set; }
        public long MaxStorageMb { get; set; }
        public string? SizeBasedCleanupMode { get; set; }
        public long StaleQueryThresholdDays { get; set; }
        public long MaxPlansPerQuery { get; set; }
        public long IntervalLengthMinutes { get; set; }
    }

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
AND   HAS_DBACCESS(d.name) = 1 /*a least-privilege login without per-db access raised 916 per db per cycle (#1823); the sibling per-database collectors already self-skip this way. On-prem only: from master on Azure SQL DB this returns 0 for every user database.*/
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

    /// <summary>
    /// Query Store shipped in SQL Server 2016 (v13); sys.database_query_store_options does not exist
    /// before it, so without this gate a pre-2016 target errors once per database per hour (review
    /// catch). The same condition QueryStoreCollector gates on, so Lite and Darling skip identically;
    /// 0 = version unknown = assume newest, and both Azure flavors always have the catalog.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target.SqlMajorVersion == 0 || target.SqlMajorVersion >= 13 || target.IsAzureSqlDb || target.IsAzureManagedInstance;

    public override string Name => "query_store_health";

    public override string TargetTable => "query_store_health";

    /// <summary>The config snapshots' prefix is config_id/capture_time in Lite's schema; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "config_id";

    public override string PrefixTimeColumnName => "capture_time";

    /// <summary>Enumerating collector — the primary query is never used.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
        => throw new NotSupportedException("query_store_health enumerates databases; BuildEnumerationQuery drives the cycle.");

    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        var text = (context.Target.IsAzureSqlDb ? AzureDatabaseListQueryText : OnPremDatabaseListQueryText)
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        /* Use [dbname].sys.sp_executesql to run in database context (Azure SQL DB compatible). One row
           always — the view answers for the database whether Query Store is on or off. */
        var text = $@"
EXECUTE [{item.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql
    N'SELECT
         actual_state = qso.actual_state_desc,
         desired_state = qso.desired_state_desc,
         readonly_reason = qso.readonly_reason,
         current_storage_size_mb = qso.current_storage_size_mb,
         max_storage_size_mb = qso.max_storage_size_mb,
         size_based_cleanup_mode = qso.size_based_cleanup_mode_desc,
         stale_query_threshold_days = qso.stale_query_threshold_days,
         max_plans_per_query = qso.max_plans_per_query,
         interval_length_minutes = qso.interval_length_minutes
     FROM sys.database_query_store_options AS qso
     OPTION(RECOMPILE);'";

        return new CollectorQuery(text);
    }

    public override async ValueTask ReadItemAsync(string item, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
    {
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row
            {
                DbName = item,
                ActualState = reader.IsDBNull(0) ? null : reader.GetString(0),
                DesiredState = reader.IsDBNull(1) ? null : reader.GetString(1),
                ReadonlyReason = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                CurrentStorageMb = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture),
                MaxStorageMb = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture),
                SizeBasedCleanupMode = reader.IsDBNull(5) ? null : reader.GetString(5),
                StaleQueryThresholdDays = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                MaxPlansPerQuery = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7), CultureInfo.InvariantCulture),
                IntervalLengthMinutes = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8), CultureInfo.InvariantCulture),
            });
        }
    }

    /// <summary>Never called — enumeration drives this collector.</summary>
    public override ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
        => throw new NotSupportedException("query_store_health enumerates databases; ReadItemAsync drives row reads.");

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("actual_state", CollectorColumnType.Varchar),
        new CollectorColumn("desired_state", CollectorColumnType.Varchar),
        new CollectorColumn("readonly_reason", CollectorColumnType.Integer),
        new CollectorColumn("current_storage_size_mb", CollectorColumnType.BigInt),
        new CollectorColumn("max_storage_size_mb", CollectorColumnType.BigInt),
        new CollectorColumn("size_based_cleanup_mode", CollectorColumnType.Varchar),
        new CollectorColumn("stale_query_threshold_days", CollectorColumnType.BigInt),
        new CollectorColumn("max_plans_per_query", CollectorColumnType.BigInt),
        new CollectorColumn("interval_length_minutes", CollectorColumnType.BigInt),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DbName)                    /* database_name VARCHAR */
            .Value(row.ActualState)               /* actual_state VARCHAR */
            .Value(row.DesiredState)              /* desired_state VARCHAR */
            .Value(row.ReadonlyReason)            /* readonly_reason INTEGER */
            .Value(row.CurrentStorageMb)          /* current_storage_size_mb BIGINT */
            .Value(row.MaxStorageMb)              /* max_storage_size_mb BIGINT */
            .Value(row.SizeBasedCleanupMode)      /* size_based_cleanup_mode VARCHAR */
            .Value(row.StaleQueryThresholdDays)   /* stale_query_threshold_days INTEGER */
            .Value(row.MaxPlansPerQuery)          /* max_plans_per_query INTEGER */
            .Value(row.IntervalLengthMinutes);    /* interval_length_minutes BIGINT */
    }
}
