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
/// Per-database configuration from sys.databases (on-load only). Extracted verbatim from Lite's
/// RemoteCollectorService.ServerConfig.cs, including the version gates: the 2019 columns
/// (accelerated database recovery, memory-optimized) are selected when major version ≥ 15,
/// version unknown (0 = assume newest), or Azure (SQL DB edition 5 / MI edition 8); the optimized-locking
/// column when major ≥ 17 (SQL Server 2025) or the target is Azure SQL Database, which reports major 12 but
/// documents the column; Managed Instance is not documented to have it, so it stays out (a missing column
/// would fail the whole collector). Ungated columns write NULL, matching the schema.
/// </summary>
public sealed class DatabaseConfigCollector : CollectorDefinitionBase<DatabaseConfigCollector.Row>
{
    public static DatabaseConfigCollector Instance { get; } = new();

    private DatabaseConfigCollector()
    {
    }

    public sealed class Row
    {
        public string DbName { get; set; } = "";
        public string? StateDesc { get; set; }
        public int CompatLevel { get; set; }
        public string? CollationName { get; set; }
        public string? RecoveryModel { get; set; }
        public bool IsReadOnly { get; set; }
        public bool AutoClose { get; set; }
        public bool AutoShrink { get; set; }
        public bool AutoCreateStats { get; set; }
        public bool AutoUpdateStats { get; set; }
        public bool AutoUpdateStatsAsync { get; set; }
        public bool Rcsi { get; set; }
        public string? SnapshotIsolation { get; set; }
        public bool ParameterizationForced { get; set; }
        public bool QueryStore { get; set; }
        public bool Encrypted { get; set; }
        public bool Trustworthy { get; set; }
        public bool DbChaining { get; set; }
        public bool BrokerEnabled { get; set; }
        public bool CdcEnabled { get; set; }
        public bool MixedPageAllocation { get; set; }
        public string? LogReuseWait { get; set; }
        public string? PageVerify { get; set; }
        public int TargetRecovery { get; set; }
        public string? DelayedDurability { get; set; }
        public bool? AcceleratedDatabaseRecovery { get; set; }
        public bool? MemoryOptimized { get; set; }
        public bool? OptimizedLocking { get; set; }
    }

    public override string Name => "database_config";

    public override string TargetTable => "database_config";

    /// <summary>The config snapshots' prefix is config_id/capture_time in Lite's schema; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "config_id";

    public override string PrefixTimeColumnName => "capture_time";

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var (has2019Columns, hasOptimizedLocking) = VersionGates(context.Target);

        /* Base columns available on all supported versions (2016+) */
        var selectColumns = @"
    database_name = d.name,
    state_desc = d.state_desc,
    compatibility_level = d.compatibility_level,
    collation_name = d.collation_name,
    recovery_model = d.recovery_model_desc,
    is_read_only = d.is_read_only,
    is_auto_close_on = d.is_auto_close_on,
    is_auto_shrink_on = d.is_auto_shrink_on,
    is_auto_create_stats_on = d.is_auto_create_stats_on,
    is_auto_update_stats_on = d.is_auto_update_stats_on,
    is_auto_update_stats_async_on = d.is_auto_update_stats_async_on,
    is_read_committed_snapshot_on = d.is_read_committed_snapshot_on,
    snapshot_isolation_state = d.snapshot_isolation_state_desc,
    is_parameterization_forced = d.is_parameterization_forced,
    is_query_store_on = d.is_query_store_on,
    is_encrypted = d.is_encrypted,
    is_trustworthy_on = d.is_trustworthy_on,
    is_db_chaining_on = d.is_db_chaining_on,
    is_broker_enabled = d.is_broker_enabled,
    is_cdc_enabled = d.is_cdc_enabled,
    is_mixed_page_allocation_on = d.is_mixed_page_allocation_on,
    log_reuse_wait_desc = d.log_reuse_wait_desc,
    page_verify_option = d.page_verify_option_desc,
    target_recovery_time_seconds = d.target_recovery_time_in_seconds,
    delayed_durability = d.delayed_durability_desc";

        if (has2019Columns)
        {
            selectColumns += @",
    is_accelerated_database_recovery_on = d.is_accelerated_database_recovery_on,
    is_memory_optimized_enabled = d.is_memory_optimized_enabled";
        }

        if (hasOptimizedLocking)
        {
            selectColumns += @",
    is_optimized_locking_on = d.is_optimized_locking_on";
        }

        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");

        var query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
{selectColumns}
FROM sys.databases AS d
WHERE (d.database_id > 4 OR d.database_id = 2)
AND   d.database_id < 32761
AND   d.name <> N'PerformanceMonitor'
{exclusionClause}
ORDER BY d.name
OPTION(RECOMPILE);";

        return new CollectorQuery(query, exclusionParameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("state_desc", CollectorColumnType.Varchar),
        new CollectorColumn("compatibility_level", CollectorColumnType.Integer),
        new CollectorColumn("collation_name", CollectorColumnType.Varchar),
        new CollectorColumn("recovery_model", CollectorColumnType.Varchar),
        new CollectorColumn("is_read_only", CollectorColumnType.Boolean),
        new CollectorColumn("is_auto_close_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_auto_shrink_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_auto_create_stats_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_auto_update_stats_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_auto_update_stats_async_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_read_committed_snapshot_on", CollectorColumnType.Boolean),
        new CollectorColumn("snapshot_isolation_state", CollectorColumnType.Varchar),
        new CollectorColumn("is_parameterization_forced", CollectorColumnType.Boolean),
        new CollectorColumn("is_query_store_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_encrypted", CollectorColumnType.Boolean),
        new CollectorColumn("is_trustworthy_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_db_chaining_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_broker_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("is_cdc_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("is_mixed_page_allocation_on", CollectorColumnType.Boolean),
        new CollectorColumn("log_reuse_wait_desc", CollectorColumnType.Varchar),
        new CollectorColumn("page_verify_option", CollectorColumnType.Varchar),
        new CollectorColumn("target_recovery_time_seconds", CollectorColumnType.Integer),
        new CollectorColumn("delayed_durability", CollectorColumnType.Varchar),
        new CollectorColumn("is_accelerated_database_recovery_on", CollectorColumnType.Boolean),
        new CollectorColumn("is_memory_optimized_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("is_optimized_locking_on", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var (has2019Columns, hasOptimizedLocking) = VersionGates(context.Target);
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var ordinal = 0;
            var r = new Row
            {
                DbName = reader.GetString(ordinal++),
                StateDesc = reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal),
                CompatLevel = reader.IsDBNull(++ordinal) ? 0 : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture),
                CollationName = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                RecoveryModel = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                IsReadOnly = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoClose = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoShrink = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoCreateStats = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoUpdateStats = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                AutoUpdateStatsAsync = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                Rcsi = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                SnapshotIsolation = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                ParameterizationForced = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                QueryStore = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                Encrypted = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                Trustworthy = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                DbChaining = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                BrokerEnabled = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                CdcEnabled = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                MixedPageAllocation = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal),
                LogReuseWait = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                PageVerify = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
                TargetRecovery = reader.IsDBNull(++ordinal) ? 0 : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture),
                DelayedDurability = reader.IsDBNull(++ordinal) ? null : reader.GetString(ordinal),
            };

            if (has2019Columns)
            {
                r.AcceleratedDatabaseRecovery = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal);
                r.MemoryOptimized = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal);
            }

            if (hasOptimizedLocking)
            {
                r.OptimizedLocking = !reader.IsDBNull(++ordinal) && reader.GetBoolean(ordinal);
            }

            rows.Add(r);
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DbName)
            .Value(row.StateDesc)
            .Value(row.CompatLevel)
            .Value(row.CollationName)
            .Value(row.RecoveryModel)
            .Value(row.IsReadOnly)
            .Value(row.AutoClose)
            .Value(row.AutoShrink)
            .Value(row.AutoCreateStats)
            .Value(row.AutoUpdateStats)
            .Value(row.AutoUpdateStatsAsync)
            .Value(row.Rcsi)
            .Value(row.SnapshotIsolation)
            .Value(row.ParameterizationForced)
            .Value(row.QueryStore)
            .Value(row.Encrypted)
            .Value(row.Trustworthy)
            .Value(row.DbChaining)
            .Value(row.BrokerEnabled)
            .Value(row.CdcEnabled)
            .Value(row.MixedPageAllocation)
            .Value(row.LogReuseWait)
            .Value(row.PageVerify)
            .Value(row.TargetRecovery)
            .Value(row.DelayedDurability)
            .Value(row.AcceleratedDatabaseRecovery)   /* version-gated: NULL when not collected */
            .Value(row.MemoryOptimized)               /* version-gated: NULL when not collected */
            .Value(row.OptimizedLocking);             /* version-gated: NULL when not collected */
    }

    private static (bool Has2019Columns, bool HasOptimizedLockingColumn) VersionGates(CollectorTargetInfo target)
    {
        var isAzure = target.IsAzureSqlDb || target.IsAzureManagedInstance;
        var has2019 = target.SqlMajorVersion >= 15 || target.SqlMajorVersion == 0 || isAzure;
        /* Azure SQL Database reports major 12 but has the column; Managed Instance is not documented to. */
        var hasOptimizedLocking = target.SqlMajorVersion >= 17 || target.IsAzureSqlDb;
        return (has2019, hasOptimizedLocking);
    }
}
