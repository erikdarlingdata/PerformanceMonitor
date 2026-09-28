/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the store's newest <c>server_properties</c> row plus <c>max degree of parallelism</c>,
/// <c>cost threshold for parallelism</c> and <c>max server memory (MB)</c> from <c>server_config</c>, plus
/// (when a database name is given) the newest <c>database_config</c> row for that database, so plan-analysis
/// rule 38 (#4530) and the Server Context card (#4597) can see the server's edition/MAXDOP/cost
/// threshold/max memory and the plan's database name/compat level without a live connection. Darling
/// analyzes STORED plans (<see cref="DarlingMcpPlanTools"/>, the drill-downs), where
/// erikdarlingdata/PerformanceStudio fetches the same values live off the target server
/// (<c>ServerMetadataService.FetchServerMetadataAsync</c> / <c>FetchDatabaseMetadataAsync</c>) — PM has no
/// live session at these call sites, so it reads the collected copy instead. A failure or no rows is
/// non-fatal (returns <c>null</c>, or leaves <c>Database</c> null), same as PS's fetch, because a missing
/// value degrades rule 38 to its Info branch rather than failing the whole analysis.
/// </summary>
public static class DarlingServerMetadataReader
{
    /// <summary>
    /// One statement per store: the newest <c>server_properties</c> row for hardware/edition, LEFT JOINed to
    /// the latest MAXDOP/cost-threshold/max-memory values from <c>server_config</c> (the same
    /// latest-value-per-name pattern <c>PgFactCollector.Config.cs.ServerConfigSql</c> uses, narrowed to the
    /// three settings rule 38 and #4597 need), LEFT JOINed to the newest <c>database_config</c> row for
    /// <paramref name="databaseName"/> when one is given. Anchored on <c>server_id</c>; no time window,
    /// because "the server's current metadata" has no window, matching every other latest-snapshot config
    /// read in this store.
    /// </summary>
    public const string ServerMetadataSql = @"
WITH props AS (
    SELECT server_name, edition, product_version, product_level, cpu_count, physical_memory_mb
    FROM server_properties
    WHERE server_id = $1
    ORDER BY collection_time DESC
    LIMIT 1
),
maxdop AS (
    SELECT value_in_use
    FROM server_config
    WHERE server_id = $1
    AND   configuration_name = 'max degree of parallelism'
    ORDER BY capture_time DESC
    LIMIT 1
),
ctfp AS (
    SELECT value_in_use
    FROM server_config
    WHERE server_id = $1
    AND   configuration_name = 'cost threshold for parallelism'
    ORDER BY capture_time DESC
    LIMIT 1
),
maxmem AS (
    SELECT value_in_use
    FROM server_config
    WHERE server_id = $1
    AND   configuration_name = 'max server memory (MB)'
    ORDER BY capture_time DESC
    LIMIT 1
),
dbconfig AS (
    SELECT database_name, compatibility_level, collation_name, is_read_committed_snapshot_on,
           is_auto_create_stats_on, is_auto_update_stats_on, is_auto_update_stats_async_on,
           is_parameterization_forced
    FROM database_config
    WHERE server_id = $1
    AND   database_name = $2
    ORDER BY capture_time DESC
    LIMIT 1
)
SELECT props.server_name, props.edition, props.product_version, props.product_level,
       props.cpu_count, props.physical_memory_mb, maxdop.value_in_use,
       ctfp.value_in_use, maxmem.value_in_use, dbconfig.database_name, dbconfig.compatibility_level,
       dbconfig.collation_name, dbconfig.is_read_committed_snapshot_on, dbconfig.is_auto_create_stats_on,
       dbconfig.is_auto_update_stats_on, dbconfig.is_auto_update_stats_async_on,
       dbconfig.is_parameterization_forced
FROM props
LEFT JOIN maxdop ON true
LEFT JOIN ctfp ON true
LEFT JOIN maxmem ON true
LEFT JOIN dbconfig ON true";

    /// <summary>
    /// The server's edition/MAXDOP/cost threshold/max memory for plan analysis, plus the database's name
    /// and compatibility level when <paramref name="databaseName"/> is given and the store has a
    /// <c>database_config</c> row for it, or <c>null</c> when the store has no <c>server_properties</c> row
    /// for this server, or the read fails. <paramref name="databaseName"/> is optional: a caller with no
    /// database name at hand (a pasted plan, a stored plan keyed on hash alone) gets everything except
    /// <see cref="ServerMetadata.Database"/>, which stays null.
    /// </summary>
    public static async Task<ServerMetadata?> ReadAsync(
        NpgsqlDataSource postgres, int serverId, string? databaseName = null, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(ServerMetadataSql, connection)
            {
                CommandTimeout = StorageCommandDeadlines.McpReadSeconds,
            };
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(databaseName ?? (object)DBNull.Value);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
                return null;

            return new ServerMetadata
            {
                ServerName = reader.IsDBNull(0) ? null : reader.GetString(0),
                Edition = reader.IsDBNull(1) ? null : reader.GetString(1),
                ProductVersion = reader.IsDBNull(2) ? null : reader.GetString(2),
                ProductLevel = reader.IsDBNull(3) ? null : reader.GetString(3),
                CpuCount = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                PhysicalMemoryMB = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                MaxDop = reader.IsDBNull(6) ? 0 : Convert.ToInt32(Convert.ToDouble(reader.GetValue(6))),
                CostThresholdForParallelism = reader.IsDBNull(7) ? 0 : Convert.ToInt32(Convert.ToDouble(reader.GetValue(7))),
                MaxServerMemoryMB = reader.IsDBNull(8) ? 0L : Convert.ToInt64(Convert.ToDouble(reader.GetValue(8))),
                Database = reader.IsDBNull(9)
                    ? null
                    : new DatabaseMetadata
                    {
                        Name = reader.GetString(9),
                        CompatibilityLevel = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10)),
                        CollationName = reader.IsDBNull(11) ? "" : reader.GetString(11),
                        IsReadCommittedSnapshotOn = !reader.IsDBNull(12) && reader.GetBoolean(12),
                        IsAutoCreateStatsOn = !reader.IsDBNull(13) && reader.GetBoolean(13),
                        IsAutoUpdateStatsOn = !reader.IsDBNull(14) && reader.GetBoolean(14),
                        IsAutoUpdateStatsAsyncOn = !reader.IsDBNull(15) && reader.GetBoolean(15),
                        IsParameterizationForced = !reader.IsDBNull(16) && reader.GetBoolean(16),
                    },
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }
}
