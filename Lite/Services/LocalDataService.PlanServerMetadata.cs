/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The <see cref="GetServerMetadataForPlanAnalysisAsync"/> read: the newest <c>v_server_properties</c>
    /// row for hardware/edition, LEFT JOINed to the latest MAXDOP / cost-threshold / max-memory values from
    /// <c>v_server_config</c> — the same latest-value-per-name pattern
    /// <see cref="GetLatestServerConfigAsync"/> reads, narrowed to the three settings plan-analysis rule 38
    /// (#4530) and the Server Context card (#4597) need — LEFT JOINed to the newest <c>v_database_config</c>
    /// row for <paramref name="databaseName"/> when one is given. Anchored on <c>server_id</c> alone: "the
    /// server's current metadata" has no time window, matching every other latest-snapshot config read in
    /// this store.
    /// </summary>
    public const string PlanServerMetadataSql = @"
SELECT p.edition, p.product_version, p.product_level, p.cpu_count, p.physical_memory_mb,
       (SELECT c.value_in_use
        FROM v_server_config AS c
        WHERE c.server_id = $1
        AND   c.configuration_name = 'max degree of parallelism'
        AND   c.capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)
        LIMIT 1) AS max_dop,
       (SELECT c.value_in_use
        FROM v_server_config AS c
        WHERE c.server_id = $1
        AND   c.configuration_name = 'cost threshold for parallelism'
        AND   c.capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)
        LIMIT 1) AS cost_threshold,
       (SELECT c.value_in_use
        FROM v_server_config AS c
        WHERE c.server_id = $1
        AND   c.configuration_name = 'max server memory (MB)'
        AND   c.capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)
        LIMIT 1) AS max_memory_mb,
       (SELECT d.database_name
        FROM v_database_config AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND database_name = $2)
        LIMIT 1) AS db_name,
       (SELECT d.compatibility_level
        FROM v_database_config AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND database_name = $2)
        LIMIT 1) AS compat_level,
       (SELECT d.collation_name
        FROM v_database_config AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND database_name = $2)
        LIMIT 1) AS collation_name,
       (SELECT d.is_read_committed_snapshot_on
        FROM v_database_config AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND database_name = $2)
        LIMIT 1) AS is_rcsi_on,
       (SELECT d.is_auto_create_stats_on
        FROM v_database_config AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND database_name = $2)
        LIMIT 1) AS is_auto_create_stats_on,
       (SELECT d.is_auto_update_stats_on
        FROM v_database_config AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND database_name = $2)
        LIMIT 1) AS is_auto_update_stats_on,
       (SELECT d.is_auto_update_stats_async_on
        FROM v_database_config AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND database_name = $2)
        LIMIT 1) AS is_auto_update_stats_async_on,
       (SELECT d.is_parameterization_forced
        FROM v_database_config AS d
        WHERE d.server_id = $1
        AND   d.database_name = $2
        AND   d.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND database_name = $2)
        LIMIT 1) AS is_parameterization_forced,
       p.engine_edition,
       p.vcore_count
FROM v_server_properties AS p
WHERE p.server_id = $1
ORDER BY p.collection_time DESC
LIMIT 1";

    /// <summary>
    /// The server's edition/MAXDOP/cost threshold/max memory for plan analysis (#4530/#4597), plus the
    /// database's name and compatibility level when <paramref name="databaseName"/> is given and the store
    /// has a <c>v_database_config</c> row for it, or <c>null</c> when the store has no
    /// <c>v_server_properties</c> row for this server, or the read fails. Mirrors
    /// <c>DarlingServerMetadataReader.ReadAsync</c>: a failure is non-fatal, because a missing value
    /// degrades rule 38 to its Info branch rather than failing the whole analysis.
    /// </summary>
    public async Task<ServerMetadata?> GetServerMetadataForPlanAnalysisAsync(int serverId, string? databaseName = null)
    {
        try
        {
            using var connection = await OpenConnectionAsync();
            using var command = connection.CreateCommand();
            command.CommandText = PlanServerMetadataSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = (object?)databaseName ?? DBNull.Value });

            using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return null;

            /* On an Azure SQL Database the stored cpu_count and physical_memory_mb are the HOST's, so the Server Context
               card's Hardware row carries the database's vCores (none for a DTU objective, which drops the row) and no
               RAM figure. Every other edition reads as it always did. */
            int? engineEdition = reader.IsDBNull(16) ? null : Convert.ToInt32(reader.GetValue(16));
            int? vcoreCount = reader.IsDBNull(17) ? null : Convert.ToInt32(reader.GetValue(17));
            int? storedCpuCount = reader.IsDBNull(3) ? null : reader.GetInt32(3);
            long? storedPhysicalMemoryMb = reader.IsDBNull(4) ? null : ToInt64(reader.GetValue(4));

            return new ServerMetadata
            {
                Edition = reader.IsDBNull(0) ? null : reader.GetString(0),
                ProductVersion = reader.IsDBNull(1) ? null : reader.GetString(1),
                ProductLevel = reader.IsDBNull(2) ? null : reader.GetString(2),
                CpuCount = ServerHardwareScope.OwnCpuCount(engineEdition, storedCpuCount, vcoreCount) ?? 0,
                PhysicalMemoryMB = ServerHardwareScope.OwnPhysicalMemoryMb(engineEdition, storedPhysicalMemoryMb) ?? 0L,
                MaxDop = reader.IsDBNull(5) ? 0 : Convert.ToInt32(Convert.ToDouble(reader.GetValue(5))),
                CostThresholdForParallelism = reader.IsDBNull(6) ? 0 : Convert.ToInt32(Convert.ToDouble(reader.GetValue(6))),
                MaxServerMemoryMB = reader.IsDBNull(7) ? 0L : ToInt64(reader.GetValue(7)),
                Database = reader.IsDBNull(8)
                    ? null
                    : new DatabaseMetadata
                    {
                        Name = reader.GetString(8),
                        CompatibilityLevel = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
                        CollationName = reader.IsDBNull(10) ? "" : reader.GetString(10),
                        IsReadCommittedSnapshotOn = !reader.IsDBNull(11) && reader.GetBoolean(11),
                        IsAutoCreateStatsOn = !reader.IsDBNull(12) && reader.GetBoolean(12),
                        IsAutoUpdateStatsOn = !reader.IsDBNull(13) && reader.GetBoolean(13),
                        IsAutoUpdateStatsAsyncOn = !reader.IsDBNull(14) && reader.GetBoolean(14),
                        IsParameterizationForced = !reader.IsDBNull(15) && reader.GetBoolean(15),
                    },
            };
        }
        catch (Exception)
        {
            return null;
        }
    }
}
