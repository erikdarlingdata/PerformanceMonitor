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
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The <see cref="GetServerMetadataForPlanAnalysisAsync"/> read: the newest <c>v_server_properties</c>
    /// row for hardware/edition, LEFT JOINed to the latest <c>max degree of parallelism</c> value from
    /// <c>v_server_config</c> — the same latest-value-per-name pattern
    /// <see cref="GetLatestServerConfigAsync"/> reads, narrowed to the one setting plan-analysis rule 38
    /// (#4530) needs. Anchored on <c>server_id</c> alone: "the server's current metadata" has no time
    /// window, matching every other latest-snapshot config read in this store.
    /// </summary>
    public const string PlanServerMetadataSql = @"
SELECT p.edition, p.product_version, p.product_level, p.cpu_count, p.physical_memory_mb,
       (SELECT c.value_in_use
        FROM v_server_config AS c
        WHERE c.server_id = $1
        AND   c.configuration_name = 'max degree of parallelism'
        AND   c.capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)
        LIMIT 1) AS max_dop
FROM v_server_properties AS p
WHERE p.server_id = $1
ORDER BY p.collection_time DESC
LIMIT 1";

    /// <summary>
    /// The server's edition and MAXDOP for plan analysis (#4530), or <c>null</c> when the store has no
    /// <c>v_server_properties</c> row for this server, or the read fails. Mirrors
    /// <c>DarlingServerMetadataReader.ReadAsync</c>: a failure is non-fatal, because a missing
    /// edition/MAXDOP degrades rule 38 to its Info branch rather than failing the whole analysis.
    /// </summary>
    public async Task<ServerMetadata?> GetServerMetadataForPlanAnalysisAsync(int serverId)
    {
        try
        {
            using var connection = await OpenConnectionAsync();
            using var command = connection.CreateCommand();
            command.CommandText = PlanServerMetadataSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });

            using var reader = await command.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return null;

            return new ServerMetadata
            {
                Edition = reader.IsDBNull(0) ? null : reader.GetString(0),
                ProductVersion = reader.IsDBNull(1) ? null : reader.GetString(1),
                ProductLevel = reader.IsDBNull(2) ? null : reader.GetString(2),
                CpuCount = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                PhysicalMemoryMB = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4)),
                MaxDop = reader.IsDBNull(5) ? 0 : Convert.ToInt32(Convert.ToDouble(reader.GetValue(5))),
            };
        }
        catch (Exception)
        {
            return null;
        }
    }
}
