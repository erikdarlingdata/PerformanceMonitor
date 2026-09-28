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
/// Reads the store's newest <c>server_properties</c> row plus <c>max degree of parallelism</c> from
/// <c>server_config</c> so plan-analysis rule 38 (#4530) can see the server's edition and MAXDOP without
/// a live connection. Darling analyzes STORED plans (<see cref="DarlingMcpPlanTools"/>, the drill-downs),
/// where erikdarlingdata/PerformanceStudio fetches the same two values live off the target server
/// (<c>ServerMetadataService.FetchServerMetadataAsync</c>) — PM has no live session at these call sites,
/// so it reads the collected copy instead. A failure or no rows is non-fatal (returns <c>null</c>), same
/// as PS's fetch, because a missing edition/MAXDOP degrades rule 38 to its Info branch rather than failing
/// the whole analysis.
/// </summary>
public static class DarlingServerMetadataReader
{
    /// <summary>
    /// One statement per store: the newest <c>server_properties</c> row for hardware/edition, LEFT JOINed to
    /// the latest <c>max degree of parallelism</c> value from <c>server_config</c> (the same
    /// latest-value-per-name pattern <c>PgFactCollector.Config.cs.ServerConfigSql</c> uses, narrowed to the
    /// one setting rule 38 needs). Both anchored on <c>server_id</c>; no time window, because "the server's
    /// current metadata" has no window, matching every other latest-snapshot config read in this store.
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
)
SELECT props.server_name, props.edition, props.product_version, props.product_level,
       props.cpu_count, props.physical_memory_mb, maxdop.value_in_use
FROM props
LEFT JOIN maxdop ON true";

    /// <summary>
    /// The server's edition and MAXDOP for plan analysis, or <c>null</c> when the store has no
    /// <c>server_properties</c> row for this server, or the read fails.
    /// </summary>
    public static async Task<ServerMetadata?> ReadAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(ServerMetadataSql, connection);
            command.Parameters.AddWithValue(serverId);

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
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }
}
