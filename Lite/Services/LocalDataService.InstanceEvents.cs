/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

/// <summary>The DuckDB read behind the Instance Restarted and Memory Dumps alerts (#8884).</summary>
public sealed partial class LocalDataService
{
    /// <summary>
    /// The server's recent <c>server_properties</c> samples, oldest first: every sample in the lookback window plus the one just before it, so the
    /// oldest returned sample has a neighbour to be compared with. $1 server_id, $2 the window start (bound, never written as now() - INTERVAL: collection_time is a
    /// naive-UTC TIMESTAMP). Reads the hot table only: the window is minutes, and archival moves rows out after days.
    /// Each sample's failed-collection count is the collection_log rows with status ERROR for the server between the previous sample and this one.
    /// </summary>
    public const string InstanceEventSamplesSql = @"
SELECT
    sp.collection_time,
    sp.sqlserver_start_time,
    sp.memory_dump_count,
    sp.newest_memory_dump_time
FROM server_properties AS sp
WHERE sp.server_id = $1
AND   sp.collection_time >= $2
ORDER BY sp.collection_time";

    public const string InstanceEventFailuresSql = @"
SELECT COUNT(*)
FROM collection_log
WHERE server_id = $1
AND   status = 'ERROR'
AND   collection_time > $2
AND   collection_time <= $3";

    public async Task<List<InstanceEventSample>> GetInstanceEventSamplesAsync(int serverId, DateTime sinceUtc)
    {
        using var connection = await OpenConnectionAsync();
        var samples = new List<InstanceEventSample>();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = InstanceEventSamplesSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = sinceUtc });
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                samples.Add(new InstanceEventSample
                {
                    CollectionTimeUtc = DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                    SqlServerStartTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    MemoryDumpCount = reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2)),
                    NewestMemoryDumpTime = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                });
            }
        }

        for (int i = 1; i < samples.Count; i++)
        {
            using var command = connection.CreateCommand();
            command.CommandText = InstanceEventFailuresSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = samples[i - 1].CollectionTimeUtc });
            command.Parameters.Add(new DuckDBParameter { Value = samples[i].CollectionTimeUtc });
            var scalar = await command.ExecuteScalarAsync();
            samples[i].FailedCollectionsSincePrevious = scalar is null or DBNull ? 0 : Convert.ToInt32(scalar);
        }

        return samples;
    }
}
