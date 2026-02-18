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

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets TempDB stats trend for charting.
    /// </summary>
    public async Task<List<TempDbRow>> GetTempDbTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    collection_time,
    user_object_reserved_mb,
    internal_object_reserved_mb,
    version_store_reserved_mb,
    total_reserved_mb,
    unallocated_mb,
    total_sessions_using_tempdb,
    top_session_id,
    top_session_tempdb_mb
FROM tempdb_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<TempDbRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TempDbRow
            {
                CollectionTime = reader.GetDateTime(0),
                UserObjectReservedMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                InternalObjectReservedMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                VersionStoreReservedMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                TotalReservedMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                UnallocatedMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TotalSessionsUsingTempDb = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                TopSessionId = reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                TopSessionTempDbMb = reader.IsDBNull(8) ? 0 : ToDouble(reader.GetValue(8))
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the latest TempDB space snapshot for alert checking.
    /// </summary>
    public async Task<TempDbSpaceInfo?> GetLatestTempDbSpaceAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    total_reserved_mb,
    unallocated_mb,
    user_object_reserved_mb,
    internal_object_reserved_mb,
    version_store_reserved_mb,
    top_session_tempdb_mb,
    top_session_id
FROM tempdb_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new TempDbSpaceInfo
            {
                TotalReservedMb = reader.IsDBNull(0) ? 0 : ToDouble(reader.GetValue(0)),
                UnallocatedMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                UserObjectReservedMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                InternalObjectReservedMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                VersionStoreReservedMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TopConsumerMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TopConsumerSessionId = reader.IsDBNull(6) ? 0 : reader.GetInt32(6)
            };
        }

        return null;
    }
}

public class TempDbSpaceInfo
{
    public double TotalReservedMb { get; set; }
    public double UnallocatedMb { get; set; }
    public double UserObjectReservedMb { get; set; }
    public double InternalObjectReservedMb { get; set; }
    public double VersionStoreReservedMb { get; set; }
    public int TopConsumerSessionId { get; set; }
    public double TopConsumerMb { get; set; }

    public double UsedPercent => TotalReservedMb + UnallocatedMb > 0
        ? TotalReservedMb / (TotalReservedMb + UnallocatedMb) * 100
        : 0;
}

public class TempDbRow
{
    public DateTime CollectionTime { get; set; }
    public double UserObjectReservedMb { get; set; }
    public double InternalObjectReservedMb { get; set; }
    public double VersionStoreReservedMb { get; set; }
    public double TotalReservedMb { get; set; }
    public double UnallocatedMb { get; set; }
    public int TotalSessionsUsingTempDb { get; set; }
    public int TopSessionId { get; set; }
    public double TopSessionTempDbMb { get; set; }
}
