/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
    /// #8571, Darling's <c>CountCollectionLogAsync</c>: the log rows that match <see cref="GetRecentCollectionLogAsync"/>'s filters in the
    /// window, uncapped, for get_collection_log's <c>runs_total</c>. Asked only when the over-fetch saw more rows than the limit.
    /// </summary>
    public async Task<long> CountRecentCollectionLogAsync(
        int serverId, int hoursBack, DateTime? asOfUtc, string? collectorName, double? minDurationMs, string? status)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);

        command.CommandText = @"
SELECT COUNT(*)
FROM v_collection_log
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   ($4 IS NULL OR collector_name = $4)
AND   ($5 IS NULL OR duration_ms >= $5)
AND   ($6 IS NULL OR status = UPPER($6))";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = string.IsNullOrWhiteSpace(collectorName) ? DBNull.Value : collectorName.Trim() });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)minDurationMs ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = string.IsNullOrWhiteSpace(status) ? DBNull.Value : status.Trim() });

        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// #8571, Darling's <c>CountCollectionLogFleetAsync</c>: the fleet twin of <see cref="CountRecentCollectionLogAsync"/>. The log rows that
    /// match <see cref="GetRecentCollectionLogFleetAsync"/>'s filters in the window across the same <paramref name="serverIds"/>, uncapped, for
    /// the fleet page's <c>runs_total</c>. Asked only when the over-fetch saw more rows than the limit.
    /// </summary>
    public async Task<long> CountRecentCollectionLogFleetAsync(
        IReadOnlyList<int> serverIds, int hoursBack, DateTime? asOfUtc, string? collectorName, double? minDurationMs, string? status)
    {
        if (serverIds.Count == 0)
        {
            return 0;
        }

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);

        command.CommandText = $@"
SELECT COUNT(*)
FROM v_collection_log
WHERE server_id IN ({string.Join(",", serverIds)})
AND   collection_time >= $1
AND   collection_time <= $2
AND   ($3 IS NULL OR collector_name = $3)
AND   ($4 IS NULL OR duration_ms >= $4)
AND   ($5 IS NULL OR status = UPPER($5))";

        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = string.IsNullOrWhiteSpace(collectorName) ? DBNull.Value : collectorName.Trim() });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)minDurationMs ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = string.IsNullOrWhiteSpace(status) ? DBNull.Value : status.Trim() });

        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// #8571, Darling's <c>CollectorEverRanAsync</c>: whether one collector has ever recorded a run for this server, ignoring any window.
    /// An event-list tool that found no rows asks it to tell a collector that never ran (not_collected) from one that found nothing (empty).
    /// </summary>
    public async Task<bool> HasCollectorEverRunAsync(int serverId, string collectorName)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = collectorName });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }
}
