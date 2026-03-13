/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Calculates delta values for cumulative metrics between collection intervals.
/// Caches previous values in memory for efficient delta calculation.
/// Seeds from DuckDB on startup to survive application restarts.
/// </summary>
public class DeltaCalculator
{
    /// <summary>
    /// Cache structure: serverId -> collectorName -> key -> (previousValue, timestamp)
    /// </summary>
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>> _cache = new();

    private readonly ILogger? _logger;

    public DeltaCalculator(ILogger? logger = null)
    {
        _logger = logger;
    }

    /// <summary>
    /// Seeds the delta cache from DuckDB so that the first collection after restart
    /// can produce accurate deltas instead of returning 0 for everything.
    /// </summary>
    public async Task SeedFromDatabaseAsync(DuckDbInitializer duckDb)
    {
        try
        {
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync();

            await SeedWaitStatsAsync(connection);
            await SeedFileIoStatsAsync(connection);
            await SeedPerfmonStatsAsync(connection);
            await SeedMemoryGrantStatsAsync(connection);

            _logger?.LogInformation("Delta calculator seeded from database");
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to seed delta calculator from database, first collection will return 0 deltas");
        }
    }

    /// <summary>
    /// Calculates the delta between the current value and the previous cached value.
    /// First-ever sighting (no baseline): returns currentValue so single-execution queries appear.
    /// Counter reset (value decreased): returns 0 to avoid inflated deltas from plan cache churn.
    /// Gap detection: if collectionTime and maxGapSeconds are provided and the gap since the
    /// last cached value exceeds maxGapSeconds, returns 0 to avoid inflated deltas after restarts.
    /// Thread-safe via atomic AddOrUpdate.
    /// </summary>
    public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
        bool baselineOnly = false, DateTime? collectionTime = null, int maxGapSeconds = 0)
    {
        var serverCache = _cache.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>());
        var collectorCache = serverCache.GetOrAdd(collectorName, _ => new ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>());

        long delta = 0;

        collectorCache.AddOrUpdate(
            key,
            /* Add: first time seeing this key.
               baselineOnly = true: store baseline only, return 0 (for cumulative counters like perfmon).
               baselineOnly = false: use current value as delta so single-execution queries surface. */
            _ =>
            {
                delta = baselineOnly ? 0 : currentValue;
                return (currentValue, collectionTime);
            },
            /* Update: compute delta atomically */
            (_, previous) =>
            {
                /* Gap detection: if too much time has passed since the last cached value,
                   treat this as a new baseline to avoid inflated deltas after app restarts */
                if (maxGapSeconds > 0 && collectionTime.HasValue && previous.Timestamp.HasValue
                    && (collectionTime.Value - previous.Timestamp.Value).TotalSeconds > maxGapSeconds)
                {
                    delta = 0;
                    return (currentValue, collectionTime);
                }

                delta = currentValue < previous.Value
                    ? 0              /* counter reset (plan cache eviction/re-entry) — not real new work */
                    : currentValue - previous.Value;
                return (currentValue, collectionTime);
            });

        return delta;
    }

    /// <summary>
    /// Seeds a single value into the cache without computing a delta.
    /// </summary>
    private void Seed(int serverId, string collectorName, string key, long value, DateTime? timestamp = null)
    {
        var serverCache = _cache.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>());
        var collectorCache = serverCache.GetOrAdd(collectorName, _ => new ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>());
        collectorCache[key] = (value, timestamp);
    }

    private async Task SeedWaitStatsAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT server_id, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, collection_time
FROM wait_stats
WHERE (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM wait_stats GROUP BY server_id
)";
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        while (await reader.ReadAsync())
        {
            var serverId = reader.GetInt32(0);
            var waitType = reader.GetString(1);
            var ts = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
            Seed(serverId, "wait_stats_tasks", waitType, reader.GetInt64(2), ts);
            Seed(serverId, "wait_stats_time", waitType, reader.GetInt64(3), ts);
            Seed(serverId, "wait_stats_signal", waitType, reader.GetInt64(4), ts);
            count++;
        }
        if (count > 0) _logger?.LogDebug("Seeded {Count} wait_stats baseline rows", count);
    }

    private async Task SeedFileIoStatsAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT server_id, database_name, file_name,
       num_of_reads, num_of_writes, read_bytes, write_bytes,
       io_stall_read_ms, io_stall_write_ms,
       io_stall_queued_read_ms, io_stall_queued_write_ms,
       collection_time
FROM file_io_stats
WHERE (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM file_io_stats GROUP BY server_id
)";
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        while (await reader.ReadAsync())
        {
            var serverId = reader.GetInt32(0);
            var dbName = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var fileName = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var deltaKey = $"{dbName}|{fileName}";
            var ts = reader.IsDBNull(11) ? (DateTime?)null : reader.GetDateTime(11);
            Seed(serverId, "file_io_reads", deltaKey, reader.IsDBNull(3) ? 0 : reader.GetInt64(3), ts);
            Seed(serverId, "file_io_writes", deltaKey, reader.IsDBNull(4) ? 0 : reader.GetInt64(4), ts);
            Seed(serverId, "file_io_read_bytes", deltaKey, reader.IsDBNull(5) ? 0 : reader.GetInt64(5), ts);
            Seed(serverId, "file_io_write_bytes", deltaKey, reader.IsDBNull(6) ? 0 : reader.GetInt64(6), ts);
            Seed(serverId, "file_io_stall_read", deltaKey, reader.IsDBNull(7) ? 0 : reader.GetInt64(7), ts);
            Seed(serverId, "file_io_stall_write", deltaKey, reader.IsDBNull(8) ? 0 : reader.GetInt64(8), ts);
            Seed(serverId, "file_io_stall_queued_read", deltaKey, reader.IsDBNull(9) ? 0 : reader.GetInt64(9), ts);
            Seed(serverId, "file_io_stall_queued_write", deltaKey, reader.IsDBNull(10) ? 0 : reader.GetInt64(10), ts);
            count++;
        }
        if (count > 0) _logger?.LogDebug("Seeded {Count} file_io_stats baseline rows", count);
    }

    private async Task SeedPerfmonStatsAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT server_id, object_name, counter_name, instance_name, cntr_value, collection_time
FROM perfmon_stats
WHERE (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM perfmon_stats GROUP BY server_id
)";
        using var reader = await cmd.ExecuteReaderAsync();
        var count = 0;
        while (await reader.ReadAsync())
        {
            var serverId = reader.GetInt32(0);
            var objectName = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var counter = reader.IsDBNull(2) ? "" : reader.GetString(2);
            var instance = reader.IsDBNull(3) ? "" : reader.GetString(3);
            var ts = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
            Seed(serverId, "perfmon", $"{objectName}|{counter}|{instance}", reader.GetInt64(4), ts);
            count++;
        }
        if (count > 0) _logger?.LogDebug("Seeded {Count} perfmon_stats baseline rows", count);
    }

    private async Task SeedMemoryGrantStatsAsync(DuckDBConnection connection)
    {
        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT server_id, pool_id, resource_semaphore_id, timeout_error_count, forced_grant_count
FROM memory_grant_stats
WHERE (server_id, collection_time) IN (
    SELECT server_id, MAX(collection_time) FROM memory_grant_stats GROUP BY server_id
)";
            using var reader = await cmd.ExecuteReaderAsync();
            var count = 0;
            while (await reader.ReadAsync())
            {
                var serverId = reader.GetInt32(0);
                var poolId = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
                var semaphoreId = reader.IsDBNull(2) ? (short)0 : reader.GetInt16(2);
                var deltaKey = $"{poolId}_{semaphoreId}";
                Seed(serverId, "memory_grants_timeouts", deltaKey, reader.IsDBNull(3) ? 0 : reader.GetInt64(3), null);
                Seed(serverId, "memory_grants_forced", deltaKey, reader.IsDBNull(4) ? 0 : reader.GetInt64(4), null);
                count++;
            }
            if (count > 0) _logger?.LogDebug("Seeded {Count} memory_grant_stats baseline rows", count);
        }
        catch
        {
            /* Table may not exist on first run after schema migration */
        }
    }

}
