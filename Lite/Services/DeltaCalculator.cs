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
    /// Cache structure: serverId -> collectorName -> key -> previousValue
    /// </summary>
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<string, ConcurrentDictionary<string, long>>> _cache = new();

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
    /// Thread-safe via atomic AddOrUpdate.
    /// </summary>
    public long CalculateDelta(int serverId, string collectorName, string key, long currentValue)
    {
        var serverCache = _cache.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>());
        var collectorCache = serverCache.GetOrAdd(collectorName, _ => new ConcurrentDictionary<string, long>());

        long delta = 0;

        collectorCache.AddOrUpdate(
            key,
            /* Add: first time seeing this key — use current value as delta
               so queries that execute once still surface in top-N views */
            _ =>
            {
                delta = currentValue;
                return currentValue;
            },
            /* Update: compute delta atomically */
            (_, previousValue) =>
            {
                delta = currentValue < previousValue
                    ? 0              /* counter reset (plan cache eviction/re-entry) — not real new work */
                    : currentValue - previousValue;
                return currentValue;
            });

        return delta;
    }

    /// <summary>
    /// Seeds a single value into the cache without computing a delta.
    /// </summary>
    private void Seed(int serverId, string collectorName, string key, long value)
    {
        var serverCache = _cache.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>());
        var collectorCache = serverCache.GetOrAdd(collectorName, _ => new ConcurrentDictionary<string, long>());
        collectorCache[key] = value;
    }

    private async Task SeedWaitStatsAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT server_id, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms
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
            Seed(serverId, "wait_stats_tasks", waitType, reader.GetInt64(2));
            Seed(serverId, "wait_stats_time", waitType, reader.GetInt64(3));
            Seed(serverId, "wait_stats_signal", waitType, reader.GetInt64(4));
            count++;
        }
        if (count > 0) _logger?.LogDebug("Seeded {Count} wait_stats baseline rows", count);
    }

    private Task SeedFileIoStatsAsync(DuckDBConnection connection)
    {
        /* File I/O collector uses "{database_id}_{file_id}" as delta key,
           but we don't store those IDs in DuckDB. Seeding for file I/O
           is skipped — the first collection after restart will have delta=0,
           and the second collection will produce accurate deltas. */
        return Task.CompletedTask;
    }

    private async Task SeedPerfmonStatsAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT server_id, object_name, counter_name, instance_name, cntr_value
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
            Seed(serverId, "perfmon", $"{objectName}|{counter}|{instance}", reader.GetInt64(4));
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
                Seed(serverId, "memory_grants_timeouts", deltaKey, reader.IsDBNull(3) ? 0 : reader.GetInt64(3));
                Seed(serverId, "memory_grants_forced", deltaKey, reader.IsDBNull(4) ? 0 : reader.GetInt64(4));
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
