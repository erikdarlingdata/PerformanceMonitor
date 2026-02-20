/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    private HashSet<string>? _ignoredWaitTypes;

    /// <summary>
    /// Gets the set of wait types to ignore during collection.
    /// </summary>
    private HashSet<string> GetIgnoredWaitTypes()
    {
        if (_ignoredWaitTypes != null)
        {
            return _ignoredWaitTypes;
        }

        _ignoredWaitTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var configPath = Path.Combine(App.ConfigDirectory, "ignored_wait_types.json");
        if (File.Exists(configPath))
        {
            try
            {
                var json = File.ReadAllText(configPath);
                using var doc = JsonDocument.Parse(json);

                if (doc.RootElement.TryGetProperty("ignored_waits", out var waitsArray))
                {
                    foreach (var wait in waitsArray.EnumerateArray())
                    {
                        var waitType = wait.GetString();
                        if (!string.IsNullOrEmpty(waitType))
                        {
                            _ignoredWaitTypes.Add(waitType);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to load ignored wait types from {Path}", configPath);
            }
        }

        return _ignoredWaitTypes;
    }

    /// <summary>
    /// Collects wait statistics from sys.dm_os_wait_stats.
    /// </summary>
    private async Task<int> CollectWaitStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        const string query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    wait_type = ws.wait_type,
    waiting_tasks_count = ws.waiting_tasks_count,
    wait_time_ms = ws.wait_time_ms,
    signal_wait_time_ms = ws.signal_wait_time_ms
FROM sys.dm_os_wait_stats AS ws
WHERE ws.wait_time_ms > 0
OPTION(RECOMPILE);";

        var ignoredWaits = GetIgnoredWaitTypes();
        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;
        var rowsCollected = 0;
        _lastSqlMs = 0;
        _lastDuckDbMs = 0;

        var sqlSw = Stopwatch.StartNew();
        using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);
        using var command = new SqlCommand(query, sqlConnection);
        command.CommandTimeout = CommandTimeoutSeconds;

        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        /* Collect all rows first, then batch insert into DuckDB */
        var waitStats = new List<(string WaitType, long WaitingTasks, long WaitTimeMs, long SignalWaitTimeMs)>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var waitType = reader.GetString(0);

            /* Skip ignored wait types */
            if (ignoredWaits.Contains(waitType))
            {
                continue;
            }

            waitStats.Add((
                WaitType: waitType,
                WaitingTasks: reader.GetInt64(1),
                WaitTimeMs: reader.GetInt64(2),
                SignalWaitTimeMs: reader.GetInt64(3)
            ));
        }
        sqlSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;

        /* Insert into DuckDB with delta calculations using Appender for bulk performance */
        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("wait_stats"))
            {
                foreach (var stat in waitStats)
                {
                    var deltaKey = stat.WaitType;
                    var deltaWaitingTasks = _deltaCalculator.CalculateDelta(serverId, "wait_stats_tasks", deltaKey, stat.WaitingTasks);
                    var deltaWaitTimeMs = _deltaCalculator.CalculateDelta(serverId, "wait_stats_time", deltaKey, stat.WaitTimeMs);
                    var deltaSignalWaitTimeMs = _deltaCalculator.CalculateDelta(serverId, "wait_stats_signal", deltaKey, stat.SignalWaitTimeMs);

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())    /* collection_id BIGINT */
                       .AppendValue(collectionTime)            /* collection_time TIMESTAMP */
                       .AppendValue(serverId)                  /* server_id INTEGER */
                       .AppendValue(server.ServerName)         /* server_name VARCHAR */
                       .AppendValue(stat.WaitType)             /* wait_type VARCHAR */
                       .AppendValue(stat.WaitingTasks)         /* waiting_tasks_count BIGINT */
                       .AppendValue(stat.WaitTimeMs)           /* wait_time_ms BIGINT */
                       .AppendValue(stat.SignalWaitTimeMs)     /* signal_wait_time_ms BIGINT */
                       .AppendValue(deltaWaitingTasks)         /* delta_waiting_tasks BIGINT */
                       .AppendValue(deltaWaitTimeMs)           /* delta_wait_time_ms BIGINT */
                       .AppendValue(deltaSignalWaitTimeMs)     /* delta_signal_wait_time_ms BIGINT */
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} wait stats for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
