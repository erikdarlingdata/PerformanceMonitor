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
using System.Linq;
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
    private static readonly string[] DefaultPerfmonCounters =
    [
        /* I/O counters */
        "Forwarded Records/sec",
        "Page reads/sec",
        "Page writes/sec",
        "Checkpoint pages/sec",
        "Page lookups/sec",
        "Readahead pages/sec",
        "Background writer pages/sec",
        "Lazy writes/sec",
        "Full Scans/sec",
        "Index Searches/sec",
        "Page Splits/sec",
        "Free list stalls/sec",
        "Non-Page latch waits",
        "Page IO latch waits",
        "Page latch waits",
        /* Transaction counters */
        "Transactions/sec",
        "Longest Transaction Running Time",
        /* Locking counters */
        "Table Lock Escalations/sec",
        "Lock Requests/sec",
        "Lock Wait Time (ms)",
        "Lock Waits/sec",
        "Number of Deadlocks/sec",
        "Lock waits",
        "Processes blocked",
        "Lock Timeouts/sec",
        /* Memory counters */
        "Granted Workspace Memory (KB)",
        "Lock Memory (KB)",
        "Memory Grants Pending",
        "SQL Cache Memory (KB)",
        "Stolen Server Memory (KB)",
        "Target Server Memory (KB)",
        "Total Server Memory (KB)",
        "Memory grant queue waits",
        "Thread-safe memory objects waits",
        /* Compilation counters */
        "SQL Compilations/sec",
        "SQL Re-Compilations/sec",
        "Query optimizations/sec",
        "Reduced memory grants/sec",
        /* Batch and request counters */
        "Batch Requests/sec",
        "Requests completed/sec",
        "Active requests",
        "Queued requests",
        "Blocked tasks",
        "Active parallel threads",
        /* Log counters */
        "Log Flushes/sec",
        "Log Bytes Flushed/sec",
        "Log Flush Write Time (ms)",
        "Log buffer waits",
        "Log write waits",
        /* TempDB counters */
        "Version Store Size (KB)",
        "Free Space in tempdb (KB)",
        "Active Temp Tables",
        "Version Generation rate (KB/s)",
        "Version Cleanup rate (KB/s)",
        "Temp Tables Creation Rate",
        "Workfiles Created/sec",
        "Worktables Created/sec",
        /* Wait counters */
        "Network IO waits",
        "Wait for the worker"
    ];

    private static string[]? s_perfmonCounters;

    private static string[] GetPerfmonCounters()
    {
        if (s_perfmonCounters != null) return s_perfmonCounters;

        var configPath = Path.Combine(App.ConfigDirectory, "perfmon_counters.json");
        if (File.Exists(configPath))
        {
            try
            {
                var json = File.ReadAllText(configPath);
                var config = JsonSerializer.Deserialize<PerfmonCounterConfig>(json);
                if (config?.Counters?.Length > 0)
                {
                    s_perfmonCounters = config.Counters;
                    return s_perfmonCounters;
                }
            }
            catch { /* fall through to defaults */ }
        }

        s_perfmonCounters = DefaultPerfmonCounters;
        return s_perfmonCounters;
    }

    private class PerfmonCounterConfig
    {
        public string[] Counters { get; set; } = [];
    }

    /// <summary>
    /// Collects key performance counters from sys.dm_os_performance_counters.
    /// </summary>
    private async Task<int> CollectPerfmonStatsAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        var counters = GetPerfmonCounters();
        var counterList = string.Join(",\n    ", counters.Select(c => $"N'{c.Replace("'", "''")}'"));

        var query = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    object_name = RTRIM(pc.object_name),
    counter_name = RTRIM(pc.counter_name),
    instance_name = RTRIM(pc.instance_name),
    cntr_value = pc.cntr_value
FROM sys.dm_os_performance_counters AS pc
WHERE pc.counter_name IN (
    {counterList}
)
OPTION(RECOMPILE);";

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
        sqlSw.Stop();
        _lastSqlMs = sqlSw.ElapsedMilliseconds;

        var duckSw = Stopwatch.StartNew();

        using (var duckConnection = _duckDb.CreateConnection())
        {
            await duckConnection.OpenAsync(cancellationToken);

            using (var appender = duckConnection.CreateAppender("perfmon_stats"))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var objectName = reader.IsDBNull(0) ? "" : reader.GetString(0);
                    var counterName = reader.IsDBNull(1) ? "" : reader.GetString(1);
                    var instanceName = reader.IsDBNull(2) ? "" : reader.GetString(2);
                    var cntrValue = reader.GetInt64(3);

                    /* Delta for per-second counters */
                    var deltaKey = $"{objectName}|{counterName}|{instanceName}";
                    var deltaCntrValue = _deltaCalculator.CalculateDelta(serverId, "perfmon", deltaKey, cntrValue);

                    var row = appender.CreateRow();
                    row.AppendValue(GenerateCollectionId())
                       .AppendValue(collectionTime)
                       .AppendValue(serverId)
                       .AppendValue(server.ServerName)
                       .AppendValue(objectName)
                       .AppendValue(counterName)
                       .AppendValue(instanceName)
                       .AppendValue(cntrValue)
                       .AppendValue(deltaCntrValue)
                       .AppendValue(600) /* 10-minute interval */
                       .EndRow();

                    rowsCollected++;
                }
            }
        }

        duckSw.Stop();
        _lastDuckDbMs = duckSw.ElapsedMilliseconds;

        _logger?.LogDebug("Collected {RowCount} perfmon counters for server '{Server}'", rowsCollected, server.DisplayName);
        return rowsCollected;
    }
}
