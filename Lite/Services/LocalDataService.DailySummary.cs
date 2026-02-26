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
    /// Gets daily summary for a specific date (or today if null).
    /// </summary>
    public async Task<DailySummaryRow?> GetDailySummaryAsync(int serverId, DateTime? summaryDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var targetDate = summaryDate?.Date ?? DateTime.UtcNow.Date;
        var dayStart = targetDate;
        var dayEnd = targetDate.AddDays(1);

        command.CommandText = @"
SELECT
    COALESCE(
        (SELECT SUM(delta_wait_time_ms) / 1000.0
         FROM v_wait_stats
         WHERE server_id = $1
         AND   collection_time >= $2 AND collection_time < $3
         AND   delta_wait_time_ms > 0), 0
    ) AS total_wait_sec,
    (SELECT wait_type
     FROM v_wait_stats
     WHERE server_id = $1
     AND   collection_time >= $2 AND collection_time < $3
     AND   delta_wait_time_ms > 0
     ORDER BY delta_wait_time_ms DESC
     LIMIT 1
    ) AS top_wait_type,
    COALESCE(
        (SELECT COUNT(DISTINCT query_hash)
         FROM v_query_stats
         WHERE server_id = $1
         AND   collection_time >= $2 AND collection_time < $3), 0
    ) AS unique_queries,
    COALESCE(
        (SELECT COUNT(*)
         FROM v_deadlocks
         WHERE server_id = $1
         AND   collection_time >= $2 AND collection_time < $3), 0
    ) AS deadlock_count,
    COALESCE(
        (SELECT COUNT(*)
         FROM v_blocked_process_reports
         WHERE server_id = $1
         AND   collection_time >= $2 AND collection_time < $3), 0
    ) AS blocking_events,
    COALESCE(
        (SELECT COUNT(*)
         FROM v_cpu_utilization_stats
         WHERE server_id = $1
         AND   sqlserver_cpu_utilization >= 80
         AND   collection_time >= $2 AND collection_time < $3), 0
    ) AS high_cpu_events,
    COALESCE(
        (SELECT COUNT(*)
         FROM v_collection_log
         WHERE server_id = $1
         AND   status = 'ERROR'
         AND   collection_time >= $2 AND collection_time < $3), 0
    ) AS collection_errors";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = dayStart });
        command.Parameters.Add(new DuckDBParameter { Value = dayEnd });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;

        var deadlocks = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));
        var blocking = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
        var highCpu = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));

        var health = "NORMAL";
        if (deadlocks > 0) health = "DEADLOCKS";
        else if (highCpu > 5) health = "CPU_CRITICAL";
        else if (blocking > 10) health = "BLOCKING";

        return new DailySummaryRow
        {
            SummaryDate = targetDate,
            TotalWaitTimeSec = reader.IsDBNull(0) ? 0m : Convert.ToDecimal(reader.GetValue(0)),
            TopWaitType = reader.IsDBNull(1) ? "" : reader.GetString(1),
            UniqueQueries = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
            DeadlockCount = deadlocks,
            BlockingEvents = blocking,
            HighCpuEvents = highCpu,
            CollectionErrors = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
            OverallHealth = health
        };
    }
}

public class DailySummaryRow
{
    public DateTime SummaryDate { get; set; }
    public decimal TotalWaitTimeSec { get; set; }
    public string TopWaitType { get; set; } = "";
    public long UniqueQueries { get; set; }
    public long DeadlockCount { get; set; }
    public long BlockingEvents { get; set; }
    public long HighCpuEvents { get; set; }
    public long CollectionErrors { get; set; }
    public string OverallHealth { get; set; } = "";

    public string SummaryDateFormatted => SummaryDate.ToString("yyyy-MM-dd");
    public string TotalWaitFormatted => TotalWaitTimeSec < 1000
        ? $"{TotalWaitTimeSec:N1} s"
        : $"{TotalWaitTimeSec / 60:N1} min";
}
