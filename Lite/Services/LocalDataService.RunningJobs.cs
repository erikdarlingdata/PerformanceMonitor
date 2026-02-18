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
    /// Gets the latest snapshot of running jobs for a server.
    /// Returns only the most recent collection_time's data.
    /// </summary>
    public async Task<List<RunningJobRow>> GetRunningJobsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    collection_time,
    job_name,
    job_id,
    job_enabled,
    start_time,
    current_duration_seconds,
    avg_duration_seconds,
    p95_duration_seconds,
    successful_run_count,
    is_running_long,
    percent_of_average
FROM running_jobs
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM running_jobs
    WHERE server_id = $1
)
ORDER BY current_duration_seconds DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<RunningJobRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new RunningJobRow
            {
                CollectionTime = reader.GetDateTime(0),
                JobName = reader.GetString(1),
                JobId = reader.GetString(2),
                JobEnabled = reader.GetBoolean(3),
                StartTime = reader.GetDateTime(4),
                CurrentDurationSeconds = ToInt64(reader.GetValue(5)),
                AvgDurationSeconds = ToInt64(reader.GetValue(6)),
                P95DurationSeconds = ToInt64(reader.GetValue(7)),
                SuccessfulRunCount = ToInt64(reader.GetValue(8)),
                IsRunningLong = reader.GetBoolean(9),
                PercentOfAverage = reader.IsDBNull(10) ? null : Convert.ToDecimal(reader.GetValue(10))
            });
        }

        return items;
    }

    /// <summary>
    /// Gets running jobs that exceed the anomaly threshold (multiplier x average duration).
    /// Excludes jobs with avg < 60s to avoid noise from very short jobs.
    /// </summary>
    public async Task<List<AnomalousJobInfo>> GetAnomalousJobsAsync(int serverId, int multiplier)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var thresholdPercent = (decimal)(multiplier * 100);

        command.CommandText = @"
SELECT
    job_name,
    job_id,
    current_duration_seconds,
    avg_duration_seconds,
    p95_duration_seconds,
    percent_of_average,
    start_time
FROM running_jobs
WHERE server_id = $1
AND collection_time = (SELECT MAX(collection_time) FROM running_jobs WHERE server_id = $1)
AND avg_duration_seconds >= 60
AND percent_of_average >= $2
ORDER BY percent_of_average DESC
LIMIT 5";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = thresholdPercent });

        var items = new List<AnomalousJobInfo>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new AnomalousJobInfo
            {
                JobName = reader.GetString(0),
                JobId = reader.GetString(1),
                CurrentDurationSeconds = ToInt64(reader.GetValue(2)),
                AvgDurationSeconds = ToInt64(reader.GetValue(3)),
                P95DurationSeconds = ToInt64(reader.GetValue(4)),
                PercentOfAverage = reader.IsDBNull(5) ? null : Convert.ToDecimal(reader.GetValue(5)),
                StartTime = reader.GetDateTime(6)
            });
        }

        return items;
    }
}

public class AnomalousJobInfo
{
    public string JobName { get; set; } = "";
    public string JobId { get; set; } = "";
    public long CurrentDurationSeconds { get; set; }
    public long AvgDurationSeconds { get; set; }
    public long P95DurationSeconds { get; set; }
    public decimal? PercentOfAverage { get; set; }
    public DateTime StartTime { get; set; }
}

public class RunningJobRow
{
    public DateTime CollectionTime { get; set; }
    public string JobName { get; set; } = "";
    public string JobId { get; set; } = "";
    public bool JobEnabled { get; set; }
    public DateTime StartTime { get; set; }
    public long CurrentDurationSeconds { get; set; }
    public long AvgDurationSeconds { get; set; }
    public long P95DurationSeconds { get; set; }
    public long SuccessfulRunCount { get; set; }
    public bool IsRunningLong { get; set; }
    public decimal? PercentOfAverage { get; set; }

    public string StartTimeLocal => StartTime.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");

    public string CurrentDurationFormatted => FormatDuration(CurrentDurationSeconds);
    public string AvgDurationFormatted => FormatDuration(AvgDurationSeconds);
    public string P95DurationFormatted => FormatDuration(P95DurationSeconds);

    public string PercentOfAverageFormatted => PercentOfAverage.HasValue
        ? $"{PercentOfAverage:F0}%"
        : "N/A";

    public string RunningLongDisplay => IsRunningLong ? "Yes" : "";

    private static string FormatDuration(long seconds)
    {
        if (seconds < 60) return $"{seconds}s";
        if (seconds < 3600) return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds / 3600}h {(seconds % 3600) / 60}m";
    }
}
