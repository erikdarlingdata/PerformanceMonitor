/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Viewer;

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Running Jobs grid's read — Lite's <c>GetRunningJobsAsync</c> ported to Postgres verbatim
    /// (runs against the <c>v_running_jobs</c> passthrough view): the latest snapshot only (the
    /// <c>collection_time = MAX(...)</c> self-subquery), ordered by current duration descending. All the
    /// derived columns (avg/p95/percent-of-average/is-running-long) are collector-side, so the read is
    /// a plain projection.
    /// $1 server_id.
    /// </summary>
    public const string RunningJobsSql = """
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
        FROM v_running_jobs
        WHERE server_id = $1
        AND   collection_time = (
            SELECT MAX(collection_time)
            FROM v_running_jobs
            WHERE server_id = $1
        )
        ORDER BY current_duration_seconds DESC
        """;

    /// <summary>
    /// The msdb-access warning derivation. Lite drives its "Running Jobs requires msdb access" banner
    /// from a live per-connection probe (<c>_hasMsdbAccess</c>) the viewer has no equivalent for — the
    /// viewer never touches the monitored SQL Server. Instead we read the running_jobs collector's most
    /// recent run outcome from the store: a login lacking msdb access makes the collector fail, which
    /// the Darling service records with Lite's status vocabulary (SUCCESS / PERMISSIONS / ERROR). The
    /// UI shows the banner when this latest status is PERMISSIONS or ERROR. collector_name is the
    /// literal 'running_jobs' (RunningJobsCollector.Name).
    /// $1 server_id.
    /// </summary>
    public const string RunningJobsCollectorStatusSql = """
        SELECT status
        FROM collection_log
        WHERE server_id = $1
        AND   collector_name = 'running_jobs'
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>The latest snapshot of running jobs for one server (latest collection_time only).</summary>
    public async Task<List<RunningJobRow>> GetRunningJobsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var items = new List<RunningJobRow>();

        await using var command = _dataSource.CreateCommand(RunningJobsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new RunningJobRow
            {
                CollectionTime = reader.GetDateTime(0),
                JobName = reader.GetString(1),
                JobId = reader.GetString(2),
                JobEnabled = reader.GetBoolean(3),
                StartTime = reader.GetDateTime(4),
                CurrentDurationSeconds = reader.GetInt64(5),
                AvgDurationSeconds = reader.GetInt64(6),
                P95DurationSeconds = reader.GetInt64(7),
                SuccessfulRunCount = reader.GetInt64(8),
                IsRunningLong = reader.GetBoolean(9),
                PercentOfAverage = reader.IsDBNull(10) ? null : reader.GetDecimal(10)
            });
        }

        return items;
    }

    /// <summary>
    /// The running_jobs collector's most recent run status (SUCCESS / PERMISSIONS / ERROR), or null
    /// when it has never run for this server. Feeds the Running Jobs tab's msdb-access banner.
    /// </summary>
    public async Task<string?> GetLatestRunningJobsCollectorStatusAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(RunningJobsCollectorStatusSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }
}

/// <summary>
/// One row of the Running Jobs grid — a currently-running SQL Agent job with its historical duration
/// comparison. Copied VERBATIM from Lite's <c>RunningJobRow</c> (LocalDataService.RunningJobs.cs): the
/// duration/percent/running-long display columns are all collector-side computations, and
/// <see cref="StartTimeLocal"/> routes the store's naive-UTC start_time through
/// <see cref="ViewerTimeHelper.ForDisplay"/> — the viewer's mode-aware Server/Local/UTC conversion every
/// other Darling timestamp also uses.
/// </summary>
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

    public string StartTimeLocal => ViewerTimeHelper.ForDisplay(StartTime).ToString("yyyy-MM-dd HH:mm:ss");

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
