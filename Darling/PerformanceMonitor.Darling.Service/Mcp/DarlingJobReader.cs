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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side read for the running-jobs MCP tool (<see cref="DarlingMcpJobTools"/>) — the SAME
/// <c>running_jobs</c> data Lite's <c>get_running_jobs</c> and the viewer's <c>ViewerDataService.RunningJobs</c>
/// read, adapted here so the MCP host never references the WPF viewer project. A STORED read of the latest
/// snapshot only (the <c>collection_time = MAX(...)</c> self-subquery), on the <c>v_running_jobs</c> passthrough
/// view. Every derived column (avg / p95 / percent-of-average / is-running-long) is collector-side, so the read
/// is a plain projection; the human-readable duration strings are formatted in the tool. Every SQL string is a
/// public const so Darling.Tests can pin the dialect + columns without a live Postgres.
/// </summary>
internal static class DarlingJobReader
{
    /// <summary>One currently-running SQL Agent job with its historical duration comparison.</summary>
    public sealed record RunningJobRow(
        DateTime CollectionTime, string JobName, string JobId, bool JobEnabled, DateTime StartTime,
        long CurrentDurationSeconds, long AvgDurationSeconds, long P95DurationSeconds, long SuccessfulRunCount,
        bool IsRunningLong, decimal? PercentOfAverage);

    /// <summary>
    /// The latest running-jobs snapshot for one server — Lite's <c>GetRunningJobsAsync</c> / the viewer's
    /// <c>RunningJobsSql</c>: the newest collection only, longest-running first. The columns are exactly those
    /// the alert engine's <c>DarlingAlertReadAdapter.AnomalousJobsSql</c> reads plus the display fields.
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

    public static async Task<List<RunningJobRow>> GetRunningJobsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<RunningJobRow>();
        await using var command = postgres.CreateCommand(RunningJobsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new RunningJobRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3),
                reader.GetDateTime(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                !reader.IsDBNull(9) && reader.GetBoolean(9),
                reader.IsDBNull(10) ? null : reader.GetDecimal(10)));
        }

        return rows;
    }

    /// <summary>Lite/Dashboard's job-duration display formatting (Xs / Xm Ys / Xh Ym).</summary>
    public static string FormatDuration(long seconds)
    {
        if (seconds < 60) return $"{seconds}s";
        if (seconds < 3600) return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds / 3600}h {(seconds % 3600) / 60}m";
    }
}
