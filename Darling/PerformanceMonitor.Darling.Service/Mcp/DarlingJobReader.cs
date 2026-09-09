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
///
/// <para><b>Server-local storage, UTC at the read boundary (load-bearing):</b> <c>running_jobs.start_time</c>
/// is the msdb Agent's LOCAL wall clock — <c>RunningJobsCollector</c> ships <c>ja.start_execution_date</c>
/// verbatim and computes <c>current_duration_seconds</c> against <c>GETDATE()</c> on the next line, so the
/// collector's own arithmetic is local-vs-local and the STORED frame has to stay local. This read de-skews to
/// naive UTC by the collected <c>server_properties.utc_offset_minutes</c> (V16) — the same expression
/// <c>DarlingDefaultTraceReader</c> and <c>ViewerDataService.SystemEvents</c> use. A server with no offset yet
/// collected falls back to 0 (treat local == UTC) and the single-row COALESCE CTE guarantees the cross join
/// never drops a job.</para>
///
/// <para><b>Why the returned value is converted and not merely labelled.</b> The tool emits
/// <c>collection_time</c> in the same payload and that is naive UTC, so a server-local <c>start_time</c>
/// beside it makes a job that started seconds ago read as having started by the server's whole offset earlier
/// — 4 hours on the production fleet, which is a long-running-job alert's exact signature. A suffix or a note
/// would leave the wrong value in the response for a reader to line up against UTC surfaces anyway. The
/// payload field name is unchanged; only the frame is.</para>
/// </summary>
internal static class DarlingJobReader
{
    /// <summary>One currently-running SQL Agent job with its historical duration comparison. The start time is
    /// naive UTC: the read de-skews the stored msdb-local value, so it shares the frame of
    /// <c>CollectionTime</c> and of every other timestamp the MCP surface returns.</summary>
    public sealed record RunningJobRow(
        DateTime CollectionTime, string JobName, string JobId, bool JobEnabled, DateTime StartTimeUtc,
        long CurrentDurationSeconds, long AvgDurationSeconds, long P95DurationSeconds, long SuccessfulRunCount,
        bool IsRunningLong, decimal? PercentOfAverage);

    /// <summary>
    /// The latest running-jobs snapshot for one server — Lite's <c>GetRunningJobsAsync</c> / the viewer's
    /// <c>RunningJobsSql</c>: the newest collection only, longest-running first. The columns are exactly those
    /// the alert engine's <c>DarlingAlertReadAdapter.AnomalousJobsSql</c> reads plus the display fields, with
    /// <c>start_time</c> de-skewed to naive UTC. The snapshot self-subquery stays on the naive-UTC
    /// <c>collection_time</c>, so which snapshot counts as latest does not depend on the offset, and the
    /// ordering stays on the collector-computed duration rather than on either clock. $1 server_id.
    ///
    /// <para>One collected offset covers the snapshot, so a job that started before a DST transition is
    /// de-skewed by the post-transition offset and is off by an hour. That is the same single-snapshot
    /// approximation <c>DarlingDefaultTraceReader</c> and #2992's <c>creation_time</c> de-skew make, stated
    /// here rather than implied.</para>
    /// </summary>
    public const string RunningJobsSql = """
        WITH svr AS (
            SELECT COALESCE((
                SELECT sp.utc_offset_minutes
                FROM server_properties AS sp
                WHERE sp.server_id = $1
                AND   sp.utc_offset_minutes IS NOT NULL
                ORDER BY sp.collection_time DESC
                LIMIT 1), 0) AS offset_minutes
        )
        SELECT
            rj.collection_time,
            rj.job_name,
            rj.job_id,
            rj.job_enabled,
            rj.start_time - make_interval(mins => svr.offset_minutes) AS start_time_utc,
            rj.current_duration_seconds,
            rj.avg_duration_seconds,
            rj.p95_duration_seconds,
            rj.successful_run_count,
            rj.is_running_long,
            rj.percent_of_average
        FROM v_running_jobs AS rj, svr
        WHERE rj.server_id = $1
        AND   rj.collection_time = (
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
