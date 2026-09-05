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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One retained job-history row for the Job History tab (issue #1433) — a single collected
/// <c>sysjobhistory</c> row (a step or the step_id 0 job-outcome). The Darling twin of Lite's
/// <c>JobHistoryRow</c>: the same duration / step / retry display helpers and the same failure /
/// long-runtime / retry flags for the grid's color-coding. run_datetime is the monitored server's LOCAL
/// wall clock, so <see cref="ViewerDataService.GetJobHistoryAsync"/> de-skews it to naive-UTC in SQL
/// (subtracting <c>server_properties.utc_offset_minutes</c>) before it reaches this row — exactly like the
/// Default Trace reader — so <see cref="RunTimeLocal"/> renders through the same
/// <see cref="ViewerTimeHelper.ForDisplay"/> as every other viewer grid and sorts consistently with them.
/// </summary>
public sealed class ViewerJobHistoryRow
{
    public int ServerId { get; init; }

    /// <summary>The operator's display alias when one is registered, the raw collected name otherwise
    /// (#2126 — the Server column and filter combo show the same names every other tab does).</summary>
    public string ServerName { get; init; } = "";

    public long InstanceId { get; init; }
    public string JobId { get; init; } = "";
    public string JobName { get; init; } = "";
    public bool JobEnabled { get; init; }
    public string? CategoryName { get; init; }
    public int StepId { get; init; }
    public string? StepName { get; init; }
    public int RunStatus { get; init; }
    public string? RunStatusDesc { get; init; }

    /// <summary>run_datetime de-skewed to naive-UTC in SQL (server-local minus utc_offset_minutes).</summary>
    public DateTime? RunDateTimeUtc { get; init; }

    public long RunDurationSeconds { get; init; }
    public int RetriesAttempted { get; init; }
    public string? Message { get; init; }
    public DateTime? LastSuccessfulRunUtc { get; init; }
    public bool IsLongRunning { get; init; }

    /// <summary>Stored naive-UTC; shown in the viewer machine's local time (the viewer convention).</summary>
    public string RunTimeLocal => RunDateTimeUtc is { } t
        ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss")
        : "";

    public string LastSuccessfulRunLocal => LastSuccessfulRunUtc is { } t
        ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss")
        : "Never";

    public string DurationFormatted => FormatDuration(RunDurationSeconds);

    public string StepDisplay => StepId == 0 ? "(Job outcome)" : $"{StepId}: {StepName}";

    public string RetriesDisplay => RetriesAttempted > 0 ? RetriesAttempted.ToString(System.Globalization.CultureInfo.InvariantCulture) : "";

    public string JobEnabledDisplay => JobEnabled ? "Yes" : "No";

    public bool IsFailed => RunStatus == 0;
    public bool IsSucceeded => RunStatus == 1;
    public bool IsRetry => RunStatus == 2;
    public bool IsCanceled => RunStatus == 3;

    private static string FormatDuration(long seconds)
    {
        if (seconds < 60) return $"{seconds}s";
        if (seconds < 3600) return $"{seconds / 60}m {seconds % 60}s";
        return $"{seconds / 3600}h {(seconds % 3600) / 60}m";
    }
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// Retained SQL Agent job-run history for the fleet Job History tab. Reads the BASE
    /// <c>job_history</c> table (no <c>v_*</c> view — a collector added after V14 has none; the bare name
    /// resolves through the store's <c>search_path</c>, like default_trace_events / server_properties),
    /// windowing on the run time so "last N hours/days" means jobs that RAN in that window (a first-run
    /// backfill of a year of history does not flood a short window the way a collection_time filter would).
    /// <para>
    /// run_datetime is the monitored server's LOCAL wall clock, so it is DE-SKEWED to naive-UTC in SQL —
    /// subtracting the collected <c>server_properties.utc_offset_minutes</c> (per-server latest, 0 when none
    /// yet) — and windowed against the naive-UTC bounds ($1), so the returned timestamps share the viewer's
    /// UTC frame and render/sort consistently on the tab (the same de-skew the Default Trace reader uses).
    /// Long-runtime is computed reader-side via a per-job window function (a step_id 0 outcome exceeding 2x
    /// its job's average successful-outcome duration, floored at 60s), and each row carries its job's last
    /// successful outcome run. With no <paramref name="serverId"/> it aggregates ALL servers (the tab
    /// default); with one it scopes to that server (the Server filter combo). server_name resolves through
    /// the <c>servers</c> registry to the operator's display alias when one exists (#2126), so the tab
    /// speaks the same names as the rest of the viewer.
    /// </para>
    /// </summary>
    public async Task<List<ViewerJobHistoryRow>> GetJobHistoryAsync(
        DateTime sinceUtc, int? serverId = null, int limit = 2000, CancellationToken cancellationToken = default)
    {
        var serverFilter = serverId.HasValue ? "AND   jh.server_id = $2" : string.Empty;
        var limitParam = serverId.HasValue ? "$3" : "$2";

        var sql = $@"
WITH svr AS (
    SELECT DISTINCT ON (server_id)
        server_id,
        utc_offset_minutes
    FROM server_properties
    WHERE utc_offset_minutes IS NOT NULL
    ORDER BY server_id, collection_time DESC
),
base AS (
    SELECT
        jh.server_id,
        COALESCE(reg.display_name, jh.server_name) AS server_name,
        jh.instance_id,
        jh.job_id,
        jh.job_name,
        jh.job_enabled,
        jh.category_name,
        jh.step_id,
        jh.step_name,
        jh.run_status,
        jh.run_status_desc,
        jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) AS run_datetime_utc,
        jh.run_duration_seconds,
        jh.retries_attempted,
        jh.message,
        AVG(CASE WHEN jh.step_id = 0 AND jh.run_status = 1 THEN jh.run_duration_seconds END)
            OVER (PARTITION BY jh.server_id, jh.job_id) AS avg_success_duration,
        MAX(CASE WHEN jh.step_id = 0 AND jh.run_status = 1
                 THEN jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) END)
            OVER (PARTITION BY jh.server_id, jh.job_id) AS last_success_run_utc
    FROM job_history AS jh
    LEFT JOIN svr ON svr.server_id = jh.server_id
    LEFT JOIN servers AS reg ON reg.server_id = jh.server_id
    WHERE jh.run_datetime - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) >= $1
    {serverFilter}
)
SELECT
    server_id,
    server_name,
    instance_id,
    job_id,
    job_name,
    job_enabled,
    category_name,
    step_id,
    step_name,
    run_status,
    run_status_desc,
    run_datetime_utc,
    run_duration_seconds,
    retries_attempted,
    message,
    last_success_run_utc,
    CASE
        WHEN step_id = 0
        AND  avg_success_duration IS NOT NULL
        AND  avg_success_duration > 0
        AND  run_duration_seconds > avg_success_duration * 2
        AND  run_duration_seconds > 60
        THEN true
        ELSE false
    END AS is_long_running
FROM base
ORDER BY run_datetime_utc DESC, instance_id DESC
LIMIT {limitParam}";

        var rows = new List<ViewerJobHistoryRow>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified) });
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerJobHistoryRow
            {
                ServerId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                InstanceId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                JobId = reader.IsDBNull(3) ? "" : reader.GetString(3),
                JobName = reader.IsDBNull(4) ? "" : reader.GetString(4),
                JobEnabled = !reader.IsDBNull(5) && reader.GetBoolean(5),
                CategoryName = reader.IsDBNull(6) ? null : reader.GetString(6),
                StepId = reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                StepName = reader.IsDBNull(8) ? null : reader.GetString(8),
                RunStatus = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                RunStatusDesc = reader.IsDBNull(10) ? null : reader.GetString(10),
                RunDateTimeUtc = reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                RunDurationSeconds = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                RetriesAttempted = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                Message = reader.IsDBNull(14) ? null : reader.GetString(14),
                LastSuccessfulRunUtc = reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                IsLongRunning = !reader.IsDBNull(16) && reader.GetBoolean(16),
            });
        }

        return rows;
    }

    /// <summary>
    /// The latest SQL Agent status snapshot per server (issue #1433 Phase 2) — Running/Stopped, startup
    /// type, and next scheduled run — read from the base <c>agent_status</c> table (newest row per server).
    /// <c>next_scheduled_run</c> is the server's local wall clock (from msdb), so it is de-skewed to
    /// naive-UTC in SQL (like the job run times) and rendered in the viewer's local time. With no
    /// <paramref name="serverId"/> it returns one row per server (the fleet header roll-up); with one it
    /// scopes to that server. The tab header consumes this; the "Agent Not Running" self-alert reads the same
    /// <c>agent_status</c> data service-side.
    /// </summary>
    public async Task<List<ViewerAgentStatusRow>> GetAgentStatusAsync(int? serverId = null, CancellationToken cancellationToken = default)
    {
        var serverFilter = serverId.HasValue ? "WHERE a.server_id = $1" : string.Empty;

        var sql = $@"
WITH svr AS (
    SELECT DISTINCT ON (server_id)
        server_id,
        utc_offset_minutes
    FROM server_properties
    WHERE utc_offset_minutes IS NOT NULL
    ORDER BY server_id, collection_time DESC
),
latest AS (
    SELECT
        a.server_id,
        COALESCE(reg.display_name, a.server_name) AS server_name,
        a.agent_running,
        a.agent_status_desc,
        a.agent_startup_desc,
        a.next_scheduled_run - make_interval(mins => COALESCE(svr.utc_offset_minutes, 0)) AS next_scheduled_run_utc,
        ROW_NUMBER() OVER (PARTITION BY a.server_id ORDER BY a.collection_time DESC) AS rn
    FROM agent_status AS a
    LEFT JOIN svr ON svr.server_id = a.server_id
    LEFT JOIN servers AS reg ON reg.server_id = a.server_id
    {serverFilter}
)
SELECT
    server_id,
    server_name,
    agent_running,
    agent_status_desc,
    agent_startup_desc,
    next_scheduled_run_utc
FROM latest
WHERE rn = 1
ORDER BY server_name";

        var rows = new List<ViewerAgentStatusRow>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerAgentStatusRow
            {
                ServerId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                ServerName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                AgentRunning = !reader.IsDBNull(2) && reader.GetBoolean(2),
                AgentStatusDesc = reader.IsDBNull(3) ? null : reader.GetString(3),
                AgentStartupDesc = reader.IsDBNull(4) ? null : reader.GetString(4),
                NextScheduledRunUtc = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            });
        }

        return rows;
    }
}

/// <summary>
/// The latest SQL Agent status snapshot for one server (issue #1433 Phase 2) — the Darling twin of Lite's
/// <c>AgentStatusRow</c>. Drives the Job History tab header and the "Agent Not Running" self-alert.
/// next_scheduled_run is de-skewed to naive-UTC in SQL and rendered in the viewer's local time.
/// </summary>
public sealed class ViewerAgentStatusRow
{
    public int ServerId { get; init; }
    public string ServerName { get; init; } = "";
    public bool AgentRunning { get; init; }
    public string? AgentStatusDesc { get; init; }
    public string? AgentStartupDesc { get; init; }
    public DateTime? NextScheduledRunUtc { get; init; }

    public string StatusDisplay => AgentRunning ? "Running" : (AgentStatusDesc ?? "Stopped");

    public string NextScheduledRunLocal => NextScheduledRunUtc is { } t
        ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss")
        : "None scheduled";
}
