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
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Retained SQL Agent job-run history for the fleet Job History tab (issue #1433). Reads the archive
    /// view <c>v_job_history</c> (hot DuckDB + parquet, deduped by the collector's instance_id watermark),
    /// windowing on <c>run_datetime</c> — the time the job actually RAN — so "last N hours/days" means jobs
    /// that ran in that window (a first-run backfill of a year of history does NOT flood a short window the
    /// way a collection_time filter would). Long-runtime is computed reader-side via a per-job window
    /// function (a step_id 0 outcome row exceeding 2x its job's average successful-outcome duration, floored
    /// at 60s so tiny jobs never flag), and each row carries its job's last successful outcome run.
    /// <para>
    /// run_datetime is the monitored server's LOCAL wall clock (decoded from run_date/run_time), so it is
    /// windowed against and displayed in local wall-clock time — the time SSMS shows for the run — rather
    /// than re-converted. With no <paramref name="serverId"/> the read aggregates ALL servers (the tab
    /// default); with one it scopes to that server (the Server filter combo).
    /// </para>
    /// </summary>
    public async Task<List<JobHistoryRow>> GetJobHistoryAsync(int hoursBack = 24, int limit = 1000, int? serverId = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* run_datetime is server-local wall clock; the cutoff is a local wall-clock instant so a same-tz
           Lite host (the common case) windows "last N hours" against the run time the way a DBA reads it. */
        var cutoff = DateTime.Now.AddHours(-hoursBack);

        var serverFilter = serverId.HasValue ? "AND   server_id = $2" : string.Empty;
        var limitParam = serverId.HasValue ? "$3" : "$2";

        command.CommandText = $@"
WITH base AS (
    SELECT
        collection_time,
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
        run_datetime,
        run_duration_seconds,
        retries_attempted,
        message,
        AVG(CASE WHEN step_id = 0 AND run_status = 1 THEN run_duration_seconds END)
            OVER (PARTITION BY server_id, job_id) AS avg_success_duration,
        MAX(CASE WHEN step_id = 0 AND run_status = 1 THEN run_datetime END)
            OVER (PARTITION BY server_id, job_id) AS last_success_run
    FROM v_job_history
    WHERE run_datetime >= $1
    {serverFilter}
)
SELECT
    collection_time,
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
    run_datetime,
    run_duration_seconds,
    retries_attempted,
    message,
    last_success_run,
    CASE
        WHEN step_id = 0
        AND  avg_success_duration IS NOT NULL
        AND  avg_success_duration > 0
        AND  run_duration_seconds > avg_success_duration * 2
        AND  run_duration_seconds > 60
        THEN TRUE
        ELSE FALSE
    END AS is_long_running
FROM base
ORDER BY run_datetime DESC, instance_id DESC
LIMIT {limitParam}";

        command.Parameters.Add(new DuckDBParameter { Value = cutoff });
        if (serverId.HasValue)
            command.Parameters.Add(new DuckDBParameter { Value = serverId.Value });
        command.Parameters.Add(new DuckDBParameter { Value = limit });

        var items = new List<JobHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new JobHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                ServerId = (int)ToInt64(reader.GetValue(1)),
                ServerName = reader.GetString(2),
                InstanceId = ToInt64(reader.GetValue(3)),
                JobId = reader.GetString(4),
                JobName = reader.GetString(5),
                JobEnabled = reader.GetBoolean(6),
                CategoryName = reader.IsDBNull(7) ? null : reader.GetString(7),
                StepId = (int)ToInt64(reader.GetValue(8)),
                StepName = reader.IsDBNull(9) ? null : reader.GetString(9),
                RunStatus = (int)ToInt64(reader.GetValue(10)),
                RunStatusDesc = reader.IsDBNull(11) ? null : reader.GetString(11),
                RunDateTime = reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                RunDurationSeconds = ToInt64(reader.GetValue(13)),
                RetriesAttempted = (int)ToInt64(reader.GetValue(14)),
                Message = reader.IsDBNull(15) ? null : reader.GetString(15),
                LastSuccessfulRun = reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                IsLongRunning = !reader.IsDBNull(17) && reader.GetBoolean(17),
            });
        }

        return items;
    }

    /// <summary>
    /// The latest SQL Agent status snapshot per server (issue #1433 Phase 2) — Running/Stopped, startup
    /// type, and next scheduled run — read from the hot <c>agent_status</c> table (the newest row per
    /// server via a window function). With no <paramref name="serverId"/> it returns one row per server (the
    /// fleet header summary); with one it returns just that server's row (the Server-filtered header). The
    /// Job History tab shows this in its header; the "Agent Not Running" alert (Darling) reads the same data.
    /// </summary>
    public async Task<List<AgentStatusRow>> GetAgentStatusAsync(int? serverId = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var serverFilter = serverId.HasValue ? "WHERE server_id = $1" : string.Empty;

        /* collection_time comes back so the caller can refuse to present a stale reading as current, and
           ever_seen_running so it can tell "Agent is off right now" apart from "this server has never run
           Agent" — a container built without it, Express, a Linux-minimal image. Without those two columns a
           header can only say "Stopped", which is misleading on the second case and wrong on the first. */
        command.CommandText = $@"
SELECT
    server_id,
    server_name,
    agent_running,
    agent_status_desc,
    agent_startup_desc,
    next_scheduled_run,
    collection_time,
    ever_seen_running
FROM (
    SELECT
        server_id,
        server_name,
        agent_running,
        agent_status_desc,
        agent_startup_desc,
        next_scheduled_run,
        collection_time,
        MAX(CASE WHEN agent_running THEN 1 ELSE 0 END) OVER (PARTITION BY server_id) = 1 AS ever_seen_running,
        ROW_NUMBER() OVER (PARTITION BY server_id ORDER BY collection_time DESC) AS rn
    FROM agent_status
    {serverFilter}
) t
WHERE rn = 1
ORDER BY server_name";

        if (serverId.HasValue)
            command.Parameters.Add(new DuckDBParameter { Value = serverId.Value });

        var items = new List<AgentStatusRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new AgentStatusRow
            {
                ServerId = (int)ToInt64(reader.GetValue(0)),
                ServerName = reader.GetString(1),
                AgentRunning = reader.GetBoolean(2),
                AgentStatusDesc = reader.IsDBNull(3) ? null : reader.GetString(3),
                AgentStartupDesc = reader.IsDBNull(4) ? null : reader.GetString(4),
                NextScheduledRun = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                CollectionTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                EverSeenRunning = !reader.IsDBNull(7) && reader.GetBoolean(7),
            });
        }

        return items;
    }
}

/// <summary>
/// One retained job-history row for the Job History tab — a single <c>sysjobhistory</c> row (a step or the
/// step_id 0 job-outcome). Display helpers mirror <see cref="RunningJobRow"/>'s duration/local-time
/// formatting; the boolean flags drive the grid's failure / long-runtime / retry color-coding.
/// </summary>
public class JobHistoryRow
{
    public DateTime CollectionTime { get; set; }
    public int ServerId { get; set; }
    public string ServerName { get; set; } = "";
    public long InstanceId { get; set; }
    public string JobId { get; set; } = "";
    public string JobName { get; set; } = "";
    public bool JobEnabled { get; set; }
    public string? CategoryName { get; set; }
    public int StepId { get; set; }
    public string? StepName { get; set; }
    public int RunStatus { get; set; }
    public string? RunStatusDesc { get; set; }
    public DateTime? RunDateTime { get; set; }
    public long RunDurationSeconds { get; set; }
    public int RetriesAttempted { get; set; }
    public string? Message { get; set; }
    public DateTime? LastSuccessfulRun { get; set; }
    public bool IsLongRunning { get; set; }

    /// <summary>run_datetime is the server's local wall clock; shown as-is (the time SSMS shows).</summary>
    public string RunTimeLocal => RunDateTime?.ToString("yyyy-MM-dd HH:mm:ss") ?? "";

    public string LastSuccessfulRunLocal => LastSuccessfulRun?.ToString("yyyy-MM-dd HH:mm:ss") ?? "Never";

    public string DurationFormatted => FormatDuration(RunDurationSeconds);

    public string StepDisplay => StepId == 0
        ? "(Job outcome)"
        : $"{StepId}: {StepName}";

    public string RetriesDisplay => RetriesAttempted > 0 ? RetriesAttempted.ToString() : "";

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

/// <summary>
/// The latest SQL Agent status snapshot for one server (issue #1433 Phase 2) — drives the Job History tab
/// header, and Darling's "Agent Not Running" alert reads the same collected data (Lite raises no such alert
/// itself). next_scheduled_run is the server's local wall clock (from
/// msdb), shown as-is like the job run times.
/// </summary>
public class AgentStatusRow
{
    public int ServerId { get; set; }
    public string ServerName { get; set; } = "";
    public bool AgentRunning { get; set; }
    public string? AgentStatusDesc { get; set; }
    public string? AgentStartupDesc { get; set; }
    public DateTime? NextScheduledRun { get; set; }

    /// <summary>When this snapshot was collected. Null only on a row that predates the column being read.</summary>
    public DateTime? CollectionTime { get; set; }

    /// <summary>Has SQL Agent been observed RUNNING on this server at any point in retained history? False for a
    /// target where Agent is off by design — a container built without it, Express, a Linux-minimal image.</summary>
    public bool EverSeenRunning { get; set; }

    /// <summary>A reading older than this is not presented as current. Mirrors the headless service's
    /// <c>StaleWindow</c>, so both surfaces refuse to judge on the same age of data — now literally, via
    /// the shared constant rather than a numerically-equal copy (#2794).</summary>
    public static readonly TimeSpan StaleWindow =
        TimeSpan.FromMinutes(ServerHealthThresholds.CollectionStoppedMinutesDefault);

    /// <summary>True when the newest snapshot is too old to describe the server right now — collection stopped,
    /// the server went away, or the collector is failing. A stale reading must never render as a current state.</summary>
    public bool IsStale => CollectionTime is null || DateTime.UtcNow - CollectionTime.Value >= StaleWindow;

    /// <summary>
    /// What to SHOW for Agent, which is not the same question as what the last row said.
    ///
    /// <para>Three cases the old <c>Running</c>/<c>Stopped</c> pair collapsed wrongly. A stale reading is
    /// <c>unknown</c>, not "Stopped" — a server nobody has collected from in days is not evidence Agent is
    /// down. A server where Agent has NEVER been seen running is <c>not present</c>, not "Stopped" — nothing
    /// stopped, the target simply does not run Agent, and Lite/Darling collect in-process so nothing here
    /// depends on it. Only a fresh reading on a server that HAS run Agent is a genuine "Stopped".</para>
    /// </summary>
    public string StatusDisplay =>
        IsStale ? "unknown (stale)"
        : AgentRunning ? "Running"
        : EverSeenRunning ? (AgentStatusDesc ?? "Stopped")
        : "not present";

    /// <summary>Does this row warrant the attention (red) treatment? Only a fresh, genuinely-stopped Agent on a
    /// server that runs one. Stale and never-present are neutral — they are absence of signal, not a problem.</summary>
    public bool IsAgentProblem => !IsStale && !AgentRunning && EverSeenRunning;

    public string NextScheduledRunLocal => NextScheduledRun?.ToString("yyyy-MM-dd HH:mm:ss") ?? "None scheduled";
}
