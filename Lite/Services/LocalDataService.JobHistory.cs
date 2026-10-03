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
using System.Linq;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
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
    /// run_datetime is the monitored server's LOCAL wall clock (decoded from run_date/run_time), and the grid
    /// still shows it as stored — the time SSMS shows for the run — rather than re-converted. The WINDOW is
    /// another matter (#4966): "the last N hours" is an exact UTC span, so it is worked out on THAT server's
    /// clock (<see cref="GetServerClockAsync"/>, as the Default Trace read does) and not on the host's: a server
    /// in another zone used to shift the window by the zone difference. Per server, the SQL pre-filters on the
    /// server-local start of the span, widened by an hour, and each row's run time is converted to UTC with the
    /// clock at the run's own date (<see cref="JobHistoryRow.RunDateTimeUtc"/>) and held to the exact span. With
    /// no <paramref name="serverId"/> the read aggregates ALL servers (the tab default), one clock read and one
    /// bounded read per server, merged newest first by the real instant of each run and cut to
    /// <paramref name="limit"/>; with one it scopes to that server (the Server filter combo). A server with no
    /// collected clock yet is windowed on the machine's own (<see cref="ServerTimeHelper.ClockForServer(ServerClock?, ServerClock?)"/>),
    /// which is what this read did for every server before. The Darling viewer's twin converts through the same
    /// clock and shows the instant in its display zone.
    /// </para>
    /// </summary>
    public async Task<List<JobHistoryRow>> GetJobHistoryAsync(int hoursBack = 24, int limit = 1000, int? serverId = null)
    {
        var windowStartUtc = DateTime.UtcNow.AddHours(-hoursBack);

        var serverIds = serverId.HasValue
            ? new List<int> { serverId.Value }
            : await ReadJobHistoryServerIdsAsync(windowStartUtc);

        var rows = new List<JobHistoryRow>();
        foreach (var id in serverIds)
        {
            var clock = await ReadJobHistoryClockAsync(id);
            rows.AddRange(await ReadJobHistoryForServerAsync(id, clock, windowStartUtc, limit));
        }

        return ApplyJobHistoryWindow(rows, windowStartUtc, limit);
    }

    /// <summary>
    /// Every server with a run at or after the earliest server-local instant any server's pre-filter can start at: the
    /// window's UTC start less 12 hours (the westernmost zone, UTC-12) and the hour the pre-filter widens by. A server
    /// whose own window starts later is found here too and its own read narrows it; the list is where the per-server
    /// reads and clock reads come from, so a server with no run that recent costs neither.
    /// </summary>
    private async Task<List<int>> ReadJobHistoryServerIdsAsync(DateTime windowStartUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT DISTINCT server_id
FROM v_job_history
WHERE run_datetime >= $1
ORDER BY server_id";
        command.Parameters.Add(new DuckDBParameter { Value = windowStartUtc.AddHours(-13) });

        var ids = new List<int>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add((int)ToInt64(reader.GetValue(0)));

        return ids;
    }

    /// <summary>
    /// The clock one server's window is worked out on (#4966): its collected clock (<see cref="GetServerClockAsync"/>),
    /// else the machine's, the end of the chain the Alert History tab uses
    /// (<see cref="ServerTimeHelper.ClockForServer(ServerClock?, ServerClock?)"/>). This read has no open tab to ask, so
    /// that middle link is empty. A clock read that fails leaves the server on the machine's clock rather than failing
    /// every server's runs, as <c>AlertsHistoryTab.ReadCollectedClocksAsync</c> does.
    /// </summary>
    internal async Task<ServerClock> ReadJobHistoryClockAsync(int serverId)
    {
        ServerClock? collected = null;
        try
        {
            collected = await GetServerClockAsync(serverId);
        }
        catch (Exception ex)
        {
            AppLogger.Debug("JobHistory", $"Server clock read failed for server {serverId}, its window takes the machine's clock: {ex.Message}");
        }

        return ServerTimeHelper.ClockForServer(collected, openTabClock: null);
    }

    /// <summary>
    /// The exact window, after the server-local to UTC conversion: keeps the runs at or after
    /// <paramref name="windowStartUtc"/> (each server's SQL pre-filter is widened by an hour, so a run just before the
    /// window can still be in the set), orders them newest first by their real instant (two servers' stored wall clocks
    /// are in different zones, so the raw order is not the order the runs happened in), and keeps the newest
    /// <paramref name="limit"/>. The server id is the last tie-break so the order is the same on every read.
    /// </summary>
    internal static List<JobHistoryRow> ApplyJobHistoryWindow(List<JobHistoryRow> rows, DateTime windowStartUtc, int limit)
    {
        return rows
            .Where(r => r.RunDateTimeUtc is { } runUtc && runUtc >= windowStartUtc)
            .OrderByDescending(r => r.RunDateTimeUtc)
            .ThenByDescending(r => r.InstanceId)
            .ThenByDescending(r => r.ServerId)
            .Take(Math.Max(limit, 0))
            .ToList();
    }

    /// <summary>One server's runs: the newest <paramref name="limit"/> at or after the server-local start of the window,
    /// each with its run time converted to UTC through <paramref name="clock"/>. Not yet held to the exact window.</summary>
    private async Task<List<JobHistoryRow>> ReadJobHistoryForServerAsync(int serverId, ServerClock clock, DateTime windowStartUtc, int limit)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* The server-local start of the exact UTC window: job_stats' lower bound, so the per-job average and last
           success cover the window the way they always have. base pre-filters an hour earlier, which covers a
           daylight-saving change inside the span; ApplyJobHistoryWindow drops what that lets in (#4966). */
        var statsStart = clock.ToServerLocal(windowStartUtc);
        var preFilterStart = statsStart.AddHours(-1);

        /* #4229 (Darling parity): the per-job average/max used to run as a window function OVER every step
           row the window matched, forcing a full sort/aggregate of the whole set before ORDER BY/LIMIT could
           trim it. job_stats computes it once per (server_id, job_id) with a GROUP BY over just the step_id-0
           SUCCESS rows, filtered by the same window/server predicates as base; base's own ORDER BY/LIMIT then
           runs unencumbered by any aggregation, and the join attaches the per-job figures to only the rows
           that survive it. Row selection and values are unchanged — see
           ViewerDataService.JobHistory.cs's BuildJobHistorySql for the full argument, identical here. */
        command.CommandText = $@"
WITH job_stats AS (
    SELECT
        server_id,
        job_id,
        AVG(run_duration_seconds) AS avg_success_duration,
        MAX(run_datetime) AS last_success_run
    FROM v_job_history
    WHERE step_id = 0
    AND   run_status = 1
    AND   run_datetime >= $3
    AND   server_id = $2
    GROUP BY server_id, job_id
),
base AS (
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
        message
    FROM v_job_history
    WHERE run_datetime >= $1
    AND   server_id = $2
    ORDER BY run_datetime DESC, instance_id DESC
    LIMIT $4
)
SELECT
    base.collection_time,
    base.server_id,
    base.server_name,
    base.instance_id,
    base.job_id,
    base.job_name,
    base.job_enabled,
    base.category_name,
    base.step_id,
    base.step_name,
    base.run_status,
    base.run_status_desc,
    base.run_datetime,
    base.run_duration_seconds,
    base.retries_attempted,
    base.message,
    job_stats.last_success_run,
    CASE
        WHEN base.step_id = 0
        AND  job_stats.avg_success_duration IS NOT NULL
        AND  job_stats.avg_success_duration > 0
        AND  base.run_duration_seconds > job_stats.avg_success_duration * 2
        AND  base.run_duration_seconds > 60
        THEN TRUE
        ELSE FALSE
    END AS is_long_running
FROM base
LEFT JOIN job_stats
    ON  job_stats.server_id = base.server_id
    AND job_stats.job_id = base.job_id
ORDER BY base.run_datetime DESC, base.instance_id DESC";

        command.Parameters.Add(new DuckDBParameter { Value = preFilterStart });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = statsStart });
        command.Parameters.Add(new DuckDBParameter { Value = limit });

        var items = new List<JobHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* The stored wall clock stays as it is for the grid; the instant is the same time through the server's clock at
               the run's own date, so a run on the far side of a daylight-saving change is not an hour off (#4966). */
            DateTime? runDateTime = reader.IsDBNull(12) ? null : reader.GetDateTime(12);
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
                RunDateTime = runDateTime,
                RunDateTimeUtc = runDateTime.HasValue ? clock.ToUtc(runDateTime.Value) : null,
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
    /// type, and next scheduled run — read from <c>v_agent_status</c> (the newest row per server via a window
    /// function). The archive view, not the hot table: <c>ever_seen_running</c> asks about every stored row, and
    /// the hot table alone loses the rows that archival (after 7 days, or at the 512 MB reset) moved to Parquet,
    /// so a stopped Agent would read as one that never ran. With no <paramref name="serverId"/> it returns one
    /// row per server (the fleet header summary); with one it returns just that server's row (the
    /// Server-filtered header). The Job History tab shows this in its header; the "Agent Not Running" alert
    /// (Darling) reads the same data.
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
    FROM v_agent_status
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
    /// <summary>The run's time as <c>sysjobhistory</c> stored it: the SERVER's own wall clock, which is what the grid shows.</summary>
    public DateTime? RunDateTime { get; set; }

    /// <summary><see cref="RunDateTime"/> as naive UTC (#4966): the stored wall clock through its server's clock at the run's own date.
    /// What the Job History window and its newest-first order use, so runs of servers in different zones compare by when
    /// they happened. Not shown in the grid, which keeps the server's wall clock. Null when the run has no time.</summary>
    public DateTime? RunDateTimeUtc { get; set; }

    public long RunDurationSeconds { get; set; }
    public int RetriesAttempted { get; set; }
    public string? Message { get; set; }
    public DateTime? LastSuccessfulRun { get; set; }
    public bool IsLongRunning { get; set; }

    /// <summary>run_datetime is the server's local wall clock; shown as-is (the time SSMS shows).</summary>
    public string RunTimeLocal => RunDateTime?.ToString("yyyy-MM-dd HH:mm:ss") ?? "";

    /// <summary><see cref="LastSuccessfulRun"/> is <c>MAX(run_datetime)</c> over this job's successful
    /// step-0 rows, so it is the same server-local wall clock as <see cref="RunTimeLocal"/> and is shown
    /// the same way — as-is, not converted to the display mode.</summary>
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

    /// <summary><c>agent_status.next_scheduled_run</c> is the monitored server's own local wall clock, and
    /// this shows it as-is — not converted to the selected display mode — matching
    /// <see cref="JobHistoryRow.RunTimeLocal"/> in this file. Darling's read of the same column de-skews it
    /// to UTC in SQL and then converts, so the two SKUs present this grid differently on purpose in
    /// Darling's case and by inheritance here; the frame is stated at the property rather than only in the
    /// class summary because that is where a reader checks it.</summary>
    public string NextScheduledRunLocal => NextScheduledRun?.ToString("yyyy-MM-dd HH:mm:ss") ?? "None scheduled";
}
