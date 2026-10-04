/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The SQL Agent running-jobs MCP tool — get_running_jobs — served over Darling's Postgres store. The tool
/// body mirrors LITE's <c>McpJobTools</c> field-for-field (Lite and the Dashboard share this shape; Lite adds
/// the snapshot's stamp to the envelope — <c>captured_at</c> since #3653, <c>collection_time</c> before it —
/// which the store-faithful Darling read carries). Reads flow through
/// <see cref="DarlingJobReader"/> — a STORED read of the latest snapshot, no live monitored-server hit.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpJobTools
{
    [McpServerTool(Name = "get_running_jobs"), Description("Gets currently running SQL Agent jobs with duration comparison. Shows each job's current duration vs its historical average and p95, flagging jobs that are running longer than usual. start_time is UTC, matching the captured_at on this payload (msdb records the Agent start in the monitored server's local clock; this read de-skews it), so start_time and current_duration_seconds agree. LATEST IS A TIME: this reads the newest running-jobs snapshot, not a window, and captured_at is the instant it was collected - a job listed here was running AT that stamp, and current_duration_seconds is how long it had been running AT that stamp, not now.")]
    public static async Task<string> GetRunningJobs(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var rows = await DarlingJobReader.GetRunningJobsAsync(postgres, resolved.ServerId, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "running_jobs", cancellationToken)
                    /* #2546: the msdb case. "No running SQL Agent jobs found" is an affirmative claim about
                       the server's Agent, and it is the wrong one when the monitoring login was refused the
                       job tables — the collector runs, is denied, and records that denial with the GRANT to
                       issue. Reporting it here is the difference between "nothing is running" and "we cannot
                       see what is running". */
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, "running_jobs", cancellationToken)
                    /* #2559: the case the line above cannot see. StatusAsync reports what the collector's last
                       run RECORDED, and a collector whose AppliesTo gate is off never runs — the runner returns
                       before writing any collection_log row, deliberately, because a per-cycle fake row was
                       thousands of rows a day of noise. So the gated-off server produced no evidence, this fell
                       through to the "empty" line below, and we went back to asserting the Agent is idle on a
                       server we were never permitted to look at. That is the exact claim #2546 set out to
                       remove, surviving in the one case that records nothing to read.

                       The gate is !IsAzureSqlDb && !IsAwsRds. The engine half is already answered above,
                       so AWS RDS is the only remaining candidate (#2559 removed HasMsdbAccess from this gate).
                       The message still names it as a possible cause: a run that is merely late looks the same
                       from here. The text is shared with Lite, so the two tools cannot drift. */
                    ?? await DarlingRuntimePrecondition.GatedOffStatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "running_jobs",
                        CollectorRuntimePrecondition.RunningJobsPossibleCauses,
                        cancellationToken)
                    ?? McpHelpers.Status("empty", "No running SQL Agent jobs found (or the running_jobs collector has not run yet).");

            var jobs = rows.Select(r => new
            {
                job_name = r.JobName,
                job_id = r.JobId,
                job_enabled = r.JobEnabled,
                start_time = r.StartTimeUtc.ToString("o"),
                current_duration_seconds = r.CurrentDurationSeconds,
                current_duration_formatted = DarlingJobReader.FormatDuration(r.CurrentDurationSeconds),
                avg_duration_seconds = r.AvgDurationSeconds,
                avg_duration_formatted = DarlingJobReader.FormatDuration(r.AvgDurationSeconds),
                p95_duration_seconds = r.P95DurationSeconds,
                p95_duration_formatted = DarlingJobReader.FormatDuration(r.P95DurationSeconds),
                successful_run_count = r.SuccessfulRunCount,
                is_running_long = r.IsRunningLong,
                percent_of_average = r.PercentOfAverage
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3653: captured_at, the #3637 census's one spelling for a latest read's stamp - see
                   DarlingMcpDataTools.GetServerProperties for why it is a cut-over and not an alias. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                running_job_count = rows.Count,
                long_running_count = rows.Count(r => r.IsRunningLong),
                jobs
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_running_jobs", ex);
        }
    }

    /// <summary>The run statuses <c>get_job_history</c> accepts, by their <c>sysjobhistory</c> codes 0 to 3.</summary>
    private static readonly string[] HistoryStatuses = ["Failed", "Succeeded", "Retry", "Canceled"];

    [McpServerTool(Name = "get_job_history"), Description("Gets retained SQL Agent job runs (steps and job outcomes) whose run time falls in a window ending at as_of, newest first, for one server or the whole fleet. Filters job_name, status and category apply before the limit. run_time and last_success are UTC. Empty: no run matched; not_collected means this engine has no Agent history. window_truncated marks a window floor, not a limit cut; effective_start gives the reach served; truncated marks a limit cut.")]
    public static async Task<string> GetJobHistory(
        NpgsqlDataSource postgres,
        [Description("Server name or display name. Omit for every server.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Exact job name, case-insensitive.")] string? job_name = null,
        [Description("Run status: Failed, Succeeded, Retry or Canceled.")] string? status = null,
        [Description("Exact job category name, case-insensitive.")] string? category = null,
        [Description("Maximum number of runs to return. Default 100.")] int limit = 100,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd)
            ?? McpHelpers.ValidateTop(limit)
            ?? McpHelpers.ValidateChoice(status, HistoryStatuses, "status");
        if (validation != null) return validation;

        try
        {
            /* Fleet-wide when no server is named: the resolver would auto-pick or refuse, and neither is the
               answer to "every server". A named server resolves through the registry as everywhere else. */
            (int ServerId, string ServerName)? scope = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (servers, fault) = await DarlingServerResolver.LoadEnabledOrFaultAsync(postgres, cancellationToken);
                if (fault != null) return fault;
                var (resolved, error) = DarlingServerResolver.ResolveOrError(servers, server_name);
                if (error != null) return error;
                scope = resolved;
            }

            var filter = new JobHistoryFilter(
                string.IsNullOrWhiteSpace(job_name) ? null : job_name.Trim(),
                string.IsNullOrWhiteSpace(status) ? null : Array.FindIndex(HistoryStatuses, s => string.Equals(s, status.Trim(), StringComparison.OrdinalIgnoreCase)),
                string.IsNullOrWhiteSpace(category) ? null : category.Trim(),
                windowEnd);

            var requestedStart = windowEnd.AddHours(-hours_back);
            var fetched = await DarlingJobHistoryReader.GetAsync(
                postgres, requestedStart, scope?.ServerId, limit + 1, McpCommandDeadlines.ReadSeconds, filter, cancellationToken);
            var truncated = fetched.Count > limit;
            var rows = fetched.Take(limit).ToList();

            var source = DataWindowFloor.Source.ForCollectorTable("job_history");
            var floor = scope is { } one
                ? await DataWindowFloor.GetForServerAsync(postgres, source, one.ServerId, requestedStart, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken)
                : await DataWindowFloor.GetAsync(postgres, [source], null, requestedStart, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
            /* Job history is an event surface: a run's own time can sit long before the collection that stored it,
               so coverage starts at the earlier of the probe's floor and the oldest run shown. */
            if (rows.Count > 0 && rows[^1].RunDateTimeUtc is { } oldest && (floor is null || oldest < floor))
                floor = oldest;
            var effectiveStart = RawWindowFloor.EffectiveStart(floor, requestedStart);
            var windowTruncated = RawWindowFloor.IsTruncated(floor, requestedStart);

            if (rows.Count == 0)
            {
                if (scope is { } s1)
                {
                    var notCollected = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, s1.ServerId, s1.ServerName, "job_history", cancellationToken);
                    if (notCollected != null) return notCollected;
                }

                return McpHelpers.Status("empty", "No job runs matched in the requested time range.", new
                {
                    effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                    window_truncated = windowTruncated
                });
            }

            var runs = rows.Select(r => new
            {
                run_time = McpHelpers.FormatEffectiveStart(r.RunDateTimeUtc),
                server = r.ServerName,
                job_name = r.JobName,
                category = r.CategoryName,
                step = r.StepId == 0 ? "(Job outcome)" : $"{r.StepId}: {r.StepName}",
                status = r.RunStatus >= 0 && r.RunStatus < HistoryStatuses.Length ? HistoryStatuses[r.RunStatus] : r.RunStatusDesc,
                duration_seconds = r.RunDurationSeconds,
                duration_formatted = DarlingJobReader.FormatDuration(r.RunDurationSeconds),
                retries = r.RetriesAttempted,
                last_success = McpHelpers.FormatEffectiveStart(r.LastSuccessfulRunUtc),
                is_long_running = r.IsLongRunning,
                message = McpHelpers.Truncate(r.Message, 500)
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = scope?.ServerName,
                hours_back,
                effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                window_truncated = windowTruncated,
                shown = runs.Count,
                truncated,
                runs
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_job_history", ex);
        }
    }
}
