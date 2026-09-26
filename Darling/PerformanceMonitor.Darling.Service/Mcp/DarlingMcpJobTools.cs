/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

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
                       so AWS RDS is the only remaining candidate and the message can name it outright rather
                       than hedging — #2559 removed HasMsdbAccess from this gate, which is what turned two
                       unpersisted candidates into one. */
                    ?? await DarlingRuntimePrecondition.GatedOffStatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "running_jobs",
                        "For this collector the gate is: this is an AWS RDS instance, where the Agent job "
                        + "tables are not reachable to a monitoring login at all and no grant changes that. "
                        + "Since #2559 msdb access is NOT a gate — a login without it now attempts and is "
                        + "reported as a permission denial, so the grant takes effect on the next cycle "
                        + "rather than the next reconnect.",
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
}
