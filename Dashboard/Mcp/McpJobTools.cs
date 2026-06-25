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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpJobTools
{
    [McpServerTool(Name = "get_running_jobs"), Description("Gets currently running SQL Agent jobs with duration comparison. Shows each job's current duration vs its historical average and p95, flagging jobs that are running longer than usual.")]
    public static async Task<string> GetRunningJobs(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var rows = await resolved.Value.Service.GetRunningJobsAsync();
            if (rows.Count == 0)
            {
                return McpHelpers.Status("empty", "No running SQL Agent jobs found (or no data in report.running_jobs).");
            }

            var result = rows.Select(r => new
            {
                job_name = r.JobName,
                job_id = r.JobId,
                job_enabled = r.JobEnabled,
                start_time = r.StartTime.ToString("o"),
                current_duration_seconds = r.CurrentDurationSeconds,
                current_duration_formatted = r.CurrentDurationFormatted,
                avg_duration_seconds = r.AvgDurationSeconds,
                avg_duration_formatted = r.AvgDurationFormatted,
                p95_duration_seconds = r.P95DurationSeconds,
                p95_duration_formatted = r.P95DurationFormatted,
                successful_run_count = r.SuccessfulRunCount,
                is_running_long = r.IsRunningLong,
                percent_of_average = r.PercentOfAverage
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                running_job_count = rows.Count,
                long_running_count = rows.Count(r => r.IsRunningLong),
                jobs = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_running_jobs", ex);
        }
    }
}
