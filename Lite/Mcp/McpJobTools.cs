using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpJobTools
{
    [McpServerTool(Name = "get_running_jobs"), Description("Gets currently running SQL Agent jobs with duration comparison. Shows each job's current duration vs its historical average and p95, flagging jobs that are running longer than usual.")]
    public static async Task<string> GetRunningJobs(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetRunningJobsAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "running_jobs")
                    /* #2546: the msdb case. "No running SQL Agent jobs found" is an affirmative claim about
                       the server's Agent, and it is the wrong one when the monitoring login was refused the
                       job tables — the collector runs, is denied, and records that denial with the GRANT to
                       issue. Reporting it here is the difference between "nothing is running" and "we cannot
                       see what is running". */
                    ?? await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.ServerName, "running_jobs")
                    /* #2559, and it must land on BOTH SKUs: the gate this reports on
                       (!IsAzureSqlDb && !IsAwsRds && HasMsdbAccess) lives in the shared collector definition,
                       so a Lite login with HAS_DBACCESS('msdb') = 0 reproduces the identical defect — a
                       collector that never runs, records no collection_log row, and leaves this read asserting
                       the Agent is idle on a server it was never permitted to query. */
                    ?? await McpRuntimePrecondition.GatedOffStatusAsync(
                        dataService, resolved.ServerId, resolved.ServerName, "running_jobs",
                        "For this collector the gate is: the monitoring login has no access to msdb "
                        + "(HAS_DBACCESS('msdb') = 0 — the grant to fix that is in the README's monitoring-login "
                        + "section), or this is an AWS RDS instance, where the Agent job tables are not reachable "
                        + "to a monitoring login at all and no grant changes that.")
                    ?? McpHelpers.Status("empty", "No running SQL Agent jobs found (or collector has not run yet).");
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
                server = resolved.ServerName,
                collection_time = rows[0].CollectionTime.ToString("o"),
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
