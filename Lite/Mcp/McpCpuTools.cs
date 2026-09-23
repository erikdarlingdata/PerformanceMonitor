using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpCpuTools
{
    [McpServerTool(Name = "get_cpu_utilization"), Description("Gets CPU utilization over time in time buckets: SQL Server CPU %, other process CPU %, total CPU % and idle %, with each bucket's busiest sample. Use this to identify CPU pressure periods, then use get_top_queries_by_cpu to find the culprit queries.")]
    public static async Task<string> GetCpuUtilization(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var budget = TrendBudget.Mcp(TrendBuckets.CpuMaxPoints);
            var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
            if (bucketError != null) return bucketError;

            /* sample_time is THIS server's local wall clock (#1262), so the window needs THIS server's
               offset — not the desktop tab's. See McpServerLocalWindow. Since v63 (#3653 item 13) the offset
               drives only the pre-rung fallback arm of the window: a row carrying sample_time_utc is selected
               by that stored UTC instant, offset-free. The emitted sample_time stays the server-local stamp
               (bucketed in SQL since #3960 — the tool used to average every sample to the minute here), the
               frame this tool has always published. */
            var utcOffsetMinutes = await McpServerLocalWindow.OffsetForAsync(dataService, resolved.ServerId);

            var points = await dataService.GetCpuBucketsAsync(resolved.ServerId, hours_back, windowEnd, utcOffsetMinutes, bucketMinutes);
            if (points.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "cpu_utilization")
                    ?? McpHelpers.Status("unavailable", "No CPU utilization data available.");
            }

            /* #3653 A15/A16: the source-cadence sentence is TrendPayloads.CpuCadenceNote, the one place both SKUs
               spell it — the ring buffer (RING_BUFFER_SCHEDULER_MONITOR, on-prem / Managed Instance / RDS) writes
               ONE record per minute, the 15-second cadence belongs to Azure SQL DB's sys.dm_db_resource_stats, and
               samples_in_bucket on every bucket is the measured count. */
            return TrendPayloads.CpuUtilization(resolved.ServerName, hours_back, points, bucketMinutes, bucket_minutes is not null, budget.AutoPoints);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_cpu_utilization", ex);
        }
    }
}
