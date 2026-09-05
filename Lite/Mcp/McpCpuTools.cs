using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpCpuTools
{
    [McpServerTool(Name = "get_cpu_utilization"), Description("Gets CPU utilization over time showing SQL Server CPU %, other process CPU %, total CPU %, and idle %. Data is downsampled to 1-minute averages. Use this to identify CPU pressure periods, then use get_top_queries_by_cpu to find the culprit queries.")]
    public static async Task<string> GetCpuUtilization(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            /* sample_time is THIS server's local wall clock (#1262), so the window needs THIS server's
               offset — not the desktop tab's. See McpServerLocalWindow. */
            var (utcOffsetMinutes, offsetError) =
                await McpServerLocalWindow.OffsetOrErrorAsync(dataService, resolved.ServerId, resolved.ServerName);
            if (offsetError != null) return offsetError;

            var rows = await dataService.GetCpuUtilizationAsync(
                resolved.ServerId, hours_back, asOfUtc: windowEnd, utcOffsetMinutes: utcOffsetMinutes);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "cpu_utilization")
                    ?? McpHelpers.Status("unavailable", "No CPU utilization data available.");
            }

            /* Downsample to 1-minute buckets to avoid overwhelming LLM context */
            var bucketed = rows
                .GroupBy(r => new DateTime(r.SampleTime.Year, r.SampleTime.Month, r.SampleTime.Day,
                    r.SampleTime.Hour, r.SampleTime.Minute, 0, r.SampleTime.Kind))
                .OrderBy(g => g.Key)
                .Select(g => new
                {
                    sample_time = g.Key.ToString("o"),
                    sql_server_cpu = (int)Math.Round(g.Average(r => r.SqlServerCpu)),
                    other_process_cpu = (int)Math.Round(g.Average(r => r.OtherProcessCpu)),
                    total_cpu = (int)Math.Round(g.Average(r => r.TotalCpu)),
                    idle_cpu = (int)Math.Round(g.Average(r => r.IdleCpu)),
                    samples_in_bucket = g.Count()
                });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                note = "Values are 1-minute averages of 15-second ring buffer samples.",
                samples = bucketed
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_cpu_utilization", ex);
        }
    }
}
