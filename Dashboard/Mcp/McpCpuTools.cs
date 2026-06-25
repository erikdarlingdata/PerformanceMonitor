using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpCpuTools
{
    [McpServerTool(Name = "get_cpu_utilization"), Description("Gets CPU utilization over time showing SQL Server CPU %, other process CPU %, and total CPU %. Data is downsampled to 1-minute averages. Use this to identify CPU pressure periods, then use get_top_queries_by_cpu to find the culprit queries.")]
    public static async Task<string> GetCpuUtilization(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 4.")] int hours_back = 4)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await resolved.Value.Service.GetCpuUtilizationAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No CPU utilization data available.");
            }

            /* Downsample to 1-minute buckets to avoid overwhelming LLM context */
            var bucketed = rows
                .GroupBy(r => new DateTime(r.SampleTime.Year, r.SampleTime.Month, r.SampleTime.Day,
                    r.SampleTime.Hour, r.SampleTime.Minute, 0, r.SampleTime.Kind))
                .OrderBy(g => g.Key)
                .Select(g => new
                {
                    sample_time = g.Key.ToString("o"),
                    sql_server_cpu = (int)Math.Round(g.Average(r => r.SqlServerCpuUtilization)),
                    other_process_cpu = (int)Math.Round(g.Average(r => r.OtherProcessCpuUtilization)),
                    total_cpu = (int)Math.Round(g.Average(r => r.TotalCpuUtilization)),
                    samples_in_bucket = g.Count()
                });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                note = "Values are 1-minute averages of ring buffer samples.",
                samples = bucketed
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_cpu_utilization", ex);
        }
    }
}
