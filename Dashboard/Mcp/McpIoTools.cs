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
public sealed class McpIoTools
{
    [McpServerTool(Name = "get_file_io_stats"), Description("Gets the latest file I/O latency statistics per database file. High read latency (>20ms) or write latency (>10ms for data, >2ms for log) indicates storage bottlenecks. Includes latency assessment and recommendations. Use get_file_io_trend to see if latency is degrading over time.")]
    public static async Task<string> GetFileIoStats(
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
            var rows = await resolved.Value.Service.GetFileIoLatencyAsync();
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No file I/O stats available.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                file_name = r.FileName,
                file_type = r.FileType,
                avg_read_latency_ms = Math.Round((double)r.AvgReadLatencyMs, 2),
                avg_write_latency_ms = Math.Round((double)r.AvgWriteLatencyMs, 2),
                reads_last_15min = r.ReadsLast15Min,
                writes_last_15min = r.WritesLast15Min,
                latency_issue = r.LatencyIssue,
                recommendation = r.Recommendation,
                last_seen = r.LastSeen.ToString("o")
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                files = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_file_io_stats", ex);
        }
    }

    [McpServerTool(Name = "get_file_io_trend"), Description("Gets I/O latency trend over time per database file. Useful for spotting degradation in storage performance or confirming whether high latency is new, worsening, or steady-state.")]
    public static async Task<string> GetFileIoTrend(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
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

            var points = await resolved.Value.Service.GetFileIoDataAsync(hours_back);
            if (points.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No I/O trend data available.");
            }

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                database_name = p.DatabaseName,
                file_name = p.FileName,
                file_type = p.FileType,
                avg_read_latency_ms = Math.Round((double)p.AvgReadLatencyMs, 2),
                avg_write_latency_ms = Math.Round((double)p.AvgWriteLatencyMs, 2)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_file_io_trend", ex);
        }
    }
}
