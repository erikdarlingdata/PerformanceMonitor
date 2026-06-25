using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpHealthTools
{
    [McpServerTool(Name = "get_server_summary"), Description("Gets a quick health overview for a SQL Server instance: current CPU %, memory usage, recent blocking count, and deadlock count. Use this for a fast health check before drilling into specific areas.")]
    public static async Task<string> GetServerSummary(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name. Optional if only one server is configured.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var summary = await dataService.GetServerSummaryAsync(resolved.Value.ServerId, resolved.Value.ServerName);
            if (summary == null)
            {
                return McpHelpers.Status(
                    "unavailable",
                    $"No data available for {resolved.Value.ServerName}. The collector may not have run yet.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                cpu_percent = summary.CpuPercent,
                memory_mb = summary.MemoryMb,
                blocking_count = summary.BlockingCount,
                deadlock_count = summary.DeadlockCount,
                last_collection = summary.LastCollectionTime?.ToString("o")
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_summary", ex);
        }
    }

    [McpServerTool(Name = "get_collection_health"), Description("Shows the health status of all data collectors for a server — whether they're running successfully, failing, or stale. Check this before investigating data to ensure collectors are working properly.")]
    public static async Task<string> GetCollectionHealth(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var rows = await dataService.GetCollectionHealthAsync(resolved.Value.ServerId);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No collection health data available.");
            }

            var result = rows.Select(r => new
            {
                collector = r.CollectorName,
                status = r.HealthStatus,
                total_runs = r.TotalRuns,
                errors = r.ErrorCount,
                failure_rate_pct = Math.Round(r.FailureRatePercent, 1),
                avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                last_success = r.LastSuccessTime?.ToString("o"),
                last_error = r.LastError
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                collectors = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_collection_health", ex);
        }
    }
}
