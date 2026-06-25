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
public sealed class McpHealthTools
{
    [McpServerTool(Name = "get_collection_health"), Description("Shows the health status of all data collectors for a server — whether they're running successfully, failing, or stale. Check this before investigating data to ensure collectors are working properly.")]
    public static async Task<string> GetCollectionHealth(
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
            var rows = await resolved.Value.Service.GetCollectionHealthAsync();
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No collection health data available.");
            }

            var result = rows.Select(r => new
            {
                collector = r.CollectorName,
                status = r.HealthStatus,
                total_runs_7d = r.TotalRuns7d,
                failed_runs_7d = r.FailedRuns7d,
                failure_rate_pct = Math.Round((double)r.FailureRatePercent, 1),
                avg_duration_ms = r.AvgDurationMs,
                total_rows_collected_7d = r.TotalRowsCollected7d,
                hours_since_success = r.HoursSinceSuccess,
                last_success = r.LastSuccessTime?.ToString("o")
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

    [McpServerTool(Name = "get_daily_summary"), Description("Gets a daily health summary: overall health status, total wait time, top wait type, expensive query count, deadlocks, blocking events, memory pressure, and high CPU events. Use this for a quick overview to decide which areas need investigation.")]
    public static async Task<string> GetDailySummary(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Summary date (yyyy-MM-dd). Default is today.")] string? summary_date = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            DateTime? date = null;
            if (!string.IsNullOrEmpty(summary_date))
            {
                if (DateTime.TryParse(summary_date, out var parsed))
                {
                    date = parsed;
                }
                else
                {
                    return $"Invalid date format '{summary_date}'. Use yyyy-MM-dd format (e.g., 2026-02-06).";
                }
            }

            var rows = await resolved.Value.Service.GetDailySummaryAsync(date);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No daily summary data available.");
            }

            var result = rows.Select(r => new
            {
                summary_date = r.SummaryDate.ToString("yyyy-MM-dd"),
                overall_health = r.OverallHealth,
                total_wait_time_sec = r.TotalWaitTimeSec,
                top_wait_type = r.TopWaitType,
                expensive_queries_count = r.ExpensiveQueriesCount,
                deadlock_count = r.DeadlockCount,
                blocking_events_count = r.BlockingEventsCount,
                memory_pressure_events = r.MemoryPressureEvents,
                high_cpu_events = r.HighCpuEvents,
                collectors_failing = r.CollectorsFailing
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                summaries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_daily_summary", ex);
        }
    }
}
