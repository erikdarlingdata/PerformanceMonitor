using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpDiagnosticTools
{
    [McpServerTool(Name = "get_plan_cache_bloat"), Description("Gets plan cache composition showing single-use vs multi-use plans. High single-use plan counts indicate ad-hoc query bloat consuming buffer pool memory. Consider enabling 'optimize for ad hoc workloads'.")]
    public static async Task<string> GetPlanCacheBloat(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetPlanCacheStatsAsync(hours_back);
            if (rows.Count == 0)
                return "No plan cache statistics available in the requested time range.";

            // Service returns all snapshots (for UI charting).
            // For MCP, return only the latest snapshot per cache/object type.
            var latestTime = rows.Max(r => r.CollectionTime);
            var latest = rows.Where(r => r.CollectionTime == latestTime).ToList();

            var totalPlans = latest.Sum(r => r.TotalPlans);
            var totalSingleUse = latest.Sum(r => r.SingleUsePlans);
            var totalSizeMb = latest.Sum(r => r.TotalSizeMb);
            var singleUseSizeMb = latest.Sum(r => r.SingleUseSizeMb);

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                collection_time = latestTime.ToString("o"),
                summary = new
                {
                    total_plans = totalPlans,
                    single_use_plans = totalSingleUse,
                    single_use_percent = totalPlans > 0 ? Math.Round(100.0 * totalSingleUse / totalPlans, 1) : 0,
                    total_size_mb = totalSizeMb,
                    single_use_size_mb = singleUseSizeMb,
                    wasted_percent = totalSizeMb > 0 ? Math.Round(100.0 * singleUseSizeMb / totalSizeMb, 1) : 0
                },
                cache_types = latest.Select(r => new
                {
                    cache_type = r.CacheObjType,
                    object_type = r.ObjType,
                    total_plans = r.TotalPlans,
                    total_size_mb = r.TotalSizeMb,
                    single_use_plans = r.SingleUsePlans,
                    single_use_size_mb = r.SingleUseSizeMb,
                    multi_use_plans = r.MultiUsePlans,
                    multi_use_size_mb = r.MultiUseSizeMb,
                    avg_use_count = r.AvgUseCount
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_cache_bloat", ex);
        }
    }

    [McpServerTool(Name = "get_critical_issues"), Description("Gets detected performance issues and configuration problems. Shows severity (CRITICAL/WARNING/INFO), affected area, source collector, and investigation queries.")]
    public static async Task<string> GetCriticalIssues(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetCriticalIssuesAsync(hours_back);
            if (rows.Count == 0)
                return "No critical issues found in the requested time range.";

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                issue_count = rows.Count,
                critical_count = rows.Count(r => r.Severity == "CRITICAL"),
                warning_count = rows.Count(r => r.Severity == "WARNING"),
                issues = rows.Select(r => new
                {
                    issue_id = r.IssueId,
                    log_date = r.LogDate.ToString("o"),
                    severity = r.Severity,
                    problem_area = r.ProblemArea,
                    source_collector = r.SourceCollector,
                    affected_database = string.IsNullOrEmpty(r.AffectedDatabase) ? null : r.AffectedDatabase,
                    message = r.Message,
                    investigate_query = string.IsNullOrEmpty(r.InvestigateQuery) ? null : McpHelpers.Truncate(r.InvestigateQuery, 2000),
                    threshold_value = r.ThresholdValue,
                    threshold_limit = r.ThresholdLimit
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_critical_issues", ex);
        }
    }

    [McpServerTool(Name = "get_session_stats"), Description("Gets session and connection statistics: total sessions, running/sleeping/dormant counts, idle sessions, memory waiters, and top application/host by connection count.")]
    public static async Task<string> GetSessionStats(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetSessionStatsAsync(hours_back);
            if (rows.Count == 0)
                return "No session statistics available in the requested time range.";

            var latest = rows[0];
            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                collection_time = latest.CollectionTime.ToString("o"),
                total_sessions = latest.TotalSessions,
                running = latest.RunningSessions,
                sleeping = latest.SleepingSessions,
                background = latest.BackgroundSessions,
                dormant = latest.DormantSessions,
                idle_over_30min = latest.IdleSessionsOver30Min,
                waiting_for_memory = latest.SessionsWaitingForMemory,
                databases_with_connections = latest.DatabasesWithConnections,
                top_application = latest.TopApplicationName,
                top_application_connections = latest.TopApplicationConnections,
                top_host = latest.TopHostName,
                top_host_connections = latest.TopHostConnections
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_session_stats", ex);
        }
    }
}
