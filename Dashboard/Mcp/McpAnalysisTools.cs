using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpAnalysisTools
{
    /// <summary>
    /// Creates an AnalysisService for the resolved server's connection.
    /// Dashboard creates per-request (each server has its own connection string).
    /// </summary>
    private static AnalysisService CreateAnalysisService(DatabaseService service)
    {
        var planFetcher = new SqlServerPlanFetcher(service.ConnectionString);
        return new AnalysisService(service.ConnectionString, planFetcher);
    }

    [McpServerTool(Name = "analyze_server"), Description("Runs the diagnostic inference engine against a server's collected data. Scores wait stats, blocking, memory, config, and other facts, then traverses a relationship graph to build evidence-backed stories about what's wrong and why. Returns structured findings with severity scores, evidence chains, drill-down data, and recommended next tools to call.")]
    public static async Task<string> AnalyzeServer(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 4.")] int hours_back = 4)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var analysisService = CreateAnalysisService(resolved.Value.Service);
            var serverId = resolved.Value.ServerName.GetHashCode();
            var findings = await analysisService.AnalyzeAsync(serverId, resolved.Value.ServerName, hours_back);

            if (analysisService.InsufficientDataMessage != null)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.Value.ServerName,
                    status = "insufficient_data",
                    message = analysisService.InsufficientDataMessage
                }, McpHelpers.JsonOptions);
            }

            if (findings.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.Value.ServerName,
                    status = "healthy",
                    message = "No significant findings. All metrics are within normal ranges.",
                    analysis_time = analysisService.LastAnalysisTime?.ToString("o")
                }, McpHelpers.JsonOptions);
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                status = "findings",
                finding_count = findings.Count,
                analysis_time = analysisService.LastAnalysisTime?.ToString("o"),
                time_range = new
                {
                    start = findings[0].TimeRangeStart?.ToString("o"),
                    end = findings[0].TimeRangeEnd?.ToString("o")
                },
                findings = findings.Select(f => new
                {
                    severity = Math.Round(f.Severity, 2),
                    confidence = Math.Round(f.Confidence, 2),
                    category = f.Category,
                    root_fact = new { key = f.RootFactKey, value = f.RootFactValue },
                    leaf_fact = f.LeafFactKey != null
                        ? new { key = f.LeafFactKey, value = f.LeafFactValue }
                        : null,
                    story_path = f.StoryPath,
                    story_path_hash = f.StoryPathHash,
                    fact_count = f.FactCount,
                    drill_down = f.DrillDown,
                    next_tools = ToolRecommendations.GetForStoryPath(f.StoryPath)
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("analyze_server", ex);
        }
    }

    [McpServerTool(Name = "get_analysis_facts"), Description("Exposes the raw scored facts from the inference engine's collect+score pipeline. Shows every observation the engine sees with base severity, final severity after amplifiers, and which amplifiers matched.")]
    public static async Task<string> GetAnalysisFacts(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 4.")] int hours_back = 4,
        [Description("Filter to a specific source category. Omit for all.")] string? source = null,
        [Description("Minimum severity to include. Default 0.")] double min_severity = 0)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var analysisService = CreateAnalysisService(resolved.Value.Service);
            var serverId = resolved.Value.ServerName.GetHashCode();
            var facts = await analysisService.CollectAndScoreFactsAsync(serverId, resolved.Value.ServerName, hours_back);

            if (facts.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.Value.ServerName,
                    fact_count = 0,
                    message = "No facts collected."
                }, McpHelpers.JsonOptions);
            }

            var filtered = facts.AsEnumerable();
            if (source != null)
                filtered = filtered.Where(f => f.Source.Equals(source, StringComparison.OrdinalIgnoreCase));
            if (min_severity > 0)
                filtered = filtered.Where(f => f.Severity >= min_severity);

            var result = filtered
                .OrderByDescending(f => f.Severity)
                .Select(f => new
                {
                    source = f.Source,
                    key = f.Key,
                    value = Math.Round(f.Value, 6),
                    base_severity = Math.Round(f.BaseSeverity, 4),
                    severity = Math.Round(f.Severity, 4),
                    metadata = f.Metadata.ToDictionary(
                        m => m.Key,
                        m => Math.Round(m.Value, 2)),
                    amplifiers = f.AmplifierResults.Count > 0
                        ? f.AmplifierResults.Select(a => new
                        {
                            description = a.Description,
                            matched = a.Matched,
                            boost = a.Boost
                        })
                        : null
                })
                .ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                total_facts = facts.Count,
                shown = result.Count,
                filters = new { source, min_severity },
                facts = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_analysis_facts", ex);
        }
    }

    [McpServerTool(Name = "compare_analysis"), Description("Compares two time periods by running fact collection and scoring on each, showing what changed.")]
    public static async Task<string> CompareAnalysis(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours back for the comparison period. Default 4.")] int hours_back = 4,
        [Description("Hours back for the baseline period start. Default 28.")] int baseline_hours_back = 28)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateHoursBack(baseline_hours_back);
        if (validation != null) return validation;

        if (baseline_hours_back <= hours_back)
            return "baseline_hours_back must be greater than hours_back.";

        try
        {
            var analysisService = CreateAnalysisService(resolved.Value.Service);
            var serverId = resolved.Value.ServerName.GetHashCode();

            var now = DateTime.UtcNow;
            var comparisonStart = now.AddHours(-hours_back);
            var baselineEnd = now.AddHours(-baseline_hours_back + hours_back);
            var baselineStart = now.AddHours(-baseline_hours_back);

            var (baselineFacts, comparisonFacts) = await analysisService.ComparePeriodsAsync(
                serverId, resolved.Value.ServerName, baselineStart, baselineEnd, comparisonStart, now);

            var baselineByKey = baselineFacts.ToDictionary(f => f.Key, f => f);
            var comparisonByKey = comparisonFacts.ToDictionary(f => f.Key, f => f);
            var allKeys = baselineByKey.Keys.Union(comparisonByKey.Keys).ToHashSet();

            var comparisons = allKeys
                .Select(key =>
                {
                    baselineByKey.TryGetValue(key, out var baseline);
                    comparisonByKey.TryGetValue(key, out var comparison);
                    var severityDelta = (comparison?.Severity ?? 0) - (baseline?.Severity ?? 0);

                    return new
                    {
                        key,
                        source = baseline?.Source ?? comparison?.Source ?? "unknown",
                        baseline_severity = baseline != null ? Math.Round(baseline.Severity, 4) : (double?)null,
                        comparison_severity = comparison != null ? Math.Round(comparison.Severity, 4) : (double?)null,
                        severity_delta = Math.Round(severityDelta, 4),
                        status = severityDelta > 0.1 ? "worse" : severityDelta < -0.1 ? "better" : "stable"
                    };
                })
                .OrderByDescending(c => Math.Abs(c.severity_delta))
                .ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                summary = new
                {
                    worse = comparisons.Count(c => c.status == "worse"),
                    better = comparisons.Count(c => c.status == "better"),
                    stable = comparisons.Count(c => c.status == "stable")
                },
                facts = comparisons
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("compare_analysis", ex);
        }
    }

    [McpServerTool(Name = "audit_config"), Description("Evaluates SQL Server configuration settings against best practices, accounting for edition and server resources.")]
    public static async Task<string> AuditConfig(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var analysisService = CreateAnalysisService(resolved.Value.Service);
            var serverId = resolved.Value.ServerName.GetHashCode();
            var facts = await analysisService.CollectAndScoreFactsAsync(serverId, resolved.Value.ServerName, 1);

            var factsByKey = facts.ToDictionary(f => f.Key, f => f);

            var edition = factsByKey.TryGetValue("SERVER_EDITION", out var edFact) ? (int)edFact.Value : 0;
            var totalMemoryMb = factsByKey.TryGetValue("MEMORY_TOTAL_PHYSICAL_MB", out var memFact) ? memFact.Value : 0;

            var editionName = edition switch
            {
                2 => "Standard",
                3 => "Enterprise",
                4 => "Express",
                _ => "Unknown"
            };
            var isEnterprise = edition == 3;
            var isExpress = edition == 4;

            var recommendations = new System.Collections.Generic.List<object>();

            if (factsByKey.TryGetValue("CONFIG_CTFP", out var ctfpFact))
            {
                var ctfp = (int)ctfpFact.Value;
                var status = ctfp <= 5 ? "warning" : ctfp < 25 ? "review" : ctfp > 100 ? "review" : "ok";
                var suggested = ctfp <= 5 || ctfp < 25 ? 50 : ctfp > 100 ? 50 : ctfp;
                recommendations.Add(new { setting = "cost threshold for parallelism", current_value = ctfp, suggested_value = suggested, status });
            }

            if (factsByKey.TryGetValue("CONFIG_MAXDOP", out var maxdopFact))
            {
                var maxdop = (int)maxdopFact.Value;
                var suggested = maxdop == 0 ? (isExpress ? 1 : isEnterprise ? 8 : 4) : maxdop;
                var status = maxdop == 0 ? "warning" : maxdop == 1 && !isExpress ? "review" : "ok";
                recommendations.Add(new { setting = "max degree of parallelism", current_value = maxdop, suggested_value = suggested, status });
            }

            if (factsByKey.TryGetValue("CONFIG_MAX_MEMORY_MB", out var maxMemFact))
            {
                var maxMem = (int)maxMemFact.Value;
                var status = maxMem == 2147483647 ? "warning" : "ok";
                recommendations.Add(new { setting = "max server memory (MB)", current_value = maxMem, suggested_value = maxMem, status });
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                edition = editionName,
                recommendations
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("audit_config", ex);
        }
    }

    [McpServerTool(Name = "get_analysis_findings"), Description("Gets persisted findings from previous analysis runs.")]
    public static async Task<string> GetAnalysisFindings(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of finding history. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var analysisService = CreateAnalysisService(resolved.Value.Service);
            var serverId = resolved.Value.ServerName.GetHashCode();
            var findings = await analysisService.GetRecentFindingsAsync(serverId, hours_back);

            if (findings.Count == 0)
                return JsonSerializer.Serialize(new { server = resolved.Value.ServerName, finding_count = 0, message = "No findings. Run analyze_server to generate new findings." }, McpHelpers.JsonOptions);

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                finding_count = findings.Count,
                findings = findings.Select(f => new
                {
                    severity = Math.Round(f.Severity, 2),
                    category = f.Category,
                    story_path = f.StoryPath,
                    story_path_hash = f.StoryPathHash,
                    analysis_time = f.AnalysisTime.ToString("o")
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_analysis_findings", ex);
        }
    }

    [McpServerTool(Name = "mute_analysis_finding"), Description("Mutes a finding pattern so it won't appear in future analysis runs.")]
    public static async Task<string> MuteAnalysisFinding(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("The story_path_hash from the finding to mute.")] string story_path_hash,
        [Description("Server name.")] string? server_name = null,
        [Description("Optional reason for muting.")] string? reason = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var analysisService = CreateAnalysisService(resolved.Value.Service);
            var serverId = resolved.Value.ServerName.GetHashCode();
            var finding = new AnalysisFinding { ServerId = serverId, StoryPathHash = story_path_hash, StoryPath = story_path_hash };
            await analysisService.MuteFindingAsync(finding, reason);

            return JsonSerializer.Serialize(new { status = "muted", story_path_hash, reason }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("mute_analysis_finding", ex);
        }
    }
}

/// <summary>
/// Maps fact keys to recommended MCP tools for further investigation.
/// Shared between Lite and Dashboard — same recommendations.
/// </summary>
internal static class ToolRecommendations
{
    private static readonly System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<ToolRecommendation>> ByFactKey = new()
    {
        ["SOS_SCHEDULER_YIELD"] = [new("get_cpu_utilization", "Check CPU usage over time"), new("get_top_queries_by_cpu", "Find CPU-expensive queries")],
        ["CXPACKET"] = [new("get_top_queries_by_cpu", "Find parallel queries", new() { ["parallel_only"] = "true" }), new("audit_config", "Check CTFP and MAXDOP")],
        ["THREADPOOL"] = [new("get_top_queries_by_cpu", "Find resource-consuming queries"), new("get_blocking", "Check if blocking is holding threads")],
        ["PAGEIOLATCH_SH"] = [new("get_file_io_stats", "Check I/O latency"), new("get_memory_stats", "Check buffer pool")],
        ["PAGEIOLATCH_EX"] = [new("get_file_io_stats", "Check I/O latency"), new("get_memory_stats", "Check buffer pool")],
        ["RESOURCE_SEMAPHORE"] = [new("get_resource_semaphore", "Check memory grants")],
        ["WRITELOG"] = [new("get_file_io_stats", "Check log file latency")],
        ["LCK"] = [new("get_blocking", "Get blocking details"), new("get_deadlocks", "Check for deadlocks")],
        ["LCK_M_S"] = [new("get_blocking", "Get reader/writer blocking details")],
        ["BLOCKING_EVENTS"] = [new("get_blocking", "Get detailed blocking reports"), new("get_deadlocks", "Check for deadlocks")],
        ["DEADLOCKS"] = [new("get_deadlocks", "Get deadlock events"), new("get_deadlock_detail", "Get full deadlock XML")],
        ["CPU_SQL_PERCENT"] = [new("get_cpu_utilization", "See CPU trend"), new("get_top_queries_by_cpu", "Find CPU queries")],
        ["CPU_SPIKE"] = [new("get_cpu_utilization", "See when spike occurred"), new("get_top_queries_by_cpu", "Find queries that drove the spike")],
        ["IO_READ_LATENCY_MS"] = [new("get_file_io_stats", "Check per-file latency"), new("get_memory_stats", "Check buffer pool")],
        ["IO_WRITE_LATENCY_MS"] = [new("get_file_io_stats", "Check per-file latency")],
        ["TEMPDB_USAGE"] = [new("get_tempdb_trend", "Track TempDB usage")],
        ["MEMORY_GRANT_PENDING"] = [new("get_resource_semaphore", "Check memory grants")],
        ["QUERY_SPILLS"] = [new("get_top_queries_by_cpu", "Find queries with spills")],
        ["QUERY_HIGH_DOP"] = [new("get_top_queries_by_cpu", "Find high-DOP queries", new() { ["parallel_only"] = "true" })],
        ["PERFMON_PLE"] = [new("get_memory_stats", "Check buffer pool"), new("get_memory_clerks", "See memory allocation")],
        ["DB_CONFIG"] = [new("audit_config", "Check configuration")],
        ["DISK_SPACE"] = [new("get_file_io_stats", "Check per-file sizes")],
        ["LATCH_EX"] = [new("get_latch_stats", "Check latch contention"), new("get_tempdb_trend", "Check TempDB")],
        ["BAD_ACTOR"] = [new("get_top_queries_by_cpu", "See full query stats"), new("analyze_query_plan", "Analyze the execution plan")],
        ["ANOMALY_CPU"] = [new("get_cpu_utilization", "See CPU trend"), new("get_active_queries", "Find what ran during spike")],
        ["ANOMALY_WAIT"] = [new("get_wait_stats", "See wait breakdown"), new("compare_analysis", "Compare current vs baseline")],
        ["ANOMALY_BLOCKING"] = [new("get_blocking", "Get blocking details"), new("get_deadlocks", "Get deadlock events")],
        ["ANOMALY_IO"] = [new("get_file_io_stats", "Check I/O latency"), new("get_memory_stats", "Check buffer pool")]
    };

    public static System.Collections.Generic.List<object> GetForStoryPath(string storyPath)
    {
        var factKeys = storyPath.Split(" → ", StringSplitOptions.RemoveEmptyEntries);
        var seen = new System.Collections.Generic.HashSet<string>();
        var result = new System.Collections.Generic.List<object>();

        foreach (var key in factKeys)
        {
            if (!ByFactKey.TryGetValue(key, out var recommendations))
            {
                if (key.StartsWith("BAD_ACTOR_"))
                    ByFactKey.TryGetValue("BAD_ACTOR", out recommendations);
                else if (key.StartsWith("ANOMALY_CPU"))
                    ByFactKey.TryGetValue("ANOMALY_CPU", out recommendations);
                else if (key.StartsWith("ANOMALY_WAIT_"))
                    ByFactKey.TryGetValue("ANOMALY_WAIT", out recommendations);
                else if (key.StartsWith("ANOMALY_BLOCKING") || key.StartsWith("ANOMALY_DEADLOCK"))
                    ByFactKey.TryGetValue("ANOMALY_BLOCKING", out recommendations);
                else if (key.StartsWith("ANOMALY_READ") || key.StartsWith("ANOMALY_WRITE"))
                    ByFactKey.TryGetValue("ANOMALY_IO", out recommendations);
                if (recommendations == null) continue;
            }

            foreach (var rec in recommendations)
            {
                if (!seen.Add(rec.Tool)) continue;
                result.Add(rec.SuggestedParams != null && rec.SuggestedParams.Count > 0
                    ? new { tool = rec.Tool, reason = rec.Reason, suggested_params = rec.SuggestedParams }
                    : (object)new { tool = rec.Tool, reason = rec.Reason });
            }
        }

        return result;
    }
}

internal record ToolRecommendation(
    string Tool,
    string Reason,
    System.Collections.Generic.Dictionary<string, string>? SuggestedParams = null);
