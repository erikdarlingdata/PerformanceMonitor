using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var analysisService = CreateAnalysisService(resolved.Service);
            var serverId = ServerIdHelper.GetDeterministicHashCode(resolved.ServerName);
            var findings = await analysisService.AnalyzeAsync(serverId, resolved.ServerName, hours_back);

            if (analysisService.InsufficientDataMessage != null)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    status = "insufficient_data",
                    message = analysisService.InsufficientDataMessage
                }, McpHelpers.JsonOptions);
            }

            if (findings.Count == 0)
            {
                /* A successful analysis that found nothing wrong: a true negative ("all clear"),
                   surfaced with the shared miss vocabulary so callers branch on it uniformly. */
                return McpHelpers.Status(
                    "empty",
                    "No significant findings. All metrics are within normal ranges.",
                    new { analysis_time = analysisService.LastAnalysisTime?.ToString("o") });
            }

            // Correlate-and-focus slice 1 (review §1d): each finding's "what else fired this window".
            var coFiredTitles = new List<(string, double)>(findings.Count);
            foreach (var wf in findings)
                coFiredTitles.Add((FactAdvice.GetForFinding(wf)?.Headline ?? wf.RootFactKey, wf.Severity));

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                status = "findings",
                finding_count = findings.Count,
                analysis_time = analysisService.LastAnalysisTime?.ToString("o"),
                time_range = new
                {
                    start = findings[0].TimeRangeStart?.ToString("o"),
                    end = findings[0].TimeRangeEnd?.ToString("o")
                },
                findings = findings.Select(f =>
                {
                    var advice = FactAdvice.GetForFinding(f);
                    return new
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
                        next_tools = ToolRecommendations.GetForStoryPath(f.StoryPath),
                        incident_id = f.IncidentId,
                        co_fired = CoFiredSummary.OtherTitles(advice?.Headline ?? f.RootFactKey, coFiredTitles),
                        advice = advice is null ? null : new
                        {
                            headline = advice.Headline,
                            investigation = advice.Investigation,
                            remediation = advice.Remediation
                        },
                        suggested_remediation_sql = advice?.RemediationTsql,
                        // B3 Phase 3 (§6): two-sided risk DISCLOSURE for a destructive
                        // remediation — read-only here (no consent gate off-app; consent
                        // is enforced only by the in-app acknowledge-each-risk dialog).
                        destructive_risk_disclosure = advice?.Risks is null ? null : new
                        {
                            risks_of_changing = advice.Risks.RisksOfChanging.Select(r => r.Text).ToArray(),
                            risks_of_not_changing = advice.Risks.RisksOfNotChanging.Select(r => r.Text).ToArray()
                        }
                    };
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var analysisService = CreateAnalysisService(resolved.Service);
            var serverId = ServerIdHelper.GetDeterministicHashCode(resolved.ServerName);
            var facts = await analysisService.CollectAndScoreFactsAsync(serverId, resolved.ServerName, hours_back);

            if (facts.Count == 0)
            {
                /* No scored facts means the underlying collectors produced nothing for the window —
                   not retrievable now rather than an all-clear (mirrors get_perfmon_trend's empty case). */
                return McpHelpers.Status("unavailable", "No facts collected.");
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
                server = resolved.ServerName,
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateHoursBack(baseline_hours_back);
        if (validation != null) return validation;

        if (baseline_hours_back <= hours_back)
            return "baseline_hours_back must be greater than hours_back.";

        try
        {
            var analysisService = CreateAnalysisService(resolved.Service);
            var serverId = ServerIdHelper.GetDeterministicHashCode(resolved.ServerName);

            // Server-local clock so the comparison windows match the collectors' SYSDATETIME rows
            // (compare returns facts only — it does not persist, so no UTC conversion is needed).
            var now = await analysisService.GetServerLocalNowAsync();
            var comparisonStart = now.AddHours(-hours_back);
            var baselineEnd = now.AddHours(-baseline_hours_back + hours_back);
            var baselineStart = now.AddHours(-baseline_hours_back);

            var (baselineFacts, comparisonFacts) = await analysisService.ComparePeriodsAsync(
                serverId, resolved.ServerName, baselineStart, baselineEnd, comparisonStart, now);

            var baselineByKey = baselineFacts.ToFactLookup();
            var comparisonByKey = comparisonFacts.ToFactLookup();
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
                server = resolved.ServerName,
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        try
        {
            var analysisService = CreateAnalysisService(resolved.Service);
            var serverId = ServerIdHelper.GetDeterministicHashCode(resolved.ServerName);
            var facts = await analysisService.CollectAndScoreFactsAsync(serverId, resolved.ServerName, 1);

            var factsByKey = facts.ToFactLookup();

            var edition = factsByKey.TryGetValue("SERVER_EDITION", out var edFact) ? (int)edFact.Value : 0;
            var totalMemoryMb = factsByKey.TryGetValue("MEMORY_TOTAL_PHYSICAL_MB", out var memFact) ? memFact.Value : 0;
            var coresPerSocket = factsByKey.TryGetValue("SERVER_HARDWARE", out var hwFact)
                && hwFact.Metadata.TryGetValue("cores_per_socket", out var cps) ? (int)cps : 0;

            var editionName = edition switch
            {
                2 => "Standard",
                3 => "Enterprise",
                4 => "Express",
                _ => "Unknown"
            };

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
                // Topology-based (min(cores-per-socket, 8)), NOT edition-based — see FactRemediation.RecommendedMaxdop.
                var suggested = maxdop == 0 ? (int)FactRemediation.RecommendedMaxdop(coresPerSocket) : maxdop;
                var status = maxdop == 0 ? "warning" : maxdop == 1 ? "review" : "ok";
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
                server = resolved.ServerName,
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
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        try
        {
            var analysisService = CreateAnalysisService(resolved.Service);
            var serverId = ServerIdHelper.GetDeterministicHashCode(resolved.ServerName);
            var findings = await analysisService.GetRecentFindingsAsync(serverId, hours_back);

            if (findings.Count == 0)
                return McpHelpers.Status("empty", "No findings. Run analyze_server to generate new findings.");

            // Correlate-and-focus slice 1 (review §1d): "what else fired", scoped per analysis run
            // (this read can span multiple runs, unlike analyze_server's single run).
            var coFiredByRun = new Dictionary<DateTime, List<(string, double)>>();
            foreach (var wf in findings)
            {
                if (!coFiredByRun.TryGetValue(wf.AnalysisTime, out var list))
                    coFiredByRun[wf.AnalysisTime] = list = new List<(string, double)>();
                list.Add((FactAdvice.GetComposedForFinding(wf)?.Headline ?? wf.RootFactKey, wf.Severity));
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                finding_count = findings.Count,
                findings = findings.Select(f =>
                {
                    // Persisted findings carry no drill-down (it is ephemeral —
                    // see AnalysisModels.cs), so generate advice prose only.
                    // The prose IS value-stated: GetComposedForFinding reads the
                    // value-bearing advice (current MAXDOP/CTFP/etc.) frozen into
                    // StoryText at analysis time, falling back to the static block.
                    // suggested_remediation_sql is intentionally omitted: it
                    // would always be null here. The operator re-runs
                    // analyze_server when they need the copy-paste T-SQL.
                    var advice = FactAdvice.GetComposedForFinding(f);
                    return new
                    {
                        severity = Math.Round(f.Severity, 2),
                        category = f.Category,
                        story_path = f.StoryPath,
                        story_path_hash = f.StoryPathHash,
                        analysis_time = f.AnalysisTime.ToString("o"),
                        incident_id = f.IncidentId,
                        co_fired = CoFiredSummary.OtherTitles(advice?.Headline ?? f.RootFactKey, coFiredByRun[f.AnalysisTime]),
                        advice = advice is null ? null : new
                        {
                            headline = advice.Headline,
                            investigation = advice.Investigation,
                            remediation = advice.Remediation
                        }
                    };
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_analysis_findings", ex);
        }
    }

    [McpServerTool(Name = "mute_analysis_finding"), Description("Mutes a finding pattern so it won't appear in future analysis runs. Use the story_path_hash from analyze_server or get_analysis_findings output. Muting is per-pattern, not per-occurrence. The response reports what the write DID: registered says whether the mute row was stored, and matched_now is how many stored findings for this server carry that hash at this moment. status is \"muted\" when the mute is registered AND matched_now is at least 1; \"muted_unmatched\" when it is registered but matched_now is 0 — the pattern is not in the retained findings, which is what a mistyped hash looks like (the mute is kept, because the registry is by pattern and the pattern may return after retention purged its history, but check the hash against analyze_server output before relying on it); \"error\" when the row could not be written (nothing is muted).")]
    public static async Task<string> MuteAnalysisFinding(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("The story_path_hash from the finding to mute.")] string story_path_hash,
        [Description("Server name.")] string? server_name = null,
        [Description("Optional reason for muting.")] string? reason = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, registry, server_name);
        if (error != null) return error;

        try
        {
            if (string.IsNullOrWhiteSpace(story_path_hash))
            {
                return JsonSerializer.Serialize(new { status = "invalid", message = "story_path_hash is required." }, McpHelpers.JsonOptions);
            }

            var analysisService = CreateAnalysisService(resolved.Service);
            var serverId = ServerIdHelper.GetDeterministicHashCode(resolved.ServerName);
            var finding = new AnalysisFinding { ServerId = serverId, StoryPathHash = story_path_hash, StoryPath = story_path_hash };

            /* #3653 (#3615's class): report what happened, not what was asked. Before this the verb returned
               "muted" for any hash — a mistyped one, one copied from another store, one whose INSERT the store
               swallowed and logged — and the agent walked away believing a pattern was silenced. The store now
               says whether the row landed, and matched_now is counted AFTER the write so the two are read against
               the same moment. The registry is a pattern registry (no row references a finding), so an unmatched
               hash is still stored — legitimately, when a pattern's history has been purged — and the status
               names that case instead of folding it into success. See SqlServerFindingStore.CountStoredFindingsAsync. */
            var registered = await analysisService.MuteFindingAsync(finding, reason);
            if (!registered)
            {
                return JsonSerializer.Serialize(new
                {
                    status = "error",
                    story_path_hash,
                    server = resolved.ServerName,
                    registered = false,
                    message = "The mute row could not be written; nothing is muted. See the Dashboard log for the store's error."
                }, McpHelpers.JsonOptions);
            }

            var matchedNow = await analysisService.CountStoredFindingsAsync(serverId, story_path_hash);

            return JsonSerializer.Serialize(new
            {
                status = matchedNow > 0 ? "muted" : "muted_unmatched",
                story_path_hash,
                server = resolved.ServerName,
                reason,
                registered = true,
                matched_now = matchedNow,
                note = matchedNow > 0
                    ? $"The mute is registered; {matchedNow} stored finding(s) for this server carry the hash and the pattern will be dropped from future analysis runs."
                    : "The mute is registered, but no stored finding for this server carries this story_path_hash. If you copied it from analyze_server or get_analysis_findings it is still valid (the pattern will be dropped if it recurs); a hash from anywhere else may be mistyped and would mute nothing."
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("mute_analysis_finding", ex);
        }
    }
}

/// <summary>
/// Maps fact keys to recommended MCP tools for further investigation.
/// Used by analyze_server to tell the AI client what to call next. Lite keeps its own
/// copy (PerformanceMonitorLite.Mcp) because the two apps expose different MCP tool
/// sets, so the recommendations are app-specific -- NOT a shared/duplicated list.
/// </summary>
internal static class ToolRecommendations
{
    private static readonly System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<ToolRecommendation>> ByFactKey = new()
    {
        ["SOS_SCHEDULER_YIELD"] =
        [
            new("get_cpu_utilization", "Check SQL Server vs other-process CPU usage over time"),
            new("get_top_queries_by_cpu", "Find the most CPU-expensive queries"),
            new("get_perfmon_trend", "Check batch requests/sec for throughput context", new() { ["counter_name"] = "Batch Requests/sec" })
        ],
        ["CXPACKET"] =
        [
            new("get_top_queries_by_cpu", "Find parallel queries consuming CPU", new() { ["parallel_only"] = "true" }),
            new("get_wait_trend", "Track the parallelism wait trend over time", new() { ["wait_type"] = "CXPACKET" }),
            new("audit_config", "Check CTFP and MAXDOP settings")
        ],
        ["THREADPOOL"] =
        [
            new("get_cpu_scheduler_pressure", "Check runnable-queue depth and worker-thread exhaustion"),
            new("get_top_queries_by_cpu", "Find queries consuming the most resources"),
            new("get_blocking", "Check whether blocking is holding worker threads")
        ],
        ["PAGEIOLATCH_SH"] =
        [
            new("get_file_io_stats", "Check I/O latency per database file"),
            new("get_file_io_trend", "Track I/O latency over time"),
            new("get_memory_stats", "Check buffer pool and memory pressure"),
            new("get_resource_semaphore", "Check for memory grant pressure competing with the buffer pool")
        ],
        ["PAGEIOLATCH_EX"] =
        [
            new("get_file_io_stats", "Check I/O latency per database file"),
            new("get_file_io_trend", "Track I/O latency over time"),
            new("get_memory_stats", "Check buffer pool and memory pressure"),
            new("get_tempdb_trend", "Check whether tempdb I/O is driving the EX-mode waits")
        ],
        ["RESOURCE_SEMAPHORE"] =
        [
            new("get_resource_semaphore", "Check granted vs available workspace memory and waiter counts"),
            new("get_memory_stats", "Check overall memory allocation"),
            new("get_top_queries_by_cpu", "Find queries requesting large memory grants")
        ],
        ["WRITELOG"] =
        [
            new("get_file_io_stats", "Check transaction log file latency"),
            new("get_file_io_trend", "Track log I/O latency over time"),
            new("get_perfmon_trend", "Check Transactions/sec to see the commit rate driving log flush pressure", new() { ["counter_name"] = "Transactions/sec" })
        ],
        ["LCK"] =
        [
            new("get_blocking", "Get detailed blocking event reports"),
            new("get_blocking_deadlock_stats", "Track blocking frequency over time"),
            new("get_active_queries", "See currently running sessions with their lock/wait detail")
        ],
        ["LCK_M_S"] =
        [
            new("get_blocking", "Get reader/writer blocking details"),
            new("get_blocking_deadlock_stats", "Track blocking frequency over time")
        ],
        ["LCK_M_IS"] =
        [
            new("get_blocking", "Get reader/writer blocking details"),
            new("get_blocking_deadlock_stats", "Track blocking frequency over time")
        ],
        ["BLOCKING_EVENTS"] =
        [
            new("get_blocking", "Get detailed blocking reports with full query text"),
            new("get_blocking_deadlock_stats", "Track blocking event frequency over time"),
            new("get_deadlocks", "Check whether blocking is escalating to deadlocks")
        ],
        ["DEADLOCKS"] =
        [
            new("get_deadlocks", "Get recent deadlock events with victim info"),
            new("get_deadlock_detail", "Get the full deadlock graph XML for deep analysis"),
            new("get_blocking_deadlock_stats", "Track deadlock frequency over time")
        ],
        ["SCH_M"] =
        [
            new("get_active_queries", "See what's waiting on schema-modification locks"),
            new("get_blocking", "Check whether DDL operations are causing blocking"),
            new("get_running_jobs", "See whether maintenance jobs (index rebuilds, stats updates) are taking schema-modification locks")
        ],
        ["CPU_SQL_PERCENT"] =
        [
            new("get_cpu_utilization", "See the CPU trend over time"),
            new("get_top_queries_by_cpu", "Find queries consuming the most CPU"),
            new("get_perfmon_trend", "Check batch requests/sec for throughput context", new() { ["counter_name"] = "Batch Requests/sec" })
        ],
        ["CPU_SPIKE"] =
        [
            new("get_cpu_utilization", "See the CPU trend to identify when the spike occurred"),
            new("get_top_queries_by_cpu", "Find the queries that drove the CPU spike"),
            new("get_active_queries", "Find what was actually running during the spike")
        ],
        ["IO_READ_LATENCY_MS"] =
        [
            new("get_file_io_stats", "Check per-file read latency"),
            new("get_file_io_trend", "Track read latency over time"),
            new("get_memory_stats", "Check whether the buffer pool is undersized")
        ],
        ["IO_WRITE_LATENCY_MS"] =
        [
            new("get_file_io_stats", "Check per-file write latency"),
            new("get_file_io_trend", "Track write latency over time")
        ],
        ["TEMPDB_USAGE"] =
        [
            new("get_tempdb_trend", "Track TempDB usage over time"),
            new("get_top_queries_by_cpu", "Find queries that may be spilling to TempDB")
        ],
        ["MEMORY_GRANT_PENDING"] =
        [
            new("get_resource_semaphore", "Check active vs pending memory grants and waiter counts"),
            new("get_memory_stats", "Check overall memory allocation"),
            new("get_top_queries_by_cpu", "Find queries requesting large grants")
        ],
        ["QUERY_SPILLS"] =
        [
            new("get_top_queries_by_cpu", "Find queries with spills"),
            new("get_resource_semaphore", "Check memory grant pressure"),
            new("get_tempdb_trend", "Check the TempDB impact from spills")
        ],
        ["QUERY_HIGH_DOP"] =
        [
            new("get_top_queries_by_cpu", "Find high-DOP queries", new() { ["parallel_only"] = "true" }),
            new("audit_config", "Check CTFP and MAXDOP settings")
        ],
        ["PARAMETER_SENSITIVITY"] =
        [
            new("get_top_queries_by_cpu", "Find the sensitive query in the plan cache and see its current cached parameters"),
            new("analyze_query_plan", "Examine the plan for the operators driving the runtime variance (seek vs scan, grant size, join type)"),
            new("get_query_trend", "Confirm the bimodal duration pattern across executions over time"),
            new("get_resource_semaphore", "Check whether the bad-parameter executions are also blowing up memory grants")
        ],
        ["PLAN_REGRESSION"] =
        [
            new("analyze_query_store_plan", "Compare the regressed plan against the prior plan to see what the optimizer changed"),
            new("get_query_trend", "Confirm the regression timing and that the new plan is consistently worse"),
            new("get_query_store_top", "Pull the full Query Store entry including plan_id and forced-plan history before considering a force")
        ],
        ["LATCH_EX"] =
        [
            new("get_latch_stats", "Check latch contention by class"),
            new("get_tempdb_trend", "Check TempDB for allocation contention"),
            new("get_wait_trend", "Track the latch contention trend", new() { ["wait_type"] = "LATCH_EX" })
        ],
        ["LATCH_SH"] =
        [
            new("get_latch_stats", "Check latch contention by class"),
            new("get_tempdb_trend", "Check TempDB for allocation contention"),
            new("get_wait_trend", "Track the latch contention trend", new() { ["wait_type"] = "LATCH_SH" })
        ],
        ["DB_CONFIG"] =
        [
            new("audit_config", "Check server-level configuration against best practices"),
            new("get_blocking", "Check whether RCSI-off databases are seeing blocking")
        ],
        ["FILE_AUTOGROWTH_PERCENT"] =
        [
            new("get_database_sizes", "See per-file sizes and autogrowth settings"),
            new("get_file_io_stats", "Check per-file growth and latency")
        ],
        ["RUNNING_JOBS"] =
        [
            new("get_running_jobs", "See currently running jobs with duration vs historical average and p95"),
            new("get_cpu_utilization", "Check whether long-running jobs are consuming CPU")
        ],
        ["DISK_SPACE"] =
        [
            new("get_database_sizes", "See per-file sizes and underlying volume free space"),
            new("get_tempdb_trend", "Check TempDB growth on the volume")
        ],
        ["BAD_ACTOR"] =
        [
            new("get_top_queries_by_cpu", "See full query stats for this query"),
            new("analyze_query_plan", "Analyze the execution plan for optimization opportunities"),
            new("get_query_trend", "Track this query's performance over time")
        ],
        ["ANOMALY_CPU"] =
        [
            new("get_cpu_utilization", "See the CPU trend to identify when the spike occurred"),
            new("get_active_queries", "Find what queries were running during the spike"),
            new("get_top_queries_by_cpu", "Find the most CPU-expensive queries in the period")
        ],
        ["ANOMALY_WAIT"] =
        [
            new("get_wait_stats", "See the full wait stats breakdown"),
            new("get_wait_trend", "Track the anomalous wait type over time"),
            new("compare_analysis", "Compare current vs baseline to see what changed")
        ],
        ["ANOMALY_BLOCKING"] =
        [
            new("get_blocking", "Get detailed blocking event reports"),
            new("get_deadlocks", "Get recent deadlock events"),
            new("get_blocking_deadlock_stats", "Track blocking frequency over time")
        ],
        ["ANOMALY_IO"] =
        [
            new("get_file_io_stats", "Check per-file I/O latency"),
            new("get_file_io_trend", "Track I/O latency over time"),
            new("get_memory_stats", "Check whether the buffer pool is undersized")
        ],
        ["ANOMALY_SESSION_SPIKE"] =
        [
            new("get_session_stats", "See which application is driving the session-count spike"),
            new("get_active_queries", "Find what those sessions were doing at the spike"),
            new("get_cpu_scheduler_pressure", "Check whether the new sessions are exhausting worker threads")
        ],
        ["ANOMALY_QUERY_DURATION"] =
        [
            new("get_query_trend", "Confirm the duration shift across the analysis window"),
            new("get_top_queries_by_cpu", "Find the queries whose runtime moved the average"),
            new("analyze_query_plan", "Examine the plan for the queries that slowed down")
        ],
        ["ANOMALY_MEMORY_PRESSURE"] =
        [
            new("get_memory_stats", "See current memory allocation and target vs total"),
            new("get_memory_clerks", "Find which clerks are growing"),
            new("get_memory_pressure_events", "Pull the RING_BUFFER_RESOURCE_MONITOR notifications driving the anomaly"),
            new("get_resource_semaphore", "Check whether query grants are competing with the buffer pool")
        ],
        ["ANOMALY_BATCH_REQUESTS"] =
        [
            new("get_perfmon_trend", "Confirm the batch-rate change across the window", new() { ["counter_name"] = "Batch Requests/sec" }),
            new("get_top_queries_by_cpu", "Find which queries account for the new batch volume"),
            new("get_active_queries", "See what's actually running at the elevated rate")
        ]
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
                if (key.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("BAD_ACTOR", out recommendations);
                else if (key.StartsWith("ANOMALY_CPU", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_CPU", out recommendations);
                else if (key.StartsWith("ANOMALY_WAIT_", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_WAIT", out recommendations);
                else if (key.StartsWith("ANOMALY_BLOCKING", StringComparison.OrdinalIgnoreCase)
                    || key.StartsWith("ANOMALY_DEADLOCK", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_BLOCKING", out recommendations);
                else if (key.StartsWith("ANOMALY_READ", StringComparison.OrdinalIgnoreCase)
                    || key.StartsWith("ANOMALY_WRITE", StringComparison.OrdinalIgnoreCase))
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

internal sealed record ToolRecommendation(
    string Tool,
    string Reason,
    System.Collections.Generic.Dictionary<string, string>? SuggestedParams = null);
