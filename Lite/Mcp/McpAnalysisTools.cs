using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpAnalysisTools
{
    [McpServerTool(Name = "analyze_server"), Description("Runs the diagnostic inference engine against a server's collected data. Scores wait stats, blocking, memory, config, and other facts, then traverses a relationship graph to build evidence-backed stories about what's wrong and why. Anomaly detection compares the analysis window against 30-day time-bucketed baselines (hour-of-day x day-of-week) to identify deviations that are unusual for this specific time slot, not just unusual overall. Returns structured findings with severity scores, evidence chains, baseline context for anomalies, and recommended next tools to call.")]
    public static async Task<string> AnalyzeServer(
        AnalysisService analysisService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 4. Longer windows give more stable results but may miss recent spikes.")] int hours_back = 4)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var findings = await analysisService.AnalyzeAsync(
                resolved.Value.ServerId, resolved.Value.ServerName, hours_back);

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

    [McpServerTool(Name = "get_analysis_facts"), Description("Exposes the raw scored facts from the inference engine's collect+score pipeline WITHOUT graph traversal. Shows every observation the engine sees: wait stats as fraction-of-period, blocking rates, config settings, memory stats, plus base severity, final severity after amplifiers, and which amplifiers matched. Use this to understand exactly what the engine is working with, or to investigate facts that didn't reach the severity threshold for findings.")]
    public static async Task<string> GetAnalysisFacts(
        AnalysisService analysisService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 4.")] int hours_back = 4,
        [Description("Filter to a specific source category: waits, blocking, config, memory. Omit for all.")] string? source = null,
        [Description("Minimum severity to include. Default 0 (all facts). Use 0.5 to see only significant facts.")] double min_severity = 0)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var facts = await analysisService.CollectAndScoreFactsAsync(
                resolved.Value.ServerId, resolved.Value.ServerName, hours_back);

            if (facts.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.Value.ServerName,
                    fact_count = 0,
                    message = "No facts collected. The collector may not have run yet, or no data exists in the requested time range."
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

    [McpServerTool(Name = "compare_analysis"), Description("Compares two time periods by running the inference engine's fact collection and scoring on each, then showing what changed. Use this to compare peak vs off-peak, before vs after a change, or yesterday vs today. Returns facts from both periods side-by-side with severity deltas. Note: for routine anomaly detection, use analyze_server instead — it automatically compares against 30-day time-bucketed baselines (hour-of-day x day-of-week). This tool is for explicit window-to-window comparisons.")]
    public static async Task<string> CompareAnalysis(
        AnalysisService analysisService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours back for the comparison (recent) period. Default 4.")] int hours_back = 4,
        [Description("Hours back for the baseline period start, measured from now. Default 28 (yesterday same time). The baseline period will be the same duration as the comparison period.")] int baseline_hours_back = 28)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateHoursBack(baseline_hours_back);
        if (validation != null) return validation;

        if (baseline_hours_back <= hours_back)
            return "baseline_hours_back must be greater than hours_back. The baseline period must be earlier than the comparison period.";

        try
        {
            var now = DateTime.UtcNow;
            var comparisonEnd = now;
            var comparisonStart = now.AddHours(-hours_back);
            var baselineEnd = now.AddHours(-baseline_hours_back + hours_back);
            var baselineStart = now.AddHours(-baseline_hours_back);

            var (baselineFacts, comparisonFacts) = await analysisService.ComparePeriodsAsync(
                resolved.Value.ServerId, resolved.Value.ServerName,
                baselineStart, baselineEnd,
                comparisonStart, comparisonEnd);

            var baselineByKey = baselineFacts.ToDictionary(f => f.Key, f => f);
            var comparisonByKey = comparisonFacts.ToDictionary(f => f.Key, f => f);
            var allKeys = baselineByKey.Keys.Union(comparisonByKey.Keys).ToHashSet();

            var comparisons = allKeys
                .Select(key =>
                {
                    var baseline = baselineByKey.GetValueOrDefault(key);
                    var comparison = comparisonByKey.GetValueOrDefault(key);
                    var severityDelta = (comparison?.Severity ?? 0) - (baseline?.Severity ?? 0);

                    return new
                    {
                        key,
                        source = baseline?.Source ?? comparison?.Source ?? "unknown",
                        baseline_value = baseline != null ? Math.Round(baseline.Value, 6) : (double?)null,
                        comparison_value = comparison != null ? Math.Round(comparison.Value, 6) : (double?)null,
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
                baseline = new
                {
                    start = baselineStart.ToString("o"),
                    end = baselineEnd.ToString("o"),
                    fact_count = baselineFacts.Count
                },
                comparison = new
                {
                    start = comparisonStart.ToString("o"),
                    end = comparisonEnd.ToString("o"),
                    fact_count = comparisonFacts.Count
                },
                summary = new
                {
                    worse = comparisons.Count(c => c.status == "worse"),
                    better = comparisons.Count(c => c.status == "better"),
                    stable = comparisons.Count(c => c.status == "stable"),
                    new_issues = comparisons.Count(c => c.baseline_severity == null && c.comparison_severity > 0),
                    resolved_issues = comparisons.Count(c => c.baseline_severity > 0 && c.comparison_severity == null)
                },
                facts = comparisons
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("compare_analysis", ex);
        }
    }

    [McpServerTool(Name = "audit_config"), Description("Evaluates SQL Server configuration settings against best practices, accounting for edition (Standard vs Enterprise) and server resources. Checks CTFP, MAXDOP, max server memory, and max worker threads. Returns specific recommendations with current values, recommended values, and reasoning.")]
    public static async Task<string> AuditConfig(
        AnalysisService analysisService,
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
            var facts = await analysisService.CollectAndScoreFactsAsync(
                resolved.Value.ServerId, resolved.Value.ServerName, 1);

            var factsByKey = facts.ToDictionary(f => f.Key, f => f);

            var edition = factsByKey.TryGetValue("SERVER_EDITION", out var edFact) ? (int)edFact.Value : 0;
            var totalMemoryMb = factsByKey.TryGetValue("MEMORY_TOTAL_PHYSICAL_MB", out var memFact) ? memFact.Value : 0;
            var totalDbSizeMb = factsByKey.TryGetValue("DATABASE_TOTAL_SIZE_MB", out var dbFact) ? dbFact.Value : 0;

            // Edition names: 3 = Enterprise, 2 = Standard, 4 = Express
            var editionName = edition switch
            {
                1 => "Personal",
                2 => "Standard",
                3 => "Enterprise",
                4 => "Express",
                5 => "Azure SQL Database",
                6 => "Azure SQL Managed Instance",
                8 => "Azure SQL Managed Instance (HADR)",
                9 => "Azure SQL Edge",
                11 => "Azure Synapse serverless",
                _ => "Unknown"
            };
            var isEnterprise = edition == 3;
            var isExpress = edition == 4;

            var recommendations = new List<ConfigRecommendation>();

            // CTFP audit
            if (factsByKey.TryGetValue("CONFIG_CTFP", out var ctfpFact))
            {
                var ctfp = (int)ctfpFact.Value;

                if (ctfp <= 5)
                {
                    recommendations.Add(new("cost threshold for parallelism", ctfp, 50, "warning",
                        $"CTFP is at the default ({ctfp}). Most OLTP workloads benefit from 50. " +
                        "A low CTFP causes excessive parallelism for trivial queries, wasting worker threads and causing CXPACKET waits."));
                }
                else if (ctfp < 25)
                {
                    recommendations.Add(new("cost threshold for parallelism", ctfp, 50, "review",
                        $"CTFP ({ctfp}) is low. Consider raising to 50 unless you have a specific reason for this value."));
                }
                else if (ctfp > 100)
                {
                    recommendations.Add(new("cost threshold for parallelism", ctfp, 50, "review",
                        $"CTFP ({ctfp}) is unusually high. This forces serial execution for many queries that would benefit from parallelism. " +
                        "Review whether this was set intentionally. Consider 50 as a starting point."));
                }
                else
                {
                    recommendations.Add(new("cost threshold for parallelism", ctfp, ctfp, "ok",
                        $"CTFP ({ctfp}) is in a reasonable range."));
                }
            }

            // MAXDOP audit
            if (factsByKey.TryGetValue("CONFIG_MAXDOP", out var maxdopFact))
            {
                var maxdop = (int)maxdopFact.Value;

                if (maxdop == 0)
                {
                    var suggested = isExpress ? 1 : isEnterprise ? 8 : 4;
                    recommendations.Add(new("max degree of parallelism", maxdop, suggested, "warning",
                        $"MAXDOP is 0 (unlimited). This allows queries to use all schedulers, " +
                        $"leading to CXPACKET waits and thread exhaustion under load. " +
                        $"For {editionName} edition, start with MAXDOP {suggested} and adjust based on workload."));
                }
                else if (maxdop == 1)
                {
                    var suggested = isExpress ? 1 : 4;
                    recommendations.Add(new("max degree of parallelism", maxdop, suggested,
                        isExpress ? "ok" : "review",
                        isExpress
                            ? "MAXDOP 1 is appropriate for Express edition."
                            : $"MAXDOP 1 forces all queries serial. Large analytical queries, index rebuilds, and DBCC operations " +
                              $"will be significantly slower. Consider MAXDOP {suggested} unless this was set to fix a specific parallelism problem."));
                }
                else if (maxdop > 8 && !isEnterprise)
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, 4, "review",
                        $"MAXDOP {maxdop} is high for {editionName} edition. Standard edition is limited to " +
                        $"fewer schedulers. Consider MAXDOP 4."));
                }
                else
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, maxdop, "ok",
                        $"MAXDOP {maxdop} is in a reasonable range for {editionName} edition."));
                }
            }

            // Max memory audit
            if (factsByKey.TryGetValue("CONFIG_MAX_MEMORY_MB", out var maxMemFact))
            {
                var maxMemory = (int)maxMemFact.Value;

                if (maxMemory == 2147483647) // Default — unlimited
                {
                    if (totalMemoryMb > 0)
                    {
                        var osReserve = Math.Max(4096, totalMemoryMb * 0.10);
                        var suggested = (int)(totalMemoryMb - osReserve);
                        recommendations.Add(new("max server memory (MB)", maxMemory, suggested, "warning",
                            $"Max server memory is at the default (unlimited). SQL Server will consume all available RAM, " +
                            $"starving the OS and other processes. With {totalMemoryMb:N0} MB physical RAM, set max server memory to " +
                            $"~{suggested:N0} MB (leaving {osReserve:N0} MB for the OS)."));
                    }
                    else
                    {
                        recommendations.Add(new("max server memory (MB)", maxMemory, maxMemory, "warning",
                            "Max server memory is at the default (unlimited). SQL Server will consume all available RAM. " +
                            "Set this to total physical memory minus 4 GB (or 10%, whichever is larger) to leave room for the OS."));
                    }
                }
                else if (totalMemoryMb > 0)
                {
                    var ratio = maxMemory / totalMemoryMb;
                    var osReserve = Math.Max(4096, totalMemoryMb * 0.10);
                    var suggested = (int)(totalMemoryMb - osReserve);

                    if (ratio > 0.95)
                    {
                        recommendations.Add(new("max server memory (MB)", maxMemory, suggested, "review",
                            $"Max server memory ({maxMemory:N0} MB) is {ratio:P0} of physical RAM ({totalMemoryMb:N0} MB). " +
                            $"Consider reducing to ~{suggested:N0} MB to leave room for the OS."));
                    }
                    else if (ratio < 0.50 && totalMemoryMb > 8192)
                    {
                        recommendations.Add(new("max server memory (MB)", maxMemory, suggested, "review",
                            $"Max server memory ({maxMemory:N0} MB) is only {ratio:P0} of physical RAM ({totalMemoryMb:N0} MB). " +
                            $"SQL Server may be under-utilizing available memory. Consider raising to ~{suggested:N0} MB unless other " +
                            "applications need the remaining RAM."));
                    }
                    else
                    {
                        recommendations.Add(new("max server memory (MB)", maxMemory, maxMemory, "ok",
                            $"Max server memory ({maxMemory:N0} MB) looks reasonable for {totalMemoryMb:N0} MB physical RAM."));
                    }
                }
                else
                {
                    recommendations.Add(new("max server memory (MB)", maxMemory, maxMemory, "ok",
                        $"Max server memory is set to {maxMemory:N0} MB."));
                }
            }

            // Max worker threads audit
            if (factsByKey.TryGetValue("CONFIG_MAX_WORKER_THREADS", out var mwtFact))
            {
                var mwt = (int)mwtFact.Value;

                if (mwt == 0)
                {
                    recommendations.Add(new("max worker threads", mwt, 0, "ok",
                        "Max worker threads is 0 (auto-configured by SQL Server). This is the recommended setting " +
                        "for most workloads. SQL Server calculates the optimal value based on the number of processors."));
                }
                else if (mwt < 256)
                {
                    recommendations.Add(new("max worker threads", mwt, 0, "review",
                        $"Max worker threads is set to {mwt}, which is low. Unless this was set to diagnose a specific " +
                        "thread exhaustion issue, consider resetting to 0 (auto) and addressing the root cause of thread pressure instead."));
                }
                else
                {
                    recommendations.Add(new("max worker threads", mwt, 0, "ok",
                        $"Max worker threads is set to {mwt}. If this was explicitly configured, ensure it was for a documented reason."));
                }
            }

            if (recommendations.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.Value.ServerName,
                    status = "no_config_data",
                    message = "No configuration data found. The config collector may not have run yet."
                }, McpHelpers.JsonOptions);
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                edition = editionName,
                total_physical_memory_mb = totalMemoryMb > 0 ? totalMemoryMb : (double?)null,
                total_database_size_mb = totalDbSizeMb > 0 ? totalDbSizeMb : (double?)null,
                summary = new
                {
                    settings_checked = recommendations.Count,
                    warnings = recommendations.Count(r => r.Status == "warning"),
                    needs_review = recommendations.Count(r => r.Status == "review")
                },
                recommendations = recommendations.Select(r => new
                {
                    setting = r.Setting,
                    current_value = r.CurrentValue,
                    suggested_value = r.SuggestedValue,
                    status = r.Status,
                    recommendation = r.Recommendation
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("audit_config", ex);
        }
    }

    [McpServerTool(Name = "get_analysis_findings"), Description("Gets persisted findings from previous analysis runs without running a new analysis. Use this to review historical findings or check if anything has changed since the last analysis.")]
    public static async Task<string> GetAnalysisFindings(
        AnalysisService analysisService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of finding history to retrieve. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var findings = await analysisService.GetRecentFindingsAsync(
                resolved.Value.ServerId, hours_back);

            if (findings.Count == 0)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.Value.ServerName,
                    finding_count = 0,
                    message = "No findings in the requested time range. Run analyze_server to generate new findings."
                }, McpHelpers.JsonOptions);
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                finding_count = findings.Count,
                findings = findings.Select(f => new
                {
                    finding_id = f.FindingId,
                    analysis_time = f.AnalysisTime.ToString("o"),
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
                    time_range = new
                    {
                        start = f.TimeRangeStart?.ToString("o"),
                        end = f.TimeRangeEnd?.ToString("o")
                    }
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_analysis_findings", ex);
        }
    }

    [McpServerTool(Name = "mute_analysis_finding"), Description("Mutes a finding pattern so it won't appear in future analysis runs. Use the story_path_hash from analyze_server or get_analysis_findings output. Muting is per-pattern, not per-occurrence — the same diagnostic chain won't be reported again until unmuted.")]
    public static async Task<string> MuteAnalysisFinding(
        AnalysisService analysisService,
        ServerManager serverManager,
        [Description("The story_path_hash from the finding to mute.")] string story_path_hash,
        [Description("Server name. If omitted, mutes across all servers.")] string? server_name = null,
        [Description("Optional reason for muting.")] string? reason = null)
    {
        try
        {
            int? serverId = null;
            if (server_name != null)
            {
                var resolved = ServerResolver.Resolve(serverManager, server_name);
                if (resolved == null)
                {
                    return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
                }
                serverId = resolved.Value.ServerId;
            }

            var finding = new AnalysisFinding
            {
                ServerId = serverId ?? 0,
                StoryPathHash = story_path_hash,
                StoryPath = story_path_hash
            };

            await analysisService.MuteFindingAsync(finding, reason);

            return JsonSerializer.Serialize(new
            {
                status = "muted",
                story_path_hash,
                server = server_name ?? "(all servers)",
                reason
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
/// Used by analyze_server to tell the AI client what to call next.
/// </summary>
internal static class ToolRecommendations
{
    private static readonly Dictionary<string, List<ToolRecommendation>> ByFactKey = new()
    {
        ["SOS_SCHEDULER_YIELD"] =
        [
            new("get_cpu_utilization", "Check SQL Server vs other process CPU usage over time"),
            new("get_top_queries_by_cpu", "Find the most CPU-expensive queries"),
            new("get_perfmon_trend", "Check batch requests/sec trend", new() { ["counter_name"] = "Batch Requests/sec" })
        ],
        ["CXPACKET"] =
        [
            new("get_top_queries_by_cpu", "Find parallel queries consuming CPU", new() { ["parallel_only"] = "true" }),
            new("get_wait_trend", "Track parallelism wait trend over time", new() { ["wait_type"] = "CXPACKET" }),
            new("audit_config", "Check CTFP and MAXDOP settings")
        ],
        ["THREADPOOL"] =
        [
            new("get_waiting_tasks", "See what's actively waiting for worker threads"),
            new("get_top_queries_by_cpu", "Find queries consuming the most resources"),
            new("get_blocked_process_reports", "Check if blocking is holding worker threads")
        ],
        ["PAGEIOLATCH_SH"] =
        [
            new("get_file_io_stats", "Check I/O latency per database file"),
            new("get_file_io_trend", "Track I/O latency trend"),
            new("get_memory_stats", "Check buffer pool and memory pressure"),
            new("get_memory_grants", "Check for memory grant pressure competing with buffer pool")
        ],
        ["PAGEIOLATCH_EX"] =
        [
            new("get_file_io_stats", "Check I/O latency per database file"),
            new("get_file_io_trend", "Track I/O latency trend"),
            new("get_memory_stats", "Check buffer pool and memory pressure")
        ],
        ["RESOURCE_SEMAPHORE"] =
        [
            new("get_memory_grants", "Check active/pending memory grants"),
            new("get_memory_stats", "Check overall memory allocation"),
            new("get_top_queries_by_cpu", "Find queries requesting large memory grants")
        ],
        ["WRITELOG"] =
        [
            new("get_file_io_stats", "Check transaction log file latency"),
            new("get_file_io_trend", "Track log I/O latency over time")
        ],
        ["LCK"] =
        [
            new("get_blocked_process_reports", "Get detailed blocking event reports"),
            new("get_blocking_trend", "Track blocking frequency over time"),
            new("get_waiting_tasks", "See currently waiting tasks with lock details")
        ],
        ["LCK_M_S"] =
        [
            new("get_blocked_process_reports", "Get reader/writer blocking details"),
            new("get_blocking_trend", "Track blocking frequency over time")
        ],
        ["LCK_M_IS"] =
        [
            new("get_blocked_process_reports", "Get reader/writer blocking details"),
            new("get_blocking_trend", "Track blocking frequency over time")
        ],
        ["BLOCKING_EVENTS"] =
        [
            new("get_blocked_process_reports", "Get detailed blocking reports with full query text"),
            new("get_blocking_trend", "Track blocking event frequency over time"),
            new("get_deadlocks", "Check if blocking is escalating to deadlocks")
        ],
        ["DEADLOCKS"] =
        [
            new("get_deadlocks", "Get recent deadlock events with victim info"),
            new("get_deadlock_detail", "Get full deadlock graph XML for deep analysis"),
            new("get_deadlock_trend", "Track deadlock frequency over time")
        ],
        ["SCH_M"] =
        [
            new("get_waiting_tasks", "See what's waiting on schema locks"),
            new("get_blocked_process_reports", "Check if DDL operations are causing blocking")
        ],
        ["CPU_SQL_PERCENT"] =
        [
            new("get_cpu_utilization", "See CPU trend over time"),
            new("get_top_queries_by_cpu", "Find queries consuming the most CPU"),
            new("get_perfmon_trend", "Check batch requests/sec for throughput context", new() { ["counter_name"] = "Batch Requests/sec" })
        ],
        ["CPU_SPIKE"] =
        [
            new("get_cpu_utilization", "See CPU trend to identify when the spike occurred"),
            new("get_top_queries_by_cpu", "Find queries that drove the CPU spike"),
            new("get_query_duration_trend", "Check if query durations spiked at the same time")
        ],
        ["IO_READ_LATENCY_MS"] =
        [
            new("get_file_io_stats", "Check per-file read latency"),
            new("get_file_io_trend", "Track read latency over time"),
            new("get_memory_stats", "Check if buffer pool is undersized")
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
            new("get_memory_grants", "Check active/pending memory grants"),
            new("get_memory_stats", "Check overall memory allocation"),
            new("get_top_queries_by_cpu", "Find queries requesting large grants")
        ],
        ["QUERY_SPILLS"] =
        [
            new("get_top_queries_by_cpu", "Find queries with spills"),
            new("get_memory_grants", "Check memory grant pressure"),
            new("get_tempdb_trend", "Check TempDB impact from spills")
        ],
        ["QUERY_HIGH_DOP"] =
        [
            new("get_top_queries_by_cpu", "Find high-DOP queries", new() { ["parallel_only"] = "true" }),
            new("audit_config", "Check CTFP and MAXDOP settings")
        ],
        ["PERFMON_PLE"] =
        [
            new("get_memory_stats", "Check buffer pool and memory allocation"),
            new("get_memory_clerks", "See where memory is allocated"),
            new("get_memory_trend", "Track memory usage over time")
        ],
        ["LATCH_EX"] =
        [
            new("get_tempdb_trend", "Check TempDB for allocation contention"),
            new("get_top_queries_by_cpu", "Find queries causing latch contention"),
            new("get_wait_trend", "Track latch contention trend", new() { ["wait_type"] = "LATCH_EX" })
        ],
        ["LATCH_SH"] =
        [
            new("get_tempdb_trend", "Check TempDB for allocation contention"),
            new("get_wait_trend", "Track latch contention trend", new() { ["wait_type"] = "LATCH_SH" })
        ],
        ["DB_CONFIG"] =
        [
            new("audit_config", "Check server-level configuration"),
            new("get_blocked_process_reports", "Check if RCSI-off databases have blocking")
        ],
        ["RUNNING_JOBS"] =
        [
            new("get_running_jobs", "See currently running jobs with duration vs historical"),
            new("get_cpu_utilization", "Check if long-running jobs are consuming CPU")
        ],
        ["ANOMALY_CPU"] =
        [
            new("get_cpu_utilization", "See CPU trend to identify when the spike occurred"),
            new("get_active_queries", "Find what queries were running during the spike"),
            new("get_top_queries_by_cpu", "Find the most CPU-expensive queries in the period")
        ],
        ["ANOMALY_WAIT"] =
        [
            new("get_wait_stats", "See full wait stats breakdown"),
            new("get_wait_trend", "Track the anomalous wait type over time"),
            new("compare_analysis", "Compare current vs baseline to see what changed")
        ],
        ["ANOMALY_BLOCKING"] =
        [
            new("get_blocked_process_reports", "Get detailed blocking event reports"),
            new("get_deadlocks", "Get recent deadlock events"),
            new("get_blocking_trend", "Track blocking frequency over time")
        ],
        ["ANOMALY_IO"] =
        [
            new("get_file_io_stats", "Check per-file I/O latency"),
            new("get_file_io_trend", "Track I/O latency over time"),
            new("get_memory_stats", "Check if buffer pool is undersized")
        ],
        ["BAD_ACTOR"] =
        [
            new("get_top_queries_by_cpu", "See full query stats for this query"),
            new("analyze_query_plan", "Analyze the execution plan for optimization opportunities"),
            new("get_query_trend", "Track this query's performance over time")
        ],
        ["DISK_SPACE"] =
        [
            new("get_file_io_stats", "Check per-file sizes and I/O"),
            new("get_tempdb_trend", "Check TempDB growth on the volume")
        ]
    };

    /// <summary>
    /// Returns tool recommendations for all fact keys in a story path.
    /// Deduplicates across the path so each tool appears at most once.
    /// </summary>
    public static List<object> GetForStoryPath(string storyPath)
    {
        var factKeys = storyPath.Split(" → ", StringSplitOptions.RemoveEmptyEntries);
        var seen = new HashSet<string>();
        var result = new List<object>();

        foreach (var key in factKeys)
        {
            if (!ByFactKey.TryGetValue(key, out var recommendations))
            {
                // Handle dynamic keys by checking prefix
                if (key.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("BAD_ACTOR", out recommendations);
                else if (key.StartsWith("ANOMALY_CPU", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_CPU", out recommendations);
                else if (key.StartsWith("ANOMALY_WAIT_", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_WAIT", out recommendations);
                else if (key.StartsWith("ANOMALY_BLOCKING", StringComparison.OrdinalIgnoreCase) || key.StartsWith("ANOMALY_DEADLOCK", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_BLOCKING", out recommendations);
                else if (key.StartsWith("ANOMALY_READ", StringComparison.OrdinalIgnoreCase) || key.StartsWith("ANOMALY_WRITE", StringComparison.OrdinalIgnoreCase))
                    ByFactKey.TryGetValue("ANOMALY_IO", out recommendations);
                if (recommendations == null) continue;
            }

            foreach (var rec in recommendations)
            {
                if (!seen.Add(rec.Tool)) continue;

                if (rec.SuggestedParams != null && rec.SuggestedParams.Count > 0)
                {
                    result.Add(new
                    {
                        tool = rec.Tool,
                        reason = rec.Reason,
                        suggested_params = rec.SuggestedParams
                    });
                }
                else
                {
                    result.Add(new
                    {
                        tool = rec.Tool,
                        reason = rec.Reason
                    });
                }
            }
        }

        return result;
    }

    private static readonly string[] DayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    /// <summary>
    /// Formats baseline context from anomaly fact metadata into a human-readable object
    /// for MCP output. Example: "4.1σ above baseline for Tue 14:00, mean 68.2"
    /// </summary>
    private static object? FormatBaselineContext(Dictionary<string, double> metadata)
    {
        var result = new Dictionary<string, object>();

        if (metadata.TryGetValue("deviation_sigma", out var sigma))
            result["deviation"] = $"{sigma:F1}σ";

        if (metadata.TryGetValue("ratio", out var ratio))
            result["ratio"] = $"{ratio:F1}x";

        if (metadata.TryGetValue("baseline_mean", out var mean))
            result["baseline_mean"] = Math.Round(mean, 2);

        if (metadata.TryGetValue("baseline_mean_ms", out var meanMs))
            result["baseline_mean"] = Math.Round(meanMs, 2);

        if (metadata.TryGetValue("baseline_stddev", out var stddev))
            result["baseline_stddev"] = Math.Round(stddev, 2);

        if (metadata.TryGetValue("baseline_hour", out var hour) &&
            metadata.TryGetValue("baseline_dow", out var dow))
        {
            var dowIdx = (int)dow;
            var dayName = dowIdx >= 0 && dowIdx < DayNames.Length ? DayNames[dowIdx] : "?";
            result["bucket"] = hour >= 0 ? $"{dayName} {(int)hour:00}:00" : "flat";
        }

        if (metadata.TryGetValue("baseline_tier", out var tier))
            result["tier"] = tier switch { 0 => "full", 1 => "hour_only", _ => "flat" };

        if (metadata.TryGetValue("baseline_samples", out var samples))
            result["baseline_samples"] = (int)samples;

        return result.Count > 0 ? result : null;
    }
}

internal record ToolRecommendation(
    string Tool,
    string Reason,
    Dictionary<string, string>? SuggestedParams = null);

internal record ConfigRecommendation(
    string Setting,
    int CurrentValue,
    int SuggestedValue,
    string Status,
    string Recommendation);
