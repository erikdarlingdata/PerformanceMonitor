/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The six diagnostic-analysis MCP tools — the SAME tool surface Lite and the Dashboard expose
/// (analyze_server, get_analysis_facts, compare_analysis, audit_config, get_analysis_findings,
/// mute_analysis_finding), served over Darling's Postgres store. Each tool body mirrors Lite's
/// <c>McpAnalysisTools</c> field-for-field (same response envelopes, the #1224 miss vocabulary
/// via <see cref="McpHelpers.Status"/>, the shared <see cref="FactAdvice"/> /
/// <see cref="CoFiredSummary"/> composition and the per-app <see cref="ToolRecommendations"/>
/// copy) so an MCP client sees one consistent product across all three SKUs; the two seam
/// differences are the service dependencies — <see cref="DarlingAnalysisService"/> instead of
/// Lite's AnalysisService, and server resolution through the Postgres servers registry
/// (<see cref="DarlingServerResolver"/>) instead of Lite's in-memory ServerManager.
/// A response-shape change here must land in BOTH apps' McpAnalysisTools too, and vice versa.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpTools
{
    [McpServerTool(Name = "analyze_server"), Description("Runs the diagnostic inference engine against a server's collected data. Scores wait stats, blocking, memory, config, and other facts, then traverses a relationship graph to build evidence-backed stories about what's wrong and why. Anomaly detection compares the analysis window against 30-day time-bucketed baselines (hour-of-day x day-of-week) to identify deviations that are unusual for this specific time slot, not just unusual overall. Returns structured findings with severity scores, evidence chains, baseline context for anomalies, and recommended next tools to call. A remediable finding also carries remediation_command: the full copy-paste T-SQL remediation (identical to the viewer card), including a two-sided risk-disclosure comment header on destructive changes; it is advisory only and never executed. A force-plan remediation additionally carries structured_remediation: the same decision as machine-readable fields — eligible, named blockers (parameter_sensitivity_cofired, secondary_replica_evidence), evidence numbers, and split force_sql/unforce_sql/verify_sql artifacts — so agents consume the verdict as data instead of parsing comment prose. Set as_of to analyze a PAST window instead of the present — hours_back stays the window's LENGTH, and the anomaly baseline moves with it, so the findings are the ones that window deserves rather than today's findings over older rows. An anchored run is EXPLORATORY: its findings are returned in full but deliberately NOT written to the store, because a finding row is stamped with the time the analysis RAN and would then be read as this server's current state by get_analysis_findings and by the viewer. The result says so in persisted / persistence_note.")]
    public static async Task<string> AnalyzeServer(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 4. Longer windows give more stable results but may miss recent spikes.")] int hours_back = 4,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        /* Null when the caller sent no anchor, and that distinction is load-bearing here rather than
           cosmetic: ValidateWindow hands back "now" for an absent as_of, and passing THAT through would
           make every ordinary run look anchored to the engine — which is exactly the set of runs that
           must still persist. AnalysisContext.AsOfUtc means "anchored", not "the window ends somewhere". */
        var anchor = string.IsNullOrWhiteSpace(as_of) ? (DateTime?)null : windowEnd;

        try
        {
            var findings = await analysisService.AnalyzeAsync(
                resolved.ServerId, resolved.ServerName, hours_back, asOfUtc: anchor);

            if (analysisService.InsufficientDataMessage != null)
            {
                return JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    status = "insufficient_data",
                    message = analysisService.InsufficientDataMessage
                }, McpHelpers.JsonOptions);
            }

            /* #2506: whether this run's findings reached the store, and why not when they did not.
               Reported rather than left to the documentation because the caller cannot otherwise tell:
               an anchored run returns a complete, correct set of findings that simply does not exist in
               analysis_findings, and an agent that assumed otherwise would tell someone to "check the
               persisted findings" for a run that never wrote any. */
            var persistenceNote = anchor is null
                ? null
                : "as_of was supplied, so this analysis ran over a PAST window and is exploratory: the findings below are complete but were NOT written to the store. A finding row carries the time the analysis RAN, and the reads that consume those rows (get_analysis_findings, the viewer's Recommendations tab) treat the newest analysis_time as this server's CURRENT state — so persisting a backdated run would make last week's findings today's headline and would inflate the occurrence stats of any live incident sharing a story path. Re-run without as_of to analyze and persist the present.";

            if (findings.Count == 0)
            {
                /* A successful analysis that found nothing wrong: a true negative ("all clear"),
                   surfaced with the shared miss vocabulary so callers branch on it uniformly. */
                return McpHelpers.Status(
                    "empty",
                    "No significant findings. All metrics are within normal ranges.",
                    new
                    {
                        analysis_time = analysisService.LastAnalysisTime?.ToString("o"),
                        persisted = anchor is null,
                        persistence_note = persistenceNote
                    });
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
                persisted = anchor is null,
                /* Null on the ordinary unanchored run — nothing needs saying when the answer is the
                   one every caller already assumed. */
                persistence_note = persistenceNote,
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
                        // The FULL copy-paste remediation command — the SAME text the viewer cards
                        // render — from the PERSISTED RemediationAction via the shared renderer (all
                        // seven shapes + the two-sided risk-disclosure comment header on the destructive
                        // ones). ADDITIVE alongside suggested_remediation_sql (the older 3-shape,
                        // drill-down-sourced advice-block SQL): this covers all seven shapes and also
                        // renders on get_analysis_findings, where the drill-down is gone. Null when the
                        // finding has no remediable action. PRODUCE ONLY — advisory text; the read-only
                        // MCP never executes it.
                        remediation_command = FactRemediation.RenderCopyPasteCommand(f.Remediation),
                        // #2138: the machine-first projection — verdict (eligible/blockers, the future
                        // bot's policy gate), evidence, and split force/unforce/verify artifacts as
                        // named fields, so an agent never regexes the comment prose above. Null for
                        // non-force-plan remediations. ADVISORY like everything else here.
                        structured_remediation = FactRemediation.BuildStructuredRemediation(f.Remediation),
                        // B3 Phase 3 (§6): two-sided risk DISCLOSURE for a destructive
                        // remediation, read-only (like Lite, Darling has no Apply path; its
                        // RCSI fields are null/0 so the inaction side shows the weak-case baseline).
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

    [McpServerTool(Name = "get_analysis_facts"), Description("Exposes the raw scored facts from the inference engine's collect+score pipeline WITHOUT graph traversal. Shows every observation the engine sees: wait stats as fraction-of-period, blocking rates, config settings, memory stats, plus base severity, final severity after amplifiers, and which amplifiers matched. Use this to understand exactly what the engine is working with, or to investigate facts that didn't reach the severity threshold for findings.")]
    public static async Task<string> GetAnalysisFacts(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 4.")] int hours_back = 4,
        [Description("Filter to a specific source category: waits, blocking, config, memory. Omit for all.")] string? source = null,
        [Description("Minimum severity to include. Default 0 (all facts). Use 0.5 to see only significant facts.")] double min_severity = 0,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        /* Null for an absent anchor — see analyze_server's note. Nothing here persists, so the
           distinction costs nothing; it is kept so AnalysisContext.AsOfUtc means one thing everywhere. */
        var anchor = string.IsNullOrWhiteSpace(as_of) ? (DateTime?)null : windowEnd;

        try
        {
            var facts = await analysisService.CollectAndScoreFactsAsync(
                resolved.ServerId, resolved.ServerName, hours_back, asOfUtc: anchor);

            if (facts.Count == 0)
            {
                /* No scored facts means the underlying collectors produced nothing for the window —
                   not retrievable now rather than an all-clear (mirrors get_perfmon_trend's empty case). */
                return McpHelpers.Status(
                    "unavailable",
                    "No facts collected. The collector may not have run yet, or no data exists in the requested time range.");
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

    [McpServerTool(Name = "compare_analysis"), Description("Compares two time periods by running the inference engine's fact collection and scoring on each, then showing what changed. Use this to compare peak vs off-peak, before vs after a change, or yesterday vs today. Returns facts from both periods side-by-side with severity deltas. Note: for routine anomaly detection, use analyze_server instead — it automatically compares against 30-day time-bucketed baselines (hour-of-day x day-of-week). This tool is for explicit window-to-window comparisons.")]
    public static async Task<string> CompareAnalysis(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours back for the comparison (recent) period. Default 4.")] int hours_back = 4,
        [Description("Hours back for the baseline period start, measured from the end of the comparison window (now, or as_of). Default 28 (yesterday same time). The baseline period will be the same duration as the comparison period.")] int baseline_hours_back = 28,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateHoursBack(baseline_hours_back);
        if (validation != null) return validation;

        if (baseline_hours_back <= hours_back)
            return "baseline_hours_back must be greater than hours_back. The baseline period must be earlier than the comparison period.";

        try
        {
            /* BOTH windows hang off the anchor, not just the comparison one — baseline_hours_back has
               always been measured from the comparison window's end, and moving only that end would
               silently change what the two windows are relative to each other. */
            var comparisonEnd = windowEnd;
            var comparisonStart = windowEnd.AddHours(-hours_back);
            var baselineEnd = windowEnd.AddHours(-baseline_hours_back + hours_back);
            var baselineStart = windowEnd.AddHours(-baseline_hours_back);

            var (baselineFacts, comparisonFacts) = await analysisService.ComparePeriodsAsync(
                resolved.ServerId, resolved.ServerName,
                baselineStart, baselineEnd,
                comparisonStart, comparisonEnd);

            var baselineByKey = baselineFacts.ToFactLookup();
            var comparisonByKey = comparisonFacts.ToFactLookup();
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

            if (comparisons.Count == 0)
            {
                /*
                    Neither window produced a single fact, and the old payload said that with all-zero
                    counters and facts: [] -- which reads as "nothing changed" when it actually means
                    "there was nothing to compare". Those are opposite conclusions about the same server.
                    No probe is needed to tell them apart: comparisons is the UNION of both windows' keys,
                    so zero entries is exactly "both fact sets were empty" and the fact_counts already in
                    hand are the whole answer.
                */
                return McpHelpers.Status(
                    "unavailable",
                    $"No analysis facts were collected for {resolved.ServerName} in EITHER window, so there is nothing to compare — this is NOT a report that nothing changed. Fact collection needs collected data in the window it scores; check that collection covered both periods (get_collection_log) before drawing any conclusion from this comparison.",
                    new
                    {
                        server = resolved.ServerName,
                        baseline_start = baselineStart.ToString("o"),
                        baseline_end = baselineEnd.ToString("o"),
                        comparison_start = comparisonStart.ToString("o"),
                        comparison_end = comparisonEnd.ToString("o"),
                    });
            }

            /*
                One window empty and the other populated is the OTHER way this read lies, and it lies
                loudly: every fact in the populated window lands in new_issues or resolved_issues purely
                because it has nothing to be compared against. "47 resolved issues" on a server whose recent
                window simply was not collected is a worse answer than no answer. Data-bearing results keep
                their own shape rather than the status envelope, so the warning rides in the payload.
            */
            var caveat =
                baselineFacts.Count == 0
                    ? "The BASELINE window produced no facts at all, so every fact below counts as a new issue only because there was nothing to compare it against. Confirm collection covered the baseline window (get_collection_log) before reading new_issues as a regression."
                    : comparisonFacts.Count == 0
                        ? "The COMPARISON window produced no facts at all, so every fact below counts as a resolved issue only because there is nothing in the recent window to compare against. Confirm collection is running (get_collection_log) before reading resolved_issues as an improvement."
                        : null;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* Null when both windows produced facts — the ordinary case, where nothing needs saying. */
                caveat,
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
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var facts = await analysisService.CollectAndScoreFactsAsync(
                resolved.ServerId, resolved.ServerName, 1);

            var factsByKey = facts.ToFactLookup();

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
            var coresPerSocket = factsByKey.TryGetValue("SERVER_HARDWARE", out var hwFact)
                && hwFact.Metadata.TryGetValue("cores_per_socket", out var cps) ? (int)cps : 0;

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

            // MAXDOP audit — topology-based (min(cores-per-socket, 8)), NOT edition-based.
            if (factsByKey.TryGetValue("CONFIG_MAXDOP", out var maxdopFact))
            {
                var maxdop = (int)maxdopFact.Value;
                var recommended = (int)FactRemediation.RecommendedMaxdop(coresPerSocket);

                if (maxdop == 0)
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, recommended, "warning",
                        $"MAXDOP is 0 (unlimited). This lets one query fan out across all schedulers, " +
                        $"leading to CXPACKET waits and thread exhaustion under load. Microsoft's guidance is " +
                        $"topology-based: keep MAXDOP at or under the logical processors in a single NUMA node, capped at 8. " +
                        $"Start with {recommended} (this server's cores-per-socket, capped at 8) and adjust to the workload."));
                }
                else if (maxdop == 1 && recommended > 1)
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, recommended, "review",
                        $"MAXDOP 1 forces every query serial. Large analytical queries, index rebuilds, and DBCC operations " +
                        $"will be significantly slower. Consider {recommended} unless this was set to fix a specific parallelism problem."));
                }
                else if (maxdop > recommended)
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, recommended, "review",
                        $"MAXDOP {maxdop} is above the topology-based guidance of {recommended} " +
                        $"(logical processors in a single NUMA node, capped at 8). Review whether queries here genuinely " +
                        $"benefit from the higher degree, or lower it to {recommended}."));
                }
                else
                {
                    recommendations.Add(new("max degree of parallelism", maxdop, maxdop, "ok",
                        $"MAXDOP {maxdop} is within the topology-based guidance (≤ {recommended})."));
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
                    server = resolved.ServerName,
                    status = "no_config_data",
                    message = "No configuration data found. The config collector may not have run yet."
                }, McpHelpers.JsonOptions);
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
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

    [McpServerTool(Name = "get_analysis_findings"), Description("Gets persisted findings from previous analysis runs without running a new analysis, deduplicated to one entry per diagnostic chain (story_path_hash + incident_id) - the engine re-persists the same stories every cycle, so each entry is the chain's LATEST occurrence plus occurrence stats (occurrences, first_seen, last_seen, peak_severity) spanning the window. Use this to review historical findings or check if anything has changed since the last analysis. A remediable finding carries remediation_command: the full copy-paste T-SQL remediation (identical to the viewer card), rendered from the finding's persisted action and including a two-sided risk-disclosure comment header on destructive changes; it is advisory only and never executed. A force-plan remediation additionally carries structured_remediation: the same decision as machine-readable fields — eligible, named blockers (parameter_sensitivity_cofired, secondary_replica_evidence), evidence numbers, and split force_sql/unforce_sql/verify_sql artifacts — so agents consume the verdict as data instead of parsing comment prose. Set include_drilldown to also return each chain's persisted evidence rows (the specific plans/queries behind the finding, capped at write time with an explicit _truncation_note; null on findings persisted before the column existed).")]
    public static async Task<string> GetAnalysisFindings(
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of finding history to retrieve. Default 24.")] int hours_back = 24,
        [Description("If true, each finding carries drill_down: the persisted evidence rows (e.g. the parameter-sensitive plans, top spill queries) behind the chain's latest occurrence. Default false - the rows can be bulky and the summary usually suffices.")] bool include_drilldown = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        /* Null for an absent anchor — see analyze_server's note. */
        var anchor = string.IsNullOrWhiteSpace(as_of) ? (DateTime?)null : windowEnd;

        try
        {
            /* #2000: the window-covering limit, not the store default 100 — occurrence stats
               computed over a silently-truncated read would lie about first_seen/occurrences.

               #2506: the window is on ANALYSIS TIME — when the scheduled pass ran — so an anchor here
               asks "what was analysis saying about this server then", which is a different question
               from "analyze that window now" (that is analyze_server with the same anchor). Both are
               worth having: this one is the historical record and cannot change, the other recomputes
               from whatever rows the store still holds. */
            var findings = await analysisService.GetRecentFindingsAsync(
                resolved.ServerId, hours_back, FindingOccurrences.WindowCoveringLimit, asOfUtc: anchor);

            if (findings.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "No findings in the requested time range. Run analyze_server to generate new findings.");
            }

            /* #2000: collapse to one entry per (story_path_hash, incident_id). Measured 27.9x
               duplication fleet-wide on a 24h read (39x worst server) with every occurrence
               re-carrying the same advice prose; severity movement survives via the occurrence
               stats. The store keeps every row — this shapes the read only. */
            var groups = FindingOccurrences.Collapse(findings);

            // Correlate-and-focus slice 1 (review §1d): "what else fired", scoped per analysis run
            // (this read can span multiple runs, unlike analyze_server's single run). Only runs
            // that produced a group REPRESENTATIVE are ever looked up below — in steady state just
            // the most recent run — and GetComposedForFinding deserializes story JSON per call, so
            // composing titles for all window-covering-limit rows would be ~100x wasted work
            // (review catch on #2001).
            var representativeRuns = groups.Select(g => g.Latest.AnalysisTime).ToHashSet();
            var coFiredByRun = new Dictionary<DateTime, List<(string, double)>>();
            foreach (var wf in findings)
            {
                if (!representativeRuns.Contains(wf.AnalysisTime))
                    continue;
                if (!coFiredByRun.TryGetValue(wf.AnalysisTime, out var list))
                    coFiredByRun[wf.AnalysisTime] = list = new List<(string, double)>();
                list.Add((FactAdvice.GetComposedForFinding(wf)?.Headline ?? wf.RootFactKey, wf.Severity));
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                finding_count = groups.Count,
                total_occurrences = findings.Count,
                // No silent caps: a read that fills the window-covering limit has had its OLDEST
                // rows dropped by the store's newest-first LIMIT, so occurrence stats may
                // under-report — say so instead of letting first_seen quietly lie.
                truncation_note = findings.Count >= FindingOccurrences.WindowCoveringLimit
                    ? $"Read hit the {FindingOccurrences.WindowCoveringLimit}-row cap; the oldest occurrences in the window were dropped, so occurrences/first_seen may under-report. Use a smaller hours_back for exact stats."
                    : null,
                findings = groups.Select(g =>
                {
                    var f = g.Latest;
                    // #2060: the drill-down now SURVIVES read-back (persisted capped as
                    // drill_down_json, the remediation_action_json pattern) and rides out
                    // behind include_drilldown; findings persisted before V52 read null.
                    // Advice prose stays composed: GetComposedForFinding reads the
                    // value-bearing advice (current MAXDOP/CTFP/etc.) frozen into
                    // StoryText at analysis time, falling back to the static block.
                    // suggested_remediation_sql STAYS omitted here: it is built live from
                    // the FULL drill-down at analysis time, and the persisted copy is
                    // capped — the copy-paste command below covers the runnable need.
                    // The FULL copy-paste command below is rendered from the PERSISTED
                    // RemediationAction instead — which DOES survive read-back — so a
                    // triaging agent gets the same runnable command a human sees on the
                    // card without re-running analyze_server.
                    var advice = FactAdvice.GetComposedForFinding(f);
                    return new
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
                        drill_down = include_drilldown ? f.DrillDown : null,
                        incident_id = f.IncidentId,
                        // #2000 occurrence stats: the collapsed timeline. severity above is the
                        // LATEST occurrence's; peak_severity is the highest any occurrence reached.
                        occurrences = g.Occurrences,
                        first_seen = g.FirstSeen.ToString("o"),
                        last_seen = g.LastSeen.ToString("o"),
                        peak_severity = Math.Round(g.PeakSeverity, 2),
                        co_fired = CoFiredSummary.OtherTitles(advice?.Headline ?? f.RootFactKey, coFiredByRun[f.AnalysisTime]),
                        // Spans the whole group: earliest analyzed-window start to latest end.
                        time_range = new
                        {
                            start = g.TimeRangeStart?.ToString("o"),
                            end = g.TimeRangeEnd?.ToString("o")
                        },
                        advice = advice is null ? null : new
                        {
                            headline = advice.Headline,
                            investigation = advice.Investigation,
                            remediation = advice.Remediation
                        },
                        // The SAME copy-paste remediation command the viewer cards render, from the
                        // persisted action via the shared renderer (all seven shapes + the two-sided
                        // risk-disclosure comment header on the destructive ones). Null when the finding
                        // has no remediable action. PRODUCE ONLY — the read-only MCP never executes it.
                        remediation_command = FactRemediation.RenderCopyPasteCommand(f.Remediation),
                        // #2138: the machine-first projection — see analyze_server's twin field.
                        structured_remediation = FactRemediation.BuildStructuredRemediation(f.Remediation)
                    };
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
        DarlingAnalysisService analysisService,
        NpgsqlDataSource postgres,
        [Description("The story_path_hash from the finding to mute.")] string story_path_hash,
        [Description("Server name. If omitted, mutes across all servers.")] string? server_name = null,
        [Description("Optional reason for muting.")] string? reason = null)
    {
        try
        {
            int? serverId = null;
            if (server_name != null)
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
                if (error != null) return error;
                serverId = resolved.ServerId;
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
/// Verbatim per-app copy of Lite's table (the Dashboard carries one too): the named tools are
/// the PRODUCT's data-tool surface — and Darling's own MCP server now hosts them itself (the
/// analysis, plan-analysis, and ~60 stored data-read tools), so these recommendations point the
/// client at tools on THIS server, exactly as documented in DarlingMcpInstructions.
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
            new("get_memory_stats", "Check buffer pool and memory pressure"),
            new("get_tempdb_trend", "Check whether tempdb I/O is driving the EX-mode waits")
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
            new("get_file_io_trend", "Track log I/O latency over time"),
            new("get_perfmon_trend", "Check Transactions/sec to see commit rate driving log flush pressure", new() { ["counter_name"] = "Transactions/sec" })
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
            new("get_blocked_process_reports", "Check if DDL operations are causing blocking"),
            new("get_running_jobs", "See whether maintenance jobs (index rebuilds, stats updates) are taking schema-modification locks")
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
        ["PARAMETER_SENSITIVITY"] =
        [
            new("get_top_queries_by_cpu", "Find the sensitive query in the plan cache and see its current cached parameters"),
            new("analyze_query_plan", "Examine the plan for the operators driving the runtime variance (seek vs scan, grant size, join type)"),
            new("get_query_trend", "Confirm the bimodal duration pattern across executions over time"),
            new("get_memory_grants", "Check whether the bad-parameter executions are also blowing up memory grants")
        ],
        ["PLAN_REGRESSION"] =
        [
            new("analyze_query_store_plan", "Compare the regressed plan against the prior plan to see what the optimizer changed"),
            new("get_query_trend", "Confirm the regression timing and that the new plan is consistently worse"),
            new("get_query_store_top", "Pull the full Query Store entry including plan_id and forced-plan history before considering a force")
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
        ["FILE_AUTOGROWTH_PERCENT"] =
        [
            new("get_database_sizes", "See per-file sizes and autogrowth settings"),
            new("get_file_io_stats", "Check per-file growth and latency")
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
        ["ANOMALY_SESSION_SPIKE"] =
        [
            new("get_session_stats", "See which application is driving the session-count spike"),
            new("get_active_queries", "Find what those sessions were doing at the spike"),
            new("get_waiting_tasks", "Check whether the new sessions are piling up on a shared wait")
        ],
        ["ANOMALY_QUERY_DURATION"] =
        [
            new("get_query_duration_trend", "Confirm the duration shift across the analysis window"),
            new("get_top_queries_by_cpu", "Find the queries whose runtime moved the average"),
            new("analyze_query_plan", "Examine the plan for the queries that slowed down")
        ],
        ["ANOMALY_MEMORY_PRESSURE"] =
        [
            new("get_memory_stats", "See current memory allocation and target vs total"),
            new("get_memory_clerks", "Find which clerks are growing"),
            new("get_memory_pressure_events", "Pull the RING_BUFFER_RESOURCE_MONITOR notifications driving the anomaly"),
            new("get_memory_grants", "Check whether query grants are competing with buffer pool")
        ],
        ["ANOMALY_BATCH_REQUESTS"] =
        [
            new("get_perfmon_trend", "Confirm the batch-rate change across the window", new() { ["counter_name"] = "Batch Requests/sec" }),
            new("get_top_queries_by_cpu", "Find which queries account for the new batch volume"),
            new("get_active_queries", "See what's actually running at the elevated rate")
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
