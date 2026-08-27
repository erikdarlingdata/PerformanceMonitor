using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Enriches findings with drill-down data from DuckDB.
/// Runs after graph traversal, only for findings above severity threshold.
/// Each drill-down query is limited to top N results with truncated text.
///
/// This makes analyze_server self-sufficient — instead of returning a list
/// of "next tools to call," findings include the actual supporting data.
/// </summary>
public partial class DrillDownCollector
{
    private readonly DuckDbInitializer _duckDb;
    private readonly IPlanFetcher? _planFetcher;
    private const int TextLimit = 500;

    public DrillDownCollector(DuckDbInitializer duckDb, IPlanFetcher? planFetcher = null)
    {
        _duckDb = duckDb;
        _planFetcher = planFetcher;
    }

    /// <summary>
    /// Enriches each finding's DrillDown dictionary based on its story path.
    /// </summary>
    public async Task EnrichFindingsAsync(List<AnalysisFinding> findings, AnalysisContext context)
    {
        foreach (var finding in findings)
        {
            /* #2443: between findings is the natural abandon point — the per-finding catch below
               deliberately does NOT swallow an abandonment, so this throw (and any residue from a
               drill-down mid-read) unwinds the pass to the service's single Information line. */
            context.CancellationToken.ThrowIfCancellationRequested();

            try
            {
                finding.DrillDown = new Dictionary<string, object>();
                var pathKeys = finding.StoryPath.Split(" → ", StringSplitOptions.RemoveEmptyEntries).ToHashSet();

                /* D7: the config drill-down is a single cheap config-table read and is
                   required to build config/RCSI/db-config advice, which legitimately scores
                   below 0.5 (RCSI-off base severity is 0.3). Collect it regardless of the
                   0.5 display gate. (Lite is advise/copy-paste only — no Apply — but still
                   needs the config drill-down to render the recommendation.) */
                if (pathKeys.Contains("DB_CONFIG"))
                    await CollectConfigIssues(finding, context);

                /* WS3: the percent-autogrowth drill-down is a single cheap config-table read
                   and is required to render the copy-paste MODIFY FILE fix for a
                   FILE_AUTOGROWTH_PERCENT finding, which scores 0.3 (advisory). Collect it
                   regardless of the 0.5 display gate, like the config drill-down above. (Lite
                   is advise/copy-paste only — the fix lives in this drill-down, not an Apply.) */
                if (pathKeys.Contains("FILE_AUTOGROWTH_PERCENT"))
                    await CollectAutogrowthPercentFiles(finding, context);

                /* WS4: re-parse the top collected query plans to render the specific missing
                   indexes / warnings for a MISSING_INDEX or PLAN_WARNING finding, which scores 0.4
                   (advisory). Run regardless of the 0.5 display gate, like the config drill-downs
                   above — the fact carries only counts, so the strings live here. */
                if (pathKeys.Contains("MISSING_INDEX") || pathKeys.Contains("PLAN_WARNING"))
                    await CollectPlanAdvisoryDetail(finding, context, pathKeys);

                // Below the 0.5 display gate, only the cheap config drill-down above runs;
                // the expensive collectors (plan fetches, multi-row reads) are skipped.
                if (finding.Severity < 0.5)
                {
                    if (finding.DrillDown.Count == 0)
                        finding.DrillDown = null;
                    continue;
                }

                if (pathKeys.Contains("DEADLOCKS"))
                    await CollectTopDeadlocks(finding, context);

                if (pathKeys.Contains("BLOCKING_EVENTS"))
                    await CollectTopBlockingChains(finding, context);

                if (pathKeys.Contains("BLOCKING_CHAIN"))
                    await CollectReconstructedBlockingChains(finding, context);

                if (pathKeys.Contains("CPU_SPIKE"))
                    await CollectQueriesAtSpike(finding, context);

                if (pathKeys.Contains("CPU_SQL_PERCENT") || pathKeys.Contains("CPU_SPIKE"))
                    await CollectTopCpuQueries(finding, context);

                if (pathKeys.Contains("QUERY_SPILLS"))
                    await CollectTopSpillingQueries(finding, context);

                if (pathKeys.Contains("IO_READ_LATENCY_MS") || pathKeys.Contains("IO_WRITE_LATENCY_MS"))
                    await CollectFileLatencyBreakdown(finding, context);

                if (pathKeys.Contains("LCK") || pathKeys.Contains("LCK_M_S") || pathKeys.Contains("LCK_M_IS"))
                    await CollectLockModeBreakdown(finding, context);

                if (pathKeys.Contains("TEMPDB_USAGE"))
                    await CollectTempDbBreakdown(finding, context);

                if (pathKeys.Contains("MEMORY_GRANT_PENDING"))
                    await CollectPendingGrants(finding, context);

                if (pathKeys.Any(k => k.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase)))
                    await CollectBadActorDetail(finding, context);

                if (pathKeys.Contains("PARAMETER_SENSITIVITY"))
                    await CollectParameterSensitiveQueries(finding, context);

                if (pathKeys.Contains("PLAN_REGRESSION"))
                    await CollectRegressedQueries(finding, context);

                // Plan analysis: for findings with top queries, analyze their cached plans
                await CollectPlanAnalysis(finding, context);

                // Remove empty drill-down dictionaries
                if (finding.DrillDown.Count == 0)
                    finding.DrillDown = null;
            }
            catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
            {
                AppLogger.Error("DrillDownCollector",
                    $"Drill-down failed for {finding.StoryPath}: {ex.GetType().Name}: {ex.Message}");
                // Don't null out — keep whatever was collected before the error
            }
        }
    }

    private static void CollectPlanNodes(PlanNode node, List<PlanNode> nodes)
    {
        nodes.Add(node);
        foreach (var child in node.Children)
            CollectPlanNodes(child, nodes);
    }

}
