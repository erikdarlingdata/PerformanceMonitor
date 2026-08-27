using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Analysis;

public partial class SqlServerDrillDownCollector
{
    /// <summary>
    /// For findings that have query hashes (bad actors), fetch the execution plan
    /// live from SQL Server via IPlanFetcher, then run PlanAnalyzer to surface
    /// warnings and missing indexes. No plan storage needed -- fetch on demand
    /// only for queries that make it into high-impact findings.
    /// </summary>
    private async Task CollectPlanAnalysis(AnalysisFinding finding, AnalysisContext context)
    {
        if (finding.DrillDown == null || _planFetcher == null) return;

        // Only analyze plans for bad actor findings (1 plan each).
        // Skip top_cpu_queries (5 plans would be too heavy).
        if (!finding.RootFactKey.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase)) return;

        var queryHash = finding.RootFactKey.Replace("BAD_ACTOR_", "");
        if (string.IsNullOrEmpty(queryHash)) return;

        // Look up plan_handle from collect.query_stats for this query_hash
        string? planHandle = null;
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1 CONVERT(VARCHAR(130), plan_handle, 1) AS plan_handle
FROM collect.query_stats
WHERE query_hash = CONVERT(BINARY(8), @queryHash, 1)
AND   plan_handle IS NOT NULL
ORDER BY collection_time DESC;";

            cmd.Parameters.Add(new SqlParameter("@queryHash", queryHash));

            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync() && !reader.IsDBNull(0))
                planHandle = reader.GetString(0);
        }
        catch { return; }

        if (string.IsNullOrEmpty(planHandle)) return;

        // Fetch plan XML live from SQL Server
        var planXml = await _planFetcher.FetchPlanXmlAsync(context.ServerId, planHandle, context.CancellationToken);
        if (string.IsNullOrEmpty(planXml)) return;

        try
        {
            var plan = ShowPlanParser.Parse(planXml);
            PlanAnalyzer.Analyze(plan);

            var allWarnings = plan.Batches
                .SelectMany(b => b.Statements)
                .Where(s => s.RootNode != null)
                .SelectMany(s =>
                {
                    var nodeWarnings = new List<PlanNode>();
                    CollectPlanNodes(s.RootNode!, nodeWarnings);
                    return s.PlanWarnings
                        .Concat(nodeWarnings.SelectMany(n => n.Warnings));
                })
                .ToList();

            var missingIndexes = plan.AllMissingIndexes;

            if (allWarnings.Count == 0 && missingIndexes.Count == 0) return;

            finding.DrillDown["plan_analysis"] = new
            {
                query_hash = queryHash,
                warning_count = allWarnings.Count,
                critical_count = allWarnings.Count(w => w.Severity == PlanWarningSeverity.Critical),
                warnings = allWarnings
                    .OrderByDescending(w => w.Severity)
                    .Take(10)
                    .Select(w => new
                    {
                        severity = w.Severity.ToString(),
                        type = w.WarningType,
                        message = McpHelpers.Truncate(w.Message, 300)
                    }),
                missing_indexes = missingIndexes.Take(5).Select(idx => new
                {
                    table = $"{idx.Schema}.{idx.Table}",
                    impact = idx.Impact,
                    create_statement = idx.CreateStatement
                })
            };
        }
        catch
        {
            // Plan parsing can fail on malformed XML -- skip silently
        }
    }

    /// <summary>
    /// WS4: re-parses the top collected query plans (same top-10-by-cost set the fact collector
    /// summarized) and attaches the specific missing indexes / plan warnings to a MISSING_INDEX or
    /// PLAN_WARNING finding's drill-down. The fact carries only counts (Fact.Metadata is numeric),
    /// so the strings — CREATE statements, warning messages — are rendered here. Best-effort: a
    /// read/parse failure leaves the finding with no plan-advisory detail rather than aborting.
    /// </summary>
    private async Task CollectPlanAdvisoryDetail(AnalysisFinding finding, AnalysisContext context, HashSet<string> pathKeys)
    {
        try
        {
            var planXmls = new List<string>();

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using var cmd = connection.CreateCommand();
                cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP (10)
    plan_xml = CAST(DECOMPRESS(qs.query_plan_text) AS nvarchar(max))
FROM collect.query_stats AS qs
WHERE qs.collection_time >= @startTime
AND   qs.collection_time <= @endTime
AND   qs.query_plan_text IS NOT NULL
ORDER BY
    qs.total_worker_time DESC;";
                cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
                cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    if (!reader.IsDBNull(0))
                        planXmls.Add(reader.GetString(0));
                }
            }

            if (planXmls.Count == 0)
                return;

            var details = PlanAdvisoryAggregator.Extract(planXmls);

            if (pathKeys.Contains("MISSING_INDEX") && details.MissingIndexes.Count > 0)
            {
                finding.DrillDown!["missing_indexes"] = details.MissingIndexes
                    .OrderByDescending(i => i.Impact)
                    .Take(5)
                    .Select(i => new
                    {
                        table = $"{i.Schema}.{i.Table}",
                        impact = Math.Round(i.Impact, 1),
                        create_statement = i.CreateStatement
                    })
                    .ToList();
            }

            if (pathKeys.Contains("PLAN_WARNING") && details.Warnings.Count > 0)
            {
                finding.DrillDown!["plan_warnings"] = details.Warnings
                    .OrderByDescending(w => w.Severity)
                    .Take(5)
                    .Select(w => new
                    {
                        type = w.WarningType,
                        severity = w.Severity.ToString(),
                        message = McpHelpers.Truncate(w.Message, 300)
                    })
                    .ToList();
            }
        }
        catch
        {
            // Plan read/parse can fail on malformed XML -- skip, the detail is best-effort.
        }
    }
}
