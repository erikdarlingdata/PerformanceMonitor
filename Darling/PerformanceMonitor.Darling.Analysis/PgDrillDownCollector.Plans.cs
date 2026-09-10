/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgDrillDownCollector
{
    public const string PlanHandleLookupSql = @"
SELECT plan_handle
FROM v_query_stats
WHERE server_id = $1
AND   query_hash = $2
AND   plan_handle IS NOT NULL AND plan_handle != ''
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// For findings that have query hashes (bad actors), fetch the execution plan
    /// live from SQL Server via IPlanFetcher, then run PlanAnalyzer to surface
    /// warnings and missing indexes. No plan storage needed — fetch on demand
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

        // Look up plan_handle from the store for this query_hash
        string? planHandle = null;
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PlanHandleLookupSql, connection) { CommandTimeout = DrillDownCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(queryHash);

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (await reader.ReadAsync(context.CancellationToken) && !reader.IsDBNull(0))
                planHandle = reader.GetString(0);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* No plan_handle for this hash — the fetch below has nothing to ask for. An abandonment
               is NOT swallowed here (#2443). */
            return;
        }

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
            // Plan parsing can fail on malformed XML — skip silently
        }
    }

    public const string PlanAdvisoryXmlSql = @"
SELECT query_plan_xml
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_plan_xml IS NOT NULL
ORDER BY delta_worker_time DESC
LIMIT 10";

    /// <summary>
    /// WS4: re-parses the top collected query plans (the same top-10-by-cost set the fact collector
    /// summarized) and attaches the specific missing indexes / plan warnings to a MISSING_INDEX or
    /// PLAN_WARNING finding's drill-down. The fact carries only counts (Fact.Metadata is numeric),
    /// so the strings — CREATE statements, warning messages — are rendered here. Mirrors the
    /// Lite/Dashboard drill-down collectors. Best-effort: a read/parse failure leaves no detail.
    /// </summary>
    private async Task CollectPlanAdvisoryDetail(AnalysisFinding finding, AnalysisContext context, HashSet<string> pathKeys)
    {
        try
        {
            var planXmls = new List<string>();

            /* PG port: Lite scopes the connection in a block to release its DuckDB read lock
               before the CPU-only parse; the scoping is kept so the connection closes before
               the parse, even though PG holds no lock. */
            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var cmd = new NpgsqlCommand(PlanAdvisoryXmlSql, connection) { CommandTimeout = DrillDownCommandTimeoutSeconds };
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            // Plan read/parse can fail on malformed XML — skip, the detail is best-effort.
            // An abandonment is NOT swallowed here (#2443).
        }
    }
}
