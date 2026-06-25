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
using System.Text.Json;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// Parses a query plan, runs the analyzer, and serializes a structured JSON result for the MCP
/// plan-analysis tools. Shared by Lite and Dashboard — both apps' McpPlanTools call this so the
/// (previously byte-identical) projection stays in one place. The per-app tool wrappers that fetch
/// the plan XML (from SQL Server vs DuckDB) remain app-specific.
/// </summary>
public static class McpPlanAnalysisFormatter
{
    /// <summary>
    /// Parses plan XML, runs the analyzer, and builds a structured JSON result.
    /// </summary>
    public static string BuildAnalysisResult(string xml, string? serverName, string source, string? identifier)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);

        var statements = plan.Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .Select(s =>
            {
                var allNodes = new List<PlanNode>();
                CollectNodes(s.RootNode!, allNodes);

                var nodeWarnings = allNodes
                    .SelectMany(n => n.Warnings)
                    .ToList();
                var stmtWarnings = s.PlanWarnings;
                var allWarnings = stmtWarnings.Concat(nodeWarnings).ToList();

                var hasActuals = allNodes.Any(n => n.HasActualStats);
                var topOps = (hasActuals
                        ? allNodes.OrderByDescending(n => n.ActualElapsedMs)
                        : allNodes.OrderByDescending(n => n.CostPercent))
                    .Take(10)
                    .Select(n => new
                    {
                        node_id = n.NodeId,
                        physical_op = n.PhysicalOp,
                        logical_op = n.LogicalOp,
                        cost_percent = n.CostPercent,
                        estimated_rows = n.EstimateRows,
                        actual_rows = n.HasActualStats ? n.ActualRows : (long?)null,
                        actual_elapsed_ms = n.HasActualStats ? n.ActualElapsedMs : (long?)null,
                        actual_cpu_ms = n.HasActualStats ? n.ActualCPUMs : (long?)null,
                        logical_reads = n.HasActualStats ? n.ActualLogicalReads : (long?)null,
                        object_name = n.ObjectName,
                        index_name = n.IndexName,
                        predicate = McpHelpers.Truncate(n.Predicate, 500),
                        seek_predicates = McpHelpers.Truncate(n.SeekPredicates, 500),
                        warning_count = n.Warnings.Count
                    });

                return new
                {
                    statement_text = McpHelpers.Truncate(s.StatementText, 2000),
                    statement_type = s.StatementType,
                    estimated_cost = Math.Round(s.StatementSubTreeCost, 4),
                    dop = s.DegreeOfParallelism,
                    serial_reason = s.NonParallelPlanReason,
                    compile_cpu_ms = s.CompileCPUMs,
                    compile_memory_kb = s.CompileMemoryKB,
                    cardinality_model = s.CardinalityEstimationModelVersion,
                    query_hash = s.QueryHash,
                    query_plan_hash = s.QueryPlanHash,
                    has_actual_stats = hasActuals,
                    warnings = allWarnings.Select(w => new
                    {
                        severity = w.Severity.ToString(),
                        type = w.WarningType,
                        message = w.Message
                    }),
                    warning_count = allWarnings.Count,
                    critical_count = allWarnings.Count(w => w.Severity == PlanWarningSeverity.Critical),
                    missing_indexes = s.MissingIndexes.Select(idx => new
                    {
                        table = $"{idx.Schema}.{idx.Table}",
                        database = idx.Database,
                        impact = idx.Impact,
                        equality_columns = idx.EqualityColumns,
                        inequality_columns = idx.InequalityColumns,
                        include_columns = idx.IncludeColumns,
                        create_statement = idx.CreateStatement
                    }),
                    parameters = s.Parameters.Select(p => new
                    {
                        name = p.Name,
                        data_type = p.DataType,
                        compiled_value = p.CompiledValue,
                        runtime_value = p.RuntimeValue,
                        sniffing_mismatch = p.CompiledValue != null && p.RuntimeValue != null
                            && p.CompiledValue != p.RuntimeValue
                    }),
                    memory_grant = s.MemoryGrant == null ? null : new
                    {
                        requested_kb = s.MemoryGrant.RequestedMemoryKB,
                        granted_kb = s.MemoryGrant.GrantedMemoryKB,
                        max_used_kb = s.MemoryGrant.MaxUsedMemoryKB,
                        desired_kb = s.MemoryGrant.DesiredMemoryKB,
                        grant_wait_ms = s.MemoryGrant.GrantWaitTimeMs,
                        feedback = s.MemoryGrant.IsMemoryGrantFeedbackAdjusted
                    },
                    top_operators = topOps
                };
            })
            .ToList();

        var totalWarnings = statements.Sum(s => s.warning_count);
        var totalCritical = statements.Sum(s => s.critical_count);
        var totalMissing = statements.Sum(s => s.missing_indexes.Count());

        var result = new
        {
            server = serverName,
            source,
            identifier,
            statement_count = statements.Count,
            total_warnings = totalWarnings,
            total_critical = totalCritical,
            total_missing_indexes = totalMissing,
            statements
        };

        return JsonSerializer.Serialize(result, McpHelpers.JsonOptions);
    }

    private static void CollectNodes(PlanNode node, List<PlanNode> nodes)
    {
        nodes.Add(node);
        foreach (var child in node.Children)
            CollectNodes(child, nodes);
    }
}
