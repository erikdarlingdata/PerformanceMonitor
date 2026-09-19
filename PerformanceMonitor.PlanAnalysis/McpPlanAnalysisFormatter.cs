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
    /// How many operators <c>top_operators</c> carries per statement. A fixed cut rather than a caller knob
    /// (the plan tools take no <c>limit</c>, and #3653 A15/A16 is about STATING the cut, not adding a knob):
    /// ten operators is enough to hold the expensive spine of any plan an LLM can reason about in one
    /// payload, and the rest of the plan is still reachable through the warnings (every node's warnings are
    /// in <c>warnings</c>, uncut) and the viewer. Published on every statement as <c>operators_cap</c> beside
    /// <c>operators_returned</c> / <c>total_operators</c> / <c>truncated</c>, so the cut is never silent.
    /// </summary>
    public const int TopOperatorCap = 10;

    /// <summary>
    /// The two <c>top_operators</c> ranking bases, published per statement as <c>operators_ranked_by</c>
    /// (#3653 A15/A16). A plan with runtime statistics ranks by measured elapsed time; an estimated plan has
    /// no measurement and ranks by the optimizer's cost SHARE. The switch was silent before: the same key
    /// carried a measurement in one payload and an estimate in the next, and only <c>has_actual_stats</c>
    /// hinted at which — a caller comparing two payloads' top operators was comparing a stopwatch to a
    /// guess without being told.
    /// </summary>
    public const string RankedByActualElapsed = "actual_elapsed_ms";

    /// <inheritdoc cref="RankedByActualElapsed"/>
    public const string RankedByEstimatedCostPercent = "cost_percent (optimizer estimate)";

    /// <summary>
    /// What <c>missing_indexes[].impact</c> IS, published as <c>impact_basis</c> beside it (#3653 A15/A16): the
    /// showplan <c>MissingIndexGroup/@Impact</c>, the optimizer's estimate of the percent by which THIS
    /// statement's cost would fall if the index existed. It is not a share of server load, not a share of
    /// the batch, and not measured; two statements' impacts do not add. The payload used to carry the bare
    /// number under <c>impact</c> as though it were self-explanatory.
    /// </summary>
    public const string MissingIndexImpactBasis = "estimated percent reduction of this statement's cost (showplan MissingIndexGroup/@Impact; optimizer estimate, statement-scoped, not additive)";

    /// <summary>
    /// The sentence every <c>missing_indexes[]</c> row carries in place of the CREATE INDEX DDL it used to
    /// paste (#3653 A15/A16). The DMV's suggestion is a HINT the optimizer emits for one statement's shape:
    /// it knows nothing of the table's existing indexes (it will suggest a near-duplicate of one), of the
    /// other statements that touch the table, of write cost, or of key order beyond equality-before-inequality.
    /// A paste-ready statement in an agent's payload reads as a design; the column lists are the evidence and
    /// stay as data, and the design is the operator's. The plan viewer still renders the DMV text for a human
    /// reading a single plan; the MCP surface is the one an agent may act on unread, so it is the one that
    /// stops handing out DDL.
    /// </summary>
    public const string MissingIndexNote = "The optimizer's missing-index suggestion for this ONE statement: a hint, not a design. It ignores the table's existing indexes (it often near-duplicates one), every other statement that touches the table, and write cost. Use the column lists as evidence against the table's actual indexes and workload before creating anything.";

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
                /* #3653 A15/A16: the cut and the basis are published, not silent — see TopOperatorCap and
                   RankedByActualElapsed. truncated is OBSERVED against the whole population (every node is in
                   hand), never inferred from the page size. */
                var topOps = (hasActuals
                        ? allNodes.OrderByDescending(n => n.ActualElapsedMs)
                        : allNodes.OrderByDescending(n => n.CostPercent))
                    .Take(TopOperatorCap)
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
                    /* #3653 A15/A16: impact is labelled for what it is, and the paste-ready CREATE INDEX is
                       gone from this surface — the column lists ARE the data; see MissingIndexImpactBasis and
                       MissingIndexNote for why. */
                    missing_indexes = s.MissingIndexes.Select(idx => new
                    {
                        table = $"{idx.Schema}.{idx.Table}",
                        database = idx.Database,
                        impact = idx.Impact,
                        impact_basis = MissingIndexImpactBasis,
                        equality_columns = idx.EqualityColumns,
                        inequality_columns = idx.InequalityColumns,
                        include_columns = idx.IncludeColumns,
                        note = MissingIndexNote,
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
                    operators_ranked_by = hasActuals ? RankedByActualElapsed : RankedByEstimatedCostPercent,
                    operators_cap = TopOperatorCap,
                    operators_returned = Math.Min(allNodes.Count, TopOperatorCap),
                    total_operators = allNodes.Count,
                    truncated = allNodes.Count > TopOperatorCap,
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
