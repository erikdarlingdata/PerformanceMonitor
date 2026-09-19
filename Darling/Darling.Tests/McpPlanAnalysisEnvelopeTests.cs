/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A15/A16 — the shared plan-analysis envelope (<see cref="McpPlanAnalysisFormatter"/>, behind every
/// <c>analyze_*_plan</c> tool on both SKUs and the frozen Dashboard twin) states its cuts and labels its numbers.
///
/// <para>Two lies it told. <c>top_operators</c> was a silent <c>Take(10)</c> whose ranking basis switched between a
/// measured <c>actual_elapsed_ms</c> and the optimizer's <c>cost_percent</c> estimate with nothing in the payload
/// saying which — a caller comparing two plans' top operators could be comparing a stopwatch to a guess. And
/// <c>missing_indexes[].impact</c> was a bare number (the showplan <c>MissingIndexGroup/@Impact</c>, an estimated
/// statement-scoped percent) beside a paste-ready <c>CREATE INDEX</c> — the optimizer's one-statement hint dressed as
/// a design, on the one surface an agent may act on unread.</para>
///
/// <para>Synthetic plans rather than fixtures from a server: the properties under test are counts and labels over a
/// known operator population, so the plan is built to have exactly the shape each assertion needs (twelve RelOps
/// nested one under the other, one missing-index group) and the expected numbers are facts of the test.</para>
/// </summary>
public sealed class McpPlanAnalysisEnvelopeTests
{
    /// <summary>The parser wraps the root RelOp in a synthetic statement node (NodeId -1), so a plan with N
    /// RelOps has N + 1 operators in the population <c>top_operators</c> ranks. Pinned as a fact so the
    /// assertions below read as arithmetic rather than magic.</summary>
    private const int SyntheticStatementNodes = 1;

    [Fact]
    public void TopOperators_PublishesTheCut_TheCount_ObservedTruncation_AndTheEstimateBasis()
    {
        const int relOps = 12;
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(relOps, withMissingIndex: true), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());

        Assert.Equal(McpPlanAnalysisFormatter.TopOperatorCap, stmt.GetProperty("operators_cap").GetInt32());
        Assert.Equal(relOps + SyntheticStatementNodes, stmt.GetProperty("total_operators").GetInt32());
        Assert.Equal(McpPlanAnalysisFormatter.TopOperatorCap, stmt.GetProperty("operators_returned").GetInt32());
        Assert.Equal(McpPlanAnalysisFormatter.TopOperatorCap, stmt.GetProperty("top_operators").GetArrayLength());
        Assert.True(stmt.GetProperty("truncated").GetBoolean());

        /* No RunTimeInformation anywhere → an estimated plan → ranked by the optimizer's cost share, and the
           payload SAYS so rather than leaving has_actual_stats as the only hint. */
        Assert.False(stmt.GetProperty("has_actual_stats").GetBoolean());
        Assert.Equal(McpPlanAnalysisFormatter.RankedByEstimatedCostPercent, stmt.GetProperty("operators_ranked_by").GetString());
        Assert.Contains("estimate", McpPlanAnalysisFormatter.RankedByEstimatedCostPercent, StringComparison.Ordinal);

        /* The ranking IS by cost_percent, descending — the label names the order the rows are actually in. */
        var costs = stmt.GetProperty("top_operators").EnumerateArray().Select(o => o.GetProperty("cost_percent").GetInt32()).ToList();
        Assert.Equal(costs.OrderByDescending(c => c).ToList(), costs);
    }

    [Fact]
    public void TopOperators_UnderTheCap_ReturnsEveryOperator_AndIsNotTruncated()
    {
        const int relOps = 3;
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(relOps, withMissingIndex: false), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());

        Assert.Equal(relOps + SyntheticStatementNodes, stmt.GetProperty("total_operators").GetInt32());
        Assert.Equal(relOps + SyntheticStatementNodes, stmt.GetProperty("operators_returned").GetInt32());
        Assert.Equal(relOps + SyntheticStatementNodes, stmt.GetProperty("top_operators").GetArrayLength());
        Assert.False(stmt.GetProperty("truncated").GetBoolean());
        Assert.Equal(0, stmt.GetProperty("missing_indexes").GetArrayLength());
        Assert.Equal(0, doc.RootElement.GetProperty("total_missing_indexes").GetInt32());
    }

    [Fact]
    public void MissingIndexes_CarryColumnListsAndALabelledImpact_AndNoCreateIndexText()
    {
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(2, withMissingIndex: true), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());
        var idx = Assert.Single(stmt.GetProperty("missing_indexes").EnumerateArray());

        Assert.Equal("dbo.Posts", idx.GetProperty("table").GetString());
        Assert.Equal(87.5, idx.GetProperty("impact").GetDouble());
        Assert.Equal(McpPlanAnalysisFormatter.MissingIndexImpactBasis, idx.GetProperty("impact_basis").GetString());
        Assert.Contains("statement", McpPlanAnalysisFormatter.MissingIndexImpactBasis, StringComparison.Ordinal);
        Assert.Contains("estimate", McpPlanAnalysisFormatter.MissingIndexImpactBasis, StringComparison.Ordinal);

        Assert.Equal(new[] { "OwnerUserId" }, idx.GetProperty("equality_columns").EnumerateArray().Select(c => c.GetString()).ToArray());
        Assert.Equal(new[] { "CreationDate" }, idx.GetProperty("inequality_columns").EnumerateArray().Select(c => c.GetString()).ToArray());
        Assert.Equal(new[] { "Score" }, idx.GetProperty("include_columns").EnumerateArray().Select(c => c.GetString()).ToArray());

        Assert.Equal(McpPlanAnalysisFormatter.MissingIndexNote, idx.GetProperty("note").GetString());
        Assert.Contains("a hint, not a design", McpPlanAnalysisFormatter.MissingIndexNote, StringComparison.Ordinal);

        /* The DDL is gone from the wire — no key carries it, and no value spells it. */
        Assert.False(idx.TryGetProperty("create_statement", out _));
        Assert.DoesNotContain("CREATE NONCLUSTERED INDEX", idx.GetRawText(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, doc.RootElement.GetProperty("total_missing_indexes").GetInt32());
    }

    /* ---------------- fixture ---------------- */

    /// <summary>An estimated plan (no RunTimeInformation) of <paramref name="relOps"/> operators nested one under the
    /// next, each with a distinct subtree cost so cost_percent ranks them unambiguously, and optionally one
    /// missing-index group on dbo.Posts with all three column usages.</summary>
    private static string EstimatedPlan(int relOps, bool withMissingIndex)
    {
        var sb = new StringBuilder();
        sb.Append("<?xml version=\"1.0\" encoding=\"utf-16\"?>");
        sb.Append("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">");
        sb.Append("<BatchSequence><Batch><Statements>");
        sb.Append("<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"100\" StatementEstRows=\"10\">");
        sb.Append("<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">");
        if (withMissingIndex)
        {
            sb.Append("<MissingIndexes><MissingIndexGroup Impact=\"87.5\">");
            sb.Append("<MissingIndex Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Posts]\">");
            sb.Append("<ColumnGroup Usage=\"EQUALITY\"><Column Name=\"[OwnerUserId]\" ColumnId=\"3\" /></ColumnGroup>");
            sb.Append("<ColumnGroup Usage=\"INEQUALITY\"><Column Name=\"[CreationDate]\" ColumnId=\"5\" /></ColumnGroup>");
            sb.Append("<ColumnGroup Usage=\"INCLUDE\"><Column Name=\"[Score]\" ColumnId=\"9\" /></ColumnGroup>");
            sb.Append("</MissingIndex></MissingIndexGroup></MissingIndexes>");
        }

        /* Subtree costs descend from the root so every operator's own cost (subtree minus children) is distinct. */
        for (var i = 0; i < relOps; i++)
        {
            var subtree = (relOps - i) * 10.0;
            var op = i == relOps - 1 ? "Clustered Index Scan" : "Nested Loops";
            sb.Append($"<RelOp NodeId=\"{i}\" PhysicalOp=\"{op}\" LogicalOp=\"{(i == relOps - 1 ? "Clustered Index Scan" : "Inner Join")}\" EstimateRows=\"{10 + i}\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"{subtree}\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">");
            sb.Append("<OutputList />");
            sb.Append(i == relOps - 1 ? "<IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" : "<NestedLoops Optimized=\"0\">");
            if (i == relOps - 1)
            {
                sb.Append("<DefinedValues /><Object Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Posts]\" Index=\"[PK_Posts]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />");
            }
        }

        for (var i = relOps - 1; i >= 0; i--)
        {
            sb.Append(i == relOps - 1 ? "</IndexScan>" : "</NestedLoops>");
            sb.Append("</RelOp>");
        }

        sb.Append("</QueryPlan></StmtSimple>");
        sb.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");
        return sb.ToString();
    }
}
