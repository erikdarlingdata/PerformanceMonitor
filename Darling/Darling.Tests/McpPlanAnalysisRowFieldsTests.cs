/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8377: every <c>top_operators</c> entry the shared plan-analysis envelope (<see cref="McpPlanAnalysisFormatter"/>,
/// behind every <c>analyze_*plan*</c> tool on both SKUs) emits carries the two figures a reader needs to reconcile
/// <c>estimated_rows</c> with <c>actual_rows</c>. <c>estimated_rows</c> is SQL Server's estimate for ONE execution;
/// <c>actual_rows</c> is the total across EVERY execution. On the inner side of a Nested Loops join an operator runs
/// once per outer row, so dividing the two reads an exact estimate as a misestimate by a factor of the execution
/// count. <c>expected_rows</c> is the execution-aware figure the plan viewer's node label, edge colour, minimap and
/// analyzer rules 5, 16 and 26 already share through <see cref="RowEstimateHelper.GetExpectedRows"/> (#4627, #4698),
/// and <c>actual_executions</c> is the count it was multiplied by; both are null, not 0, on an estimated plan.
/// </summary>
public sealed class McpPlanAnalysisRowFieldsTests
{
    /// <summary>
    /// Node 4 of <c>key_lookup_plan.sqlplan</c>, the Key Lookup the viewer's row label was written for: inner side of
    /// a Nested Loops join, EstimateRows 0.00964372, 117 executions, 1 actual row. The viewer reads it as
    /// <c>1 of 1.128 (89%)</c>; the payload's <c>expected_rows</c> is the same 1.12831524, beside
    /// <c>actual_executions</c> 117, and it equals what <see cref="RowEstimateHelper.GetExpectedRows"/> gives the
    /// viewer for the same node, so the two surfaces cannot disagree about whether the operator is misestimated.
    /// </summary>
    [Fact]
    public void KeyLookupFixture_OnTheNestedLoopsInnerSide_CarriesTheExecutionAwareEstimateAndTheExecutionCount()
    {
        var xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", "key_lookup_plan.sqlplan"));
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(xml, "s", "xml", null));
        var lookup = doc.RootElement.GetProperty("statements").EnumerateArray()
            .SelectMany(s => s.GetProperty("top_operators").EnumerateArray())
            .Single(o => o.GetProperty("node_id").GetInt32() == 4);

        Assert.Equal(0.00964372, lookup.GetProperty("estimated_rows").GetDouble(), precision: 8);
        Assert.Equal(1L, lookup.GetProperty("actual_rows").GetInt64());

        Assert.True(lookup.TryGetProperty("actual_executions", out var executions), "the top_operators entry carries no actual_executions");
        Assert.Equal(117L, executions.GetInt64());

        Assert.True(lookup.TryGetProperty("expected_rows", out var expected), "the top_operators entry carries no expected_rows");
        Assert.Equal(1.12831524, expected.GetDouble(), precision: 8);

        /* The same number the viewer's label, edge colour and rules read for this node. */
        var node = ShowPlanParser.Parse(xml).Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .SelectMany(s => Flatten(s.RootNode!))
            .Single(n => n.NodeId == 4);
        Assert.True(RowEstimateHelper.IsInnerSideOfNestedLoops(node));
        Assert.Equal(RowEstimateHelper.GetExpectedRows(node), expected.GetDouble(), precision: 8);
    }

    /// <summary>
    /// A scan under a Gather Streams, not on any inner side: its ActualExecutions is one per thread (a thread count,
    /// DOP 4 here), not a loop count, so <c>expected_rows</c> is the plain estimate and NOT the estimate times four.
    /// <c>actual_executions</c> still reports the 4, so a reader can see why the two figures are equal.
    /// </summary>
    [Fact]
    public void ParallelScan_NotOnAnInnerSide_HasExpectedRowsEqualToTheEstimate_AndReportsItsExecutions()
    {
        const int dop = 4;
        const double estimate = 1000;
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(ParallelScanPlan(estimate, rowsPerThread: 250, dop), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());
        Assert.True(stmt.GetProperty("has_actual_stats").GetBoolean());
        Assert.True(stmt.GetProperty("parallel").GetBoolean());

        var scan = Assert.Single(stmt.GetProperty("top_operators").EnumerateArray(), o => o.GetProperty("node_id").GetInt32() == 1);
        Assert.Equal("Clustered Index Scan", scan.GetProperty("physical_op").GetString());
        Assert.Equal(estimate, scan.GetProperty("estimated_rows").GetDouble());
        Assert.Equal((long)estimate, scan.GetProperty("actual_rows").GetInt64());

        Assert.True(scan.TryGetProperty("actual_executions", out var executions), "the top_operators entry carries no actual_executions");
        Assert.Equal(dop, executions.GetInt64());

        Assert.True(scan.TryGetProperty("expected_rows", out var expected), "the top_operators entry carries no expected_rows");
        Assert.Equal(scan.GetProperty("estimated_rows").GetDouble(), expected.GetDouble());
    }

    /// <summary>An estimated plan has no execution counts, so both figures are null (null, not 0, the #3541 A12 rule for
    /// every runtime field) on every row, while <c>estimated_rows</c> stays the per-execution estimate it always was.</summary>
    [Fact]
    public void EstimatedPlan_CarriesNullExpectedRowsAndNullActualExecutions_OnEveryOperator()
    {
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(EstimatedPlan(), "s", "xml", null));
        var stmt = Assert.Single(doc.RootElement.GetProperty("statements").EnumerateArray());
        Assert.False(stmt.GetProperty("has_actual_stats").GetBoolean());

        var top = stmt.GetProperty("top_operators").EnumerateArray().ToList();
        Assert.NotEmpty(top);
        Assert.All(top, o =>
        {
            Assert.NotEqual(JsonValueKind.Null, o.GetProperty("estimated_rows").ValueKind);
            Assert.Equal(JsonValueKind.Null, o.GetProperty("actual_rows").ValueKind);

            Assert.True(o.TryGetProperty("expected_rows", out var expected), "the top_operators entry carries no expected_rows");
            Assert.Equal(JsonValueKind.Null, expected.ValueKind);

            Assert.True(o.TryGetProperty("actual_executions", out var executions), "the top_operators entry carries no actual_executions");
            Assert.Equal(JsonValueKind.Null, executions.ValueKind);
        });
    }

    /* ---------------- fixtures ---------------- */

    private static IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
        {
            foreach (var n in Flatten(child))
            {
                yield return n;
            }
        }
    }

    private const string PlanOpen =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"100\" StatementEstRows=\"10\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">";

    private const string PlanClose = "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    private const string ScanBody =
        "<IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues /><Object Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Posts]\" Index=\"[PK_Posts]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />";

    private static string RelOpOpen(int nodeId, string physicalOp, string logicalOp, double estimateRows, double subtreeCost, bool parallel) =>
        $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"{physicalOp}\" LogicalOp=\"{logicalOp}\" EstimateRows=\"{estimateRows}\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"{subtreeCost}\" Parallel=\"{(parallel ? 1 : 0)}\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\"><OutputList />";

    /// <summary>One thread record per DOP, each reporting one execution of <paramref name="rowsPerThread"/> rows, the
    /// way showplan reports a parallel scan: the parser sums both across threads.</summary>
    private static string ThreadRecords(int dop, long rowsPerThread)
    {
        var sb = new StringBuilder("<RunTimeInformation>");
        for (var t = 0; t < dop; t++)
        {
            sb.Append($"<RunTimeCountersPerThread Thread=\"{t}\" ActualRows=\"{rowsPerThread}\" ActualExecutions=\"1\" ActualExecutionMode=\"Row\" ActualElapsedms=\"{10 + t}\" ActualCPUms=\"{10 + t}\" />");
        }

        return sb.Append("</RunTimeInformation>").ToString();
    }

    /// <summary>A Gather Streams exchange over a parallel Clustered Index Scan, both with runtime statistics.</summary>
    private static string ParallelScanPlan(double estimate, long rowsPerThread, int dop)
    {
        var sb = new StringBuilder();
        sb.Append(PlanOpen);
        sb.Append(RelOpOpen(0, "Parallelism", "Gather Streams", estimate, subtreeCost: 30.0, parallel: true)).Append(ThreadRecords(1, rowsPerThread * dop)).Append("<Parallelism>");
        sb.Append(RelOpOpen(1, "Clustered Index Scan", "Clustered Index Scan", estimate, subtreeCost: 10.0, parallel: true)).Append(ThreadRecords(dop, rowsPerThread)).Append(ScanBody).Append("</IndexScan></RelOp>");
        sb.Append("</Parallelism></RelOp>");
        sb.Append(PlanClose);
        return sb.ToString();
    }

    /// <summary>A Nested Loops join over two seeks with no RunTimeInformation anywhere: an estimated plan whose inner
    /// side would be the multiplied case if it had executions to multiply by.</summary>
    private static string EstimatedPlan()
    {
        var sb = new StringBuilder();
        sb.Append(PlanOpen);
        sb.Append(RelOpOpen(0, "Nested Loops", "Inner Join", estimateRows: 117, subtreeCost: 30.0, parallel: false)).Append("<NestedLoops Optimized=\"0\">");
        sb.Append(RelOpOpen(1, "Index Seek", "Index Seek", estimateRows: 117, subtreeCost: 10.0, parallel: false)).Append(ScanBody).Append("</IndexScan></RelOp>");
        sb.Append(RelOpOpen(2, "Clustered Index Seek", "Clustered Index Seek", estimateRows: 0.00964372, subtreeCost: 10.0, parallel: false)).Append(ScanBody).Append("</IndexScan></RelOp>");
        sb.Append("</NestedLoops></RelOp>");
        sb.Append(PlanClose);
        return sb.ToString();
    }
}
