/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7890. Rule 24 "Top Above Scan" is for a Top that makes the engine scan and sort a whole table to
/// return a few rows. A plain Top over an ordered scan, fed through streaming operators only, is the shape
/// <c>SELECT TOP (n) ... ORDER BY &lt;index key&gt;</c> compiles to: the Top stops the scan after n rows,
/// so there is nothing to index away and the rule stays quiet. A Top over an unordered scan, and a Top N
/// Sort, which reads the whole scan before it can return a row, keep the warning.
/// </summary>
public sealed class PlanSync7890TopAboveScanTests
{
    private static IReadOnlyList<PlanWarning> TopAboveScanWarnings(string xml)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalysisPipeline.Run(plan, null, null, CancellationToken.None);
        var root = PlanStatements.EnumerateAll(plan).Single().RootNode!;

        var found = new List<PlanWarning>();
        Collect(root, found);
        return found;

        static void Collect(PlanNode node, List<PlanWarning> into)
        {
            into.AddRange(node.Warnings.Where(w => w.WarningType == "Top Above Scan"));
            foreach (var child in node.Children)
                Collect(child, into);
        }
    }

    [Fact]
    public void PlainTop_OverOrderedScan_DoesNotWarn()
    {
        Assert.Empty(TopAboveScanWarnings(TopOverScan(ordered: true, topNSort: false)));
    }

    /// <summary>A Compute Scalar between the Top and the scan streams rows, so the Top still stops the scan.</summary>
    [Fact]
    public void PlainTop_OverOrderedScan_ThroughComputeScalar_DoesNotWarn()
    {
        Assert.Empty(TopAboveScanWarnings(TopOverScan(ordered: true, topNSort: false, computeScalarBetween: true)));
    }

    [Fact]
    public void PlainTop_OverUnorderedScan_Warns()
    {
        var warning = Assert.Single(TopAboveScanWarnings(TopOverScan(ordered: false, topNSort: false)));

        Assert.Equal(24, warning.RuleNumber);
        Assert.Equal(PlanWarningSeverity.Warning, warning.Severity);
    }

    /// <summary>The Sort is the blocking operator: a Top N Sort reads every row of the scan before it
    /// returns one, whether or not the scan is ordered.</summary>
    [Fact]
    public void TopNSort_OverOrderedScan_StillWarns()
    {
        var warning = Assert.Single(TopAboveScanWarnings(TopOverScan(ordered: true, topNSort: true)));

        Assert.Equal(24, warning.RuleNumber);
        Assert.Contains("Top N Sort", warning.Message);
    }

    [Fact]
    public void TopNSort_OverUnorderedScan_Warns()
    {
        Assert.Single(TopAboveScanWarnings(TopOverScan(ordered: false, topNSort: true)));
    }

    /// <summary>The APPLY idiom for the latest row per key: a Top over an ordered scan on the inner side of
    /// a Nested Loops. It is the best plan for the query, so it earns no finding, let alone a Critical one.</summary>
    [Fact]
    public void ApplyTop_OverOrderedScan_OnNestedLoopsInner_DoesNotWarn()
    {
        Assert.Empty(TopAboveScanWarnings(ApplyTopOverScan(ordered: true)));
    }

    [Fact]
    public void ApplyTop_OverUnorderedScan_OnNestedLoopsInner_IsCritical()
    {
        var warning = Assert.Single(TopAboveScanWarnings(ApplyTopOverScan(ordered: false)));

        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);
        Assert.Contains("inner side of Nested Loops", warning.Message);
    }

    private const string Head =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">" +
        "<BatchSequence><Batch><Statements>";

    private const string Tail = "</Statements></Batch></BatchSequence></ShowPlanXML>";

    /// <summary>A clustered index scan on dbo.t that read one row (an actual plan). Ordered as asked, backward
    /// so it reads as the ORDER BY id DESC shape.</summary>
    private static string Scan(int nodeId, bool ordered) =>
        $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
        "EstimateRows=\"1\" EstimatedRowsRead=\"1\" EstimateRowsWithoutRowGoal=\"1000000\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"15\" " +
        "EstimatedTotalSubtreeCost=\"0.0033\" TableCardinality=\"1000000\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"id\" /></OutputList>" +
        "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"1\" ActualRowsRead=\"1\" ActualExecutions=\"1\" " +
        "ActualElapsedms=\"0\" ActualCPUms=\"0\" ActualLogicalReads=\"3\" /></RunTimeInformation>" +
        $"<IndexScan Ordered=\"{(ordered ? "true" : "false")}\" {(ordered ? "ScanDirection=\"BACKWARD\" " : "")}ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues><DefinedValue><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"id\" /></DefinedValue></DefinedValues>" +
        "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
        "</IndexScan></RelOp>";

    private static string ComputeScalar(int nodeId, string child) =>
        $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"Compute Scalar\" LogicalOp=\"Compute Scalar\" EstimateRows=\"1\" EstimateIO=\"0\" EstimateCPU=\"0.0000001\" AvgRowSize=\"15\" " +
        "EstimatedTotalSubtreeCost=\"0.0033\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"id\" /></OutputList>" +
        "<ComputeScalar><DefinedValues />" + child + "</ComputeScalar></RelOp>";

    private static string Top(int nodeId, string child) =>
        $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"Top\" LogicalOp=\"Top\" EstimateRows=\"1\" EstimateIO=\"0\" EstimateCPU=\"0.0000001\" AvgRowSize=\"15\" " +
        "EstimatedTotalSubtreeCost=\"0.0033\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"id\" /></OutputList>" +
        "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"1\" ActualExecutions=\"1\" ActualElapsedms=\"0\" ActualCPUms=\"0\" /></RunTimeInformation>" +
        "<Top RowCount=\"0\" IsPercent=\"0\" WithTies=\"0\">" +
        "<TopExpression><ScalarOperator ScalarString=\"(1)\"><Const ConstValue=\"(1)\" /></ScalarOperator></TopExpression>" +
        child + "</Top></RelOp>";

    private static string TopNSort(int nodeId, string child) =>
        $"<RelOp NodeId=\"{nodeId}\" PhysicalOp=\"Sort\" LogicalOp=\"Top N Sort\" EstimateRows=\"1\" EstimateIO=\"0.011\" EstimateCPU=\"0.0001\" AvgRowSize=\"15\" " +
        "EstimatedTotalSubtreeCost=\"0.015\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"id\" /></OutputList>" +
        "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"1\" ActualExecutions=\"1\" ActualElapsedms=\"0\" ActualCPUms=\"0\" /></RunTimeInformation>" +
        "<TopSort Distinct=\"0\" Rows=\"1\">" +
        "<OrderBy><OrderByColumn Ascending=\"false\"><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"id\" /></OrderByColumn></OrderBy>" +
        child + "</TopSort></RelOp>";

    private static string Statement(string text, string root) =>
        $"<StmtSimple StatementText=\"{text}\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" " +
        "StatementSubTreeCost=\"0.0033\" StatementEstRows=\"1\" StatementOptmLevel=\"FULL\">" +
        "<QueryPlan DegreeOfParallelism=\"1\" CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"120\">" +
        "<QueryTimeStats CpuTime=\"0\" ElapsedTime=\"0\" />" +
        root + "</QueryPlan></StmtSimple>";

    private static string TopOverScan(bool ordered, bool topNSort, bool computeScalarBetween = false)
    {
        var scan = Scan(2, ordered);
        var below = computeScalarBetween ? ComputeScalar(1, scan) : scan;
        var root = topNSort ? TopNSort(0, below) : Top(0, below);
        return Head + Statement("SELECT TOP (1) id FROM dbo.t ORDER BY id DESC", root) + Tail;
    }

    /// <summary>CROSS APPLY (SELECT TOP (1) ... ORDER BY ...): a Nested Loops whose inner side is a Top over
    /// a scan of dbo.t, driven by an outer scan of dbo.k.</summary>
    private static string ApplyTopOverScan(bool ordered)
    {
        var outer =
            "<RelOp NodeId=\"1\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" EstimateRows=\"10\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"11\" " +
            "EstimatedTotalSubtreeCost=\"0.0033\" TableCardinality=\"10\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
            "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[k]\" Column=\"id\" /></OutputList>" +
            "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"10\" ActualRowsRead=\"10\" ActualExecutions=\"1\" ActualElapsedms=\"0\" ActualCPUms=\"0\" ActualLogicalReads=\"2\" /></RunTimeInformation>" +
            "<IndexScan Ordered=\"false\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
            "<DefinedValues><DefinedValue><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[k]\" Column=\"id\" /></DefinedValue></DefinedValues>" +
            "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[k]\" Index=\"[PK_k]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
            "</IndexScan></RelOp>";
        var inner = Top(2, Scan(3, ordered));
        var root =
            "<RelOp NodeId=\"0\" PhysicalOp=\"Nested Loops\" LogicalOp=\"Inner Join\" EstimateRows=\"10\" EstimateIO=\"0\" EstimateCPU=\"0.00004\" AvgRowSize=\"15\" " +
            "EstimatedTotalSubtreeCost=\"0.04\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
            "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"id\" /></OutputList>" +
            "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"10\" ActualExecutions=\"1\" ActualElapsedms=\"0\" ActualCPUms=\"0\" /></RunTimeInformation>" +
            "<NestedLoops Optimized=\"0\"><OuterReferences><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[k]\" Column=\"id\" /></OuterReferences>" +
            outer + inner + "</NestedLoops></RelOp>";
        return Head + Statement("SELECT t.id FROM dbo.k CROSS APPLY (SELECT TOP (1) t.id FROM dbo.t WHERE t.k_id = k.id ORDER BY t.id DESC) AS t", root) + Tail;
    }
}
