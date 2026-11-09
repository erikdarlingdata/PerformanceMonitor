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
/// #8381. The scan rules' row floors (#7891) apply only to a figure the plan carries, and a figure the plan
/// does not carry is not zero. Rule 34 "Bare Scan" judges an estimated plan on TableCardinality, so a plan
/// without the attribute is judged on the scan's cost share alone. Rule 11 "Scan With Predicate" judges an
/// actual plan on ActualRowsRead; when the plan has none, the table's cardinality stands in, and when it
/// carries neither there is no floor.
/// </summary>
public sealed class PlanSync8381AbsentFigureTests
{
    private static IReadOnlyList<PlanWarning> WarningsOfType(string xml, string warningType)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalysisPipeline.Run(plan, null, null, CancellationToken.None);
        var root = PlanStatements.EnumerateAll(plan).Single().RootNode!;

        var found = new List<PlanWarning>();
        Collect(root, found);
        return found;

        void Collect(PlanNode node, List<PlanWarning> into)
        {
            into.AddRange(node.Warnings.Where(w => w.WarningType == warningType));
            foreach (var child in node.Children)
                Collect(child, into);
        }
    }

    // ---- Rule 34: Bare Scan, estimated plans without TableCardinality ---------------------------

    /// <summary>An estimated plan from a build that writes no TableCardinality, whose only scan is the whole
    /// plan's cost. The plan does not say how big the table is, so the cost share alone decides.</summary>
    [Fact]
    public void BareScan_Estimated_NoTableCardinality_FullCost_Warns()
    {
        var xml = EstimatedPlan(tableCardinality: null, estimateRows: 1_000, predicate: null);
        Assert.Single(WarningsOfType(xml, "Bare Scan"));
    }

    // ---- Rule 11: Scan With Predicate, actual plans without ActualRowsRead ----------------------

    /// <summary>The issue's case: an actual plan with neither ActualRowsRead nor QueryTimeStats, whose predicate
    /// scan over a 10,000,000 row table returned 100 rows. The table's cardinality stands in for the rows read.</summary>
    [Fact]
    public void ScanWithPredicate_NoRowsRead_TenMillionRowTable_ReturnsHundred_Warns()
    {
        var xml = ActualPlanWithoutRowsRead(tableCardinality: 10_000_000, rowsReturned: 100);
        var warning = Assert.Single(WarningsOfType(xml, "Scan With Predicate"));

        Assert.Contains("reading every row", warning.Message);
    }

    /// <summary>The stand-in floor still holds: without ActualRowsRead, a 200 row table is under it.</summary>
    [Fact]
    public void ScanWithPredicate_NoRowsRead_TwoHundredRowTable_DoesNotWarn()
    {
        var xml = ActualPlanWithoutRowsRead(tableCardinality: 200, rowsReturned: 100);
        Assert.Empty(WarningsOfType(xml, "Scan With Predicate"));
    }

    /// <summary>Neither figure: the plan gives nothing to put a floor on, so the scan is reported.</summary>
    [Fact]
    public void ScanWithPredicate_NoRowsRead_NoTableCardinality_Warns()
    {
        var xml = ActualPlanWithoutRowsRead(tableCardinality: null, rowsReturned: 100);
        Assert.Single(WarningsOfType(xml, "Scan With Predicate"));
    }

    // ---- fixtures -------------------------------------------------------------------------------

    private const string Predicate = "[db].[dbo].[t].[a]=(1)";

    private const string Head =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">" +
        "<BatchSequence><Batch><Statements>";

    private const string Tail = "</Statements></Batch></BatchSequence></ShowPlanXML>";

    /// <summary>A statement whose only operator is a predicate scan of dbo.t with run-time counters that carry
    /// no ActualRowsRead, and no QueryTimeStats on the statement. The scan is the whole statement's cost.</summary>
    private static string ActualPlanWithoutRowsRead(double? tableCardinality, long rowsReturned)
    {
        var runtime =
            $"<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"{rowsReturned}\" ActualExecutions=\"1\" " +
            "ActualElapsedms=\"5\" ActualCPUms=\"5\" ActualLogicalReads=\"100\" /></RunTimeInformation>";
        var scan = Scan(estimateRows: rowsReturned, tableCardinality, scanCost: 1.0, runtime, Predicate);
        return Head + Statement(scan, statementCost: 1.0, queryTimeStats: "") + Tail;
    }

    private static string EstimatedPlan(double? tableCardinality, double estimateRows, string? predicate)
    {
        var scan = Scan(estimateRows, tableCardinality, scanCost: 1.0, runtime: "", predicate);
        return Head + Statement(scan, statementCost: 1.0, queryTimeStats: "") + Tail;
    }

    private static string Scan(double estimateRows, double? tableCardinality, double scanCost, string runtime, string? predicate) =>
        $"<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
        $"EstimateRows=\"{estimateRows}\" EstimateIO=\"{scanCost * 0.9}\" EstimateCPU=\"{scanCost * 0.1}\" AvgRowSize=\"15\" " +
        $"EstimatedTotalSubtreeCost=\"{scanCost}\" " +
        (tableCardinality.HasValue ? $"TableCardinality=\"{tableCardinality.Value}\" " : "") +
        "Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"a\" /></OutputList>" +
        runtime +
        "<IndexScan Ordered=\"false\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues><DefinedValue><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"a\" /></DefinedValue></DefinedValues>" +
        "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
        (predicate == null ? "" : $"<Predicate><ScalarOperator ScalarString=\"{predicate}\" /></Predicate>") +
        "</IndexScan></RelOp>";

    private static string Statement(string root, double statementCost, string queryTimeStats) =>
        $"<StmtSimple StatementText=\"SELECT a FROM dbo.t\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" " +
        $"StatementSubTreeCost=\"{statementCost}\" StatementEstRows=\"1\" StatementOptmLevel=\"FULL\">" +
        "<QueryPlan DegreeOfParallelism=\"1\" CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"120\">" +
        queryTimeStats + root + "</QueryPlan></StmtSimple>";
}
