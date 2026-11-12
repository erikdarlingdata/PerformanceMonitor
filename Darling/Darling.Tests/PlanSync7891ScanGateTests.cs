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
/// #7891. Rule 34 "Bare Scan" and rule 11 "Scan With Predicate" are about reads an index could spare, so
/// both are gated on what the scan cost. In an actual plan a scan must have read at least 10,000 rows and
/// taken at least 10 percent of the statement's elapsed time as its own time; rule 11 also needs the
/// predicate to have thrown away more than half of the rows read, and takes its Critical level from the
/// elapsed share, not the optimizer's cost share. An estimated plan is judged on the table's cardinality
/// and the cost share, as before.
/// </summary>
public sealed class PlanSync7891ScanGateTests
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

    // ---- Rule 34: Bare Scan, actual plans -------------------------------------------------------

    /// <summary>The issue's case: a 200 row lookup table read in 1 ms of a 500 ms statement.</summary>
    [Fact]
    public void BareScan_TwoHundredRows_OneMsOfFiveHundred_DoesNotWarn()
    {
        var xml = ActualPlan(rowsRead: 200, rowsReturned: 200, scanMs: 1, statementMs: 500, predicate: null);
        Assert.Empty(WarningsOfType(xml, "Bare Scan"));
    }

    /// <summary>The rows floor on its own: a small table that happens to be most of a short statement.</summary>
    [Fact]
    public void BareScan_TwoHundredRows_SixtyPercentOfStatement_DoesNotWarn()
    {
        var xml = ActualPlan(rowsRead: 200, rowsReturned: 200, scanMs: 6, statementMs: 10, predicate: null);
        Assert.Empty(WarningsOfType(xml, "Bare Scan"));
    }

    /// <summary>The time share on its own: a large table whose scan is 1 percent of the statement.</summary>
    [Fact]
    public void BareScan_MillionRows_OnePercentOfStatement_DoesNotWarn()
    {
        var xml = ActualPlan(rowsRead: 1_000_000, rowsReturned: 1_000_000, scanMs: 20, statementMs: 2_000, predicate: null);
        Assert.Empty(WarningsOfType(xml, "Bare Scan"));
    }

    [Fact]
    public void BareScan_MillionRows_SixtyPercentOfStatement_Warns()
    {
        var xml = ActualPlan(rowsRead: 1_000_000, rowsReturned: 1_000_000, scanMs: 600, statementMs: 1_000, predicate: null);
        var warning = Assert.Single(WarningsOfType(xml, "Bare Scan"));

        Assert.Equal(34, warning.RuleNumber);
        Assert.Contains("Consider a nonclustered index", warning.Message);
    }

    // ---- Rule 34: Bare Scan, estimated plans ----------------------------------------------------

    [Fact]
    public void BareScan_Estimated_TwoHundredRowTable_FullCost_DoesNotWarn()
    {
        var xml = EstimatedPlan(tableCardinality: 200, scanCost: 1.0, statementCost: 1.0, predicate: null);
        Assert.Empty(WarningsOfType(xml, "Bare Scan"));
    }

    [Fact]
    public void BareScan_Estimated_MillionRowTable_FullCost_Warns()
    {
        var xml = EstimatedPlan(tableCardinality: 1_000_000, scanCost: 1.0, statementCost: 1.0, predicate: null);
        Assert.Single(WarningsOfType(xml, "Bare Scan"));
    }

    // ---- Rule 11: Scan With Predicate, actual plans ---------------------------------------------

    /// <summary>The issue's case: 1,000 rows read, 900 returned. The predicate removed almost nothing.</summary>
    [Fact]
    public void ScanWithPredicate_ReadThousandReturnedNineHundred_DoesNotWarn()
    {
        var xml = ActualPlan(rowsRead: 1_000, rowsReturned: 900, scanMs: 950, statementMs: 1_000, predicate: Predicate, scanCost: 0.95);
        Assert.Empty(WarningsOfType(xml, "Scan With Predicate"));
    }

    /// <summary>The selectivity gate on its own: a large, slow scan that kept half of what it read.</summary>
    [Fact]
    public void ScanWithPredicate_ReturnsHalfOfMillionRead_DoesNotWarn()
    {
        var xml = ActualPlan(rowsRead: 1_000_000, rowsReturned: 500_000, scanMs: 950, statementMs: 1_000, predicate: Predicate, scanCost: 0.95);
        Assert.Empty(WarningsOfType(xml, "Scan With Predicate"));
    }

    /// <summary>The time share on its own: a selective scan of a large table that is 1 percent of the statement.</summary>
    [Fact]
    public void ScanWithPredicate_LowSelectivity_OnePercentOfStatement_DoesNotWarn()
    {
        var xml = ActualPlan(rowsRead: 1_000_000, rowsReturned: 10, scanMs: 20, statementMs: 2_000, predicate: Predicate, scanCost: 0.95);
        Assert.Empty(WarningsOfType(xml, "Scan With Predicate"));
    }

    /// <summary>The optimizer put 95 percent of the cost on the scan; the clock put 15 percent of the time
    /// there. With actuals in hand the clock sets the level, and the text gives the rows read and returned.</summary>
    [Fact]
    public void ScanWithPredicate_LowSelectivity_HighCostShare_LowTimeShare_IsWarning_NamesRows()
    {
        var xml = ActualPlan(rowsRead: 1_000_000, rowsReturned: 10, scanMs: 150, statementMs: 1_000, predicate: Predicate, scanCost: 0.95);
        var warning = Assert.Single(WarningsOfType(xml, "Scan With Predicate"));

        Assert.Equal(11, warning.RuleNumber);
        Assert.Equal(PlanWarningSeverity.Warning, warning.Severity);
        Assert.Contains("read 1,000,000 rows to return 10", warning.Message);
    }

    [Fact]
    public void ScanWithPredicate_LowSelectivity_NinetyFivePercentOfStatement_IsCritical()
    {
        var xml = ActualPlan(rowsRead: 1_000_000, rowsReturned: 10, scanMs: 950, statementMs: 1_000, predicate: Predicate, scanCost: 0.5);
        var warning = Assert.Single(WarningsOfType(xml, "Scan With Predicate"));

        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);
        Assert.Contains("read 1,000,000 rows to return 10", warning.Message);
    }

    // ---- Rule 11: Scan With Predicate, estimated plans ------------------------------------------

    /// <summary>An estimated plan has no clock, so the cost share still sets the level.</summary>
    [Fact]
    public void ScanWithPredicate_Estimated_NinetyFivePercentOfCost_IsCritical()
    {
        var xml = EstimatedPlan(tableCardinality: 1_000_000, scanCost: 0.95, statementCost: 1.0, predicate: Predicate);
        var warning = Assert.Single(WarningsOfType(xml, "Scan With Predicate"));

        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);
        Assert.Contains("reading every row", warning.Message);
    }

    // ---- fixtures -------------------------------------------------------------------------------

    private const string Predicate = "[db].[dbo].[t].[a]=(1)";

    private const string Head =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">" +
        "<BatchSequence><Batch><Statements>";

    private const string Tail = "</Statements></Batch></BatchSequence></ShowPlanXML>";

    /// <summary>A statement whose only operator is a clustered index scan of dbo.t that outputs one column, with
    /// run-time counters. <paramref name="scanCost"/> against a statement cost of 1.0 sets the cost share.</summary>
    private static string ActualPlan(long rowsRead, long rowsReturned, long scanMs, long statementMs, string? predicate, double scanCost = 1.0)
    {
        var runtime =
            $"<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"{rowsReturned}\" ActualRowsRead=\"{rowsRead}\" ActualExecutions=\"1\" " +
            $"ActualElapsedms=\"{scanMs}\" ActualCPUms=\"{scanMs}\" ActualLogicalReads=\"{rowsRead / 50 + 1}\" /></RunTimeInformation>";
        var scan = Scan(rowsReturned, tableCardinality: rowsRead, scanCost, runtime, predicate);
        return Head + Statement(scan, statementCost: 1.0, $"<QueryTimeStats CpuTime=\"{statementMs}\" ElapsedTime=\"{statementMs}\" />") + Tail;
    }

    private static string EstimatedPlan(double tableCardinality, double scanCost, double statementCost, string? predicate)
    {
        var scan = Scan(estimateRows: predicate == null ? tableCardinality : tableCardinality / 100, tableCardinality, scanCost, runtime: "", predicate);
        return Head + Statement(scan, statementCost, queryTimeStats: "") + Tail;
    }

    private static string Scan(double estimateRows, double tableCardinality, double scanCost, string runtime, string? predicate) =>
        $"<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
        $"EstimateRows=\"{estimateRows}\" EstimateIO=\"{scanCost * 0.9}\" EstimateCPU=\"{scanCost * 0.1}\" AvgRowSize=\"15\" " +
        $"EstimatedTotalSubtreeCost=\"{scanCost}\" TableCardinality=\"{tableCardinality}\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
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
