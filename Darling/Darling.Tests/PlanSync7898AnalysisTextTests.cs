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
/// #7898. Four places in the plan analysis said something the plan itself contradicts. Rule 13 keys on
/// <c>GetRangeThroughConvert</c> and <c>GetRangeWithMismatchedTypes</c>, which exist because the optimizer
/// built a seek range through the conversion, so the message says that rather than "preventing index seeks".
/// Rule 20 leaves a parameter that OPTIMIZE FOR UNKNOWN left without a compiled value to rule 27, and the
/// parameters card names OPTION(RECOMPILE) only when the statement carries it. Rule 23 dates the 100 row
/// guess to the 2014 cardinality estimator. The parser leaves <c>EstimatedRowsRead</c> at 0 when the plan
/// does not carry it, so the viewer's "Est. Rows to Read" line is omitted instead of repeating the no-row-goal
/// estimate.
/// </summary>
public sealed class PlanSync7898AnalysisTextTests
{
    private static ParsedPlan Analyze(string xml)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalysisPipeline.Run(plan, null, null, CancellationToken.None);
        return plan;
    }

    private static IEnumerable<PlanNode> Flatten(PlanNode node)
    {
        yield return node;
        foreach (var child in node.Children)
            foreach (var n in Flatten(child))
                yield return n;
    }

    private static IReadOnlyList<PlanWarning> NodeWarnings(ParsedPlan plan, string warningType) =>
        Flatten(PlanStatements.EnumerateAll(plan).Single().RootNode!)
            .SelectMany(n => n.Warnings)
            .Where(w => w.WarningType == warningType)
            .ToList();

    private static IReadOnlyList<PlanWarning> StatementWarnings(ParsedPlan plan, string warningType) =>
        PlanStatements.EnumerateAll(plan).Single().PlanWarnings.Where(w => w.WarningType == warningType).ToList();

    // ---- Rule 13: Data Type Mismatch -----------------------------------------------------------

    [Fact]
    public void Rule13_GetRangeThroughConvert_SaysTheSeekRangeWasBuilt_NotThatSeeksArePrevented()
    {
        var warning = Assert.Single(NodeWarnings(Analyze(ComputeScalarOverScan("GetRangeThroughConvert(1,2,3)")), "Data Type Mismatch"));

        Assert.Equal(13, warning.RuleNumber);
        Assert.Contains("built a dynamic seek range", warning.Message);
        Assert.DoesNotContain("preventing index seeks", warning.Message);
        Assert.DoesNotContain("converting every row", warning.Message);
    }

    [Fact]
    public void Rule13_GetRangeWithMismatchedTypes_SaysTheSeekRangeWasBuilt_NotThatEveryRowIsConverted()
    {
        var warning = Assert.Single(NodeWarnings(Analyze(ComputeScalarOverScan("GetRangeWithMismatchedTypes(1,2,3)")), "Data Type Mismatch"));

        Assert.Contains("built a dynamic seek range", warning.Message);
        Assert.DoesNotContain("preventing index seeks", warning.Message);
        Assert.DoesNotContain("converting every row", warning.Message);
    }

    // ---- Rule 20: Local Variables, against rule 27 ---------------------------------------------

    [Fact]
    public void Rule20_OptimizeForUnknown_IsNotReportedAsLocalVariables()
    {
        var plan = Analyze(ParameterizedStatement("SELECT a FROM dbo.t WHERE a = @p OPTION (OPTIMIZE FOR UNKNOWN)"));

        Assert.Empty(StatementWarnings(plan, "Local Variables"));
        Assert.Single(StatementWarnings(plan, "Optimize For Unknown"));
    }

    [Fact]
    public void Rule20_UnsniffedParameterWithoutHints_IsStillReportedAsLocalVariables()
    {
        var plan = Analyze(ParameterizedStatement("SELECT a FROM dbo.t WHERE a = @p"));

        var warning = Assert.Single(StatementWarnings(plan, "Local Variables"));
        Assert.Contains("@p", warning.Message);
        Assert.Empty(StatementWarnings(plan, "Optimize For Unknown"));
    }

    // ---- The parameters card ----------------------------------------------------------------------

    [Fact]
    public void ParameterCard_AllCompiledNull_WithoutRecompileOrHint_SaysTheValuesWereNotCaptured()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = "5" },
        };
        var text = "SELECT * FROM t WHERE x = @p1";

        var annotation = Assert.Single(ParameterCard.Annotations(parameters, text, text, []));

        Assert.DoesNotContain("OPTION(RECOMPILE)", annotation.Text);
        Assert.Contains("not captured", annotation.Text);
        Assert.Equal(ParameterCardAnnotationTone.Warning, annotation.Tone);
    }

    [Fact]
    public void ParameterCard_AllCompiledNull_WithRecompile_SaysRecompile()
    {
        var parameters = new List<PlanParameter>
        {
            new() { Name = "@p1", DataType = "int", CompiledValue = null, RuntimeValue = "5" },
        };
        var text = "SELECT * FROM t WHERE x = @p1 OPTION (RECOMPILE)";

        var annotation = Assert.Single(ParameterCard.Annotations(parameters, text, text, []));

        Assert.Contains("OPTION(RECOMPILE)", annotation.Text);
    }

    // ---- Rule 23: Table-Valued Function ----------------------------------------------------------

    [Fact]
    public void Rule23_DatesTheHundredRowGuessToSqlServer2014()
    {
        var warning = Assert.Single(NodeWarnings(Analyze(UserFunctionXml), "Table-Valued Function"));

        Assert.Equal(23, warning.RuleNumber);
        Assert.Contains("2014", warning.Message);
        Assert.DoesNotContain("pre-2017", warning.Message);
        Assert.DoesNotContain("2017+", warning.Message);
    }

    // ---- The parser's EstimatedRowsRead -----------------------------------------------------------

    [Fact]
    public void Parser_AbsentEstimatedRowsRead_StaysZero_BesideTheNoRowGoalEstimate()
    {
        var scan = ScanNode(ShowPlanParser.Parse(RowGoalScan(estimatedRowsRead: null)));

        Assert.Equal(1_000_000, scan.EstimateRowsWithoutRowGoal);
        Assert.Equal(0, scan.EstimatedRowsRead);
    }

    [Fact]
    public void Parser_PresentEstimatedRowsRead_IsKept()
    {
        var scan = ScanNode(ShowPlanParser.Parse(RowGoalScan(estimatedRowsRead: "5")));

        Assert.Equal(5, scan.EstimatedRowsRead);
    }

    private static PlanNode ScanNode(ParsedPlan plan) =>
        Flatten(PlanStatements.EnumerateAll(plan).Single().RootNode!).Single(n => n.PhysicalOp == "Clustered Index Scan");

    // ---- fixtures -------------------------------------------------------------------------------

    private const string Head =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">" +
        "<BatchSequence><Batch><Statements>";

    private const string Tail = "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private const string TableScan =
        "<RelOp NodeId=\"1\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"1\" EstimateIO=\"0\" EstimateCPU=\"0\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"1\" TableCardinality=\"0\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList/><TableScan Storage=\"RowStore\"><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Storage=\"RowStore\"/></TableScan></RelOp>";

    /// <summary>The Compute Scalar the optimizer puts above the data access when it builds a dynamic seek
    /// range through a conversion; <paramref name="definedValue"/> is the range function it defines.</summary>
    private static string ComputeScalarOverScan(string definedValue) =>
        Head +
        "<StmtSimple StatementText=\"SELECT a FROM dbo.t WHERE b = @p\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"1\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Compute Scalar\" LogicalOp=\"Compute Scalar\" EstimateRows=\"1\" EstimateIO=\"0\" EstimateCPU=\"0\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"1\" TableCardinality=\"0\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList/><ComputeScalar><DefinedValues><DefinedValue><ColumnReference Column=\"Expr1001\"/>" +
        $"<ScalarOperator ScalarString=\"{definedValue}\"/></DefinedValue></DefinedValues>" +
        TableScan +
        "</ComputeScalar></RelOp></QueryPlan></StmtSimple>" +
        Tail;

    /// <summary>A statement costed above rule 20's floor whose one parameter has a runtime value and no compiled
    /// value, which is what both a local variable and OPTIMIZE FOR UNKNOWN leave in the plan.</summary>
    private static string ParameterizedStatement(string statementText) =>
        Head +
        $"<StmtSimple StatementText=\"{statementText}\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"5\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">" +
        TableScan +
        "<ParameterList><ColumnReference Column=\"@p\" ParameterDataType=\"int\" ParameterRuntimeValue=\"(1)\"/></ParameterList>" +
        "</QueryPlan></StmtSimple>" +
        Tail;

    private const string UserFunctionXml =
        Head +
        "<StmtSimple StatementText=\"SELECT a FROM dbo.MyTvf(1)\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"1\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Table-valued function\" LogicalOp=\"Table-valued function\" EstimateRows=\"100\" EstimateIO=\"0\" EstimateCPU=\"0\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"1\" TableCardinality=\"0\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList/><TableValuedFunction><DefinedValues/><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[MyTvf]\"/></TableValuedFunction>" +
        "</RelOp></QueryPlan></StmtSimple>" +
        Tail;

    /// <summary>A scan under a row goal: a 1,000,000 row estimate without the goal, 1 row with it, and
    /// <c>EstimatedRowsRead</c> present or absent as asked.</summary>
    private static string RowGoalScan(string? estimatedRowsRead) =>
        Head +
        "<StmtSimple StatementText=\"SELECT TOP (1) a FROM dbo.t\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"0.01\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" EstimateRows=\"1\" " +
        (estimatedRowsRead == null ? "" : $"EstimatedRowsRead=\"{estimatedRowsRead}\" ") +
        "EstimateRowsWithoutRowGoal=\"1000000\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"15\" " +
        "EstimatedTotalSubtreeCost=\"0.0033\" TableCardinality=\"1000000\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"a\" /></OutputList>" +
        "<IndexScan Ordered=\"true\" ScanDirection=\"FORWARD\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues/><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
        "</IndexScan></RelOp></QueryPlan></StmtSimple>" +
        Tail;
}
