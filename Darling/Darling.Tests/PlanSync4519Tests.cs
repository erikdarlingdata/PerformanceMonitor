/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4519 — the non-SARGable predicate check knew a column only by its dotted, bracketed name
/// (<c>[t].[col]</c>) and had no idea which table a name belonged to. Two shapes went wrong:
///
/// <para>(a) An unaliased table variable column renders as a bare name with no dotted qualifier at
/// all — the same shape a parameter or an expression uses — so a function or conversion on it was
/// never flagged.</para>
///
/// <para>(b) A Nested Loops join passes a value from its outer input into the inner scan as an
/// outer reference. A function on that outer reference wraps a value from the OUTER input, not the
/// scanned table's own column, and costs the scan nothing — but the old check counted every
/// column-shaped name as the scan's own, so it flagged this too.</para>
///
/// <para>Each fact goes through the real call path — <see cref="ShowPlanParser.Parse"/> on a
/// synthetic plan, then <see cref="PlanAnalyzer.Analyze"/> — rather than calling a helper directly,
/// so the pins are proof of the actual wiring: the scan's alias, table, table-variable flag and
/// outer references, threaded from the parsed tree into the predicate check.</para>
/// </summary>
public sealed class PlanSync4519Tests
{
    private const string Header =
        """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.539" Build="16.0.1000.6"><BatchSequence><Batch><Statements>
        """;

    private const string Footer = "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static bool HasNonSargableWarning(string planXml)
    {
        var plan = ShowPlanParser.Parse(planXml);
        PlanAnalyzer.Analyze(plan);
        return plan.AllWarnings.Any(w => w.WarningType == "Non-SARGable Predicate");
    }

    // ---------------------------------------------------------------
    // (a) A table variable scan with a bare, unqualified column in its predicate
    // ---------------------------------------------------------------

    /// <summary>
    /// <c>SELECT * FROM @t WHERE upper(col) = @p</c> — the table variable has no alias, so its own
    /// column renders bare (<c>[col]</c>), with no dotted qualifier. Old behavior: not flagged,
    /// since a bare name never matched <c>ColumnReferenceRegex</c>.
    /// </summary>
    [Fact]
    public void UnaliasedTableVariableBareColumn_IsFlagged_EndToEnd()
    {
        const string planXml =
            Header +
            """
            <StmtSimple StatementText="SELECT 1 FROM @t WHERE upper(col) = @p" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="1">
            <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="64">
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="100" EstimateIO="0.1" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
            <OutputList/>
            <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
            <DefinedValues/>
            <Object Table="[@t]" Storage="RowStore"/>
            <Predicate><ScalarOperator ScalarString="upper([col])=[@p]"/></Predicate>
            </TableScan>
            </RelOp>
            </QueryPlan></StmtSimple>
            """ +
            Footer;

        Assert.True(HasNonSargableWarning(planXml));
    }

    // ---------------------------------------------------------------
    // (b) A Nested Loops inner scan whose predicate wraps an outer reference, not its own column
    // ---------------------------------------------------------------

    /// <summary>
    /// The inner scan's predicate is <c>[t].[a]=upper([o].[x])</c>, where <c>[o].[x]</c> is an outer
    /// reference the Nested Loops join passes in from its outer (first) input, not a column of
    /// <c>[t]</c> itself. Old behavior: flagged, since any dotted name counted as a column
    /// regardless of which table it actually named.
    /// </summary>
    [Fact]
    public void OuterReferenceFunctionOnOuterSide_NotFlagged_EndToEnd()
    {
        const string planXml =
            Header +
            """
            <StmtSimple StatementText="SELECT 1 FROM dbo.o CROSS APPLY (SELECT 1 FROM dbo.t WHERE t.a = upper(o.x)) AS q" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="1">
            <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="64">
            <RelOp NodeId="0" PhysicalOp="Nested Loops" LogicalOp="Inner Join" EstimateRows="3" EstimateIO="0" EstimateCPU="1e-05" AvgRowSize="15" EstimatedTotalSubtreeCost="1" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
            <OutputList/>
            <NestedLoops Optimized="0">
            <OuterReferences><ColumnReference Table="[o]" Column="x"></ColumnReference></OuterReferences>
            <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="3" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="11" EstimatedTotalSubtreeCost="0.003" TableCardinality="3" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
            <OutputList/>
            <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
            <DefinedValues/>
            <Object Schema="[dbo]" Table="[o]" Alias="[o]" IndexKind="Heap" Storage="RowStore"/>
            </TableScan>
            </RelOp>
            <RelOp NodeId="2" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="11" EstimatedTotalSubtreeCost="0.003" TableCardinality="2" Parallel="0" EstimateRebinds="0" EstimateRewinds="2" EstimatedExecutionMode="Row">
            <OutputList/>
            <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
            <DefinedValues/>
            <Object Schema="[dbo]" Table="[t]" Alias="[t]" IndexKind="Heap" Storage="RowStore"/>
            <Predicate><ScalarOperator ScalarString="[t].[a]=upper([o].[x])"/></Predicate>
            </TableScan>
            </RelOp>
            </NestedLoops>
            </RelOp>
            </QueryPlan></StmtSimple>
            """ +
            Footer;

        Assert.False(HasNonSargableWarning(planXml));
    }

    // ---------------------------------------------------------------
    // (c) A #temp table scan whose full tempdb name is cleaned before the owner comparison
    // ---------------------------------------------------------------

    /// <summary>
    /// The predicate names the temp table by its full tempdb name (padded with underscores, then a
    /// hex suffix), while the scan's own <c>ObjectName</c> is already cleaned to <c>#t</c> by the
    /// parser. Without cleaning the predicate's owner the same way, the two never match and a real
    /// column-side function on the temp table's own column would be missed.
    /// </summary>
    [Fact]
    public void TempTableFullTempdbName_OwnerComparisonMatches_EndToEnd()
    {
        var fullName = "#t" + new string('_', 110) + "000000000003";
        var planXml =
            Header +
            $"""
            <StmtSimple StatementText="SELECT 1 FROM #t WHERE abs(a) = 1" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="1">
            <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="64">
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="100" EstimateIO="0.1" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
            <OutputList/>
            <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
            <DefinedValues/>
            <Object Database="[tempdb]" Schema="[dbo]" Table="[{fullName}]" Storage="RowStore"/>
            <Predicate><ScalarOperator ScalarString="abs([tempdb].[dbo].[{fullName}].[a])=(1)"/></Predicate>
            </TableScan>
            </RelOp>
            </QueryPlan></StmtSimple>
            """ +
            Footer;

        Assert.True(HasNonSargableWarning(planXml));
    }

    /// <summary>
    /// Mirror image: the predicate names a DIFFERENT temp table's full tempdb name (<c>#u</c>, not
    /// this scan's own <c>#t</c>). Cleaning the owner must not make the two collide — only this
    /// scan's own cleaned name should ever match.
    /// </summary>
    [Fact]
    public void TempTableFullTempdbName_AnotherTempTable_NotFlagged_EndToEnd()
    {
        var otherFullName = "#u" + new string('_', 110) + "000000000004";
        var planXml =
            Header +
            $"""
            <StmtSimple StatementText="SELECT 1 FROM #t WHERE abs(a) = 1" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="1">
            <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="64">
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="100" EstimateIO="0.1" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
            <OutputList/>
            <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
            <DefinedValues/>
            <Object Database="[tempdb]" Schema="[dbo]" Table="[#t]" Storage="RowStore"/>
            <Predicate><ScalarOperator ScalarString="abs([tempdb].[dbo].[{otherFullName}].[a])=(1)"/></Predicate>
            </TableScan>
            </RelOp>
            </QueryPlan></StmtSimple>
            """ +
            Footer;

        Assert.False(HasNonSargableWarning(planXml));
    }
}
