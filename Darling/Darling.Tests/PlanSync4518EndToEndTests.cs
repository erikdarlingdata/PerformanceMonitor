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
/// #4518 end-to-end: the same four faults pinned directly against <see cref="PlanAnalyzer.DetectNonSargablePattern"/>
/// in <see cref="PlanSync4518Tests"/>, proven through the real call path — <see cref="ShowPlanParser.Parse"/> reading
/// a synthetic scan's <c>Predicate</c>/<c>ScalarString</c> XML, then <see cref="PlanAnalyzer.Analyze"/> populating
/// <c>PlanNode.Warnings</c> — so the seam pins above are backed by proof the parser and analyzer actually wire the
/// predicate text through to a "Non-SARGable Predicate" warning (or its absence), not just that the string helper
/// itself behaves.
/// </summary>
public sealed class PlanSync4518EndToEndTests
{
    private static string ScanPlanXml(string predicate)
    {
        return $"""
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.539" Build="16.0.1000.6"><BatchSequence><Batch><Statements>
            <StmtSimple StatementText="SELECT 1 FROM dbo.t WHERE ..." StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="1">
            <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="64">
            <RelOp NodeId="0" PhysicalOp="Index Scan" LogicalOp="Index Scan" EstimateRows="100" EstimateIO="0.1" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
            <OutputList/>
            <IndexScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
            <DefinedValues/>
            <Object Database="[db]" Schema="[dbo]" Table="[t]" Index="[ix_t]" IndexKind="NonClustered" Storage="RowStore"/>
            <Predicate><ScalarOperator ScalarString="{predicate}"/></Predicate>
            </IndexScan>
            </RelOp>
            </QueryPlan></StmtSimple>
            </Statements></Batch></BatchSequence></ShowPlanXML>
            """;
    }

    private static bool HasNonSargableWarning(string predicate)
    {
        var plan = ShowPlanParser.Parse(ScanPlanXml(predicate));
        PlanAnalyzer.Analyze(plan);
        return plan.AllWarnings.Any(w => w.WarningType == "Non-SARGable Predicate");
    }

    // ---------------------------------------------------------------
    // Fault 1: CONVERT_IMPLICIT on the parameter side, end to end
    // ---------------------------------------------------------------

    [Fact]
    public void ConvertImplicitOnParameter_NoWarning_EndToEnd()
    {
        Assert.False(HasNonSargableWarning("[t].[a]=CONVERT_IMPLICIT(int,[@p1],0)"));
    }

    // ---------------------------------------------------------------
    // Fault 2: a parameter-side function in an earlier conjunct, end to end
    // ---------------------------------------------------------------

    [Fact]
    public void ParameterSideFunctionInEarlierComparison_NoWarning_EndToEnd()
    {
        Assert.False(HasNonSargableWarning("[t].[a]=[@p1] AND [t].[b]=upper([@p2])"));
    }

    [Fact]
    public void ColumnSideFunctionInLaterComparison_StillWarns_EndToEnd()
    {
        Assert.True(HasNonSargableWarning("[t].[a]=[@p1] AND upper([t].[b])=[@p2]"));
    }

    // ---------------------------------------------------------------
    // Fault 3: parameter-side ISNULL, end to end
    // ---------------------------------------------------------------

    [Fact]
    public void ParameterSideIsnull_NoWarning_EndToEnd()
    {
        Assert.False(HasNonSargableWarning("[t].[a] = isnull([@p],0)"));
    }

    // ---------------------------------------------------------------
    // Fault 4: LIKE recognized as a comparison operator, end to end
    // ---------------------------------------------------------------

    [Fact]
    public void ParameterSideFunctionInLikePattern_NoWarning_EndToEnd()
    {
        Assert.False(HasNonSargableWarning("[t].[c] like upper([@p])"));
    }
}
