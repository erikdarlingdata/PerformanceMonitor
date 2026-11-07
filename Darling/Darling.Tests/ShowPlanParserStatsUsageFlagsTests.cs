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
/// #8147 — the parser records whether a statement carried a QueryPlan and whether that plan carried an
/// OptimizerStatsUsage element, so a consumer can tell "this plan used no statistics" apart from "this
/// plan does not say" (SQL Server before 2017 / 2016 SP2) and from "this statement has no plan at all".
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ShowPlanParserStatsUsageFlagsTests
{
    private const string TempRaw = "[#orders_______________________________________________________________________________________________000000000012]";

    private static PlanStatement ParseSingle(string statementXml)
    {
        var xml = $"""
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.5" Build="16.0.1000.6">
              <BatchSequence><Batch><Statements>
                {statementXml}
              </Statements></Batch></BatchSequence>
            </ShowPlanXML>
            """;
        var plan = ShowPlanParser.Parse(xml);
        return Assert.Single(plan.Batches.SelectMany(b => b.Statements));
    }

    private static string SelectStatement(string? statsUsage, string objectTable = "[Orders]") => $"""
        <StmtSimple StatementText="SELECT * FROM dbo.Orders" StatementId="1" StatementType="SELECT">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="100">
            {statsUsage}
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0032" TableCardinality="100" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
                <DefinedValues/>
                <Object Database="[SalesDb]" Schema="[dbo]" Table="{objectTable}" IndexKind="Heap" Storage="RowStore"/>
              </TableScan>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        """;

    [Fact]
    public void Parser_StatementWithStatsUsage_SetsBothFlags()
    {
        var stmt = ParseSingle(SelectStatement("""
            <OptimizerStatsUsage>
              <StatisticsInfo Database="[SalesDb]" Schema="[dbo]" Table="[Orders]" Statistics="[IX_Orders_CustomerId]" ModificationCount="5" SamplingPercent="100" LastUpdate="2026-11-01T10:00:00"/>
              <StatisticsInfo Database="[SalesDb]" Schema="[dbo]" Table="[Orders]" Statistics="[_WA_Sys_00000002_0]" ModificationCount="0" SamplingPercent="50" LastUpdate="2026-10-01T10:00:00"/>
            </OptimizerStatsUsage>
            """));

        Assert.True(stmt.HasQueryPlan);
        Assert.True(stmt.HasStatsUsage);
        Assert.Equal(2, stmt.StatsUsage.Count);
    }

    [Fact]
    public void Parser_StatementWithoutStatsUsage_HasPlanButNoStats()
    {
        var stmt = ParseSingle(SelectStatement(null));

        Assert.True(stmt.HasQueryPlan);
        Assert.False(stmt.HasStatsUsage);
        Assert.Empty(stmt.StatsUsage);
    }

    [Fact]
    public void Parser_DeclareStatementWithoutQueryPlan_HasNeither()
    {
        var stmt = ParseSingle("""<StmtSimple StatementText="DECLARE @i int" StatementId="1" StatementType="DECLARE"/>""");

        Assert.False(stmt.HasQueryPlan);
        Assert.False(stmt.HasStatsUsage);
        Assert.NotNull(stmt.RootNode);
    }

    [Fact]
    public void Parser_EmptyStatsUsageElement_IsRecordedWithNoItems()
    {
        var stmt = ParseSingle(SelectStatement("<OptimizerStatsUsage/>"));

        Assert.True(stmt.HasQueryPlan);
        Assert.True(stmt.HasStatsUsage);
        Assert.Empty(stmt.StatsUsage);
    }

    [Fact]
    public void Parser_TempTableStatistic_TableNameIsCleanedLikeTheNode()
    {
        var stmt = ParseSingle(SelectStatement($"""
            <OptimizerStatsUsage>
              <StatisticsInfo Database="[tempdb]" Schema="[dbo]" Table="{TempRaw}" Statistics="[_WA_Sys_00000001_0]" ModificationCount="3" SamplingPercent="100" LastUpdate="2026-11-01T10:00:00"/>
            </OptimizerStatsUsage>
            """, TempRaw));

        var nodeTable = Flatten(stmt.RootNode!)
            .First(n => n.ObjectName != null && n.ObjectName.Contains("#orders"))
            .ObjectName!;

        Assert.Equal("#orders", stmt.StatsUsage[0].TableName);
        Assert.EndsWith(stmt.StatsUsage[0].TableName, nodeTable);
    }

    private static System.Collections.Generic.IEnumerable<PlanNode> Flatten(PlanNode n)
    {
        yield return n;
        foreach (var c in n.Children.SelectMany(Flatten))
        {
            yield return c;
        }
    }
}
