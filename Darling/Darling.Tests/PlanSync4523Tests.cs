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
/// #4523 (part of #4511, ported from erikdarlingdata/PerformanceStudio@cc18844): Rule 30's duplicate-index-
/// suggestion check grouped missing index suggestions by <c>$"{Schema}.{Table}"</c>, so a plan touching a
/// same-named table (<c>dbo.t</c>) in two different databases had its suggestions merged and reported as
/// duplicates of each other, even though they target unrelated tables. <see cref="MissingIndex"/> already
/// carries <c>Database</c>; the fix adds it to the grouping key.
///
/// <para>The repro XML is synthetic (<c>dbo.t</c>, <c>[db1]</c>/<c>[db2]</c>): one statement with two missing
/// index groups for <c>[db1].[dbo].[t]</c> and <c>[db2].[dbo].[t]</c>.</para>
/// </summary>
public sealed class PlanSync4523Tests
{
    private static string ReproXml(string db1, string db2) => $"""
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT c FROM {db1}.dbo.t JOIN {db2}.dbo.t AS t2 ON t2.id = c" StatementId="1" StatementCompId="1" StatementType="SELECT" RetrievedFromCache="true" QueryHash="0x1111111111111111" QueryPlanHash="0x2222222222222222">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <MissingIndexes>
              <MissingIndexGroup Impact="50.0"><MissingIndex Database="[{db1}]" Schema="[dbo]" Table="[t]"><ColumnGroup Usage="EQUALITY"><Column Name="[id]" ColumnId="1"/></ColumnGroup></MissingIndex></MissingIndexGroup>
              <MissingIndexGroup Impact="60.0"><MissingIndex Database="[{db2}]" Schema="[dbo]" Table="[t]"><ColumnGroup Usage="EQUALITY"><Column Name="[id]" ColumnId="1"/></ColumnGroup></MissingIndex></MissingIndexGroup>
            </MissingIndexes>
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0032" TableCardinality="100" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row"><OutputList/><TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore"><DefinedValues/><Object Database="[{db1}]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore"/></TableScan></RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static ParsedPlan ParseAndAnalyze(string db1, string db2)
    {
        var plan = ShowPlanParser.Parse(ReproXml(db1, db2));
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    [Fact]
    public void SameTableName_DifferentDatabases_AreNotReportedAsDuplicates()
    {
        var plan = ParseAndAnalyze("db1", "db2");
        var stmt = plan.Batches.Single().Statements.Single();

        Assert.DoesNotContain(stmt.PlanWarnings, w => w.WarningType == "Duplicate Index Suggestions");
    }

    [Fact]
    public void SameTableName_SameDatabase_AreStillReportedAsDuplicates()
    {
        var plan = ParseAndAnalyze("db1", "db1");
        var stmt = plan.Batches.Single().Statements.Single();

        Assert.Contains(stmt.PlanWarnings, w => w.WarningType == "Duplicate Index Suggestions");
    }
}
