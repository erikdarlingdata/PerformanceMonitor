/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8517: a count in the plan analysis text agrees with its noun when it is one. An exchange that spilled once reads
/// "1 write to TempDB", a Key Lookup that ran once is graded "over 1 lookup", a lazy spool with one cache hit reads
/// "1 rewind (cache hit)", and a plan with one wait reads "across 1 wait type". Larger counts keep their plural. The
/// exchange spill is parsed from a plan whose ExchangeSpillDetails carries the count; the analyzer rules run over
/// in-memory <see cref="PlanNode"/> trees, the shape <c>KeyLookupSeverityTests</c> uses.
/// </summary>
public sealed class PlanAnalysisCountOfOneTextTests
{
    private static string ExchangeSpillPlanXml(long writesToTempDb) => $"""
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT c FROM dbo.t" StatementId="1" StatementCompId="1" StatementType="SELECT" RetrievedFromCache="true" QueryHash="0x1111" QueryPlanHash="0x2222">
          <QueryPlan DegreeOfParallelism="4" CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Parallelism" LogicalOp="Gather Streams" EstimateRows="1" EstimateIO="0" EstimateCPU="0.0285" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0317" Parallel="true" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList><ColumnReference Database="[db]" Schema="[dbo]" Table="[t]" Column="c"/></OutputList>
              <Warnings><ExchangeSpillDetails WritesToTempDb="{writesToTempDb}"/></Warnings>
              <Parallelism>
                <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0032" TableCardinality="1" Parallel="true" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList><ColumnReference Database="[db]" Schema="[dbo]" Table="[t]" Column="c"/></OutputList>
                  <TableScan Ordered="false" ForcedIndex="false" ForceScan="false" NoExpandHint="false" Storage="RowStore">
                    <DefinedValues><DefinedValue><ColumnReference Database="[db]" Schema="[dbo]" Table="[t]" Column="c"/></DefinedValue></DefinedValues>
                    <Object Database="[db]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore"/>
                  </TableScan>
                </RelOp>
              </Parallelism>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static PlanWarning ParseExchangeSpill(long writesToTempDb)
    {
        var plan = ShowPlanParser.Parse(ExchangeSpillPlanXml(writesToTempDb));
        Assert.Null(plan.ParseError);
        var stmt = plan.Batches.Single().Statements.Single();
        var warnings = stmt.PlanWarnings.Concat(Descendants(stmt.RootNode!).SelectMany(n => n.Warnings));
        return Assert.Single(warnings, w => w.WarningType == "Exchange Spill");
    }

    private static IEnumerable<PlanNode> Descendants(PlanNode node) =>
        new[] { node }.Concat(node.Children.SelectMany(Descendants));

    [Fact]
    public void ExchangeSpill_OneWrite_ReadsSingular()
    {
        var warning = ParseExchangeSpill(1);

        Assert.StartsWith("Exchange spill: 1 write to TempDB.", warning.Message, StringComparison.Ordinal);
        Assert.Equal(1, warning.SpillDetails!.WritesToTempDb);
    }

    [Fact]
    public void ExchangeSpill_ManyWrites_ReadsPlural()
    {
        var warning = ParseExchangeSpill(5_786_565);

        Assert.StartsWith("Exchange spill: 5,786,565 writes to TempDB.", warning.Message, StringComparison.Ordinal);
        Assert.Equal(5_786_565, warning.SpillDetails!.WritesToTempDb);
    }

    private static PlanWarning AnalyzeSingle(PlanNode root, long statementMs, string warningType)
    {
        var stmt = new PlanStatement { RootNode = root, QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = statementMs } };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan, null, null, CancellationToken.None);
        return Assert.Single(root.Warnings, w => w.WarningType == warningType);
    }

    private static PlanNode Lookup(long executions) => new()
    {
        NodeId = 0,
        PhysicalOp = "Key Lookup (Clustered)",
        LogicalOp = "Key Lookup",
        Lookup = true,
        HasActualStats = true,
        ActualExecutions = executions,
        ActualRows = executions,
        ActualElapsedMs = 100
    };

    [Theory]
    [InlineData(1, "over 1 lookup.")]
    [InlineData(12, "over 12 lookups.")]
    public void KeyLookup_CountsItsLookupsAsAPersonSays(long executions, string expected)
    {
        var warning = AnalyzeSingle(Lookup(executions), statementMs: 1_000, "Key Lookup");

        Assert.Contains(expected, warning.Message, StringComparison.Ordinal);
    }

    /// <summary>An actual plan's spool carries whole rebind and rewind counts; an estimated plan's carries fractions.</summary>
    private static PlanNode LazySpool(bool actual, double rebinds, double rewinds) => new()
    {
        NodeId = 0,
        PhysicalOp = "Table Spool",
        LogicalOp = "Lazy Spool",
        HasActualStats = actual,
        ActualRebinds = actual ? (long)rebinds : 0,
        ActualRewinds = actual ? (long)rewinds : 0,
        ActualExecutions = actual ? (long)(rebinds + rewinds) : 0,
        ActualRows = actual ? (long)(rebinds + rewinds) : 0,
        EstimateRebinds = actual ? 0 : rebinds,
        EstimateRewinds = actual ? 0 : rewinds
    };

    [Theory]
    [InlineData(true, 500, 1, "500 rebinds (cache misses), 1 rewind (cache hit),")]
    [InlineData(true, 500, 20, "500 rebinds (cache misses), 20 rewinds (cache hits),")]
    [InlineData(true, 500, 0, "500 rebinds (cache misses), 0 rewinds (cache hits),")]
    // An estimated count is shown rounded, and the word follows the number shown: 1.3 and 1.49 read "1 rewind".
    [InlineData(false, 500, 1.3, "500 rebinds (cache misses), 1 rewind (cache hit),")]
    [InlineData(false, 500, 1.49, "500 rebinds (cache misses), 1 rewind (cache hit),")]
    // The display rounds a midpoint to the even neighbour, so 1.5 reads "2 rewinds", 2.5 "2 rewinds" and 0.5 "0 rewinds".
    [InlineData(false, 500, 1.5, "500 rebinds (cache misses), 2 rewinds (cache hits),")]
    [InlineData(false, 500, 2.5, "500 rebinds (cache misses), 2 rewinds (cache hits),")]
    [InlineData(false, 500, 0.5, "500 rebinds (cache misses), 0 rewinds (cache hits),")]
    public void LazySpool_CountsItsRewindsAsAPersonSays(bool actual, double rebinds, double rewinds, string expected)
    {
        var warning = AnalyzeSingle(LazySpool(actual, rebinds, rewinds), statementMs: 1_000, "Lazy Spool Ineffective");

        Assert.Contains(expected, warning.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "250 ms of waits across 1 wait type")]
    [InlineData(3, "250 ms of waits across 3 wait types")]
    public void WaitStatsHeader_CountsItsWaitTypesAsAPersonSays(int waitCount, string expected)
    {
        Assert.Equal(expected, PlanDisplayText.WaitStatsHeaderTooltip(waitCount, 250));
    }
}
