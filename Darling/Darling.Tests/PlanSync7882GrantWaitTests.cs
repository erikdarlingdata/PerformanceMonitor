/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7882 and #7899. The showplan schema records MemoryGrantInfo's GrantWaitTime in SECONDS. The parser
/// stores milliseconds (seconds * 1000), so the warning text, its severity cut, the benefit score and
/// every other consumer keep their millisecond meaning. #7899: the benefit scorer keys the grant-wait
/// benefit on the analyzer's "Memory Grant Wait" warning, and a modified table variable is one finding.
/// </summary>
public sealed class PlanSync7882GrantWaitTests
{
    private const string Fixture = "memory_grant_wait_plan.sqlplan";
    private const string FixtureWait = "GrantWaitTime=\"1000\"";

    /// <summary>The fixture's statement ran 26,819 ms, so a wait is a fraction of that.</summary>
    private const long FixtureElapsedMs = 26819;

    private static ParsedPlan AnalyzeWithWaitSeconds(int seconds)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "OriginPlans", Fixture);
        var xml = File.ReadAllText(path);
        Assert.Contains(FixtureWait, xml);

        var plan = ShowPlanParser.Parse(xml.Replace(FixtureWait, $"GrantWaitTime=\"{seconds}\""));
        PlanAnalysisPipeline.Run(plan, null, null, CancellationToken.None);
        return plan;
    }

    private static PlanStatement FirstStatement(ParsedPlan plan) =>
        PlanStatements.EnumerateAll(plan).First(s => s.MemoryGrant != null);

    private static PlanWarning WaitWarning(ParsedPlan plan) =>
        Assert.Single(FirstStatement(plan).PlanWarnings, w => w.WarningType == "Memory Grant Wait");

    [Fact]
    public void ParsedGrantWait_IsSecondsConvertedToMilliseconds()
    {
        var plan = AnalyzeWithWaitSeconds(12);

        Assert.Equal(12_000, FirstStatement(plan).MemoryGrant!.GrantWaitTimeMs);
    }

    [Fact]
    public void WaitWarningText_ReadsTheWaitInMilliseconds()
    {
        var plan = AnalyzeWithWaitSeconds(12);

        Assert.Contains("12,000ms", WaitWarning(plan).Message);
    }

    [Fact]
    public void FiveSecondWait_IsCritical()
    {
        Assert.Equal(PlanWarningSeverity.Critical, WaitWarning(AnalyzeWithWaitSeconds(5)).Severity);
    }

    [Fact]
    public void FourSecondWait_IsWarning()
    {
        Assert.Equal(PlanWarningSeverity.Warning, WaitWarning(AnalyzeWithWaitSeconds(4)).Severity);
    }

    /// <summary>The scorer used to key on the parser's "Memory Grant" warning, so the analyzer's wait
    /// warning was never scored and a query that waited a third of its time for a grant ranked last.</summary>
    [Fact]
    public void WaitWarning_GetsBenefitOfWaitOverElapsed()
    {
        var plan = AnalyzeWithWaitSeconds(12);

        var expected = Math.Round(12_000.0 / FixtureElapsedMs * 100, 1);
        Assert.Equal(expected, WaitWarning(plan).MaxBenefitPercent);
    }

    /// <summary>The parser's "Memory Grant" warning is the engine's own grant note. It says nothing about
    /// time waited, so it earns no benefit, whatever the statement's grant wait was.</summary>
    [Fact]
    public void ParserMemoryGrantWarning_GetsNoWaitBenefit()
    {
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = 1000, CpuTimeMs = 1000 },
            MemoryGrant = new MemoryGrantInfo { GrantWaitTimeMs = 500 },
            PlanWarnings =
            [
                new PlanWarning { WarningType = "Memory Grant", Severity = PlanWarningSeverity.Warning }
            ]
        };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };

        BenefitScorer.Score(plan, CancellationToken.None);

        Assert.Null(stmt.PlanWarnings.Single(w => w.WarningType == "Memory Grant").MaxBenefitPercent);
    }

    /// <summary>One table variable modified by one operator: the statement warning is the one report,
    /// so the counts the MCP result publishes read 1, and the operators stay named in origin_node_ids.</summary>
    [Fact]
    public void ModifiedTableVariable_IsOneCriticalFinding()
    {
        const string xml = """
            <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
            <StmtSimple StatementText="INSERT INTO @tv SELECT a FROM dbo.t" StatementId="1" StatementCompId="1" StatementType="INSERT">
              <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
                <RelOp NodeId="10" PhysicalOp="Table Insert" LogicalOp="Insert" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <TableInsert>
                    <Object Database="[Repro]" Table="[@tv]" Storage="RowStore"/>
                    <RelOp NodeId="11" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                      <OutputList/>
                      <TableScan Storage="RowStore">
                        <Object Database="[Repro]" Schema="[dbo]" Table="[t]" Storage="RowStore"/>
                      </TableScan>
                    </RelOp>
                  </TableInsert>
                </RelOp>
              </QueryPlan>
            </StmtSimple>
            </Statements></Batch></BatchSequence></ShowPlanXML>
            """;

        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(xml, "test-server", "showplan", null);

        using var doc = JsonDocument.Parse(json);
        var stmt = doc.RootElement.GetProperty("statements").EnumerateArray().Single();
        var tableVariable = stmt.GetProperty("warnings").EnumerateArray()
            .Where(w => w.GetProperty("type").GetString() == "Table Variable")
            .ToList();

        Assert.Single(tableVariable);
        Assert.Equal("Critical", tableVariable[0].GetProperty("severity").GetString());
        Assert.Equal(10, tableVariable[0].GetProperty("origin_node_ids").EnumerateArray().Single().GetInt32());
        Assert.Equal(1, stmt.GetProperty("critical_count").GetInt32());
    }
}
