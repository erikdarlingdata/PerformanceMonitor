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
/// #4520 (part of #4511, ported from erikdarlingdata/PerformanceStudio@60eb0bf): a reader could not tell a
/// warning SQL Server itself wrote into the plan apart from one the analyzer inferred from plan shape, because
/// both arrive as a <see cref="PlanWarning"/> carrying nothing but type, severity and message.
///
/// <para><see cref="PlanWarning.Source"/> defaults to <see cref="PlanWarningSource.Analyzer"/>. Everything
/// <c>ShowPlanParser.ParseWarningsFromElement</c> returns is stamped <see cref="PlanWarningSource.SqlServer"/>
/// in one place, so a warning lifted straight out of the plan's own <c>&lt;Warnings&gt;</c> element is marked as
/// the engine's record, not an inference the analyzer could get wrong for a given plan.</para>
///
/// <para>The repro XML is synthetic (<c>dbo.t</c>, <c>[db]</c>): one Table Scan carries both a
/// <c>PlanAffectingConvert</c> warning (the engine's own record) and a non-SARGable residual predicate that Rule
/// 12 flags as "Non-SARGable Predicate" (the analyzer's inference).</para>
/// </summary>
public sealed class PlanSync4520Tests
{
    private const string ReproXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT c FROM dbo.t WHERE CONVERT_IMPLICIT(varchar, c, 0) = @p" StatementId="1" StatementCompId="1" StatementType="SELECT" RetrievedFromCache="true" QueryHash="0x1111111111111111" QueryPlanHash="0x2222222222222222">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0032" TableCardinality="100" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
                <DefinedValues/>
                <Predicate><ScalarOperator ScalarString="CONVERT_IMPLICIT(varchar(30),[db].[dbo].[t].[c],0)=@p"/></Predicate>
                <Object Database="[db]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore"/>
              </TableScan>
              <Warnings>
                <PlanAffectingConvert ConvertIssue="Cardinality Estimate" Expression="CONVERT_IMPLICIT(varchar(30),[db].[dbo].[t].[c],0)"/>
              </Warnings>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static ParsedPlan ParseAndAnalyze()
    {
        var plan = ShowPlanParser.Parse(ReproXml);
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    [Fact]
    public void EngineWarning_IsStampedSqlServer()
    {
        var plan = ParseAndAnalyze();
        var scanNode = plan.Batches.Single().Statements.Single().RootNode!.Children.Single();
        var convertWarning = scanNode.Warnings.Single(w => w.WarningType == "Implicit Conversion");

        Assert.Equal(PlanWarningSource.SqlServer, convertWarning.Source);
    }

    [Fact]
    public void AnalyzerWarning_KeepsTheDefaultSource()
    {
        var plan = ParseAndAnalyze();
        var scanNode = plan.Batches.Single().Statements.Single().RootNode!.Children.Single();
        var sargableWarning = scanNode.Warnings.Single(w => w.WarningType == "Non-SARGable Predicate");

        Assert.Equal(PlanWarningSource.Analyzer, sargableWarning.Source);
    }

    [Fact]
    public void McpFormatter_CarriesSourceOnEachWarning()
    {
        // Goes through the product's own MCP formatting path, not the parser directly.
        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(ReproXml, serverName: null, source: "test", identifier: null);

        Assert.Contains("\"source\":\"SqlServer\"", json);
        Assert.Contains("\"source\":\"Analyzer\"", json);
    }
}
