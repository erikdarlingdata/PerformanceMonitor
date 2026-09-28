/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4514 (part of #4511, ported from erikdarlingdata/PerformanceStudio@74ad0e4 /
/// erikdarlingdata/PerformanceStudio@73ca692 / erikdarlingdata/PerformanceStudio@4d978b4):
/// <c>ShowPlanParser</c> has always read a statement's <c>UdfPlans</c>/<c>StoredProcPlan</c> —
/// for an <c>EXEC &lt;procedure&gt;</c> plan, every statement actually lives there, because the
/// EXEC statement itself carries no query plan of its own. Nothing downstream read those fields:
/// <see cref="PlanAnalyzer.Analyze"/>, <see cref="BenefitScorer.Score"/> and
/// <c>ShowPlanParser.ComputeOperatorCosts</c> all walked <c>batch.Statements</c> only, so a plan
/// nesting a procedure or function body got no findings, no benefit scores and no operator
/// costs for the statements doing the actual work. A cursor's own operation statements never
/// even read this sub-plan XSD gap at all, so a function called by a cursor's query lost its
/// whole body silently.
///
/// <para><see cref="PlanStatements.EnumerateAll(ParsedPlan)"/> is the shared, explicit-stack walk
/// that visits every statement including nested bodies, used everywhere the outer-only walk used
/// to be.</para>
/// </summary>
public sealed class PlanSync4514Tests
{
    private const string Ns = "xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"";

    /// <summary>
    /// An estimated <c>EXEC</c> plan: the top statement carries no query plan of its own and
    /// nests a <c>StoredProc</c> whose one statement has a Non-SARGable scan
    /// (<c>upper([t].[a])=[@p]</c>). Before this port, <c>PlanAnalyzer.Analyze</c> walked
    /// <c>batch.Statements</c> and never looked at <c>StoredProcPlan</c>, so this finding never
    /// existed on dev's code.
    /// </summary>
    private const string ExecProcedureWithNonSargableScan = $"""
        <ShowPlanXML {Ns} Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="EXEC dbo.GetByA @p" StatementId="1" StatementCompId="1" StatementType="EXEC" QueryHash="0x1111111111111111" QueryPlanHash="0x2222222222222222">
          <StoredProc ProcName="[db].[dbo].[GetByA]" IsNativelyCompiled="false">
            <Statements>
              <StmtSimple StatementText="SELECT t.a FROM dbo.t AS t WHERE upper(t.a) = @p" StatementId="1" StatementCompId="1" StatementType="SELECT">
                <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
                  <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0032" TableCardinality="100" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                    <OutputList/>
                    <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
                      <DefinedValues/>
                      <Predicate><ScalarOperator ScalarString="upper([db].[dbo].[t].[a])=[@p]"/></Predicate>
                      <Object Database="[db]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore"/>
                    </TableScan>
                  </RelOp>
                </QueryPlan>
              </StmtSimple>
            </Statements>
          </StoredProc>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    /// <summary>
    /// A statement whose call carries no plan of its own but nests a <c>UDF</c> sub-plan with
    /// one statement scanning with the same Non-SARGable predicate shape.
    /// </summary>
    private const string StatementWithUdfNonSargableScan = $"""
        <ShowPlanXML {Ns} Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT dbo.ScalarFn(1)" StatementId="1" StatementCompId="1" StatementType="SELECT">
          <UDF ProcName="[db].[dbo].[ScalarFn]" IsNativelyCompiled="false">
            <Statements>
              <StmtSimple StatementText="SELECT t.a FROM dbo.t AS t WHERE upper(t.a) = @p" StatementId="1" StatementCompId="1" StatementType="SELECT">
                <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
                  <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="0.0032" TableCardinality="100" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                    <OutputList/>
                    <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
                      <DefinedValues/>
                      <Predicate><ScalarOperator ScalarString="upper([db].[dbo].[t].[a])=[@p]"/></Predicate>
                      <Object Database="[db]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore"/>
                    </TableScan>
                  </RelOp>
                </QueryPlan>
              </StmtSimple>
            </Statements>
          </UDF>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    /// <summary>
    /// A cursor operation whose query calls a scalar UDF; the function body carries one
    /// statement. The cursor's operation statement is built by <c>ParseQueryPlanAsStatement</c>,
    /// which never read the <c>UDF</c>/<c>StoredProc</c> XSD gap at all before this port — not a
    /// missing-consumer bug like the analyzer's, a missing-read bug in the parser itself.
    /// </summary>
    private const string CursorOverUdfPlan = $"""
        <ShowPlanXML {Ns} Version="1.564" Build="16.0.4135.4"><BatchSequence><Batch><Statements>
        <StmtCursor StatementText="DECLARE cur CURSOR FAST_FORWARD FOR SELECT dbo.CursorFn(o.Id) FROM dbo.Orders AS o" StatementId="1" StatementCompId="1">
          <CursorPlan CursorName="cur" CursorActualType="FastForward" CursorRequestedType="FastForward" CursorConcurrency="Read Only" ForwardOnly="true">
            <Operation OperationType="FetchQuery">
              <QueryPlan>
                <RelOp NodeId="0" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="10" EstimatedTotalSubtreeCost="0.005" />
              </QueryPlan>
              <UDF ProcName="[db].[dbo].[CursorFn]" IsNativelyCompiled="false">
                <Statements>
                  <StmtSimple StatementText="SELECT @Total = COUNT(*) FROM dbo.Numbers AS n" StatementId="2" StatementCompId="2">
                    <QueryPlan>
                      <RelOp NodeId="0" PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan" EstimateRows="100" EstimatedTotalSubtreeCost="0.02" />
                    </QueryPlan>
                  </StmtSimple>
                </Statements>
              </UDF>
            </Operation>
          </CursorPlan>
        </StmtCursor>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static ParsedPlan ParseAndAnalyze(string xml)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        BenefitScorer.Score(plan);
        return plan;
    }

    /// <summary>
    /// The core fix: a Non-SARGable finding on a statement nested inside a <c>StoredProc</c>
    /// body exists after analysis, through the product's own <c>ShowPlanParser.Parse</c> +
    /// <c>PlanAnalyzer.Analyze</c> path — not a manual call into a helper standing in for the
    /// wiring.
    /// </summary>
    [Fact]
    public void ProcedureBodyStatementGetsItsFinding()
    {
        var plan = ParseAndAnalyze(ExecProcedureWithNonSargableScan);

        Assert.Null(plan.ParseError);
        var topStmt = Assert.Single(Assert.Single(plan.Batches).Statements);
        Assert.NotNull(topStmt.StoredProcPlan);
        var bodyStmt = Assert.Single(topStmt.StoredProcPlan!.Statements);

        var scanNode = bodyStmt.RootNode!.Children.Single();
        var finding = Assert.Single(scanNode.Warnings, w => w.WarningType == "Non-SARGable Predicate");
        Assert.NotNull(finding);
    }

    /// <summary>
    /// The same finding, reached through the shared traversal every consumer now uses instead of
    /// walking <c>batch.Statements</c>.
    /// </summary>
    [Fact]
    public void EnumerateAllSurfacesTheProcedureBodyFinding()
    {
        var plan = ParseAndAnalyze(ExecProcedureWithNonSargableScan);

        var allFindings = PlanStatements.EnumerateAll(plan)
            .Where(s => s.RootNode != null)
            .SelectMany(s => s.RootNode!.Children.SelectMany(c => c.Warnings))
            .ToList();

        Assert.Contains(allFindings, w => w.WarningType == "Non-SARGable Predicate");
    }

    /// <summary>
    /// A UDF sub-plan's statement gets its finding too, and <see cref="BenefitScorer.Score"/>
    /// reaches it through the same shared traversal as the analyzer.
    /// </summary>
    [Fact]
    public void UdfBodyStatementGetsItsFinding()
    {
        var plan = ParseAndAnalyze(StatementWithUdfNonSargableScan);

        var topStmt = Assert.Single(Assert.Single(plan.Batches).Statements);
        var udf = Assert.Single(topStmt.UdfPlans);
        var bodyStmt = Assert.Single(udf.Statements);

        var scanNode = bodyStmt.RootNode!.Children.Single();
        Assert.Contains(scanNode.Warnings, w => w.WarningType == "Non-SARGable Predicate");
    }

    /// <summary>
    /// A cursor statement's sub-plan is read: before this port, a cursor operation's statement
    /// carried no <c>UdfPlans</c> at all, because <c>ParseQueryPlanAsStatement</c> never called
    /// the UDF/StoredProc reader.
    /// </summary>
    [Fact]
    public void CursorOperationStatementCarriesItsFunctionBody()
    {
        var plan = ShowPlanParser.Parse(CursorOverUdfPlan);

        Assert.Null(plan.ParseError);
        var operation = Assert.Single(Assert.Single(plan.Batches).Statements);
        Assert.Equal("cur", operation.CursorName);

        var udf = Assert.Single(operation.UdfPlans);
        Assert.Equal("[db].[dbo].[CursorFn]", udf.ProcName);
        var bodyStmt = Assert.Single(udf.Statements);
        Assert.StartsWith("SELECT @Total", bodyStmt.StatementText);
    }

    /// <summary>
    /// <see cref="PlanStatements.EnumerateAll(ParsedPlan)"/> visits every statement exactly once
    /// on a 3-level nest (procedure &gt; procedure &gt; UDF), in source order: the outer
    /// statement first, then each nested body following the statement that owns it.
    /// </summary>
    [Fact]
    public void EnumerateAllVisitsEveryStatementExactlyOnceOnAThreeLevelNest()
    {
        var xml = ThreeLevelNestPlan();

        var plan = ShowPlanParser.Parse(xml);
        Assert.Null(plan.ParseError);

        var all = PlanStatements.EnumerateAll(plan).ToList();

        Assert.Equal(3, all.Count);
        Assert.Equal(3, all.Distinct().Count());
        Assert.Equal("EXEC outer", all[0].StatementText);
        Assert.Equal("EXEC inner", all[1].StatementText);
        Assert.Equal("SELECT leaf", all[2].StatementText);
    }

    /// <summary>
    /// A 500-deep procedure chain completes: no recursion in the traversal itself (an explicit
    /// stack), and the #4512 depth limit (1,000) bounds the parse that built the tree in the
    /// first place.
    /// </summary>
    [Fact]
    public void EnumerateAllCompletesOnA500DeepProcedureChain()
    {
        var xml = NestedProcedurePlan(500);

        var plan = ShowPlanParser.Parse(xml);
        Assert.Null(plan.ParseError);

        var all = PlanStatements.EnumerateAll(plan).ToList();

        Assert.Equal(501, all.Count);
    }

    private static string ThreeLevelNestPlan()
    {
        return $"""
            <ShowPlanXML {Ns}><BatchSequence><Batch><Statements>
            <StmtSimple StatementText="EXEC outer" StatementType="EXEC">
              <StoredProc ProcName="[dbo].[Outer]">
                <Statements>
                  <StmtSimple StatementText="EXEC inner" StatementType="EXEC">
                    <StoredProc ProcName="[dbo].[Inner]">
                      <Statements>
                        <StmtSimple StatementText="SELECT leaf" StatementType="SELECT">
                          <QueryPlan><RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" /></QueryPlan>
                        </StmtSimple>
                      </Statements>
                    </StoredProc>
                  </StmtSimple>
                </Statements>
              </StoredProc>
            </StmtSimple>
            </Statements></Batch></BatchSequence></ShowPlanXML>
            """;
    }

    private static string NestedProcedurePlan(int levels)
    {
        var xml = new StringBuilder($"<ShowPlanXML {Ns}><BatchSequence><Batch><Statements>");
        for (var level = 0; level < levels; level++)
            xml.Append("<StmtSimple StatementText=\"EXEC p\" StatementType=\"EXEC\">"
                + "<QueryPlan><RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" /></QueryPlan>"
                + "<StoredProc ProcName=\"p\"><Statements>");
        xml.Append("<StmtSimple StatementText=\"SELECT 1\" StatementType=\"SELECT\">"
            + "<QueryPlan><RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" /></QueryPlan></StmtSimple>");
        for (var level = 0; level < levels; level++)
            xml.Append("</Statements></StoredProc></StmtSimple>");
        xml.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");
        return xml.ToString();
    }
}
