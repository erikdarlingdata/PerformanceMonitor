/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4535 (part of #4511): the severity-override pass, keyed on <see cref="PlanWarning.RuleNumber"/>
/// rather than matching <c>WarningType</c> against a rule-to-name table. Ported from
/// erikdarlingdata/PerformanceStudio dev (85492a1) commit dcc06db (PS#575),
/// <c>tests/PlanViewer.Core.Tests/SeverityOverrideTests.cs</c>. PM's rules don't stamp
/// <c>RuleNumber</c> yet (a parallel step adds the stamps), so these pins build the
/// <see cref="PlanWarning"/> directly with <c>RuleNumber</c> set, calling
/// <see cref="PlanAnalysisPipeline.Run(ParsedPlan, AnalyzerConfig?, ServerMetadata?, CancellationToken)"/>
/// so the override pass itself — not a hand call to the helper — is what is under test.
/// </summary>
public sealed class PlanSync4535SeverityOverrideTests
{
    private const string Ns = "xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"";

    private static AnalyzerConfig OverrideRule(int rule, PlanWarningSeverity severity) => new()
    {
        Rules = new RulesConfig
        {
            SeverityOverrides = { [rule] = severity.ToString() }
        }
    };

    /// <summary>A plan with one statement carrying one analyzer-sourced warning, no rule number stamped by the parser.</summary>
    private const string OneStatementPlan = $"""
        <ShowPlanXML {Ns} Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM dbo.t" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="1" StatementOptmLevel="FULL">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <TableScan Storage="RowStore">
                <Object Database="[db]" Schema="[dbo]" Table="[t]" Storage="RowStore"/>
              </TableScan>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    /// <summary>An EXEC procedure plan whose one statement (in the body) carries a stamped warning, same shape as PlanSync4514Tests.</summary>
    private const string ExecProcedurePlan = $"""
        <ShowPlanXML {Ns} Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="EXEC dbo.GetByA @p" StatementId="1" StatementCompId="1" StatementType="EXEC">
          <StoredProc ProcName="[db].[dbo].[GetByA]" IsNativelyCompiled="false">
            <Statements>
              <StmtSimple StatementText="SELECT t.a FROM dbo.t AS t WHERE t.a = @p" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="1" StatementOptmLevel="FULL">
                <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
                  <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="1" TableCardinality="1" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                    <OutputList/>
                    <TableScan Storage="RowStore">
                      <Object Database="[db]" Schema="[dbo]" Table="[t]" Storage="RowStore"/>
                    </TableScan>
                  </RelOp>
                </QueryPlan>
              </StmtSimple>
            </Statements>
          </StoredProc>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static ParsedPlan Analyze(string xml, AnalyzerConfig cfg)
    {
        var plan = ShowPlanParser.Parse(xml);
        PlanAnalysisPipeline.Run(plan, cfg, null, CancellationToken.None);
        return plan;
    }

    /// <summary>
    /// (a) An override for a stamped warning's rule changes its Severity, and the arithmetic
    /// <see cref="McpPlanAnalysisFormatter.BuildAnalysisResult"/> uses for critical_count
    /// (<c>allWarnings.Count(w =&gt; w.Severity == Critical)</c>) follows it. The formatter takes
    /// no <see cref="AnalyzerConfig"/> parameter yet, so this pins the post-override plan's own
    /// warning list with that exact predicate, rather than a hand call to the override helper.
    /// </summary>
    [Fact]
    public void OverrideForStampedRule_ChangesSeverity_AndCriticalCountArithmeticFollows()
    {
        var plan = ShowPlanParser.Parse(OneStatementPlan);
        var stmt = plan.Batches[0].Statements[0];
        stmt.PlanWarnings.Add(new PlanWarning
        {
            WarningType = "Test Finding",
            Message = "test",
            Severity = PlanWarningSeverity.Warning,
            RuleNumber = 41
        });

        PlanAnalysisPipeline.Run(plan, OverrideRule(41, PlanWarningSeverity.Critical), null, CancellationToken.None);

        var warning = plan.Batches[0].Statements[0].PlanWarnings.Single(w => w.WarningType == "Test Finding");
        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);

        var criticalCount = plan.Batches[0].Statements[0].PlanWarnings
            .Count(w => w.Severity == PlanWarningSeverity.Critical);
        Assert.Equal(1, criticalCount);
    }

    /// <summary>
    /// (b) A Source == SqlServer warning is untouched by an override for its rule, even when it
    /// carries a RuleNumber (a shape the parser itself never produces — the engine's own
    /// warnings are never stamped — but the skip must hold on Source alone, not rely on
    /// RuleNumber being null to fail closed).
    /// </summary>
    [Fact]
    public void SqlServerSourcedWarning_IsNeverOverridden()
    {
        var plan = ShowPlanParser.Parse(OneStatementPlan);
        var stmt = plan.Batches[0].Statements[0];
        stmt.PlanWarnings.Add(new PlanWarning
        {
            WarningType = "Implicit Conversion",
            Message = "engine warning",
            Severity = PlanWarningSeverity.Warning,
            Source = PlanWarningSource.SqlServer,
            RuleNumber = 29
        });

        PlanAnalysisPipeline.Run(plan, OverrideRule(29, PlanWarningSeverity.Critical), null, CancellationToken.None);

        var warning = plan.Batches[0].Statements[0].PlanWarnings.Single(w => w.WarningType == "Implicit Conversion");
        Assert.Equal(PlanWarningSeverity.Warning, warning.Severity);
    }

    /// <summary>(c) An override reaches a warning inside an EXEC procedure body (PlanStatements.EnumerateAll).</summary>
    [Fact]
    public void OverrideReachesWarningInsideExecProcedureBody()
    {
        var plan = ShowPlanParser.Parse(ExecProcedurePlan);
        var bodyStmt = PlanStatements.EnumerateAll(plan).Single(s => s.StatementText.Contains("t.a = @p"));
        bodyStmt.PlanWarnings.Add(new PlanWarning
        {
            WarningType = "Body Finding",
            Message = "test",
            Severity = PlanWarningSeverity.Warning,
            RuleNumber = 12
        });

        PlanAnalysisPipeline.Run(plan, OverrideRule(12, PlanWarningSeverity.Info), null, CancellationToken.None);

        var bodyStmtAfter = PlanStatements.EnumerateAll(plan).Single(s => s.StatementText.Contains("t.a = @p"));
        var warning = bodyStmtAfter.PlanWarnings.Single(w => w.WarningType == "Body Finding");
        Assert.Equal(PlanWarningSeverity.Info, warning.Severity);
    }

    /// <summary>(d) An unknown severity string is ignored (Enum.TryParse fails silently, no throw, no change).</summary>
    [Fact]
    public void UnknownSeverityString_IsIgnored()
    {
        var plan = ShowPlanParser.Parse(OneStatementPlan);
        var stmt = plan.Batches[0].Statements[0];
        stmt.PlanWarnings.Add(new PlanWarning
        {
            WarningType = "Test Finding",
            Message = "test",
            Severity = PlanWarningSeverity.Warning,
            RuleNumber = 41
        });

        var cfg = new AnalyzerConfig
        {
            Rules = new RulesConfig { SeverityOverrides = { [41] = "NotASeverity" } }
        };
        PlanAnalysisPipeline.Run(plan, cfg, null, CancellationToken.None);

        var warning = plan.Batches[0].Statements[0].PlanWarnings.Single(w => w.WarningType == "Test Finding");
        Assert.Equal(PlanWarningSeverity.Warning, warning.Severity);
    }

    /// <summary>(e) EnumerateAllWithContainer gives the container path for a nested proc statement.</summary>
    [Fact]
    public void EnumerateAllWithContainer_GivesContainerPathForNestedProcStatement()
    {
        var plan = ShowPlanParser.Parse(ExecProcedurePlan);

        var entries = PlanStatements.EnumerateAllWithContainer(plan).ToList();

        Assert.Equal(2, entries.Count);
        Assert.Null(entries[0].ContainerPath);
        Assert.Equal("EXEC dbo.GetByA @p", entries[0].Statement.StatementText);

        Assert.Equal("[db].[dbo].[GetByA]", entries[1].ContainerPath);
        Assert.Contains("t.a = @p", entries[1].Statement.StatementText);

        // EnumerateAll must still give the same statements, in the same order, as the
        // context-carrying walk it now delegates to.
        var plain = PlanStatements.EnumerateAll(plan).ToList();
        Assert.Equal(entries.Select(e => e.Statement), plain);
    }
}
