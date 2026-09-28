/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4535 step 1 — the plumbing for per-rule analyzer configuration: <see cref="AnalyzerConfig"/>,
/// <see cref="ServerMetadata"/> and <see cref="PlanWarning.RuleNumber"/>, ported from
/// erikdarlingdata/PerformanceStudio dev (85492a1), plus the new
/// <c>Analyze(plan, config, serverMetadata, ct)</c>/<c>Run(plan, config, serverMetadata, ct)</c>
/// overloads. No rule reads <c>cfg</c> or <c>serverMetadata</c> yet (that starts in later #4535
/// steps and #4530), so this file pins that the new overloads exist, parse the same JSON shape
/// PerformanceStudio does, and change no analyzer output.
/// </summary>
public sealed class PlanSync4535ConfigPlumbingTests
{
    /// <summary>
    /// A plan with a serial-plan finding (rule 3, statement-level), a filter finding (rule 1,
    /// node-level) and a bare scan (rule 34, node-level) — enough surface for the identical-output
    /// pin to actually exercise both the statement and node walks.
    /// </summary>
    private const string ReproXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM dbo.t WHERE b = 1" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="5" StatementOptmLevel="FULL">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104" NonParallelPlanReason="CouldNotGenerateValidParallelPlan">
            <RelOp NodeId="0" PhysicalOp="Filter" LogicalOp="Filter" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="0" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <Filter StartupExpression="0">
                <Predicate>
                  <ScalarOperator ScalarString="[dbo].[t].[b]=(1)"/>
                </Predicate>
                <RelOp NodeId="1" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1000" EstimateIO="5" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
                  <OutputList/>
                  <TableScan Storage="RowStore">
                    <Object Database="[Repro]" Schema="[dbo]" Table="[t]" Storage="RowStore"/>
                  </TableScan>
                </RelOp>
              </Filter>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static List<PlanWarning> AllWarnings(ParsedPlan plan) =>
        plan.Batches.SelectMany(b => b.Statements).SelectMany(s => s.PlanWarnings).ToList();

    /// <summary>(a) The default config disables no rule and overrides no severity.</summary>
    [Fact]
    public void Default_DisablesNoRule_AndOverridesNoSeverity()
    {
        var cfg = AnalyzerConfig.Default;

        for (var rule = 1; rule <= 39; rule++)
        {
            Assert.False(cfg.IsRuleDisabled(rule));
            Assert.Null(cfg.GetSeverityOverride(rule));
        }
    }

    /// <summary>
    /// (b) A PerformanceStudio-shaped config JSON round-trips to the same <see cref="AnalyzerConfig"/>
    /// values PS itself produces from that shape: rule 5 disabled, rule 12 overridden to "Info".
    /// </summary>
    [Fact]
    public void Parse_PsShapedJson_RoundTripsToTheSameValues()
    {
        const string json = """{"rules":{"disabled":[5],"severity_overrides":{"12":"Info"}}}""";

        var cfg = JsonSerializer.Deserialize<AnalyzerConfig>(json);

        Assert.NotNull(cfg);
        Assert.True(cfg!.IsRuleDisabled(5));
        Assert.False(cfg.IsRuleDisabled(12));
        Assert.Equal("Info", cfg.GetSeverityOverride(12));
        Assert.Null(cfg.GetSeverityOverride(5));
    }

    /// <summary>
    /// (c) The no-behaviour-change pin: passing an explicit <see cref="AnalyzerConfig.Default"/>
    /// and a null <see cref="ServerMetadata"/> through the new overload gives byte-for-byte the
    /// same findings (type, severity, message, benefit) as the old no-config overload, because no
    /// rule reads either parameter yet.
    /// </summary>
    [Fact]
    public void Run_WithDefaultConfigAndNullMetadata_MatchesTheNoConfigOverload()
    {
        var oldPlan = ShowPlanParser.Parse(ReproXml);
        PlanAnalysisPipeline.Run(oldPlan);

        var newPlan = ShowPlanParser.Parse(ReproXml);
        PlanAnalysisPipeline.Run(newPlan, AnalyzerConfig.Default, null, CancellationToken.None);

        var oldWarnings = AllWarnings(oldPlan);
        var newWarnings = AllWarnings(newPlan);

        Assert.NotEmpty(oldWarnings);
        Assert.Equal(oldWarnings.Count, newWarnings.Count);

        for (var i = 0; i < oldWarnings.Count; i++)
        {
            Assert.Equal(oldWarnings[i].WarningType, newWarnings[i].WarningType);
            Assert.Equal(oldWarnings[i].Severity, newWarnings[i].Severity);
            Assert.Equal(oldWarnings[i].Message, newWarnings[i].Message);
            Assert.Equal(oldWarnings[i].MaxBenefitPercent, newWarnings[i].MaxBenefitPercent);
        }
    }

    /// <summary>
    /// Same no-behaviour-change pin at the analyzer level (not just the pipeline), and through the
    /// same-shaped call as PerformanceStudio's <c>Analyze(plan, config, serverMetadata)</c>.
    /// </summary>
    [Fact]
    public void Analyze_WithDefaultConfigAndNullMetadata_MatchesTheNoConfigOverload()
    {
        var oldPlan = ShowPlanParser.Parse(ReproXml);
        PlanAnalyzer.Analyze(oldPlan);

        var newPlan = ShowPlanParser.Parse(ReproXml);
        PlanAnalyzer.Analyze(newPlan, AnalyzerConfig.Default, null, CancellationToken.None);

        var oldWarnings = AllWarnings(oldPlan);
        var newWarnings = AllWarnings(newPlan);

        Assert.NotEmpty(oldWarnings);
        Assert.Equal(oldWarnings.Count, newWarnings.Count);

        for (var i = 0; i < oldWarnings.Count; i++)
        {
            Assert.Equal(oldWarnings[i].WarningType, newWarnings[i].WarningType);
            Assert.Equal(oldWarnings[i].Severity, newWarnings[i].Severity);
            Assert.Equal(oldWarnings[i].Message, newWarnings[i].Message);
        }
    }

}
