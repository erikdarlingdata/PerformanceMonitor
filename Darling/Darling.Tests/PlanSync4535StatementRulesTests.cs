/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4535 step 2 — the statement-level rules' <c>cfg.IsRuleDisabled(N)</c> guards and
/// <see cref="PlanWarning.RuleNumber"/> stamps, ported from erikdarlingdata/PerformanceStudio dev
/// (85492a1) <c>src/PlanViewer.Core/Services/PlanAnalyzer.Statement.cs</c> at the same points for
/// each rule. Rules 21, 25 and 31 are left alone here (#4537 removes them); rule 38 is another
/// step's.
/// </summary>
public sealed class PlanSync4535StatementRulesTests
{
    private static ParsedPlan Analyze(PlanStatement stmt, AnalyzerConfig? cfg = null)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalysisPipeline.Run(plan, cfg, null, CancellationToken.None);
        return plan;
    }

    private static AnalyzerConfig Disabling(int ruleNumber) =>
        new() { Rules = new RulesConfig { Disabled = [ruleNumber] } };

    // ---- fixtures, one per rule, each shaped to fire exactly that rule -------------------------

    private static PlanStatement Rule3_SerialPlan() => new()
    {
        NonParallelPlanReason = "CouldNotGenerateValidParallelPlan",
        StatementSubTreeCost = 5,
        StatementOptmLevel = "FULL",
        StatementText = "SELECT a FROM dbo.t"
    };

    private static PlanStatement Rule9_MemoryGrant() => new()
    {
        MemoryGrant = new MemoryGrantInfo { GrantedMemoryKB = 2097152, MaxUsedMemoryKB = 10240 }
    };

    private static PlanStatement Rule18_CompileMemoryExceeded() => new()
    {
        StatementOptmEarlyAbortReason = "MemoryLimitExceeded"
    };

    private static PlanStatement Rule19_HighCompileCpu() => new()
    {
        CompileCPUMs = 6000
    };

    private static PlanStatement Rule4Stmt_UdfExecution() => new()
    {
        QueryUdfCpuTimeMs = 500,
        QueryUdfElapsedTimeMs = 1500
    };

    private static PlanStatement Rule20_LocalVariables() => new()
    {
        StatementSubTreeCost = 5,
        StatementText = "SELECT a FROM dbo.t WHERE b = @p1",
        Parameters = [new PlanParameter { Name = "@p1", CompiledValue = null }]
    };

    private static PlanStatement Rule27_OptimizeForUnknown() => new()
    {
        StatementText = "SELECT a FROM dbo.t WHERE b = @p1 OPTION (OPTIMIZE FOR UNKNOWN)"
    };

    private static PlanStatement Rule30_MissingIndexQuality() => new()
    {
        MissingIndexes =
        [
            new MissingIndex { Database = "db", Schema = "dbo", Table = "t", Impact = 10 }
        ]
    };

    private static PlanStatement Rule22Stmt_TableVariable() => new()
    {
        StatementType = "SELECT",
        RootNode = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            ObjectName = "@tv"
        }
    };

    private static PlanStatement Rule36_DynamicCursor() => new()
    {
        CursorName = "c1",
        CursorActualType = "Dynamic"
    };

    private static PlanStatement Rule37_CursorMissingLocal() => new()
    {
        StatementText = "DECLARE c1 CURSOR FOR SELECT a FROM dbo.t"
    };

    private static PlanStatement Rule39_TruncatedStatementText() => new()
    {
        StatementText = new string('a', PlanStatement.TruncationLengthThreshold)
    };

    private static readonly Dictionary<int, System.Func<PlanStatement>> Fixtures = new()
    {
        [3] = Rule3_SerialPlan,
        [9] = Rule9_MemoryGrant,
        [18] = Rule18_CompileMemoryExceeded,
        [19] = Rule19_HighCompileCpu,
        [4] = Rule4Stmt_UdfExecution,
        [20] = Rule20_LocalVariables,
        [27] = Rule27_OptimizeForUnknown,
        [30] = Rule30_MissingIndexQuality,
        [22] = Rule22Stmt_TableVariable,
        [36] = Rule36_DynamicCursor,
        [37] = Rule37_CursorMissingLocal,
        [39] = Rule39_TruncatedStatementText
    };

    public static IEnumerable<object[]> Rules() => Fixtures.Keys.Select(n => new object[] { n });

    /// <summary>(a) Each rule's finding is stamped with its own <see cref="PlanWarning.RuleNumber"/>.</summary>
    [Theory]
    [MemberData(nameof(Rules))]
    public void EachRule_StampsItsOwnRuleNumber(int ruleNumber)
    {
        var plan = Analyze(Fixtures[ruleNumber]());
        var findings = plan.Batches[0].Statements[0].PlanWarnings;

        Assert.NotEmpty(findings);
        Assert.All(findings, w => Assert.Equal(ruleNumber, w.RuleNumber));
    }

    /// <summary>
    /// (b) Disabling a rule via <see cref="AnalyzerConfig"/> (PS dev's exact shape:
    /// <c>Rules.Disabled</c>) suppresses every finding that rule would otherwise produce.
    /// </summary>
    [Theory]
    [MemberData(nameof(Rules))]
    public void EachRule_DisabledViaConfig_ProducesNoFinding(int ruleNumber)
    {
        var plan = Analyze(Fixtures[ruleNumber](), Disabling(ruleNumber));
        var findings = plan.Batches[0].Statements[0].PlanWarnings;

        Assert.Empty(findings);
    }

    /// <summary>(c) Disabling one rule leaves every other rule's fixture firing normally.</summary>
    [Theory]
    [MemberData(nameof(Rules))]
    public void DisablingOneRule_LeavesEveryOtherRuleFiring(int disabledRule)
    {
        var otherRule = Fixtures.Keys.First(n => n != disabledRule);
        var plan = Analyze(Fixtures[otherRule](), Disabling(disabledRule));
        var findings = plan.Batches[0].Statements[0].PlanWarnings;

        Assert.NotEmpty(findings);
        Assert.All(findings, w => Assert.Equal(otherRule, w.RuleNumber));
    }

    /// <summary>
    /// The mutation target: rule 20's guard. With the guard removed in product code, this goes
    /// RED (a finding appears despite the rule being disabled); reverting restores GREEN. See the
    /// report for the recorded before/after.
    /// </summary>
    [Fact]
    public void Rule20_DisabledViaConfig_ProducesNoFinding_MutationTarget()
    {
        var plan = Analyze(Rule20_LocalVariables(), Disabling(20));
        Assert.Empty(plan.Batches[0].Statements[0].PlanWarnings);
    }
}
