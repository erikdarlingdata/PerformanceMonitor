/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7889: a Key Lookup's severity follows what it cost, not its presence. In an actual plan the lookup's own share of
/// the statement's time grades it on the spill rule's bands (half Critical, a tenth Warning, less Info); without
/// statement time its executions stand in and it never reaches Critical; in an estimated plan the operator's cost
/// share grades it, never past Warning. A severity override from the analyzer config still wins. Fixtures are
/// in-memory <see cref="PlanNode"/> trees analyzed through <see cref="PlanAnalyzer.Analyze"/>, the shape
/// <c>PlanSync4535NodeRulesATests</c> uses.
/// </summary>
public sealed class KeyLookupSeverityTests
{
    private static PlanNode Lookup(bool actual, long executions = 0, long ownMs = 0, int costPercent = 0) => new()
    {
        NodeId = 0,
        PhysicalOp = "Key Lookup (Clustered)",
        LogicalOp = "Key Lookup",
        Lookup = true,
        HasActualStats = actual,
        ActualExecutions = executions,
        ActualRows = executions,
        ActualElapsedMs = ownMs,
        CostPercent = costPercent
    };

    private static PlanWarning Analyze(PlanNode root, long? statementMs, AnalyzerConfig? cfg = null)
    {
        var stmt = new PlanStatement { RootNode = root };
        if (statementMs is { } ms)
            stmt.QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = ms };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan, cfg, null, CancellationToken.None);
        return Assert.Single(root.Warnings, w => w.WarningType == "Key Lookup");
    }

    [Fact]
    public void ACheapRareLookup_IsInfo_NotCritical()
    {
        // Twelve lookups that took no measurable time in a one-second statement.
        var warning = Analyze(Lookup(actual: true, executions: 12, ownMs: 0), statementMs: 1_000);

        Assert.Equal(PlanWarningSeverity.Info, warning.Severity);
    }

    [Fact]
    public void ADominantLookup_IsCritical()
    {
        // A quarter of a million lookups taking 60 percent of the statement.
        var warning = Analyze(Lookup(actual: true, executions: 250_000, ownMs: 600), statementMs: 1_000);

        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);
    }

    [Theory]
    [InlineData(0, PlanWarningSeverity.Info)]
    [InlineData(99, PlanWarningSeverity.Info)]
    [InlineData(100, PlanWarningSeverity.Warning)]
    [InlineData(499, PlanWarningSeverity.Warning)]
    [InlineData(500, PlanWarningSeverity.Critical)]
    public void AnActualLookup_IsGradedOnItsShareOfStatementTime(long ownMs, PlanWarningSeverity expected)
    {
        var warning = Analyze(Lookup(actual: true, executions: 5_000, ownMs: ownMs), statementMs: 1_000);

        Assert.Equal(expected, warning.Severity);
    }

    [Fact]
    public void AnActualLookup_StatesItsTimeShareInTheMessage()
    {
        var warning = Analyze(Lookup(actual: true, executions: 5_000, ownMs: 100), statementMs: 1_000);

        Assert.Contains("Operator time: 100ms (10% of statement)", warning.Message);
    }

    [Theory]
    [InlineData(12, PlanWarningSeverity.Info)]
    [InlineData(999, PlanWarningSeverity.Info)]
    [InlineData(1_000, PlanWarningSeverity.Warning)]
    [InlineData(5_000_000, PlanWarningSeverity.Warning)]
    public void WithoutStatementTime_ExecutionsGradeIt_NeverCritical(long executions, PlanWarningSeverity expected)
    {
        var warning = Analyze(Lookup(actual: true, executions: executions, ownMs: 0), statementMs: null);

        Assert.Equal(expected, warning.Severity);
    }

    [Theory]
    [InlineData(5, PlanWarningSeverity.Info)]
    [InlineData(24, PlanWarningSeverity.Info)]
    [InlineData(25, PlanWarningSeverity.Warning)]
    [InlineData(90, PlanWarningSeverity.Warning)]
    public void AnEstimatedLookup_IsGradedOnItsCostShare_NeverCritical(int costPercent, PlanWarningSeverity expected)
    {
        var warning = Analyze(Lookup(actual: false, costPercent: costPercent), statementMs: null);

        Assert.Equal(expected, warning.Severity);
    }

    [Fact]
    public void ASeverityOverride_StillWins_EitherWay()
    {
        var up = new AnalyzerConfig { Rules = new RulesConfig { SeverityOverrides = new Dictionary<int, string> { [10] = "Critical" } } };
        var down = new AnalyzerConfig { Rules = new RulesConfig { SeverityOverrides = new Dictionary<int, string> { [10] = "Info" } } };

        Assert.Equal(PlanWarningSeverity.Critical, Analyze(Lookup(actual: true, executions: 12, ownMs: 0), statementMs: 1_000, up).Severity);
        Assert.Equal(PlanWarningSeverity.Info, Analyze(Lookup(actual: true, executions: 250_000, ownMs: 600), statementMs: 1_000, down).Severity);
    }
}
