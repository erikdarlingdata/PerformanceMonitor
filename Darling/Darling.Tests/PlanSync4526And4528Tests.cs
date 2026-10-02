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
/// #4526 — rule 34 "Bare Scan": a Clustered Index Scan or heap Table Scan with no predicate
/// (and not a lookup) is a candidate for a narrower index. An actual plan fires when the
/// operator's own time is above 0ms; an estimated plan fires when the scan costs at least 20%
/// of the plan. Three or fewer output columns suggests a covering nonclustered index; more
/// columns suggests columnstore, because column count doesn't penalize columnstore the way it
/// does a rowstore index.
///
/// <para>Mirrors erikdarlingdata/PerformanceStudio@f5e041e (narrow output) and @e05bc69 (wide
/// output, columnstore advice).</para>
/// </summary>
public sealed class PlanSync4526Tests
{
    private static ParsedPlan Analyze(PlanNode root)
    {
        var stmt = new PlanStatement { StatementText = "SELECT a FROM dbo.t", RootNode = root };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    private static bool Has(PlanNode node, string warningType) =>
        node.Warnings.Any(w => w.WarningType == warningType);

    [Fact]
    public void ActualPlan_BareHeapScanWithOwnTime_NarrowOutput_WarnsWithCoveringIndexAdvice()
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            HasActualStats = true,
            ActualElapsedMs = 50,
            OutputColumns = "[dbo].[t].[a],[dbo].[t].[b]"
        };
        Analyze(scan);

        var warning = Assert.Single(scan.Warnings, w => w.WarningType == "Bare Scan");
        Assert.Contains("Consider a clustered or nonclustered index", warning.Message);
        Assert.DoesNotContain("columnstore index (CCI or NCCI)", warning.Message);
    }

    [Fact]
    public void ActualPlan_BareClusteredIndexScanWithOwnTime_WideOutput_WarnsWithColumnstoreAdvice()
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Clustered Index Scan",
            LogicalOp = "Clustered Index Scan",
            HasActualStats = true,
            ActualElapsedMs = 50,
            OutputColumns = "[dbo].[t].[a],[dbo].[t].[b],[dbo].[t].[c],[dbo].[t].[d],[dbo].[t].[e]"
        };
        Analyze(scan);

        var warning = Assert.Single(scan.Warnings, w => w.WarningType == "Bare Scan");
        Assert.Contains("columnstore index (CCI or NCCI)", warning.Message);
    }

    [Fact]
    public void ActualPlan_ScanWithPredicate_DoesNotWarn()
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            HasActualStats = true,
            ActualElapsedMs = 50,
            OutputColumns = "[dbo].[t].[a]",
            Predicate = "[dbo].[t].[a] = 1"
        };
        Analyze(scan);

        Assert.False(Has(scan, "Bare Scan"));
    }

    [Fact]
    public void EstimatedPlan_CostAtOrAbove20Percent_Warns()
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            HasActualStats = false,
            CostPercent = 25,
            OutputColumns = "[dbo].[t].[a]"
        };
        Analyze(scan);

        Assert.True(Has(scan, "Bare Scan"));
    }

    [Fact]
    public void EstimatedPlan_CostBelow20Percent_DoesNotWarn()
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            HasActualStats = false,
            CostPercent = 10,
            OutputColumns = "[dbo].[t].[a]"
        };
        Analyze(scan);

        Assert.False(Has(scan, "Bare Scan"));
    }
}

/// <summary>
/// #4528 — the Serial Plan (rule 3) benefit formula. PM's old two-branch estimate (a flat 75%
/// when CPU-bound, otherwise the CPU/elapsed ratio times 75% capped at 50%) is replaced with
/// PS's single formula: <c>(cpu * (DOP - 1) / DOP) / elapsed * 100</c>, DOP 4, capped at 100%,
/// with no benefit when <see cref="PlanStatement.StatementSubTreeCost"/> is under 1 (a trivial
/// plan doesn't gain from parallelism).
///
/// <para>Mirrors erikdarlingdata/PerformanceStudio@4a5efa5.</para>
/// </summary>
public sealed class PlanSync4528Tests
{
    private static PlanWarning ScoreSerialPlan(long cpuMs, long elapsedMs, double subTreeCost)
    {
        var stmt = new PlanStatement
        {
            StatementText = "SELECT a FROM dbo.t",
            StatementSubTreeCost = subTreeCost,
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = cpuMs, ElapsedTimeMs = elapsedMs }
        };
        stmt.PlanWarnings.Add(new PlanWarning { WarningType = "Serial Plan" });

        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        BenefitScorer.Score(plan);

        return stmt.PlanWarnings.Single();
    }

    [Fact]
    public void NotCpuBound_NewFormulaExceedsTheOldFiftyPercentCap()
    {
        // cpu 900 / elapsed 1000 / cost 5. Old: 0.9 ratio * 75% = 67.5%, capped at 50% -> 50%.
        // New: (900 * 3/4) / 1000 * 100 = 67.5%, uncapped.
        var warning = ScoreSerialPlan(cpuMs: 900, elapsedMs: 1000, subTreeCost: 5);
        Assert.Equal(67.5, warning.MaxBenefitPercent);
    }

    [Fact]
    public void CpuBound_NewFormulaExceedsTheOldFlatSeventyFivePercent()
    {
        // cpu 2000 / elapsed 1000 / cost 5. Old: CPU-bound flat 75%.
        // New: (2000 * 3/4) / 1000 * 100 = 150%, capped at 100%.
        var warning = ScoreSerialPlan(cpuMs: 2000, elapsedMs: 1000, subTreeCost: 5);
        Assert.Equal(100, warning.MaxBenefitPercent);
    }

    [Fact]
    public void TrivialStatement_CostUnderOne_GetsNoBenefit()
    {
        // cost 0.5: the new formula gives no benefit at all (StatementSubTreeCost < 1 gate).
        // The old formula had no such gate and would have scored this statement.
        var warning = ScoreSerialPlan(cpuMs: 900, elapsedMs: 1000, subTreeCost: 0.5);
        Assert.Null(warning.MaxBenefitPercent);
    }
}
