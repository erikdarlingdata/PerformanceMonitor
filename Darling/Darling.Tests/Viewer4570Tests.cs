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
/// #4570 — the shared plan viewer's runtime summary card matches PerformanceStudio (PS) dev's:
/// title, row order, memory-grant colors, and the plan-wide spill flag. Mirrors
/// erikdarlingdata/PerformanceStudio@40ade29 (title, row order, spill override) and @5731ae9
/// (loosened efficiency thresholds, compile time always shown).
/// </summary>
public sealed class Viewer4570Tests
{
    private static PlanStatement Statement() => new() { StatementText = "SELECT 1" };

    // --- RuntimeSummaryTitle (E7) ---------------------------------------------------------

    [Fact]
    public void RuntimeSummaryTitle_NoQueryTimeStats_IsPredictedRuntime()
    {
        var stmt = Statement();
        Assert.Null(stmt.QueryTimeStats);
        Assert.Equal("Predicted Runtime", PlanDisplayText.RuntimeSummaryTitle(stmt));
    }

    [Fact]
    public void RuntimeSummaryTitle_HasQueryTimeStats_IsRuntimeSummary()
    {
        var stmt = Statement();
        stmt.QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = 100, CpuTimeMs = 50 };
        Assert.Equal("Runtime Summary", PlanDisplayText.RuntimeSummaryTitle(stmt));
    }

    // --- HasSpillInPlanTree (E9/E10) -------------------------------------------------------

    [Fact]
    public void HasSpillInPlanTree_NullRoot_ReturnsFalse()
    {
        Assert.False(PlanDisplayText.HasSpillInPlanTree(null));
    }

    [Fact]
    public void HasSpillInPlanTree_NoWarningsAnywhere_ReturnsFalse()
    {
        var root = new PlanNode { Children = { new PlanNode() } };
        Assert.False(PlanDisplayText.HasSpillInPlanTree(root));
    }

    [Fact]
    public void HasSpillInPlanTree_WarningOnRootNotSpillSuffixed_ReturnsFalse()
    {
        var root = new PlanNode();
        root.Warnings.Add(new PlanWarning { WarningType = "No Join Predicate" });
        Assert.False(PlanDisplayText.HasSpillInPlanTree(root));
    }

    [Fact]
    public void HasSpillInPlanTree_WarningOnRoot_ReturnsTrue()
    {
        var root = new PlanNode();
        root.Warnings.Add(new PlanWarning { WarningType = "Hash Spill" });
        Assert.True(PlanDisplayText.HasSpillInPlanTree(root));
    }

    [Fact]
    public void HasSpillInPlanTree_WarningDeepInChildTree_ReturnsTrue()
    {
        var grandchild = new PlanNode();
        grandchild.Warnings.Add(new PlanWarning { WarningType = "Sort Spill" });
        var child = new PlanNode { Children = { grandchild } };
        var root = new PlanNode { Children = { child } };
        Assert.True(PlanDisplayText.HasSpillInPlanTree(root));
    }

    // --- EfficiencyColorKey (C1: >=40 default, 20-39 warning, <20 error) -------------------

    [Theory]
    [InlineData(100, null)]
    [InlineData(40, null)]
    [InlineData(39.9, "WarningBrush")]
    [InlineData(20, "WarningBrush")]
    [InlineData(19.9, "ErrorBrush")]
    [InlineData(0, "ErrorBrush")]
    public void EfficiencyColorKey_MatchesPsThresholds(double pct, string? expected)
    {
        Assert.Equal(expected, PlanDisplayText.EfficiencyColorKey(pct));
    }

    // --- MemoryGrantColorKey (E8/E9: >100% always error; spill forces >= warning) ----------

    [Fact]
    public void MemoryGrantColorKey_OverUsed_IsErrorEvenWithoutSpill()
    {
        Assert.Equal("ErrorBrush", PlanDisplayText.MemoryGrantColorKey(100.1, hasSpill: false));
    }

    [Fact]
    public void MemoryGrantColorKey_OverUsedWithSpill_IsStillError()
    {
        Assert.Equal("ErrorBrush", PlanDisplayText.MemoryGrantColorKey(150, hasSpill: true));
    }

    [Fact]
    public void MemoryGrantColorKey_HighUtilizationWithSpill_IsForcedToWarning()
    {
        // 60% utilized would be the default (good) tier on its own, but a plan-wide spill
        // forces at best the warning tier (#215 E9).
        Assert.Equal("WarningBrush", PlanDisplayText.MemoryGrantColorKey(60, hasSpill: true));
    }

    [Fact]
    public void MemoryGrantColorKey_HighUtilizationNoSpill_IsDefault()
    {
        Assert.Null(PlanDisplayText.MemoryGrantColorKey(60, hasSpill: false));
    }

    [Fact]
    public void MemoryGrantColorKey_LowUtilizationNoSpill_IsError()
    {
        Assert.Equal("ErrorBrush", PlanDisplayText.MemoryGrantColorKey(10, hasSpill: false));
    }

    // --- BuildRuntimeSummaryRows: row order (E11) ------------------------------------------

    [Fact]
    public void BuildRuntimeSummaryRows_ActualPlan_OrdersElapsedCpuElapsedDopCpuCompileMemoryCeOptEarlyAbort()
    {
        var stmt = Statement();
        stmt.QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = 1000, CpuTimeMs = 2000 };
        stmt.DegreeOfParallelism = 4;
        stmt.CompileTimeMs = 5;
        stmt.MemoryGrant = new MemoryGrantInfo { GrantedMemoryKB = 1024, MaxUsedMemoryKB = 512 };
        stmt.StatementOptmLevel = "FULL";
        stmt.StatementOptmEarlyAbortReason = "TimeOut";
        stmt.CardinalityEstimationModelVersion = 160;

        var labels = PlanDisplayText.BuildRuntimeSummaryRows(stmt).Select(r => r.Label).ToArray();

        // #4836 (PerformanceStudio#613/#614): CE model moved above Optimization, and Early abort
        // follows Optimization, so Optimization and its reason end the card.
        Assert.Equal(
            new[] { "Elapsed", "CPU:Elapsed", "DOP", "CPU", "Compile", "Memory grant", "CE model", "Optimization", "Early abort" },
            labels);
    }

    // --- #4836: the early abort reason nests under Optimization ----------------------------

    [Fact]
    public void BuildRuntimeSummaryRows_EarlyAbortWithOptimizationLevel_IsNestedAndNothingElseIs()
    {
        var stmt = Statement();
        stmt.QueryTimeStats = new QueryTimeInfo { ElapsedTimeMs = 1000, CpuTimeMs = 2000 };
        stmt.StatementOptmLevel = "FULL";
        stmt.StatementOptmEarlyAbortReason = "GoodEnoughPlanFound";
        stmt.CardinalityEstimationModelVersion = 160;

        var rows = PlanDisplayText.BuildRuntimeSummaryRows(stmt);

        var earlyAbort = rows.Single(r => r.Label == "Early abort");
        Assert.Equal("GoodEnoughPlanFound", earlyAbort.Value);
        Assert.True(earlyAbort.Nested);
        Assert.False(rows.Single(r => r.Label == "Optimization").Nested);
        Assert.All(rows.Where(r => r.Label != "Early abort"), r => Assert.False(r.Nested, r.Label));
    }

    [Fact]
    public void BuildRuntimeSummaryRows_EarlyAbortWithoutOptimizationLevel_IsNotNested()
    {
        var stmt = Statement();
        stmt.StatementOptmEarlyAbortReason = "TimeOut";
        stmt.CardinalityEstimationModelVersion = 160;

        var rows = PlanDisplayText.BuildRuntimeSummaryRows(stmt);

        // No Optimization row to sit under, so the reason is a plain row, still after CE model.
        Assert.Equal(new[] { "CE model", "Early abort" }, rows.Select(r => r.Label).ToArray());
        Assert.All(rows, r => Assert.False(r.Nested, r.Label));
    }

    [Fact]
    public void BuildRuntimeSummaryRows_EstimatedPlan_SkipsRuntimeOnlyRows()
    {
        var stmt = Statement();
        stmt.DegreeOfParallelism = 2;
        stmt.NonParallelPlanReason = null;

        var labels = PlanDisplayText.BuildRuntimeSummaryRows(stmt).Select(r => r.Label).ToArray();

        // No QueryTimeStats: Elapsed/CPU:Elapsed/CPU are absent, and DOP has no efficiency
        // suffix (no CPU/elapsed to compute a speedup from).
        Assert.Equal(new[] { "DOP" }, labels);
        Assert.Equal("2", PlanDisplayText.BuildRuntimeSummaryRows(stmt)[0].Value);
    }

    [Fact]
    public void BuildRuntimeSummaryRows_MemoryGrantRow_CarriesItsColorAndSpillTag()
    {
        var root = new PlanNode();
        root.Warnings.Add(new PlanWarning { WarningType = "Hash Spill" });

        var stmt = Statement();
        stmt.RootNode = root;
        stmt.MemoryGrant = new MemoryGrantInfo { GrantedMemoryKB = 1024, MaxUsedMemoryKB = 600 };

        var row = PlanDisplayText.BuildRuntimeSummaryRows(stmt).Single(r => r.Label == "Memory grant");

        Assert.Equal("WarningBrush", row.ColorKey);
        Assert.Contains("\u26a0 spill", row.Value);
    }

    // --- #4628: GrantedMemory="0" must not print a fake 100% ------------------------------

    [Fact]
    public void BuildRuntimeSummaryRows_MemoryGrantRow_RealGrant_KeepsGrantedUsedPercentText()
    {
        var stmt = Statement();
        stmt.MemoryGrant = new MemoryGrantInfo { GrantedMemoryKB = 1024, MaxUsedMemoryKB = 512 };

        var row = PlanDisplayText.BuildRuntimeSummaryRows(stmt).Single(r => r.Label == "Memory grant");

        Assert.Equal("1.0 MB granted, 512 KB used (50%)", row.Value);
    }

    [Fact]
    public void BuildRuntimeSummaryRows_MemoryGrantRow_ZeroGrant_SaysNoGrantWithNoPercent()
    {
        var stmt = Statement();
        stmt.MemoryGrant = new MemoryGrantInfo { GrantedMemoryKB = 0, MaxUsedMemoryKB = 0 };

        var row = PlanDisplayText.BuildRuntimeSummaryRows(stmt).Single(r => r.Label == "Memory grant");

        Assert.Equal("No memory grant", row.Value);
        Assert.DoesNotContain("%", row.Value);
        Assert.Null(row.ColorKey);
    }

    [Fact]
    public void BuildRuntimeSummaryRows_CompileTime_AlwaysShownRegardlessOfOtherStats()
    {
        var stmt = Statement();
        stmt.CompileTimeMs = 7;

        var row = PlanDisplayText.BuildRuntimeSummaryRows(stmt).Single(r => r.Label == "Compile");
        Assert.Equal("7ms", row.Value);
    }
}
