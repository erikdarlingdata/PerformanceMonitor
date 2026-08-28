/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Decision-table pins for <see cref="QueryStorePlanXmlState.Learn"/> (#2312 Finding 1) — the fold
/// that finally wires the adaptive candidate sizing its own doc comment promised. The stakes, from
/// the field: sizing every pass from the 160KB seed put K at ~118 on every database, and on the
/// churn-heavy ones the fetch decompressed that window every cycle whether the database's real
/// average was 15KB or 162KB.
/// </summary>
public sealed class QueryStorePlanSizeLearnTests
{
    /// <summary>
    /// An empty pass proves the walk CAUGHT UP (nothing qualified past the watermark) but teaches
    /// nothing about plan size — catch-up clears, the carried average stands.
    /// </summary>
    [Fact]
    public void AnEmptyPassClearsCatchUpAndKeepsTheAverage()
    {
        var previous = new QueryStorePlanXmlState.PlanSizeEstimate(40_000, CatchUpInProgress: true);

        var next = QueryStorePlanXmlState.Learn(previous, bytesShipped: 0, plansShipped: 0, plansMeasured: 0, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.Equal(40_000, next.AvgBytes);
        Assert.False(next.CatchUpInProgress);
    }

    /// <summary>A pass that consumed its whole candidate window proves a backlog remains.</summary>
    [Fact]
    public void AFullWindowPassSetsCatchUp()
    {
        var next = QueryStorePlanXmlState.Learn(
            default, bytesShipped: 1_180_000, plansShipped: 118, plansMeasured: 118, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.True(next.CatchUpInProgress);
        Assert.Equal(10_000, next.AvgBytes);
    }

    /// <summary>
    /// A pass cut by the byte budget proves the same thing from the other bound — the shipped set
    /// can overshoot the budget by up to one plan (the predicate admits the plan that crosses the
    /// line), so >= is the correct comparison.
    /// </summary>
    [Fact]
    public void ABudgetCutPassSetsCatchUp()
    {
        var next = QueryStorePlanXmlState.Learn(
            default, bytesShipped: 13_000_000, plansShipped: 80, plansMeasured: 80, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.True(next.CatchUpInProgress);
        Assert.Equal(162_500, next.AvgBytes);
    }

    /// <summary>An ordinary partial pass learns its average and clears catch-up.</summary>
    [Fact]
    public void AnOrdinaryPassLearnsAndClearsCatchUp()
    {
        var previous = new QueryStorePlanXmlState.PlanSizeEstimate(160_000, CatchUpInProgress: true);

        var next = QueryStorePlanXmlState.Learn(
            previous, bytesShipped: 450_000, plansShipped: 30, plansMeasured: 30, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.Equal(15_000, next.AvgBytes);
        Assert.False(next.CatchUpInProgress);
    }

    /// <summary>
    /// Zero bytes with a nonzero count (every plan NULL — possible, plan XML can be unavailable)
    /// must not zero the carried average: ObservedAvgPlanBytes yields null there and the previous
    /// estimate stands, exactly like the quiet pass.
    /// </summary>
    [Fact]
    public void AnAllNullPassKeepsThePreviousAverage()
    {
        var previous = new QueryStorePlanXmlState.PlanSizeEstimate(52_000, CatchUpInProgress: false);

        var next = QueryStorePlanXmlState.Learn(
            previous, bytesShipped: 0, plansShipped: 5, plansMeasured: 0, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.Equal(52_000, next.AvgBytes);
    }

    /// <summary>
    /// The review catch, pinned: NULL-XML plans ship as rows (the watermark must pass unpersistable
    /// plans) so they count for the window comparison — but averaging real bytes over a NULL-inflated
    /// divisor would understate plan size and INFLATE the next window, the unsafe direction. A pass
    /// of 10 rows where only 5 carried XML averages over 5.
    /// </summary>
    [Fact]
    public void AMixedPassAveragesOverOnlyTheMeasuredPlans()
    {
        var next = QueryStorePlanXmlState.Learn(
            default, bytesShipped: 500_000, plansShipped: 10, plansMeasured: 5, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.Equal(100_000, next.AvgBytes);
    }

    /// <summary>
    /// The never-learned default round-trips as "pass null to CandidatePlanCount": zero AvgBytes is
    /// the sentinel the call site tests, so the seed applies exactly until the first real sample.
    /// </summary>
    [Fact]
    public void TheDefaultEstimateMeansNeverLearned()
    {
        QueryStorePlanXmlState.PlanSizeEstimate estimate = default;

        Assert.Equal(0, estimate.AvgBytes);
        Assert.False(estimate.CatchUpInProgress);
        Assert.Equal(0, estimate.ClampedStreak);
    }

    /* ---------- #2683: runaway back-off (a store whose fetch never drains) ---------- */

    /// <summary>Each consecutive catch-up (clamped) pass increments the runaway streak.</summary>
    [Fact]
    public void ConsecutiveCatchUpPassesGrowTheClampedStreak()
    {
        var e = new QueryStorePlanXmlState.PlanSizeEstimate(15_000, CatchUpInProgress: true, ClampedStreak: 5);

        // A full-window pass = still clamped -> streak advances.
        var next = QueryStorePlanXmlState.Learn(
            e, bytesShipped: 30_000_000, plansShipped: 2048, plansMeasured: 2048, candidateWindow: 2048, budgetBytes: 33_554_432);

        Assert.True(next.CatchUpInProgress);
        Assert.Equal(6, next.ClampedStreak);
    }

    /// <summary>A pass that finally DRAINS (partial, under budget) resets the streak — a legit heavy store is never throttled.</summary>
    [Fact]
    public void ADrainedPassResetsTheClampedStreak()
    {
        var e = new QueryStorePlanXmlState.PlanSizeEstimate(15_000, CatchUpInProgress: true, ClampedStreak: 30);

        // Shipped fewer than the window and under budget -> caught up.
        var next = QueryStorePlanXmlState.Learn(
            e, bytesShipped: 900_000, plansShipped: 60, plansMeasured: 60, candidateWindow: 2048, budgetBytes: 33_554_432);

        Assert.False(next.CatchUpInProgress);
        Assert.Equal(0, next.ClampedStreak);
    }

    /// <summary>An empty pass also resets the streak (it proves the fetch caught up).</summary>
    [Fact]
    public void AnEmptyPassResetsTheClampedStreak()
    {
        var e = new QueryStorePlanXmlState.PlanSizeEstimate(15_000, CatchUpInProgress: true, ClampedStreak: 40);

        var next = QueryStorePlanXmlState.Learn(
            e, bytesShipped: 0, plansShipped: 0, plansMeasured: 0, candidateWindow: 2048, budgetBytes: 33_554_432);

        Assert.Equal(0, next.ClampedStreak);
    }

    /// <summary>
    /// The back-off itself. The expensive AYR-shaped passes — the ones that clamp to the full 2048 — run with
    /// catch-up NOT in progress (a small learned average is trusted, not seed-floored), so wanted greatly
    /// exceeds the cap. Below the streak threshold that lands at MaxCandidatePlans; at/after the threshold the
    /// ceiling drops to RunawayCandidatePlans. (With catch-up in progress the seed-floor already shrinks
    /// wanted, so the ceiling is moot — the back-off targets exactly the passes that were costing 2048.)
    /// </summary>
    [Fact]
    public void PastTheStreakThreshold_TheCeilingDropsToRunawayCandidatePlans()
    {
        // ~15 KB plans (trusted, not catching up), 32 MB budget -> wants ~3277, well past the full cap.
        long avg = 15 * 1024, budget = 32L * 1024 * 1024;

        var belowThreshold = QueryStorePlanXmlState.CandidatePlanCount(
            avg, budget, catchUpInProgress: false, out var clampedBelow,
            clampedStreak: QueryStorePlanXmlState.RunawayClampedStreakThreshold - 1);
        Assert.Equal(QueryStorePlanXmlState.MaxCandidatePlans, belowThreshold);
        Assert.True(clampedBelow);

        var runaway = QueryStorePlanXmlState.CandidatePlanCount(
            avg, budget, catchUpInProgress: false, out var clampedRunaway,
            clampedStreak: QueryStorePlanXmlState.RunawayClampedStreakThreshold);
        Assert.Equal(QueryStorePlanXmlState.RunawayCandidatePlans, runaway);
        Assert.True(clampedRunaway);

        Assert.True(runaway < belowThreshold, "runaway back-off must reduce the per-pass cap");
    }
}
