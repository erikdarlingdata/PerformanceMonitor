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

        var next = QueryStorePlanXmlState.Learn(previous, bytesShipped: 0, plansShipped: 0, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.Equal(40_000, next.AvgBytes);
        Assert.False(next.CatchUpInProgress);
    }

    /// <summary>A pass that consumed its whole candidate window proves a backlog remains.</summary>
    [Fact]
    public void AFullWindowPassSetsCatchUp()
    {
        var next = QueryStorePlanXmlState.Learn(
            default, bytesShipped: 1_180_000, plansShipped: 118, candidateWindow: 118, budgetBytes: 12_582_912);

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
            default, bytesShipped: 13_000_000, plansShipped: 80, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.True(next.CatchUpInProgress);
        Assert.Equal(162_500, next.AvgBytes);
    }

    /// <summary>An ordinary partial pass learns its average and clears catch-up.</summary>
    [Fact]
    public void AnOrdinaryPassLearnsAndClearsCatchUp()
    {
        var previous = new QueryStorePlanXmlState.PlanSizeEstimate(160_000, CatchUpInProgress: true);

        var next = QueryStorePlanXmlState.Learn(
            previous, bytesShipped: 450_000, plansShipped: 30, candidateWindow: 118, budgetBytes: 12_582_912);

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
            previous, bytesShipped: 0, plansShipped: 5, candidateWindow: 118, budgetBytes: 12_582_912);

        Assert.Equal(52_000, next.AvgBytes);
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
    }
}
