/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4579: plan-edge coloring by the child operator's actual-vs-estimated row ratio, matching
/// erikdarlingdata/PerformanceStudio dev's <c>GetLinkColorBrush</c>. <see cref="PlanEdgeColour"/> is a
/// new type, so every assertion here is a compile-only RED against dev.
/// </summary>
public sealed class Viewer4579Tests
{
    private const double Limit = 10.0;

    [Fact]
    public void EstimatedPlan_AlwaysNeutral()
    {
        // hasActualStats = false, even with a wildly diverging ratio, stays neutral (estimated plans
        // never get accuracy-based color).
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(false, 1_000_000, 1, 1, Limit));
    }

    [Fact]
    public void ActualPlan_ExactMatch_IsNeutral()
    {
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(true, 100, 1, 100, Limit));
    }

    [Fact]
    public void ActualPlan_ZeroEstimate_ZeroActual_IsNeutral()
    {
        // estRows == 0 and actualRows == 0 => ratio treated as 1.0 (PS's exact fallback).
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(true, 0, 1, 0, Limit));
    }

    [Fact]
    public void ActualPlan_ZeroEstimate_NonZeroActual_IsFluoRed()
    {
        // estRows == 0 and actualRows > 0 => ratio treated as double.MaxValue, the top underestimate tier.
        Assert.Equal(PlanEdgeColourKey.FluoRed, PlanEdgeColour.ForChild(true, 5, 1, 0, Limit));
    }

    // ---- Underestimate side (accuracyRatio > 1: more actual rows than estimated) ----

    [Theory]
    [InlineData(9.9)]   // just inside the neutral band
    [InlineData(1.0 / 9.9)]
    public void ActualPlan_JustInsideNeutralBand_IsNeutral(double ratio)
    {
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(true, ratio * 100, 1, 100, Limit));
    }

    [Fact]
    public void ActualPlan_AtLimit_IsNeutral()
    {
        // accuracyRatio == limit is still inside the closed neutral band ([1/limit, limit]).
        Assert.Equal(PlanEdgeColourKey.Neutral, PlanEdgeColour.ForChild(true, Limit * 100, 1, 100, Limit));
    }

    [Fact]
    public void ActualPlan_JustAboveLimit_IsLightOrange()
    {
        Assert.Equal(PlanEdgeColourKey.LightOrange, PlanEdgeColour.ForChild(true, Limit * 100 + 1, 1, 100, Limit));
    }

    [Fact]
    public void ActualPlan_AtLimitTimes10_IsFluoOrange()
    {
        Assert.Equal(PlanEdgeColourKey.FluoOrange, PlanEdgeColour.ForChild(true, Limit * 10 * 100, 1, 100, Limit));
    }

    [Fact]
    public void ActualPlan_JustBelowLimitTimes10_IsLightOrange()
    {
        Assert.Equal(PlanEdgeColourKey.LightOrange, PlanEdgeColour.ForChild(true, (Limit * 10 - 0.001) * 100, 1, 100, Limit));
    }

    [Fact]
    public void ActualPlan_AtLimitTimes100_IsFluoRed()
    {
        Assert.Equal(PlanEdgeColourKey.FluoRed, PlanEdgeColour.ForChild(true, Limit * 100 * 100, 1, 100, Limit));
    }

    [Fact]
    public void ActualPlan_JustBelowLimitTimes100_IsFluoOrange()
    {
        Assert.Equal(PlanEdgeColourKey.FluoOrange, PlanEdgeColour.ForChild(true, (Limit * 100 - 0.001) * 100, 1, 100, Limit));
    }

    // ---- Overestimate side (accuracyRatio < 1: fewer actual rows than estimated) ----

    [Fact]
    public void ActualPlan_JustBelowInverseLimit_IsBlue()
    {
        Assert.Equal(PlanEdgeColourKey.Blue, PlanEdgeColour.ForChild(true, 100, 1, Limit * 100 + 1, Limit));
    }

    [Fact]
    public void ActualPlan_AtInverseLimitTimes10_IsBlue()
    {
        // Overestimate tiers use a strict '<' boundary (PS's exact form), so exactly at the tier
        // boundary the ratio has not yet crossed into the next tier.
        Assert.Equal(PlanEdgeColourKey.Blue, PlanEdgeColour.ForChild(true, 100, 1, Limit * 10 * 100, Limit));
    }

    [Fact]
    public void ActualPlan_JustAboveInverseLimitTimes10_IsBlue()
    {
        Assert.Equal(PlanEdgeColourKey.Blue, PlanEdgeColour.ForChild(true, 100, 1, (Limit * 10 - 0.001) * 100, Limit));
    }

    [Fact]
    public void ActualPlan_AtInverseLimitTimes100_IsLightBlue()
    {
        // Same strict '<' boundary behavior at the top tier.
        Assert.Equal(PlanEdgeColourKey.LightBlue, PlanEdgeColour.ForChild(true, 100, 1, Limit * 100 * 100, Limit));
    }

    [Fact]
    public void ActualPlan_JustAboveInverseLimitTimes100_IsLightBlue()
    {
        Assert.Equal(PlanEdgeColourKey.LightBlue, PlanEdgeColour.ForChild(true, 100, 1, (Limit * 100 - 0.001) * 100, Limit));
    }

    // ---- Divergence-limit floor ----

    [Fact]
    public void DivergenceLimit_BelowFloor_IsClampedToTwo()
    {
        // limit = 1 is floored to 2.0 (PS's Math.Max(2.0, ...)); a ratio of 3 is then past the floored
        // limit, not inside a (nonsensical) [1, 1] neutral band.
        Assert.Equal(PlanEdgeColourKey.LightOrange, PlanEdgeColour.ForChild(true, 300, 1, 100, 1.0));
    }

    [Fact]
    public void DefaultDivergenceLimit_MatchesPerformanceStudio()
    {
        Assert.Equal(10.0, PlanEdgeColour.DefaultDivergenceLimit);
    }

    [Fact]
    public void MinDivergenceLimit_MatchesPerformanceStudio()
    {
        Assert.Equal(2.0, PlanEdgeColour.MinDivergenceLimit);
    }
}
