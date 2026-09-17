/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared persistence/hysteresis primitive (#3282): fire only after N consecutive breaching samples,
/// resolve only after M consecutive clears, and never flap in between. Pure, so these are exhaustive
/// sequence tests with no store or clock.
/// </summary>
public class AlertPersistenceGateTests
{
    /// <summary>Threads a breach/clear sequence through the gate, returning the outcome at each step.</summary>
    private static List<PersistenceOutcome> Run(int breachSamples, int clearSamples, params bool[] breaching)
    {
        var state = PersistenceState.Initial;
        var outcomes = new List<PersistenceOutcome>();
        foreach (var b in breaching)
        {
            var evaluation = AlertPersistenceGate.Evaluate(state, b, breachSamples, clearSamples);
            state = evaluation.State;
            outcomes.Add(evaluation.Outcome);
        }

        return outcomes;
    }

    [Fact]
    public void NoHysteresis_FiresOnFirstBreach()
    {
        var outcomes = Run(breachSamples: 1, clearSamples: 1, true);
        Assert.Equal(new[] { PersistenceOutcome.Fire }, outcomes);
    }

    [Fact]
    public void BreachSamplesThree_FiresOnlyOnTheThirdConsecutiveBreach()
    {
        var outcomes = Run(breachSamples: 3, clearSamples: 1, true, true, true, true);
        Assert.Equal(
            new[] { PersistenceOutcome.None, PersistenceOutcome.None, PersistenceOutcome.Fire, PersistenceOutcome.None },
            outcomes);
    }

    [Fact]
    public void AClearBeforeTheThreshold_ResetsTheBreachStreak()
    {
        // breach, breach, clear (resets), breach, breach -> still only 2 in a row, never reaches 3.
        var outcomes = Run(breachSamples: 3, clearSamples: 1, true, true, false, true, true);
        Assert.DoesNotContain(PersistenceOutcome.Fire, outcomes);
    }

    [Fact]
    public void OnceFiring_FurtherBreachesDoNotRefire()
    {
        var outcomes = Run(breachSamples: 2, clearSamples: 2, true, true, true, true, true);
        Assert.Equal(PersistenceOutcome.Fire, outcomes[1]);
        Assert.All(outcomes.GetRange(2, 3), o => Assert.Equal(PersistenceOutcome.None, o));
    }

    [Fact]
    public void ResolvesOnlyAfterClearSamplesConsecutiveClears()
    {
        // Fire at sample 2, then two clears required to resolve.
        var outcomes = Run(breachSamples: 2, clearSamples: 2, true, true, false, false);
        Assert.Equal(
            new[] { PersistenceOutcome.None, PersistenceOutcome.Fire, PersistenceOutcome.None, PersistenceOutcome.Resolve },
            outcomes);
    }

    [Fact]
    public void ABreachBeforeTheClearThreshold_KeepsTheIncidentOpen()
    {
        // Fire, clear (1/2), breach (resets clears), clear (1/2 again) -> never resolves.
        var outcomes = Run(breachSamples: 1, clearSamples: 2, true, false, true, false);
        Assert.Equal(PersistenceOutcome.Fire, outcomes[0]);
        Assert.DoesNotContain(PersistenceOutcome.Resolve, outcomes);
    }

    [Fact]
    public void OnceResolved_FurtherClearsAreNoChange()
    {
        var outcomes = Run(breachSamples: 1, clearSamples: 1, true, false, false, false);
        Assert.Equal(
            new[] { PersistenceOutcome.Fire, PersistenceOutcome.Resolve, PersistenceOutcome.None, PersistenceOutcome.None },
            outcomes);
    }

    [Fact]
    public void NeverBreaching_NeverFires()
    {
        var outcomes = Run(breachSamples: 1, clearSamples: 1, false, false, false);
        Assert.All(outcomes, o => Assert.Equal(PersistenceOutcome.None, o));
    }

    [Fact]
    public void AFullFireResolveCycle_CanFireAgain()
    {
        // fire (2 breaches), resolve (1 clear), then fire again on the next 2 breaches.
        var outcomes = Run(breachSamples: 2, clearSamples: 1, true, true, false, true, true);
        Assert.Equal(
            new[]
            {
                PersistenceOutcome.None, PersistenceOutcome.Fire, PersistenceOutcome.Resolve,
                PersistenceOutcome.None, PersistenceOutcome.Fire,
            },
            outcomes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void NonPositiveSampleCounts_ClampToOne_FiringOnFirstBreach(int degenerate)
    {
        var evaluation = AlertPersistenceGate.Evaluate(PersistenceState.Initial, breaching: true, degenerate, degenerate);
        Assert.Equal(PersistenceOutcome.Fire, evaluation.Outcome);
    }

    [Fact]
    public void BreachCounter_IsCappedAtThreshold_NoUnboundedGrowth()
    {
        // A long-lived firing incident must not overflow the persisted counter.
        var state = PersistenceState.Initial;
        for (var i = 0; i < 1000; i++)
        {
            state = AlertPersistenceGate.Evaluate(state, breaching: true, breachSamples: 3, clearSamples: 2).State;
        }

        Assert.Equal(3, state.ConsecutiveBreaches);
        Assert.True(state.Firing);
    }
}
