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
/// Pins for the query_store fetch backoff (#2776). The defect these guard against: a fetch whose STORE
/// write timed out kept its whole carry-over set and re-attempted the identical width next cycle, so a
/// database re-paid full plan-XML decompression on the target every cycle forever. Measured on
/// prod-pos-use1-monitor-01 over 14.9h: 125 plan-fetch failures, 16 text-fetch failures, and
/// "candidate cap clamped to 512" 2,965 times — a missing set pinned at maximum that never drained.
///
/// <para>The two failure modes worth testing FOR, because either would be worse than no backoff at all:
/// a backoff that never engages (the bug survives, now with a comment claiming it does not) and one that
/// gives up permanently (a transient store stall becomes a restart-only outage — a shape this codebase
/// has been bitten by before). Every test below exists to pin one or the other.</para>
/// </summary>
public sealed class QueryStoreFetchBackoffTests
{
    /// <summary>
    /// Zero failures must be INERT. Every healthy database on the fleet takes this path every cycle, so a
    /// backoff that narrowed even slightly at zero would be a fleet-wide throughput cut shipped as a
    /// reliability fix.
    /// </summary>
    [Theory]
    [InlineData(512)]
    [InlineData(118)]
    [InlineData(32)]
    [InlineData(1)]
    public void ZeroFailuresLeavesTheWidthUntouched(int fullWidth)
    {
        Assert.Equal(fullWidth, QueryStorePlanXmlState.NarrowForFailures(fullWidth, consecutiveFetchFailures: 0));
    }

    /// <summary>
    /// The backoff ENGAGES, and halves per consecutive failure. 512 is the observed real-world width —
    /// the cap was clamped there 2,965 times in one day — so this is the actual sequence a failing
    /// database walks down.
    /// </summary>
    [Theory]
    [InlineData(1, 256)]
    [InlineData(2, 128)]
    [InlineData(3, 64)]
    [InlineData(4, 32)]
    public void EachConsecutiveFailureHalvesTheWidth(int failures, int expected)
    {
        Assert.Equal(expected, QueryStorePlanXmlState.NarrowForFailures(512, failures));
    }

    /// <summary>
    /// It NARROWS rather than stopping dead. At and past saturation the width is the same floor the
    /// candidate cap already uses, never zero — a backed-off database still attempts real work every
    /// cycle, which is what lets it recover with no operator action and no restart.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(50)]
    [InlineData(int.MaxValue)]
    public void SaturatedBackoffFloorsAtMinCandidatePlansAndNeverStops(int failures)
    {
        var width = QueryStorePlanXmlState.NarrowForFailures(512, failures);

        Assert.Equal(QueryStorePlanXmlState.MinCandidatePlans, width);
        Assert.True(width > 0, "a backed-off database must still attempt work — a zero width is a give-up latch");
    }

    /// <summary>
    /// A width already at or below the floor is left alone rather than driven under it. The text fetch
    /// passes its raw missing-set count here, which on a nearly-drained database is a handful of ids.
    /// </summary>
    [Theory]
    [InlineData(32, 4)]
    [InlineData(10, 4)]
    [InlineData(1, 4)]
    public void AWidthAtOrBelowTheFloorIsNotNarrowedFurther(int fullWidth, int failures)
    {
        Assert.Equal(fullWidth, QueryStorePlanXmlState.NarrowForFailures(fullWidth, failures));
    }

    /// <summary>
    /// Narrowing is monotonic: more consecutive failures never produces a WIDER attempt. Guards against
    /// an off-by-one or overflow in the halving loop handing a failing database a bigger write than the
    /// one that just timed out.
    /// </summary>
    [Fact]
    public void NarrowingIsMonotonicAcrossTheWholeFailureRange()
    {
        var previous = QueryStorePlanXmlState.NarrowForFailures(512, 0);

        for (var failures = 1; failures <= 10; failures++)
        {
            var width = QueryStorePlanXmlState.NarrowForFailures(512, failures);
            Assert.True(
                width <= previous,
                $"width grew from {previous} to {width} at {failures} consecutive failures");
            previous = width;
        }
    }

    /// <summary>
    /// A failure advances the counter and, critically, leaves the SIZE estimate exactly as it was: a pass
    /// that threw measured nothing, so folding a fake measurement in would corrupt the next cap.
    /// </summary>
    [Fact]
    public void RecordFetchFailureAdvancesTheCountAndPreservesTheEstimate()
    {
        var previous = new QueryStorePlanXmlState.PlanSizeEstimate(52_000, CatchUpInProgress: true);

        var next = QueryStorePlanXmlState.RecordFetchFailure(previous);

        Assert.Equal(1, next.ConsecutiveFetchFailures);
        Assert.Equal(52_000, next.AvgBytes);
        Assert.True(next.CatchUpInProgress);
    }

    /// <summary>
    /// The counter saturates instead of growing without bound. A database failing for a week should read
    /// "at the floor", not carry a five-figure counter that means the same thing.
    /// </summary>
    [Fact]
    public void RecordFetchFailureSaturatesAtMaxBackoffHalvings()
    {
        var estimate = default(QueryStorePlanXmlState.PlanSizeEstimate);

        for (var pass = 0; pass < 25; pass++)
        {
            estimate = QueryStorePlanXmlState.RecordFetchFailure(estimate);
        }

        Assert.Equal(QueryStorePlanXmlState.MaxBackoffHalvings, estimate.ConsecutiveFetchFailures);
    }

    /// <summary>
    /// SUCCESS RESETS TO FULL WIDTH — the anti-latch property. A database that recovers gets its whole
    /// width back on the very next pass rather than crawling up, and the size estimate it learned along
    /// the way survives the reset.
    /// </summary>
    [Fact]
    public void RecordFetchSuccessRestoresFullWidthImmediately()
    {
        var backedOff = new QueryStorePlanXmlState.PlanSizeEstimate(
            48_000, CatchUpInProgress: true, ConsecutiveFetchFailures: QueryStorePlanXmlState.MaxBackoffHalvings);

        Assert.Equal(
            QueryStorePlanXmlState.MinCandidatePlans,
            QueryStorePlanXmlState.NarrowForFailures(512, backedOff.ConsecutiveFetchFailures));

        var recovered = QueryStorePlanXmlState.RecordFetchSuccess(backedOff);

        Assert.Equal(0, recovered.ConsecutiveFetchFailures);
        Assert.Equal(512, QueryStorePlanXmlState.NarrowForFailures(512, recovered.ConsecutiveFetchFailures));
        Assert.Equal(48_000, recovered.AvgBytes);
    }

    /// <summary>
    /// The whole cycle end to end: fail down to the floor, succeed once, back to full width. This is the
    /// property that makes the backoff self-healing rather than a latch, and it is the one a future
    /// refactor is most likely to break without noticing.
    /// </summary>
    [Fact]
    public void TheBackoffIsSelfHealingAcrossAFailThenRecoverSequence()
    {
        var estimate = default(QueryStorePlanXmlState.PlanSizeEstimate);
        var widths = new int[6];

        for (var pass = 0; pass < 5; pass++)
        {
            widths[pass] = QueryStorePlanXmlState.NarrowForFailures(512, estimate.ConsecutiveFetchFailures);
            estimate = QueryStorePlanXmlState.RecordFetchFailure(estimate);
        }

        estimate = QueryStorePlanXmlState.RecordFetchSuccess(estimate);
        widths[5] = QueryStorePlanXmlState.NarrowForFailures(512, estimate.ConsecutiveFetchFailures);

        Assert.Equal(new[] { 512, 256, 128, 64, 32, 512 }, widths);
    }

    /// <summary>
    /// <see cref="QueryStorePlanXmlState.Learn"/> must CARRY the failure count through, not reset it.
    /// Learn runs mid-pass, before the store write that is the likeliest thing to throw — so a Learn that
    /// returned a fresh record would clear the backoff on exactly the pass about to fail, and the
    /// narrowing would never engage. This is the subtlest way the whole feature could silently no-op.
    /// </summary>
    [Fact]
    public void LearnPreservesTheFailureCountSoTheBackoffCannotSilentlyReset()
    {
        var backedOff = new QueryStorePlanXmlState.PlanSizeEstimate(
            30_000, CatchUpInProgress: false, ConsecutiveFetchFailures: 3);

        var learned = QueryStorePlanXmlState.Learn(
            backedOff, bytesShipped: 900_000, plansShipped: 30, plansMeasured: 30, candidateWindow: 64, budgetBytes: 12_582_912);

        Assert.Equal(3, learned.ConsecutiveFetchFailures);
    }

    /// <summary>
    /// The defaulted parameter keeps every pre-#2776 construction site meaning "no failures", so the
    /// backoff is opt-in by failure rather than something existing state accidentally carries.
    /// </summary>
    [Fact]
    public void ADefaultEstimateCarriesNoFailures()
    {
        QueryStorePlanXmlState.PlanSizeEstimate estimate = default;

        Assert.Equal(0, estimate.ConsecutiveFetchFailures);
        Assert.Equal(512, QueryStorePlanXmlState.NarrowForFailures(512, estimate.ConsecutiveFetchFailures));
    }

    /// <summary>
    /// The halving count and the floor have to agree: exactly <see cref="QueryStorePlanXmlState.MaxBackoffHalvings"/>
    /// halvings must take the ceiling to the floor. If either constant moves without the other, the
    /// counter either saturates before reaching the floor (backoff stops short) or keeps counting after
    /// it (a meaningless number). Derived from the constants rather than hard-coded so they cannot drift.
    /// </summary>
    [Fact]
    public void MaxBackoffHalvingsIsExactlyWhatTakesTheCeilingToTheFloor()
    {
        var width = QueryStorePlanXmlState.MaxCandidatePlans;
        var halvings = 0;

        while (width > QueryStorePlanXmlState.MinCandidatePlans)
        {
            width /= 2;
            halvings++;
        }

        Assert.Equal(QueryStorePlanXmlState.MinCandidatePlans, width);
        Assert.Equal(QueryStorePlanXmlState.MaxBackoffHalvings, halvings);
    }
}
