/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3368: <see cref="ServerHealthClassifier.DeadlockSeverity"/> bands deadlocks per HOUR over the window the
/// count was taken from, against two store-backed tiers.
///
/// <para>It was <c>count &gt; 0 ? Critical : Healthy</c>. Measured on the 43-server production OLTP fleet
/// that motivated the issue, that band read <b>13.4%</b> of one-hour windows Critical and <b>87.9%</b> of
/// 24-hour windows Critical — the same servers, the same code, a 6.5x swing from the window length alone.
/// The thing being fixed is not that the threshold was too low; it is that a window-scoped count was banded
/// by a window-blind ladder.</para>
///
/// <para><b>Every pin here states a PROPERTY, not an arithmetic result.</b> The assertions this file
/// replaced were <c>DeadlockSeverity(1) == Critical</c> and <c>ServerSummaryItem { DeadlockCount = 1 }</c>
/// reading Critical — true statements about the old ladder, and the defect. A pin that only ever passes one
/// window length cannot tell a rate band from a count band and would stay green through a revert to
/// counting, so the window is varied in every case where it can be.</para>
/// </summary>
public sealed class DeadlockRateBandTests
{
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);
    private static readonly TimeSpan Day = TimeSpan.FromHours(24);

    private static HealthSeverity Band(int? count, TimeSpan window) =>
        ServerHealthClassifier.DeadlockSeverity(count, window, DeadlockRateThresholds.Default);

    /* ─────────────────────── the issue's own control ─────────────────────── */

    /// <summary>
    /// THE control for #3368: a single resolved deadlock in a busy window does not read Critical. Erik's
    /// words were that one deadlock making a server Critical is absurd, and it is what the band did.
    ///
    /// <para>Asserted across every window length a production surface can ask for — the MCP tool and
    /// <c>/api/fleet</c> both take whole hours in [1, <c>McpHelpers.MaxHoursBack</c>] — rather than at one
    /// convenient span, because "not Critical at 1 hour" is satisfiable by a ladder that is still counting
    /// with a higher number in it.</para>
    /// </summary>
    [Fact]
    public void OneDeadlock_IsNeverCritical_AtAnyWindowAProductionSurfaceCanAsk()
    {
        var windowsChecked = 0;

        foreach (var hours in Enumerable.Range(1, McpHelpers.MaxHoursBack))
        {
            var band = Band(1, TimeSpan.FromHours(hours));
            Assert.NotEqual(HealthSeverity.Critical, band);
            Assert.Equal(HealthSeverity.Healthy, band);
            windowsChecked++;
        }

        /* The loop's REACH, asserted rather than assumed, and derived from the validator's own ceiling
           rather than restated — "not Critical at one hour" is satisfiable by a ladder that is still
           counting with a bigger number in it, so the breadth IS the claim. Narrowing this loop left the
           pin green until this line existed. */
        Assert.Equal(McpHelpers.MaxHoursBack, windowsChecked);
        Assert.Equal(168, windowsChecked);
    }

    /// <summary>
    /// The measured typical server-hour — 0, 1 or 2 deadlocks, which is 99.1% of the 14,448 server-hours in
    /// the 14-day sample — reads Healthy. A default that bands a typical server-hour Critical is wrong by
    /// construction, whatever its derivation.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void ATypicalServerHour_IsHealthy(int deadlocksInTheHour) =>
        Assert.Equal(HealthSeverity.Healthy, Band(deadlocksInTheHour, Hour));

    /* ─────────────────────── the discriminating case ─────────────────────── */

    /// <summary>
    /// The one case that distinguishes a rate band from a count band: the SAME raw count over two different
    /// windows produces two different severities. A count band cannot do this at all — the window is not an
    /// input to it — so this pin goes red on any revert to counting, which the "one deadlock is not
    /// Critical" control alone would not.
    ///
    /// <para>Both directions are asserted. 120 deadlocks is 120/hr over an hour (Critical) and 5/hr over a
    /// day (Warning); 30 is 30/hr (Critical) and 1.25/hr (Healthy). Two pairs rather than one, so a ladder
    /// that happened to straddle one boundary cannot satisfy it.</para>
    /// </summary>
    [Fact]
    public void TheSameCountOverTwoWindows_BandsDifferently()
    {
        Assert.Equal(HealthSeverity.Critical, Band(120, Hour));
        Assert.Equal(HealthSeverity.Warning, Band(120, Day));

        Assert.Equal(HealthSeverity.Critical, Band(30, Hour));
        Assert.Equal(HealthSeverity.Healthy, Band(30, Day));
    }

    /// <summary>
    /// The other half of the same property, and the one the issue asked for in so many words: the same RATE
    /// over different windows bands the SAME. "Deadlocks per hour normalised over the window, so the same
    /// thresholds mean the same thing on a 1-hour read and a 24-hour read."
    ///
    /// <para>Held for a rate in each of the three bands, across the whole production window range, so the
    /// invariance is a property of the ladder rather than of one lucky span.</para>
    /// </summary>
    [Theory]
    [InlineData(1, HealthSeverity.Healthy)]
    [InlineData(2, HealthSeverity.Healthy)]
    [InlineData(4, HealthSeverity.Healthy)]
    [InlineData(5, HealthSeverity.Warning)]
    [InlineData(10, HealthSeverity.Warning)]
    [InlineData(19, HealthSeverity.Warning)]
    [InlineData(20, HealthSeverity.Critical)]
    [InlineData(50, HealthSeverity.Critical)]
    public void TheSameRateBandsTheSame_WhateverTheWindow(int ratePerHour, HealthSeverity expected)
    {
        var windowsVisited = 0;
        var countsSeen = new HashSet<int>();

        foreach (var hours in new[] { 1, 2, 6, 12, 24, 72, 168 })
        {
            var window = TimeSpan.FromHours(hours);

            /* An INTEGER rate times whole hours, so the count is exact and the rate this asserts about is
               the rate the band sees. Deriving the count by rounding a fractional rate is how this pin
               first went wrong: 4.9/hr over one hour rounds to 5 deadlocks, which is 5.0/hr and a different
               band — the test would have been asserting about a rate it had not constructed. */
            var count = ratePerHour * hours;
            Assert.Equal(ratePerHour, ServerHealthClassifier.DeadlockRatePerHour(count, window)!.Value, precision: 9);

            Assert.Equal(expected, Band(count, window));

            windowsVisited++;
            countsSeen.Add(count);
        }

        /* The loop's REACH, asserted rather than assumed — this pin's whole claim is invariance ACROSS
           windows, and collapsing the list to one entry left it passing while asserting nothing about
           invariance at all. Caught by crippling this test rather than the band: a control that survives
           being crippled was never one. The counts must differ too, or seven windows over one count would
           satisfy the reach check while still only exercising one rate/denominator pair. */
        Assert.Equal(7, windowsVisited);
        Assert.Equal(7, countsSeen.Count);
    }

    /// <summary>
    /// The tier boundaries are inclusive at the tier and exclusive just below it, asserted at a window
    /// where a fractional rate is exactly representable — 49 deadlocks over 10 hours is 4.9/hr, which is
    /// Healthy, and one more is 5.0/hr, which is Warning.
    /// </summary>
    [Fact]
    public void TheTierBoundariesAreInclusiveAtTheTier()
    {
        var tenHours = TimeSpan.FromHours(10);

        Assert.Equal(HealthSeverity.Healthy, Band(49, tenHours));    // 4.9/hr
        Assert.Equal(HealthSeverity.Warning, Band(50, tenHours));    // 5.0/hr
        Assert.Equal(HealthSeverity.Warning, Band(199, tenHours));   // 19.9/hr
        Assert.Equal(HealthSeverity.Critical, Band(200, tenHours));  // 20.0/hr
    }

    /* ─────────────────────── the two unrateable arms ─────────────────────── */

    /// <summary>
    /// A window below <see cref="ServerHealthThresholds.DeadlockRateMinimumWindow"/> yields no rate, so the
    /// band declines to claim one — and it fails AWAY from Healthy in both directions.
    ///
    /// <para>A count above zero reads Warning: deadlocks demonstrably happened and #3368 calls that a real
    /// finding, but nothing supports Critical, and banding the bare count would be the count ladder this
    /// replaced. A count of zero reads Unknown rather than Healthy — a window of no length measured
    /// nothing, and a green dot for an unmeasured metric is the failure the <see cref="HealthSeverity"/>
    /// Unknown arm exists for on every other band in this class.</para>
    ///
    /// <para>Zero, negative and merely-short windows are all asserted: a zero-length one is the
    /// <c>default(TimeSpan)</c> a bundle that declared nothing carries, and it must not divide by zero or
    /// read as "none per hour".</para>
    /// </summary>
    [Fact]
    public void AnUnrateableWindow_FailsAwayFromHealthy_AndNeverIntoCritical()
    {
        var unrateable = new[]
        {
            TimeSpan.Zero,
            TimeSpan.FromMinutes(-30),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMinutes(59),
            ServerHealthThresholds.DeadlockRateMinimumWindow - TimeSpan.FromTicks(1),
        };

        foreach (var window in unrateable)
        {
            Assert.Null(ServerHealthClassifier.DeadlockRatePerHour(1, window));

            /* Deadlocks happened but no rate is computable: Warning, and never the Critical a bare count
               would have produced — 1 deadlock in a second is 3,600/hr arithmetically. */
            Assert.Equal(HealthSeverity.Warning, Band(1, window));
            Assert.Equal(HealthSeverity.Warning, Band(10_000, window));

            /* Nothing counted over no time is not a measurement of health. */
            Assert.Equal(HealthSeverity.Unknown, Band(0, window));

            /* And an engine with no source stays Unknown — the #3272 arm is read before any of this. */
            Assert.Equal(HealthSeverity.Unknown, Band(null, window));
        }
    }

    /// <summary>
    /// The minimum window is exactly one hour, and it excludes nothing this product can produce: the MCP
    /// read validates <c>hours_back</c> into [1, <c>McpHelpers.MaxHoursBack</c>], and the viewer's Overview
    /// card derives its window from this same constant. So the minimum buys honest arithmetic at no
    /// operational cost, which is the argument for having one at all.
    /// </summary>
    [Fact]
    public void TheMinimumWindowIsTheSmallestWindowAnySurfaceCanAskFor()
    {
        Assert.Equal(TimeSpan.FromHours(1), ServerHealthThresholds.DeadlockRateMinimumWindow);

        /* The floor of the MCP/web range is rateable, and so is everything above it. */
        Assert.NotNull(ServerHealthClassifier.DeadlockRatePerHour(1, TimeSpan.FromHours(1)));
        Assert.NotNull(ServerHealthClassifier.DeadlockRatePerHour(
            1, TimeSpan.FromHours(McpHelpers.MaxHoursBack)));

        /* And the validator refuses everything below it, which is what makes the minimum free. */
        Assert.NotNull(McpHelpers.ValidateHoursBack(0));
        Assert.Null(McpHelpers.ValidateHoursBack(1));
    }

    /* ─────────────────────── the null (no-source) arm ─────────────────────── */

    /// <summary>
    /// A PostgreSQL target has no SQL-Server deadlock reading, so it bands off none (#3272/#3017) — and that
    /// survives the rate change at every window, including the unrateable ones where a COUNT above zero
    /// reads Warning. The null arm is tested first in the method for exactly this reason: an absent source
    /// must not be reachable by the arm that exists for an absent denominator.
    /// </summary>
    [Fact]
    public void NoDeadlockSourceForTheEngine_StaysUnknown()
    {
        foreach (var hours in new[] { 0, 1, 24, 168 })
        {
            Assert.Equal(HealthSeverity.Unknown, Band(null, TimeSpan.FromHours(hours)));
        }

        Assert.Equal(
            HealthSeverity.Unknown,
            ServerHealthClassifier.DeadlockSeverity(
                ServerMetricSources.DmvSourced(0, isPostgres: true), Hour, DeadlockRateThresholds.Default));
    }

    /* ─────────────────────── the tiers are settable ─────────────────────── */

    /// <summary>
    /// The band reads the tiers it is handed, not the shipped pair — the #3297 property, one surface over. A
    /// fleet with a deliberate retry-on-deadlock design raises them and the SAME reading changes band.
    ///
    /// <para>Asserted as a CHANGE from the default reading rather than as an absolute, so a band that
    /// ignored its <c>thresholds</c> argument entirely cannot pass: the identical count and window are
    /// banded twice against two different pairs and must disagree.</para>
    /// </summary>
    [Fact]
    public void TheBandReadsTheTiersItIsHanded()
    {
        const int count = 10;   // 10/hr: Warning on the shipped pair

        Assert.Equal(HealthSeverity.Warning, Band(count, Hour));

        /* Raised past the reading: the same server goes calm. */
        Assert.Equal(
            HealthSeverity.Healthy,
            ServerHealthClassifier.DeadlockSeverity(count, Hour, new DeadlockRateThresholds(50.0, 200.0)));

        /* Tightened below it: the same server goes Critical. */
        Assert.Equal(
            HealthSeverity.Critical,
            ServerHealthClassifier.DeadlockSeverity(count, Hour, new DeadlockRateThresholds(2.0, 8.0)));
    }

    /// <summary>
    /// The tiers clamp on READ, so a hand-edited store row cannot drive a nonsense threshold — and the
    /// clamp bounds are the same two constants the MCP writer and the Settings window validate against, so
    /// no surface can accept a value the band then rewrites.
    ///
    /// <para>The <c>default</c> struct is the case that matters: every field zero, which unclamped would
    /// make <c>rate &gt;= 0</c> true and band every measured deadlock Critical. It clamps to the floor
    /// instead.</para>
    /// </summary>
    [Fact]
    public void TheTiersClampOnRead_AndTheDefaultStructCannotBandEverythingCritical()
    {
        var floor = ServerHealthThresholds.DeadlockRatePerHourFloor;
        var ceiling = ServerHealthThresholds.DeadlockRatePerHourCeiling;

        var below = new DeadlockRateThresholds(-5.0, 0.0);
        Assert.Equal(floor, below.WarnPerHour);
        Assert.Equal(floor, below.CriticalPerHour);

        var above = new DeadlockRateThresholds(ceiling * 10, double.PositiveInfinity);
        Assert.Equal(ceiling, above.WarnPerHour);
        Assert.Equal(ceiling, above.CriticalPerHour);

        var nan = new DeadlockRateThresholds(double.NaN, double.NaN);
        Assert.Equal(floor, nan.WarnPerHour);
        Assert.Equal(floor, nan.CriticalPerHour);

        /* default(DeadlockRateThresholds): the floor, not zero. A zero critical tier would make a single
           deadlock in an hour Critical again, by a route no surface reports. */
        var unset = default(DeadlockRateThresholds);
        Assert.Equal(floor, unset.CriticalPerHour);
        Assert.Equal(
            HealthSeverity.Healthy,
            ServerHealthClassifier.DeadlockSeverity(0, Hour, unset));
    }

    /// <summary>
    /// The FLOOR is what keeps the knob a rate. At its tightest setting the band still says "more than one
    /// deadlock for every hour observed", so no value reachable through the control plane can restore the
    /// "any deadlock in the window is Critical" reading #3368 removed.
    ///
    /// <para>That is the property, and it is why the floor is 1.0 rather than something smaller: at the
    /// floor, a 24-hour window needs 24 deadlocks to read Critical, where the old band needed one.</para>
    /// </summary>
    [Fact]
    public void EvenAtTheFloor_TheBandCannotBecomeTheCountBandAgain()
    {
        var tightest = new DeadlockRateThresholds(
            ServerHealthThresholds.DeadlockRatePerHourFloor,
            ServerHealthThresholds.DeadlockRatePerHourFloor);

        /* One deadlock over a day is 0.042/hr — Healthy even at the tightest setting the store allows. */
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.DeadlockSeverity(1, Day, tightest));

        /* It takes a full day's worth at one per hour to reach the tier. */
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.DeadlockSeverity(24, Day, tightest));
    }

    /// <summary>
    /// A critical tier set BELOW the warning tier is not corrected: every banded rate is Critical and the
    /// Warning tier is empty, which is a coherent reading of what an operator who set it there asked for.
    /// Same call as its #3297 sibling — a <c>Math.Max</c> would band on a number
    /// <c>get_alert_settings</c> does not report.
    /// </summary>
    [Fact]
    public void ACriticalTierBelowTheWarningTier_MakesEveryFireCritical()
    {
        var inverted = new DeadlockRateThresholds(WarnPerHourRaw: 50.0, CriticalPerHourRaw: 5.0);

        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.DeadlockSeverity(5, Hour, inverted));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.DeadlockSeverity(60, Hour, inverted));

        /* And the Warning tier is genuinely empty, not merely unreached in these two cases. */
        for (var count = 0; count <= 100; count++)
        {
            Assert.NotEqual(
                HealthSeverity.Warning,
                ServerHealthClassifier.DeadlockSeverity(count, Hour, inverted));
        }
    }

    /* ─────────────────────── the defaults' derivation ─────────────────────── */

    /// <summary>
    /// The shipped tiers, pinned against the measured distribution they were derived from rather than as
    /// bare numbers — 14 days of <c>collect.deadlocks</c> on a 43-server production OLTP fleet, 2,722
    /// deadlocks over 14,448 server-hours, bucketed one server-hour at a time.
    ///
    /// <para>The histogram's whole non-zero body is replayed through the band, so what is asserted is the
    /// SHAPE of the outcome on real data: the routine population is not Critical, and the two storm hours
    /// are. A pin on the constants alone would survive a change that kept the numbers and broke the
    /// ladder.</para>
    /// </summary>
    [Fact]
    public void TheShippedTiersSeparateTheMeasuredPopulations()
    {
        /* deadlocks-in-one-hour => server-hours observed, 14 days x 43 servers. Zero is 12,504 of them. */
        var histogram = new (int DeadlocksInTheHour, int ServerHours)[]
        {
            (0, 12_504), (1, 1_566), (2, 250), (3, 81), (4, 27), (5, 11),
            (6, 3), (7, 1), (8, 1), (10, 1), (15, 1), (90, 1), (102, 1),
        };

        Assert.Equal(14_448, histogram.Sum(h => h.ServerHours));
        Assert.Equal(2_722, histogram.Sum(h => h.DeadlocksInTheHour * h.ServerHours));

        var critical = 0;
        var warning = 0;

        foreach (var (deadlocks, serverHours) in histogram)
        {
            var band = Band(deadlocks, Hour);

            /* The routine mode tops out at 15 in an hour; the next observation at all is 90. Nothing in
               [16, 89] exists in the sample, which is the empty interval the Critical tier sits in. */
            if (deadlocks <= 15)
            {
                Assert.NotEqual(HealthSeverity.Critical, band);
            }
            else
            {
                Assert.Equal(HealthSeverity.Critical, band);
            }

            if (band == HealthSeverity.Critical) critical += serverHours;
            else if (band == HealthSeverity.Warning) warning += serverHours;
        }

        /* Critical fires on 2 of 14,448 server-hours — the two storm hours. Warning on 18. Twenty
           server-hours reach the warn tier OR ABOVE, which is the figure the store measurement reports and
           is NOT the size of the Warning band: 18 + 2. Getting that wrong is what this assertion caught
           the first time it ran, so both numbers and their sum are pinned.

           The old count band fired Critical on all 1,944 non-zero server-hours. */
        Assert.Equal(2, critical);
        Assert.Equal(18, warning);
        Assert.Equal(20, warning + critical);
        Assert.Equal(1_944, histogram.Where(h => h.DeadlocksInTheHour > 0).Sum(h => h.ServerHours));
    }

    /// <summary>
    /// The same 14-day sample bucketed by DAY instead of by hour: the worst 24-hour server-day held 104
    /// deadlocks, which is 4.33 per hour and Healthy. That is the honest reading of a day's average, and it
    /// is the reading the band now gives on the 24-hour window where the count ladder called <b>87.9%</b> of
    /// server-days Critical.
    /// </summary>
    [Fact]
    public void TheWorstMeasuredServerDay_IsNotCriticalOnADayWindow()
    {
        Assert.Equal(HealthSeverity.Healthy, Band(104, Day));

        /* The storm hour inside it still reads Critical when asked about on its own hour, which is what
           makes the two windows honest about their own spans rather than agreeing by accident. */
        Assert.Equal(HealthSeverity.Critical, Band(102, Hour));
    }

    /* ─────────────────────── the fold and the rank ─────────────────────── */

    /// <summary>
    /// The bundle fold reads the window and the tiers off the bundle, so a card's overall band and its
    /// worst-first score cannot disagree with its own deadlock dot — the contradiction #3281 fixed on the
    /// CPU arm, which is the same failure one metric over.
    /// </summary>
    [Fact]
    public void TheBundleFoldBandsOnTheSameRateTheDotDoes()
    {
        var storm = new ServerHealthMetrics
        {
            DeadlockCount = 102,
            DeadlockWindow = Hour,
            DeadlockRateThresholds = DeadlockRateThresholds.Default,
        };
        var quiet = storm with { DeadlockWindow = TimeSpan.FromHours(102) };   // exactly 1/hr

        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.OverallMetricSeverity(storm));
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.OverallMetricSeverity(quiet));

        Assert.Contains(HealthSeverity.Critical, ServerHealthClassifier.MetricSeverities(storm));
        Assert.DoesNotContain(HealthSeverity.Critical, ServerHealthClassifier.MetricSeverities(quiet));
    }

    /// <summary>
    /// A bundle that declares NO tiers bands on the shipped pair — the unsupplied-seam fallback its #3297
    /// sibling uses, so a path written before the knobs existed behaves like a store at its V120 defaults
    /// rather than banding on zeros.
    ///
    /// <para>What stops a PRODUCTION path taking that fallback is
    /// <c>DeadlockRateBandRungTests.EveryProductionMetricBundleDeclaresTheWindowAndTheTiers</c>, not this
    /// pin.</para>
    /// </summary>
    [Fact]
    public void ABundleWithNoTiers_BandsOnTheShippedPair()
    {
        var noTiers = new ServerHealthMetrics { DeadlockCount = 102, DeadlockWindow = Hour };
        var shipped = noTiers with { DeadlockRateThresholds = DeadlockRateThresholds.Default };

        Assert.Null(noTiers.DeadlockRateThresholds);
        Assert.Equal(
            ServerHealthClassifier.OverallMetricSeverity(shipped),
            ServerHealthClassifier.OverallMetricSeverity(noTiers));
    }

    /// <summary>
    /// The rate helper is exact over the window rather than rounded to whole hours, so a 90-minute window
    /// is not silently read as one hour or two. Nothing in production asks for one today; the pin exists
    /// because a surface that starts to would otherwise be banded on a denominator it did not supply.
    /// </summary>
    [Fact]
    public void TheRateDividesByTheActualWindow()
    {
        Assert.Equal(2.0, ServerHealthClassifier.DeadlockRatePerHour(3, TimeSpan.FromMinutes(90))!.Value, precision: 9);
        Assert.Equal(0.5, ServerHealthClassifier.DeadlockRatePerHour(12, Day)!.Value, precision: 9);
        Assert.Equal(0.0, ServerHealthClassifier.DeadlockRatePerHour(0, Day)!.Value, precision: 9);
    }
}
