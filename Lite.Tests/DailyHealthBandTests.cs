/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Table-driven tests for the shared Performance Calendar day-banding (<see cref="DailyHealthBandCalculator"/>).
/// This is the one classification both apps color their month heatmap by, so it is pinned here (Lite) and,
/// identically, in Darling.Tests. Covers the no-data / healthy / warning / critical tiers, first-match-wins
/// priority, the threshold boundaries, and the label / brush-key / tooltip mappings.
/// </summary>
public class DailyHealthBandTests
{
    private static readonly TimeSpan Day = TimeSpan.FromHours(24);
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    private static DailyHealthSignals Signals(
        bool hasData = true, long deadlocks = 0, long collectionErrors = 0, long highCpu = 0,
        long blocking = 0, long memPressure = 0, long memCritical = 0, long alerts = 0,
        TimeSpan window = default, long collectionRuns = 0, long peakBlockMs = 0) => new()
    {
        HasData = hasData,
        Deadlocks = deadlocks,
        CollectionErrors = collectionErrors,
        CollectionRuns = collectionRuns,
        HighCpuEvents = highCpu,
        BlockingEvents = blocking,
        PeakBlockWaitMs = peakBlockMs,
        MemoryPressureEvents = memPressure,
        MemoryCriticalEvents = memCritical,
        AlertCount = alerts,
        Window = window,
    };

    [Fact]
    public void NoCollection_IsNoData_RegardlessOfOtherCounts()
    {
        // Even absurd counts can't override "we didn't collect that day".
        var s = Signals(hasData: false, deadlocks: 99, collectionErrors: 99, highCpu: 99, blocking: 99, memCritical: 99, alerts: 99);
        Assert.Equal(DailyHealthBand.NoData, DailyHealthBandCalculator.Classify(s));
    }

    [Fact]
    public void Collected_AndNothingElevated_IsHealthy()
    {
        Assert.Equal(DailyHealthBand.Healthy, DailyHealthBandCalculator.Classify(Signals()));
    }

    /// <summary>
    /// Each remaining Critical trigger alone, over a finished 24-hour day (#3539 A2 made every count
    /// trigger window-aware, so the window is declared): severe memory pressure on presence; sustained
    /// CPU at the day-scale bar of 30 hot samples; a blocking RATE at the 20/hr tier (480 over the day);
    /// a single 60-second block whatever the count. Collection errors are no longer a Critical trigger at
    /// any count — see the collection-error pins below.
    /// </summary>
    [Theory]
    [InlineData("memory-critical", 0, 0, 0L, 1)]
    [InlineData("sustained-cpu", 30, 0, 0L, 0)]
    [InlineData("blocking-rate", 0, 480, 0L, 0)]
    [InlineData("sixty-second-block", 0, 1, 60_000L, 0)]
    public void CriticalTriggers_EachAloneIsCritical(string _, long highCpu, long blocking, long peakBlockMs, long memCritical)
    {
        var s = Signals(highCpu: highCpu, blocking: blocking, peakBlockMs: peakBlockMs, memCritical: memCritical, window: Day);
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(s));
    }

    /* ── the deadlock RATE trigger (#3525 — #3368's twin, routed through the card band's tiers) ── */

    [Fact]
    public void ACriticalDeadlockRate_AloneIsCritical()
    {
        // 480 over 24h = 20.0/hr, the Critical tier — the same pair the Overview card's dot bands on.
        Assert.Equal(
            DailyHealthBand.Critical,
            DailyHealthBandCalculator.Classify(Signals(deadlocks: 480, window: Day)));
    }

    /// <summary>
    /// The defect #3525 was filed on: one deadlock in a 24-hour day banded the WHOLE day Critical — the
    /// count trigger #3368 removed from the card, still shipping in this classifier. Measured on the same
    /// 43-server fleet, count &gt; 0 read 87.9% of 24-hour windows Critical, so ~7 of 8 calendar cells
    /// painted red from deadlocks alone. At 0.04/hr the day is Healthy; the deadlock stays countable — the
    /// tooltip lists it and the drill is offered — the BAND just stops claiming a crisis.
    /// </summary>
    [Fact]
    public void ASingleDeadlockInADay_IsNoLongerCritical()
    {
        var s = Signals(deadlocks: 1, window: Day);
        Assert.Equal(DailyHealthBand.Healthy, DailyHealthBandCalculator.Classify(s));

        Assert.Contains(DayDrillTarget.Deadlocks, DailyHealthBandCalculator.AvailableDrills(s));
        Assert.Contains("1 deadlock", DailyHealthBandCalculator.Describe(s));
    }

    /// <summary>
    /// The two-window proof, on the DAY classifier — the same per-hour rate bands the day identically over
    /// a 1-hour window (a fleet-sweep span at the cadence ceiling's scale) and a 24-hour one (a calendar
    /// day). Counts are integer-rate-times-whole-hours so the asserted rate is exactly the one the band
    /// sees (the DeadlockRateBandTests discipline).
    /// </summary>
    [Theory]
    [InlineData(4, DailyHealthBand.Healthy)]
    [InlineData(5, DailyHealthBand.Warning)]
    [InlineData(19, DailyHealthBand.Warning)]
    [InlineData(20, DailyHealthBand.Critical)]
    [InlineData(50, DailyHealthBand.Critical)]
    public void TheSameDeadlockRate_BandsTheDayTheSame_OverAnHourAndADay(long ratePerHour, DailyHealthBand expected)
    {
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(deadlocks: ratePerHour, window: Hour)));
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(deadlocks: ratePerHour * 24, window: Day)));
    }

    /// <summary>
    /// And the discriminating converse: the same COUNT over the two windows bands differently, which a
    /// count trigger cannot do at all — the pin that goes red on any revert to counting.
    /// </summary>
    [Fact]
    public void TheSameDeadlockCount_OverTwoWindows_BandsDifferently()
    {
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(Signals(deadlocks: 30, window: Hour)));
        Assert.Equal(DailyHealthBand.Healthy, DailyHealthBandCalculator.Classify(Signals(deadlocks: 30, window: Day)));
    }

    /// <summary>
    /// A sub-hour window — a fleet sweep at any cadence under an hour — is not rate-banded (#3368's own
    /// arm): a non-zero count reads Warning (deadlocks demonstrably happened; no rate supports Critical,
    /// and 1 deadlock in 15 minutes is 4/hr arithmetically but the hour was not observed), and a zero
    /// count stays out of the deadlock trigger entirely rather than claiming anything. An undeclared
    /// window — <c>default(TimeSpan)</c>, a producer that declared nothing — takes the same arm, so no
    /// path can rate-multiply or restore count-is-Critical by omission.
    /// </summary>
    [Fact]
    public void ASubHourOrUndeclaredWindow_FallsToWarning_NeverCritical()
    {
        foreach (var window in new[] { default, TimeSpan.FromMinutes(15), TimeSpan.FromMinutes(59) })
        {
            Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(deadlocks: 1, window: window)));
            Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(deadlocks: 10_000, window: window)));
            Assert.Equal(DailyHealthBand.Healthy, DailyHealthBandCalculator.Classify(Signals(deadlocks: 0, window: window)));
        }
    }

    /// <summary>
    /// The day bands on the tiers it is handed — the store-backed pair (#3368, V120) travels through
    /// <see cref="DailyHealthThresholds.DeadlockRates"/>. Lite has no store knobs for these, so it bands
    /// on the shipped defaults, but the seam is the shared one and must honour a handed pair identically.
    /// </summary>
    [Fact]
    public void TheDeadlockRateTiers_AreOverridable()
    {
        var raised = new DailyHealthThresholds { DeadlockRates = new DeadlockRateThresholds(100.0, 500.0) };
        var s = Signals(deadlocks: 480, window: Day); // 20/hr: Critical on the shipped pair
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(s));
        Assert.Equal(DailyHealthBand.Healthy, DailyHealthBandCalculator.Classify(s, raised));
    }

    /// <summary>Each Warning trigger alone over a finished day: six hot samples (the day-scale Warning bar),
    /// blocking at the 5/hr tier (120 over the day), non-severe memory pressure, an alert.</summary>
    [Theory]
    [InlineData("moderate-cpu", 6, 0, 0, 0)]
    [InlineData("blocking-rate", 0, 120, 0, 0)]
    [InlineData("memory-pressure", 0, 0, 1, 0)]
    [InlineData("alert", 0, 0, 0, 1)]
    public void WarningTriggers_EachAloneIsWarning(string _, long highCpu, long blocking, long memPressure, long alerts)
    {
        var s = Signals(highCpu: highCpu, blocking: blocking, memPressure: memPressure, alerts: alerts, window: Day);
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(s));
    }

    [Fact]
    public void CriticalBeatsWarning_WhenBothPresent()
    {
        // A critical deadlock rate (480/24h = 20/hr) plus moderate CPU + alerts (warning) still bands Critical.
        var s = Signals(deadlocks: 480, highCpu: 3, alerts: 4, window: Day);
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(s));
    }

    /* ── the high-CPU trigger (#3539 A2): a bar that scales with the window ── */

    /// <summary>
    /// Over an HOUR the pre-#3539 constants hold exactly: one hot sample is Warning, six is Critical — the
    /// excursion-scale minimum dominates below 4.8 hours, so every fleet-sweep span at the default cadence
    /// bands as it always did.
    /// </summary>
    [Theory]
    [InlineData(0, DailyHealthBand.Healthy)]
    [InlineData(1, DailyHealthBand.Warning)]
    [InlineData(5, DailyHealthBand.Warning)]
    [InlineData(6, DailyHealthBand.Critical)]
    public void HighCpu_OverAnHour_BandsOnTheExcursionScaleMinimum(long highCpu, DailyHealthBand expected)
    {
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(highCpu: highCpu, window: Hour)));
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(highCpu: highCpu, window: TimeSpan.FromMinutes(15))));
        /* An undeclared window yields the same minimum — a producer that declares nothing bands as it did. */
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(highCpu: highCpu)));
    }

    /// <summary>
    /// Over a DAY the rate decides: six hot samples — which reddened 5.9% of the measured server-days — is
    /// now the Warning bar, and Critical needs thirty, the sustained-heat count 1.4% of server-days reach.
    /// The measured routine excursion is 1–3 samples (95.8% of 758 excursions), so two of them in a day
    /// (six) is Warning and one (up to five) is Healthy, where 19.2% of days used to turn amber on a single
    /// 80%+ sample.
    /// </summary>
    [Theory]
    [InlineData(0, DailyHealthBand.Healthy)]
    [InlineData(5, DailyHealthBand.Healthy)]
    [InlineData(6, DailyHealthBand.Warning)]
    [InlineData(29, DailyHealthBand.Warning)]
    [InlineData(30, DailyHealthBand.Critical)]
    [InlineData(155, DailyHealthBand.Critical)]  // the worst measured server-day
    public void HighCpu_OverADay_BandsOnTheSustainedHeatRate(long highCpu, DailyHealthBand expected) =>
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(highCpu: highCpu, window: Day)));

    /// <summary>The bar is the GREATER of the minimum and the rate: 6 until 4.8 hours, then 1.25 × hours.
    /// Stated at the seam so the shape — not just its two endpoints — is pinned.</summary>
    [Fact]
    public void HighCpu_TheBar_IsTheGreaterOfMinimumAndRate()
    {
        var t = DailyHealthThresholds.Default;
        Assert.Equal(6.0, t.HighCpuCriticalSamplesFor(TimeSpan.Zero));
        Assert.Equal(6.0, t.HighCpuCriticalSamplesFor(TimeSpan.FromMinutes(15)));
        Assert.Equal(6.0, t.HighCpuCriticalSamplesFor(Hour));
        Assert.Equal(6.0, t.HighCpuCriticalSamplesFor(TimeSpan.FromHours(4.8)));
        Assert.Equal(15.0, t.HighCpuCriticalSamplesFor(TimeSpan.FromHours(12)));
        Assert.Equal(30.0, t.HighCpuCriticalSamplesFor(Day));

        Assert.Equal(1.0, t.HighCpuWarningSamplesFor(Hour));
        Assert.Equal(6.0, t.HighCpuWarningSamplesFor(Day));

        /* The two constants each carry their measured lineage: 1.25/hr is 30 per day; the minimum is six,
           the 97.8th percentile of excursion length. */
        Assert.Equal(30.0, t.HighCpuCriticalSamplesPerHour * 24);
        Assert.Equal(6, t.HighCpuCriticalSamplesMinimum);
    }

    /// <summary>Two routine 3-sample excursions in a day used to redden the cell (#3282's finding); they
    /// are Warning now, and the same six in one hour — a sustained excursion for that span — stays Critical.</summary>
    [Fact]
    public void TwoRoutineExcursions_InADay_AreNoLongerCritical()
    {
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(highCpu: 6, window: Day)));
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(Signals(highCpu: 6, window: Hour)));
    }

    /* ── the blocking trigger (#3539 A2/A3): the card band's own rate and wait arms ── */

    /// <summary>
    /// The same per-hour rate bands the day identically over an hour and a day, through the card band's
    /// tiers — 5/hr Warning, 20/hr Critical. Counts are rate × whole hours so the asserted rate is exactly
    /// the one the band sees.
    /// </summary>
    [Theory]
    [InlineData(4, DailyHealthBand.Healthy)]
    [InlineData(5, DailyHealthBand.Warning)]
    [InlineData(19, DailyHealthBand.Warning)]
    [InlineData(20, DailyHealthBand.Critical)]
    public void TheSameBlockingRate_BandsTheDayTheSame_OverAnHourAndADay(long ratePerHour, DailyHealthBand expected)
    {
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(blocking: ratePerHour, window: Hour)));
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(blocking: ratePerHour * 24, window: Day)));
    }

    /// <summary>
    /// The constant this replaced: eleven blocking events reddened 5.1% of measured server-days. Eleven in a
    /// day is 0.46/hr and Healthy by count; eleven in an hour is inside the measured trough and Warning;
    /// the same count over two windows bands differently, which the count trigger could not do.
    /// </summary>
    [Fact]
    public void ElevenBlockingEventsInADay_IsNoLongerCritical()
    {
        var day = Signals(blocking: 11, window: Day);
        Assert.Equal(DailyHealthBand.Healthy, DailyHealthBandCalculator.Classify(day));
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(blocking: 11, window: Hour)));

        /* Still countable: the drill is offered and the tooltip lists it with its rate. */
        Assert.Contains(DayDrillTarget.Blocking, DailyHealthBandCalculator.AvailableDrills(day));
        Assert.Contains("11 blocking events (0.5/hr)", DailyHealthBandCalculator.Describe(day));
    }

    /// <summary>The day's peak block is the wait arm: a 60-second block is a Critical day and a 10-second
    /// one a Warning day whatever the count or the window — the card band's rate-independent arms.</summary>
    [Fact]
    public void ThePeakBlock_IsTheDaysWaitArm()
    {
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(Signals(blocking: 1, peakBlockMs: 60_000, window: Day)));
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(blocking: 1, peakBlockMs: 10_000, window: Day)));
        Assert.Equal(DailyHealthBand.Healthy, DailyHealthBandCalculator.Classify(Signals(blocking: 1, peakBlockMs: 9_999, window: Day)));
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(Signals(blocking: 1, peakBlockMs: 60_000, window: TimeSpan.FromMinutes(15))));
    }

    /// <summary>A sub-hour or undeclared window is not rate-banded on blocking either: a non-zero count reads
    /// Warning, never Critical by count, and a zero count stays out of the trigger.</summary>
    [Fact]
    public void ASubHourOrUndeclaredWindow_BandsBlockingWarning_NeverCriticalByCount()
    {
        foreach (var window in new[] { default, TimeSpan.FromMinutes(15), TimeSpan.FromMinutes(59) })
        {
            Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(blocking: 1, window: window)));
            Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(blocking: 10_000, window: window)));
            Assert.Equal(DailyHealthBand.Healthy, DailyHealthBandCalculator.Classify(Signals(blocking: 0, window: window)));
        }
    }

    /* ── the collection-error trigger (#3539 A2): a share of runs, Warning ceiling ── */

    /// <summary>
    /// The rule this replaced was <c>CollectionErrors &gt; 0 → Critical</c>; one transient ERROR row painted
    /// a day red (#1805 was one). The errors now band as a share of the window's runs against the
    /// collector-health classifier's own 20% bar, at ITS tier: past the bar the day is Warning, below it
    /// the errors are disclosed but do not band, and nothing here can make a day Critical.
    /// </summary>
    [Theory]
    [InlineData(1, 30_000, DailyHealthBand.Healthy)]        // one transient error in a day's ~30k runs
    [InlineData(6_000, 30_000, DailyHealthBand.Healthy)]    // exactly 20% — the bar is strict, as the classifier's is
    [InlineData(6_001, 30_000, DailyHealthBand.Warning)]
    [InlineData(30_000, 30_000, DailyHealthBand.Warning)]   // every run erroring is still Warning here, never Critical
    [InlineData(0, 0, DailyHealthBand.Healthy)]
    public void CollectionErrors_BandOnTheirShareOfRuns_WarningAtMost(long errors, long runs, DailyHealthBand expected) =>
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(collectionErrors: errors, collectionRuns: runs, window: Day)));

    /// <summary>With no denominator declared a non-zero error count fails away from Healthy into Warning —
    /// never Critical — and the tooltip prints the count without a share it could not compute.</summary>
    [Fact]
    public void CollectionErrors_WithNoDeclaredRuns_AreWarning_NeverCritical()
    {
        var s = Signals(collectionErrors: 1, window: Day);
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(s));
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(collectionErrors: 10_000, window: Day)));
        Assert.Contains("1 collection error", DailyHealthBandCalculator.Describe(s));
        Assert.DoesNotContain("% of", DailyHealthBandCalculator.Describe(s));
    }

    [Fact]
    public void CollectionErrorLine_CarriesTheShare_WhenRunsAreDeclared()
    {
        var s = Signals(collectionErrors: 12, collectionRuns: 9_800, window: Day);
        Assert.Contains("12 collection errors (0.1% of 9,800 runs)", DailyHealthBandCalculator.Describe(s));
        Assert.Contains("12 collection errors (0.1% of 9,800 runs)", DailyHealthBandCalculator.BuildReasons(s));
        Assert.Equal(HealthSeverity.Healthy, DailyHealthBandCalculator.CollectionErrorSeverity(12, 9_800));
        Assert.Equal(20.0, CollectorHealthClassifier.WarningFailureRatePercent);
    }

    /// <summary>The 15-minute sweep span and the 24-hour day agree on identical behaviour: the same per-hour
    /// blocking rate, the same error share, the same excursion-scale CPU count below the seam.</summary>
    [Fact]
    public void TheSweepSpan_AndTheDay_AgreeOnIdenticalBehaviour()
    {
        var quarterHour = TimeSpan.FromMinutes(15);

        /* Blocking: sub-hour is the unrateable arm, so the comparison that CAN be made is the hour vs the
           day at one rate — pinned above — and the wait arm, which is identical at every span. */
        Assert.Equal(
            DailyHealthBandCalculator.Classify(Signals(blocking: 3, peakBlockMs: 60_000, window: quarterHour)),
            DailyHealthBandCalculator.Classify(Signals(blocking: 288, peakBlockMs: 60_000, window: Day)));

        /* Collection errors: a share is a share. */
        Assert.Equal(
            DailyHealthBandCalculator.Classify(Signals(collectionErrors: 30, collectionRuns: 100, window: quarterHour)),
            DailyHealthBandCalculator.Classify(Signals(collectionErrors: 3_000, collectionRuns: 10_000, window: Day)));
        Assert.Equal(
            DailyHealthBand.Warning,
            DailyHealthBandCalculator.Classify(Signals(collectionErrors: 30, collectionRuns: 100, window: quarterHour)));

        /* CPU: the minimum decides at both 15 minutes and an hour — the sweep floor and the default cadence. */
        Assert.Equal(
            DailyHealthBandCalculator.Classify(Signals(highCpu: 6, window: quarterHour)),
            DailyHealthBandCalculator.Classify(Signals(highCpu: 6, window: Hour)));
    }

    [Fact]
    public void Thresholds_AreOverridable()
    {
        var strict = new DailyHealthThresholds { HighCpuCriticalSamplesMinimum = 3 };
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(Signals(highCpu: 3, window: Hour), strict));
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(highCpu: 3, window: Hour)));
    }

    [Theory]
    [InlineData(DailyHealthBand.NoData, "No Data", "ForegroundMutedBrush")]
    [InlineData(DailyHealthBand.Healthy, "Healthy", "SuccessBrush")]
    [InlineData(DailyHealthBand.Warning, "Warning", "WarningBrush")]
    [InlineData(DailyHealthBand.Critical, "Critical", "ErrorBrush")]
    public void Label_And_BrushKey_MapEachBand(DailyHealthBand band, string label, string brushKey)
    {
        Assert.Equal(label, DailyHealthBandCalculator.Label(band));
        Assert.Equal(brushKey, DailyHealthBandCalculator.BrushKey(band));
    }

    [Fact]
    public void Describe_NoData_And_NoIssues_And_MultiSignal()
    {
        Assert.Equal("No data collected.", DailyHealthBandCalculator.Describe(Signals(hasData: false)));
        Assert.Equal("No issues detected.", DailyHealthBandCalculator.Describe(Signals()));

        var described = DailyHealthBandCalculator.Describe(Signals(deadlocks: 2, blocking: 1, alerts: 3));
        Assert.Contains("2 deadlocks", described);
        Assert.Contains("1 blocking event", described);   // singular
        Assert.Contains("3 alerts", described);
    }

    /// <summary>
    /// #3653: the state-aware overload the two calendars' tooltips call. A grey cell used to say only "No data
    /// collected.", which for a purged day is false — the day WAS collected; retention took it — so each state
    /// gets one sentence saying what the zeros are, with the horizon named; inside retention the signal lines
    /// stand and a missing run record is a trailing disclosure, never a replacement.
    /// </summary>
    [Fact]
    public void Describe_WithRetentionState_SaysWhatTheZerosAre()
    {
        var horizon = new DateTime(2026, 7, 20, 0, 0, 0, DateTimeKind.Utc);

        var purged = DailyHealthBandCalculator.Describe(Signals(hasData: false), DailySummaryDataState.Purged, horizon);
        Assert.StartsWith("No verdict: this day is before the store's retention horizon (2026-07-20)", purged, StringComparison.Ordinal);
        Assert.Contains("purged", purged, StringComparison.Ordinal);
        Assert.DoesNotContain("No data collected.", purged, StringComparison.Ordinal);

        var pastHorizon = DailyHealthBandCalculator.Describe(Signals(hasData: false), DailySummaryDataState.PastHorizon, horizon, signalSourcesPresent: 3);
        Assert.Contains("(2026-07-20)", pastHorizon, StringComparison.Ordinal);
        Assert.Contains("3 of 7 signal sources still hold rows", pastHorizon, StringComparison.Ordinal);

        /* No horizon handed in: the sentence stands without the parenthetical, never a placeholder date. */
        Assert.StartsWith("No verdict: this day is before the store's retention horizon and", DailyHealthBandCalculator.Describe(Signals(hasData: false), DailySummaryDataState.Purged, null), StringComparison.Ordinal);

        var noRunRecord = DailyHealthBandCalculator.Describe(Signals(deadlocks: 2), DailySummaryDataState.NoRunRecord, horizon);
        Assert.StartsWith("2 deadlocks", noRunRecord, StringComparison.Ordinal);
        Assert.EndsWith("nothing records that the day was fully collected.", noRunRecord, StringComparison.Ordinal);

        /* Collected is the plain overload, verbatim. */
        Assert.Equal(DailyHealthBandCalculator.Describe(Signals()), DailyHealthBandCalculator.Describe(Signals(), DailySummaryDataState.Collected, horizon));
        Assert.Equal(DailyHealthBandCalculator.Describe(Signals(deadlocks: 2, alerts: 3)), DailyHealthBandCalculator.Describe(Signals(deadlocks: 2, alerts: 3), DailySummaryDataState.Collected, horizon));
        /* A no-data row inside retention (the absent day) keeps the old sentence: nothing was collected. */
        Assert.Equal("No data collected.", DailyHealthBandCalculator.Describe(Signals(hasData: false), DailySummaryDataState.NoRunRecord, horizon));
    }

    [Fact]
    public void Describe_MemoryPressure_DoesNotDoubleCountSevere()
    {
        // 3 pressure events, 1 severe -> "1 severe" + "2 (non-severe)", never "3 memory-pressure events".
        var described = DailyHealthBandCalculator.Describe(Signals(memPressure: 3, memCritical: 1));
        Assert.Contains("1 severe memory-pressure event", described);
        Assert.Contains("2 memory-pressure events", described);
        Assert.DoesNotContain("3 memory-pressure events", described);
    }

    // ── Day-detail panel logic (the click-a-day panel's reasons / drills / window / metrics line) ──

    [Fact]
    public void BuildReasons_TerminalMessages_ForNoData_AndQuietDay()
    {
        // No-Data uses the day-detail phrasing (distinct from the tooltip's "No data collected.").
        Assert.Equal(new[] { "No collection this day." }, DailyHealthBandCalculator.BuildReasons(Signals(hasData: false)));
        Assert.Equal(new[] { "No issues detected." }, DailyHealthBandCalculator.BuildReasons(Signals()));
    }

    [Fact]
    public void BuildReasons_ListsEachNonZeroSignal()
    {
        var reasons = DailyHealthBandCalculator.BuildReasons(
            Signals(deadlocks: 2, collectionErrors: 1, highCpu: 4, blocking: 3, memPressure: 5, memCritical: 2, alerts: 1));
        Assert.Contains("2 deadlocks", reasons);
        Assert.Contains("1 collection error", reasons);
        Assert.Contains("4 high-CPU samples", reasons);
        Assert.Contains("3 blocking events", reasons);
        Assert.Contains("2 severe memory-pressure events", reasons);
        Assert.Contains("3 memory-pressure events", reasons); // 5 total - 2 severe, not double-counted
        Assert.Contains("1 alert", reasons);
    }

    [Theory]
    [InlineData(800, "4 blocking events (peak block 800 ms)")]
    [InlineData(12500, "4 blocking events (peak block 12.5 s)")]
    [InlineData(150000, "4 blocking events (peak block 2.5 min)")]
    public void BuildReasons_BlockingLine_CarriesPeakBlock_WhenProvided(long peakMs, string expected)
    {
        Assert.Contains(expected, DailyHealthBandCalculator.BuildReasons(Signals(blocking: 4), peakMs));
    }

    [Fact]
    public void BuildReasons_BlockingLine_OmitsPeak_WhenZero()
    {
        var reasons = DailyHealthBandCalculator.BuildReasons(Signals(blocking: 4), peakBlockMs: 0);
        Assert.Contains("4 blocking events", reasons);
        Assert.DoesNotContain(reasons, r => r.Contains("peak block", StringComparison.Ordinal));
    }

    /// <summary>
    /// The deadlock line carries the per-hour rate the band evaluated (#3525) — the card reason's own
    /// disclosure rule: "120 deadlocks" against an amber cell cannot say which tier was crossed, because
    /// 120 in an hour and 120 in a day are the same string. The count stays (the countable fact); the rate
    /// is added (the banded one). Shared by the tooltip and the day-detail reasons, so both surfaces say it.
    /// </summary>
    [Fact]
    public void DeadlockLine_CarriesTheRate_WhenTheWindowIsRateable()
    {
        var day = Signals(deadlocks: 120, window: Day); // 5.0/hr — the Warning tier exactly
        Assert.Contains("120 deadlocks (5.0/hr)", DailyHealthBandCalculator.Describe(day));
        Assert.Contains("120 deadlocks (5.0/hr)", DailyHealthBandCalculator.BuildReasons(day));

        // A single deadlock still reads singular, rate beside it.
        Assert.Contains("1 deadlock (0.0/hr)", DailyHealthBandCalculator.Describe(Signals(deadlocks: 1, window: Day)));
    }

    [Fact]
    public void DeadlockLine_PrintsTheCountAlone_OnAnUnrateableWindow()
    {
        // No declared window: no rate is computable, and printing one would claim a measurement nobody
        // took — the count alone is exactly what the band had to go on.
        var described = DailyHealthBandCalculator.Describe(Signals(deadlocks: 2));
        Assert.Contains("2 deadlocks", described);
        Assert.DoesNotContain("/hr", described);
    }

    [Fact]
    public void AvailableDrills_NoData_OffersNothing()
    {
        Assert.Empty(DailyHealthBandCalculator.AvailableDrills(Signals(hasData: false)));
    }

    [Fact]
    public void AvailableDrills_CollectedDay_AlwaysOffersTopQueries()
    {
        Assert.Equal(new[] { DayDrillTarget.TopQueries }, DailyHealthBandCalculator.AvailableDrills(Signals()));
    }

    [Fact]
    public void AvailableDrills_AddsDeadlocks_And_Blocking_OnlyWhenPresent_InPanelOrder()
    {
        Assert.Equal(
            new[] { DayDrillTarget.Deadlocks, DayDrillTarget.Blocking, DayDrillTarget.TopQueries },
            DailyHealthBandCalculator.AvailableDrills(Signals(deadlocks: 1, blocking: 2)));
        Assert.Equal(
            new[] { DayDrillTarget.Deadlocks, DayDrillTarget.TopQueries },
            DailyHealthBandCalculator.AvailableDrills(Signals(deadlocks: 3)));
        Assert.Equal(
            new[] { DayDrillTarget.Blocking, DayDrillTarget.TopQueries },
            DailyHealthBandCalculator.AvailableDrills(Signals(blocking: 7)));
    }

    [Fact]
    public void DayWindowUtc_IsClickedDay_MidnightToNextMidnight()
    {
        var (start, end) = DailyHealthBandCalculator.DayWindowUtc(new DateTime(2026, 7, 8, 14, 37, 12));
        Assert.Equal(new DateTime(2026, 7, 8), start); // time component dropped
        Assert.Equal(new DateTime(2026, 7, 9), end);
        Assert.Equal(TimeSpan.FromDays(1), end - start);
    }

    [Fact]
    public void BuildKeyMetricsLine_RendersRollup()
    {
        var line = DailyHealthBandCalculator.BuildKeyMetricsLine("CXPACKET", 4, 150m, 87);
        Assert.Contains("Top wait: CXPACKET", line);
        Assert.Contains("High-CPU samples: 4", line);
        Assert.Contains("Total wait: 150.0 s", line);
        Assert.Contains("Unique queries: 87", line);
    }

    [Fact]
    public void BuildKeyMetricsLine_NoWait_ShowsNone_And_LargeWaitInMinutes()
    {
        var line = DailyHealthBandCalculator.BuildKeyMetricsLine(null, 0, 1200m, 0);
        Assert.Contains("Top wait: none", line);
        Assert.Contains("Total wait: 20.0 min", line); // 1200s / 60
    }

    /* ─────────── #3525 review: the still-forming day clamps to its elapsed portion ─────────── */

    [Fact]
    public void CalendarDayWindow_FinishedDay_IsTwentyFourHours()
    {
        var day = new DateTime(2026, 7, 8);
        Assert.Equal(TimeSpan.FromDays(1), DailyHealthBandCalculator.CalendarDayWindow(day, new DateTime(2026, 7, 9)));
        Assert.Equal(TimeSpan.FromDays(1), DailyHealthBandCalculator.CalendarDayWindow(day, new DateTime(2026, 9, 1, 12, 0, 0)));
    }

    [Fact]
    public void CalendarDayWindow_TodayClampsToElapsed_AndFutureIsZero()
    {
        var day = new DateTime(2026, 7, 8);
        Assert.Equal(TimeSpan.FromHours(1), DailyHealthBandCalculator.CalendarDayWindow(day, day.AddHours(1)));
        Assert.Equal(TimeSpan.FromMinutes(30), DailyHealthBandCalculator.CalendarDayWindow(day, day.AddMinutes(30)));
        Assert.Equal(TimeSpan.Zero, DailyHealthBandCalculator.CalendarDayWindow(day, day.AddDays(-1)));
    }

    [Fact]
    public void TodayCell_ActiveStorm_IsNotDilutedByUnelapsedHours()
    {
        /* The review's own numbers: 60 deadlocks in the first hour of the still-forming day. Banded over
           a full 24h the rate reads 2.5/hr (below the 5/hr Warning tier) and the crisis paints Healthy;
           over the elapsed hour it is 60/hr — past the 20/hr Critical tier. The clamp is what keeps an
           in-progress storm red on the calendar. The same 60 over a genuinely FINISHED day is honestly
           2.5/hr, and stays sub-Warning by design. */
        var day = new DateTime(2026, 7, 8);
        var stormWindow = DailyHealthBandCalculator.CalendarDayWindow(day, day.AddHours(1));
        Assert.Equal(
            DailyHealthBand.Critical,
            DailyHealthBandCalculator.Classify(Signals(deadlocks: 60, window: stormWindow)));

        var finishedWindow = DailyHealthBandCalculator.CalendarDayWindow(day, day.AddDays(2));
        Assert.Equal(
            DailyHealthBand.Healthy,
            DailyHealthBandCalculator.Classify(Signals(deadlocks: 60, window: finishedWindow)));
    }

    [Fact]
    public void TodayCell_MinutesOld_FallsToTheUnrateableArm()
    {
        /* Sub-hour elapsed lands in DeadlockSeverity's unrateable arm (#3368's Warning-not-rate rule):
           minutes into the day a single deadlock reads Warning, never a fabricated multiplied rate. */
        var window = DailyHealthBandCalculator.CalendarDayWindow(
            new DateTime(2026, 7, 8), new DateTime(2026, 7, 8, 0, 10, 0));
        Assert.Equal(
            DailyHealthBand.Warning,
            DailyHealthBandCalculator.Classify(Signals(deadlocks: 1, window: window)));
    }
}


/// <summary>
/// #3541 A9: the one decision both SKUs' daily-summary readers make about a returned day — is it a
/// measurement, or the shape retention left behind — pinned identically here and in the twin project.
///
/// <para>The defect: the daily aggregate's spine is a UNION over nine sources aging out at different
/// horizons, each COALESCEd to zero, so a day between the shortest horizon (the signals' 30 days) and the
/// longest (the alert log's 90) kept its spine row while every signal the band reads was gone — and zeros
/// band Healthy. The state is decided from two inputs, the day and the horizon; the run count only decides
/// between the two INSIDE-retention states. The horizon test comes first, because <c>runs &gt; 0</c> alone was
/// half the fix and called the whole second month collected.</para>
/// </summary>
public class DailySummaryRetentionTests
{
    private static readonly DateTime Horizon = new(2026, 8, 19);

    [Theory]
    [InlineData("2026-08-18", 1_440, 0, DailySummaryDataState.Purged)]      /* the day before: a surviving run record does not rescue it */
    [InlineData("2026-08-18", 0, 0, DailySummaryDataState.Purged)]          /* nor does the absence of one change the verdict */
    [InlineData("2026-07-01", 5, 0, DailySummaryDataState.Purged)]
    [InlineData("2026-08-18", 1_440, 7, DailySummaryDataState.PastHorizon)] /* signal rows still there: the purge has not reached it */
    [InlineData("2026-08-18", 0, 1, DailySummaryDataState.PastHorizon)]     /* even one source present withholds "purged" */
    [InlineData("2026-08-19", 1, 0, DailySummaryDataState.Collected)]       /* the horizon day itself is held */
    [InlineData("2026-09-01", 1_440, 7, DailySummaryDataState.Collected)]
    [InlineData("2026-09-01", 1_440, 0, DailySummaryDataState.Collected)]   /* inside retention a quiet day needs no signal rows to be collected */
    [InlineData("2026-09-01", 0, 3, DailySummaryDataState.NoRunRecord)]     /* inside retention, signals but no run recorded — a disclosure, the band stands */
    public void TheState_IsDecidedByTheHorizonAndPresenceFirst_ThenByTheRunCount(string day, long runs, int present, DailySummaryDataState expected)
    {
        var date = DateTime.ParseExact(day, "yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(expected, DailySummaryRetention.StateFor(date, runs, present, Horizon));
        /* A time-of-day on either side changes nothing: the decision is on DATES. */
        Assert.Equal(expected, DailySummaryRetention.StateFor(date.AddHours(23), runs, present, Horizon.AddHours(5)));
    }

    /// <summary>The horizon is the cutoff instant's DATE — see <see cref="DailySummaryRetention.HorizonFor"/>
    /// for why that is exact on the TimescaleDB path and at most a partial day generous on the DELETE path.</summary>
    [Fact]
    public void TheHorizon_IsTheCutoffsDate()
    {
        var now = new DateTime(2026, 9, 18, 14, 30, 0, DateTimeKind.Utc);
        Assert.Equal(new DateTime(2026, 8, 19), DailySummaryRetention.HorizonFor(now, 30));
        Assert.Equal(new DateTime(2026, 9, 17), DailySummaryRetention.HorizonFor(now, 1));
        Assert.Equal(DateTimeKind.Utc, DailySummaryRetention.HorizonFor(now, 30).Kind);
        Assert.Throws<ArgumentOutOfRangeException>(() => DailySummaryRetention.HorizonFor(now, 0));
    }

    [Fact]
    public void TheVocabulary_IsOneWordPerState_AndTheNoteSaysWhatTheZerosAre()
    {
        Assert.Equal("collected", DailySummaryRetention.Label(DailySummaryDataState.Collected));
        Assert.Equal("purged", DailySummaryRetention.Label(DailySummaryDataState.Purged));
        Assert.Equal("past_horizon", DailySummaryRetention.Label(DailySummaryDataState.PastHorizon));
        Assert.Equal("no_run_record", DailySummaryRetention.Label(DailySummaryDataState.NoRunRecord));

        Assert.Null(DailySummaryRetention.Note(DailySummaryDataState.Collected, Horizon));
        var purged = DailySummaryRetention.Note(DailySummaryDataState.Purged, Horizon)!;
        Assert.StartsWith("PURGED", purged, StringComparison.Ordinal);
        Assert.Contains("2026-08-19", purged, StringComparison.Ordinal);
        Assert.Contains("absences, not measurements", purged, StringComparison.Ordinal);
        var past = DailySummaryRetention.Note(DailySummaryDataState.PastHorizon, Horizon, 3)!;
        Assert.StartsWith("PAST HORIZON", past, StringComparison.Ordinal);
        Assert.Contains("3 of 7 signal sources", past, StringComparison.Ordinal);
        Assert.Equal(7, DailySummaryRetention.SignalSourceCount);
        var noRun = DailySummaryRetention.Note(DailySummaryDataState.NoRunRecord, Horizon)!;
        Assert.StartsWith("NO RUN RECORD", noRun, StringComparison.Ordinal);
        Assert.Contains("the band stands", noRun, StringComparison.Ordinal);
    }

    /// <summary>The band's own contract, end to end: a signals projection with <c>HasData</c> folded from a
    /// past-horizon state is No Data even under a Critical count — the calendar's grey, never green or red.
    /// The two inside-retention states keep the band, because inside retention a zero is a measurement.</summary>
    [Fact]
    public void APastHorizonDay_BandsNoData_WhateverItsCounts_AndAnInsideRetentionDayKeepsItsBand()
    {
        foreach (var state in new[] { DailySummaryDataState.Collected, DailySummaryDataState.NoRunRecord })
        {
            var signals = new DailyHealthSignals
            {
                HasData = state is not (DailySummaryDataState.Purged or DailySummaryDataState.PastHorizon),
                Deadlocks = 480,
                CollectionRuns = 1_440,
                Window = TimeSpan.FromDays(1),
            };
            Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(signals));
        }

        foreach (var state in new[] { DailySummaryDataState.Purged, DailySummaryDataState.PastHorizon })
        {
            var signals = new DailyHealthSignals
            {
                HasData = state is not (DailySummaryDataState.Purged or DailySummaryDataState.PastHorizon),
                Deadlocks = 480,
                CollectionRuns = 1_440,
                Window = TimeSpan.FromDays(1),
            };
            Assert.Equal(DailyHealthBand.NoData, DailyHealthBandCalculator.Classify(signals));
        }
    }
}
