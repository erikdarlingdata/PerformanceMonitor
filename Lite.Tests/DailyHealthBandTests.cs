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
        TimeSpan window = default) => new()
    {
        HasData = hasData,
        Deadlocks = deadlocks,
        CollectionErrors = collectionErrors,
        HighCpuEvents = highCpu,
        BlockingEvents = blocking,
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

    [Theory]
    [InlineData("collection-error", 1, 0, 0, 0, 0)]
    [InlineData("memory-critical", 0, 0, 0, 1, 0)]
    [InlineData("sustained-cpu", 0, 6, 0, 0, 0)]
    [InlineData("heavy-blocking", 0, 0, 11, 0, 0)]
    public void CriticalTriggers_EachAloneIsCritical(string _, long collErrors, long highCpu, long blocking, long memCritical, long alerts)
    {
        var s = Signals(collectionErrors: collErrors, highCpu: highCpu, blocking: blocking, memCritical: memCritical, alerts: alerts);
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

    [Theory]
    [InlineData("moderate-cpu", 3, 0, 0, 0)]
    [InlineData("some-blocking", 0, 5, 0, 0)]
    [InlineData("memory-pressure", 0, 0, 1, 0)]
    [InlineData("alert", 0, 0, 0, 1)]
    public void WarningTriggers_EachAloneIsWarning(string _, long highCpu, long blocking, long memPressure, long alerts)
    {
        var s = Signals(highCpu: highCpu, blocking: blocking, memPressure: memPressure, alerts: alerts);
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(s));
    }

    [Fact]
    public void CriticalBeatsWarning_WhenBothPresent()
    {
        // A critical deadlock rate (480/24h = 20/hr) plus moderate CPU + alerts (warning) still bands Critical.
        var s = Signals(deadlocks: 480, highCpu: 3, alerts: 4, window: Day);
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(s));
    }

    [Theory]
    [InlineData(5, DailyHealthBand.Warning)]   // 5 high-CPU samples = moderate
    [InlineData(6, DailyHealthBand.Critical)]  // 6 = sustained (default threshold)
    public void HighCpu_CriticalThreshold_IsSixSamples(long highCpu, DailyHealthBand expected)
    {
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(highCpu: highCpu)));
    }

    [Theory]
    [InlineData(10, DailyHealthBand.Warning)]  // 10 blocking events = some
    [InlineData(11, DailyHealthBand.Critical)] // 11 = heavy (default threshold)
    public void Blocking_CriticalThreshold_IsElevenEvents(long blocking, DailyHealthBand expected)
    {
        Assert.Equal(expected, DailyHealthBandCalculator.Classify(Signals(blocking: blocking)));
    }

    [Fact]
    public void Thresholds_AreOverridable()
    {
        // Same day, stricter warning thresholds: 3 high-CPU samples now clears the Critical bar.
        var strict = new DailyHealthThresholds { HighCpuCriticalSamples = 3 };
        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(Signals(highCpu: 3), strict));
        Assert.Equal(DailyHealthBand.Warning, DailyHealthBandCalculator.Classify(Signals(highCpu: 3)));
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
}
