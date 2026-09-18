/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared brush-free server-health classifier (#1562) — the ONE place the freshness thresholds and the six
/// per-metric severity cutoffs live, consumed by the web dashboard, the get_fleet_overview MCP tool, and the WPF
/// viewer's Overview cards. This table-driven pin reproduces the exact cutoffs the WPF <c>ServerSummaryItem</c>
/// used to own (see ViewerW2aTests) so the unification cannot silently drift a threshold, plus the fleet band +
/// worst-first score derivations.
/// </summary>
public sealed class ServerHealthClassifierTests
{
    private static readonly DateTime Now = new(2026, 7, 18, 12, 0, 0, DateTimeKind.Utc);

    /* ── freshness (2x 1-min cadence stale / 15-min offline / null never-collected) ── */

    [Fact]
    public void ClassifyFreshness_NoCollection_IsNeverCollected() =>
        Assert.Equal(ServerFreshness.NeverCollected, ServerHealthClassifier.ClassifyFreshness(null, Now));

    [Theory]
    [InlineData(0, ServerFreshness.Fresh)]
    [InlineData(120, ServerFreshness.Fresh)]     // exactly 2x cadence — still fresh
    [InlineData(121, ServerFreshness.Stale)]     // just past 2x cadence
    [InlineData(900, ServerFreshness.Stale)]     // 15 min — the OLD Offline boundary, now mid-band (#2794)
    [InlineData(1158, ServerFreshness.Stale)]    // 19m18s — the worst MEASURED legitimate sweep stretch (#2794's evidence); must never band Offline
    [InlineData(1800, ServerFreshness.Stale)]    // exactly 30 min — still stale (strict >)
    [InlineData(1801, ServerFreshness.Offline)]  // just past the shared collection-stopped window
    [InlineData(3600, ServerFreshness.Offline)]
    public void ClassifyFreshness_BandsByAge(int ageSeconds, ServerFreshness expected) =>
        Assert.Equal(expected, ServerHealthClassifier.ClassifyFreshness(Now.AddSeconds(-ageSeconds), Now));

    /* ── per-metric bands ── */

    /// <summary>The ring-buffer arm, whose reading is a fraction of a FIXED host and so is banded
    /// directly. #3281 left these cutoffs and this arm untouched; what it changed is which percentage the
    /// Performance Insights arm hands to the same ladder — see
    /// <c>PgCpuCapacityHeadroomTests</c>.</summary>
    [Theory]
    [InlineData(40.0, HealthSeverity.Healthy)]
    [InlineData(79.0, HealthSeverity.Healthy)]
    [InlineData(80.0, HealthSeverity.Warning)]
    [InlineData(94.0, HealthSeverity.Warning)]
    [InlineData(95.0, HealthSeverity.Critical)]
    [InlineData(100.0, HealthSeverity.Critical)]
    public void CpuSeverity_BandsOnTotalCpu(double cpu, HealthSeverity expected) =>
        Assert.Equal(
            expected,
            ServerHealthClassifier.CpuSeverity(cpu, null, FleetCpuSource.RingBuffer));

    [Fact]
    public void CpuSeverity_NoData_IsUnknown() =>
        Assert.Equal(
            HealthSeverity.Unknown,
            ServerHealthClassifier.CpuSeverity(null, null, FleetCpuSource.NotCollected));

    [Theory]
    [InlineData(false, HealthSeverity.Healthy)]
    [InlineData(true, HealthSeverity.Critical)]
    public void MemorySeverity_CriticalOnPressure(bool pressure, HealthSeverity expected) =>
        Assert.Equal(expected, ServerHealthClassifier.MemorySeverity(pressure));

    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);
    private static readonly TimeSpan Day = TimeSpan.FromHours(24);
    private static readonly TimeSpan Week = TimeSpan.FromHours(168);

    /* ── the blocking RATE band (#3539 A3) ── */

    /// <summary>
    /// The three arms over a one-hour window, where a count and a per-hour rate coincide: the 60 s wait arm
    /// is Critical; the count arm is Critical at 20/hr and Warning at 5/hr; the 10 s wait arm is Warning;
    /// the quiet mode (1–4 reports) is Healthy by count.
    /// </summary>
    [Theory]
    [InlineData(0, 0.0, HealthSeverity.Healthy)]
    [InlineData(1, 0.0, HealthSeverity.Healthy)]    // the quiet mode: 51 of 88 measured active hours hold 1–4
    [InlineData(4, 0.0, HealthSeverity.Healthy)]
    [InlineData(5, 0.0, HealthSeverity.Warning)]    // 5/hr, the top of the quiet mode
    [InlineData(19, 0.0, HealthSeverity.Warning)]   // inside the measured trough
    [InlineData(20, 0.0, HealthSeverity.Critical)]  // 20/hr, the lower edge of the storm mode
    [InlineData(232, 0.0, HealthSeverity.Critical)] // the worst measured hour
    [InlineData(1, 10.0, HealthSeverity.Warning)]   // the 10 s wait arm, whatever the rate
    [InlineData(1, 60.0, HealthSeverity.Critical)]  // the 60 s wait arm, whatever the rate
    public void BlockingSeverity_BandsOnRateAndWait_OverAnHour(int count, double maxSeconds, HealthSeverity expected) =>
        Assert.Equal(expected, ServerHealthClassifier.BlockingSeverity(count, maxSeconds, Hour));

    /// <summary>
    /// #3539 A3's headline: the SAME per-hour rate bands identically over an hour, a day and a week, so the
    /// count no longer means something different on every surface that windows it. Counts are
    /// integer-rate-times-whole-hours so the asserted rate is exactly the one the band sees.
    /// </summary>
    [Theory]
    [InlineData(4, HealthSeverity.Healthy)]
    [InlineData(5, HealthSeverity.Warning)]
    [InlineData(19, HealthSeverity.Warning)]
    [InlineData(20, HealthSeverity.Critical)]
    public void BlockingSeverity_TheSameRate_BandsTheSame_OverAnHourADayAndAWeek(long ratePerHour, HealthSeverity expected)
    {
        Assert.Equal(expected, ServerHealthClassifier.BlockingSeverity(ratePerHour, 0.0, Hour));
        Assert.Equal(expected, ServerHealthClassifier.BlockingSeverity(ratePerHour * 24, 0.0, Day));
        Assert.Equal(expected, ServerHealthClassifier.BlockingSeverity(ratePerHour * 168, 0.0, Week));
    }

    /// <summary>
    /// The defect as filed: five reports were Critical at a 168-hour read and Healthy at a one-hour read
    /// of the same server under <c>count &gt;= 5 → Critical</c>. Now five reports in a WEEK is 0.03/hr and
    /// Healthy by count, five in an HOUR is the Warning tier, and the same count over the two windows bands
    /// differently — which a count trigger cannot do at all, so this is the pin that goes red on a revert
    /// to counting.
    /// </summary>
    [Fact]
    public void BlockingSeverity_FiveReportsAWeek_IsNoLongerCritical()
    {
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.BlockingSeverity(5, 0.0, Week));
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(5, 0.0, Hour));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(20, 0.0, Hour));
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.BlockingSeverity(20, 0.0, Day)); // 0.8/hr
    }

    /// <summary>The wait arms are per-event magnitude claims and do NOT normalise: a 60-second block is
    /// Critical over a week exactly as over an hour, and a 10-second one is Warning — the rate has no say.</summary>
    [Fact]
    public void BlockingSeverity_TheWaitArms_AreRateIndependent()
    {
        foreach (var window in new[] { Hour, Day, Week })
        {
            Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(1, 60.0, window));
            Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(1, 10.0, window));
            Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(1, 59.9, window));
        }
    }

    /// <summary>
    /// The unrateable arm — a window under an hour or undeclared — fails away from Healthy and never into
    /// Critical by count (#3368's rule, one metric over): the wait arms still decide (they need no
    /// denominator), past them a non-zero count reads Warning even at 10,000 reports in 15 minutes, and a
    /// zero count reads Unknown, not Healthy.
    /// </summary>
    [Fact]
    public void BlockingSeverity_ASubHourOrUndeclaredWindow_FallsToWarning_NeverCriticalByCount()
    {
        foreach (var window in new[] { default, TimeSpan.FromMinutes(15), TimeSpan.FromMinutes(59) })
        {
            Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(1, 0.0, window));
            Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(10_000, 0.0, window));
            Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.BlockingSeverity(0, 0.0, window));
            Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(1, 60.0, window));
            Assert.Null(ServerHealthClassifier.BlockingRatePerHour(1, window));
        }

        Assert.Equal(1.0, ServerHealthClassifier.BlockingRatePerHour(24, Day));
        Assert.Equal(ServerHealthThresholds.DeadlockRateMinimumWindow, ServerHealthThresholds.BlockingRateMinimumWindow);
    }

    /// <summary>
    /// The tiers sit where the 14-day measurement puts them (MEASUREMENTS for #3539, 43 SQL Server
    /// primaries, 14,448 server-hours): 88 active hours, of which 51 hold 1–4 reports, 5 hold 5–10, 9 hold
    /// 11–19 and 23 hold 20 or more. Restated here as the count-per-hour histogram so a moved constant has
    /// to argue with the distribution rather than with a literal.
    /// </summary>
    [Fact]
    public void BlockingTiers_SitAtTheTopOfTheQuietMode_AndTheFootOfTheStormMode()
    {
        Assert.Equal(5.0, ServerHealthThresholds.BlockingWarnPerHour);
        Assert.Equal(20.0, ServerHealthThresholds.BlockingCriticalPerHour);
        Assert.Equal(60.0, ServerHealthThresholds.BlockingCriticalWaitSeconds);
        Assert.Equal(10.0, ServerHealthThresholds.BlockingWarnWaitSeconds);

        /* (reports in the hour, server-hours measured at that count) — the histogram's bands, at their
           upper edges. The quiet mode ends at 4 and bands Healthy by count; the storm mode begins at 20. */
        var quietMode = new (int Count, int Hours)[] { (1, 35), (2, 11), (4, 5) };
        foreach (var (count, _) in quietMode)
        {
            Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.BlockingSeverity(count, 0.0, Hour));
        }

        Assert.Equal(51, quietMode.Sum(b => b.Hours));                 // of 88 active hours
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(10, 0.0, Hour));   // 5–10: 5 hours
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(19, 0.0, Hour));   // 11–19: 9 hours
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(20, 0.0, Hour));  // 20+: 23 hours
    }

    /* ── the collector SHARE band (#3539 A8d) ── */

    /// <summary>
    /// One failing of forty and forty of forty no longer band alike: any FAILING collector is Warning, and a
    /// FAILING share past the collector-health classifier's own 20% bar is Critical. Nothing failing is
    /// Healthy whatever the denominator; no denominator with something failing is Warning and never
    /// Critical — a share nobody computed cannot escalate.
    /// </summary>
    [Theory]
    [InlineData(0, 40, HealthSeverity.Healthy)]
    [InlineData(0, 0, HealthSeverity.Healthy)]
    [InlineData(1, 40, HealthSeverity.Warning)]     // 2.5%
    [InlineData(8, 40, HealthSeverity.Warning)]     // exactly 20% — the bar is strict, as the classifier's is
    [InlineData(9, 40, HealthSeverity.Critical)]    // 22.5%
    [InlineData(40, 40, HealthSeverity.Critical)]
    [InlineData(1, 0, HealthSeverity.Warning)]      // no denominator declared
    [InlineData(40, 0, HealthSeverity.Warning)]
    public void CollectorSeverity_GradesOnTheFailingShare(int failing, int total, HealthSeverity expected) =>
        Assert.Equal(expected, ServerHealthClassifier.CollectorSeverity(failing, total));

    [Fact]
    public void CollectorSeverity_TheBar_IsTheCollectorHealthClassifiersOwn()
    {
        Assert.Equal(20.0, CollectorHealthClassifier.WarningFailureRatePercent);
        Assert.Equal(22.5, ServerHealthClassifier.FailingCollectorSharePercent(9, 40));
        Assert.Null(ServerHealthClassifier.FailingCollectorSharePercent(9, 0));
    }

    /// <summary>
    /// #3368's headline control, and the assertion this replaced said the opposite: a single deadlock in a
    /// normal window is NOT Critical. One resolved deadlock on a 43-server production fleet routinely made a
    /// server the top entry in the worst-first ranking with every other metric Healthy.
    /// </summary>
    [Fact]
    public void DeadlockSeverity_OneDeadlockInANormalWindow_IsNotCritical()
    {
        Assert.NotEqual(
            HealthSeverity.Critical,
            ServerHealthClassifier.DeadlockSeverity(1, TimeSpan.FromHours(1), DeadlockRateThresholds.Default));

        Assert.NotEqual(
            HealthSeverity.Critical,
            ServerHealthClassifier.DeadlockSeverity(1, TimeSpan.FromHours(24), DeadlockRateThresholds.Default));
    }

    /// <summary>
    /// #3368: the band reads a RATE, so the SAME raw count over two different windows bands differently.
    /// This is the one case that can tell a rate band from a count band — a suite that only ever passes one
    /// window length would stay green if someone reverted to counting.
    /// </summary>
    [Fact]
    public void DeadlockSeverity_SameCountTwoWindows_BandsDifferently()
    {
        const int count = 24;

        var tight = ServerHealthClassifier.DeadlockSeverity(
            count, TimeSpan.FromHours(1), DeadlockRateThresholds.Default);
        var wide = ServerHealthClassifier.DeadlockSeverity(
            count, TimeSpan.FromHours(24), DeadlockRateThresholds.Default);

        Assert.Equal(HealthSeverity.Critical, tight);   // 24/hr
        Assert.Equal(HealthSeverity.Healthy, wide);     // 1/hr
        Assert.NotEqual(tight, wide);
    }

    [Fact]
    public void ThreadsSeverity_NoSnapshot_IsUnknown() =>
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.ThreadsSeverity(null, null, 0, 0));

    [Fact]
    public void ThreadsSeverity_WorkQueueStarvation_IsCritical() =>
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.ThreadsSeverity(512, 412, 0, 3));

    [Fact]
    public void ThreadsSeverity_ManyRunnable_IsWarning() =>
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.ThreadsSeverity(512, 412, 20, 0));

    [Fact]
    public void ThreadsSeverity_LowAvailable_IsWarning() =>
        // available 42 < 10% of 512 (51.2)
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.ThreadsSeverity(512, 42, 0, 0));

    [Fact]
    public void ThreadsSeverity_Ample_IsHealthy() =>
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.ThreadsSeverity(512, 400, 0, 0));

    /* ── overall reduce ── */

    [Fact]
    public void OverallMetricSeverity_CriticalWins()
    {
        /* #3368: the Critical half is now a RATE, so the bundle has to declare the window its count covers
           — 30 deadlocks in an hour, well past the 20/hr tier. A bare count of 1 reads Warning here, which
           is the finding this band was changed for. */
        var m = new ServerHealthMetrics
        {
            CpuPercentForAlert = 82,                     // Warning
            DeadlockCount = 30,
            DeadlockWindow = TimeSpan.FromHours(1),      // 30/hr -> Critical
        };
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.OverallMetricSeverity(m));
    }

    [Fact]
    public void OverallMetricSeverity_WarningWhenNoCritical()
    {
        var m = new ServerHealthMetrics { CpuPercentForAlert = 82, FailedCollectorCount = 1 };
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.OverallMetricSeverity(m));
    }

    [Fact]
    public void OverallMetricSeverity_AllCalm_IsHealthy_UnknownNeverEscalates()
    {
        // No CPU snapshot (Unknown) and no threads snapshot (Unknown) must not escalate the card.
        var m = new ServerHealthMetrics { CpuPercentForAlert = null, TotalThreads = null };
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.OverallMetricSeverity(m));
    }

    /* ── measured-metric coverage (#3528) ── */

    [Fact]
    public void MeasuredMetricCounts_FullyMeasuredBundle_CountsAllSix()
    {
        var m = new ServerHealthMetrics
        {
            CpuPercentForAlert = 50,
            TotalThreads = 512,
            AvailableThreads = 400,
            HasMemoryPressure = false,
            BlockingCount = 0,
            BlockingWindow = TimeSpan.FromHours(1),      // #3539 A3: a zero count is measured only over a window
            DeadlockCount = 0,
            DeadlockWindow = TimeSpan.FromHours(1),
        };

        Assert.Equal((6, 6), ServerHealthClassifier.MeasuredMetricCounts(m));
    }

    [Fact]
    public void MeasuredMetricCounts_UnknownHeavyBundle_SaysSo_WhileTheFoldStillReadsHealthy()
    {
        /* The PostgreSQL-card shape #3528 was filed about: five of the six metrics structurally Unknown
           (no CPU/threads snapshot, DMV-sourced memory/blocking/deadlocks nulled), only the collector row
           measured. The fold deliberately skips Unknown, so the band label is still Healthy — and the
           counts are what let a consumer render that label as "Healthy — 1 of 6 measured" instead of an
           unqualified green. */
        var m = new ServerHealthMetrics();

        Assert.Equal((1, 6), ServerHealthClassifier.MeasuredMetricCounts(m));

        var overall = ServerHealthClassifier.OverallMetricSeverity(m);
        Assert.Equal(HealthSeverity.Healthy, overall);
        Assert.Equal(FleetHealthBand.Healthy,
            ServerHealthClassifier.ClassifyBand(isOnline: true, awaitingFirstCollection: false, collectionStale: false, overall));
    }

    [Fact]
    public void MeasuredMetricCounts_AreRankNeutral()
    {
        /* The counts describe, they never rank: two bundles differing only in how many metrics are
           measured score identically, which is the Unknown rank-neutrality
           UnmeasuredMetricsAreNotHealthyTests pins, restated against the new fields' own inputs. */
        var measured = new ServerHealthMetrics
        {
            CpuPercentForAlert = 50,
            TotalThreads = 512,
            AvailableThreads = 400,
            HasMemoryPressure = false,
            BlockingCount = 0,
            BlockingWindow = TimeSpan.FromHours(1),
            DeadlockCount = 0,
            DeadlockWindow = TimeSpan.FromHours(1),
        };
        var unmeasured = new ServerHealthMetrics();

        Assert.NotEqual(
            ServerHealthClassifier.MeasuredMetricCounts(measured),
            ServerHealthClassifier.MeasuredMetricCounts(unmeasured));
        Assert.Equal(
            ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Healthy, measured),
            ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Healthy, unmeasured));
    }

    /* ── fleet band (collapse) ── */

    [Fact]
    public void ClassifyBand_Offline_WhenNotOnline() =>
        Assert.Equal(FleetHealthBand.Offline,
            ServerHealthClassifier.ClassifyBand(isOnline: false, awaitingFirstCollection: false, collectionStale: false, HealthSeverity.Healthy));

    [Fact]
    public void ClassifyBand_AwaitingFirstCollection_IsWarning_NotOffline() =>
        Assert.Equal(FleetHealthBand.Warning,
            ServerHealthClassifier.ClassifyBand(isOnline: null, awaitingFirstCollection: true, collectionStale: false, HealthSeverity.Healthy));

    [Fact]
    public void ClassifyBand_CriticalMetric_IsCritical() =>
        Assert.Equal(FleetHealthBand.Critical,
            ServerHealthClassifier.ClassifyBand(isOnline: true, awaitingFirstCollection: false, collectionStale: false, HealthSeverity.Critical));

    [Fact]
    public void ClassifyBand_StaleCollectionCalmMetrics_IsWarning() =>
        Assert.Equal(FleetHealthBand.Warning,
            ServerHealthClassifier.ClassifyBand(isOnline: true, awaitingFirstCollection: false, collectionStale: true, HealthSeverity.Healthy));

    [Fact]
    public void ClassifyBand_OnlineCalm_IsHealthy() =>
        Assert.Equal(FleetHealthBand.Healthy,
            ServerHealthClassifier.ClassifyBand(isOnline: true, awaitingFirstCollection: false, collectionStale: false, HealthSeverity.Healthy));

    /* ── worst-first score ── */

    [Fact]
    public void FleetHealthScore_BandRankDominates()
    {
        var m = new ServerHealthMetrics();
        var offline = ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Offline, m);
        var critical = ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Critical, m);
        var warning = ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Warning, m);
        var healthy = ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Healthy, m);
        Assert.True(offline > critical);
        Assert.True(critical > warning);
        Assert.True(warning > healthy);
    }

    [Fact]
    public void FleetHealthScore_WithinBand_MoreBadMetricsRanksHigher()
    {
        /* #3368: both bundles declare a window, so "Critical metric" means a Critical RATE and not a
           non-zero count. */
        var worse = new ServerHealthMetrics
        {
            CpuPercentForAlert = 96,
            DeadlockCount = 30,
            DeadlockWindow = TimeSpan.FromHours(1),
        };                                                                                  // two Critical metrics
        var milder = new ServerHealthMetrics
        {
            DeadlockCount = 30,
            DeadlockWindow = TimeSpan.FromHours(1),
        };                                                                                  // one Critical metric
        Assert.True(
            ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Critical, worse) >
            ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Critical, milder));
    }

    [Fact]
    public void FleetHealthScore_WithinBandTermsNeverCrossABandBoundary()
    {
        // A Warning server maxed out on within-band magnitude (every metric bad, capped incidents) must still
        // rank below the LOWEST possible Critical (no bad metrics) — the within-band total stays under 1000.
        var maxed = new ServerHealthMetrics
        {
            CpuPercentForAlert = 96,
            HasMemoryPressure = true,
            BlockingCount = 99,
            MaxBlockedSeconds = 120,
            DeadlockCount = 99,
            DeadlockWindow = TimeSpan.FromHours(1),   // #3368: 99/hr, so this metric really is maxed
            TotalThreads = 512,
            AvailableThreads = 1,
            FailedCollectorCount = 1,
        };
        var maxedWarning = ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Warning, maxed);
        var bareCritical = ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Critical, new ServerHealthMetrics());
        Assert.True(bareCritical > maxedWarning);
    }

    [Fact]
    public void BandLabel_MapsEachBand()
    {
        Assert.Equal("Healthy", ServerHealthClassifier.BandLabel(FleetHealthBand.Healthy));
        Assert.Equal("Warning", ServerHealthClassifier.BandLabel(FleetHealthBand.Warning));
        Assert.Equal("Critical", ServerHealthClassifier.BandLabel(FleetHealthBand.Critical));
        Assert.Equal("Offline", ServerHealthClassifier.BandLabel(FleetHealthBand.Offline));
    }
}
