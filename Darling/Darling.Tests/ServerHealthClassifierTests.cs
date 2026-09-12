/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    [Theory]
    [InlineData(0, 0.0, HealthSeverity.Healthy)]
    [InlineData(1, 0.0, HealthSeverity.Warning)]   // any blocking is Warning
    [InlineData(2, 0.0, HealthSeverity.Warning)]
    [InlineData(5, 0.0, HealthSeverity.Critical)]  // >= 5 events Critical
    [InlineData(0, 10.0, HealthSeverity.Warning)]  // >= 10s max wait Warning
    [InlineData(0, 60.0, HealthSeverity.Critical)] // >= 60s max wait Critical
    public void BlockingSeverity_BandsOnCountAndWait(int count, double maxSeconds, HealthSeverity expected) =>
        Assert.Equal(expected, ServerHealthClassifier.BlockingSeverity(count, maxSeconds));

    /// <summary>
    /// #3368: the count ladder has ONE Warning arm, so every count that is not Critical and not zero lands
    /// on the same severity. A second arm at <c>&gt;= 2</c> existed above it returning the same Warning and
    /// decided nothing.
    ///
    /// <para>Stated as a PROPERTY over the whole non-Critical range rather than as the old pair of
    /// <c>InlineData</c> rows: the rows above happened to cover 1 and 2 and would have kept passing if a
    /// third indistinguishable arm were added, where this cannot.</para>
    /// </summary>
    [Fact]
    public void BlockingSeverity_HasOneWarningArmOnTheCount()
    {
        for (var count = 1; count < 5; count++)
        {
            Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(count, 0.0));
        }

        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.BlockingSeverity(0, 0.0));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(5, 0.0));
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

    [Theory]
    [InlineData(0, HealthSeverity.Healthy)]
    [InlineData(1, HealthSeverity.Warning)]
    public void CollectorSeverity_AnyFailingWarning(int failing, HealthSeverity expected) =>
        Assert.Equal(expected, ServerHealthClassifier.CollectorSeverity(failing));

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
