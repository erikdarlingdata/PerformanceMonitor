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

    /* ── #3236: the cadence-aware stale cutoff — a server is stale only when every enabled scheduled
       collector is overdue against ITS OWN schedule, not against the flat 2-minute floor ── */

    [Theory]
    [InlineData(0, 2)]      // on-load / unknown — the floor
    [InlineData(1, 2)]      // a 1-min collector keeps the flat threshold exactly
    [InlineData(5, 10)]     // pg_cpu_utilization's tier: twice its own cadence
    [InlineData(1440, 2880)] // a daily collector — wide, and the Offline band still fires first at 30 min
    public void StaleThresholdFor_IsTwiceTheCadenceFlooredAtTheFlatThreshold(int frequencyMinutes, int expectedMinutes) =>
        Assert.Equal(TimeSpan.FromMinutes(expectedMinutes), ServerHealthThresholds.StaleThresholdFor(frequencyMinutes));

    [Fact]
    public void EffectiveStaleThreshold_NoSamples_IsTheFlatFloor()
    {
        Assert.Equal(
            ServerHealthThresholds.StaleThreshold,
            ServerHealthClassifier.EffectiveStaleThreshold(Now, Array.Empty<CollectorCadenceSample>()));
        Assert.Equal(
            ServerHealthThresholds.StaleThreshold,
            ServerHealthClassifier.EffectiveStaleThreshold(Now, null!));
    }

    [Fact]
    public void EffectiveStaleThreshold_TheLatestCollectorExpiryWins()
    {
        var lastCollection = Now;

        /* A 1-min collector current at the newest collection holds the floor; a 5-min collector whose
           newest run is 60s older still vouches for 10min - 60s = 9min of the newest collection's age. */
        var threshold = ServerHealthClassifier.EffectiveStaleThreshold(
            lastCollection,
            new[]
            {
                new CollectorCadenceSample(lastCollection, 1),
                new CollectorCadenceSample(lastCollection.AddSeconds(-60), 5),
            });

        Assert.Equal(TimeSpan.FromMinutes(9), threshold);
    }

    [Fact]
    public void EffectiveStaleThreshold_AStragglerCannotTightenTheFloor()
    {
        /* A 1-min collector that stopped 10 minutes before the newest collection contributes a negative
           term; the floor is the answer, never less. */
        var threshold = ServerHealthClassifier.EffectiveStaleThreshold(
            Now, new[] { new CollectorCadenceSample(Now.AddMinutes(-10), 1) });

        Assert.Equal(ServerHealthThresholds.StaleThreshold, threshold);
    }

    /// <summary>
    /// The #3236 field case, to the second: the fleet snapshot landed 3m49s after the newest collection on
    /// a target whose metric-driving collector (<c>pg_cpu_utilization</c>) legitimately runs every 5
    /// minutes. On the flat floor that banded Warning — 14 healthy Aurora targets at once — and against
    /// the collector's own schedule it is simply on time.
    /// </summary>
    [Fact]
    public void ClassifyFreshness_FiveMinuteCadenceCollector_FourMinutesOld_IsFreshNotStale()
    {
        var lastCollection = Now.AddSeconds(-229);
        var samples = new[] { new CollectorCadenceSample(lastCollection, 5) };

        var threshold = ServerHealthClassifier.EffectiveStaleThreshold(lastCollection, samples);

        Assert.Equal(TimeSpan.FromMinutes(10), threshold);
        Assert.Equal(ServerFreshness.Fresh, ServerHealthClassifier.ClassifyFreshness(lastCollection, Now, threshold));

        /* The flat overload still calls this Stale — that IS the false positive this fix removes, kept
           here as the statement of what changed rather than deleted as an inconvenience. */
        Assert.Equal(ServerFreshness.Stale, ServerHealthClassifier.ClassifyFreshness(lastCollection, Now));
    }

    /// <summary>The failure direction stays safe: past twice its OWN cadence, the same collector bands.</summary>
    [Fact]
    public void ClassifyFreshness_FiveMinuteCadenceCollector_PastTwiceItsCadence_IsStale()
    {
        var lastCollection = Now.AddSeconds(-601);
        var samples = new[] { new CollectorCadenceSample(lastCollection, 5) };

        var threshold = ServerHealthClassifier.EffectiveStaleThreshold(lastCollection, samples);

        Assert.Equal(ServerFreshness.Stale, ServerHealthClassifier.ClassifyFreshness(lastCollection, Now, threshold));
    }

    /// <summary>
    /// The Offline band is NOT cadence-relative (#2794's 30-minute contract with the alert engine): a
    /// slow-cadence server whose stale window would stretch past it simply has no amber band, and the red
    /// one still fires on schedule.
    /// </summary>
    [Fact]
    public void ClassifyFreshness_CadenceAwareCutoff_NeverMovesTheOfflineBand()
    {
        var lastCollection = Now.AddSeconds(-1801);
        var threshold = ServerHealthClassifier.EffectiveStaleThreshold(
            lastCollection, new[] { new CollectorCadenceSample(lastCollection, 30) });

        Assert.True(threshold > ServerHealthThresholds.OfflineThreshold);
        Assert.Equal(ServerFreshness.Offline, ServerHealthClassifier.ClassifyFreshness(lastCollection, Now, threshold));
    }

    /* ── per-metric bands ── */

    [Theory]
    [InlineData(40.0, HealthSeverity.Healthy)]
    [InlineData(79.0, HealthSeverity.Healthy)]
    [InlineData(80.0, HealthSeverity.Warning)]
    [InlineData(94.0, HealthSeverity.Warning)]
    [InlineData(95.0, HealthSeverity.Critical)]
    [InlineData(100.0, HealthSeverity.Critical)]
    public void CpuSeverity_BandsOnTotalCpu(double cpu, HealthSeverity expected) =>
        Assert.Equal(expected, ServerHealthClassifier.CpuSeverity(cpu));

    [Fact]
    public void CpuSeverity_NoData_IsUnknown() =>
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.CpuSeverity(null));

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

    [Theory]
    [InlineData(0, HealthSeverity.Healthy)]
    [InlineData(1, HealthSeverity.Critical)]
    public void DeadlockSeverity_AnyDeadlockCritical(int count, HealthSeverity expected) =>
        Assert.Equal(expected, ServerHealthClassifier.DeadlockSeverity(count));

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
        var m = new ServerHealthMetrics { CpuPercentForAlert = 82, DeadlockCount = 1 }; // Warning + Critical
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
        var worse = new ServerHealthMetrics { CpuPercentForAlert = 96, DeadlockCount = 1 }; // two Critical metrics
        var milder = new ServerHealthMetrics { DeadlockCount = 1 };                          // one Critical metric
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
