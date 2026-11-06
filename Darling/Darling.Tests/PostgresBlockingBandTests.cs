/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7616: the PostgreSQL blocking band is the share of the captures actually taken, scaled to the default one-a-minute cadence
/// (<c>withBlocking / taken * 60</c> an hour), against the store's per-hour tiers (5 and 20). A rate over the nominal hour reads
/// quieter when the collector ran a slower schedule or only part of the window (#3695), and 2 of 2 would read Critical for a target
/// registered a minute ago, so fewer than 3 captures is Unknown.
/// </summary>
public sealed class PostgresBlockingBandTests
{
    [Fact]
    public void NoCaptureInTheWindow_IsUnknownWithNoRate()
    {
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.PostgresBlockingSeverity(null, 0));
        Assert.Null(ServerHealthClassifier.PostgresBlockingRatePerHour(null, 0));
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(0, 2)]
    public void FewerThanThreeCaptures_IsUnknown_NotCritical(long withBlocking, long taken)
    {
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.PostgresBlockingSeverity(withBlocking, taken));
        Assert.Null(ServerHealthClassifier.PostgresBlockingRatePerHour(withBlocking, taken));
        Assert.Equal("fewer than 3 captures in this window", ServerHealthClassifier.PostgresBlockingTooFewCapturesDetail);
    }

    [Fact]
    public void ThreeBlockedOfTwelveTakenAtAFiveMinuteCadence_Is15PerHour_Warning()
    {
        Assert.Equal(15.0, ServerHealthClassifier.PostgresBlockingRatePerHour(3, 12));
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.PostgresBlockingSeverity(3, 12));
    }

    [Fact]
    public void TwelveBlockedOfTwelveTaken_Is60PerHour_Critical()
    {
        Assert.Equal(60.0, ServerHealthClassifier.PostgresBlockingRatePerHour(12, 12));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.PostgresBlockingSeverity(12, 12));
    }

    [Fact]
    public void TwoBlockedOf59Sampled_StaysBelowWarning()
    {
        var rate = ServerHealthClassifier.PostgresBlockingRatePerHour(2, 59);
        Assert.Equal(2.03, rate!.Value, 2);
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.PostgresBlockingSeverity(2, 59));
    }

    [Fact]
    public void OnlyTenCapturesTaken_DoesNotReadQuieterThanAFullHourWithTheSameShare()
    {
        /* 5 of 10 and 30 of 60 are the same share, so the same rate and the same band. A division by the nominal hour read the first as 5 an hour. */
        Assert.Equal(
            ServerHealthClassifier.PostgresBlockingRatePerHour(30, 60),
            ServerHealthClassifier.PostgresBlockingRatePerHour(5, 10));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.PostgresBlockingSeverity(5, 10));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.PostgresBlockingSeverity(30, 60));
    }

    [Fact]
    public void TheStoresTiers_AreWhatTheBandReads()
    {
        var tiers = BlockingBandThresholds.Default with { WarnPerHourRaw = 30, CriticalPerHourRaw = 50 };
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.PostgresBlockingSeverity(5, 12, tiers));
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.PostgresBlockingSeverity(6, 12, tiers));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.PostgresBlockingSeverity(10, 12, tiers));
    }

    [Fact]
    public void ATierAbove60_ReadsAsEveryCapture_SoAFullyBlockedTargetStillReachesCritical()
    {
        /* The rate cannot pass 60 an hour but the stored tiers accept 1 to 10000 (#7616). Unclamped, a Critical tier of 80 or a
           Warning tier of 70 left a target blocked in every capture of the hour Healthy. */
        var raised = BlockingBandThresholds.Default with { WarnPerHourRaw = 70, CriticalPerHourRaw = 80 };
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.PostgresBlockingSeverity(12, 12, raised));

        var onlyCriticalRaised = BlockingBandThresholds.Default with { WarnPerHourRaw = 20, CriticalPerHourRaw = 80 };
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.PostgresBlockingSeverity(12, 12, onlyCriticalRaised));
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.PostgresBlockingSeverity(6, 12, onlyCriticalRaised));
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.PostgresBlockingSeverity(2, 59, onlyCriticalRaised));
    }

    [Fact]
    public void ATierAbove60_StillBandsAHalfBlockedTargetByTheTiersBelow60()
    {
        var warnRaised = BlockingBandThresholds.Default with { WarnPerHourRaw = 70, CriticalPerHourRaw = 5000 };
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.PostgresBlockingSeverity(6, 12, warnRaised));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.PostgresBlockingSeverity(11, 11, warnRaised));

        var criticalOnly = BlockingBandThresholds.Default with { WarnPerHourRaw = 10, CriticalPerHourRaw = 70 };
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.PostgresBlockingSeverity(6, 12, criticalOnly));
    }

    [Fact]
    public void TheSameRaisedTiers_BandASqlServerCountAsBefore()
    {
        /* SQL Server banding must not change: 80 blocked waits in an hour is a rate of 80, under a Critical tier of 100. */
        var raised = BlockingBandThresholds.Default with { WarnPerHourRaw = 70, CriticalPerHourRaw = 100 };
        var hour = System.TimeSpan.FromHours(1);
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(80, 0, hour, raised, null));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(100, 0, hour, raised, null));
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.BlockingSeverity(60, 0, hour, raised, null));
    }

    [Fact]
    public void TheMetricsBand_AppliesTheSameClampForARaisedTier()
    {
        var raised = BlockingBandThresholds.Default with { WarnPerHourRaw = 70, CriticalPerHourRaw = 80 };
        var m = new ServerHealthMetrics { BlockingCount = 12, BlockingCapturesTaken = 12, BlockingBandThresholds = raised };
        Assert.Equal(HealthSeverity.Critical, System.Linq.Enumerable.ElementAt(ServerHealthClassifier.MetricSeverities(m), 3));
    }

    [Fact]
    public void TheReasonNamesTheUnitAndTheRate_AndDropsTheRateBelowThreeCaptures()
    {
        Assert.Equal("Blocking 25 captures (25.0/hr)", ServerHealthClassifier.PostgresBlockingReason(25, 60));
        Assert.Equal("Blocking 1 capture (1.0/hr)", ServerHealthClassifier.PostgresBlockingReason(1, 60));
        Assert.Equal("Blocking 2 captures", ServerHealthClassifier.PostgresBlockingReason(2, 2));
    }

    [Fact]
    public void TheMetricsBand_ReadsTheSameShareAsTheClassifier()
    {
        var m = new ServerHealthMetrics { BlockingCount = 12, BlockingCapturesTaken = 12 };
        Assert.Equal(HealthSeverity.Critical, System.Linq.Enumerable.ElementAt(ServerHealthClassifier.MetricSeverities(m), 3));
        Assert.Equal("Blocking 12 captures (60.0/hr)", ServerHealthClassifier.BlockingReason(m));
        var young = new ServerHealthMetrics { BlockingCount = 1, BlockingCapturesTaken = 1 };
        Assert.Equal(HealthSeverity.Unknown, System.Linq.Enumerable.ElementAt(ServerHealthClassifier.MetricSeverities(young), 3));
    }

    [Theory]
    [InlineData(true, true, "SQL Server blocked waits plus PostgreSQL blocking captures, in one number")]
    [InlineData(true, false, "blocked waits")]
    [InlineData(false, true, "blocking captures")]
    [InlineData(false, false, null)]
    public void TheBlockingTotalNamesEveryUnitItAdds(bool sql, bool postgres, string? expected)
    {
        Assert.Equal(expected, ServerHealthClassifier.BlockingTotalUnitsDetail(sql, postgres));
    }
}
