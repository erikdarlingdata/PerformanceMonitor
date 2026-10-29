using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>#6927: the blocking band reads settable tiers whose defaults are the constants every earlier build used.</summary>
public sealed class BlockingBandTiersTests
{
    private static readonly TimeSpan Day = TimeSpan.FromHours(24);

    [Fact]
    public void Defaults_EqualTheCompileTimeConstants()
    {
        var d = BlockingBandThresholds.Default;
        Assert.Equal(ServerHealthThresholds.BlockingWarnPerHour, d.WarnPerHour);
        Assert.Equal(ServerHealthThresholds.BlockingCriticalPerHour, d.CriticalPerHour);
        Assert.Equal(ServerHealthThresholds.BlockingWarnWaitSeconds, d.WarnWaitSeconds);
        Assert.Equal(ServerHealthThresholds.BlockingCriticalWaitSeconds, d.CriticalWaitSeconds);
        Assert.Equal(
            ServerHealthClassifier.BlockingSeverity(1434, 162.0, Day),
            ServerHealthClassifier.BlockingSeverity(1434, 162.0, Day, d));
    }

    [Fact]
    public void ADayTunedTo100EventsAnd180Seconds_IsNotCritical_ButIsUnderTheDefaults()
    {
        /* 2026-10-23 in the issue: 1,434 reports over a day is 59.75 an hour here (73.5 on the day's own window), peak block 2.7 minutes. */
        const long reports = 1764; // 73.5 an hour over 24 hours
        const double peakSeconds = 162.0; // 2.7 minutes

        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(reports, peakSeconds, Day));

        var tuned = new BlockingBandThresholds(50.0, 100.0, 60.0, 180.0);
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(reports, peakSeconds, Day, tuned));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(2400, 100.0, Day, tuned)); // 100 an hour
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(1, 180.0, Day, tuned));
    }

    [Fact]
    public void TheDailyClassifier_UsesTheTunedTiers()
    {
        var signals = new DailyHealthSignals
        {
            Window = Day,
            BlockingReports = 1764,
            BlockingEvents = 1764,
            PeakBlockWaitMs = 162_000,
        };

        Assert.Equal(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(signals));
        var tuned = new DailyHealthThresholds { BlockingBands = new BlockingBandThresholds(50.0, 100.0, 60.0, 180.0) };
        Assert.NotEqual(DailyHealthBand.Critical, DailyHealthBandCalculator.Classify(signals, tuned));
    }

    [Fact]
    public void RawValuesClampToTheFloorsAndCeilings_AndADefaultStructClampsToTheFloorNotZero()
    {
        var wild = new BlockingBandThresholds(-5, double.NaN, 0, 1e12);
        Assert.Equal(ServerHealthThresholds.BlockingRatePerHourFloor, wild.WarnPerHour);
        Assert.Equal(ServerHealthThresholds.BlockingRatePerHourFloor, wild.CriticalPerHour);
        Assert.Equal(ServerHealthThresholds.BlockingWaitSecondsFloor, wild.WarnWaitSeconds);
        Assert.Equal(ServerHealthThresholds.BlockingWaitSecondsCeiling, wild.CriticalWaitSeconds);
        Assert.Equal(ServerHealthThresholds.BlockingRatePerHourFloor, default(BlockingBandThresholds).WarnPerHour);
    }

    [Fact]
    public void TheFleetTotalsTiles_AreBandedByTheClassifier_NotOnAnyNonZeroCount()
    {
        var now = new DateTime(2026, 10, 29, 12, 0, 0, DateTimeKind.Utc);
        var empty = DarlingFleetReader.BuildRollup(Array.Empty<FleetServerCard>(), now, now.AddHours(-1), now);
        Assert.Equal(HealthSeverity.Healthy, empty.TotalDeadlockSeverity);

        var tuned = DarlingFleetReader.BuildRollup(
            new List<FleetServerCard>(), now, now.AddHours(-1), now,
            deadlockTiers: new DeadlockRateThresholds(50.0, 100.0), blockingTiers: new BlockingBandThresholds(50.0, 100.0, 60.0, 180.0));
        Assert.Equal(HealthSeverity.Healthy, tuned.TotalDeadlockSeverity);
    }
}
