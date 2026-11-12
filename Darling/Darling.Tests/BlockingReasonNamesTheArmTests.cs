/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7444: a fleet card's blocking reason names the arm of the band that fired it. Before, the text always named
/// the rate, so a server the longest block had put at Warning read <c>Blocking 1 (0.0 waits/hr)</c>, which
/// explains nothing and suggests that nothing is wrong. The helper reads the same arms
/// <see cref="ServerHealthClassifier.BlockingSeverity"/> does, so these tests state the wording and the
/// choice among arms, and the card tests prove both fleet surfaces use it.
/// </summary>
public sealed class BlockingReasonNamesTheArmTests
{
    private static readonly TimeSpan Day = TimeSpan.FromHours(24);

    /* Tiers with the snapshot arm on: warn at 60 s, critical at 300 s. The shipped default leaves it off. */
    private static readonly BlockingBandThresholds SnapshotOn = new(
        ServerHealthThresholds.BlockingWarnPerHour,
        ServerHealthThresholds.BlockingCriticalPerHour,
        ServerHealthThresholds.BlockingWarnWaitSeconds,
        ServerHealthThresholds.BlockingCriticalWaitSeconds,
        60,
        300);

    [Fact]
    public void TheLongestBlock_IsNamed_WhenItFiredTheBand()
    {
        // The reported case: one wait over a day is a rate near zero, the 19.4 s block is what banded.
        Assert.Equal(
            HealthSeverity.Warning,
            ServerHealthClassifier.BlockingSeverity(1, 19.4, Day));
        Assert.Equal(
            "Blocking 1 (longest block 19.4 s)",
            ServerHealthClassifier.BlockingReason(1, 19.4, Day));
    }

    [Fact]
    public void TheSnapshotWait_IsNamed_WhenItFiredTheBand()
    {
        Assert.Equal(
            HealthSeverity.Warning,
            ServerHealthClassifier.BlockingSeverity(1, 0, Day, SnapshotOn, 120));
        Assert.Equal(
            "Blocking 1 (peak snapshot 2.0 min)",
            ServerHealthClassifier.BlockingReason(1, 0, Day, SnapshotOn, 120));
    }

    [Fact]
    public void TheRate_IsNamed_WhenItFiredTheBand()
    {
        var window = TimeSpan.FromHours(3.2);

        Assert.Equal(
            HealthSeverity.Warning,
            ServerHealthClassifier.BlockingSeverity(40, 0.5, window));
        Assert.Equal(
            "Blocking 40 (12.5 waits/hr)",
            ServerHealthClassifier.BlockingReason(40, 0.5, window));
    }

    [Fact]
    public void WhenTheRateAndTheWaitBothFired_TheHigherSeverityIsNamed_AndTheWaitWinsATie()
    {
        // 100 waits in 2 hours is 50/hr, past the 20/hr critical bar. A 12 s block is only Warning.
        Assert.Equal(
            "Blocking 100 (50.0 waits/hr)",
            ServerHealthClassifier.BlockingReason(100, 12, TimeSpan.FromHours(2)));

        // 90 s is past the 60 s critical bar and the rate is only Warning (6/hr).
        Assert.Equal(
            "Blocking 6 (longest block 1.5 min)",
            ServerHealthClassifier.BlockingReason(6, 90, TimeSpan.FromHours(1)));

        // Both Warning: 12 s block and 6/hr. The wait arm wins the tie.
        Assert.Equal(
            "Blocking 6 (longest block 12.0 s)",
            ServerHealthClassifier.BlockingReason(6, 12, TimeSpan.FromHours(1)));

        // The snapshot arm Critical beats a Warning wait, and ties with a Critical rate go to the snapshot.
        Assert.Equal(
            "Blocking 1 (peak snapshot 6.0 min)",
            ServerHealthClassifier.BlockingReason(1, 12, Day, SnapshotOn, 360));
        Assert.Equal(
            "Blocking 100 (peak snapshot 6.0 min)",
            ServerHealthClassifier.BlockingReason(100, 0, TimeSpan.FromHours(2), SnapshotOn, 360));
    }

    [Fact]
    public void TheCriticalLongestBlock_IsNamed()
    {
        Assert.Equal(
            HealthSeverity.Critical,
            ServerHealthClassifier.BlockingSeverity(1, 78, Day));
        Assert.Equal(
            "Blocking 1 (longest block 1.3 min)",
            ServerHealthClassifier.BlockingReason(1, 78, Day));
    }

    [Fact]
    public void AnUnrateableWindowWithNoWaitFired_PrintsTheCountAlone()
    {
        Assert.Equal(
            HealthSeverity.Warning,
            ServerHealthClassifier.BlockingSeverity(3, 2, TimeSpan.FromMinutes(5)));
        Assert.Equal(
            "Blocking 3",
            ServerHealthClassifier.BlockingReason(3, 2, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public void TheStoresConfiguredTiers_DecideWhatFired_NotTheDefaults()
    {
        // The store lowered the wait bar to 5 s: a 6 s block fires the band that the 10 s default would not.
        var lowered = new BlockingBandThresholds(
            ServerHealthThresholds.BlockingWarnPerHour,
            ServerHealthThresholds.BlockingCriticalPerHour,
            5,
            ServerHealthThresholds.BlockingCriticalWaitSeconds);

        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.BlockingSeverity(1, 6, Day));
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(1, 6, Day, lowered));
        Assert.Equal(
            "Blocking 1 (longest block 6.0 s)",
            ServerHealthClassifier.BlockingReason(1, 6, Day, lowered));
    }

    [Fact]
    public void EveryWarningOnARateableWindow_NamesAnArm_AndNeverARateThatDidNotFire()
    {
        var counts = new long[] { 1, 6, 40 };
        var waits = new[] { 0.0, 2.0, 12.0, 90.0 };
        var windows = new[] { TimeSpan.FromHours(1), TimeSpan.FromHours(24) };
        double?[] snapshots = { null, 0.0, 120.0, 400.0 };

        foreach (var count in counts)
        {
            foreach (var wait in waits)
            {
                foreach (var window in windows)
                {
                    foreach (var snapshot in snapshots)
                    {
                        var severity = ServerHealthClassifier.BlockingSeverity(count, wait, window, SnapshotOn, snapshot);
                        var reason = ServerHealthClassifier.BlockingReason(count, wait, window, SnapshotOn, snapshot);
                        var rate = ServerHealthClassifier.BlockingRatePerHour(count, window)!.Value;
                        var context = $"count {count}, wait {wait}, window {window}, snapshot {snapshot}: {reason}";

                        if (severity >= HealthSeverity.Warning)
                        {
                            Assert.True(reason.EndsWith(')'), context);
                        }

                        if (reason.Contains("waits/hr", StringComparison.Ordinal))
                        {
                            Assert.True(rate >= SnapshotOn.WarnPerHour, context);
                        }
                    }
                }
            }
        }
    }

    [Fact]
    public void TheServiceFleetCard_NamesTheLongestBlock()
    {
        var card = new FleetServerCard
        {
            IsOnline = true,
            BlockingCount = 1,
            MaxBlockingWaitMs = 19_400,
            BlockingWindow = Day,
            BlockingSeverity = ServerHealthClassifier.BlockingSeverity(1, 19.4, Day),
        };

        Assert.Equal(HealthSeverity.Warning, card.BlockingSeverity);
        Assert.Contains("Blocking 1 (longest block 19.4 s)", DarlingFleetReader.BuildReason(card), StringComparison.Ordinal);
        Assert.DoesNotContain("waits/hr", DarlingFleetReader.BuildReason(card), StringComparison.Ordinal);
    }

    [Fact]
    public void TheServiceFleetCard_NamesThePeakSnapshot_UnderTheStoresTiers()
    {
        var card = new FleetServerCard
        {
            IsOnline = true,
            BlockingCount = 1,
            MaxBlockingWaitMs = 0,
            MaxSnapshotWaitMs = 120_000,
            BlockingWindow = Day,
            BlockingBandThresholds = SnapshotOn,
            BlockingSeverity = ServerHealthClassifier.BlockingSeverity(1, 0, Day, SnapshotOn, 120),
        };

        Assert.Contains("Blocking 1 (peak snapshot 2.0 min)", DarlingFleetReader.BuildReason(card), StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewerFleetCard_NamesTheLongestBlock_AndTheRate()
    {
        var longest = new ServerSummaryItem
        {
            IsOnline = true,
            BlockingCount = 1,
            MaxBlockingWaitMs = 19_400,
            BlockingWindow = Day,
        };

        Assert.Equal(HealthSeverity.Warning, longest.BlockingSeverity);
        Assert.Contains("Blocking 1 (longest block 19.4 s)", FleetRollup.BuildReason(longest), StringComparison.Ordinal);

        var rate = new ServerSummaryItem
        {
            IsOnline = true,
            BlockingCount = 40,
            MaxBlockingWaitMs = 500,
            BlockingWindow = TimeSpan.FromHours(3.2),
        };

        Assert.Contains("Blocking 40 (12.5 waits/hr)", FleetRollup.BuildReason(rate), StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewerFleetCard_NamesThePeakSnapshot()
    {
        var snapshot = new ServerSummaryItem
        {
            IsOnline = true,
            BlockingCount = 1,
            MaxBlockingWaitMs = 0,
            PeakSnapshotWaitMs = 120_000,
            BlockingWindow = Day,
            BlockingBandThresholds = SnapshotOn,
        };

        Assert.Equal(HealthSeverity.Warning, snapshot.BlockingSeverity);
        Assert.Contains("Blocking 1 (peak snapshot 2.0 min)", FleetRollup.BuildReason(snapshot), StringComparison.Ordinal);
    }
}
