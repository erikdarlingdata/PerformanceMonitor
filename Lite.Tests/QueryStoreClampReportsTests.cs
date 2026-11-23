/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9088: the rule that decides whether a stale-watermark catch-up clamp was already reported. The runner tests drive it through
/// the real collection loops. These cover the arms a loop cannot reach in test time: a gap longer than the catch-up window, and
/// databases that must not share a decision.
/// </summary>
public sealed class QueryStoreClampReportsTests
{
    private static readonly DateTime Raw = new(2026, 11, 19, 3, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T0 = Raw.AddHours(10);

    [Fact]
    public void ALandedCycleSilencesTheSameRawWatermark_WhileTheFloorIsStillInsideWhatItRead()
    {
        var reports = new QueryStoreClampReports();
        Assert.False(reports.AlreadyReported(1, "Sales", Raw, T0.AddHours(-1)));

        reports.Land(1, "Sales", Raw, T0);

        Assert.True(reports.AlreadyReported(1, "Sales", Raw, T0.AddHours(-1) + TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public void AGapLongerThanTheCatchUpWindow_IsANewGap_EvenForTheSameRawWatermark()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        /* Nothing ran for two days: the new floor lies past everything the landed cycle read, so the range between is unread. */
        var later = T0.AddDays(2);
        Assert.False(reports.AlreadyReported(1, "Sales", Raw, later - WatermarkPolicy.MaxCatchup));
    }

    [Fact]
    public void ADifferentRawWatermark_ADifferentDatabase_AndACycleThatDidNotClamp_AreNotSilenced()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        Assert.False(reports.AlreadyReported(1, "Sales", Raw.AddMinutes(1), T0.AddHours(-1)));
        Assert.False(reports.AlreadyReported(1, "Orders", Raw, T0.AddHours(-1)));
        Assert.False(reports.AlreadyReported(2, "Sales", Raw, T0.AddHours(-1)));

        reports.Land(1, "Sales", null, T0.AddMinutes(5));
        Assert.False(reports.AlreadyReported(1, "Sales", Raw, T0.AddHours(-1)));
    }

    [Fact]
    public void ANewGapAfterALandedCycle_OpensItsHoleAtWhatTheCycleRead_NotAtTheRawWatermark()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        /* Nothing ran for two days. The range from the raw watermark to T0 was reported and read by the landed cycle, so the new
           hole starts at T0. Starting it at the raw watermark made it overlap the earlier hole and re-walk what was filled (#9352). */
        Assert.Equal(T0, reports.HoleFloor(1, "Sales", Raw));
    }

    [Fact]
    public void ACycleThatRanOnTime_OnAnIdleDatabase_LeavesNothingToRecord()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        /* The next cycle, on time: its floor is inside what the landed cycle read, so it is already reported and opens no hole. */
        var next = T0.AddMinutes(5);
        var floor = next - WatermarkPolicy.MaxCatchup;
        Assert.True(reports.AlreadyReported(1, "Sales", Raw, floor));
        Assert.True(reports.HoleFloor(1, "Sales", Raw) >= floor);
    }

    [Fact]
    public void AnUnknownDatabase_AnotherRawWatermark_OrANonClampingCycle_OpenTheirHoleAtTheRawWatermark()
    {
        var reports = new QueryStoreClampReports();
        reports.Land(1, "Sales", Raw, T0);

        Assert.Equal(Raw, reports.HoleFloor(1, "Orders", Raw));
        Assert.Equal(Raw, reports.HoleFloor(2, "Sales", Raw));
        Assert.Equal(Raw.AddMinutes(1), reports.HoleFloor(1, "Sales", Raw.AddMinutes(1)));

        reports.Land(1, "Sales", null, T0.AddMinutes(5));
        Assert.Equal(Raw, reports.HoleFloor(1, "Sales", Raw));
    }
}
