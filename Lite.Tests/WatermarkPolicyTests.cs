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
/// #1556/#2102: the catch-up clamp boundaries. A stale query_store watermark must not point its cutoff
/// far into the past — the per-database query's cost grows with window width, so a wide one-shot window
/// either blows the commit limit (#1556, days-wide) or times out every cycle and wedges the database
/// permanently (#2102, hours-wide). The clamp floors a stale watermark to now-MaxCatchup; a fresh
/// watermark and a null watermark pass through untouched.
/// </summary>
public sealed class WatermarkPolicyTests
{
    private static readonly DateTime Now = new(2026, 7, 17, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ClampCatchup_Null_StaysNull()
    {
        /* Null = nothing collected yet; the definition's documented first-run window applies, not a clamp. */
        Assert.Null(WatermarkPolicy.ClampCatchup(null, Now));
    }

    [Fact]
    public void ClampCatchup_WithinHorizon_ReturnedUnchanged()
    {
        /* A routine restart: the watermark is minutes old and never clamps. */
        var tenMinutesAgo = Now.AddMinutes(-10);
        Assert.Equal(tenMinutesAgo, WatermarkPolicy.ClampCatchup(tenMinutesAgo, Now));

        var justInside = Now - WatermarkPolicy.MaxCatchup + TimeSpan.FromSeconds(1);
        Assert.Equal(justInside, WatermarkPolicy.ClampCatchup(justInside, Now));
    }

    [Fact]
    public void ClampCatchup_ExactlyAtHorizon_NotClamped()
    {
        /* The floor is strict (< floor clamps): a watermark exactly MaxCatchup old is at the horizon,
           not past it. */
        var atHorizon = Now - WatermarkPolicy.MaxCatchup;
        Assert.Equal(atHorizon, WatermarkPolicy.ClampCatchup(atHorizon, Now));
    }

    [Fact]
    public void ClampCatchup_StalerThanHorizon_FlooredToNowMinusMaxCatchup()
    {
        /* The field incidents: a stale watermark is floored to now-MaxCatchup so one cycle's window is
           bounded; the skipped range is the backfill worker's job. */
        var floor = Now - WatermarkPolicy.MaxCatchup;

        Assert.Equal(floor, WatermarkPolicy.ClampCatchup(Now.AddDays(-3), Now));
        Assert.Equal(floor, WatermarkPolicy.ClampCatchup(Now.AddHours(-6), Now));

        var justPast = Now - WatermarkPolicy.MaxCatchup - TimeSpan.FromSeconds(1);
        Assert.Equal(floor, WatermarkPolicy.ClampCatchup(justPast, Now));
    }

    [Fact]
    public void ClampCatchup_FutureWatermark_ReturnedUnchanged()
    {
        /* A watermark ahead of now (clock skew) is not older than the floor, so it is left alone. */
        var future = Now.AddHours(1);
        Assert.Equal(future, WatermarkPolicy.ClampCatchup(future, Now));
    }

    [Fact]
    public void MaxCatchup_IsOneHour_AndMatchesTheBackfillSliceSpan()
    {
        /* Drift tripwire: one hour is the live path's one-query cost envelope — the width the fleet
           proves every day under Query Store's 900s flush cadence. It was 24h until #2102 showed the
           clamp sat far above the cost tipping point on big databases and never interrupted the
           timeout spiral. The equality half is the design invariant: NO path, live or backfill, may
           window wider than the other, or one of them re-becomes the wide-window casualty. */
        Assert.Equal(TimeSpan.FromHours(1), WatermarkPolicy.MaxCatchup);
        Assert.Equal(QueryStoreBackfillState.MaxSliceSpan, WatermarkPolicy.MaxCatchup);
    }

    /// <summary>
    /// #2344: the read floor must sit STRICTLY OLDER than the clamp horizon, because that ordering is the
    /// whole safety argument. A floor at or newer than the horizon could hide a row the clamp would have
    /// honoured; older by any margin cannot, since every outcome is max(stored, now - MaxCatchup) and a row
    /// below the horizon produces the same answer found or not.
    /// </summary>
    [Fact]
    public void ReadFloor_SitsStrictlyOlderThanTheClampHorizon()
    {
        var floor = WatermarkPolicy.ReadFloor(Now);

        Assert.NotNull(floor);
        Assert.True(floor < Now - WatermarkPolicy.MaxCatchup,
            "the read floor must be older than the clamp horizon, or the bound could hide a row the clamp would honour");
        Assert.Equal(Now - WatermarkPolicy.MaxCatchup - WatermarkPolicy.ReadFloorMargin, floor);
    }

    /// <summary>
    /// The equivalence the bound rests on, stated as a test rather than a comment: for any watermark at or
    /// below the read floor, the CLAMPED result is the horizon — identical to what the caller derives when
    /// the bounded read returns nothing at all (null falls back to query_store's 60-minute window, which is
    /// the same instant as the horizon). So bounding the read cannot change a single caller's outcome.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(6)]
    [InlineData(48)]
    [InlineData(24 * 90)]
    public void AnyWatermarkBelowTheReadFloor_ClampsToTheSameInstantAsFindingNothing(int hoursOld)
    {
        var floor = WatermarkPolicy.ReadFloor(Now)!.Value;
        var buried = Now.AddHours(-hoursOld);
        Assert.True(buried <= floor, "fixture must sit at or below the read floor");

        /* Found-but-old and not-found-at-all reach the same place. */
        var clampedIfFound = WatermarkPolicy.ClampCatchup(buried, Now);
        Assert.Equal(Now - WatermarkPolicy.MaxCatchup, clampedIfFound);
        Assert.Equal(Now.AddMinutes(-60), clampedIfFound);
        Assert.Null(WatermarkPolicy.ClampCatchup(null, Now));
    }

    /// <summary>A default input yields no floor — callers pass it straight through as "unbounded".</summary>
    [Fact]
    public void ReadFloor_OnDefault_IsNull()
    {
        Assert.Null(WatermarkPolicy.ReadFloor(default));
    }
}
