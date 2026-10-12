/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>The per-source sign-in wait, pinned without a clock: the free failures, the doubling, the cap,
/// the forgetting, the reset on success, and the bound on tracked sources.</summary>
public sealed class DarlingSignInThrottleTests
{
    private static readonly DateTime T0 = new(2026, 9, 29, 6, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void DelayAfter_IsFreeThenDoublesThenCaps()
    {
        Assert.Equal(TimeSpan.Zero, DarlingSignInThrottle.DelayAfter(0));
        Assert.Equal(TimeSpan.Zero, DarlingSignInThrottle.DelayAfter(DarlingSignInThrottle.FreeFailures));
        Assert.Equal(DarlingSignInThrottle.BaseDelay, DarlingSignInThrottle.DelayAfter(DarlingSignInThrottle.FreeFailures + 1));
        Assert.Equal(DarlingSignInThrottle.BaseDelay * 2, DarlingSignInThrottle.DelayAfter(DarlingSignInThrottle.FreeFailures + 2));
        Assert.Equal(DarlingSignInThrottle.BaseDelay * 4, DarlingSignInThrottle.DelayAfter(DarlingSignInThrottle.FreeFailures + 3));
        Assert.Equal(DarlingSignInThrottle.MaxDelay, DarlingSignInThrottle.DelayAfter(DarlingSignInThrottle.FreeFailures + 20));
        Assert.Equal(DarlingSignInThrottle.MaxDelay, DarlingSignInThrottle.DelayAfter(int.MaxValue));
    }

    [Fact]
    public void FreeFailures_CostNoWait_AndTheNextOneDoes()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            Assert.Equal(TimeSpan.Zero, throttle.RetryAfter("a", T0));
            throttle.RecordFailure("a", T0);
        }

        Assert.Equal(TimeSpan.Zero, throttle.RetryAfter("a", T0));
        throttle.RecordFailure("a", T0);
        Assert.Equal(DarlingSignInThrottle.BaseDelay, throttle.RetryAfter("a", T0));

        /* The wait counts down from the last failure, and is over once it has elapsed. */
        Assert.Equal(DarlingSignInThrottle.BaseDelay / 2, throttle.RetryAfter("a", T0 + DarlingSignInThrottle.BaseDelay / 2));
        Assert.Equal(TimeSpan.Zero, throttle.RetryAfter("a", T0 + DarlingSignInThrottle.BaseDelay));
    }

    [Fact]
    public void Sources_AreIndependent()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            throttle.RecordFailure("a", T0);
        }

        Assert.True(throttle.RetryAfter("a", T0) > TimeSpan.Zero);
        Assert.Equal(TimeSpan.Zero, throttle.RetryAfter("b", T0));
    }

    [Fact]
    public void Success_ForgetsTheSource()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            throttle.RecordFailure("a", T0);
        }

        throttle.RecordSuccess("a");
        Assert.Equal(TimeSpan.Zero, throttle.RetryAfter("a", T0));
        Assert.Equal(0, throttle.TrackedCount);
    }

    [Fact]
    public void AQuietSource_IsForgotten()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            throttle.RecordFailure("a", T0);
        }

        var later = T0 + DarlingSignInThrottle.ForgetAfter;
        Assert.Equal(TimeSpan.Zero, throttle.RetryAfter("a", later));
        Assert.Equal(0, throttle.TrackedCount);

        /* And its next failure is a first one again. */
        throttle.RecordFailure("a", later);
        Assert.Equal(TimeSpan.Zero, throttle.RetryAfter("a", later));
    }

    [Fact]
    public void TrackedSources_AreBounded_ByDroppingTheLongestQuiet()
    {
        var throttle = new DarlingSignInThrottle(maxTrackedSources: 2);
        throttle.RecordFailure("oldest", T0);
        throttle.RecordFailure("newer", T0.AddSeconds(1));
        throttle.RecordFailure("newest", T0.AddSeconds(2));

        Assert.Equal(2, throttle.TrackedCount);

        /* The dropped one is the longest quiet; a fresh failure from it starts over rather than compounding. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            throttle.RecordFailure("newer", T0.AddSeconds(3));
        }

        Assert.True(throttle.RetryAfter("newer", T0.AddSeconds(3)) > TimeSpan.Zero);
        Assert.Equal(TimeSpan.Zero, throttle.RetryAfter("oldest", T0.AddSeconds(3)));
    }
}
