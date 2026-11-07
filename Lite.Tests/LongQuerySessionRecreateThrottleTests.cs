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

/// <summary>#8146: one recreate attempt of the own long-query session per server and database per hour.</summary>
public sealed class LongQuerySessionRecreateThrottleTests
{
    private static readonly DateTime T0 = new(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TheSecondAttemptWithinTheHour_IsRefused_AndTheOneAfterTheHour_IsAllowed()
    {
        var throttle = new LongQuerySessionRecreateThrottle();

        Assert.True(throttle.TryBegin("s1", null, T0));
        Assert.False(throttle.TryBegin("s1", null, T0.AddMinutes(59)));
        Assert.True(throttle.TryBegin("s1", null, T0.AddHours(1)));
        Assert.False(throttle.TryBegin("s1", null, T0.AddHours(1).AddMinutes(1)));
    }

    [Fact]
    public void EachServerAndDatabase_HasItsOwnHour()
    {
        var throttle = new LongQuerySessionRecreateThrottle();

        Assert.True(throttle.TryBegin("s1", "alpha", T0));
        Assert.True(throttle.TryBegin("s1", "beta", T0));
        Assert.True(throttle.TryBegin("s1", null, T0));
        Assert.True(throttle.TryBegin("s2", "alpha", T0));
        Assert.False(throttle.TryBegin("s1", "ALPHA", T0.AddMinutes(1)));
    }
}
