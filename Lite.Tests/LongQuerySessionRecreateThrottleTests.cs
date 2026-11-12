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
/// #8146: when the own long-query session may be recreated. A completed attempt is final for its server, database and target
/// limit for the life of the process; a failed one may run again after the hour. Only a database on the create list may be
/// recreated.
/// </summary>
public sealed class LongQuerySessionRecreateThrottleTests
{
    private static readonly DateTime T0 = new(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void AFailedAttempt_IsRefusedWithinTheHour_AndAllowedAfterIt()
    {
        var throttle = new LongQuerySessionRecreateThrottle();

        Assert.True(throttle.TryBegin("s1", null, 5000, T0));
        Assert.False(throttle.TryBegin("s1", null, 5000, T0.AddMinutes(59)));
        Assert.True(throttle.TryBegin("s1", null, 5000, T0.AddHours(1)));
        Assert.False(throttle.TryBegin("s1", null, 5000, T0.AddHours(1).AddMinutes(1)));
    }

    [Fact]
    public void ACompletedAttempt_IsNeverTriedAgain()
    {
        var throttle = new LongQuerySessionRecreateThrottle();

        Assert.True(throttle.TryBegin("s1", "alpha", 5000, T0));
        throttle.Completed("s1", "alpha", 5000);

        Assert.False(throttle.TryBegin("s1", "alpha", 5000, T0.AddHours(2)));
        Assert.False(throttle.TryBegin("s1", "alpha", 5000, T0.AddDays(30)));
    }

    [Fact]
    public void AChangedSettingValue_IsTriedOnceMore()
    {
        var throttle = new LongQuerySessionRecreateThrottle();

        Assert.True(throttle.TryBegin("s1", null, 5000, T0));
        throttle.Completed("s1", null, 5000);

        Assert.True(throttle.TryBegin("s1", null, 8000, T0.AddMinutes(1)));
    }

    [Fact]
    public void EachServerAndDatabase_HasItsOwnKey()
    {
        var throttle = new LongQuerySessionRecreateThrottle();

        Assert.True(throttle.TryBegin("s1", "alpha", 5000, T0));
        Assert.True(throttle.TryBegin("s1", "beta", 5000, T0));
        Assert.True(throttle.TryBegin("s1", null, 5000, T0));
        Assert.True(throttle.TryBegin("s2", "alpha", 5000, T0));
        Assert.False(throttle.TryBegin("s1", "ALPHA", 5000, T0.AddMinutes(1)));
    }

    [Fact]
    public void IsOnCreateList_KeepsTheServersSession_AndOnlyListedDatabases()
    {
        Assert.True(LongQuerySessionRecreate.IsOnCreateList(new LongQuerySessionRecreateRequest(null, 1000), new[] { "alpha" }));
        Assert.True(LongQuerySessionRecreate.IsOnCreateList(new LongQuerySessionRecreateRequest("alpha", 1000), new[] { "alpha" }));
        Assert.False(LongQuerySessionRecreate.IsOnCreateList(new LongQuerySessionRecreateRequest("beta", 1000), new[] { "alpha" }));
        Assert.True(LongQuerySessionRecreate.IsOnCreateList(new LongQuerySessionRecreateRequest("beta", 1000), Array.Empty<string>()));
    }
}
