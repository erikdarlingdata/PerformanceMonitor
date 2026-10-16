/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Xunit;
using Xunit.Sdk;

namespace Darling.Tests;

/// <summary>#5943: the two test-time helpers behave as their summaries say.</summary>
public sealed class TestTimeHelpersTests
{
    [Fact]
    public void WaitCore_PassesWhenTheConditionIsMetOnTheThirdPoll()
    {
        var polls = 0;
        var pauses = new List<TimeSpan>();
        var clock = TimeSpan.Zero;

        var met = TestWait.WaitCore(
            () => ++polls == 3,
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMilliseconds(100),
            pause => { pauses.Add(pause); clock += pause; },
            () => clock);

        Assert.True(met);
        Assert.Equal(3, polls);
        Assert.Equal(2, pauses.Count);
    }

    [Fact]
    public void WaitCore_ShortensTheLastPauseToTheTimeLeft_AndChecksOnceMoreAtTheTimeout()
    {
        var polls = 0;
        var pauses = new List<TimeSpan>();
        var clock = TimeSpan.Zero;

        var met = TestWait.WaitCore(
            () => { polls++; return false; },
            TimeSpan.FromMilliseconds(250),
            TimeSpan.FromMilliseconds(100),
            pause => { pauses.Add(pause); clock += pause; },
            () => clock);

        Assert.False(met);
        Assert.Equal(
            new[] { TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(50) },
            pauses);
        Assert.Equal(4, polls);
    }

    [Fact]
    public void WaitUntil_PassesWhenTheConditionIsMetOnTheThirdPoll()
    {
        var polls = 0;

        TestWait.WaitUntil(() => ++polls >= 3, TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(1));

        Assert.Equal(3, polls);
    }

    [Fact]
    public void WaitUntil_FailsWithTheElapsedTimeAndTheConditionsText_WhenTheTimeoutPasses()
    {
        var neverTrue = false;

        var failure = Assert.ThrowsAny<XunitException>(
            () => TestWait.WaitUntil(() => neverTrue, TimeSpan.FromMilliseconds(40), TimeSpan.FromMilliseconds(5)));

        Assert.Contains("timed out after", failure.Message, StringComparison.Ordinal);
        Assert.Contains("limit 40 ms", failure.Message, StringComparison.Ordinal);
        Assert.Contains("neverTrue", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TimedOutMessage_NamesTheElapsedTimeTheLimitAndTheCondition()
    {
        var message = TestWait.TimedOutMessage("file.Exists", TimeSpan.FromMilliseconds(1234), TimeSpan.FromSeconds(1));

        Assert.Equal("WaitUntil timed out after 1234 ms (limit 1000 ms) waiting for: file.Exists", message);
    }

    [Fact]
    public void TryWaitUntil_ReportsInsteadOfFailing()
    {
        Assert.True(TestWait.TryWaitUntil(() => true, TimeSpan.FromSeconds(1)));
        Assert.False(TestWait.TryWaitUntil(() => false, TimeSpan.FromMilliseconds(30), TimeSpan.FromMilliseconds(5)));
    }

    [Fact]
    public void WaitCore_RejectsAPollThatIsNotPositive()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => TestWait.WaitCore(() => true, TimeSpan.FromSeconds(1), TimeSpan.Zero, _ => { }, () => TimeSpan.Zero));
    }

    [Fact]
    public void AnchorClock_ReadsTheClockOnce_AndEveryDerivedTimeComesFromThatInstant()
    {
        var provider = new CountingTimeProvider(new DateTimeOffset(2026, 10, 16, 23, 59, 30, TimeSpan.Zero));

        var anchor = AnchorClock.Start(provider);
        var first = anchor.Now;
        provider.Advance(TimeSpan.FromMinutes(5)); // past midnight: the anchor must not follow
        var second = anchor.Now;

        Assert.Equal(first, second);
        Assert.Equal(1, provider.Reads);
        Assert.Equal(first - TimeSpan.FromMinutes(10), anchor.Ago(TimeSpan.FromMinutes(10)));
        Assert.Equal(first + TimeSpan.FromHours(1), anchor.FromNow(TimeSpan.FromHours(1)));
    }

    [Fact]
    public void AnchorClock_TodayAt_StaysOnTheAnchorsDayWhateverTheRealClockDoes()
    {
        var anchor = AnchorClock.At(new DateTimeOffset(2026, 10, 16, 23, 59, 30, TimeSpan.Zero));

        Assert.Equal(new DateTimeOffset(2026, 10, 16, 3, 15, 0, TimeSpan.Zero), anchor.TodayAt(3, 15));
        Assert.Equal(new DateTimeOffset(2026, 10, 16, 23, 0, 0, TimeSpan.Zero), anchor.TodayAt(23));
    }

    [Fact]
    public void AnchorClock_Start_WithNoProvider_UsesTheSystemClock()
    {
        var before = DateTimeOffset.UtcNow;
        var anchor = AnchorClock.Start();
        var after = DateTimeOffset.UtcNow;

        Assert.InRange(anchor.Now, before, after);
    }

    private sealed class CountingTimeProvider : TimeProvider
    {
        private DateTimeOffset _now;

        internal CountingTimeProvider(DateTimeOffset now) => _now = now;

        internal int Reads { get; private set; }

        internal void Advance(TimeSpan by) => _now += by;

        public override DateTimeOffset GetUtcNow()
        {
            Reads++;
            return _now;
        }
    }
}
