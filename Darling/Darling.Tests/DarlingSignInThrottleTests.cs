/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>The per-source sign-in wait, pinned without a clock: the free failures, the doubling, the cap,
/// the forgetting, the one-step check-and-count under concurrency, the give-back that touches only its own
/// attempt, the /64 grouping, the total budget, and the bound on tracked sources.</summary>
public sealed class DarlingSignInThrottleTests
{
    private static readonly DateTime T0 = new(2026, 9, 29, 6, 0, 0, DateTimeKind.Utc);

    /// <summary>Reserves one attempt that misses (a miss needs no further call).</summary>
    private static SignInAttempt Miss(DarlingSignInThrottle throttle, string source, DateTime now)
        => throttle.BeginAttempt(source, now);

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
            Assert.False(Miss(throttle, "a", T0).IsHeld);
        }

        /* The attempt past the free number is still compared (it was admitted), and it starts the wait. */
        Assert.False(Miss(throttle, "a", T0).IsHeld);

        var held = throttle.BeginAttempt("a", T0);
        Assert.True(held.IsHeld);
        Assert.Equal(DarlingSignInThrottle.BaseDelay, held.RetryAfter);

        var half = throttle.BeginAttempt("a", T0 + DarlingSignInThrottle.BaseDelay / 2);
        Assert.True(half.IsHeld);
        Assert.Equal(DarlingSignInThrottle.BaseDelay / 2, half.RetryAfter);
        Assert.False(throttle.BeginAttempt("a", T0 + DarlingSignInThrottle.BaseDelay).IsHeld);
    }

    [Fact]
    public void AHeldAttempt_ReservesNothing_SoRetryingOnATimerNeverLengthensTheWait()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            Miss(throttle, "a", T0);
        }

        for (var i = 0; i < 50; i++)
        {
            Assert.True(throttle.BeginAttempt("a", T0 + TimeSpan.FromMilliseconds(i)).IsHeld);
        }

        Assert.False(throttle.BeginAttempt("a", T0 + DarlingSignInThrottle.BaseDelay).IsHeld);
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 2, throttle.GlobalCount);
    }

    [Fact]
    public void Sources_AreIndependent()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            Miss(throttle, "a", T0);
        }

        Assert.True(throttle.BeginAttempt("a", T0).IsHeld);
        Assert.False(throttle.BeginAttempt("b", T0).IsHeld);
    }

    [Fact]
    public async Task ManyConcurrentAttempts_FromOneSource_ReserveExactlyTheAllowance()
    {
        var throttle = new DarlingSignInThrottle();
        const int Burst = 500;
        var gate = new System.Threading.ManualResetEventSlim(false);
        var tasks = Enumerable.Range(0, Burst).Select(_ => Task.Run(() =>
        {
            gate.Wait();
            return throttle.BeginAttempt("a", T0);
        })).ToArray();

        gate.Set();
        var attempts = await Task.WhenAll(tasks);

        /* The free number plus the one that starts the wait; everything else is held with a wait to report. */
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, attempts.Count(a => !a.IsHeld));
        Assert.Equal(Burst - (DarlingSignInThrottle.FreeFailures + 1), attempts.Count(a => a.IsHeld));
        Assert.All(attempts.Where(a => a.IsHeld), a => Assert.True(a.RetryAfter > TimeSpan.Zero));
    }

    [Fact]
    public void ASuccess_GivesBackOnlyItsOwnAttempt_NeverTheCountOfOtherClientsOnTheSource()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            Miss(throttle, "a", T0);
        }

        var right = throttle.BeginAttempt("a", T0);
        Assert.False(right.IsHeld);
        throttle.Accepted(right);

        /* The three misses are still counted: the next one is the one that starts the wait. */
        Assert.False(Miss(throttle, "a", T0).IsHeld);
        Assert.True(throttle.BeginAttempt("a", T0).IsHeld);
    }

    [Fact]
    public void ASuccess_OnAnEmptySource_LeavesNothingTracked()
    {
        var throttle = new DarlingSignInThrottle();
        throttle.Accepted(throttle.BeginAttempt("a", T0));

        Assert.Equal(0, throttle.TrackedCount);
        Assert.Equal(0, throttle.GlobalCount);
    }

    [Fact]
    public void ASuccess_DoesNotShortenTheWaitOtherClientsEarned()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            Miss(throttle, "a", T0);
        }

        /* After the wait the operator's right token is admitted and given back; the wait the misses earned
           is neither cleared nor pushed out by the operator's attempt. */
        var later = T0 + DarlingSignInThrottle.BaseDelay;
        var right = throttle.BeginAttempt("a", later);
        Assert.False(right.IsHeld);
        throttle.Accepted(right);

        Assert.False(throttle.BeginAttempt("a", later).IsHeld);
    }

    [Fact]
    public void AQuietSource_IsForgotten()
    {
        var throttle = new DarlingSignInThrottle();
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            Miss(throttle, "a", T0);
        }

        var later = T0 + DarlingSignInThrottle.ForgetAfter;
        Assert.False(throttle.BeginAttempt("a", later).IsHeld);
        Assert.Equal(1, throttle.TrackedCount);

        /* And its next failure is a first one again: the free number is available once more. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures - 1; i++)
        {
            Assert.False(throttle.BeginAttempt("a", later).IsHeld);
        }
    }

    [Fact]
    public void TrackedSources_AreBounded_ByDroppingTheLongestQuiet()
    {
        var throttle = new DarlingSignInThrottle(maxTrackedSources: 2);
        Miss(throttle, "oldest", T0);
        Miss(throttle, "newer", T0.AddSeconds(1));
        Miss(throttle, "newest", T0.AddSeconds(2));

        Assert.Equal(2, throttle.TrackedCount);

        /* The dropped one is the longest quiet; a fresh failure from it starts over rather than compounding. */
        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            Miss(throttle, "newer", T0.AddSeconds(3));
        }

        Assert.True(throttle.BeginAttempt("newer", T0.AddSeconds(3)).IsHeld);
        Assert.False(throttle.BeginAttempt("oldest", T0.AddSeconds(3)).IsHeld);
    }

    [Fact]
    public void Ipv6Addresses_AreGroupedByTheirSlash64_AndIpv4MappedOnesAreIpv4()
    {
        var a = DarlingSignInThrottle.SourceKey(IPAddress.Parse("2001:db8:1:2::1"));
        var b = DarlingSignInThrottle.SourceKey(IPAddress.Parse("2001:db8:1:2:ffff:eeee:dddd:cccc"));
        var c = DarlingSignInThrottle.SourceKey(IPAddress.Parse("2001:db8:1:3::1"));

        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
        Assert.Equal("2001:db8:1:2::/64", a);
        Assert.Equal("10.0.0.5", DarlingSignInThrottle.SourceKey(IPAddress.Parse("::ffff:10.0.0.5")));
        Assert.Equal("10.0.0.5", DarlingSignInThrottle.SourceKey(IPAddress.Parse("10.0.0.5")));
        Assert.Equal("unknown", DarlingSignInThrottle.SourceKey(null));
    }

    [Fact]
    public void RotatingAddresses_RunIntoTheTotalBudget_NotAFreshAllowanceEach()
    {
        var throttle = new DarlingSignInThrottle(globalMaxAttempts: 10);

        /* Ten different sources, one attempt each, spend the whole budget... */
        for (var i = 0; i < 10; i++)
        {
            Assert.False(throttle.BeginAttempt("src-" + i, T0).IsHeld);
        }

        /* ...so an eleventh source, which has never failed, is held until a slot frees. */
        var held = throttle.BeginAttempt("src-new", T0);
        Assert.True(held.IsHeld);
        Assert.Equal(DarlingSignInThrottle.GlobalWindow, held.RetryAfter);

        /* The slots free as the window passes. */
        Assert.False(throttle.BeginAttempt("src-new", T0 + DarlingSignInThrottle.GlobalWindow).IsHeld);
    }

    [Fact]
    public void TheTotalBudget_IsBoundedWhateverTheSourceCount_AndASuccessGivesItsSlotBack()
    {
        var throttle = new DarlingSignInThrottle(maxTrackedSources: 4, globalMaxAttempts: 6);
        var admitted = 0;
        for (var i = 0; i < 100; i++)
        {
            /* More sources than are tracked: eviction must not reset the total. */
            if (!throttle.BeginAttempt("src-" + i, T0.AddSeconds(i)).IsHeld)
            {
                admitted++;
            }
        }

        Assert.Equal(6, admitted);
        Assert.Equal(6, throttle.GlobalCount);

        var fresh = new DarlingSignInThrottle(globalMaxAttempts: 2);
        var first = fresh.BeginAttempt("a", T0);
        fresh.Accepted(first);
        Assert.Equal(0, fresh.GlobalCount);
    }

    [Fact]
    public void TheBudgetConstants_AreThePublishedOnes()
    {
        Assert.Equal(64, DarlingSignInThrottle.Ipv6GroupPrefixBits);
        Assert.Equal(60, DarlingSignInThrottle.GlobalMaxAttempts);
        Assert.Equal(TimeSpan.FromHours(1), DarlingSignInThrottle.GlobalWindow);
    }
}
