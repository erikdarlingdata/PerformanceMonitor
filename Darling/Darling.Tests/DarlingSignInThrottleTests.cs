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

    /// <summary>Reserves one attempt and reports that its token did not match.</summary>
    private static SignInAttempt Miss(DarlingSignInThrottle throttle, string source, DateTime now)
    {
        var attempt = throttle.BeginAttempt(source, now);
        throttle.Missed(attempt);
        return attempt;
    }

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
    public async Task ManyConcurrentAttempts_FromOneSource_ReserveExactlyTheAllowance_AndTheRestWaitOnThem()
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

        /* The free number plus the one that starts the wait is reserved; every other attempt reserves
           nothing and is told to wait on the reservations in flight. */
        var reserved = attempts.Where(a => a.IsReserved).ToList();
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, reserved.Count);
        Assert.Equal(Burst - reserved.Count, attempts.Count(a => a.IsPending));
        Assert.DoesNotContain(attempts, a => a.IsHeld);
        Assert.Equal(reserved.Count, throttle.GlobalCount);

        /* When the reservations turn out to be misses, the wait starts and the next attempt is held. */
        foreach (var attempt in reserved)
        {
            throttle.Missed(attempt);
        }

        var held = throttle.BeginAttempt("a", T0);
        Assert.True(held.IsHeld);
        Assert.Equal(SignInHold.Source, held.Hold);
        Assert.Equal(DarlingSignInThrottle.BaseDelay, held.RetryAfter);
    }

    [Fact]
    public async Task ManyConcurrentAsyncAttempts_AllMissing_CompareExactlyTheAllowance_AndTheRestAreHeld()
    {
        var throttle = new DarlingSignInThrottle();
        const int Burst = 200;
        var gate = new System.Threading.ManualResetEventSlim(false);
        var tasks = Enumerable.Range(0, Burst).Select(_ => Task.Run(async () =>
        {
            gate.Wait();
            var attempt = await throttle.BeginAttemptAsync("a", () => T0, default);
            if (attempt.IsReserved)
            {
                await Task.Yield();
                throttle.Missed(attempt);
            }

            return attempt;
        })).ToArray();

        gate.Set();
        var attempts = await Task.WhenAll(tasks);

        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, attempts.Count(a => a.IsReserved));
        Assert.Equal(Burst - (DarlingSignInThrottle.FreeFailures + 1), attempts.Count(a => a.IsHeld));
        Assert.All(attempts.Where(a => a.IsHeld), a =>
        {
            Assert.Equal(SignInHold.Source, a.Hold);
            Assert.True(a.RetryAfter > TimeSpan.Zero);
        });
    }

    [Fact]
    public async Task ManyConcurrentAsyncAttempts_AllMatching_AreAllAdmitted_AndLeaveNothingCounted()
    {
        var throttle = new DarlingSignInThrottle();
        const int Burst = 200;
        var gate = new System.Threading.ManualResetEventSlim(false);
        var tasks = Enumerable.Range(0, Burst).Select(_ => Task.Run(async () =>
        {
            gate.Wait();
            var attempt = await throttle.BeginAttemptAsync("a", () => T0, default);
            if (attempt.IsReserved)
            {
                await Task.Yield();
                throttle.Accepted(attempt);
            }

            return attempt;
        })).ToArray();

        gate.Set();
        var attempts = await Task.WhenAll(tasks);

        Assert.All(attempts, a => Assert.True(a.IsReserved));
        Assert.Equal(0, throttle.GlobalCount);
        Assert.Equal(0, throttle.TrackedCount);
    }

    [Fact]
    public async Task AMixWithinTheAllowance_AdmitsEveryMatchingAttempt_AndAMixPastItComparesOnlyTheAllowance()
    {
        /* Two missing among fourteen matching: the misses stay inside the free number, so no wait starts and
           every matching attempt is admitted, however the sixteen interleave. */
        var within = new DarlingSignInThrottle();
        var gate = new System.Threading.ManualResetEventSlim(false);
        var withinTasks = Enumerable.Range(0, 16).Select(i => Task.Run(async () =>
        {
            gate.Wait();
            var attempt = await within.BeginAttemptAsync("a", () => T0, default);
            if (attempt.IsReserved)
            {
                await Task.Yield();
                if (i < 14)
                {
                    within.Accepted(attempt);
                }
                else
                {
                    within.Missed(attempt);
                }
            }

            return attempt;
        })).ToArray();

        /* Eight matching and eight missing: once the allowance of misses is recorded the wait starts and the
           rest are held, matching ones too. */
        var past = new DarlingSignInThrottle();
        var pastTasks = Enumerable.Range(0, 16).Select(i => Task.Run(async () =>
        {
            gate.Wait();
            var attempt = await past.BeginAttemptAsync("b", () => T0, default);
            if (attempt.IsReserved)
            {
                await Task.Yield();
                if (i < 8)
                {
                    past.Accepted(attempt);
                }
                else
                {
                    past.Missed(attempt);
                }
            }

            return (attempt, wrong: i >= 8);
        })).ToArray();

        gate.Set();

        var withinResults = await Task.WhenAll(withinTasks);
        Assert.All(withinResults, a => Assert.True(a.IsReserved));

        var pastResults = await Task.WhenAll(pastTasks);
        Assert.True(pastResults.Count(r => r.attempt.IsReserved && r.wrong) <= DarlingSignInThrottle.FreeFailures + 1);
        Assert.All(pastResults.Where(r => !r.attempt.IsReserved), r => Assert.True(r.attempt.IsHeld));
    }

    [Fact]
    public async Task AnAttemptOnlyHeldUpByReservationsInFlight_WaitsForThem_AndIsAdmittedWhenOneMatches()
    {
        var throttle = new DarlingSignInThrottle();
        var inFlight = Enumerable.Range(0, DarlingSignInThrottle.FreeFailures + 1)
            .Select(_ => throttle.BeginAttempt("a", T0)).ToList();
        Assert.All(inFlight, a => Assert.True(a.IsReserved));

        Assert.True(throttle.BeginAttempt("a", T0).IsPending);
        var waiting = throttle.BeginAttemptAsync("a", () => T0, default);
        await Task.Delay(50);
        Assert.False(waiting.IsCompleted);

        /* One of the in-flight attempts matches: its reservation is given back and the waiter goes through. */
        throttle.Accepted(inFlight[0]);
        var admitted = await waiting.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(admitted.IsReserved);
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, throttle.GlobalCount);
    }

    [Fact]
    public async Task AnAttemptWaitingOnReservationsInFlight_IsHeldWithoutAComparison_WhenTheyMiss()
    {
        var throttle = new DarlingSignInThrottle();
        var inFlight = Enumerable.Range(0, DarlingSignInThrottle.FreeFailures + 1)
            .Select(_ => throttle.BeginAttempt("a", T0)).ToList();

        var waiting = throttle.BeginAttemptAsync("a", () => T0, default);
        await Task.Delay(50);
        Assert.False(waiting.IsCompleted);

        foreach (var attempt in inFlight)
        {
            throttle.Missed(attempt);
        }

        var held = await waiting.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(held.IsHeld);
        Assert.False(held.IsReserved);
        Assert.Equal(DarlingSignInThrottle.BaseDelay, held.RetryAfter);
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, throttle.GlobalCount);
    }

    [Fact]
    public async Task TheWaitOnReservationsInFlight_IsBounded_AndCancellable()
    {
        var throttle = new DarlingSignInThrottle(inFlightWaitLimit: TimeSpan.FromMilliseconds(200));
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            throttle.BeginAttempt("a", T0);
        }

        /* Nothing ever reports: the wait ends at the bound and the attempt is held, not left hanging. */
        var held = await throttle.BeginAttemptAsync("a", () => T0, default).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(held.IsHeld);
        Assert.False(held.IsReserved);

        /* A cancelled wait throws and leaves nothing reserved. */
        var slow = new DarlingSignInThrottle(inFlightWaitLimit: TimeSpan.FromMinutes(5));
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            slow.BeginAttempt("a", T0);
        }

        using var cts = new System.Threading.CancellationTokenSource();
        var waiting = slow.BeginAttemptAsync("a", () => T0, cts.Token);
        await Task.Delay(50);
        Assert.False(waiting.IsCompleted);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(DarlingSignInThrottle.FreeFailures + 1, slow.GlobalCount);
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
        Assert.Equal(SignInHold.Total, held.Hold);
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
    public void EveryLoopbackAddress_IsOneSource()
    {
        var key = DarlingSignInThrottle.SourceKey(IPAddress.Loopback);
        Assert.Equal(DarlingSignInThrottle.LoopbackKey, key);
        Assert.Equal(key, DarlingSignInThrottle.SourceKey(IPAddress.Parse("127.0.0.2")));
        Assert.Equal(key, DarlingSignInThrottle.SourceKey(IPAddress.Parse("127.255.255.254")));
        Assert.Equal(key, DarlingSignInThrottle.SourceKey(IPAddress.IPv6Loopback));
        Assert.Equal(key, DarlingSignInThrottle.SourceKey(IPAddress.Parse("::ffff:127.0.0.1")));
        Assert.Equal(key, DarlingSignInThrottle.SourceKey(IPAddress.Parse("::ffff:127.0.0.9")));

        /* Only loopback is folded: a neighbouring range and another IPv6 address in ::/64 are their own sources. */
        Assert.NotEqual(key, DarlingSignInThrottle.SourceKey(IPAddress.Parse("128.0.0.1")));
        Assert.NotEqual(key, DarlingSignInThrottle.SourceKey(IPAddress.Parse("::2")));
    }

    [Fact]
    public void ASourceWithARecentSuccess_IsNotHeldByTheTotalBudget_ButStillByItsOwnWait()
    {
        var throttle = new DarlingSignInThrottle(globalMaxAttempts: 8);

        /* "worker" presents the right token, then other sources spend the whole budget. */
        throttle.Accepted(throttle.BeginAttempt("worker", T0));
        Assert.Equal(1, throttle.RecentSuccessCount);
        for (var i = 0; i < 8; i++)
        {
            Miss(throttle, "src-" + i, T0.AddMinutes(1));
        }

        var stranger = throttle.BeginAttempt("stranger", T0.AddMinutes(2));
        Assert.True(stranger.IsHeld);
        Assert.Equal(SignInHold.Total, stranger.Hold);

        /* The source that was already working still gets its token compared, and its success is given back. */
        var worker = throttle.BeginAttempt("worker", T0.AddMinutes(2));
        Assert.True(worker.IsReserved);
        throttle.Accepted(worker);

        /* Its own wait is not lifted: misses from that source start a wait that even a right token meets. */
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            Miss(throttle, "worker", T0.AddMinutes(3));
        }

        var held = throttle.BeginAttempt("worker", T0.AddMinutes(3));
        Assert.True(held.IsHeld);
        Assert.Equal(SignInHold.Source, held.Hold);
        Assert.Equal(DarlingSignInThrottle.BaseDelay, held.RetryAfter);
    }

    [Fact]
    public void TheRecentSuccess_ExpiresAfterItsWindow_IsRenewedByASuccess_AndIsBounded()
    {
        var throttle = new DarlingSignInThrottle(globalMaxAttempts: 2);
        throttle.Accepted(throttle.BeginAttempt("worker", T0));
        var spent = T0 + DarlingSignInThrottle.RecentSuccessWindow / 2;
        Miss(throttle, "x", spent);
        Miss(throttle, "y", spent);

        /* Inside the window the worker passes; a success renews it. */
        var inside = spent + TimeSpan.FromMinutes(20);
        var renewed = throttle.BeginAttempt("worker", inside);
        Assert.True(renewed.IsReserved);
        throttle.Accepted(renewed);

        /* A source that never presented the right token is held for as long as the budget is spent. */
        Assert.Equal(SignInHold.Total, throttle.BeginAttempt("new", inside).Hold);

        /* Past the window with no success since, the worker is held like any other source. */
        var other = new DarlingSignInThrottle(globalMaxAttempts: 2);
        other.Accepted(other.BeginAttempt("worker", T0));
        var late = T0 + DarlingSignInThrottle.RecentSuccessWindow;
        Miss(other, "x", late - TimeSpan.FromMinutes(1));
        Miss(other, "y", late - TimeSpan.FromMinutes(1));
        var expired = other.BeginAttempt("worker", late);
        Assert.True(expired.IsHeld);
        Assert.Equal(SignInHold.Total, expired.Hold);

        /* The map of remembered successes has a bound. */
        var bounded = new DarlingSignInThrottle();
        for (var i = 0; i < DarlingSignInThrottle.MaxRecentSuccessSources + 50; i++)
        {
            bounded.Accepted(bounded.BeginAttempt("ok-" + i, T0.AddSeconds(i)));
        }

        Assert.Equal(DarlingSignInThrottle.MaxRecentSuccessSources, bounded.RecentSuccessCount);
    }

    [Fact]
    public void AFailureNeverEarnsTheRecentSuccess()
    {
        var throttle = new DarlingSignInThrottle(globalMaxAttempts: 3);
        Miss(throttle, "a", T0);
        Miss(throttle, "a", T0);
        Miss(throttle, "a", T0);

        Assert.Equal(0, throttle.RecentSuccessCount);
        var held = throttle.BeginAttempt("a", T0.AddMinutes(1));
        Assert.True(held.IsHeld);
        Assert.Equal(SignInHold.Total, held.Hold);
    }

    [Fact]
    public void TheBudgetConstants_AreThePublishedOnes()
    {
        Assert.Equal(64, DarlingSignInThrottle.Ipv6GroupPrefixBits);
        Assert.Equal(60, DarlingSignInThrottle.GlobalMaxAttempts);
        Assert.Equal(TimeSpan.FromHours(1), DarlingSignInThrottle.GlobalWindow);
    }
}
