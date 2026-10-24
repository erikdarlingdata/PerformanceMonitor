/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Darling.Tests;

/// <summary>
/// The waits a test may use instead of <c>Thread.Sleep</c> (#5943), which is banned in both test projects
/// (<c>BannedSymbols.txt</c>).
///
/// <para><b>Which one.</b> A test that waits for something to happen uses <see cref="WaitUntil"/>: it polls the
/// condition and fails with the elapsed time and the condition's own text, so a slow runner gets a long timeout
/// and a fast one never waits the full time. A background loop that needs a pace between polls waits on its
/// cancellation token's <c>WaitHandle</c> instead. <see cref="Elapse"/> is for the one case where time passing IS
/// the stimulus (a stand-in for a slow call, a lock held for a set time): it is not a wait for anything, and a test
/// that can name the thing it is waiting for must use <see cref="WaitUntil"/>.</para>
///
/// <para>Linked into Lite.Tests (see <c>Lite.Tests.csproj</c> and <c>lite_linked_shard</c> in
/// <c>.github/lite-paths-filter.yml</c>).</para>
/// </summary>
internal static class TestWait
{
    private static readonly TimeSpan s_defaultPoll = TimeSpan.FromMilliseconds(10);

    /// <summary>
    /// Polls <paramref name="condition"/> until it is true. Fails the test when <paramref name="timeout"/> passes
    /// first, with the elapsed time and the condition's source text. The condition is checked once more at the
    /// timeout, so a condition that becomes true during the last pause still passes.
    /// </summary>
    internal static void WaitUntil(
        Func<bool> condition,
        TimeSpan timeout,
        TimeSpan? poll = null,
        [CallerArgumentExpression(nameof(condition))] string? conditionName = null)
    {
        var started = TimeProvider.System.GetTimestamp();
        if (!WaitCore(condition, timeout, poll ?? s_defaultPoll, Pause, () => TimeProvider.System.GetElapsedTime(started)))
        {
            Xunit.Assert.Fail(TimedOutMessage(conditionName, TimeProvider.System.GetElapsedTime(started), timeout));
        }
    }

    /// <summary>
    /// <see cref="WaitUntil"/> that reports instead of failing: true when the condition became true in time. For a
    /// retry whose last error the caller still needs to throw (a folder that stays locked).
    /// </summary>
    internal static bool TryWaitUntil(Func<bool> condition, TimeSpan timeout, TimeSpan? poll = null)
    {
        var started = TimeProvider.System.GetTimestamp();
        return WaitCore(condition, timeout, poll ?? s_defaultPoll, Pause, () => TimeProvider.System.GetElapsedTime(started));
    }

    /// <summary>
    /// Holds the calling thread for <paramref name="duration"/> because the elapsed time is the test's input (a
    /// fake slow call, a lock held open), not because anything is being waited for.
    /// </summary>
    internal static void Elapse(TimeSpan duration)
    {
        /* A timed wait on Windows can return up to one timer tick early, so a single Pause can hold for less than it was
           asked to (#6704's CI: 60 ms asked, 47 ms measured by Stopwatch). Pause again for whatever is left. */
        var started = TimeProvider.System.GetTimestamp();
        for (var left = duration; left > TimeSpan.Zero; left = duration - TimeProvider.System.GetElapsedTime(started))
        {
            Pause(left);
        }
    }

    /// <summary>The polling loop, with the pause and the clock passed in so the unit tests need no real time.</summary>
    internal static bool WaitCore(Func<bool> condition, TimeSpan timeout, TimeSpan poll, Action<TimeSpan> pause, Func<TimeSpan> elapsed)
    {
        ArgumentNullException.ThrowIfNull(condition);
        if (poll <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(poll), poll, "The poll interval must be positive.");
        }

        while (true)
        {
            if (condition())
            {
                return true;
            }

            var remaining = timeout - elapsed();
            if (remaining <= TimeSpan.Zero)
            {
                return false;
            }

            pause(remaining < poll ? remaining : poll);
        }
    }

    internal static string TimedOutMessage(string? conditionName, TimeSpan elapsed, TimeSpan timeout) =>
        $"WaitUntil timed out after {elapsed.TotalMilliseconds:F0} ms (limit {timeout.TotalMilliseconds:F0} ms) waiting for: {conditionName ?? "(unnamed condition)"}";

    /* An event that is never set: Wait(duration) is a plain timed block, with no Thread.Sleep in it. */
    private static void Pause(TimeSpan duration)
    {
        using var never = new ManualResetEventSlim(false);
        never.Wait(duration);
    }
}

/// <summary>
/// One fixed instant for a test to compute its expected values from (#5943), so the test never reads the real
/// clock twice and never straddles midnight or a minute boundary between the setup and the assertion.
///
/// <para><b>When to use it.</b> A test that seeds rows at "ten minutes ago" and later asserts a window over "the last
/// hour" takes <see cref="Start"/> once and derives every timestamp from <see cref="Now"/>, <see cref="Ago"/>,
/// <see cref="FromNow"/> and <see cref="TodayAt"/>. A live test whose rows are stamped by the store's own
/// <c>now()</c> reads that instant once from the store and passes it to <see cref="At"/>. A test that must control
/// the clock inside product code needs a <see cref="TimeProvider"/> in that code, which this does not replace.</para>
/// </summary>
internal sealed class AnchorClock
{
    private AnchorClock(DateTimeOffset now) => Now = now;

    /// <summary>The anchor. The same value on every read.</summary>
    internal DateTimeOffset Now { get; }

    /// <summary>Reads the clock once (the system clock, or <paramref name="provider"/>'s) and keeps that instant.</summary>
    internal static AnchorClock Start(TimeProvider? provider = null) => new((provider ?? TimeProvider.System).GetUtcNow());

    /// <summary>An anchor at an instant the test already has (the store's <c>now()</c>, a pinned date).</summary>
    internal static AnchorClock At(DateTimeOffset instant) => new(instant);

    /// <summary><paramref name="age"/> before the anchor.</summary>
    internal DateTimeOffset Ago(TimeSpan age) => Now - age;

    /// <summary><paramref name="delay"/> after the anchor.</summary>
    internal DateTimeOffset FromNow(TimeSpan delay) => Now + delay;

    /// <summary>
    /// The anchor's own UTC day at <paramref name="hour"/>:<paramref name="minute"/>. It can be later than
    /// <see cref="Now"/>; the day never moves under the test, which is the point.
    /// </summary>
    internal DateTimeOffset TodayAt(int hour, int minute = 0) =>
        new(Now.UtcDateTime.Date.AddHours(hour).AddMinutes(minute), TimeSpan.Zero);
}
