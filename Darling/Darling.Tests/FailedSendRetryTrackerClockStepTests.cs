/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4732: <see cref="FailedSendRetryTracker.RetryPending"/> compared a retry's due time with the clock raw. A due time is
/// <c>now + delay</c> when the failed send is recorded and the delay is never more than the cap the caller passes, so a
/// due time further ahead than that cap can only come from a wall clock that stepped backwards after it was stamped, and
/// the alert waited out the step. It now counts as due when it is more than the cap it was recorded under ahead of the
/// clock, and a wait up to that cap is honoured. The cap is kept with each key because callers pass their own.
/// </summary>
public sealed class FailedSendRetryTrackerClockStepTests
{
    private static readonly DateTime T0 = new(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Cap = TimeSpan.FromMinutes(30);

    private static AlertDelivery Failed() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Slack: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    [Fact]
    public void ARetryStampedFarPastTheCapAhead_IsDueNow_AndOneInsideTheCapStillWaits()
    {
        var tracker = new FailedSendRetryTracker();
        Assert.True(tracker.Record("k", Failed(), T0, Cap));
        var due = tracker.DueUtc("k")!.Value;
        Assert.Equal(T0.AddMinutes(1), due);

        // The clock steps back three hours: the stamp is far more than the 30 minute cap ahead of it.
        Assert.False(tracker.RetryPending("k", T0.AddHours(-3)), "a stamp three hours ahead must count as due");

        // The stamp is 11 minutes ahead: inside the cap, so it is a wait.
        Assert.True(tracker.RetryPending("k", T0.AddMinutes(-10)));
    }

    [Fact]
    public void TheCapItself_IsAWait_AndOneTickPastIt_IsAStep()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("k", Failed(), T0, Cap);
        var due = tracker.DueUtc("k")!.Value;

        Assert.True(tracker.RetryPending("k", due - Cap), "exactly the cap ahead is the longest wait the writer records");
        Assert.False(tracker.RetryPending("k", due - Cap - TimeSpan.FromTicks(1)), "one tick past the cap can only be a step");
    }

    [Fact]
    public void AWaitThatReachedTheCap_IsHonouredForItsWholeLength()
    {
        var tracker = new FailedSendRetryTracker();
        var now = T0;
        var delay = TimeSpan.Zero;
        tracker.Record("k", Failed(), now, Cap);

        // 1, 2, 4, 8, 16 minutes, then the 30 minute cap: each retry is recorded when the previous one came due.
        for (var i = 0; i < 6; i++)
        {
            now = tracker.DueUtc("k")!.Value;
            Assert.True(tracker.Record("k", Failed(), now, Cap, out delay, out _));
        }

        Assert.Equal(Cap, delay);
        Assert.Equal(now + Cap, tracker.DueUtc("k"));

        Assert.True(tracker.RetryPending("k", now), "the full cap wait, from the instant it was recorded, is not a step");
        Assert.True(tracker.RetryPending("k", now + Cap - TimeSpan.FromSeconds(1)));
        Assert.False(tracker.RetryPending("k", now + Cap));
    }

    [Fact]
    public void TheCapIsKeptPerKey_BecauseCallersPassTheirOwn()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("short", Failed(), T0, TimeSpan.FromMinutes(10));
        tracker.Record("long", Failed(), T0, TimeSpan.FromMinutes(60));

        // Both stamps are at T0 + 1 minute. At T0 - 39 minutes they are 40 minutes ahead: past the short key's cap,
        // inside the long key's.
        var at = T0.AddMinutes(-39);
        Assert.False(tracker.RetryPending("short", at));
        Assert.True(tracker.RetryPending("long", at));
    }

    [Fact]
    public void ANewFailureOnTheSameKey_ReplacesTheCapItWasStampedUnder()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("k", Failed(), T0, TimeSpan.FromMinutes(10));
        tracker.Record("k", Failed(), T0, TimeSpan.FromMinutes(60));

        // The second failure is a 2 minute wait under a 60 minute cap.
        Assert.Equal(T0.AddMinutes(2), tracker.DueUtc("k"));
        Assert.True(tracker.RetryPending("k", T0.AddMinutes(-40)));
        Assert.False(tracker.RetryPending("k", T0.AddMinutes(-59)));
    }

    [Fact]
    public void TheClamp_IsTheRuleOfCollectorCadence_ForEveryClockReadingAroundTheStamp()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("k", Failed(), T0, Cap);
        var due = tracker.DueUtc("k")!.Value;

        var readings = new[]
        {
            due - TimeSpan.FromHours(6), due - Cap - TimeSpan.FromMinutes(1), due - Cap - TimeSpan.FromTicks(1),
            due - Cap, due - Cap + TimeSpan.FromTicks(1), due - TimeSpan.FromMinutes(5), due - TimeSpan.FromTicks(1),
            due, due + TimeSpan.FromTicks(1), due + TimeSpan.FromHours(2),
        };

        foreach (var now in readings)
        {
            Assert.Equal(now < CollectorCadence.ClampDue(due, now, Cap), tracker.RetryPending("k", now));
        }
    }

    [Fact]
    public void DueUtc_StillReportsTheStampedTime_AndAnUnknownKeyIsNeverPending()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("k", Failed(), T0, Cap);

        // The stamp is what the record wrote, whatever the clock reads.
        Assert.Equal(T0.AddMinutes(1), tracker.DueUtc("k"));
        Assert.False(tracker.RetryPending("nothing recorded", T0.AddHours(-3)));
        Assert.Null(tracker.DueUtc("nothing recorded"));
    }
}
