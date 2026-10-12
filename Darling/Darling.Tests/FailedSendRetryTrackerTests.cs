/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4795: <see cref="FailedSendRetryTracker"/> keeps, per alert key, when an alert that no channel delivered is
/// due again: a minute after the first failure, doubling with each further one, never more than the cap. A
/// delivered send (or a partly delivered one, which a retry would send a second time down the working channel)
/// clears the key. Connection and availability group alerts in both apps use it.
/// </summary>
public sealed class FailedSendRetryTrackerTests
{
    private static readonly DateTime T0 = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Cap = TimeSpan.FromMinutes(30);

    private static AlertDelivery Failed() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Failed, WebhookSendError: "Slack: 429 Too Many Requests", AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery Delivered() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.NotAttempted, SendError: null,
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    private static AlertDelivery PartlyDelivered() => AlertDelivery.FromFanout(
        new EmailFanoutResult(
            EmailOutcome: AlertChannelOutcome.Failed, SendError: "SMTP: 535 authentication failed",
            WebhookOutcome: AlertChannelOutcome.Delivered, WebhookSendError: null, AnyChannelConfigured: true),
        muted: false, trayChannelPresent: false);

    [Fact]
    public void AFailedSend_SetsTheDueTimeAMinuteOut_AndASecondFailureTwoMinutes()
    {
        var tracker = new FailedSendRetryTracker();
        Assert.Null(tracker.StampedDueUtc("k"));

        Assert.True(tracker.Record("k", Failed(), T0, Cap));
        Assert.Equal(T0.AddMinutes(1), tracker.StampedDueUtc("k"));

        var second = T0.AddMinutes(1);
        Assert.True(tracker.Record("k", Failed(), second, Cap));
        Assert.Equal(second.AddMinutes(2), tracker.StampedDueUtc("k"));

        var third = second.AddMinutes(2);
        Assert.True(tracker.Record("k", Failed(), third, Cap));
        Assert.Equal(third.AddMinutes(4), tracker.StampedDueUtc("k"));
    }

    [Fact]
    public void TheDelayNeverPassesTheCap()
    {
        var tracker = new FailedSendRetryTracker();
        var cap = TimeSpan.FromMinutes(3);
        var now = T0;
        for (var i = 0; i < 5; i++)
        {
            Assert.True(tracker.Record("k", Failed(), now, cap));
            now = tracker.StampedDueUtc("k")!.Value;
        }

        /* 1, 2, then 3, 3, 3: the waits stop growing at the cap. */
        Assert.True(tracker.Record("k", Failed(), now, cap));
        Assert.Equal(now.AddMinutes(3), tracker.StampedDueUtc("k"));
    }

    [Fact]
    public void ADeliveredSend_ClearsTheKey_AndTheNextFailureStartsAtAMinuteAgain()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("k", Failed(), T0, Cap);
        tracker.Record("k", Failed(), T0.AddMinutes(1), Cap);

        Assert.False(tracker.Record("k", Delivered(), T0.AddMinutes(3), Cap));
        Assert.Null(tracker.StampedDueUtc("k"));

        Assert.True(tracker.Record("k", Failed(), T0.AddMinutes(10), Cap));
        Assert.Equal(T0.AddMinutes(11), tracker.StampedDueUtc("k"));
    }

    [Fact]
    public void APartlyDeliveredOrUnreportedSend_IsNotRetried()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("k", Failed(), T0, Cap);

        Assert.False(tracker.Record("k", PartlyDelivered(), T0.AddMinutes(1), Cap));
        Assert.Null(tracker.StampedDueUtc("k"));

        tracker.Record("k", Failed(), T0.AddMinutes(2), Cap);
        Assert.False(tracker.Record("k", null, T0.AddMinutes(3), Cap));
        Assert.Null(tracker.StampedDueUtc("k"));
    }

    [Fact]
    public void Clear_EndsThePendingRetry_AndTheKeysAreIndependent()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("a", Failed(), T0, Cap);
        tracker.Record("b", Failed(), T0, Cap);

        tracker.Clear("a");
        Assert.Null(tracker.StampedDueUtc("a"));
        Assert.Equal(T0.AddMinutes(1), tracker.StampedDueUtc("b"));
    }

    [Fact]
    public void RetryPending_IsTrueOnlyFromTheFailureUntilTheDueTime()
    {
        var tracker = new FailedSendRetryTracker();
        Assert.False(tracker.RetryPending("k", T0));

        tracker.Record("k", Failed(), T0, Cap);
        Assert.True(tracker.RetryPending("k", T0));
        Assert.True(tracker.RetryPending("k", T0.AddSeconds(59)));
        Assert.False(tracker.RetryPending("k", T0.AddSeconds(60)));

        tracker.Record("k", Delivered(), T0.AddMinutes(1), Cap);
        Assert.False(tracker.RetryPending("k", T0.AddMinutes(1)));
    }

    [Fact]
    public void ClearPrefix_EndsEveryKeyThatStartsWithIt_AndItsStreak_AndNoOtherKey()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("42|a", Failed(), T0, Cap);
        tracker.Record("42|b", Failed(), T0, Cap);
        tracker.Record("42|b", Failed(), T0.AddMinutes(1), Cap);
        tracker.Record("4242|a", Failed(), T0, Cap);
        tracker.Record("43|a", Failed(), T0, Cap);

        tracker.ClearPrefix("42|");

        Assert.Null(tracker.StampedDueUtc("42|a"));
        Assert.Null(tracker.StampedDueUtc("42|b"));
        Assert.Equal(T0.AddMinutes(1), tracker.StampedDueUtc("4242|a"));
        Assert.Equal(T0.AddMinutes(1), tracker.StampedDueUtc("43|a"));

        /* The streak went with the key: the next failure starts at a minute, not at the four that "42|b" had earned. */
        var later = T0.AddMinutes(10);
        Assert.True(tracker.Record("42|b", Failed(), later, Cap));
        Assert.Equal(later.AddMinutes(1), tracker.StampedDueUtc("42|b"));
    }

    [Fact]
    public void ClearPrefix_MatchesTheCaseOfTheKeys_AndAPrefixNothingStartsWithClearsNothing()
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("Ab|x", Failed(), T0, Cap);

        tracker.ClearPrefix("ab|");
        tracker.ClearPrefix("Ab|x|longer");
        tracker.ClearPrefix("zz");

        Assert.Equal(T0.AddMinutes(1), tracker.StampedDueUtc("Ab|x"));
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void ClearPrefix_RefusesAnEmptyPrefix_ThatWouldEndEveryKey(string? prefix)
    {
        var tracker = new FailedSendRetryTracker();
        tracker.Record("k", Failed(), T0, Cap);

        Assert.ThrowsAny<ArgumentException>(() => tracker.ClearPrefix(prefix!));

        Assert.Equal(T0.AddMinutes(1), tracker.StampedDueUtc("k"));
    }
}
