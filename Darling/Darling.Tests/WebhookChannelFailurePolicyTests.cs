/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the SHARED edge (<see cref="WebhookChannelFailurePolicy"/>, #4750) from Darling's suite, mirrored in
/// Lite.Tests: both apps announce a failing webhook channel from this one rule, so a divergence would be a
/// second implementation. One announcement when the count first reaches the threshold, none while it stays
/// there or climbs, one recovery when it is back at exactly 0.
/// </summary>
public sealed class WebhookChannelFailurePolicyTests
{
    private static List<WebhookChannelNotice> Replay(params int[] counts) =>
        ReplayLooks(counts.Select(c => (c, true)).ToArray());

    /// <summary>Feeds successive looks (a count and whether the channel still had a destination) through the
    /// policy the way a host does: its memory is set by a Failing notice and cleared by either resolution.</summary>
    private static List<WebhookChannelNotice> ReplayLooks(params (int Count, bool Configured)[] looks)
    {
        var failing = false;
        var notices = new List<WebhookChannelNotice>();
        foreach (var (count, configured) in looks)
        {
            var notice = WebhookChannelFailurePolicy.Decide(failing, count, configured);
            if (notice == WebhookChannelNotice.Failing)
            {
                failing = true;
            }
            else if (notice is WebhookChannelNotice.Recovered or WebhookChannelNotice.TurnedOff)
            {
                failing = false;
            }

            notices.Add(notice);
        }

        return notices;
    }

    [Fact]
    public void ACountHistory_NoticesAtThreeAndAtRecovery_AndNothingInBetween()
    {
        Assert.Equal(
            new[]
            {
                WebhookChannelNotice.None, WebhookChannelNotice.None, WebhookChannelNotice.None,
                WebhookChannelNotice.Failing, WebhookChannelNotice.None, WebhookChannelNotice.None,
                WebhookChannelNotice.None, WebhookChannelNotice.Recovered, WebhookChannelNotice.None,
            },
            Replay(0, 1, 2, 3, 4, 5, 50, 0, 0));
    }

    [Theory]
    [InlineData(false, 0, true, WebhookChannelNotice.None)]
    [InlineData(false, 2, true, WebhookChannelNotice.None)]
    [InlineData(false, 3, true, WebhookChannelNotice.Failing)]
    [InlineData(false, 99, true, WebhookChannelNotice.Failing)]
    [InlineData(true, 99, true, WebhookChannelNotice.None)]
    [InlineData(true, 3, true, WebhookChannelNotice.None)]
    [InlineData(true, 2, true, WebhookChannelNotice.None)]
    [InlineData(true, 1, true, WebhookChannelNotice.None)]
    [InlineData(true, 0, true, WebhookChannelNotice.Recovered)]
    [InlineData(false, 0, false, WebhookChannelNotice.None)]
    [InlineData(false, 2, false, WebhookChannelNotice.None)]
    [InlineData(false, 3, false, WebhookChannelNotice.None)]
    [InlineData(false, 99, false, WebhookChannelNotice.None)]
    [InlineData(true, 99, false, WebhookChannelNotice.TurnedOff)]
    [InlineData(true, 3, false, WebhookChannelNotice.TurnedOff)]
    [InlineData(true, 1, false, WebhookChannelNotice.TurnedOff)]
    [InlineData(true, 0, false, WebhookChannelNotice.TurnedOff)]
    public void EachState_CountAndConfiguration_HasOneAnswer(bool wasFailing, int count, bool configured, WebhookChannelNotice expected)
    {
        Assert.Equal(expected, WebhookChannelFailurePolicy.Decide(wasFailing, count, configured));
    }

    [Fact]
    public void AnAnnouncedChannelThatIsTurnedOff_IsResolvedOnce_AndTurningItBackOnStartsFromScratch()
    {
        /* Failing at 3, then a look that finds no destination (with the count it had), then the reset count
           with still no destination, then the channel back on at 0, two new failures, and the third. */
        Assert.Equal(
            new[]
            {
                WebhookChannelNotice.Failing, WebhookChannelNotice.TurnedOff, WebhookChannelNotice.None,
                WebhookChannelNotice.None, WebhookChannelNotice.None, WebhookChannelNotice.Failing,
            },
            ReplayLooks((3, true), (3, false), (0, false), (0, true), (2, true), (3, true)));
    }

    [Fact]
    public void ATurnedOffChannelThatWasNeverAnnounced_SaysNothing_HoweverHighItsCount()
    {
        Assert.All(ReplayLooks((2, false), (0, false), (40, false)), n => Assert.Equal(WebhookChannelNotice.None, n));
    }
}
