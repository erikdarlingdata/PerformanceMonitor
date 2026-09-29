/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
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
    private static List<WebhookChannelNotice> Replay(params int[] counts)
    {
        var failing = false;
        var notices = new List<WebhookChannelNotice>();
        foreach (var count in counts)
        {
            var notice = WebhookChannelFailurePolicy.Decide(failing, count);
            if (notice == WebhookChannelNotice.Failing)
            {
                failing = true;
            }
            else if (notice == WebhookChannelNotice.Recovered)
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
    [InlineData(false, 0, WebhookChannelNotice.None)]
    [InlineData(false, 2, WebhookChannelNotice.None)]
    [InlineData(false, 3, WebhookChannelNotice.Failing)]
    [InlineData(false, 99, WebhookChannelNotice.Failing)]
    [InlineData(true, 99, WebhookChannelNotice.None)]
    [InlineData(true, 3, WebhookChannelNotice.None)]
    [InlineData(true, 2, WebhookChannelNotice.None)]
    [InlineData(true, 1, WebhookChannelNotice.None)]
    [InlineData(true, 0, WebhookChannelNotice.Recovered)]
    public void EachState_AndCount_HasOneAnswer(bool wasFailing, int count, WebhookChannelNotice expected)
    {
        Assert.Equal(expected, WebhookChannelFailurePolicy.Decide(wasFailing, count));
    }
}
