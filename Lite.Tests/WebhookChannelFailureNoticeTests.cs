/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins Lite's signal for a webhook channel that keeps failing (#4750). A channel can fail for weeks while
/// another one delivers every alert, and Lite has no self-alert evaluator, so the signal is a tray notice on
/// the status timer that runs the connection checks: one when a channel reaches three failures in a row, one
/// when it delivers again, none in between. The edge is the SHARED <see cref="WebhookChannelFailurePolicy"/>
/// (mirrored in Darling.Tests, the <see cref="ConnectionAlertPolicyTests"/> discipline), and the wording is
/// <see cref="WebhookChannelTrayNotice"/>, which names the channel and the count and never an error.
/// </summary>
public sealed class WebhookChannelFailureNoticeTests
{
    /// <summary>Feeds a channel's count history through the policy the way the window does: the caller's
    /// memory is set by a Failing notice and cleared by a Recovered one.</summary>
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
        var notices = Replay(0, 1, 2, 3, 4, 5, 50, 0, 0);

        Assert.Equal(
            new[]
            {
                WebhookChannelNotice.None, WebhookChannelNotice.None, WebhookChannelNotice.None,
                WebhookChannelNotice.Failing, WebhookChannelNotice.None, WebhookChannelNotice.None,
                WebhookChannelNotice.None, WebhookChannelNotice.Recovered, WebhookChannelNotice.None,
            },
            notices);
    }

    [Fact]
    public void ACountThatNeverReachesThree_IsNeverAnnounced()
    {
        Assert.All(Replay(0, 1, 2, 1, 0, 2, 2, 0), n => Assert.Equal(WebhookChannelNotice.None, n));
    }

    [Fact]
    public void AChannelAlreadyPastThreeAtTheFirstLook_IsAnnouncedOnce()
    {
        var notices = Replay(7, 8, 9);

        Assert.Equal(
            new[] { WebhookChannelNotice.Failing, WebhookChannelNotice.None, WebhookChannelNotice.None },
            notices);
    }

    [Fact]
    public void AnAnnouncedChannelThatDeliveredAndFailedAgainBetweenLooks_HoldsUntilItIsBackAtZero()
    {
        /* 5 then 2: the channel delivered (count reset to 0) and failed twice more before the next look. That
           is neither a recovery nor a new failure, so nothing is announced until a look finds it at 0. */
        var notices = Replay(3, 5, 2, 0, 3);

        Assert.Equal(
            new[]
            {
                WebhookChannelNotice.Failing, WebhookChannelNotice.None, WebhookChannelNotice.None,
                WebhookChannelNotice.Recovered, WebhookChannelNotice.Failing,
            },
            notices);
    }

    [Fact]
    public void TheThreshold_IsTheCountTheChannelsLoggingAlreadyStopsEveryFailureAt()
    {
        Assert.Equal(3, WebhookAlertService.FailingChannelThreshold);

        var source = ReadRepoFile(Path.Combine("PerformanceMonitor.Notifications", "WebhookAlertService.cs"));
        foreach (var channel in new[] { "Teams", "Slack", "Generic", "PagerDuty" })
        {
            Assert.Contains($"_consecutive{channel}Failures <= FailingChannelThreshold", source, StringComparison.Ordinal);
        }

        /* A bare 3 back in a logging test would let the log and the notice disagree about when a channel is
           failing. */
        Assert.DoesNotContain("Failures <= 3", source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTrayNotice_NamesTheChannelAndTheCount_AndSaysNothingWhenThereIsNothingToSay()
    {
        var failing = WebhookChannelTrayNotice.For(WebhookChannelNotice.Failing, "Slack", 3);
        Assert.NotNull(failing);
        Assert.Equal("Notification Channel Failing", failing!.Value.Title);
        Assert.Contains("Slack", failing.Value.Message, StringComparison.Ordinal);
        Assert.Contains("3 times in a row", failing.Value.Message, StringComparison.Ordinal);

        var recovered = WebhookChannelTrayNotice.For(WebhookChannelNotice.Recovered, "Slack", 0);
        Assert.NotNull(recovered);
        Assert.Equal("Notification Channel Recovered", recovered!.Value.Title);
        Assert.Contains("Slack", recovered.Value.Message, StringComparison.Ordinal);

        Assert.Null(WebhookChannelTrayNotice.For(WebhookChannelNotice.None, "Slack", 3));
    }

    [Fact]
    public void TheTrayNotice_TakesNoErrorText_SoNoWebhookUrlCanReachIt()
    {
        /* The builder's whole input is the notice kind, the channel name and the count: there is no parameter
           an error string could arrive through. */
        var parameters = typeof(WebhookChannelTrayNotice)
            .GetMethod("For", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public
                | System.Reflection.BindingFlags.Static)!
            .GetParameters();

        Assert.Equal(
            new[] { typeof(WebhookChannelNotice), typeof(string), typeof(int) },
            parameters.Select(p => p.ParameterType).ToArray());
    }

    /// <summary>The window wires the check onto the status timer that runs the connection checks, reads the
    /// counts from the webhook service it built, and decides with the shared policy. A WPF window cannot be
    /// driven from a unit test, so this is pinned from source.</summary>
    [Fact]
    public void TheStatusTimer_RunsTheChannelCheck_RightAfterTheConnectionCheck()
    {
        var window = ReadRepoFile(Path.Combine("Lite", "MainWindow.xaml.cs"));

        Assert.Contains(
            "CheckConnectionsAndNotify();\r\n            CheckWebhookChannelsAndNotify();",
            window.Replace("\r\n", "\n").Replace("\n", "\r\n"), StringComparison.Ordinal);
        Assert.Contains("_webhookAlertService.GetChannelFailureCounts()", window, StringComparison.Ordinal);
        Assert.Contains("WebhookChannelFailurePolicy.Decide(", window, StringComparison.Ordinal);
        Assert.Contains("WebhookChannelTrayNotice.For(", window, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
