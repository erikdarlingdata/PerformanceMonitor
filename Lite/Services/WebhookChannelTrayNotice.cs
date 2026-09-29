/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The wording of Lite's tray notice for a webhook channel that keeps failing (#4750), kept out of the window
/// so it pins without a window. The text names the channel and, for a failure, the count; a channel that was
/// turned off says so rather than saying it delivered again. It carries no error
/// text on purpose: a webhook error can carry the endpoint's URL, and the URL is the credential, so the
/// balloon (which is readable over a shoulder and lands in the notification history) must not hold it.
/// </summary>
internal static class WebhookChannelTrayNotice
{
    /// <summary>The title and message for one edge, or null when <paramref name="notice"/> announces nothing.</summary>
    internal static (string Title, string Message)? For(WebhookChannelNotice notice, string channel, int consecutiveFailures) =>
        notice switch
        {
            WebhookChannelNotice.Failing => (
                "Notification Channel Failing",
                $"The {channel} webhook has failed {consecutiveFailures} times in a row, so alerts sent to it are not arriving. Other channels are not affected. The log has the error."),
            WebhookChannelNotice.Recovered => (
                "Notification Channel Recovered",
                $"The {channel} webhook is delivering again."),
            WebhookChannelNotice.TurnedOff => (
                "Notification Channel Turned Off",
                $"The {channel} webhook was turned off (no destination is configured for it)."),
            _ => null,
        };
}
