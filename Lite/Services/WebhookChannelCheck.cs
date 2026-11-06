/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// One look at the webhook destinations' failure counts (#4750), kept out of the window so it pins without one. Each
/// destination is decided by the shared <see cref="WebhookChannelFailurePolicy"/> against the caller's memory of what it
/// has announced, keyed by <see cref="WebhookChannelFailureCount.Key"/>, and the memory is advanced. Two routes pointing
/// one channel at two destinations are two keys, so each is announced and closed on its own. Pure: no clock, no tray,
/// and no error text, which can carry the webhook URL.
/// </summary>
internal static class WebhookChannelCheck
{
    /// <summary>What one look announces about one destination: the edge, the destination as the notice names it
    /// (<see cref="WebhookChannelFailureCount.Label"/>, never the URL), and its failures in a row.</summary>
    internal readonly record struct Announcement(WebhookChannelNotice Notice, string Label, int ConsecutiveFailures);

    /// <summary>
    /// Decides every destination in <paramref name="counts"/> and returns the ones with something to announce, in the
    /// order given. <paramref name="announcedFailing"/> is the caller's memory: a destination is remembered after a
    /// <see cref="WebhookChannelNotice.Failing"/> and forgotten after a <see cref="WebhookChannelNotice.Recovered"/> or a
    /// <see cref="WebhookChannelNotice.TurnedOff"/>, so a destination that is gone leaves nothing behind.
    /// </summary>
    internal static List<Announcement> Look(IEnumerable<WebhookChannelFailureCount> counts, Dictionary<string, bool> announcedFailing)
    {
        var announcements = new List<Announcement>();
        foreach (var destination in counts)
        {
            announcedFailing.TryGetValue(destination.Key, out var wasFailing);
            var notice = WebhookChannelFailurePolicy.Decide(wasFailing, destination.ConsecutiveFailures, destination.Configured);
            if (notice == WebhookChannelNotice.None)
            {
                continue;
            }

            if (notice == WebhookChannelNotice.Failing)
            {
                announcedFailing[destination.Key] = true;
            }
            else
            {
                announcedFailing.Remove(destination.Key);
            }

            announcements.Add(new Announcement(notice, destination.Label, destination.ConsecutiveFailures));
        }

        return announcements;
    }
}
