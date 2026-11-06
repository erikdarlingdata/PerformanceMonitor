/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// One webhook destination's failures in a row (#4750). The count only: a webhook error can carry the endpoint's
/// URL, and the URL is the credential, so this type has no field a host could build alert text from that
/// would leak it. <paramref name="Channel"/> is a <see cref="NotificationRouter"/> channel name, and
/// <paramref name="RouteId"/> is the route whose destination this is, or null for the destination the settings
/// row carries. Two routes can point one channel at two destinations, and a delivery to one says nothing about
/// the other, so each destination is counted and judged on its own. <paramref name="Configured"/> is whether the
/// destination is still there to send to: the settings row carries it, or the route still names it and can match.
/// A destination with none was turned off (disabled, or its URL or key removed), and its count is not a failure
/// to announce.
/// </summary>
public readonly record struct WebhookChannelFailureCount(string Channel, int ConsecutiveFailures, bool Configured, int? RouteId = null)
{
    /// <summary>The destination's identity for a host's memory of what it has announced: the channel name for
    /// the settings row's destination, the channel name and route id for a route's.</summary>
    public string Key => RouteId is { } id ? $"{Channel}:route:{id}" : Channel;

    /// <summary>The destination as alert and notice text names it: <c>Slack</c> for the settings row's
    /// destination, <c>Slack (route 7)</c> for a route's. Never the URL or key, which is the credential.</summary>
    public string Label => LabelOf(Channel, RouteId);

    /// <summary>The <see cref="Label"/> rule for a caller that holds the channel and route id and not the count.</summary>
    public static string LabelOf(string channel, int? routeId) => routeId is { } id ? $"{channel} (route {id})" : channel;
}

/// <summary>What a host should announce about one webhook channel on this observation, if anything.</summary>
public enum WebhookChannelNotice
{
    /// <summary>Nothing to announce: the channel is healthy, is still failing after it was announced, or has
    /// not yet failed often enough.</summary>
    None,

    /// <summary>The channel has reached <see cref="WebhookAlertService.FailingChannelThreshold"/> failures in
    /// a row and has not been announced as failing.</summary>
    Failing,

    /// <summary>A channel that was announced as failing has delivered again: its count is back at 0.</summary>
    Recovered,

    /// <summary>A channel that was announced as failing has no destination left: it was turned off, which is
    /// the expected response to the announcement, so the announcement is closed. Not
    /// <see cref="Recovered"/>, because the channel did not deliver again, and a host says which of the two
    /// happened.</summary>
    TurnedOff,
}

/// <summary>
/// The single definition of when a failing webhook channel is announced (#4750), shared by Darling's
/// "Notification Channel Failing" self-alert and Lite's tray notice: two implementations of the same edge are
/// how the two apps drift, the <see cref="ConnectionAlertDecision"/> discipline.
///
/// <para><b>An edge, not a standing condition.</b> The announcement is made once, when the count first reaches
/// the threshold, and is not repeated while the channel stays broken, however far the count climbs. It is
/// closed once: when the count returns to 0 (<see cref="WebhookChannelNotice.Recovered"/>), or when the
/// channel has no destination left (<see cref="WebhookChannelNotice.TurnedOff"/>).</para>
///
/// <para><b>Recovery is a count of exactly 0.</b> A channel's counter only ever rises by one per failed send or
/// resets to 0 on a successful one, so a count of 1 or 2 after an announcement means the channel delivered and
/// then failed again between two observations. That is not a recovery worth announcing and not a new failure
/// worth announcing either: the channel stays announced until an observation finds it at 0.</para>
///
/// <para><b>A channel with no destination is never failing.</b> Turning a failing channel off is the expected
/// response to the announcement, and a channel with nothing to send to can neither fail nor deliver, so
/// without this its count would hold the announcement open until the process restarted. An announced channel
/// that is no longer configured is closed as <see cref="WebhookChannelNotice.TurnedOff"/> whatever its count,
/// and a channel that is not configured is never announced.</para>
/// </summary>
public static class WebhookChannelFailurePolicy
{
    /// <summary>
    /// Decides what one observation announces. <paramref name="wasFailing"/> is the caller's memory of whether
    /// this channel is currently announced as failing: set it after a <see cref="WebhookChannelNotice.Failing"/>
    /// and clear it after a <see cref="WebhookChannelNotice.Recovered"/> or a
    /// <see cref="WebhookChannelNotice.TurnedOff"/>. <paramref name="configured"/> is
    /// <see cref="WebhookChannelFailureCount.Configured"/>: whether the channel still has a destination.
    /// </summary>
    public static WebhookChannelNotice Decide(bool wasFailing, int consecutiveFailures, bool configured)
    {
        if (wasFailing && !configured)
        {
            return WebhookChannelNotice.TurnedOff;
        }

        if (!wasFailing && configured && consecutiveFailures >= WebhookAlertService.FailingChannelThreshold)
        {
            return WebhookChannelNotice.Failing;
        }

        if (wasFailing && consecutiveFailures == 0)
        {
            return WebhookChannelNotice.Recovered;
        }

        return WebhookChannelNotice.None;
    }
}
