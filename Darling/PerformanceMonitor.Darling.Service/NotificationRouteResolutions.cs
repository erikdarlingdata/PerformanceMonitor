/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The fleet's "send resolutions" flags, one per channel (V187, #5825), as the send path reads them. PagerDuty is not
/// one of them: its closing event is governed by <c>pagerduty_auto_resolve</c> (V166), which this carries as is.
/// </summary>
public sealed record ResolutionChannelFlags(bool Slack, bool Teams, bool Generic, bool Email, bool PagerDutyAutoResolve)
{
    /// <summary>Every flag off: what a store that never turned one on holds.</summary>
    public static readonly ResolutionChannelFlags None = new(false, false, false, false, false);

    /// <summary>The flags from the stored notification row's two halves.</summary>
    public static ResolutionChannelFlags From(WebhooksConfig webhooks, SmtpConfig smtp)
    {
        ArgumentNullException.ThrowIfNull(webhooks);
        ArgumentNullException.ThrowIfNull(smtp);
        return new(webhooks.SlackSendResolutions, webhooks.TeamsSendResolutions, webhooks.GenericSendResolutions,
            smtp.SendResolutions, webhooks.PagerDutyAutoResolve);
    }

    /// <summary>The channel's own flag, by <see cref="NotificationRouter"/> channel name. An unknown name is false.</summary>
    public bool ForChannel(string channel) => channel switch
    {
        NotificationRouter.SlackChannel => Slack,
        NotificationRouter.TeamsChannel => Teams,
        NotificationRouter.GenericChannel => Generic,
        NotificationRouter.EmailChannel => Email,
        NotificationRouter.PagerDutyChannel => PagerDutyAutoResolve,
        _ => false,
    };
}

/// <summary>
/// The one rule for whether a resolution goes to a channel (#5825): the route that carried the fire decides when it
/// sets <see cref="NotificationRoute.SendResolutions"/>, and the channel's own flag decides when it does not. This
/// answers only the setting. Whether the fire reached the destination, mute rules and the send itself are the send
/// path's.
/// </summary>
public static class NotificationRouteResolutions
{
    /// <summary>Whether <paramref name="channel"/> sends resolutions for an alert that <paramref name="route"/> carried,
    /// or for an alert that no route matched when <paramref name="route"/> is null. PagerDuty follows
    /// <c>pagerduty_auto_resolve</c> alone, so a route's override does not change it.</summary>
    public static bool SendsResolutions(NotificationRoute? route, string channel, ResolutionChannelFlags flags)
    {
        ArgumentNullException.ThrowIfNull(flags);
        if (string.Equals(channel, NotificationRouter.PagerDutyChannel, StringComparison.Ordinal))
        {
            return flags.PagerDutyAutoResolve;
        }

        return route?.SendResolutions ?? flags.ForChannel(channel);
    }

    /// <summary>The history row's columns a sent resolution writes on <c>config.config_alert_log</c> (V187).</summary>
    public const string SentAtColumn = "resolution_sent_at";

    /// <summary>The channels the resolution went to, comma-separated <see cref="NotificationRouter"/> channel names.</summary>
    public const string ChannelsColumn = "resolution_channels";

    /// <summary>What the send did, for example <c>sent</c>, <c>failed: ...</c> or <c>muted</c>.</summary>
    public const string OutcomeColumn = "resolution_outcome";

    /// <summary>The three resolution columns, in the order they were added.</summary>
    public static IReadOnlyList<string> HistoryColumns { get; } = new[] { SentAtColumn, ChannelsColumn, OutcomeColumn };
}
