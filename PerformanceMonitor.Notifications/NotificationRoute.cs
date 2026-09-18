/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// One row of the sparse notification-routes table (#3598, Darling V130 <c>config.config_notification_routes</c>)
/// as the send path sees it: what it matches, and a destination per channel type — each empty by default,
/// and empty means "fall through to the parent <c>config_notification</c> row for this channel".
///
/// <para><b>Sparse, per channel.</b> The parent row holds one destination per channel TYPE; a route
/// overrides any subset of them for the alerts it matches and inherits the rest. So a route can point a
/// family at a second Slack channel and leave PagerDuty exactly as the parent has it, and — because a
/// non-empty column enables a channel the parent may not have configured at all — the operator can put a
/// paging channel on the <c>performance</c> route ALONE and leave the parent without one, which is how
/// "only pages page" is spelled. The converse is not expressible: an empty column inherits, so a route
/// cannot silence a channel the parent has. That is the issue's stated semantic and the one this shape
/// carries; a sink sentinel is a later decision.</para>
///
/// <para><b>What a route does NOT carry.</b> Proxies, the generic channel's headers and body template,
/// and PagerDuty's EU-region flag stay on the parent: they describe HOW a channel type is reached, not
/// WHERE this family lands, and a second Slack webhook on the same network goes through the same proxy.</para>
/// </summary>
/// <param name="RouteId">The identity key, for the ledger's provenance and the editors' updates.</param>
/// <param name="MetricMatch">A family name (<see cref="AlertFamily.All"/>, matched case-insensitively) or an
/// exact metric name. Exact wins over family at resolution.</param>
/// <param name="TeamsUrl">Teams incoming-webhook URL, or empty to inherit.</param>
/// <param name="SlackUrl">Slack incoming-webhook URL, or empty to inherit.</param>
/// <param name="GenericUrl">The generic JSON-POST endpoint, or empty to inherit. Headers, body template and
/// proxy come from the parent.</param>
/// <param name="PagerDutyRoutingKey">Events API v2 routing key, or empty to inherit.</param>
/// <param name="SmtpRecipients">Comma-separated recipient list for the email channel, or empty to inherit.
/// Only a redirect: a recipient list is not an SMTP configuration, so email is delivered only where the
/// parent's host/from/credentials are set.</param>
/// <param name="Enabled">A disabled route is skipped at resolution exactly as if it did not exist.</param>
public sealed record NotificationRoute(
    int RouteId,
    string MetricMatch,
    string TeamsUrl,
    string SlackUrl,
    string GenericUrl,
    string PagerDutyRoutingKey,
    string SmtpRecipients,
    bool Enabled)
{
    /// <summary>The family this route matches, or null when <see cref="MetricMatch"/> is an exact metric name.</summary>
    public string? Family => AlertFamily.NormalizeFamily(MetricMatch);

    /// <summary>Whether this route names a family rather than one metric.</summary>
    public bool IsFamilyRoute => Family is not null;

    /// <summary>Whether any webhook-class destination is set — what decides whether the route can make the
    /// webhook fan-out attempt anything the parent would not.</summary>
    public bool HasAnyWebhookDestination =>
        !string.IsNullOrWhiteSpace(TeamsUrl)
        || !string.IsNullOrWhiteSpace(SlackUrl)
        || !string.IsNullOrWhiteSpace(GenericUrl)
        || !string.IsNullOrWhiteSpace(PagerDutyRoutingKey);

    /// <summary>Whether this route names <paramref name="metricName"/> ITSELF — case-insensitively and
    /// trimmed, the same latitude a mute rule's metric name gets. The most specific match there is.</summary>
    public bool MatchesName(string metricName) =>
        !IsFamilyRoute && !string.IsNullOrWhiteSpace(MetricMatch)
        && string.Equals(MetricMatch.Trim(), metricName, StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether this route names the FIRING a recovery pairs with (<see cref="AlertFamily.Canonical"/>)
    /// — "Server Unreachable" catching "Server Restored". One notch less specific than
    /// <see cref="MatchesName"/>: a route that spells the recovery's own name outranks it.</summary>
    public bool MatchesFiring(string metricName) =>
        !IsFamilyRoute && !string.IsNullOrWhiteSpace(MetricMatch)
        && !MatchesName(metricName)
        && string.Equals(MetricMatch.Trim(), AlertFamily.Canonical(metricName), StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether this route matches <paramref name="metricName"/> EXACTLY — by its own name or by the
    /// firing it pairs with.</summary>
    public bool MatchesExactly(string metricName) => MatchesName(metricName) || MatchesFiring(metricName);

    /// <summary>Whether this route matches the FAMILY of <paramref name="metricName"/>.</summary>
    public bool MatchesFamily(string metricName) =>
        Family is not null && string.Equals(Family, AlertFamily.Of(metricName), StringComparison.Ordinal);
}
