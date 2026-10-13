/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

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
/// <remarks>
/// <para><b>Scope (#5750).</b> A route may be limited to one server (<see cref="ScopeServerId"/>) or to a tag and
/// its whole subtree (<see cref="ScopeTagId"/>); never both. The scope is two init members rather than
/// positional parameters so every existing construction keeps compiling. The product's loader expands the scope to
/// <see cref="ExpandedServerIds"/> (with <see cref="NotificationRouteScopeExpander"/>) and the router only reads
/// that set.</para>
/// </remarks>
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
    /// <summary>The webhook channels (by <see cref="NotificationRouter"/> channel name) that are OFF on this route
    /// because a destination the route set could not be read (#5366). Not a stored column: the reader that could not open
    /// a saved value sets it. A channel that is off here sends nothing for this route's alerts, and does not fall back
    /// to the settings row or a less specific route, because the alert would then go somewhere it was not configured to
    /// go. Null or empty means no channel is off.</summary>
    public IReadOnlyCollection<string>? OffChannels { get; init; }

    /// <summary>Whether <paramref name="channel"/> is off on this route (see <see cref="OffChannels"/>).</summary>
    public bool IsOffFor(string channel) =>
        OffChannels is not null && OffChannels.Contains(channel, StringComparer.Ordinal);

    /// <summary>The metric match that means "every metric" (#5750). Legal only on a scoped route: a fleet-wide
    /// route with this match never matches anything (storage refuses to save one, and
    /// <see cref="NotificationRouter"/> ignores one that slipped through).</summary>
    public const string WildcardMatch = "*";

    /// <summary>The stored server scope (#5750): the product's server id this route is limited to, or null.
    /// At most one of <see cref="ScopeServerId"/> and <see cref="ScopeTagId"/> is set; both null is a fleet-wide
    /// route, which is what every route was before scopes existed. Lite's id is the hash its alert path and
    /// <c>server_tag_map</c> use; Darling's is the registry <c>server_id</c>.</summary>
    public int? ScopeServerId { get; init; }

    /// <summary>The stored tag scope (#5750): a tag id, covering the servers mapped to that tag or to ANY
    /// descendant tag, or null. See <see cref="ScopeServerId"/>.</summary>
    public int? ScopeTagId { get; init; }

    /// <summary>Loader-set, not stored (#5750): the server ids this route applies to, produced by
    /// <see cref="NotificationRouteScopeExpander"/> from <see cref="ScopeServerId"/> / <see cref="ScopeTagId"/>.
    /// Null means "no scope applied": right for a fleet-wide route, and — fail closed — a route that has a
    /// scope but was never expanded matches nothing (see <see cref="CanMatch"/>).</summary>
    public IReadOnlySet<int>? ExpandedServerIds { get; init; }

    /// <summary>Loader-set, not stored (#5750): how many ancestors the scope tag has. The tag tier's tie rule
    /// prefers the deeper tag. Zero for every route that is not tag-scoped.</summary>
    public int ScopeTagDepth { get; init; }

    /// <summary>Loader-set, not stored (#5750): the scope's server or tag no longer exists (or the scope is
    /// malformed). An orphaned route matches nothing; it must never widen to fleet-wide, so there is no
    /// cascade and no set-null behind it.</summary>
    public bool Orphaned { get; init; }

    /// <summary>Whether this route carries a stored scope (#5750).</summary>
    public bool IsScoped => ScopeServerId.HasValue || ScopeTagId.HasValue;

    /// <summary>The scope tier this route sits in (#5750): server-scoped, tag-scoped, or fleet-wide. A route with
    /// both scopes set is malformed and reports the server tier but never matches (<see cref="CanMatch"/>).</summary>
    public RouteScopeTier ScopeTier =>
        ScopeServerId.HasValue ? RouteScopeTier.Server : ScopeTagId.HasValue ? RouteScopeTier.Tag : RouteScopeTier.Fleet;

    /// <summary>Whether <see cref="MetricMatch"/> is the all-metrics wildcard (#5750).</summary>
    public bool IsWildcard => string.Equals(MetricMatch?.Trim(), WildcardMatch, StringComparison.Ordinal);

    /// <summary>Whether this route can match any firing at all (#5750): enabled, not orphaned, a well-formed
    /// scope that has been expanded, and not a fleet-wide wildcard. What the "does any route configure a channel"
    /// questions ask, so a route that can never match does not turn a channel on.</summary>
    public bool CanMatch =>
        Enabled && !Orphaned
        && !(ScopeServerId.HasValue && ScopeTagId.HasValue)
        && (IsScoped ? ExpandedServerIds is not null : !IsWildcard);

    /// <summary>Whether this route's scope admits a firing on <paramref name="serverId"/> (#5750). A fleet-wide
    /// route admits every firing, including one with no server id; a scoped route admits only a known server id
    /// in its expanded set; an orphaned or malformed route admits nothing. Says nothing about the metric.</summary>
    public bool AppliesTo(int? serverId)
    {
        if (Orphaned || (ScopeServerId.HasValue && ScopeTagId.HasValue))
        {
            return false;
        }

        if (!IsScoped)
        {
            return true;
        }

        return serverId.HasValue && ExpandedServerIds is not null && ExpandedServerIds.Contains(serverId.Value);
    }

    /// <summary>The family this route matches, or null when <see cref="MetricMatch"/> is an exact metric name.</summary>
    public string? Family => AlertFamily.NormalizeFamily(MetricMatch);

    /// <summary>Whether this route names a family rather than one metric.</summary>
    public bool IsFamilyRoute => Family is not null;

    /// <summary>Whether any webhook-class destination is set — what decides whether the route can make the
    /// webhook fan-out attempt anything the parent would not.</summary>
    public bool HasAnyWebhookDestination =>
        HasWebhookDestinationFor(NotificationRouter.TeamsChannel)
        || HasWebhookDestinationFor(NotificationRouter.SlackChannel)
        || HasWebhookDestinationFor(NotificationRouter.GenericChannel)
        || HasWebhookDestinationFor(NotificationRouter.PagerDutyChannel);

    /// <summary>Whether this route sets a destination for ONE webhook channel, by its
    /// <see cref="NotificationRouter"/> channel name (#4750): the per-channel form of
    /// <see cref="HasAnyWebhookDestination"/>, which is built from it. A name that is not a webhook channel
    /// has none.</summary>
    public bool HasWebhookDestinationFor(string channel) => channel switch
    {
        NotificationRouter.TeamsChannel => !string.IsNullOrWhiteSpace(TeamsUrl),
        NotificationRouter.SlackChannel => !string.IsNullOrWhiteSpace(SlackUrl),
        NotificationRouter.GenericChannel => !string.IsNullOrWhiteSpace(GenericUrl),
        NotificationRouter.PagerDutyChannel => !string.IsNullOrWhiteSpace(PagerDutyRoutingKey),
        _ => false,
    };

    /// <summary>Whether this route names email recipients (#4751) — what decides whether the route can make
    /// the email path send something the parent's default recipient list alone would not. Recipients only:
    /// the host, from address and credentials still come from the parent.</summary>
    public bool HasEmailDestination => !string.IsNullOrWhiteSpace(SmtpRecipients);

    /// <summary>Whether this route names <paramref name="metricName"/> ITSELF — case-insensitively and
    /// trimmed, the same latitude a mute rule's metric name gets. The most specific match there is.</summary>
    public bool MatchesName(string metricName) =>
        !IsFamilyRoute && !IsWildcard && !string.IsNullOrWhiteSpace(MetricMatch)
        && string.Equals(MetricMatch.Trim(), metricName, StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether this route names the FIRING a recovery pairs with (<see cref="AlertFamily.Canonical"/>)
    /// — "Server Unreachable" catching "Server Restored". One notch less specific than
    /// <see cref="MatchesName"/>: a route that spells the recovery's own name outranks it.</summary>
    public bool MatchesFiring(string metricName) =>
        !IsFamilyRoute && !IsWildcard && !string.IsNullOrWhiteSpace(MetricMatch)
        && !MatchesName(metricName)
        && string.Equals(MetricMatch.Trim(), AlertFamily.Canonical(metricName), StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether this route matches <paramref name="metricName"/> EXACTLY — by its own name or by the
    /// firing it pairs with.</summary>
    public bool MatchesExactly(string metricName) => MatchesName(metricName) || MatchesFiring(metricName);

    /// <summary>Whether this route matches the FAMILY of <paramref name="metricName"/>.</summary>
    public bool MatchesFamily(string metricName) =>
        Family is not null && string.Equals(Family, AlertFamily.Of(metricName), StringComparison.Ordinal);
}
