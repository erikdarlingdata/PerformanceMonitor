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
/// exact metric name, or <c>Analysis: {category}</c> for every analysis finding of one category (#5632). Exact wins
/// over category, and category over family, at resolution.</param>
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

    /// <summary>The signing keys of this route's own generic URL (#5998, decision D2): sealed as the store holds them, opened
    /// once the product's reader has opened them. Used only when <see cref="GenericUrl"/> is set; a route that inherits the
    /// settings row's generic URL signs with the settings row's keys. Null means no key.</summary>
    public WebhookSigningKeys? GenericSigning { get; init; }

    /// <summary>#6003, D8: the route an agent route's unclaimed, expired request goes to instead (V193
    /// <c>fallback_route_id</c>); null for none. An id that names no enabled route reads as none.</summary>
    public int? FallbackRouteId { get; init; }

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

    /// <summary>The agent level that means "off, and stop looking" (#5632): no investigation request, and no less
    /// specific route is asked.</summary>
    public const int AgentLevelOff = 0;

    /// <summary>The agent level that opens Level 1 investigation requests (#5632). The only level Phase A stores.</summary>
    public const int AgentLevelInvestigate = 1;

    /// <summary>The agent level that opens Level 2 requests (#5632 Phase B, #5912): an agent may also change alert settings
    /// on the request's server, through the one gate, while its claim is live. A request opened by alerts from several routes
    /// takes the lowest level among them.</summary>
    public const int AgentLevelAct = 2;

    /// <summary>The agent level that opens Level 3 requests (#5632, step 5): everything Level 2 allows, and, once a server action exists,
    /// an action on the monitored server. A Level 3 route alone does not allow one: each action also needs that server's own switch
    /// (off until a person turns it on), the server's remediation login given as an <c>env:</c> or <c>file:</c> reference, and the
    /// service's <c>agent_actions_enabled</c>. No server action exists yet. A request opened by alerts from several routes takes the
    /// lowest level among them.</summary>
    public const int AgentLevelServerAct = 3;

    /// <summary>Default and bounds of <see cref="AgentMaxPerHour"/> (#5632, ruling R6). The store CHECK holds the same bounds.</summary>
    public const int DefaultAgentMaxPerHour = 20, MinAgentMaxPerHour = 1, MaxAgentMaxPerHour = 60;

    /// <summary>Default and bounds of <see cref="AgentMaxPerDay"/> (#5632, ruling R6).</summary>
    public const int DefaultAgentMaxPerDay = 100, MinAgentMaxPerDay = 1, MaxAgentMaxPerDay = 500;

    /// <summary>Default and bounds of <see cref="AgentLeaseMinutes"/> (#5632).</summary>
    public const int DefaultAgentLeaseMinutes = 15, MinAgentLeaseMinutes = 5, MaxAgentLeaseMinutes = 120;

    /// <summary>Default and bounds of <see cref="AgentExpiryMinutes"/> (#5632).</summary>
    public const int DefaultAgentExpiryMinutes = 240, MinAgentExpiryMinutes = 15, MaxAgentExpiryMinutes = 1440;

    /// <summary>The agent channel (#5632, store column <c>agent_level</c>): null inherits from a less specific route,
    /// <see cref="AgentLevelOff"/> turns investigations off for the alerts this route matches and stops the lookup, and
    /// <see cref="AgentLevelInvestigate"/> opens Level 1 investigation requests. Read by
    /// <see cref="NotificationRouter.ResolveAgent"/> only: it is not a destination, so it never changes where a page
    /// goes. An init member like the scope, so every existing construction keeps compiling.</summary>
    public int? AgentLevel { get; init; }

    /// <summary>The most investigation requests this route opens in an hour (<c>agent_max_per_hour</c>, #5632).
    /// Alerts that join an open request do not count.</summary>
    public int AgentMaxPerHour { get; init; } = DefaultAgentMaxPerHour;

    /// <summary>The most investigation requests this route opens in a day (<c>agent_max_per_day</c>, #5632).</summary>
    public int AgentMaxPerDay { get; init; } = DefaultAgentMaxPerDay;

    /// <summary>How long an agent's claim on a request lasts before the request opens again
    /// (<c>agent_lease_minutes</c>, #5632).</summary>
    public int AgentLeaseMinutes { get; init; } = DefaultAgentLeaseMinutes;

    /// <summary>How long a request nobody claims stays open before it expires (<c>agent_expiry_minutes</c>, #5632).</summary>
    public int AgentExpiryMinutes { get; init; } = DefaultAgentExpiryMinutes;

    /// <summary>Whether this route's destinations get an alert's resolution (<c>send_resolutions</c>, #5825): null follows
    /// each channel's own setting, true sends it and false does not, whatever the channel says.</summary>
    public bool? SendResolutions { get; init; }

    /// <summary>The severity filter (<c>severities</c>, #6928): <see cref="NotificationRouteKinds.SeveritiesBoth"/> (the default, every route
    /// before the filter), <see cref="NotificationRouteKinds.SeveritiesCritical"/> or <see cref="NotificationRouteKinds.SeveritiesWarning"/>.
    /// A route whose filter excludes an alert's severity does not match it, and matching goes on to the next route.</summary>
    public string Severities { get; init; } = NotificationRouteKinds.SeveritiesBoth;

    /// <summary>Whether this route's severity filter takes an alert of <paramref name="severity"/> (#6928). Null takes every route.</summary>
    public bool AcceptsSeverity(AlertSeverityLevel? severity) => NotificationRouteKinds.SeveritiesAccept(Severities, severity);

    /// <summary>Whether this route sets the agent channel at all (#5632): either level, as opposed to inheriting.</summary>
    public bool HasAgentLevel => AgentLevel.HasValue;

    /// <summary>Whether this route carries a stored scope (#5750).</summary>
    public bool IsScoped => ScopeServerId.HasValue || ScopeTagId.HasValue;

    /// <summary>Whether this route has a well-formed scope that no loader expanded (#5750): it is scoped, not orphaned,
    /// and <see cref="ExpandedServerIds"/> is null. A loader leaves a scoped route this way when it could not read what the
    /// expansion needs (Lite: the tag tables), so the route matches nothing even though its server or tag still exists.
    /// Surfaces that report a route's state (Lite's routes window, its MCP read) show this as "not active", never as
    /// "OK" or "orphaned".</summary>
    public bool ScopeUnexpanded =>
        IsScoped && !Orphaned && !(ScopeServerId.HasValue && ScopeTagId.HasValue) && ExpandedServerIds is null;

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

    /// <summary>The finding category this route names when <see cref="MetricMatch"/> is <c>Analysis: {category}</c>
    /// with no hash suffix (#5632, ruling R5), else null. Such a route matches every finding of that category,
    /// <c>Analysis: {category} [{hash8}]</c>, whatever its hash.</summary>
    public string? AnalysisCategory => AnalysisCategoryOf(MetricMatch);

    /// <summary>The category a <c>metric_match</c> of the shape <c>Analysis: {category}</c> names (#5632), or null:
    /// the rule behind <see cref="AnalysisCategory"/>, for surfaces that hold only the match text.</summary>
    public static string? AnalysisCategoryOf(string? metricMatch)
    {
        var match = metricMatch?.Trim();
        if (string.IsNullOrEmpty(match)
            || !match.StartsWith(AlertFamily.AnalysisMetricPrefix, StringComparison.OrdinalIgnoreCase)
            || match.Contains('['))
        {
            return null;
        }

        var category = match[AlertFamily.AnalysisMetricPrefix.Length..].Trim();
        return category.Length == 0 ? null : category;
    }

    /// <summary>Whether this route matches <paramref name="metricName"/> by its analysis finding CATEGORY (#5632,
    /// ruling R5): the route is <c>Analysis: queries</c> and the metric is <c>Analysis: queries [1a2b3c4d]</c>. Less
    /// specific than a route that names the finding's whole metric (hash included), more specific than a family.</summary>
    public bool MatchesCategory(string metricName) =>
        AnalysisCategory is { } category
        && !MatchesName(metricName)
        && AlertFamily.TryGetAnalysisCategory(metricName, out var found)
        && string.Equals(category, found, StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether this route matches <paramref name="metricName"/> EXACTLY — by its own name or by the
    /// firing it pairs with.</summary>
    public bool MatchesExactly(string metricName) => MatchesName(metricName) || MatchesFiring(metricName);

    /// <summary>Whether this route matches the FAMILY of <paramref name="metricName"/>.</summary>
    public bool MatchesFamily(string metricName) =>
        Family is not null && string.Equals(Family, AlertFamily.Of(metricName), StringComparison.Ordinal);
}
