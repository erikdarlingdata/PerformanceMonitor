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
/// Where one route's destination for one channel came from, most specific last. Persisted by NAME in the
/// alert-history row's <c>Route</c> member, so the spelling is the contract.
/// </summary>
public enum RouteSource
{
    /// <summary>No route and no parent default: the channel is not delivered for this alert.</summary>
    None,

    /// <summary>The parent <c>config_notification</c> row's own destination — what every alert got before
    /// routes existed.</summary>
    Default,

    /// <summary>A route whose <c>metric_match</c> is the alert's family.</summary>
    Family,

    /// <summary>A route whose <c>metric_match</c> is the alert's exact metric name (or its paired firing).</summary>
    Exact,

    /// <summary>A scoped route whose <c>metric_match</c> is <c>*</c> (#5750): every metric for its server or tag.
    /// The lowest metric level; added last so the existing names keep their meaning.</summary>
    Wildcard,

    /// <summary>A route whose <c>metric_match</c> is <c>Analysis: {category}</c> (#5632, ruling R5): every analysis
    /// finding of that category. Between the exact level and the family level; added last because the names persist.</summary>
    Category,
}

/// <summary>
/// How narrowly a route is scoped (#5750). Lower is more specific, and resolution tries the tiers in this order:
/// a server-scoped route beats a tag-scoped one, which beats a fleet-wide one, whatever their metric levels.
/// </summary>
public enum RouteScopeTier
{
    /// <summary>The route names one server.</summary>
    Server = 0,

    /// <summary>The route names a tag and its subtree.</summary>
    Tag = 1,

    /// <summary>No scope: the route applies to every server (and to a firing with no server).</summary>
    Fleet = 2,
}

/// <summary>One channel's resolved destination for one firing: the URL / routing key / recipient list the
/// send will use (null = this channel is not delivered), and the provenance the ledger records.</summary>
/// <param name="Channel">The channel's name as the fan-out spells it in <c>send_error</c> and the health
/// getters: Teams, Slack, Generic, PagerDuty, Email.</param>
/// <param name="Destination">The resolved destination, or null when nothing supplies one.</param>
/// <param name="RouteId">The route that supplied it, or null for the parent default / none.</param>
/// <param name="Source">Which metric level answered.</param>
/// <param name="Tier">The scope tier of the route that supplied it (#5750), or null for the parent default / none.
/// Optional so a four-argument construction keeps compiling.</param>
public readonly record struct RoutedDestination(
    string Channel, string? Destination, int? RouteId, RouteSource Source, RouteScopeTier? Tier = null)
{
    /// <summary>Whether the channel is delivered at all for this firing.</summary>
    public bool IsDelivered => !string.IsNullOrWhiteSpace(Destination);
}

/// <summary>
/// The whole routing decision for one firing (#3598): the family the metric belongs to, the most specific
/// route that matched it, and one <see cref="RoutedDestination"/> per channel type. Pure data; the fan-out
/// reads the destinations, the deliverer records the provenance.
/// </summary>
public sealed record NotificationRouteDecision(
    string MetricName,
    string Family,
    int? RouteId,
    RoutedDestination Teams,
    RoutedDestination Slack,
    RoutedDestination Generic,
    RoutedDestination PagerDuty,
    RoutedDestination Email)
{
    /// <summary>The scope tier of the winning route (#5750) - the one <see cref="RouteId"/> names - or null when no
    /// route matched. Not part of the persisted <see cref="ToDto"/> projection.</summary>
    public RouteScopeTier? RouteTier { get; init; }

    /// <summary>The five channels in fan-out order.</summary>
    public IEnumerable<RoutedDestination> All
    {
        get
        {
            yield return Teams;
            yield return Slack;
            yield return Generic;
            yield return PagerDuty;
            yield return Email;
        }
    }

    /// <summary>Whether any webhook-class channel resolved to a destination.</summary>
    public bool AnyWebhookDelivered => Teams.IsDelivered || Slack.IsDelivered || Generic.IsDelivered || PagerDuty.IsDelivered;

    /// <summary>Whether every channel answered from the parent row or not at all — i.e. no route touched this
    /// firing, and the fan-out is exactly what it was before routes existed.</summary>
    public bool IsAllDefaults => All.All(d => d.Source is RouteSource.Default or RouteSource.None);

    /// <summary>The persisted projection for the alert-history row's context (design point 3): the family,
    /// the winning route, and each channel that RESOLVED to a destination with the route that supplied it.
    /// Channels that resolved to nothing are omitted — the row says where the post went, not where it did not.
    ///
    /// <para>#4750: <paramref name="outcomes"/> is what each channel's send did, keyed by the channel names
    /// above. With it every listed channel says <see cref="AlertRouteOutcomes.Delivered"/> or
    /// <see cref="AlertRouteOutcomes.Failed"/>, and a channel the map does not name — or names as anything but
    /// those two, such as a cooldown or a fold — says <see cref="AlertRouteOutcomes.NotAttempted"/>. Without
    /// it the outcome stays null, "this row does not say". Only that word is stored, never the error text: a
    /// webhook failure message can carry the endpoint's URL, which is a secret, so <c>send_error</c> remains
    /// the one place a reason is written.</para>
    ///
    /// <para>#6718: <paramref name="genericSentKey"/> is the <c>{{dedup_key}}</c> the generic post rendered, stored on the
    /// generic destination as <see cref="AlertRouteDestinationDto.SentKey"/>. Null leaves it null.</para>
    ///
    /// <para><paramref name="pagerDutySentKey"/> is the <c>dedup_key</c> the PagerDuty trigger carried, stored the same way on
    /// the PagerDuty destination. Null leaves it null.</para></summary>
    public AlertRouteDto ToDto(
        IReadOnlyDictionary<string, AlertChannelOutcome>? outcomes = null, string? genericSentKey = null,
        string? pagerDutySentKey = null) => new(
        Family,
        RouteId,
        All.Where(d => d.IsDelivered)
           .Select(d => new AlertRouteDestinationDto(
               d.Channel, d.RouteId, d.Source.ToString(), OutcomeText(d.Channel, outcomes),
               d.Channel == NotificationRouter.GenericChannel ? genericSentKey
                   : d.Channel == NotificationRouter.PagerDutyChannel ? pagerDutySentKey
                   : null))
           .ToList());

    private static string? OutcomeText(string channel, IReadOnlyDictionary<string, AlertChannelOutcome>? outcomes)
    {
        if (outcomes is null)
        {
            return null;
        }

        return outcomes.TryGetValue(channel, out var outcome)
            ? outcome switch
            {
                AlertChannelOutcome.Delivered => AlertRouteOutcomes.Delivered,
                AlertChannelOutcome.Failed => AlertRouteOutcomes.Failed,
                _ => AlertRouteOutcomes.NotAttempted,
            }
            : AlertRouteOutcomes.NotAttempted;
    }
}

/// <summary>
/// The pure resolution behind alert routing (#3598): metric name + routes + parent defaults → one
/// destination per channel. No I/O, no clock, no state — the same inputs always produce the same decision,
/// which is what lets the email path and the webhook fan-out each resolve after their OWN cooldown (design
/// point 2) and still agree on where the firing lands.
///
/// <para><b>Resolution order, per channel (#5750 adds the scope tiers):</b> routes are tried by SCOPE TIER first -
/// server-scoped, then tag-scoped, then fleet-wide - and inside a tier by METRIC LEVEL: a route matching the metric
/// EXACTLY (its delivered name first, then the firing a recovery pairs with), then an analysis finding's CATEGORY
/// (<c>Analysis: {category}</c>, #5632), then its FAMILY, then <c>*</c>.
/// Inside the tag tier at one metric level the DEEPER tag (more ancestors) wins, then the lowest <c>route_id</c>;
/// elsewhere the lowest <c>route_id</c> wins. After every route comes the parent row's default. A scoped route
/// matches only when the firing's server id is in its expanded set, so a firing with no server id can only be
/// answered by a fleet-wide route. An orphaned route matches nothing, and a fleet-wide <c>*</c> is never matched
/// (storage refuses to save one). "Per channel" is the load-bearing phrase: an exact route that sets only Slack
/// takes Slack and lets Teams fall to the next candidate, and so on down to the parent. A route with nothing for a
/// channel is transparent for that channel, so a partially-filled route never blanks a destination the operator did
/// not mention, and one firing on one channel never goes to two destinations.</para>
///
/// <para><b>Zero routes ⇒ today's fan-out, byte for byte.</b> With no routes every channel answers
/// <see cref="RouteSource.Default"/> from the same <see cref="IAlertSettings"/> members the fan-out read
/// directly before this existed, gated by the same "enabled and non-empty" tests. The migration that adds
/// the table is therefore a no-op for every existing install, which the pins state as an equality between
/// the resolved destinations and the settings.</para>
/// </summary>
public static class NotificationRouter
{
    /// <summary>The channel names, spelled as the fan-out's <c>Record(...)</c> calls and health getters spell them.</summary>
    public const string TeamsChannel = "Teams";
    public const string SlackChannel = "Slack";
    public const string GenericChannel = "Generic";
    public const string PagerDutyChannel = "PagerDuty";
    public const string EmailChannel = "Email";

    /// <summary>
    /// Resolves the destinations for one firing. <paramref name="routes"/> may be null or empty (the shared
    /// default - an adapter that serves no routes), in which case the answer is the parent defaults.
    /// <paramref name="serverId"/> (#5750) is the product's server id for the firing - Lite's hash id, Darling's
    /// registry id - or null for a firing with no server (a fleet-level preview): only fleet-wide routes can match
    /// then. Optional so a caller with no server keeps compiling.
    /// <paramref name="severity"/> (#6928) is the alert's tier: a route whose severity filter excludes it is dropped before
    /// the order below runs, so matching goes on to the next route and ends at the defaults. Null (a caller that does not
    /// know the tier) is taken by every route, which is today's behaviour.
    /// </summary>
    public static NotificationRouteDecision Resolve(
        string metricName,
        IReadOnlyList<NotificationRoute>? routes,
        IAlertSettings defaults,
        int? serverId = null,
        AlertSeverityLevel? severity = null)
    {
        if (metricName is null)
        {
            throw new ArgumentNullException(nameof(metricName));
        }

        if (defaults is null)
        {
            throw new ArgumentNullException(nameof(defaults));
        }

        var family = AlertFamily.Of(metricName);
        var ordered = OrderedCandidates(metricName, routes, serverId, severity);

        /* The winning route for the ledger's headline: the most specific one that matched at all, whether
           or not it ended up supplying every channel. Per-channel provenance sits on each destination. */
        int? routeId = ordered.Count > 0 ? ordered[0].Route.RouteId : null;
        RouteScopeTier? routeTier = ordered.Count > 0 ? ordered[0].Tier : null;

        var teams = ResolveChannel(
            TeamsChannel, ordered, r => r.TeamsUrl,
            defaults.TeamsWebhookEnabled ? defaults.TeamsWebhookUrl : null);
        var slack = ResolveChannel(
            SlackChannel, ordered, r => r.SlackUrl,
            defaults.SlackWebhookEnabled ? defaults.SlackWebhookUrl : null);
        var generic = ResolveChannel(
            GenericChannel, ordered, r => r.GenericUrl,
            defaults.GenericWebhookEnabled ? defaults.GenericWebhookUrl : null);
        var pagerDuty = ResolveChannel(
            PagerDutyChannel, ordered, r => r.PagerDutyRoutingKey,
            defaults.PagerDutyEnabled ? defaults.PagerDutyRoutingKey : null);
        /* #4751: SmtpEnabled no longer implies a non-blank default recipient list, so the default arm can be
           blank here; ResolveChannel's last arm turns a blank default into a null destination, and the email
           path reads null (or blank) as "no recipients for this firing". */
        var email = ResolveChannel(
            EmailChannel, ordered, r => r.SmtpRecipients,
            defaults.SmtpEnabled ? defaults.SmtpRecipients : null);

        return new NotificationRouteDecision(metricName, family, routeId, teams, slack, generic, pagerDuty, email)
        {
            RouteTier = routeTier,
        };
    }

    /// <summary>
    /// The route whose agent channel answers for one firing (#5632), or null when no investigation is due. Reuses
    /// <see cref="Resolve"/>'s order (scope tier, metric level, deeper tag, route id): the FIRST matching route whose
    /// <see cref="NotificationRoute.AgentLevel"/> is set answers. Level <see cref="NotificationRoute.AgentLevelInvestigate"/>
    /// or <see cref="NotificationRoute.AgentLevelAct"/> (#5912) or <see cref="NotificationRoute.AgentLevelServerAct"/> (#5632, Level 3) returns that route (its limits, lease and expiry are the ones that apply); level
    /// <see cref="NotificationRoute.AgentLevelOff"/> stops the lookup and returns null, so a specific route can turn
    /// investigations off for metrics a broader route sends to agents. A route with no level is transparent, as a
    /// route with no destination is for a channel. There is no parent default: no route, no investigation.
    /// </summary>
    public static NotificationRoute? ResolveAgent(
        string metricName,
        IReadOnlyList<NotificationRoute>? routes,
        int? serverId = null,
        AlertSeverityLevel? severity = null)
    {
        if (metricName is null)
        {
            throw new ArgumentNullException(nameof(metricName));
        }

        foreach (var (route, _, _, _) in OrderedCandidates(metricName, routes, serverId, severity))
        {
            if (route.AgentLevel is { } level)
            {
                return level is NotificationRoute.AgentLevelInvestigate or NotificationRoute.AgentLevelAct or NotificationRoute.AgentLevelServerAct ? route : null;
            }
        }

        return null;
    }

    /// <summary>
    /// The matching routes for one firing, most specific first: the one order both <see cref="Resolve"/> and
    /// <see cref="ResolveAgent"/> read. Ordered (scope tier, metric level, deeper tag first, route id) so "first
    /// matching route wins" is a statement about the ordering and not about the order a reader happened to return
    /// rows in. Disabled, orphaned, wrong-server and fleet-wide-<c>*</c> routes are dropped here, once, so no level
    /// below can see one.
    /// <para>The metric levels, each also the <see cref="RouteSource"/> a destination reports: 0 the delivered name and
    /// 1 the firing a recovery pairs with (both <see cref="RouteSource.Exact"/>, so "Server Restored" lands where
    /// "Server Unreachable" did unless the operator named the recovery); 2 the analysis finding's category,
    /// <c>Analysis: {category}</c> (#5632, ruling R5: after the finding's own name with its hash, before the family);
    /// 3 the family; 4 <c>*</c>.</para>
    /// </summary>
    private static List<(NotificationRoute Route, RouteScopeTier Tier, int Level, RouteSource Source)> OrderedCandidates(
        string metricName,
        IReadOnlyList<NotificationRoute>? routes,
        int? serverId,
        AlertSeverityLevel? severity)
    {
        var candidates = new List<(NotificationRoute Route, RouteScopeTier Tier, int Level, RouteSource Source)>();
        if (routes is not null)
        {
            foreach (var route in routes)
            {
                if (route is null || !route.CanMatch || !route.AppliesTo(serverId) || !route.AcceptsSeverity(severity))
                {
                    continue;
                }

                if (route.MatchesName(metricName))
                {
                    candidates.Add((route, route.ScopeTier, 0, RouteSource.Exact));
                }
                else if (route.MatchesFiring(metricName))
                {
                    candidates.Add((route, route.ScopeTier, 1, RouteSource.Exact));
                }
                else if (route.MatchesCategory(metricName))
                {
                    candidates.Add((route, route.ScopeTier, 2, RouteSource.Category));
                }
                else if (route.MatchesFamily(metricName))
                {
                    candidates.Add((route, route.ScopeTier, 3, RouteSource.Family));
                }
                else if (route.IsWildcard)
                {
                    candidates.Add((route, route.ScopeTier, 4, RouteSource.Wildcard));
                }
            }
        }

        return candidates
            .OrderBy(c => (int)c.Tier)
            .ThenBy(c => c.Level)
            .ThenByDescending(c => c.Tier == RouteScopeTier.Tag ? c.Route.ScopeTagDepth : 0)
            .ThenBy(c => c.Route.RouteId)
            .ToList();
    }

    /// <summary>Parses the string server id the send paths carry (Lite: the hash id; Darling: the registry id) into
    /// the int the router matches scopes against (#5750). Null for a blank or non-numeric id - a self-alert or a
    /// fleet-level firing - so only fleet-wide routes apply.</summary>
    public static int? ParseServerId(string? serverId) =>
        int.TryParse(serverId, System.Globalization.NumberStyles.AllowLeadingSign, System.Globalization.CultureInfo.InvariantCulture, out var id)
            ? id
            : null;

    /// <summary>Whether any enabled route could make the webhook fan-out attempt something the parent's
    /// configuration alone would not — the routes half of <c>WebhookAlertService.AnyWebhookConfigured</c>.
    /// Configuration only; never resolves a metric.</summary>
    public static bool AnyRouteConfiguresAWebhook(IReadOnlyList<NotificationRoute>? routes) =>
        routes is not null && routes.Any(r => r is not null && r.CanMatch && r.HasAnyWebhookDestination);

    /// <summary>Whether any enabled route names a destination for ONE webhook channel (#4750) — the
    /// per-channel counterpart of <see cref="AnyRouteConfiguresAWebhook"/>, for "does this channel still have
    /// somewhere to send". Configuration only; never resolves a metric.</summary>
    public static bool AnyRouteConfiguresWebhookChannel(IReadOnlyList<NotificationRoute>? routes, string channel) =>
        routes is not null && routes.Any(r => r is not null && r.CanMatch && r.HasWebhookDestinationFor(channel));

    /// <summary>Whether any enabled route names email recipients (#4751) — the email path's counterpart of
    /// <see cref="AnyRouteConfiguresAWebhook"/>: with a blank default recipient list, a route's recipients are
    /// what make email worth attempting at all. Configuration only; never resolves a metric.</summary>
    public static bool AnyRouteConfiguresEmail(IReadOnlyList<NotificationRoute>? routes) =>
        routes is not null && routes.Any(r => r is not null && r.CanMatch && r.HasEmailDestination);

    private static RoutedDestination ResolveChannel(
        string channel,
        List<(NotificationRoute Route, RouteScopeTier Tier, int Level, RouteSource Source)> ordered,
        Func<NotificationRoute, string> column,
        string? parentDefault)
    {
        foreach (var (route, tier, _, source) in ordered)
        {
            /* #5366: a destination the route set but that could not be read is off, not inherited. */
            if (route.IsOffFor(channel))
            {
                return new RoutedDestination(channel, null, route.RouteId, RouteSource.None, tier);
            }

            var value = column(route);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return new RoutedDestination(channel, value.Trim(), route.RouteId, source, tier);
            }
        }

        /* The parent default is handed in UNTRIMMED, exactly as the fan-out read it before routes existed —
           the byte-identity pin compares this destination to the settings member, so no normalization may
           happen on the default arm. */
        return string.IsNullOrWhiteSpace(parentDefault)
            ? new RoutedDestination(channel, null, null, RouteSource.None)
            : new RoutedDestination(channel, parentDefault, null, RouteSource.Default);
    }
}
