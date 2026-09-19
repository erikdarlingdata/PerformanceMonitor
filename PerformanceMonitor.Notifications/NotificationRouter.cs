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
}

/// <summary>One channel's resolved destination for one firing: the URL / routing key / recipient list the
/// send will use (null = this channel is not delivered), and the provenance the ledger records.</summary>
/// <param name="Channel">The channel's name as the fan-out spells it in <c>send_error</c> and the health
/// getters: Teams, Slack, Generic, PagerDuty, Email.</param>
/// <param name="Destination">The resolved destination, or null when nothing supplies one.</param>
/// <param name="RouteId">The route that supplied it, or null for the parent default / none.</param>
/// <param name="Source">Which level answered.</param>
public readonly record struct RoutedDestination(string Channel, string? Destination, int? RouteId, RouteSource Source)
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
    /// the winning route, and each DELIVERED channel with the route that supplied it. Channels that resolved
    /// to nothing are omitted — the row says where the post went, not where it did not.</summary>
    public AlertRouteDto ToDto() => new(
        Family,
        RouteId,
        All.Where(d => d.IsDelivered)
           .Select(d => new AlertRouteDestinationDto(d.Channel, d.RouteId, d.Source.ToString()))
           .ToList());
}

/// <summary>
/// The pure resolution behind alert routing (#3598): metric name + routes + parent defaults → one
/// destination per channel. No I/O, no clock, no state — the same inputs always produce the same decision,
/// which is what lets the email path and the webhook fan-out each resolve after their OWN cooldown (design
/// point 2) and still agree on where the firing lands.
///
/// <para><b>Resolution order, per channel:</b> an enabled route matching the metric EXACTLY (its delivered
/// name first, then the firing a recovery pairs with), then an enabled route matching its FAMILY, then the
/// parent row's default — and within each level the first route by <c>route_id</c> whose column for that
/// channel is non-empty.
/// "Per channel" is the load-bearing phrase: an exact route that sets only Slack takes Slack and lets Teams
/// fall to the family route, which lets Teams fall to the parent. A route with nothing for a channel is
/// transparent for that channel, so a partially-filled route never blanks a destination the operator did
/// not mention.</para>
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
    /// default — every SKU without a routes table), in which case the answer is the parent defaults.
    /// </summary>
    public static NotificationRouteDecision Resolve(
        string metricName,
        IReadOnlyList<NotificationRoute>? routes,
        IAlertSettings defaults)
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

        /* The two candidate lists, each ordered by route id so "first matching enabled route wins" is a
           statement about the table and not about the order a reader happened to return rows in. Disabled
           routes are dropped here, once, so no level below can see one. */
        var byName = new List<NotificationRoute>();
        var byFiring = new List<NotificationRoute>();
        var byFamily = new List<NotificationRoute>();
        if (routes is not null)
        {
            foreach (var route in routes.Where(r => r is not null && r.Enabled).OrderBy(r => r.RouteId))
            {
                if (route.MatchesName(metricName))
                {
                    byName.Add(route);
                }
                else if (route.MatchesFiring(metricName))
                {
                    byFiring.Add(route);
                }
                else if (route.MatchesFamily(metricName))
                {
                    byFamily.Add(route);
                }
            }
        }

        /* The exact level, most specific first: a route spelling the delivered name itself, then one spelling
           the firing a recovery pairs with — so "Server Restored" lands where "Server Unreachable" did unless
           the operator named the recovery, in which case they meant it. Both report RouteSource.Exact. */
        var exact = byName.Concat(byFiring).ToList();

        /* The winning route for the ledger's headline: the most specific one that matched at all, whether
           or not it ended up supplying every channel. Per-channel provenance sits on each destination. */
        int? routeId = exact.Count > 0 ? exact[0].RouteId : byFamily.Count > 0 ? byFamily[0].RouteId : null;

        var teams = ResolveChannel(
            TeamsChannel, exact, byFamily, r => r.TeamsUrl,
            defaults.TeamsWebhookEnabled ? defaults.TeamsWebhookUrl : null);
        var slack = ResolveChannel(
            SlackChannel, exact, byFamily, r => r.SlackUrl,
            defaults.SlackWebhookEnabled ? defaults.SlackWebhookUrl : null);
        var generic = ResolveChannel(
            GenericChannel, exact, byFamily, r => r.GenericUrl,
            defaults.GenericWebhookEnabled ? defaults.GenericWebhookUrl : null);
        var pagerDuty = ResolveChannel(
            PagerDutyChannel, exact, byFamily, r => r.PagerDutyRoutingKey,
            defaults.PagerDutyEnabled ? defaults.PagerDutyRoutingKey : null);
        var email = ResolveChannel(
            EmailChannel, exact, byFamily, r => r.SmtpRecipients,
            defaults.SmtpEnabled ? defaults.SmtpRecipients : null);

        return new NotificationRouteDecision(metricName, family, routeId, teams, slack, generic, pagerDuty, email);
    }

    /// <summary>Whether any enabled route could make the webhook fan-out attempt something the parent's
    /// configuration alone would not — the routes half of <c>WebhookAlertService.AnyWebhookConfigured</c>.
    /// Configuration only; never resolves a metric.</summary>
    public static bool AnyRouteConfiguresAWebhook(IReadOnlyList<NotificationRoute>? routes) =>
        routes is not null && routes.Any(r => r is not null && r.Enabled && r.HasAnyWebhookDestination);

    private static RoutedDestination ResolveChannel(
        string channel,
        List<NotificationRoute> exact,
        List<NotificationRoute> byFamily,
        Func<NotificationRoute, string> column,
        string? parentDefault)
    {
        foreach (var route in exact)
        {
            var value = column(route);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return new RoutedDestination(channel, value.Trim(), route.RouteId, RouteSource.Exact);
            }
        }

        foreach (var route in byFamily)
        {
            var value = column(route);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return new RoutedDestination(channel, value.Trim(), route.RouteId, RouteSource.Family);
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
