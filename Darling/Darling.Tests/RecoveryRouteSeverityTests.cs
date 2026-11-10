/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8525: a recovery the product delivers as an alert of its own ("Server Restored", "AG Replica Reconnected") is routed at
/// the tier of the firing it closes, not at its own RESOLVED badge. With one family split across a Critical-only route and a
/// Warning-only route, the recovery follows its Critical firing to the Critical route; a recovery whose firing's tier is not
/// known is taken by every route; a route that takes both tiers and an exact-name route behave as before.
/// </summary>
public sealed class RecoveryRouteSeverityTests
{
    private const string CriticalSlack = "https://slack.example.invalid/critical";
    private const string WarningSlack = "https://slack.example.invalid/warning";

    private static NotificationRoute Route(int id, string match, string severities = "both", string slack = "", string pagerDuty = "") =>
        new(id, match, "", slack, "", pagerDuty, "", true) { Severities = severities };

    private static DarlingAlertSettings Defaults() => new(new DarlingConfig());

    /// <summary>The two family routes of the issue: Critical-only first, Warning-only second.</summary>
    private static NotificationRoute[] SplitFamily() => new[]
    {
        Route(1, AlertFamily.Performance, NotificationRouteKinds.SeveritiesCritical, slack: CriticalSlack),
        Route(2, AlertFamily.Performance, NotificationRouteKinds.SeveritiesWarning, slack: WarningSlack),
    };

    /// <summary>Where the send lands <paramref name="metric"/> with the tier the send path computes for it: the fire site's tier when it
    /// carries one, else what the tier rule says for the name.</summary>
    private static string? SlackFor(string metric, AlertSeverityLevel? fireSite, IReadOnlyList<NotificationRoute> routes) =>
        NotificationRouter.Resolve(metric, routes, Defaults(), severity: AlertSeverity.RouteLevelOf(metric, fireSite)).Slack.Destination;

    /// <summary>A recovery's route tier is its firing's: both firings in the census fire Critical, so both recoveries route as Critical.</summary>
    [Theory]
    [InlineData("Server Restored")]
    [InlineData("AG Replica Reconnected")]
    public void ARecoveryRoutesAtItsFiringsTier(string recovery)
    {
        var firing = AlertFamily.RecoveryPairs[recovery];

        Assert.Equal<AlertSeverityLevel?>(AlertSeverityLevel.Critical, AlertSeverity.RouteLevelOf(firing, null));
        Assert.Equal<AlertSeverityLevel?>(AlertSeverityLevel.Critical, AlertSeverity.RouteLevelOf(recovery, null));
    }

    /// <summary>The issue's table: the firing went to the Critical-only route, and the recovery goes there too, not to the Warning-only one.</summary>
    [Theory]
    [InlineData("Server Unreachable", "Server Restored")]
    [InlineData("AG Replica Disconnected", "AG Replica Reconnected")]
    public void ACriticalFiringsRecovery_FollowsItToTheCriticalRoute(string firing, string recovery)
    {
        var routes = SplitFamily();

        Assert.Equal(CriticalSlack, SlackFor(firing, AlertSeverityLevel.Critical, routes));
        Assert.Equal(CriticalSlack, SlackFor(recovery, null, routes));
    }

    /// <summary>A recovery carrying a Warning firing's tier lands on the Warning-only route, as the firing did.</summary>
    [Fact]
    public void AWarningFiringsRecovery_FollowsItToTheWarningRoute()
    {
        var routes = SplitFamily();

        Assert.Equal(WarningSlack, SlackFor("Server Unreachable", AlertSeverityLevel.Warning, routes));
        Assert.Equal(WarningSlack, NotificationRouter.Resolve("Server Restored", routes, Defaults(), severity: AlertSeverityLevel.Warning).Slack.Destination);
    }

    /// <summary>A recovery whose firing's tier is not known is taken by every route: the most specific route wins whatever its filter.</summary>
    [Fact]
    public void ARecoveryWithNoKnownFiringTier_IsTakenByEveryRoute()
    {
        Assert.Equal(CriticalSlack, NotificationRouter.Resolve("Server Restored", SplitFamily(), Defaults(), severity: null).Slack.Destination);

        var warningFirst = new[]
        {
            Route(1, AlertFamily.Performance, NotificationRouteKinds.SeveritiesWarning, slack: WarningSlack),
            Route(2, AlertFamily.Performance, NotificationRouteKinds.SeveritiesCritical, slack: CriticalSlack),
        };
        Assert.Equal(WarningSlack, NotificationRouter.Resolve("Server Restored", warningFirst, Defaults(), severity: null).Slack.Destination);
    }

    /// <summary>A route with no severity filter takes the recovery as it always has.</summary>
    [Fact]
    public void ARouteThatTakesBothTiers_IsUnchanged()
    {
        const string both = "https://slack.example.invalid/both";
        var routes = new[] { Route(1, AlertFamily.Performance, NotificationRouteKinds.SeveritiesBoth, slack: both) };

        Assert.Equal(both, SlackFor("Server Unreachable", AlertSeverityLevel.Critical, routes));
        Assert.Equal(both, SlackFor("Server Restored", null, routes));
        Assert.Equal(both, SlackFor("AG Replica Reconnected", null, routes));
    }

    /// <summary>An exact-name route still outranks the family: one on the recovery's own name, and one on its firing's name that takes
    /// Critical only, which the recovery now reaches because it routes at the firing's tier.</summary>
    [Fact]
    public void AnExactNameRoute_StillOutranksTheFamily()
    {
        const string exactRecovery = "https://slack.example.invalid/exact-recovery";
        const string exactFiring = "https://slack.example.invalid/exact-firing";
        const string family = "https://slack.example.invalid/family";

        var onTheRecovery = new[]
        {
            Route(1, AlertFamily.Performance, NotificationRouteKinds.SeveritiesCritical, slack: family),
            Route(2, "AG Replica Reconnected", NotificationRouteKinds.SeveritiesBoth, slack: exactRecovery),
        };
        Assert.Equal(exactRecovery, SlackFor("AG Replica Reconnected", null, onTheRecovery));

        var onTheFiring = new[]
        {
            Route(1, AlertFamily.Performance, NotificationRouteKinds.SeveritiesBoth, slack: family),
            Route(2, "AG Replica Disconnected", NotificationRouteKinds.SeveritiesCritical, slack: exactFiring),
        };
        Assert.Equal(exactFiring, SlackFor("AG Replica Disconnected", AlertSeverityLevel.Critical, onTheFiring));
        Assert.Equal(exactFiring, SlackFor("AG Replica Reconnected", null, onTheFiring));
    }

    /// <summary>Through the webhook send with PagerDuty auto-resolve on: the firing's trigger and the recovery's resolve carry the
    /// Critical-only route's routing key, so the incident the firing opened is the one the recovery closes.</summary>
    [Fact]
    public async Task TheRecoverysPagerDutyResolve_ClosesTheIncidentOnTheCriticalRoutesKey()
    {
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyAutoResolve = true;
        config.NotificationRoutes = new[]
        {
            Route(1, AlertFamily.Performance, NotificationRouteKinds.SeveritiesCritical, pagerDuty: "rk-critical"),
            Route(2, AlertFamily.Performance, NotificationRouteKinds.SeveritiesWarning, pagerDuty: "rk-warning"),
        };
        var posted = new List<string>();
        var webhooks = new WebhookAlertService(new DarlingAlertSettings(config), DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, new DiscardingHistoryStore())
        {
            PostPagerDutyAsyncOverride = (_, payload) =>
            {
                posted.Add(payload);
                return Task.FromResult<string?>(null);
            },
        };

        await webhooks.TrySendWebhookAlertsAsync(
            "Server Unreachable", "SQL01", "Login timeout", "Online", "7",
            new AlertContext { SeverityOverride = AlertSeverityLevel.Critical }, cancellationToken: TestContext.Current.CancellationToken);
        await webhooks.TrySendWebhookAlertsAsync(
            "Server Restored", "SQL01", "Online", "Online", "7",
            context: null, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, posted.Count);
        var trigger = JsonDocument.Parse(posted[0]).RootElement;
        var resolve = JsonDocument.Parse(posted[1]).RootElement;
        Assert.Equal("trigger", trigger.GetProperty("event_action").GetString());
        Assert.Equal("rk-critical", trigger.GetProperty("routing_key").GetString());
        Assert.Equal("resolve", resolve.GetProperty("event_action").GetString());
        Assert.Equal("rk-critical", resolve.GetProperty("routing_key").GetString());
    }
}
