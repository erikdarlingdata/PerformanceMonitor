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
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6928: a notification route can be limited by severity (Critical only, Warning only, or both). These pin the router's
/// filter (default both, an excluded severity falls through to the next route and then the defaults), the tier rule the send
/// uses, the issue's three-channel example driven through the deliverer to loopback endpoints, and the PagerDuty close
/// riding the severity the fire went out at.
/// </summary>
public sealed class NotificationRouteSeverityTests
{
    private const string Metric = "High CPU";

    private static NotificationRoute Route(int id, string match, string severities = "both", string slack = "", string teams = "",
        string generic = "", string pagerDuty = "") =>
        new(id, match, teams, slack, generic, pagerDuty, "", true) { Severities = severities };

    private static DarlingAlertSettings Defaults(string pagerDutyKey = "")
    {
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyRoutingKey = pagerDutyKey;
        return new DarlingAlertSettings(config);
    }

    /// <summary>A route nobody gave a filter to reads both, so every route stored before the filter keeps its effect.</summary>
    [Fact]
    public void ARouteWithNoSeverityFilter_ReadsBoth_AndTakesEveryTier()
    {
        var route = new NotificationRoute(1, Metric, "", "https://slack.example.invalid/a", "", "", "", true);

        Assert.Equal("both", route.Severities);
        Assert.True(route.AcceptsSeverity(AlertSeverityLevel.Critical));
        Assert.True(route.AcceptsSeverity(AlertSeverityLevel.Warning));
        Assert.True(route.AcceptsSeverity(null));

        var decision = NotificationRouter.Resolve(Metric, new[] { route }, Defaults(), severity: AlertSeverityLevel.Warning);
        Assert.Equal("https://slack.example.invalid/a", decision.Slack.Destination);
        Assert.Equal("both", NotificationRouteKinds.NormalizeSeverities(null));
        Assert.Equal("both", NotificationRouteKinds.NormalizeSeverities("urgent"));
        Assert.Equal("critical", NotificationRouteKinds.NormalizeSeverities(" Critical "));
    }

    /// <summary>The issue's example: a Critical-only channel, a Warning-only channel and a both channel. Each alert lands on its own.</summary>
    [Fact]
    public void TheThreeChannelExample_EachAlertLandsWhereItShould()
    {
        var routes = new[]
        {
            Route(1, Metric, "critical", slack: "https://slack.example.invalid/critical"),
            Route(2, Metric, "warning", teams: "https://teams.example.invalid/warning"),
            Route(3, Metric, "both", generic: "https://generic.example.invalid/both"),
        };

        var critical = NotificationRouter.Resolve(Metric, routes, Defaults(), severity: AlertSeverityLevel.Critical);
        Assert.Equal("https://slack.example.invalid/critical", critical.Slack.Destination);
        Assert.Null(critical.Teams.Destination);
        Assert.Equal("https://generic.example.invalid/both", critical.Generic.Destination);

        var warning = NotificationRouter.Resolve(Metric, routes, Defaults(), severity: AlertSeverityLevel.Warning);
        Assert.Null(warning.Slack.Destination);
        Assert.Equal("https://teams.example.invalid/warning", warning.Teams.Destination);
        Assert.Equal("https://generic.example.invalid/both", warning.Generic.Destination);
    }

    /// <summary>A route whose filter excludes the alert is skipped: the next matching route answers, and when none does the defaults do.</summary>
    [Fact]
    public void AnExcludedSeverity_FallsThroughToTheNextRoute_ThenToTheDefaults()
    {
        var exactCritical = Route(1, Metric, "critical", pagerDuty: "rk-exact-critical");
        var familyBoth = Route(2, AlertFamily.Of(Metric), "both", pagerDuty: "rk-family");
        var settings = Defaults("rk-default");

        Assert.Equal("rk-exact-critical",
            NotificationRouter.Resolve(Metric, new[] { exactCritical, familyBoth }, settings, severity: AlertSeverityLevel.Critical).PagerDuty.Destination);

        var next = NotificationRouter.Resolve(Metric, new[] { exactCritical, familyBoth }, settings, severity: AlertSeverityLevel.Warning);
        Assert.Equal("rk-family", next.PagerDuty.Destination);
        Assert.Equal(RouteSource.Family, next.PagerDuty.Source);

        var defaulted = NotificationRouter.Resolve(Metric, new[] { exactCritical }, settings, severity: AlertSeverityLevel.Warning);
        Assert.Equal("rk-default", defaulted.PagerDuty.Destination);
        Assert.Equal(RouteSource.Default, defaulted.PagerDuty.Source);
        Assert.True(defaulted.IsAllDefaults);

        /* A caller that does not know the tier is taken by every route, which is how Lite and the agent gate behave. */
        Assert.Equal("rk-exact-critical", NotificationRouter.Resolve(Metric, new[] { exactCritical, familyBoth }, settings).PagerDuty.Destination);
    }

    /// <summary>The agent route search drops an excluded route too, so a Warning alert does not open an investigation under a Critical-only route.</summary>
    [Fact]
    public void ResolveAgent_SkipsARouteWhoseFilterExcludesTheAlert()
    {
        var criticalAgent = Route(1, Metric, "critical") with { AgentLevel = NotificationRoute.AgentLevelInvestigate };

        Assert.Same(criticalAgent, NotificationRouter.ResolveAgent(Metric, new[] { criticalAgent }, severity: AlertSeverityLevel.Critical));
        Assert.Null(NotificationRouter.ResolveAgent(Metric, new[] { criticalAgent }, severity: AlertSeverityLevel.Warning));
    }

    /// <summary>The tier the send gives a route filter: the fire site's tier when it carries one, else the badge (red is Critical, every other badge Warning).</summary>
    [Theory]
    [InlineData("Capture Down", null, AlertSeverityLevel.Critical)]      // red badge
    [InlineData("High CPU", null, AlertSeverityLevel.Warning)]           // amber badge
    [InlineData("Blocking Detected", null, AlertSeverityLevel.Warning)]  // ALERT badge
    [InlineData("A Metric Nobody Mapped", null, AlertSeverityLevel.Warning)] // INFO badge
    [InlineData("Capture Down", AlertSeverityLevel.Warning, AlertSeverityLevel.Warning)] // fire site beats a red badge
    [InlineData("High CPU", AlertSeverityLevel.Critical, AlertSeverityLevel.Critical)]   // fire site beats an amber badge
    public void TheTier_IsTheFireSitesElseTheBadge(string metric, AlertSeverityLevel? fireSite, AlertSeverityLevel expected) =>
        Assert.Equal(expected, AlertSeverity.RouteLevelOf(metric, fireSite));

    /// <summary>The issue's three channels driven through the deliverer: a Critical alert and a Warning alert reach the loopback endpoints their routes name.</summary>
    [Fact]
    public async Task TheThreeChannelExample_ThroughTheDeliverer_ReachesTheEndpointsEachAlertBelongsTo()
    {
        using var criticalSlack = new CapturingWebhookEndpoint();
        using var warningTeams = new CapturingWebhookEndpoint();
        using var bothGeneric = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.NotificationRoutes = new[]
        {
            Route(1, Metric, "critical", slack: criticalSlack.Url),
            Route(2, Metric, "warning", teams: warningTeams.Url),
            Route(3, Metric, "both", generic: bothGeneric.Url),
        };
        var settings = new DarlingAlertSettings(config);
        var history = new DiscardingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        /* Two servers, so the two fires are not folded by the per-server cooldown. */
        await deliverer.DeliverAsync(Fire("7", "CRIT-SRV", AlertSeverityLevel.Critical), TestContext.Current.CancellationToken);
        await deliverer.DeliverAsync(Fire("8", "WARN-SRV", AlertSeverityLevel.Warning), TestContext.Current.CancellationToken);

        Assert.Contains("CRIT-SRV", Assert.Single(criticalSlack.Bodies), StringComparison.Ordinal);
        Assert.Contains("WARN-SRV", Assert.Single(warningTeams.Bodies), StringComparison.Ordinal);
        Assert.Equal(2, bothGeneric.Bodies.Count);
        Assert.Contains(bothGeneric.Bodies, b => b.Contains("CRIT-SRV", StringComparison.Ordinal));
        Assert.Contains(bothGeneric.Bodies, b => b.Contains("WARN-SRV", StringComparison.Ordinal));
    }

    /// <summary>The PagerDuty close routes at the severity the fire went out at: each tier's close reaches the key its fire's route carried.</summary>
    [Theory]
    [InlineData(AlertSeverityLevel.Critical, "rk-critical")]
    [InlineData(AlertSeverityLevel.Warning, "rk-warning")]
    public async Task ThePagerDutyClose_UsesTheFiresSeverity_ToPickTheKey(AlertSeverityLevel firedSeverity, string expectedKey)
    {
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyAutoResolve = true;
        config.NotificationRoutes = new[]
        {
            Route(1, Metric, "critical", pagerDuty: "rk-critical"),
            Route(2, Metric, "warning", pagerDuty: "rk-warning"),
        };
        var settings = new DarlingAlertSettings(config);
        var posted = new List<string>();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, new DiscardingHistoryStore())
        {
            PostPagerDutyAsyncOverride = (_, payload) =>
            {
                posted.Add(payload);
                return Task.FromResult<string?>(null);
            },
        };

        var outcome = await webhooks.TrySendPagerDutyResolveAsync(
            Metric, "SQL01", "7", "back to normal", firedSeverity, TestContext.Current.CancellationToken);

        Assert.Equal(AlertChannelOutcome.Delivered, outcome);
        var body = JsonDocument.Parse(Assert.Single(posted)).RootElement;
        Assert.Equal(expectedKey, body.GetProperty("routing_key").GetString());
        Assert.Equal("resolve", body.GetProperty("event_action").GetString());
    }

    /// <summary>An episode that fired at Warning and climbed to Critical closes the incident on each route's key, and a route that
    /// takes both tiers is closed once.</summary>
    [Fact]
    public async Task ThePagerDutyClose_ForAnEpisodeAtBothTiers_ReachesEachKeyOnce()
    {
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyAutoResolve = true;
        config.NotificationRoutes = new[]
        {
            Route(1, Metric, "critical", pagerDuty: "rk-critical"),
            Route(2, Metric, "warning", pagerDuty: "rk-warning"),
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

        var outcome = await webhooks.TrySendPagerDutyResolveAsync(
            Metric, "SQL01", "7", "back to normal", new AlertSeverityLevel?[] { AlertSeverityLevel.Warning, AlertSeverityLevel.Critical },
            TestContext.Current.CancellationToken);

        Assert.Equal(AlertChannelOutcome.Delivered, outcome);
        Assert.Equal(
            new[] { "rk-critical", "rk-warning" },
            posted.Select(p => JsonDocument.Parse(p).RootElement.GetProperty("routing_key").GetString()!).OrderBy(k => k, StringComparer.Ordinal).ToArray());

        posted.Clear();
        var both = new DarlingConfig();
        both.Webhooks.PagerDutyAutoResolve = true;
        both.NotificationRoutes = new[] { Route(1, Metric, "both", pagerDuty: "rk-both") };
        var bothWebhooks = new WebhookAlertService(new DarlingAlertSettings(both), DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, new DiscardingHistoryStore())
        {
            PostPagerDutyAsyncOverride = (_, payload) =>
            {
                posted.Add(payload);
                return Task.FromResult<string?>(null);
            },
        };
        await bothWebhooks.TrySendPagerDutyResolveAsync(
            Metric, "SQL01", "7", "back to normal", new AlertSeverityLevel?[] { AlertSeverityLevel.Warning, AlertSeverityLevel.Critical },
            TestContext.Current.CancellationToken);
        Assert.Single(posted);
    }

    /// <summary>The deliverer path: Warning-only key A, Critical-only key B, auto-resolve on. The alert fires Warning, escalates to
    /// Critical and clears, and the clear resolves the incident on A as well as on B.</summary>
    [Fact]
    public async Task AWarningThatEscalatesToCritical_ClosesThePagerDutyIncidentOnBothKeys()
    {
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyAutoResolve = true;
        config.NotificationRoutes = new[]
        {
            Route(1, Metric, "warning", pagerDuty: "rk-warning"),
            Route(2, Metric, "critical", pagerDuty: "rk-critical"),
        };
        var settings = new DarlingAlertSettings(config);
        var posted = new List<string>();
        var history = new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history)
        {
            PostPagerDutyAsyncOverride = (_, payload) =>
            {
                posted.Add(payload);
                return Task.FromResult<string?>(null);
            },
        };
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        await deliverer.DeliverAsync(Fire("7", "SQL01", AlertSeverityLevel.Warning), TestContext.Current.CancellationToken);
        await deliverer.DeliverAsync(
            Fire("7", "SQL01", AlertSeverityLevel.Critical) with { Context = new AlertContext { EscalationBypassesCooldown = true } },
            TestContext.Current.CancellationToken);
        posted.Clear();
        await deliverer.DeliverResolutionAsync(
            new AlertResolution("7", "SQL01", Metric, "CPU Resolved", "SQL01: CPU back to normal"),
            cancellationToken: TestContext.Current.CancellationToken);

        var resolves = posted
            .Select(p => JsonDocument.Parse(p).RootElement)
            .Where(e => e.GetProperty("event_action").GetString() == "resolve")
            .Select(e => e.GetProperty("routing_key").GetString()!)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(new[] { "rk-critical", "rk-warning" }, resolves);
    }

    /// <summary>A chat channel: the same escalation sends the clear to the Warning-only channel as well as the Critical-only one.</summary>
    [Fact]
    public async Task AWarningThatEscalatesToCritical_SendsTheClearToTheWarningOnlyChatChannelToo()
    {
        using var warningSlack = new CapturingWebhookEndpoint();
        using var criticalSlack = new CapturingWebhookEndpoint();
        var config = new DarlingConfig();
        config.Webhooks.SlackSendResolutions = true;
        config.NotificationRoutes = new[]
        {
            Route(1, Metric, "warning", slack: warningSlack.Url),
            Route(2, Metric, "critical", slack: criticalSlack.Url),
        };
        var settings = new DarlingAlertSettings(config);
        var history = new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        await deliverer.DeliverAsync(Fire("7", "SQL01", AlertSeverityLevel.Warning), TestContext.Current.CancellationToken);
        await deliverer.DeliverAsync(
            Fire("7", "SQL01", AlertSeverityLevel.Critical) with { Context = new AlertContext { EscalationBypassesCooldown = true } },
            TestContext.Current.CancellationToken);
        Assert.Single(warningSlack.Bodies);
        Assert.Single(criticalSlack.Bodies);
        await deliverer.DeliverResolutionAsync(
            new AlertResolution("7", "SQL01", Metric, "CPU Resolved", "SQL01: CPU back to normal"),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, warningSlack.Bodies.Count);
        Assert.Equal(2, criticalSlack.Bodies.Count);
        Assert.Contains("Resolved", warningSlack.Bodies[1], StringComparison.Ordinal);
    }

    private static AlertOutcome Fire(string serverKey, string server, AlertSeverityLevel severity) =>
        new(serverKey, server, Metric, "97%", "90%", Context: null, DetailText: null,
            NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: severity);
}
