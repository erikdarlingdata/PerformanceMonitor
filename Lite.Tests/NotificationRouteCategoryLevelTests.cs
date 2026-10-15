/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the metric levels of the shared route matcher (#5632, ruling R5): a route on an analysis finding's whole metric
/// name (hash included) beats a route on its category, <c>Analysis: {category}</c>, which beats the family, which beats
/// <c>*</c>. One matcher serves both products (Lite's <c>EmailSendCore</c> and the webhook fan-out, Darling's
/// <c>WebhookAlertService</c>), so this class is the pin for both. Also pins <see cref="NotificationRouter.ResolveAgent"/>:
/// the same order, the first route with an agent level answers, and level 0 stops the lookup.
/// </summary>
public sealed class NotificationRouteCategoryLevelTests
{
    private const string Finding = "Analysis: queries [1a2b3c4d]";
    private const int Server = 7;

    private static NotificationRoute Route(int id, string match, string slack, int? server = null, int? agentLevel = null) =>
        new(id, match, "", slack, "", "", "", true)
        {
            ScopeServerId = server,
            ExpandedServerIds = server.HasValue ? new HashSet<int> { server.Value } : null,
            AgentLevel = agentLevel,
        };

    [Fact]
    public void ExactBeatsCategoryBeatsFamilyBeatsWildcard()
    {
        var routes = new List<NotificationRoute>
        {
            Route(1, "*", "wild", server: Server),
            Route(2, AlertFamily.Performance, "family", server: Server),
            Route(3, "Analysis: queries", "category", server: Server),
            Route(4, Finding, "exact", server: Server),
        };

        var ladder = new[] { ("exact", RouteSource.Exact), ("category", RouteSource.Category), ("family", RouteSource.Family), ("wild", RouteSource.Wildcard) };
        for (var drop = 0; drop < ladder.Length; drop++)
        {
            /* Remove the more specific routes one at a time: the next level answers each time. */
            var kept = routes.Where(r => r.RouteId <= 4 - drop).ToList();
            var slack = NotificationRouter.Resolve(Finding, kept, new NoDefaults(), Server).Slack;
            Assert.Equal(ladder[drop], (slack.Destination!, slack.Source));
        }
    }

    [Fact]
    public void CategoryRouteMatchesEveryHashOfItsCategoryOnly()
    {
        var routes = new[] { Route(1, "  analysis: QUERIES ", "category") };

        Assert.Equal("category", NotificationRouter.Resolve("Analysis: queries [ffff0000]", routes, new NoDefaults()).Slack.Destination);
        Assert.Equal("category", NotificationRouter.Resolve("Analysis: Queries [00000000]", routes, new NoDefaults()).Slack.Destination);
        Assert.Null(NotificationRouter.Resolve("Analysis: blocking [ffff0000]", routes, new NoDefaults()).Slack.Destination);
        /* The whole category must match, not a prefix of it. */
        Assert.Null(NotificationRouter.Resolve("Analysis: queries_extra [ffff0000]", routes, new NoDefaults()).Slack.Destination);
        /* A digest title and a name without the hash suffix are not findings. */
        Assert.False(AlertFamily.TryGetAnalysisCategory("Analysis: 3 findings on 2 servers", out _));
        Assert.False(AlertFamily.TryGetAnalysisCategory("Analysis: queries", out _));
        Assert.True(AlertFamily.TryGetAnalysisCategory(Finding, out var category));
        Assert.Equal("queries", category);
    }

    [Fact]
    public void ACategoryRouteNamingItselfExactlyStillMatchesAtTheExactLevel()
    {
        /* "Analysis: queries" with no hash is both a category route and a metric name; a firing with exactly that name
           (none is delivered today) matches it by name, at the exact level, not by category. */
        var slack = NotificationRouter.Resolve("Analysis: queries", new[] { Route(1, "Analysis: queries", "x") }, new NoDefaults()).Slack;
        Assert.Equal(RouteSource.Exact, slack.Source);
    }

    [Fact]
    public void TheCategoryMatchKindHasItsOwnWord()
    {
        Assert.Equal(NotificationRouteKinds.AnalysisCategoryKind, NotificationRouteKinds.MatchKind("Analysis: queries"));
        Assert.Equal(NotificationRouteKinds.ExactMetric, NotificationRouteKinds.MatchKind(Finding));
        Assert.Equal("analysis_category", NotificationRouteKinds.AnalysisCategoryKind);
    }

    [Fact]
    public void CategoryIsAppendedLastToRouteSource()
    {
        /* The names persist in the alert-history row, so a new member goes last and the existing ones keep their numbers. */
        Assert.Equal(
            new[] { "None", "Default", "Family", "Exact", "Wildcard", "Category" },
            System.Enum.GetNames<RouteSource>());
    }

    [Fact]
    public void ResolveAgent_FirstRouteWithALevelAnswers_InTheSameOrder()
    {
        /* The exact route sets no level, so it is transparent; the category route sends to agents. */
        var routes = new[]
        {
            Route(1, AlertFamily.Performance, "f", agentLevel: 0),
            Route(2, "Analysis: queries", "c", agentLevel: 1),
            Route(3, Finding, "e"),
        };

        Assert.Equal(2, NotificationRouter.ResolveAgent(Finding, routes)?.RouteId);
        /* A finding of another category falls to the family route, whose level 0 says no. */
        Assert.Null(NotificationRouter.ResolveAgent("Analysis: blocking [00000000]", routes));
    }

    [Fact]
    public void ResolveAgent_LevelZeroStopsTheLookup()
    {
        var routes = new[]
        {
            Route(1, AlertFamily.Performance, "f", agentLevel: 1),
            Route(2, "Blocking Detected", "e", agentLevel: 0),
        };

        Assert.Null(NotificationRouter.ResolveAgent("Blocking Detected", routes));
        Assert.Equal(1, NotificationRouter.ResolveAgent("High CPU", routes)?.RouteId);
    }

    [Fact]
    public void ResolveAgent_ScopeTierComesFirst_AndNoRouteMeansNoInvestigation()
    {
        var routes = new[]
        {
            Route(1, "Blocking Detected", "fleet", agentLevel: 1),
            Route(2, "*", "server", server: Server, agentLevel: 0),
        };

        /* The server-scoped `*` outranks a fleet-wide exact route, as it does for a destination. */
        Assert.Null(NotificationRouter.ResolveAgent("Blocking Detected", routes, Server));
        Assert.Equal(1, NotificationRouter.ResolveAgent("Blocking Detected", routes, 99)?.RouteId);
        Assert.Null(NotificationRouter.ResolveAgent("Blocking Detected", new[] { Route(3, "Blocking Detected", "x") }));
        Assert.Null(NotificationRouter.ResolveAgent("Blocking Detected", null));
        /* A disabled route is skipped as if it did not exist. */
        Assert.Null(NotificationRouter.ResolveAgent("Blocking Detected", new[] { Route(4, "Blocking Detected", "x", agentLevel: 1) with { Enabled = false } }));
    }

    [Fact]
    public void AnAgentLevelNeverChangesADestination()
    {
        var plain = new[] { Route(1, "Blocking Detected", "slack") };
        var withAgent = new[] { Route(1, "Blocking Detected", "slack", agentLevel: 1) };

        Assert.Equal(
            NotificationRouter.Resolve("Blocking Detected", plain, new NoDefaults()).All.Select(d => (d.Destination, d.Source)),
            NotificationRouter.Resolve("Blocking Detected", withAgent, new NoDefaults()).All.Select(d => (d.Destination, d.Source)));
    }

    [Fact]
    public void AgentMembersDefaultToTheRulingFigures()
    {
        var route = new NotificationRoute(1, "Blocking Detected", "", "", "", "", "", true);

        Assert.Null(route.AgentLevel);
        Assert.Equal((20, 100, 15, 240), (route.AgentMaxPerHour, route.AgentMaxPerDay, route.AgentLeaseMinutes, route.AgentExpiryMinutes));
    }

    /// <summary>Settings with no parent default on any channel.</summary>
    private sealed class NoDefaults : IAlertSettings
    {
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 1.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 5;
        public string TriageBaseUrl => "";
    }
}
