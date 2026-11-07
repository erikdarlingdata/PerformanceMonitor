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
    public void ExactBeatsCategoryBeatsAnalysisAllBeatsFamilyBeatsWildcard()
    {
        var routes = new List<NotificationRoute>
        {
            Route(1, "*", "wild", server: Server),
            Route(2, AlertFamily.Performance, "family", server: Server),
            Route(3, "Analysis: *", "all", server: Server),
            Route(4, "Analysis: queries", "category", server: Server),
            Route(5, Finding, "exact", server: Server),
        };

        var ladder = new[]
        {
            ("exact", RouteSource.Exact), ("category", RouteSource.Category), ("all", RouteSource.AnalysisAll),
            ("family", RouteSource.Family), ("wild", RouteSource.Wildcard),
        };
        for (var drop = 0; drop < ladder.Length; drop++)
        {
            /* Remove the more specific routes one at a time: the next level answers each time (#7999). */
            var kept = routes.Where(r => r.RouteId <= 5 - drop).ToList();
            var slack = NotificationRouter.Resolve(Finding, kept, new NoDefaults(), Server).Slack;
            Assert.Equal(ladder[drop], (slack.Destination!, slack.Source));
        }
    }

    [Theory]
    [InlineData("Analysis: queries [1a2b3c4d]", true)]
    [InlineData("Analysis: blocking [ffff0000]", true)]
    [InlineData("Analysis: brand_new_category [00000000]", true)]
    [InlineData("Analysis Singles Digest", false)]
    [InlineData("Analysis: queries", false)]
    [InlineData("Analysis: 3 findings on 2 servers", false)]
    [InlineData("High CPU", false)]
    [InlineData("Custom:12", false)]
    [InlineData("External:x:y", false)]
    public void AnalysisAllMatchesFindingsOfAnyCategoryAndNothingElse(string metric, bool matches)
    {
        var slack = NotificationRouter.Resolve(metric, new[] { Route(1, "Analysis: *", "all") }, new NoDefaults()).Slack;

        Assert.Equal(matches, slack.Destination == "all");
        if (matches)
        {
            Assert.Equal(RouteSource.AnalysisAll, slack.Source);
        }
    }

    [Fact]
    public void AnalysisAllSpelling_IsTolerantLikeACategory_AndNeverReadAsCategoryStar()
    {
        var spaced = Route(1, "  analysis:   * ", "all");
        Assert.True(spaced.IsAnalysisAll);
        Assert.Equal("all", NotificationRouter.Resolve(Finding, new[] { spaced }, new NoDefaults()).Slack.Destination);

        Assert.Null(NotificationRoute.AnalysisCategoryOf("Analysis: *"));
        Assert.Null(NotificationRoute.AnalysisCategoryOf("  analysis:   * "));
        Assert.Equal("queries", NotificationRoute.AnalysisCategoryOf("Analysis: queries"));
        Assert.True(NotificationRoute.IsAnalysisAllMatch("Analysis: *"));
        Assert.False(NotificationRoute.IsAnalysisAllMatch("Analysis:*"));
        Assert.False(NotificationRoute.IsAnalysisAllMatch("Analysis: **"));
        Assert.False(NotificationRoute.IsAnalysisAllMatch("Analysis: queries"));
        Assert.False(NotificationRoute.IsAnalysisAllMatch(null));
        Assert.Equal("Analysis: *", NotificationRoute.AnalysisAllMatch);
        /* No space after the colon: not the all shape, and not a category either; it is an exact name, as `Analysis:queries` is today. */
        Assert.Null(NotificationRouter.Resolve(Finding, new[] { Route(2, "Analysis:*", "all") }, new NoDefaults()).Slack.Destination);
    }

    [Fact]
    public void AnalysisAll_CanMatchFleetWide()
    {
        /* Unlike `*`, it is narrower than a family, so a fleet-wide one can match (CanMatch only; the writers' acceptance is tested in the Viewer and writer classes). */
        Assert.True(Route(1, "Analysis: *", "all").CanMatch);
    }

    [Fact]
    public void ScopeTierComesBeforeTheAnalysisAllLevel()
    {
        var serverFamily = new[] { Route(1, "Analysis: *", "fleetAll"), Route(2, AlertFamily.Performance, "serverFamily", server: Server) };
        Assert.Equal("serverFamily", NotificationRouter.Resolve(Finding, serverFamily, new NoDefaults(), Server).Slack.Destination);

        var serverAll = new[] { Route(1, "Analysis: queries", "fleetCategory"), Route(2, "Analysis: *", "serverAll", server: Server) };
        Assert.Equal("serverAll", NotificationRouter.Resolve(Finding, serverAll, new NoDefaults(), Server).Slack.Destination);
    }

    [Fact]
    public void AnAnalysisAllRouteWithOnlySlackLetsTeamsFallToTheFamilyRoute()
    {
        var routes = new[]
        {
            Route(1, "Analysis: *", "slackAll"),
            new NotificationRoute(2, AlertFamily.Performance, "teamsFamily", "slackFamily", "", "", "", true),
        };

        var resolved = NotificationRouter.Resolve(Finding, routes, new NoDefaults());
        Assert.Equal(("slackAll", RouteSource.AnalysisAll), (resolved.Slack.Destination!, resolved.Slack.Source));
        Assert.Equal(("teamsFamily", RouteSource.Family), (resolved.Teams.Destination!, resolved.Teams.Source));
    }

    [Fact]
    public void ACriticalOnlyAnalysisAllRouteTakesNoWarningFinding_AndMatchingGoesOnToTheFamily()
    {
        var routes = new[]
        {
            Route(1, "Analysis: *", "all") with { Severities = NotificationRouteKinds.SeveritiesCritical },
            Route(2, AlertFamily.Performance, "family"),
        };

        var slack = NotificationRouter.Resolve(Finding, routes, new NoDefaults(), severity: AlertSeverityLevel.Warning).Slack;
        Assert.Equal(("family", RouteSource.Family), (slack.Destination!, slack.Source));
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
    public void AnalysisAllIsAppendedLastToRouteSource()
    {
        /* The names persist in the alert-history row, so a new member goes last and the existing ones keep their numbers (#7999). */
        Assert.Equal(
            new[] { "None", "Default", "Family", "Exact", "Wildcard", "Category", "AnalysisAll" },
            System.Enum.GetNames<RouteSource>());
    }

    [Fact]
    public void TheAnalysisAllMatchKindHasItsOwnWord_AndStaysInThePerformanceFamily()
    {
        Assert.Equal("analysis_all", NotificationRouteKinds.AnalysisAllKind);
        Assert.Equal(NotificationRouteKinds.AnalysisAllKind, NotificationRouteKinds.MatchKind("Analysis: *"));
        Assert.Equal(NotificationRouteKinds.AnalysisAllKind, NotificationRouteKinds.MatchKind("  analysis:  * "));
        Assert.Equal(NotificationRouteKinds.AnalysisCategoryKind, NotificationRouteKinds.MatchKind("Analysis: queries"));
        Assert.Equal(NotificationRouteKinds.Wildcard, NotificationRouteKinds.MatchKind("*"));
        Assert.Equal(AlertFamily.Performance, NotificationRouteKinds.FamilyOf("Analysis: *"));
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
    public void ResolveAgent_AnAnalysisAllRouteOpensEveryFinding_AndACategoryLevelZeroOptsOneOut()
    {
        var routes = new[]
        {
            Route(1, "Analysis: *", "all", agentLevel: 1),
            Route(2, "Analysis: queries", "c", agentLevel: 0),
            Route(3, AlertFamily.Performance, "f", agentLevel: 0),
        };

        Assert.Equal(1, NotificationRouter.ResolveAgent("Analysis: blocking [00000000]", routes)?.RouteId);
        Assert.Null(NotificationRouter.ResolveAgent(Finding, routes));
        /* Not a finding: the all route never answers, so the family route's level 0 does. */
        Assert.Null(NotificationRouter.ResolveAgent("High CPU", routes));
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
