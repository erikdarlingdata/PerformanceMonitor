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
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5750: a route can be scoped to one server or to a tag and its subtree. These pin the shared router's
/// specificity ladder (scope tier, then metric level, then the tag tie rule), the orphan and wildcard rules,
/// per-channel transparency across tiers, the null-server rule, and the scope-expansion helper both products'
/// loaders call. The product loaders and stores are pinned in their own lanes' classes.
/// </summary>
public sealed class NotificationRouterScopeTests
{
    private const int ServerA = 11;
    private const int ServerB = 12;
    private const string Metric = "Deadlocks Detected";

    /* ---- the ladder ---------------------------------------------------------------------------------- */

    [Fact]
    public void ServerTier_BeatsTagTier_BeatsFleetTier_AtEveryMetricLevel()
    {
        foreach (var (match, metric) in new[]
        {
            (Metric, Metric),                                       // exact name
            ("Server Unreachable", "Server Restored"),              // the paired firing
            (AlertFamily.Of(Metric), Metric),                       // family
            (NotificationRoute.WildcardMatch, Metric),              // wildcard (scoped tiers only)
        })
        {
            /* Route ids run the OPPOSITE way to specificity, so the id tie-break cannot make a pass by luck. */
            var server = Scoped(3, match, slack: "server", server: ServerA);
            var tag = Scoped(2, match, slack: "tag", tag: 5, members: [ServerA]);
            var fleet = Fleet(1, match, slack: "fleet");
            var routes = new[] { fleet, tag, server };

            var decision = NotificationRouter.Resolve(metric, routes, NoDefaults(), ServerA);
            Assert.Equal("server", decision.Slack.Destination);
            Assert.Equal(RouteScopeTier.Server, decision.Slack.Tier);
            Assert.Equal(RouteScopeTier.Server, decision.RouteTier);

            decision = NotificationRouter.Resolve(metric, new[] { fleet, tag }, NoDefaults(), ServerA);
            Assert.Equal("tag", decision.Slack.Destination);
            Assert.Equal(RouteScopeTier.Tag, decision.RouteTier);

            if (match != NotificationRoute.WildcardMatch)
            {
                decision = NotificationRouter.Resolve(metric, new[] { fleet }, NoDefaults(), ServerA);
                Assert.Equal("fleet", decision.Slack.Destination);
                Assert.Equal(RouteScopeTier.Fleet, decision.RouteTier);
            }
        }
    }

    [Fact]
    public void ScopeTier_BeatsMetricLevel_AScopedWildcardBeatsAFleetExactRoute()
    {
        var routes = new[]
        {
            Fleet(1, Metric, slack: "fleet-exact"),
            Scoped(2, NotificationRoute.WildcardMatch, slack: "server-wildcard", server: ServerA),
        };

        var decision = NotificationRouter.Resolve(Metric, routes, NoDefaults(), ServerA);
        Assert.Equal("server-wildcard", decision.Slack.Destination);
        Assert.Equal(RouteSource.Wildcard, decision.Slack.Source);

        /* A different server never sees the server-scoped route, so the fleet route answers. */
        decision = NotificationRouter.Resolve(Metric, routes, NoDefaults(), ServerB);
        Assert.Equal("fleet-exact", decision.Slack.Destination);
        Assert.Equal(RouteSource.Exact, decision.Slack.Source);
    }

    [Fact]
    public void WithinATier_MetricLevelOrder_IsNameThenFiringThenFamilyThenWildcard()
    {
        var routes = new[]
        {
            Scoped(1, NotificationRoute.WildcardMatch, slack: "wild", server: ServerA),
            Scoped(2, AlertFamily.Of("Server Restored"), slack: "family", server: ServerA),
            Scoped(3, "Server Unreachable", slack: "firing", server: ServerA),
            Scoped(4, "Server Restored", slack: "name", server: ServerA),
        };

        Assert.Equal("name", NotificationRouter.Resolve("Server Restored", routes, NoDefaults(), ServerA).Slack.Destination);
        Assert.Equal("firing", NotificationRouter.Resolve("Server Restored", routes.Where(r => r.RouteId != 4).ToList(), NoDefaults(), ServerA).Slack.Destination);
        Assert.Equal("family", NotificationRouter.Resolve("Server Restored", routes.Where(r => r.RouteId is 1 or 2).ToList(), NoDefaults(), ServerA).Slack.Destination);
        Assert.Equal("wild", NotificationRouter.Resolve("Server Restored", routes.Where(r => r.RouteId == 1).ToList(), NoDefaults(), ServerA).Slack.Destination);
    }

    /* ---- the tag tie rule ---------------------------------------------------------------------------- */

    [Fact]
    public void TagTier_DeeperTagWins_ThenLowestRouteId_AtOneMetricLevel()
    {
        var shallow = Scoped(1, Metric, slack: "shallow", tag: 5, members: [ServerA], depth: 0);
        var deep = Scoped(9, Metric, slack: "deep", tag: 7, members: [ServerA], depth: 2);
        var deepToo = Scoped(4, Metric, slack: "deep-too", tag: 8, members: [ServerA], depth: 2);

        Assert.Equal("deep", NotificationRouter.Resolve(Metric, new[] { shallow, deep }, NoDefaults(), ServerA).Slack.Destination);
        Assert.Equal("deep-too", NotificationRouter.Resolve(Metric, new[] { shallow, deep, deepToo }, NoDefaults(), ServerA).Slack.Destination);
        Assert.Equal(4, NotificationRouter.Resolve(Metric, new[] { deep, deepToo, shallow }, NoDefaults(), ServerA).RouteId);
    }

    [Fact]
    public void TagDepth_DoesNotOutrankMetricLevel()
    {
        var deepFamily = Scoped(1, AlertFamily.Of(Metric), slack: "deep-family", tag: 7, members: [ServerA], depth: 3);
        var shallowExact = Scoped(2, Metric, slack: "shallow-exact", tag: 5, members: [ServerA], depth: 0);

        Assert.Equal("shallow-exact", NotificationRouter.Resolve(Metric, new[] { deepFamily, shallowExact }, NoDefaults(), ServerA).Slack.Destination);
    }

    /* ---- wildcard, orphan, server id ----------------------------------------------------------------- */

    [Fact]
    public void WildcardOnAScopedRoute_Matches_AndOnAFleetRoute_NeverDoes()
    {
        var settings = NoDefaults();
        var scopedWild = Scoped(1, "*", slack: "scoped-wild", tag: 5, members: [ServerA]);
        var fleetWild = Fleet(2, "*", slack: "fleet-wild");

        foreach (var metric in new[] { Metric, "High CPU", "Server Restored", "External:nagios:disk_full" })
        {
            Assert.Equal("scoped-wild", NotificationRouter.Resolve(metric, new[] { fleetWild, scopedWild }, settings, ServerA).Slack.Destination);

            var fleetOnly = NotificationRouter.Resolve(metric, new[] { fleetWild }, settings, ServerA);
            Assert.Equal(RouteSource.None, fleetOnly.Slack.Source);
            Assert.Null(fleetOnly.RouteId);
            Assert.Null(fleetOnly.RouteTier);
        }

        /* A fleet-wide '*' does not count as configuring a channel either. */
        Assert.False(NotificationRouter.AnyRouteConfiguresAWebhook(new[] { fleetWild }));
        Assert.True(NotificationRouter.AnyRouteConfiguresAWebhook(new[] { scopedWild }));
    }

    [Fact]
    public void AnOrphanedRoute_MatchesNothing_AndFallsThroughToTheNextTier()
    {
        var orphan = Scoped(1, Metric, slack: "orphan", server: 99) with { Orphaned = true, ExpandedServerIds = new HashSet<int>() };
        var orphanWild = Scoped(2, "*", slack: "orphan-wild", tag: 40) with { Orphaned = true, ExpandedServerIds = new HashSet<int>() };
        var fleet = Fleet(3, Metric, slack: "fleet");

        foreach (var serverId in new int?[] { ServerA, 99, null })
        {
            var decision = NotificationRouter.Resolve(Metric, new[] { orphan, orphanWild, fleet }, NoDefaults(), serverId);
            Assert.Equal("fleet", decision.Slack.Destination);
            Assert.Equal(3, decision.RouteId);
        }

        Assert.False(NotificationRouter.AnyRouteConfiguresAWebhook(new[] { orphan, orphanWild }));
        Assert.False(NotificationRouter.AnyRouteConfiguresEmail(new[] { orphan with { SmtpRecipients = "a@example.invalid" } }));
    }

    [Fact]
    public void AScopedRoute_NeverExpanded_FailsClosed_AndASetOnBothScopes_IsMalformed()
    {
        var unexpanded = new NotificationRoute(1, Metric, "", "unexpanded", "", "", "", true) { ScopeServerId = ServerA };
        var both = new NotificationRoute(2, Metric, "", "both", "", "", "", true)
        {
            ScopeServerId = ServerA, ScopeTagId = 5, ExpandedServerIds = new HashSet<int> { ServerA },
        };

        var decision = NotificationRouter.Resolve(Metric, new[] { unexpanded, both }, NoDefaults(), ServerA);
        Assert.Equal(RouteSource.None, decision.Slack.Source);
        Assert.Null(decision.RouteId);
    }

    [Fact]
    public void ServerIdNull_OnlyFleetRoutesMatch()
    {
        var routes = new[]
        {
            Scoped(1, Metric, slack: "server", server: ServerA),
            Scoped(2, Metric, slack: "tag", tag: 5, members: [ServerA]),
            Fleet(3, Metric, slack: "fleet"),
        };

        var decision = NotificationRouter.Resolve(Metric, routes, NoDefaults(), null);
        Assert.Equal("fleet", decision.Slack.Destination);

        decision = NotificationRouter.Resolve(Metric, routes.Take(2).ToList(), NoDefaults(), null);
        Assert.Equal(RouteSource.None, decision.Slack.Source);

        /* Omitting the argument is the same as passing null. */
        var settings = NoDefaults();
        Assert.Equal(NotificationRouter.Resolve(Metric, routes, settings, null), NotificationRouter.Resolve(Metric, routes, settings));
    }

    [Fact]
    public void AServerOutsideTheScope_DoesNotMatch()
    {
        var tag = Scoped(1, Metric, slack: "tag", tag: 5, members: [ServerA]);
        Assert.Equal(RouteSource.None, NotificationRouter.Resolve(Metric, new[] { tag }, NoDefaults(), ServerB).Slack.Source);
        Assert.Equal("tag", NotificationRouter.Resolve(Metric, new[] { tag }, NoDefaults(), ServerA).Slack.Destination);
    }

    [Fact]
    public void ADisabledScopedRoute_IsSkipped_AsIfItDidNotExist()
    {
        var disabled = Scoped(1, Metric, slack: "off", server: ServerA) with { Enabled = false };
        var fleet = Fleet(2, Metric, slack: "fleet");
        Assert.Equal("fleet", NotificationRouter.Resolve(Metric, new[] { disabled, fleet }, NoDefaults(), ServerA).Slack.Destination);
    }

    /* ---- per-channel transparency across tiers -------------------------------------------------------- */

    [Fact]
    public void ARouteWithNothingForAChannel_IsTransparent_ForThatChannel_AcrossTiers()
    {
        var server = Scoped(1, Metric, slack: "server-slack", server: ServerA);
        var tag = Scoped(2, "*", teams: "tag-teams", pagerDuty: "tag-pd", tag: 5, members: [ServerA]);
        var fleet = Fleet(3, AlertFamily.Of(Metric), generic: "fleet-generic", pagerDuty: "fleet-pd", teams: "fleet-teams");

        var decision = NotificationRouter.Resolve(Metric, new[] { fleet, tag, server }, NoDefaults(), ServerA);

        Assert.Equal(("server-slack", 1, RouteScopeTier.Server), (decision.Slack.Destination, decision.Slack.RouteId, decision.Slack.Tier));
        Assert.Equal(("tag-teams", 2, RouteScopeTier.Tag), (decision.Teams.Destination, decision.Teams.RouteId, decision.Teams.Tier));
        Assert.Equal(("tag-pd", 2, RouteScopeTier.Tag), (decision.PagerDuty.Destination, decision.PagerDuty.RouteId, decision.PagerDuty.Tier));
        Assert.Equal(("fleet-generic", 3, RouteScopeTier.Fleet), (decision.Generic.Destination, decision.Generic.RouteId, decision.Generic.Tier));
        Assert.Equal(RouteSource.Wildcard, decision.Teams.Source);
        Assert.Equal(RouteSource.Family, decision.Generic.Source);

        /* Email: nothing anywhere, so the parent default answers with no route tier. */
        Assert.Equal(RouteSource.None, decision.Email.Source);
        Assert.Null(decision.Email.Tier);
        Assert.Equal(1, decision.RouteId);
    }

    [Fact]
    public void AChannelOffOnAScopedRoute_DoesNotFallBackToALessSpecificRoute()
    {
        var server = Scoped(1, Metric, server: ServerA) with { OffChannels = new[] { NotificationRouter.SlackChannel } };
        var fleet = Fleet(2, Metric, slack: "fleet");

        var decision = NotificationRouter.Resolve(Metric, new[] { server, fleet }, NoDefaults(), ServerA);
        Assert.Equal(RouteSource.None, decision.Slack.Source);
        Assert.Null(decision.Slack.Destination);
        Assert.Equal(1, decision.Slack.RouteId);
    }

    [Fact]
    public void UnscopedRoutes_BehaveAsBeforeScopesExisted_WhateverTheServerId()
    {
        var settings = NoDefaults();
        var routes = new[] { Fleet(1, AlertFamily.Of(Metric), slack: "family"), Fleet(2, Metric, slack: "exact") };

        foreach (var serverId in new int?[] { null, ServerA, 0, -5 })
        {
            Assert.Equal("exact", NotificationRouter.Resolve(Metric, routes, settings, serverId).Slack.Destination);
            Assert.True(NotificationRouter.Resolve(Metric, null, settings, serverId).IsAllDefaults);
        }
    }

    [Theory]
    [InlineData("42", 42)]
    [InlineData("-7", -7)]
    [InlineData("", null)]
    [InlineData(null, null)]
    [InlineData("Monitor Store", null)]
    [InlineData("4 2", null)]
    public void ParseServerId_ReadsTheNumericKeyTheSendPathsCarry(string? text, int? expected) =>
        Assert.Equal(expected, NotificationRouter.ParseServerId(text));

    /* ---- the expansion helper ------------------------------------------------------------------------ */

    [Fact]
    public void Expand_TagScope_CoversTheSubtree_AndNotSiblingsOrParents()
    {
        // 1 (root) -> 2 -> 3 ; 1 -> 4 ; 9 (separate root)
        var parents = new Dictionary<int, int?> { [1] = null, [2] = 1, [3] = 2, [4] = 1, [9] = null };
        var serverTags = new Dictionary<int, IReadOnlyCollection<int>>
        {
            [10] = new[] { 1 }, [11] = new[] { 2 }, [12] = new[] { 3 }, [13] = new[] { 4 }, [14] = new[] { 9 }, [15] = Array.Empty<int>(),
        };
        var known = new[] { 10, 11, 12, 13, 14, 15 };

        var expanded = NotificationRouteScopeExpander.Expand(
            new[] { TagRoute(1, 2), TagRoute(2, 1), TagRoute(3, 3), TagRoute(4, 9) }, known, parents, serverTags);

        Assert.Equal(new[] { 11, 12 }, expanded[0].ExpandedServerIds!.OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 10, 11, 12, 13 }, expanded[1].ExpandedServerIds!.OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 12 }, expanded[2].ExpandedServerIds!.ToArray());
        Assert.Equal(new[] { 14 }, expanded[3].ExpandedServerIds!.ToArray());
        Assert.Equal(new[] { 1, 0, 2, 0 }, expanded.Select(r => r.ScopeTagDepth).ToArray());
        Assert.All(expanded, r => Assert.False(r.Orphaned));
    }

    [Fact]
    public void Expand_ServerScope_KnownIsASet_UnknownIsOrphaned()
    {
        var routes = new[] { ServerRoute(1, ServerA), ServerRoute(2, 99) };
        var expanded = NotificationRouteScopeExpander.Expand(
            routes, new[] { ServerA, ServerB }, new Dictionary<int, int?>(), new Dictionary<int, IReadOnlyCollection<int>>());

        Assert.Equal(new[] { ServerA }, expanded[0].ExpandedServerIds!.ToArray());
        Assert.False(expanded[0].Orphaned);
        Assert.True(expanded[1].Orphaned);
        Assert.Empty(expanded[1].ExpandedServerIds!);

        /* The orphan matches nothing end to end. */
        Assert.Null(NotificationRouter.Resolve(Metric, expanded, NoDefaults(), 99).RouteId);
        Assert.Equal(1, NotificationRouter.Resolve(Metric, expanded, NoDefaults(), ServerA).RouteId);
    }

    [Fact]
    public void Expand_UnknownTag_IsOrphaned_AndAKnownTagWithNoServersIsNot()
    {
        var parents = new Dictionary<int, int?> { [1] = null };
        var expanded = NotificationRouteScopeExpander.Expand(
            new[] { TagRoute(1, 404), TagRoute(2, 1) }, new[] { ServerA }, parents, new Dictionary<int, IReadOnlyCollection<int>>());

        Assert.True(expanded[0].Orphaned);
        Assert.Empty(expanded[0].ExpandedServerIds!);
        Assert.False(expanded[1].Orphaned);
        Assert.Empty(expanded[1].ExpandedServerIds!);
    }

    [Fact]
    public void Expand_IgnoresUnknownServersInTheTagMap_AndLeavesFleetRoutesAlone()
    {
        var parents = new Dictionary<int, int?> { [1] = null };
        var serverTags = new Dictionary<int, IReadOnlyCollection<int>> { [ServerA] = new[] { 1 }, [777] = new[] { 1 } };
        var fleet = Fleet(5, Metric, slack: "fleet") with { OffChannels = new[] { "Teams" } };

        var expanded = NotificationRouteScopeExpander.Expand(new[] { fleet, TagRoute(6, 1) }, new[] { ServerA }, parents, serverTags);

        Assert.Same(fleet, expanded[0]);
        Assert.Null(expanded[0].ExpandedServerIds);
        Assert.Equal(new[] { ServerA }, expanded[1].ExpandedServerIds!.ToArray());
    }

    [Fact]
    public async Task Expand_ACycleInTheParentMap_IsCapped_NotLoopedOn()
    {
        // 1 -> 3 -> 2 -> 1 (cycle), 4 hangs off 3, 5 points at a parent that does not exist.
        var parents = new Dictionary<int, int?> { [1] = 3, [2] = 1, [3] = 2, [4] = 3, [5] = 404 };
        var serverTags = new Dictionary<int, IReadOnlyCollection<int>>
        {
            [10] = new[] { 1 }, [11] = new[] { 4 }, [12] = new[] { 5 },
        };

        var expanded = await Task.Run(() => NotificationRouteScopeExpander.Expand(
            new[] { TagRoute(1, 1), TagRoute(2, 4), TagRoute(3, 5) }, new[] { 10, 11, 12 }, parents, serverTags)).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { 10, 11 }, expanded[0].ExpandedServerIds!.OrderBy(x => x).ToArray());
        Assert.Equal(new[] { 11 }, expanded[1].ExpandedServerIds!.ToArray());
        Assert.Equal(new[] { 12 }, expanded[2].ExpandedServerIds!.ToArray());
        Assert.Equal(2, expanded[0].ScopeTagDepth);
        Assert.Equal(3, expanded[1].ScopeTagDepth);
        Assert.Equal(0, expanded[2].ScopeTagDepth);
    }

    [Fact]
    public void Expand_ARouteWithBothScopes_IsOrphaned()
    {
        var both = new NotificationRoute(1, Metric, "", "x", "", "", "", true) { ScopeServerId = ServerA, ScopeTagId = 1 };
        var expanded = NotificationRouteScopeExpander.Expand(
            new[] { both }, new[] { ServerA }, new Dictionary<int, int?> { [1] = null }, new Dictionary<int, IReadOnlyCollection<int>>());
        Assert.True(expanded[0].Orphaned);
        Assert.False(expanded[0].CanMatch);
    }

    /* ---- helpers ------------------------------------------------------------------------------------- */

    private static NotificationRoute Fleet(
        int id, string match, string teams = "", string slack = "", string generic = "", string pagerDuty = "", string email = "") =>
        new(id, match, teams, slack, generic, pagerDuty, email, true);

    private static NotificationRoute Scoped(
        int id, string match, string teams = "", string slack = "", string generic = "", string pagerDuty = "",
        string email = "", int? server = null, int? tag = null, int[]? members = null, int depth = 0) =>
        new(id, match, teams, slack, generic, pagerDuty, email, true)
        {
            ScopeServerId = server,
            ScopeTagId = tag,
            ExpandedServerIds = server is { } s ? new HashSet<int> { s } : new HashSet<int>(members ?? []),
            ScopeTagDepth = depth,
        };

    private static NotificationRoute TagRoute(int id, int tag) =>
        new(id, Metric, "", "x", "", "", "", true) { ScopeTagId = tag };

    private static NotificationRoute ServerRoute(int id, int server) =>
        new(id, Metric, "", "x", "", "", "", true) { ScopeServerId = server };

    private static NoDefaultsSettings NoDefaults() => new();

    /// <summary>Settings with no parent default on any channel, so a result of <see cref="RouteSource.None"/>
    /// means no route matched.</summary>
    private sealed class NoDefaultsSettings : IAlertSettings
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
