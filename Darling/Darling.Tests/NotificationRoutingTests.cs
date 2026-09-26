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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3598: alert families route to different channels through a sparse routes table. These are the pins on
/// the PURE half — the <see cref="AlertFamily"/> census and the <see cref="NotificationRouter"/> resolution
/// — and on the one fan-out behaviour that must not move: a store with zero routes posts exactly what it
/// posted before, to exactly where it posted it. The live half (the V131 table, its trigger, the reload)
/// runs in <see cref="NotificationRoutesLivePostgresTests"/>.
/// </summary>
public sealed class NotificationRoutingTests
{
    /* ---- the family census --------------------------------------------------------------------------- */

    /// <summary>
    /// Every metric name the product DELIVERS is classified explicitly — by census entry or by prefix — so
    /// a new alert cannot ship routing through the fall-through by accident. The list is built from the
    /// constants the engines declare (never from the census itself, which would be circular) plus the
    /// literals <c>AlertEngine</c> and <c>DarlingSelfAlertEvaluator</c> fire without a constant, read out of
    /// their SOURCE so a literal added to either file joins the census automatically.
    /// </summary>
    [Fact]
    public void EveryDeliveredMetricName_IsClassifiedExplicitly_AndToExactlyOneFamily()
    {
        var delivered = DeliveredMetricNames();

        /* Non-vacuous floor: the product fires well over thirty distinct names today. */
        Assert.True(delivered.Count >= 30, $"the delivered-metric scan found only {delivered.Count} names — the scan is broken");

        var unclassified = delivered.Where(m => !AlertFamily.IsClassified(m)).OrderBy(m => m, StringComparer.Ordinal).ToList();
        Assert.True(unclassified.Count == 0,
            "Metric name(s) the product delivers that AlertFamily does not classify — they would route through the " +
            "performance fall-through without anyone deciding that. Add each to AlertFamily.MetricFamilies:\n  " +
            string.Join("\n  ", unclassified));

        /* Exactly one family: the census is a dictionary, so duplicates are unrepresentable, but a name that
           is BOTH an entry and a prefix match would be two answers — assert Of() agrees with the entry. */
        foreach (var (metric, family) in AlertFamily.MetricFamilies)
        {
            Assert.Equal(family, AlertFamily.Of(metric));
            Assert.Contains(family, AlertFamily.All);
        }
    }

    /// <summary>
    /// The converse: every census entry names a metric something delivers. An orphan entry is a taxonomy
    /// describing an alert that does not exist — harmless to the resolver, misleading to the operator reading
    /// the family list in the Viewer and <c>get_notification_routes</c>.
    /// </summary>
    [Fact]
    public void EveryCensusEntry_IsAMetricSomethingDelivers()
    {
        var delivered = DeliveredMetricNames();
        var orphans = AlertFamily.MetricFamilies.Keys.Where(m => !delivered.Contains(m)).OrderBy(m => m, StringComparer.Ordinal).ToList();
        Assert.True(orphans.Count == 0,
            "AlertFamily.MetricFamilies names metric(s) no engine delivers:\n  " + string.Join("\n  ", orphans));
    }

    /// <summary>
    /// Every metric <see cref="AlertSeverity.ForMetric"/> gives a severity arm is in the census too — that
    /// switch is the other place a new alert has to be registered, so the two lists cannot drift apart
    /// without one of them failing.
    /// </summary>
    [Fact]
    public void EverySeverityArm_IsInTheCensus()
    {
        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Notifications", "AlertSeverity.cs");
        var arms = Regex.Matches(source, "^\\s*\"([^\"]+)\" => \\(", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToList();
        Assert.True(arms.Count >= 25, $"the severity-arm scan found only {arms.Count} arms — the scan is broken");

        var missing = arms.Where(a => !AlertFamily.MetricFamilies.ContainsKey(a)).ToList();
        Assert.True(missing.Count == 0,
            "AlertSeverity.ForMetric has arm(s) AlertFamily.MetricFamilies does not name:\n  " + string.Join("\n  ", missing));
    }

    [Fact]
    public void TheFourFamilies_AreTheIssuesFour_AndTheFallThroughIsPerformance()
    {
        Assert.Equal(new[] { "self-monitor", "reports", "agent-jobs", "performance" }, AlertFamily.All);
        Assert.Equal(AlertFamily.Performance, AlertFamily.Of("Some Alert Nobody Has Written Yet"));
        Assert.False(AlertFamily.IsClassified("Some Alert Nobody Has Written Yet"));

        /* The dynamic shapes classify by prefix. */
        Assert.Equal(AlertFamily.Performance, AlertFamily.Of(CustomAlertEvaluator.MetricNameFor(42)));
        Assert.Equal(AlertFamily.Performance, AlertFamily.Of("Analysis: PLAN_REGRESSION [a1b2c3]"));
        Assert.True(AlertFamily.IsClassified(CustomAlertEvaluator.MetricNameFor(42)));

        /* Family names are matched case-insensitively and trimmed — an operator's spelling, not a key. */
        Assert.True(AlertFamily.IsFamily(" Self-Monitor "));
        Assert.Equal(AlertFamily.SelfMonitor, AlertFamily.NormalizeFamily("SELF-MONITOR"));
        Assert.False(AlertFamily.IsFamily("Deadlocks Detected"));
    }

    /// <summary>
    /// The two delivered recoveries route AS their firing (design point 4), and the pairing is the one
    /// <c>DarlingTriageEndpoint</c> and the self-alert evaluator already state — both firing names are
    /// delivered, both recoveries are, and the recovery is classified to the firing's family.
    /// </summary>
    [Fact]
    public void TheDeliveredRecoveries_RouteAsTheirFiring()
    {
        Assert.Equal("Server Unreachable", AlertFamily.Canonical("Server Restored"));
        Assert.Equal(AgAlertPolicy.ReplicaDisconnectedMetric, AlertFamily.Canonical(AgAlertPolicy.ReplicaReconnectedMetric));
        Assert.Equal("Deadlocks Detected", AlertFamily.Canonical("Deadlocks Detected"));

        var delivered = DeliveredMetricNames();
        foreach (var (recovery, firing) in AlertFamily.RecoveryPairs)
        {
            Assert.Contains(recovery, delivered);
            Assert.Contains(firing, delivered);
            Assert.Equal(AlertFamily.Of(firing), AlertFamily.Of(recovery));
        }
    }

    /* ---- the resolver -------------------------------------------------------------------------------- */

    /// <summary>
    /// Zero routes ⇒ every channel answers <see cref="RouteSource.Default"/> with the SAME string the fan-out
    /// read from the settings before routes existed, for every delivered metric. Reference equality on the
    /// destination, not just value equality: the default arm must not trim, normalize or copy.
    /// </summary>
    [Fact]
    public void ZeroRoutes_ResolvesEveryChannel_ToTheParentSettingsMember_ForEveryMetric()
    {
        var settings = FullyConfigured();

        foreach (var metric in DeliveredMetricNames())
        {
            var decision = NotificationRouter.Resolve(metric, Array.Empty<NotificationRoute>(), settings);

            Assert.Null(decision.RouteId);
            Assert.True(decision.IsAllDefaults);
            Assert.Same(settings.TeamsWebhookUrl, decision.Teams.Destination);
            Assert.Same(settings.SlackWebhookUrl, decision.Slack.Destination);
            Assert.Same(settings.GenericWebhookUrl, decision.Generic.Destination);
            Assert.Same(settings.PagerDutyRoutingKey, decision.PagerDuty.Destination);
            Assert.Same(settings.SmtpRecipients, decision.Email.Destination);
            Assert.All(decision.All, d => Assert.Equal(RouteSource.Default, d.Source));
            Assert.All(decision.All, d => Assert.Null(d.RouteId));

            /* And a null list is the same answer as an empty one — the interface default. */
            Assert.Equal(decision, NotificationRouter.Resolve(metric, null, settings));
        }
    }

    /// <summary>An unconfigured parent channel resolves to <see cref="RouteSource.None"/>, never to an empty
    /// string — the fan-out gates on the destination being non-null, so "" would be an attempted post to
    /// nowhere.</summary>
    [Fact]
    public void AnUnconfiguredParentChannel_ResolvesToNone_NotToAnEmptyDestination()
    {
        var settings = new FakeSettings { SlackWebhookUrl = "https://hooks.example.invalid/parent" };
        var decision = NotificationRouter.Resolve("High CPU", null, settings);

        Assert.True(decision.Slack.IsDelivered);
        Assert.Equal(RouteSource.None, decision.Teams.Source);
        Assert.Null(decision.Teams.Destination);
        Assert.Equal(RouteSource.None, decision.Email.Source);
        Assert.True(decision.AnyWebhookDelivered);
    }

    [Fact]
    public void ExactBeatsFamily_FamilyBeatsDefault_PerChannel()
    {
        var settings = FullyConfigured();
        var routes = new[]
        {
            Route(1, AlertFamily.Performance, slack: "https://slack.example.invalid/perf", teams: "https://teams.example.invalid/perf"),
            Route(2, "Deadlocks Detected", slack: "https://slack.example.invalid/deadlocks"),
        };

        var deadlocks = NotificationRouter.Resolve("Deadlocks Detected", routes, settings);

        /* Slack: the exact route has it. Teams: the exact route is empty there, so the FAMILY route answers.
           Generic/PagerDuty/Email: neither route says anything, so the parent does. Per channel, as the
           issue's sketch says. */
        Assert.Equal(("https://slack.example.invalid/deadlocks", 2, RouteSource.Exact), (deadlocks.Slack.Destination, deadlocks.Slack.RouteId, deadlocks.Slack.Source));
        Assert.Equal(("https://teams.example.invalid/perf", 1, RouteSource.Family), (deadlocks.Teams.Destination, deadlocks.Teams.RouteId, deadlocks.Teams.Source));
        Assert.Equal((settings.GenericWebhookUrl, (int?)null, RouteSource.Default), (deadlocks.Generic.Destination, deadlocks.Generic.RouteId, deadlocks.Generic.Source));
        Assert.Equal((settings.PagerDutyRoutingKey, (int?)null, RouteSource.Default), (deadlocks.PagerDuty.Destination, deadlocks.PagerDuty.RouteId, deadlocks.PagerDuty.Source));
        Assert.Equal((settings.SmtpRecipients, (int?)null, RouteSource.Default), (deadlocks.Email.Destination, deadlocks.Email.RouteId, deadlocks.Email.Source));

        /* The headline route is the most specific one that matched. */
        Assert.Equal(2, deadlocks.RouteId);
        Assert.Equal(AlertFamily.Performance, deadlocks.Family);
        Assert.False(deadlocks.IsAllDefaults);

        /* A sibling in the same family takes only the family route. */
        var cpu = NotificationRouter.Resolve("High CPU", routes, settings);
        Assert.Equal(("https://slack.example.invalid/perf", 1, RouteSource.Family), (cpu.Slack.Destination, cpu.Slack.RouteId, cpu.Slack.Source));
        Assert.Equal(1, cpu.RouteId);

        /* An alert in another family is untouched by either. */
        var digest = NotificationRouter.Resolve("Collector Cost Digest", routes, settings);
        Assert.True(digest.IsAllDefaults);
        Assert.Null(digest.RouteId);
        Assert.Equal(AlertFamily.Reports, digest.Family);
    }

    /// <summary>The issue's "only pages page" spelling: no PagerDuty on the parent, a key on the performance
    /// route alone. A page pages; a report and a self-alert do not.</summary>
    [Fact]
    public void ARoute_CanEnableAChannelTheParentDoesNotHave()
    {
        var settings = new FakeSettings { SlackWebhookUrl = "https://hooks.example.invalid/everything" };
        var routes = new[] { Route(1, AlertFamily.Performance, pagerDuty: "pd-key-for-pages") };

        var page = NotificationRouter.Resolve("Blocking Detected", routes, settings);
        Assert.Equal(("pd-key-for-pages", RouteSource.Family), (page.PagerDuty.Destination, page.PagerDuty.Source));
        Assert.Equal(RouteSource.Default, page.Slack.Source);

        foreach (var quiet in new[] { "Collector Cost Digest", "Compression Job Stuck", "Failed Agent Job" })
        {
            var decision = NotificationRouter.Resolve(quiet, routes, settings);
            Assert.Equal(RouteSource.None, decision.PagerDuty.Source);
            Assert.Equal(RouteSource.Default, decision.Slack.Source);
        }

        /* And the gate the fan-out answers before the cooldown knows about it. */
        Assert.True(NotificationRouter.AnyRouteConfiguresAWebhook(routes));
        Assert.False(NotificationRouter.AnyRouteConfiguresAWebhook(new[] { Route(1, AlertFamily.Performance, email: "only@example.invalid") }));
        Assert.False(NotificationRouter.AnyRouteConfiguresAWebhook(new[] { Route(1, AlertFamily.Performance, pagerDuty: "x", enabled: false) }));
    }

    [Fact]
    public void ADisabledRoute_IsSkippedAsIfAbsent_AndTheLowestRouteIdWinsWithinALevel()
    {
        var settings = FullyConfigured();
        var routes = new[]
        {
            Route(7, AlertFamily.SelfMonitor, slack: "https://slack.example.invalid/second"),
            Route(3, AlertFamily.SelfMonitor, slack: "https://slack.example.invalid/first"),
            Route(1, AlertFamily.SelfMonitor, slack: "https://slack.example.invalid/disabled", enabled: false),
            /* A route that matches but says nothing for Slack is transparent for Slack. */
            Route(2, AlertFamily.SelfMonitor, teams: "https://teams.example.invalid/self"),
        };

        var decision = NotificationRouter.Resolve("Compression Job Stuck", routes, settings);
        Assert.Equal(("https://slack.example.invalid/first", 3), (decision.Slack.Destination, decision.Slack.RouteId));
        Assert.Equal(("https://teams.example.invalid/self", 2), (decision.Teams.Destination, decision.Teams.RouteId));
        /* The headline is the lowest-id ENABLED match at the most specific level. */
        Assert.Equal(2, decision.RouteId);

        /* All disabled ⇒ defaults, and no headline. */
        var allOff = routes.Select(r => r with { Enabled = false }).ToArray();
        Assert.True(NotificationRouter.Resolve("Compression Job Stuck", allOff, settings).IsAllDefaults);
        Assert.Null(NotificationRouter.Resolve("Compression Job Stuck", allOff, settings).RouteId);
    }

    /// <summary>Design point 4 at the resolver: an exact route on the firing catches the recovery, and the
    /// recovery's family route is the firing's family route.</summary>
    [Fact]
    public void ARecovery_LandsWhereItsFiringLanded()
    {
        var settings = FullyConfigured();
        var exact = new[] { Route(1, "Server Unreachable", slack: "https://slack.example.invalid/outages") };

        var fire = NotificationRouter.Resolve("Server Unreachable", exact, settings);
        var clear = NotificationRouter.Resolve("Server Restored", exact, settings);
        Assert.Equal(fire.Slack, clear.Slack);
        Assert.Equal(1, clear.RouteId);

        var ag = new[] { Route(4, AgAlertPolicy.ReplicaDisconnectedMetric, teams: "https://teams.example.invalid/ag") };
        Assert.Equal(
            NotificationRouter.Resolve(AgAlertPolicy.ReplicaDisconnectedMetric, ag, settings).Teams,
            NotificationRouter.Resolve(AgAlertPolicy.ReplicaReconnectedMetric, ag, settings).Teams);

        /* An exact route on the RECOVERY's own name outranks one on its firing — the operator said so by name;
           the firing's route still takes the firing. */
        var onRecovery = new[]
        {
            Route(1, "Server Unreachable", slack: "https://slack.example.invalid/outages"),
            Route(2, "Server Restored", slack: "https://slack.example.invalid/all-clear"),
        };
        Assert.Equal("https://slack.example.invalid/all-clear", NotificationRouter.Resolve("Server Restored", onRecovery, settings).Slack.Destination);
        Assert.Equal("https://slack.example.invalid/outages", NotificationRouter.Resolve("Server Unreachable", onRecovery, settings).Slack.Destination);
    }

    [Fact]
    public void MetricMatch_IsCaseInsensitiveAndTrimmed_ForBothKinds()
    {
        var settings = FullyConfigured();
        var routes = new[]
        {
            Route(1, "  deadlocks detected ", slack: "https://slack.example.invalid/exact"),
            Route(2, " REPORTS ", slack: "https://slack.example.invalid/reports"),
        };

        Assert.Equal(RouteSource.Exact, NotificationRouter.Resolve("Deadlocks Detected", routes, settings).Slack.Source);
        Assert.Equal(RouteSource.Family, NotificationRouter.Resolve("Fleet Sweep Rollup", routes, settings).Slack.Source);
        Assert.True(routes[1].IsFamilyRoute);
        Assert.False(routes[0].IsFamilyRoute);
    }

    /* ---- the ledger ---------------------------------------------------------------------------------- */

    /// <summary>
    /// The persisted projection carries only DELIVERED channels, round-trips through the serializer's
    /// trailing member, is readable by the one-property reader, and — the #3539 A8e lesson — is absent, not
    /// broken, on a row written before it existed.
    /// </summary>
    [Fact]
    public void TheRouteRecord_RoundTripsThroughTheContext_AndIsNullOnOlderRows()
    {
        var settings = new FakeSettings { SlackWebhookUrl = "https://hooks.example.invalid/parent" };
        var routes = new[] { Route(5, AlertFamily.SelfMonitor, teams: "https://teams.example.invalid/self") };
        var decision = NotificationRouter.Resolve("Compression Job Stuck", routes, settings);

        var dto = decision.ToDto();
        Assert.Equal(AlertFamily.SelfMonitor, dto.Family);
        Assert.Equal(5, dto.RouteId);
        /* Teams from the route, Slack from the parent; Generic/PagerDuty/Email resolved to nothing and are
           not listed — the row says where the post went, not where it did not. */
        Assert.Equal(new[] { ("Teams", (int?)5, "Family"), ("Slack", (int?)null, "Default") },
            dto.Destinations.Select(d => (d.Channel, d.RouteId, d.Source)).ToArray());

        var context = new AlertContext { SeverityOverride = AlertSeverityLevel.Critical, Route = dto };
        var json = AlertContextSerializer.Serialize(context);
        Assert.Contains("\"Route\":{\"Family\":\"self-monitor\",\"RouteId\":5", json, StringComparison.Ordinal);

        var read = AlertContextSerializer.TryReadRoute(json);
        Assert.NotNull(read);
        Assert.Equal(Flat(dto), Flat(read!));

        Assert.True(AlertContextSerializer.TryDeserialize(json, out var rehydrated));
        Assert.NotNull(rehydrated.Route);
        Assert.Equal(Flat(dto), Flat(rehydrated.Route!));
        /* The severity beside it is untouched. */
        Assert.Equal(AlertSeverityLevel.Critical, AlertContextSerializer.TryReadSeverity(json));

        /* A pre-#3598 row: the member is absent and reads as absent. */
        const string older = "{\"Details\":[],\"Incidents\":null,\"Severity\":\"Warning\"}";
        Assert.Null(AlertContextSerializer.TryReadRoute(older));
        Assert.True(AlertContextSerializer.TryDeserialize(older, out var olderContext));
        Assert.Null(olderContext.Route);
        Assert.Null(AlertContextSerializer.TryReadRoute(null));
        Assert.Null(AlertContextSerializer.TryReadRoute("not json"));
    }

    /* ---- the fan-out --------------------------------------------------------------------------------- */

    /// <summary>
    /// The whole path, measured at the bytes that left the process: a self-monitor route pointing Slack at a
    /// second endpoint takes the self-alert and leaves the page on the parent; the recovery of an exact-routed
    /// firing lands where the firing did; and the history row records the route each post took. Zero-route
    /// identity is asserted on the SAME service by firing the unrouted family and comparing its body to the
    /// payload builder's own render on the same fixed clock (#3355's idiom) — the fan-out with no matching
    /// route is the builder's output, byte for byte.
    /// </summary>
    [Fact]
    public async Task TheFanOut_PostsEachFamilyWhereItsRouteSays_AndTheLedgerSaysSo()
    {
        using var parent = new CapturingWebhookEndpoint();
        using var selfMonitor = new CapturingWebhookEndpoint();
        using var outages = new CapturingWebhookEndpoint();

        var config = new DarlingConfig();
        config.Webhooks.SlackUrl = parent.Url;
        config.NotificationRoutes = new[]
        {
            Route(1, AlertFamily.SelfMonitor, slack: selfMonitor.Url),
            Route(2, "Server Unreachable", slack: outages.Url),
        };

        var settings = new DarlingAlertSettings(config);
        var history = new RecordingHistoryStore();
        var clock = new DateTime(2026, 9, 18, 22, 0, 0, DateTimeKind.Utc);
        var webhooks = new WebhookAlertService(
            settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history, () => clock);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        /* A page: no route matches, so the parent gets it — and its body is the builder's own render. */
        await deliverer.DeliverAsync(Outcome("Deadlocks Detected", "SQL01", "3", "1"), TestContext.Current.CancellationToken);
        var expected = WebhookAlertService.BuildSlackPayload(
            "Deadlocks Detected", "SQL01", "3", "1", DarlingAlertDeliverer.Branding, context: null, triageUrl: null,
            detailText: null, displayName: null, nowUtc: clock);
        Assert.Equal(expected, Assert.Single(parent.Bodies));
        Assert.Empty(selfMonitor.Bodies);

        /* A self-alert: the family route takes Slack, the parent sees nothing. */
        await deliverer.DeliverAsync(Outcome("Compression Job Stuck", "Monitor Store", "stuck", "running"), TestContext.Current.CancellationToken);
        Assert.Single(parent.Bodies);
        Assert.Contains("Compression Job Stuck", Assert.Single(selfMonitor.Bodies), StringComparison.Ordinal);

        /* Fire and clear: both on the exact route's endpoint, neither on the parent. */
        await deliverer.DeliverAsync(Outcome("Server Unreachable", "SQL02", "Offline", "Online"), TestContext.Current.CancellationToken);
        await deliverer.DeliverAsync(Outcome("Server Restored", "SQL02", "Online", "Online"), TestContext.Current.CancellationToken);
        Assert.Equal(2, outages.Bodies.Count);
        Assert.Contains("Server Unreachable", outages.Bodies[0], StringComparison.Ordinal);
        Assert.Contains("Server Restored", outages.Bodies[1], StringComparison.Ordinal);
        Assert.Single(parent.Bodies);

        /* The ledger: four rows, each carrying the route its post took. */
        Assert.Equal(4, history.Records.Count);
        var routes = history.Records.Select(r => AlertContextSerializer.TryReadRoute(r.ContextJson)).ToList();
        Assert.All(routes, r => Assert.NotNull(r));
        Assert.Equal((AlertFamily.Performance, (int?)null, "Slack", (int?)null, "Default"), Flatten(routes[0]!));
        Assert.Equal((AlertFamily.SelfMonitor, (int?)1, "Slack", (int?)1, "Family"), Flatten(routes[1]!));
        Assert.Equal((AlertFamily.Performance, (int?)2, "Slack", (int?)2, "Exact"), Flatten(routes[2]!));
        Assert.Equal((AlertFamily.Performance, (int?)2, "Slack", (int?)2, "Exact"), Flatten(routes[3]!));
        Assert.All(history.Records, r => Assert.True(r.Delivery.Sent));
    }

    /// <summary>A route can make the fan-out attempt a channel the parent lacks, and the gate before the
    /// cooldown knows it — but only for the family the route names; the rest report nothing attempted.</summary>
    [Fact]
    public async Task ARouteAlone_ConfiguresTheFanOut_ForItsFamilyOnly()
    {
        using var pages = new CapturingWebhookEndpoint();

        var config = new DarlingConfig();
        config.NotificationRoutes = new[] { Route(1, AlertFamily.Performance, slack: pages.Url) };
        var settings = new DarlingAlertSettings(config);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);

        Assert.True(webhooks.AnyWebhookConfigured);

        var page = await webhooks.TrySendWebhookAlertsAsync("High CPU", "SQL01", "97%", "90%");
        Assert.Equal(AlertChannelOutcome.Delivered, page.Outcome);
        Assert.Equal(RouteSource.Family, page.Route!.Slack.Source);
        Assert.Single(pages.Bodies);

        var report = await webhooks.TrySendWebhookAlertsAsync("Fleet Sweep Rollup", "Monitor Store", "3", "0");
        Assert.Equal(AlertChannelOutcome.NotAttempted, report.Outcome);
        Assert.NotNull(report.Route);
        Assert.False(report.Route!.AnyWebhookDelivered);
        Assert.Single(pages.Bodies);

        /* And with nothing anywhere, the pre-routes early return: no route record at all. */
        var bare = new WebhookAlertService(new DarlingAlertSettings(new DarlingConfig()), DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance);
        Assert.False(bare.AnyWebhookConfigured);
        Assert.Null((await bare.TrySendWebhookAlertsAsync("High CPU", "SQL01", "97%", "90%")).Route);
    }

    /* ---- helpers ------------------------------------------------------------------------------------- */

    /// <summary>The record carries a List, so record equality is reference equality on it — compare the projection.</summary>
    private static string Flat(AlertRouteDto dto) =>
        $"{dto.Family}|{dto.RouteId}|" + string.Join(";", dto.Destinations.Select(d => $"{d.Channel}:{d.RouteId}:{d.Source}"));

    private static (string Family, int? RouteId, string Channel, int? ChannelRouteId, string Source) Flatten(AlertRouteDto dto)
    {
        var destination = Assert.Single(dto.Destinations);
        return (dto.Family, dto.RouteId, destination.Channel, destination.RouteId, destination.Source);
    }

    private static AlertOutcome Outcome(string metric, string server, string value, string threshold) =>
        new(server, server, metric, value, threshold, Context: null, DetailText: null,
            NumericCurrentValue: 0, NumericThresholdValue: 0, Muted: false, Severity: null);

    internal static NotificationRoute Route(
        int id, string match, string teams = "", string slack = "", string generic = "", string pagerDuty = "",
        string email = "", bool enabled = true) =>
        new(id, match, teams, slack, generic, pagerDuty, email, enabled);

    private static FakeSettings FullyConfigured() => new()
    {
        TeamsWebhookUrl = "https://teams.example.invalid/parent",
        SlackWebhookUrl = "https://hooks.example.invalid/parent",
        GenericWebhookUrl = "https://generic.example.invalid/parent",
        PagerDutyRoutingKey = "parent-pd-key",
        SmtpServer = "smtp.example.invalid",
        SmtpFromAddress = "monitor@example.invalid",
        SmtpRecipients = "ops@example.invalid",
    };

    /// <summary>
    /// Every metric name the product delivers through a channel: the public constants, plus the string
    /// literals <c>AlertEngine</c>, <c>DarlingWorker</c> and <c>DarlingSelfAlertEvaluator</c> fire without
    /// one — read from SOURCE at the fire sites (an <c>AlertOutcome(</c> construction's third argument, a
    /// <c>FireAsync(</c> call's third argument) so the census tracks the code rather than a list here.
    /// </summary>
    private static HashSet<string> DeliveredMetricNames()
    {
        var names = new HashSet<string>(StringComparer.Ordinal)
        {
            AlertEngine.BlockingWatermarkMetric, AlertEngine.DeadlockWatermarkMetric,
            AlertEngine.LongRunningQueryWatermarkMetric, AlertEngine.VolumeFreeSpaceWatermarkMetric,
            AlertEngine.PvsWatermarkMetric, AlertEngine.FileGrowthWatermarkMetric,
            AlertEngine.AnomalousJobWatermarkMetric, AlertEngine.FailedJobWatermarkMetric,
            AlertEngine.CpuPersistenceMetric,
            PostgresAlertEvaluator.WraparoundMetric, PostgresAlertEvaluator.XminHorizonMetric,
            PostgresAlertEvaluator.SlotRetentionMetric, PostgresAlertEvaluator.PoisonWaitMetric,
            ForcePlanTokens.MetricName, DatabaseStateTokens.MetricName,
            AgAlertPolicy.FailoverMetric, AgAlertPolicy.ReplicaDisconnectedMetric, AgAlertPolicy.ReplicaReconnectedMetric,
            AgAlertPolicy.SyncFellBehindMetric, AgAlertPolicy.DatabaseSuspendedMetric,
            DarlingSelfAlertEvaluator.CollectorCostDigestMetric, DarlingSelfAlertEvaluator.FleetSweepRollupMetric,
            DarlingSelfAlertEvaluator.AnalysisSinglesDigestMetric,
            DarlingSelfAlertEvaluator.DiskPressureMetric, DarlingSelfAlertEvaluator.CustomRuleHealthMetric,
            DarlingSelfAlertEvaluator.StaleMuteMetric, DarlingSelfAlertEvaluator.WebTlsCertExpiryMetric,
            DarlingSelfAlertEvaluator.StoreSettingsMetric,
            DarlingSelfAlertEvaluator.CompressionJobMetric, DarlingSelfAlertEvaluator.JobCadenceMetric,
            DarlingSelfAlertEvaluator.RetentionHoldMetric, DarlingSelfAlertEvaluator.StoreUpgradeMetric,
            /* #3816: the policy-job self-heal's two new per-family names and its total_failures arm. Listed
               here rather than found by the FireAsync literal scan below because all three fire through a
               band record's field (band.Metric) rather than a quoted string at the call site — the scan
               cannot see a variable, so the census's forcing function is this line. */
            DarlingSelfAlertEvaluator.RefreshJobStuckMetric, DarlingSelfAlertEvaluator.RetentionJobStuckMetric,
            DarlingSelfAlertEvaluator.PolicyJobFailingMetric,
            /* #3783: the store's TOAST slack and checkpointer pressure, both fired through the constant. */
            DarlingSelfAlertEvaluator.ToastSlackMetric, DarlingSelfAlertEvaluator.CheckpointerPressureMetric,
        };

        /* The literals: the third argument of every `new AlertOutcome(` in the shared engine and the worker
           when it is a quoted string, and of every `FireAsync(` in the self-alert evaluator. */
        var engine = RepoFile.ReadRepoFile("PerformanceMonitor.Alerting", "AlertEngine.cs");
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var self = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs");

        foreach (var source in new[] { engine, worker })
        {
            foreach (Match m in Regex.Matches(source, "new AlertOutcome\\(\\s*[^,]+,\\s*[^,]+,\\s*\"([^\"]+)\""))
            {
                names.Add(m.Groups[1].Value);
            }
        }

        foreach (Match m in Regex.Matches(self, "FireAsync\\(\\s*[^,]+,\\s*[^,]+,\\s*\"([^\"]+)\""))
        {
            names.Add(m.Groups[1].Value);
        }

        /* Two literals the scans above cannot see: "Blocking Wait Time" and "Blocking Detected" are fired via
           a `metricName` local in the worker, which the const scan catches through the engine constants;
           "Capture Down" / "Agent Not Running" / "Collection Stopped" / "Server Unreachable" / "Server
           Restored" / "Collector Cost Regression" are FireAsync literals the regex above reads. Assert the
           scan really found the self-alert literals so a regex drift fails here, not in the census. */
        Assert.Contains("Collection Stopped", names);
        Assert.Contains("Server Restored", names);
        Assert.Contains("Collector Cost Regression", names);
        Assert.Contains("Blocking Wait Time", names);
        return names;
    }

    private sealed class RecordingHistoryStore : IAlertHistoryStore
    {
        public List<AlertHistoryRecord> Records { get; } = new();

        public Task RecordAlertAsync(AlertHistoryRecord record)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }

    /// <summary>An <see cref="IAlertSettings"/> whose every member is a plain settable default — the routes
    /// member deliberately NOT overridden, so it exercises the interface's own empty default.</summary>
    private sealed class FakeSettings : IAlertSettings
    {
        public bool SmtpEnabled => !string.IsNullOrWhiteSpace(SmtpServer) && !string.IsNullOrWhiteSpace(SmtpFromAddress) && !string.IsNullOrWhiteSpace(SmtpRecipients);
        public string SmtpServer { get; init; } = "";
        public int SmtpPort => 25;
        public bool SmtpUseSsl => false;
        public string SmtpUsername => "";
        public string SmtpFromAddress { get; init; } = "";
        public string SmtpRecipients { get; init; } = "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => !string.IsNullOrWhiteSpace(TeamsWebhookUrl);
        public string TeamsWebhookUrl { get; init; } = "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => !string.IsNullOrWhiteSpace(SlackWebhookUrl);
        public string SlackWebhookUrl { get; init; } = "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => !string.IsNullOrWhiteSpace(GenericWebhookUrl);
        public string GenericWebhookUrl { get; init; } = "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => !string.IsNullOrWhiteSpace(PagerDutyRoutingKey);
        public string PagerDutyRoutingKey { get; init; } = "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity => 1.5;
        public int AnalysisNotifyCooldownMinutes => 360;
        public int AnalysisPageCap => 5;
        public string TriageBaseUrl => "";
    }
}
