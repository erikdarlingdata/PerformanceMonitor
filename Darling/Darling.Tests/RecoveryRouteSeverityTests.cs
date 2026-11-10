/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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
/// #8525: a recovery the product delivers as an alert of its own ("Server Restored", "AG Replica Reconnected") is routed at
/// the tier the firing it closes went out at, not at its own RESOLVED badge. Through the deliverer, the recovery reads the tier
/// its firing was delivered at: a Warning-graded firing's recovery and PagerDuty resolve go to the Warning-only route, a Critical
/// firing's to the Critical-only route. With no delivered fire on record, the recovery routes as a tierless firing of that name
/// does. A route that takes both tiers and an exact-name route behave as before.
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

    /// <summary>An alert as a fire site hands it to the deliverer: the tier is the site's, or null for a site that grades nothing.</summary>
    private static AlertOutcome Outcome(string metric, string value, string threshold, AlertSeverityLevel? severity) =>
        new("7", "SQL01", metric, value, threshold, Context: null, DetailText: null,
            NumericCurrentValue: null, NumericThresholdValue: null, Muted: false, Severity: severity);

    /// <summary>A deliverer over the issue's two PagerDuty routes with auto-resolve on, capturing every PagerDuty post. The history
    /// store is <paramref name="history"/>, so a second deliverer over the same store stands for the service after a restart.</summary>
    private static (DarlingAlertDeliverer Deliverer, List<string> Posted) PagerDutyDeliverer(IAlertHistoryStore? history = null)
    {
        var config = new DarlingConfig();
        config.Webhooks.PagerDutyAutoResolve = true;
        config.NotificationRoutes = new[]
        {
            Route(1, AlertFamily.Performance, NotificationRouteKinds.SeveritiesCritical, pagerDuty: "rk-critical"),
            Route(2, AlertFamily.Performance, NotificationRouteKinds.SeveritiesWarning, pagerDuty: "rk-warning"),
        };
        var settings = new DarlingAlertSettings(config);
        var posted = new List<string>();
        history ??= new CapturingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history)
        {
            PostPagerDutyAsyncOverride = (_, payload) =>
            {
                posted.Add(payload);
                return Task.FromResult<string?>(null);
            },
        };
        return (new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance), posted);
    }

    private static (string Action, string Key) Posted(string payload)
    {
        var root = JsonDocument.Parse(payload).RootElement;
        return (root.GetProperty("event_action").GetString()!, root.GetProperty("routing_key").GetString()!);
    }

    /// <summary>With no delivered fire on record, a recovery routes exactly as a tierless firing of its name does: one badge rule for
    /// both edges, so the destination never depends on the order routes were created in. Both census firings read Critical.</summary>
    [Theory]
    [InlineData("Server Restored")]
    [InlineData("AG Replica Reconnected")]
    public void ARecoveryWithNoRecordedFire_RoutesAsATierlessFiringOfItsNameDoes(string recovery)
    {
        var firing = AlertFamily.RecoveryPairs[recovery];

        Assert.Equal<AlertSeverityLevel?>(AlertSeverityLevel.Critical, AlertSeverity.RouteLevelOf(firing, null));
        Assert.Equal<AlertSeverityLevel?>(AlertSeverity.RouteLevelOf(firing, null), AlertSeverity.RouteLevelOf(recovery, null));

        Assert.Equal(CriticalSlack, SlackFor(recovery, null, SplitFamily()));
        var warningFirst = new[]
        {
            Route(1, AlertFamily.Performance, NotificationRouteKinds.SeveritiesWarning, slack: WarningSlack),
            Route(2, AlertFamily.Performance, NotificationRouteKinds.SeveritiesCritical, slack: CriticalSlack),
        };
        Assert.Equal(CriticalSlack, SlackFor(recovery, null, warningFirst));
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
    /// Critical only, which the recovery reaches because it routes at the firing's tier.</summary>
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

    /// <summary>Through the deliverer, a firing graded Warning at its site: its trigger goes to the Warning-only route's key, and the
    /// recovery, sent with no tier of its own, reads the tier the firing was delivered at and resolves on that same key.</summary>
    [Theory]
    [InlineData("AG Replica Disconnected", "DISCONNECTED", "AG Replica Reconnected", "CONNECTED")]
    [InlineData("Server Unreachable", "Login timeout", "Server Restored", "Online")]
    public async Task AWarningGradedFiring_ThroughTheDeliverer_HasItsRecoveryResolveOnTheWarningRoutesKey(
        string firing, string firingValue, string recovery, string recoveryValue)
    {
        var (deliverer, posted) = PagerDutyDeliverer();

        await deliverer.DeliverAsync(Outcome(firing, firingValue, "CONNECTED", AlertSeverityLevel.Warning), TestContext.Current.CancellationToken);
        await deliverer.DeliverAsync(Outcome(recovery, recoveryValue, "CONNECTED", null), TestContext.Current.CancellationToken);

        Assert.Equal(new[] { ("trigger", "rk-warning"), ("resolve", "rk-warning") }, posted.Select(Posted).ToArray());
    }

    /// <summary>Through the deliverer, the firing at the tier its site gives it today, Critical: the recovery resolves on the Critical-only
    /// route's key, the one the trigger opened the incident on.</summary>
    [Theory]
    [InlineData("AG Replica Disconnected", "DISCONNECTED", "AG Replica Reconnected", "CONNECTED")]
    [InlineData("Server Unreachable", "Login timeout", "Server Restored", "Online")]
    public async Task ACriticalFiring_ThroughTheDeliverer_HasItsRecoveryResolveOnTheCriticalRoutesKey(
        string firing, string firingValue, string recovery, string recoveryValue)
    {
        var (deliverer, posted) = PagerDutyDeliverer();

        await deliverer.DeliverAsync(Outcome(firing, firingValue, "CONNECTED", AlertSeverityLevel.Critical), TestContext.Current.CancellationToken);
        await deliverer.DeliverAsync(Outcome(recovery, recoveryValue, "CONNECTED", null), TestContext.Current.CancellationToken);

        Assert.Equal(new[] { ("trigger", "rk-critical"), ("resolve", "rk-critical") }, posted.Select(Posted).ToArray());
    }

    /// <summary>Through the webhook send alone, with nothing delivered before it: the recovery falls back to its firing's tierless rule,
    /// Critical for both census pairs, and its resolve carries the Critical-only route's key.</summary>
    [Fact]
    public async Task TheRecoverysPagerDutyResolve_WithNoRecordedFire_FallsBackToTheCriticalRoutesKey()
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
            "Server Restored", "SQL01", "Online", "Online", "7",
            context: null, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(new[] { ("resolve", "rk-critical") }, posted.Select(Posted).ToArray());
    }

    /// <summary>After a restart emptied the deliverer's memory, the recovery still routes at the tier its firing was delivered at, read
    /// from the firing's open history row: the firing's delivered rows written after the last row stored under the recovery's own
    /// name. The history outlives the deliverer, as the store does; the second deliverer is the service after the restart.</summary>
    [Theory]
    [InlineData("AG Replica Disconnected", "DISCONNECTED", "AG Replica Reconnected", "CONNECTED")]
    [InlineData("Server Unreachable", "Login timeout", "Server Restored", "Online")]
    public async Task ARestartedDeliverer_RoutesTheRecoveryAtTheTierTheFiringsHistoryRowCarries(
        string firing, string firingValue, string recovery, string recoveryValue)
    {
        var history = new GenericWebhookStatusDedupTests.HistoryBackedStore();
        var (before, postedBefore) = PagerDutyDeliverer(history);
        await before.DeliverAsync(Outcome(firing, firingValue, "CONNECTED", AlertSeverityLevel.Warning), TestContext.Current.CancellationToken);
        Assert.Equal(new[] { ("trigger", "rk-warning") }, postedBefore.Select(Posted).ToArray());

        var (after, postedAfter) = PagerDutyDeliverer(history);
        await after.DeliverAsync(Outcome(recovery, recoveryValue, "CONNECTED", null), TestContext.Current.CancellationToken);

        Assert.Equal(new[] { ("resolve", "rk-warning") }, postedAfter.Select(Posted).ToArray());
    }

    /// <summary>The boundary of the history read is the recovery's own row: a firing the recovery already closed before the restart is
    /// not read again, so a recovery that arrives with no open firing takes the fallback, its firing's tierless rule, Critical.</summary>
    [Theory]
    [InlineData("AG Replica Disconnected", "DISCONNECTED", "AG Replica Reconnected", "CONNECTED")]
    [InlineData("Server Unreachable", "Login timeout", "Server Restored", "Online")]
    public async Task ARestartedDeliverer_DoesNotReadAFiringTheRecoveryAlreadyClosed(
        string firing, string firingValue, string recovery, string recoveryValue)
    {
        var history = new GenericWebhookStatusDedupTests.HistoryBackedStore();
        var (before, postedBefore) = PagerDutyDeliverer(history);
        await before.DeliverAsync(Outcome(firing, firingValue, "CONNECTED", AlertSeverityLevel.Warning), TestContext.Current.CancellationToken);
        await before.DeliverAsync(Outcome(recovery, recoveryValue, "CONNECTED", null), TestContext.Current.CancellationToken);
        Assert.Equal(new[] { ("trigger", "rk-warning"), ("resolve", "rk-warning") }, postedBefore.Select(Posted).ToArray());

        var (after, postedAfter) = PagerDutyDeliverer(history);
        await after.DeliverAsync(Outcome(recovery, recoveryValue, "CONNECTED", null), TestContext.Current.CancellationToken);

        Assert.Equal(new[] { ("resolve", "rk-critical") }, postedAfter.Select(Posted).ToArray());
    }
}
