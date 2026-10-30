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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The investigation dispatch hook without a store (#5632 Phase A, plan v2 section 4, ruling R1): which written rows reach the
/// store, what the store is handed, and that a store failure or a cancellation never fails the delivery that called it. The
/// store's own behavior is <see cref="InvestigationDispatchLiveTests"/> and <c>InvestigationRequestStoreLiveTests</c>.
/// </summary>
public sealed class InvestigationDispatcherTests
{
    private static NotificationRoute AgentRoute(int routeId, string match, int? level = NotificationRoute.AgentLevelInvestigate) =>
        new(routeId, match, "", "", "", "", "", Enabled: true) { AgentLevel = level, AgentMaxPerHour = 7, AgentMaxPerDay = 70 };

    private static DarlingConfig Config(params NotificationRoute[] routes)
    {
        var config = new DarlingConfig();
        config.NotificationRoutes = routes;
        return config;
    }

    private static AlertOutcome Outcome(string metric, bool muted = false, int incidents = 0, string serverKey = "42")
    {
        AlertContext? context = null;
        if (incidents > 0)
        {
            context = new AlertContext();
            AlertIncidentRenderer.Apply(context, Enumerable.Range(0, incidents)
                .Select(i => new AlertIncident("key" + i, new[] { "db.dbo.T" + i }, OccurrenceCount: i + 1)).ToList());
        }

        return new AlertOutcome(serverKey, "srv", metric, "3", "1", context, "detail",
            NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: muted, Severity: null);
    }

    private static (DarlingAlertDeliverer Deliverer, RecordingHistoryStore History, List<InvestigationAlert> Asked) Build(
        DarlingConfig config, Func<InvestigationAlert, Task<InvestigationOpenResult>>? store = null)
    {
        var settings = new DarlingAlertSettings(config);
        var history = new RecordingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var asked = new List<InvestigationAlert>();
        var dispatcher = new InvestigationDispatcher(
            (alert, _) =>
            {
                asked.Add(alert);
                return store is null
                    ? Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, Guid.NewGuid()))
                    : store(alert);
            },
            settings, NullLogger.Instance);
        return (new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, investigationDispatcher: dispatcher), history, asked);
    }

    [Fact]
    public async Task AnEngineAlert_WithAnAgentRoute_IsHandedToTheStore_WithItsRowsTimeFamilyRouteAndEveryIncidentKey()
    {
        var (deliverer, history, asked) = Build(Config(AgentRoute(5, "Deadlocks Detected")));

        Assert.NotNull(await deliverer.DeliverAndReportAsync(Outcome("Deadlocks Detected", incidents: 3), TestContext.Current.CancellationToken));

        var record = Assert.Single(history.Records);
        var alert = Assert.Single(asked);
        Assert.NotNull(record.AlertTimeUtc);
        Assert.Equal(record.AlertTimeUtc, alert.AlertTimeUtc);
        Assert.Equal(0, record.AlertTimeUtc!.Value.Ticks % 10);
        Assert.Equal((5, 42, "blocking", "Deadlocks Detected"), (alert.RouteId, alert.ServerId, alert.Family, alert.MetricName));
        Assert.Equal((7, 70), (alert.MaxPerHour, alert.MaxPerDay));
        /* The PagerDuty key is the first incident's, then every incident's own key: one link per incident. */
        Assert.Equal(new[] { "key0", "key1", "key2" }, alert.DedupKeys);
        Assert.Null(alert.StoryPathHash);
    }

    [Fact]
    public async Task APerEventSplit_HandsEachIncidentRowToTheStore()
    {
        var config = Config(AgentRoute(5, "Deadlocks Detected"));
        config.Alerts.DeliveryMode = AlertNotificationMode.PerEvent;
        var (deliverer, history, asked) = Build(config);

        await deliverer.DeliverAsync(Outcome("Deadlocks Detected", incidents: 3), TestContext.Current.CancellationToken);

        Assert.Equal(3, history.Records.Count);
        Assert.Equal(new[] { "key0", "key1", "key2" }, asked.Select(a => Assert.Single(a.DedupKeys)).ToArray());
        Assert.Equal(history.Records.Select(r => r.AlertTimeUtc), asked.Select(a => (DateTime?)a.AlertTimeUtc));
    }

    [Theory]
    [InlineData("Deadlocks Detected", true)] // muted
    [InlineData("Collector Cost Digest", false)] // a report
    [InlineData("Server Restored", false)] // a recovery
    [InlineData("High CPU", false)] // no route sends it to an agent
    public async Task AMutedRow_AReport_ARecovery_AndAnUnroutedAlert_NeverReachTheStore(string metric, bool muted)
    {
        /* The reports and performance families are routed to an agent, so a report and a recovery are refused by the family
           map, not by a missing route. */
        var (deliverer, history, asked) = Build(Config(
            AgentRoute(1, "Deadlocks Detected"), AgentRoute(2, AlertFamily.Reports), AgentRoute(3, "Server Restored")));

        await deliverer.DeliverAsync(Outcome(metric, muted), TestContext.Current.CancellationToken);

        Assert.Single(history.Records);
        Assert.Empty(asked);
    }

    [Fact]
    public async Task AnAgentLevelOfZero_OnAMoreSpecificRoute_StopsTheLookup()
    {
        var (deliverer, _, asked) = Build(Config(
            AgentRoute(1, AlertFamily.Performance), AgentRoute(2, "Deadlocks Detected", NotificationRoute.AgentLevelOff)));

        await deliverer.DeliverAsync(Outcome("Deadlocks Detected"), TestContext.Current.CancellationToken);

        Assert.Empty(asked);
    }

    [Fact]
    public async Task AStoreFailure_NeverFailsTheDelivery()
    {
        var (deliverer, history, asked) = Build(Config(AgentRoute(5, "Deadlocks Detected")),
            _ => throw new NpgsqlException("store down"));

        var delivery = await deliverer.DeliverAndReportAsync(Outcome("Deadlocks Detected"), TestContext.Current.CancellationToken);

        /* The row was written and the delivery reported; a throw would have turned the answer null. */
        Assert.NotNull(delivery);
        Assert.Single(history.Records);
        Assert.Single(asked);
    }

    [Fact]
    public async Task ACancelledDispatch_ReturnsNull_AndDoesNotThrow()
    {
        var settings = new DarlingAlertSettings(Config(AgentRoute(5, "Deadlocks Detected")));
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var dispatcher = new InvestigationDispatcher(
            (_, token) => Task.FromException<InvestigationOpenResult>(new OperationCanceledException(token)),
            settings, NullLogger.Instance);
        var record = new AlertHistoryRecord("42", "srv", "Deadlocks Detected", "3", "1", 3, 1,
            AlertDelivery.RoutedToDigest(), false, null, null);

        Assert.Null(await dispatcher.DispatchAsync(record, null, cancellationToken: cts.Token));
    }

    [Fact]
    public async Task TheFindingSender_HandsASinglePageAndEachHeldPage_ButNeverADigestFinding()
    {
        var settings = new DarlingAlertSettings(Config(AgentRoute(5, AlertFamily.Performance)));
        var history = new RecordingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var asked = new List<InvestigationAlert>();
        var dispatcher = new InvestigationDispatcher(
            (alert, _) =>
            {
                asked.Add(alert);
                return Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, Guid.NewGuid()));
            },
            settings, NullLogger.Instance);
        var sender = new DarlingFindingAlertSender(settings, history, webhooks, NullLogger.Instance, dispatcher);

        await sender.SendFindingAlertAsync(Finding("Analysis: blocking [aaaa1111]", FindingRoute.Digest));
        Assert.Empty(asked);

        await sender.SendFindingAlertAsync(Finding("Analysis: blocking [bbbb2222]", FindingRoute.Page));
        Assert.Equal("blocking", Assert.Single(asked).Family);

        await sender.SendFindingSummaryAsync(new[] { Finding("Analysis: blocking [cccc3333]"), Finding("Analysis: memory [dddd4444]") });
        Assert.Equal(3, asked.Count);
        Assert.Equal(history.Records.Skip(1).Select(r => r.AlertTimeUtc), asked.Select(a => (DateTime?)a.AlertTimeUtc));
    }

    /// <summary>#5632 Phase A: the metric name holds eight characters of the story path hash; the request records the
    /// whole hash the finding alert carries, on a single page and on each held page a summary names.</summary>
    [Fact]
    public async Task AFindingPage_HandsTheStoreItsFullStoryPathHash_NotTheMetricNamesEightCharacters()
    {
        var settings = new DarlingAlertSettings(Config(AgentRoute(5, AlertFamily.Performance)));
        var history = new RecordingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var asked = new List<InvestigationAlert>();
        var dispatcher = new InvestigationDispatcher(
            (alert, _) =>
            {
                asked.Add(alert);
                return Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, Guid.NewGuid()));
            },
            settings, NullLogger.Instance);
        var sender = new DarlingFindingAlertSender(settings, history, webhooks, NullLogger.Instance, dispatcher);

        await sender.SendFindingAlertAsync(Finding("Analysis: blocking [bbbb2222]", hash: "bbbb2222cccc3333dddd4444"));
        await sender.SendFindingSummaryAsync(new[]
        {
            Finding("Analysis: memory [dddd4444]", hash: "dddd4444eeee5555ffff6666"),
            Finding("Analysis: blocking [aaaa1111]"),
        });

        Assert.Equal(new string?[] { "bbbb2222cccc3333dddd4444", "dddd4444eeee5555ffff6666", null }, asked.Select(a => a.StoryPathHash));
    }

    /// <summary>#6596: a finding page whose story the analysis pass labelled "recurring at this hour" opens no request, on a
    /// single page and on each held page a summary names. Its row is still written and the skip is journaled once per page. The
    /// same page without the label is handed to the store as before, so the check is not over-broad.</summary>
    [Fact]
    public async Task ARecurringAtThisHourFindingPage_OpensNoRequest_ButItsRowIsWritten_AndTheSkipIsJournaled()
    {
        var settings = new DarlingAlertSettings(Config(AgentRoute(5, AlertFamily.Performance)));
        var history = new RecordingHistoryStore();
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var asked = new List<InvestigationAlert>();
        var skipped = new List<InvestigationAlert>();
        var dispatcher = new InvestigationDispatcher(
            (alert, _) =>
            {
                asked.Add(alert);
                return Task.FromResult(new InvestigationOpenResult(InvestigationOpenOutcome.Opened, Guid.NewGuid()));
            },
            settings, NullLogger.Instance,
            recurringSkipped: (alert, _) =>
            {
                skipped.Add(alert);
                return Task.CompletedTask;
            });
        var sender = new DarlingFindingAlertSender(settings, history, webhooks, NullLogger.Instance, dispatcher);

        var delivery = await sender.SendFindingAlertAsync(Finding("Analysis: anomaly [bbbb2222]", hash: "bbbb2222cccc", recurring: true));
        await sender.SendFindingSummaryAsync(new[] { Finding("Analysis: anomaly [cccc3333]", hash: "cccc3333dddd", recurring: true) });

        Assert.NotNull(delivery);
        Assert.Empty(asked);
        Assert.Equal(2, history.Records.Count);
        Assert.Equal(new[] { "bbbb2222cccc", "cccc3333dddd" }, skipped.Select(a => a.StoryPathHash));
        Assert.All(skipped, a => Assert.Equal((5, 43), (a.RouteId, a.ServerId)));

        await sender.SendFindingAlertAsync(Finding("Analysis: anomaly [dddd4444]", hash: "dddd4444eeee"));
        Assert.Equal("dddd4444eeee", Assert.Single(asked).StoryPathHash);
        Assert.Equal(2, skipped.Count);
    }

    internal static FindingAlert Finding(string metric, FindingRoute route = FindingRoute.Page, string serverId = "43", string? hash = null,
        bool recurring = false) =>
        new(metric, "srv", "0.9", "0.5", serverId, new AlertContext(), 0.9, 0.5, "finding detail", false, route, hash, recurring);

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
}
