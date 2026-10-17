/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The request events on a live store (#6003, D5 to D8 of #5632 Phase C): a same-severity join is journaled quiet and never
/// delivered, a higher-severity join is a <c>joined</c> event; <c>resolved</c> waits for every linked metric to have a
/// resolution row (one that names it in <c>Resolves</c>) after its last alert, and a route with SendResolutions off still
/// writes that row; an unclaimed expiry falls back to the fallback route, else the route's human channels, else a self-alert,
/// once; an event id is delivered at most once, a failed post included; and the MCP long-poll wakes for a new event and
/// resumes from its cursor without repeats. A custom rule's clear carries <c>Resolves</c> too (the external drain's is pinned
/// in <see cref="ExternalAlertDrainLiveTests"/>, next to its rig).
/// </summary>
public sealed class InvestigationRequestEventsLiveTests
{
    private const int ServerId = 7;

    /* Whole seconds an hour back: a resolution row written without a stamped time takes the store's real clock, which must
       land after the linked alerts. */
    private static readonly DateTime T0 = new(DateTime.UtcNow.AddHours(-1).Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond,
        DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private DateTime _now = T0;

    private static InvestigationAlert Alert(DateTime at, string metric, string? severity = null, int routeId = 1) =>
        new(routeId, ServerId, "srv-7", "resources", metric, at, Array.Empty<string>(), null, 50, 500,
            LeaseMinutes: 15, ExpiryMinutes: 240)
        { Severity = severity };

    private sealed class FakeSends : IInvestigationEventSends
    {
        public List<(int RouteId, string Url, string EventId, string Payload)> Posts { get; } = [];

        public List<IReadOnlyList<(string Channel, string Destination)>> Notices { get; } = [];

        public List<string> SelfAlerts { get; } = [];

        /// <summary>The error the next post answers; null for a delivered post.</summary>
        public string? PostError { get; set; }

        public Task<string?> PostEventAsync(int routeId, string url, string payload, string eventId, CancellationToken cancellationToken)
        {
            Posts.Add((routeId, url, eventId, payload));
            return Task.FromResult(PostError);
        }

        public Task<IReadOnlyList<string>> SendNoticeAsync(
            IReadOnlyList<(string Channel, string Destination)> targets, string title, string serverName, string currentValue,
            string detail, CancellationToken cancellationToken)
        {
            Notices.Add(targets);
            return Task.FromResult<IReadOnlyList<string>>(targets.Select(t => t.Channel).ToList());
        }

        public Task RaiseSelfAlertAsync(string serverName, int serverId, string shortMessage, string detail, CancellationToken cancellationToken)
        {
            SelfAlerts.Add(shortMessage);
            return Task.CompletedTask;
        }
    }

    private InvestigationEventDelivery Delivery(InvestigationRequestStore store, FakeSends sends, params NotificationRoute[] routes) =>
        new(store, () => routes, sends, null, NullLogger.Instance, () => _now);

    private static NotificationRoute Route(int routeId, string slack = "", string teams = "", string generic = "", int? fallback = null) =>
        new(routeId, "High CPU", teams, slack, generic, "", "", Enabled: true)
        {
            AgentLevel = NotificationRoute.AgentLevelInvestigate, AgentMaxPerHour = 50, AgentMaxPerDay = 500,
            FallbackRouteId = fallback,
        };

    [Fact]
    public async Task ASameSeverityJoin_IsJournaledQuiet_AndNeverDelivered()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var opened = await store.OpenOrJoinAsync(Alert(T0, "High CPU", InvestigationEvents.Warning), ct);
            Assert.True(opened.Evented);

            _now = T0.AddMinutes(5);
            var join = await store.OpenOrJoinAsync(Alert(_now, "High CPU", InvestigationEvents.Warning), ct);
            Assert.Equal((InvestigationOpenOutcome.Joined, opened.RequestId, false), (join.Outcome, join.RequestId, join.Evented));

            Assert.Equal("opened,joined_quiet", await ScalarAsync(connection,
                "SELECT string_agg(event, ',' ORDER BY journal_id) FROM config.investigation_journal", ct));
            var pending = await store.PendingDeliveriesAsync(T0.AddMinutes(-1), 100, ct);
            Assert.Equal(new[] { InvestigationEvents.Opened }, pending.Select(p => p.Event));
        });
    }

    [Fact]
    public async Task AHigherSeverityJoin_IsAJoinedEvent_AndALaterSameSeverityJoinIsQuiet()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var opened = await store.OpenOrJoinAsync(Alert(T0, "High CPU", InvestigationEvents.Warning), ct);

            _now = T0.AddMinutes(5);
            var higher = await store.OpenOrJoinAsync(Alert(_now, "High CPU", InvestigationEvents.Critical), ct);
            Assert.True(higher.Evented);

            /* The request's max_severity is critical now: another critical is no news. */
            _now = T0.AddMinutes(6);
            var again = await store.OpenOrJoinAsync(Alert(_now, "High CPU", InvestigationEvents.Critical), ct);
            Assert.False(again.Evented);

            Assert.Equal("opened,joined,joined_quiet", await ScalarAsync(connection,
                "SELECT string_agg(event, ',' ORDER BY journal_id) FROM config.investigation_journal", ct));
            Assert.Equal("critical", await ScalarAsync(connection,
                $"SELECT max_severity FROM config.investigation_requests WHERE request_id = '{opened.RequestId}'", ct));
            var pending = await store.PendingDeliveriesAsync(T0.AddMinutes(-1), 100, ct);
            Assert.Equal(new[] { InvestigationEvents.Opened, InvestigationEvents.Joined }, pending.Select(p => p.Event));
            Assert.Contains("\"severity\": \"critical\"", pending[1].DetailJson, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Resolved_WaitsForEveryLinkedMetric_AndARefireIsNotAClear()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var history = new PgAlertHistoryStore(_dataSource!, NullLogger.Instance);
            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            _now = T0.AddMinutes(2);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, "Memory Pressure"), ct)).Evented);

            /* High CPU clears: Memory Pressure is still firing, so nothing resolves. */
            await history.RecordAlertAsync(Clear("High CPU", "CPU Resolved", T0.AddMinutes(3)));
            _now = T0.AddMinutes(4);
            Assert.Equal(0, (await store.SweepAsync(ct)).Resolved);

            /* Memory Pressure fires again: a row whose metric_name IS the metric is the next fire, not its clear. */
            await history.RecordAlertAsync(new AlertHistoryRecord(
                ServerId.ToString(CultureInfo.InvariantCulture), "srv-7", "Memory Pressure", "1", "0", 1, 0,
                AlertDelivery.RoutedToDigest(), false, null, null) { AlertTimeUtc = T0.AddMinutes(5) });
            _now = T0.AddMinutes(6);
            Assert.Equal(0, (await store.SweepAsync(ct)).Resolved);

            /* Memory Pressure clears: every linked metric has a clear after its last alert. */
            await history.RecordAlertAsync(Clear("Memory Pressure", "Memory Pressure Resolved", T0.AddMinutes(7)));
            _now = T0.AddMinutes(8);
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            _now = T0.AddMinutes(9);
            Assert.Equal(0, (await store.SweepAsync(ct)).Resolved);

            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'resolved' AND request_id = '{id}'", ct));
        });
    }

    [Fact]
    public async Task ARouteWithSendResolutionsOff_SendsNoClear_AndStillResolvesTheRequest()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            using var slack = new CapturingWebhookEndpoint();
            var config = new DarlingConfig();
            config.Webhooks.SlackSendResolutions = true;
            config.NotificationRoutes = [new NotificationRoute(1, "High CPU", "", slack.Url, "", "", "", true) { SendResolutions = false }];
            var settings = new DarlingAlertSettings(config);
            var history = new PgAlertHistoryStore(_dataSource!, NullLogger.Instance);
            var webhooks = new WebhookAlertService(
                settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
            var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);
            var key = ServerId.ToString(CultureInfo.InvariantCulture);

            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            await deliverer.DeliverAsync(new AlertOutcome(key, "srv-7", "High CPU", "97%", "90%", Context: null, DetailText: null,
                NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null), ct);
            await deliverer.DeliverResolutionAsync(
                new AlertResolution(key, "srv-7", "High CPU", "CPU Resolved", "srv-7: back to 12%"), cancellationToken: ct);

            /* The fire reached Slack; the clear did not, and still wrote its row naming the metric it ends. */
            Assert.Single(slack.Bodies);
            Assert.Equal("High CPU", await ScalarAsync(connection,
                "SELECT context_json::jsonb ->> 'Resolves' FROM config.config_alert_log WHERE metric_name = 'CPU Resolved'", ct));

            _now = DateTime.UtcNow;
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            Assert.Equal("resolved", await ScalarAsync(connection,
                $"SELECT event FROM config.investigation_journal WHERE request_id = '{id}' ORDER BY journal_id DESC LIMIT 1", ct));
        });
    }

    [Theory]
    [InlineData("fallback")]
    [InlineData("route")]
    [InlineData("self")]
    public async Task AnUnclaimedExpiry_FallsBackInOrder_Once(string reaches)
    {
        await RunAsync(async (store, connection, ct) =>
        {
            NotificationRoute[] routes = reaches switch
            {
                "fallback" => [Route(1, teams: "https://teams.example/agent-route", fallback: 2), Route(2, slack: "https://slack.example/humans")],
                "route" => [Route(1, teams: "https://teams.example/agent-route", fallback: 3), Route(3)],
                _ => [Route(1, generic: "https://agent.example/hook")],
            };
            var sends = new FakeSends();
            var delivery = Delivery(store, sends, routes);

            await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct);
            /* A request an agent claimed and let lapse is not unclaimed: its expiry sends no fallback. */
            var claimedId = (await store.OpenOrJoinAsync(Alert(T0, "Blocking Detected") with { Family = "blocking" }, ct)).RequestId!.Value;
            Assert.Equal(InvestigationClaimOutcome.Claimed, (await store.ClaimAsync(claimedId, "agent-a", ct)).Outcome);

            _now = T0.AddMinutes(241);
            Assert.Equal(2, (await store.SweepAsync(ct)).Expired);
            Assert.Equal(1, (await delivery.RunOnceAsync(ct)).Fallbacks);
            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Fallbacks);

            switch (reaches)
            {
                case "fallback":
                    Assert.Equal((NotificationRouter.SlackChannel, "https://slack.example/humans"), Assert.Single(Assert.Single(sends.Notices)));
                    Assert.Empty(sends.SelfAlerts);
                    break;
                case "route":
                    Assert.Equal((NotificationRouter.TeamsChannel, "https://teams.example/agent-route"), Assert.Single(Assert.Single(sends.Notices)));
                    Assert.Empty(sends.SelfAlerts);
                    break;
                default:
                    Assert.Empty(sends.Notices);
                    Assert.Single(sends.SelfAlerts);
                    break;
            }

            Assert.Equal("1", await ScalarAsync(connection,
                "SELECT count(*) FROM config.investigation_journal WHERE event = 'fallback_sent'", ct));
        });
    }

    [Fact]
    public async Task AnEventId_IsDeliveredOnce_AndAFailedPostIsNotRetried()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var sends = new FakeSends();
            var delivery = Delivery(store, sends, Route(1, generic: "https://agent.example/hook"));

            await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct);
            Assert.Equal(1, (await delivery.RunOnceAsync(ct)).Posted);
            var post = Assert.Single(sends.Posts);
            var eventId = Guid.Parse(post.EventId);
            Assert.Equal(eventId.ToString("D"), (string)JsonNode.Parse(post.Payload)!["event_id"]!);

            /* The same event read again: its delivery mark exists, so nothing is sent and the mark is not written twice. */
            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Posted);
            Assert.Single(sends.Posts);
            var replay = new InvestigationEventRow(0, eventId, InvestigationEvents.Opened, T0, Guid.Empty, 1, ServerId, "srv-7",
                "resources", null, null);
            Assert.False(await store.MarkDeliveredAsync(replay, null, ct));

            /* A post that fails is logged, marked, and never retried. */
            sends.PostError = "503 Service Unavailable";
            _now = T0.AddMinutes(5);
            await store.OpenOrJoinAsync(Alert(_now, "Memory Pressure"), ct);
            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Posted);
            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Posted);
            Assert.Equal(2, sends.Posts.Count);
            Assert.Equal("2", await ScalarAsync(connection,
                "SELECT count(*) FROM config.investigation_journal WHERE event = 'event_delivered'", ct));
        });
    }

    [Fact]
    public async Task TheLongPoll_WakesForANewEvent_AndResumesFromItsCursorWithoutRepeats()
    {
        await RunAsync(async (store, _, ct) =>
        {
            var signal = new InvestigationOpenSignal();
            Task<string> Wait(string? after, int seconds) =>
                DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, [1], after, seconds, ct);

            var start = (string)JsonNode.Parse(await Wait(null, 0))!["cursor"]!;
            _now = T0.AddSeconds(10);
            var id = (await store.OpenOrJoinAsync(Alert(_now, "High CPU", InvestigationEvents.Warning), ct)).RequestId!.Value;

            var first = JsonNode.Parse(await Wait(start, 0))!;
            var opened = Assert.Single(first["requests"]!.AsArray())!;
            Assert.Equal((InvestigationEvents.Opened, id.ToString("D")), ((string)opened["event"]!, (string)opened["request_id"]!));
            var cursor = (string)first["cursor"]!;

            /* A call waiting on that cursor wakes for the next event and answers it alone. */
            var wait = Wait(cursor, 20);
            var until = DateTime.UtcNow.AddSeconds(10);
            while (signal.WaiterCount < 1 && DateTime.UtcNow < until)
            {
                await Task.Delay(10, ct);
            }

            Assert.Equal(1, signal.WaiterCount);
            _now = T0.AddSeconds(20);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, "High CPU", InvestigationEvents.Critical), ct)).Evented);
            signal.Opened(1);

            var second = JsonNode.Parse(await wait.WaitAsync(TimeSpan.FromSeconds(10), ct))!;
            var joined = Assert.Single(second["requests"]!.AsArray())!;
            Assert.Equal((InvestigationEvents.Joined, id.ToString("D")), ((string)joined["event"]!, (string)joined["request_id"]!));

            /* The next call from that cursor repeats neither event. */
            _now = T0.AddSeconds(30);
            var third = JsonNode.Parse(await Wait((string)second["cursor"]!, 0))!;
            Assert.Equal("empty", (string)third["status"]!);
            Assert.Empty(third["requests"]!.AsArray());
        });
    }

    [Fact]
    public async Task ACustomRulesClear_NamesTheMetricItResolves_AndResolvesTheRequest()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            const string definitionJson =
                "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
                "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}," +
                "\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1}," +
                "\"scope\":{\"mode\":\"servers\",\"servers\":[\"srv-7\"]}}";
            var (definition, error) = CustomAlertRuleDefinition.TryParse(definitionJson);
            Assert.True(definition is not null, error);
            var rule = new CustomAlertRule(6003, "events rule", definitionJson, null, true, 1, T0, T0, null);
            var deliverer = new RecordingDeliverer();
            var evaluator = new CustomAlertEvaluator(
                new CustomAlertRuleStore(_dataSource!), new CustomAlertStateStore(_dataSource!), _dataSource!, deliverer,
                isAlertMuted: _ => false, alertsEnabled: static () => true, new PgAlertHistoryStore(_dataSource!, NullLogger.Instance),
                defaultIntervalSeconds: 0, cacheTtl: TimeSpan.Zero, NullLogger.Instance);

            var state = CustomAlertRuleState.Fresh(1);
            var fireAt = T0.AddMinutes(1);
            state = await evaluator.ApplyValueAsync(rule, definition!, ServerId, "srv-7", fireAt, fireAt.AddSeconds(30), 50, state, ct);
            var metric = Assert.Single(deliverer.Outcomes).MetricName;

            var id = (await store.OpenOrJoinAsync(Alert(fireAt, metric), ct)).RequestId!.Value;
            var clearAt = T0.AddMinutes(2);
            await evaluator.ApplyValueAsync(rule, definition!, ServerId, "srv-7", clearAt, clearAt.AddSeconds(30), 10, state, ct);

            Assert.Equal(metric, await ScalarAsync(connection,
                "SELECT context_json::jsonb ->> 'Resolves' FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'", ct));
            _now = DateTime.UtcNow;
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'resolved' AND request_id = '{id}'", ct));
        });
    }

    /// <summary>A deliverer that is not a resolution sender, so the custom evaluator writes its clear's row itself.</summary>
    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = [];

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return null;
        }
    }

    private static AlertHistoryRecord Clear(string metric, string title, DateTime at) =>
        DarlingSelfAlertEvaluator.BuildResolutionRecord(
            new AlertResolution(ServerId.ToString(CultureInfo.InvariantCulture), "srv-7", metric, title, "back to normal"))
        with { AlertTimeUtc = at };

    private NpgsqlDataSource? _dataSource;

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation request event live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        _dataSource = dataSource;
        var bodySucceeded = false;
        try
        {
            await body(new InvestigationRequestStore(dataSource, 30, () => _now), connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }
}
