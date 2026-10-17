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

        public Task<bool> RaiseSelfAlertAsync(Guid requestId, string serverName, int serverId, string shortMessage, string detail, CancellationToken cancellationToken)
        {
            SelfAlerts.Add(shortMessage);
            return Task.FromResult(true);
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
            Assert.Equal("critical", (string)JsonNode.Parse(pending[1].DetailJson!)!["severity"]!);
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

    /// <summary>
    /// D7: a clear that sends nothing still writes its row naming the metric it ends, so the request resolves: the route's
    /// SendResolutions off, a mute rule on the clear, a fire that went to the digest, and a fire the cooldown throttled (the
    /// last two leave no delivered fire, so the clear has nothing to answer). Each clear goes through the deliverer.
    /// </summary>
    [Theory]
    [InlineData("off")]
    [InlineData("muted")]
    [InlineData("digested")]
    [InlineData("throttled")]
    public async Task AClearThatSendsNothing_StillWritesItsRow_AndResolvesTheRequest(string why)
    {
        await RunAsync(async (store, connection, ct) =>
        {
            using var slack = new CapturingWebhookEndpoint();
            var config = new DarlingConfig();
            config.Webhooks.SlackSendResolutions = true;
            config.NotificationRoutes = [new NotificationRoute(1, "High CPU", "", slack.Url, "", "", "", true) { SendResolutions = why != "off" }];
            var settings = new DarlingAlertSettings(config);
            var history = new PgAlertHistoryStore(_dataSource!, NullLogger.Instance);
            var webhooks = new WebhookAlertService(
                settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
            var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance,
                isAlertMuted: why == "muted" ? _ => true : null);
            var key = ServerId.ToString(CultureInfo.InvariantCulture);

            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            AlertHistoryRecord Fire(AlertDelivery delivery) =>
                new(key, "srv-7", "High CPU", "97%", "90%", 97, 90, delivery, false, null, null) { AlertTimeUtc = T0 };
            switch (why)
            {
                case "digested":
                    await history.RecordAlertAsync(Fire(AlertDelivery.RoutedToDigest()));
                    break;
                case "throttled":
                    await history.RecordAlertAsync(Fire(AlertDelivery.FromFanout(
                        new EmailFanoutResult(AlertChannelOutcome.Throttled, null, AlertChannelOutcome.Throttled, null, true),
                        muted: false, trayChannelPresent: false)));
                    break;
                default:
                    await deliverer.DeliverAsync(new AlertOutcome(key, "srv-7", "High CPU", "97%", "90%", Context: null, DetailText: null,
                        NumericCurrentValue: 97, NumericThresholdValue: 90, Muted: false, Severity: null), ct);
                    break;
            }

            await deliverer.DeliverResolutionAsync(
                new AlertResolution(key, "srv-7", "High CPU", "CPU Resolved", "srv-7: back to 12%"), cancellationToken: ct);

            /* What reached Slack is the fire, never the clear; the clear still wrote its row naming the metric it ends. */
            if (why is "off" or "muted")
            {
                var body = Assert.Single(slack.Bodies);
                Assert.Contains("High CPU", body, StringComparison.Ordinal);
                Assert.DoesNotContain("CPU Resolved", body, StringComparison.Ordinal);
            }
            else
            {
                Assert.Empty(slack.Bodies);
            }

            Assert.Equal(why == "muted" ? "True" : "False", await ScalarAsync(connection,
                "SELECT muted FROM config.config_alert_log WHERE metric_name = 'CPU Resolved'", ct));
            Assert.Equal("High CPU", await ScalarAsync(connection,
                "SELECT context_json::jsonb ->> 'Resolves' FROM config.config_alert_log WHERE metric_name = 'CPU Resolved'", ct));

            _now = DateTime.UtcNow;
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            Assert.Equal("resolved", await ScalarAsync(connection,
                $"SELECT event FROM config.investigation_journal WHERE request_id = '{id}' ORDER BY journal_id DESC LIMIT 1", ct));
        });
    }

    /// <summary>A metric that fires again after its clear (and joins the request) keeps the request open until it clears
    /// again: the clear must come after the metric's LAST alert, not any alert.</summary>
    [Fact]
    public async Task AMetricThatFiresAgainAfterItsClear_KeepsTheRequestOpen_UntilItClearsAgain()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var history = new PgAlertHistoryStore(_dataSource!, NullLogger.Instance);
            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            await history.RecordAlertAsync(Clear("High CPU", "CPU Resolved", T0.AddMinutes(3)));

            _now = T0.AddMinutes(5);
            Assert.Equal(InvestigationOpenOutcome.Joined, (await store.OpenOrJoinAsync(Alert(_now, "High CPU"), ct)).Outcome);
            _now = T0.AddMinutes(6);
            Assert.Equal(0, (await store.SweepAsync(ct)).Resolved);

            await history.RecordAlertAsync(Clear("High CPU", "CPU Resolved", T0.AddMinutes(7)));
            _now = T0.AddMinutes(8);
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'resolved' AND request_id = '{id}'", ct));
        });
    }

    /// <summary>#6003 review: an alert-log context a text column holds but jsonb refuses (a \u0000 escape) neither fails the
    /// sweep nor hides the clear it carries: the detector matches the member as text, in a transaction of its own.</summary>
    [Fact]
    public async Task AContextJsonbRefuses_StillResolves_AndTheExpiryStillCommits()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var history = new PgAlertHistoryStore(_dataSource!, NullLogger.Instance);
            var resolvedId = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            var expiringId = (await store.OpenOrJoinAsync(Alert(T0, "Blocking Detected") with { Family = "blocking", ExpiryMinutes = 5 }, ct))
                .RequestId!.Value;
            await history.RecordAlertAsync(Clear("High CPU", "CPU Resolved", T0.AddMinutes(3))
                with { ContextJson = "{\"Resolves\":\"High CPU\",\"note\":\"\\u0000\"}" });
            Assert.Equal("1", await ScalarAsync(connection,
                "SELECT count(*) FROM config.config_alert_log WHERE strpos(context_json, '\\u0000') > 0", ct));

            _now = T0.AddMinutes(6);
            var swept = await store.SweepAsync(ct);
            Assert.Equal((1, 1), (swept.Expired, swept.Resolved));
            Assert.Equal("expired|resolved", await ScalarAsync(connection,
                $"SELECT (SELECT state FROM config.investigation_requests WHERE request_id = '{expiringId}') || '|' || "
                + $"(SELECT event FROM config.investigation_journal WHERE request_id = '{resolvedId}' ORDER BY journal_id DESC LIMIT 1)", ct));
        });
    }

    /// <summary>#6003 review: a claim whose lease lapsed with no report leaves the request claimed, but no agent is on it, so
    /// its expiry is unclaimed and sends the fallback.</summary>
    [Fact]
    public async Task AClaimWhoseLeaseLapsed_ExpiresUnclaimed_AndFallsBack()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var sends = new FakeSends();
            var delivery = Delivery(store, sends, Route(1, teams: "https://teams.example/agent-route"));
            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            Assert.Equal(InvestigationClaimOutcome.Claimed, (await store.ClaimAsync(id, "agent-a", ct)).Outcome);

            _now = T0.AddMinutes(241);
            var swept = await store.SweepAsync(ct);
            Assert.Equal((1, 1), (swept.LeasesLapsed, swept.Expired));
            Assert.Equal("true", await ScalarAsync(connection,
                $"SELECT detail ->> 'unclaimed' FROM config.investigation_journal WHERE event = 'expired' AND request_id = '{id}'", ct));
            Assert.Equal(1, (await delivery.RunOnceAsync(ct)).Fallbacks);
            Assert.Equal((NotificationRouter.TeamsChannel, "https://teams.example/agent-route"), Assert.Single(Assert.Single(sends.Notices)));
        });
    }

    /// <summary>The delivery pass, with the service's real signal, wakes a long-poll parked on its cursor for an event the
    /// sweep journaled (the sweep itself wakes nobody), and the waiter answers that event alone.</summary>
    [Fact]
    public async Task ADeliveryPass_WakesAParkedLongPoll_ForAResolvedEvent()
    {
        await RunAsync(async (store, _, ct) =>
        {
            var signal = new InvestigationOpenSignal();
            var history = new PgAlertHistoryStore(_dataSource!, NullLogger.Instance);
            var delivery = new InvestigationEventDelivery(store, () => [Route(1)], new FakeSends(), signal, NullLogger.Instance, () => _now);
            Task<string> Wait(string? after, int seconds) =>
                DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, [1], after, seconds, ct);

            var start = (string)JsonNode.Parse(await Wait(null, 0))!["cursor"]!;
            _now = T0.AddSeconds(10);
            var id = (await store.OpenOrJoinAsync(Alert(_now, "High CPU"), ct)).RequestId!.Value;
            var cursor = (string)JsonNode.Parse(await Wait(start, 0))!["cursor"]!;

            var wait = Wait(cursor, 20);
            var until = DateTime.UtcNow.AddSeconds(10);
            while (signal.WaiterCount < 1 && DateTime.UtcNow < until)
            {
                await Task.Delay(10, ct);
            }

            Assert.True(signal.WaiterCount == 1, wait.IsCompleted ? await wait : "the long-poll did not park");
            await history.RecordAlertAsync(Clear("High CPU", "CPU Resolved", T0.AddMinutes(3)));
            _now = T0.AddMinutes(4);
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            await Task.Delay(300, ct);
            Assert.False(wait.IsCompleted);

            await delivery.RunOnceAsync(ct);
            var woke = JsonNode.Parse(await wait.WaitAsync(TimeSpan.FromSeconds(10), ct))!;
            var resolved = Assert.Single(woke["requests"]!.AsArray())!;
            Assert.Equal((InvestigationEvents.Resolved, id.ToString("D")), ((string)resolved["event"]!, (string)resolved["request_id"]!));
        });
    }

    /// <summary>Two events journaled at the same instant, the cursor taken after the first: the next call answers the second
    /// alone, and the call after that answers neither.</summary>
    [Fact]
    public async Task TwoEventsAtOneInstant_WithTheCursorTakenBetween_AreEachAnsweredOnce()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var signal = new InvestigationOpenSignal();
            Task<string> Wait(string? after) =>
                DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, [1], after, 0, ct);

            var start = (string)JsonNode.Parse(await Wait(null))!["cursor"]!;
            _now = T0.AddSeconds(10);
            var first = (await store.OpenOrJoinAsync(Alert(_now, "High CPU"), ct)).RequestId!.Value;
            var a = JsonNode.Parse(await Wait(start))!;
            Assert.Equal(first.ToString("D"), (string)Assert.Single(a["requests"]!.AsArray())!["request_id"]!);

            var second = (await store.OpenOrJoinAsync(Alert(_now, "Blocking Detected") with { Family = "blocking" }, ct)).RequestId!.Value;
            Assert.Equal("1", await ScalarAsync(connection,
                "SELECT count(DISTINCT event_time) FROM config.investigation_journal WHERE event = 'opened'", ct));
            var b = JsonNode.Parse(await Wait((string)a["cursor"]!))!;
            Assert.Equal(second.ToString("D"), (string)Assert.Single(b["requests"]!.AsArray())!["request_id"]!);

            var c = JsonNode.Parse(await Wait((string)b["cursor"]!))!;
            Assert.Empty(c["requests"]!.AsArray());
        });
    }

    /// <summary>
    /// #6003 review: the journal trigger (managed provisioning's <see cref="DarlingManagedRoles.BuildJournalEventRulesSql"/>)
    /// refuses, from a role holding the mcp role's journal grants, an event row, a delivery or fallback mark, a once key an
    /// event or a mark uses, and a row dated in the future, and lets in the role's own audit rows and a practice request's
    /// one opened row. The service's own writers (open, the sweep, the delivery and fallback marks) still write.
    /// </summary>
    [Fact]
    public async Task TheJournalTrigger_RefusesForgedEventsAndMarks_FromTheMcpRole_AndTheServiceStillWrites()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            var practiceId = Guid.NewGuid();
            await ExecAsync(connection,
                $"CREATE TEMP TABLE practice AS SELECT * FROM config.investigation_requests WHERE request_id = '{id}'; "
                + $"UPDATE practice SET request_id = '{practiceId}', practice_of = '{id}'; "
                + "INSERT INTO config.investigation_requests SELECT * FROM practice;", ct);
            await ExecAsync(connection, DarlingManagedRoles.BuildJournalEventRulesSql("config"), ct);

            var roleName = "inv_evt_" + Guid.NewGuid().ToString("N")[..8];
            var rolePassword = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
            await ExecAsync(connection,
                $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{rolePassword}'; GRANT USAGE ON SCHEMA config TO {roleName}; "
                + $"GRANT SELECT ON config.investigation_requests, config.investigation_journal TO {roleName}; "
                + $"GRANT INSERT ON config.investigation_journal TO {roleName};", ct);
            try
            {
                var asMcp = new NpgsqlConnectionStringBuilder(connection.ConnectionString) { Username = roleName, Password = rolePassword }.ConnectionString;
                await using var mcp = new NpgsqlConnection(asMcp);
                await mcp.OpenAsync(ct);
                string Row(string journalEvent, string? onceKey = null, string time = "now()", Guid? request = null) =>
                    "INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, once_key) "
                    + $"VALUES ({time}, '{request ?? id}', 1, {ServerId}, '{journalEvent}', 'agent:forger', "
                    + (onceKey is null ? "NULL)" : $"'{onceKey}')");

                foreach (var forged in new[]
                {
                    Row(InvestigationEvents.Resolved), Row(InvestigationEvents.Expired), Row(InvestigationEvents.Opened),
                    Row(InvestigationEvents.EvidenceReady), Row(InvestigationEvents.Delivered), Row(InvestigationEvents.FallbackSent),
                    Row("claimed", "delivered:" + Guid.NewGuid().ToString("D")), Row("claimed", InvestigationEvents.FallbackOnceKey(id)),
                    Row("claimed", "resolved:" + id.ToString("D")), Row("claimed", "expired:" + id.ToString("D")),
                    Row("claimed", "evidence_ready:" + id.ToString("D")), Row("claimed", time: "now() + interval '1 day'"),
                })
                {
                    Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(mcp, forged, ct))).SqlState);
                }

                await ExecAsync(mcp, Row("claimed"), ct);
                await ExecAsync(mcp, Row("refused", "refused:" + id.ToString("D")), ct);
                await ExecAsync(mcp, Row(InvestigationEvents.Opened, request: practiceId), ct);
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(
                    () => ExecAsync(mcp, Row(InvestigationEvents.Opened, request: practiceId), ct))).SqlState);
            }
            finally
            {
                await ExecAsync(connection, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", ct);
            }

            /* The service writes through the trigger: a join event, the resolved sweep, the delivery marks. Only the real
               events are delivered. */
            var sends = new FakeSends();
            _now = T0.AddMinutes(2);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, "Memory Pressure"), ct)).Evented);
            var history = new PgAlertHistoryStore(_dataSource!, NullLogger.Instance);
            await history.RecordAlertAsync(Clear("High CPU", "CPU Resolved", T0.AddMinutes(3)));
            await history.RecordAlertAsync(Clear("Memory Pressure", "Memory Pressure Resolved", T0.AddMinutes(3)));
            _now = T0.AddMinutes(4);
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            Assert.Equal(4, (await Delivery(store, sends, Route(1, generic: "https://agent.example/hook")).RunOnceAsync(ct)).Posted);
            Assert.Equal(
                new[] { InvestigationEvents.Opened, InvestigationEvents.Opened, InvestigationEvents.Joined, InvestigationEvents.Resolved },
                sends.Posts.Select(p => (string)JsonNode.Parse(p.Payload)!["event"]!));
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
            /* A request an agent still holds when it expires (its lease runs past the expiry) is not unclaimed: its expiry
               sends no fallback. */
            var claimedId = (await store.OpenOrJoinAsync(Alert(T0, "Blocking Detected") with { Family = "blocking" }, ct)).RequestId!.Value;
            _now = T0.AddMinutes(230);
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

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }
}
