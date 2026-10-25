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

        /// <summary>Runs while a post is in flight, before it answers: a gate that lets a test look at the world mid-send.</summary>
        public Func<CancellationToken, Task>? WhileSending { get; set; }

        public async Task<string?> PostEventAsync(int routeId, string url, string payload, string eventId, CancellationToken cancellationToken)
        {
            Posts.Add((routeId, url, eventId, payload));
            if (WhileSending is not null)
            {
                await WhileSending(cancellationToken);
            }

            return PostError;
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
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
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
    /// A follow-up opens now but copies its source's alert rows with their original times, so its last alert can be weeks
    /// old. A clear logged after that alert still resolves it: the read's bound on the alert log is the oldest last alert,
    /// not a fixed window from the clock. The rows are aged in place to the state a follow-up is in (opened now, alerts old).
    /// </summary>
    [Fact]
    public async Task Resolved_CountsAClearOlderThanThreeDays_ForARequestWhoseAlertsAreOlder()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            var alertAt = T0.AddDays(-10);
            await using (var age = new NpgsqlCommand(
                "UPDATE config.investigation_request_alerts SET alert_time = @t WHERE request_id = @id", connection))
            {
                age.Parameters.AddWithValue("t", alertAt);
                age.Parameters.AddWithValue("id", id);
                await age.ExecuteNonQueryAsync(ct);
            }

            await history.RecordAlertAsync(Clear("High CPU", "CPU Resolved", alertAt.AddDays(1)));
            _now = T0.AddMinutes(1);
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
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
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
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
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
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
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
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

    /// <summary>#6003 review round 2: the alert-log write scrubs a context and re-serializes it with an encoder that escapes
    /// &lt; &gt; &amp; ' + and every non-ASCII character (an emoji as a surrogate pair). The detector parses the stored
    /// text, so a metric name any of those characters is in still resolves its request.</summary>
    [Theory]
    [InlineData("CPU > 90%")]
    [InlineData("Latence élevée")]
    [InlineData("Hot \U0001F525 alert")]
    [InlineData("a+b & <c> 'd' \"e\"")]
    public async Task AClearWhoseContextWasRewrittenByTheScrub_StillResolves(string metric)
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
            var id = (await store.OpenOrJoinAsync(Alert(T0, metric), ct)).RequestId!.Value;
            var record = Clear(metric, metric + " Resolved", T0.AddMinutes(3));
            record = record with
            {
                ContextJson = DarlingSelfAlertEvaluator.ResolutionContextJson(
                    "{\"query_text\":\"" + StatementScrubCanary.CanaryStatement + "\"}", metric),
            };
            await history.RecordAlertAsync(record);

            /* The scrub did rewrite it, and the metric name reads back whole from the stored text. */
            Assert.Contains(SensitiveStatements.PlaceholderText, await ScalarAsync(connection,
                "SELECT context_json FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'", ct), StringComparison.Ordinal);
            Assert.Equal(metric, await ScalarAsync(connection,
                "SELECT context_json::jsonb ->> 'Resolves' FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'", ct));

            _now = T0.AddMinutes(4);
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'resolved' AND request_id = '{id}'", ct));
        });
    }

    /// <summary>A <c>Resolves</c> that is not the row's own top-level member (text inside another member's value, or a
    /// member of a nested object) clears nothing.</summary>
    [Theory]
    [InlineData("{\"Resolves\":\"Other Metric\",\"note\":\"\\\"Resolves\\\":\\\"High CPU\\\"\"}")]
    [InlineData("{\"Resolves\":\"Other Metric\",\"inner\":{\"Resolves\":\"High CPU\"}}")]
    [InlineData("{\"note\":\"High CPU\"}")]
    [InlineData("[\"Resolves\",\"High CPU\"]")]
    [InlineData("not json, Resolves High CPU")]
    public async Task AResolvesThatIsNotTheRowsOwnMember_ClearsNothing(string context)
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
            await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct);
            await history.RecordAlertAsync(Clear("Other Metric", "Other Resolved", T0.AddMinutes(3)) with { ContextJson = context });

            _now = T0.AddMinutes(4);
            Assert.Equal(0, (await store.SweepAsync(ct)).Resolved);
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
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
            /* A route with a URL, so the pass makes a post, and a post that stays in flight until the test has looked: the waiter
               must already be awake by then, which holds only if the pass wakes it before the first POST (#6003 review). */
            var sends = new FakeSends();
            var delivery = new InvestigationEventDelivery(store, () => [Route(1, generic: "https://agent.example/hook")], sends, signal,
                NullLogger.Instance, () => _now);
            Task<string> Wait(string? after, int seconds) =>
                DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, [1], after, seconds, ct);

            var start = (string)JsonNode.Parse(await Wait(null, 0))!["cursor"]!;
            _now = T0.AddSeconds(10);
            var id = (await store.OpenOrJoinAsync(Alert(_now, "High CPU"), ct)).RequestId!.Value;
            var cursor = (string)JsonNode.Parse(await Wait(start, 0))!["cursor"]!;

            /* #6048: WaiterCount says "registered", not "parked": the call registers BEFORE its first store read, so a sweep that
               commits in between is found by that read and the poll answers with no delivery pass at all (correct product
               behaviour: a waiter must not miss an event). Park means the first read has returned empty; the read is observed
               through a wrapper, and the poll then goes straight to waiting on the signal. */
            var firstReadDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            async Task<IReadOnlyList<InvestigationOpenedRow>> ObservedRead(
                DateTime since, IReadOnlyCollection<int>? routes, int limit, CancellationToken token)
            {
                var rows = await store.OpenedSinceAsync(since, routes, limit, token);
                firstReadDone.TrySetResult();
                return rows;
            }

            var wait = DarlingMcpInvestigationTools.WaitAsync(ObservedRead, signal, () => _now, [1], cursor,
                DarlingMcpInvestigationTools.MaxWaitSeconds, ct);
            await Task.WhenAny(firstReadDone.Task, Task.Delay(TimeSpan.FromSeconds(10), ct));
            Assert.True(firstReadDone.Task.IsCompleted && signal.WaiterCount == 1,
                wait.IsCompleted ? await wait : "the long-poll did not park");
            await history.RecordAlertAsync(Clear("High CPU", "CPU Resolved", T0.AddMinutes(3)));
            _now = T0.AddMinutes(4);
            Assert.Equal(1, (await store.SweepAsync(ct)).Resolved);
            await Task.Delay(300, ct);
            Assert.False(wait.IsCompleted);

            var wokeBeforeTheSendAnswered = false;
            sends.WhileSending = async token =>
            {
                try
                {
                    await wait.WaitAsync(TimeSpan.FromSeconds(5), token);
                    wokeBeforeTheSendAnswered = true;
                }
                catch (TimeoutException)
                {
                }
            };
            await delivery.RunOnceAsync(ct);
            Assert.NotEmpty(sends.Posts);
            Assert.True(wokeBeforeTheSendAnswered, "the long-poll was not woken before the first POST answered");
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
    /// refuses, from a role holding the mcp role's journal grants (the column list of
    /// <see cref="DarlingManagedRoles.JournalWriterColumns"/>), an event row, a delivery or fallback mark, any once key but the
    /// ones the tools write, and an insert that sets the identity or the event id; clamps a future date to the store's clock;
    /// and lets in the role's own audit rows and a practice request's one opened row (exact reason, once key set by the
    /// trigger). The service's own writers (open, the sweep, the delivery and fallback marks) still write.
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
                + $"GRANT INSERT ({DarlingManagedRoles.JournalWriterColumns}) ON config.investigation_journal TO {roleName};", ct);
            try
            {
                var asMcp = new NpgsqlConnectionStringBuilder(connection.ConnectionString) { Username = roleName, Password = rolePassword }.ConnectionString;
                await using var mcp = new NpgsqlConnection(asMcp);
                await mcp.OpenAsync(ct);
                await AssertForgeriesRefusedAsync(mcp, id, practiceId, ct);

                await ExecAsync(mcp, JournalRow(id, "claimed"), ct);
                await ExecAsync(mcp, JournalRow(id, "refused", onceKey: "refused:" + id.ToString("D")), ct);
                await ExecAsync(mcp, JournalRow(id, "journal_cap", onceKey: "journal_capped:" + id.ToString("D")), ct);
                await ExecAsync(mcp, JournalRow(id, "refused", onceKey: "agent_refused:" + id.ToString("D") + ":tool"), ct);
                await ExecAsync(mcp, JournalRow(id, "link_recorded", onceKey: "link:" + id.ToString("D") + ":abc"), ct);

                /* A future date is clamped to the store's clock, not refused: a fast clock cannot move a cursor ahead. */
                await ExecAsync(mcp, JournalRow(id, "claimed", time: "now() + interval '1 day'"), ct);
                Assert.Equal("True", await ScalarAsync(connection,
                    "SELECT bool_and(event_time <= now()) FROM config.investigation_journal WHERE actor = 'agent:forger'", ct));

                /* The practice request's one opened row: the exact reason, once. The trigger sets its once key. */
                await ExecAsync(mcp, JournalRow(practiceId, InvestigationEvents.Opened, reason: "practice of " + id.ToString("D")), ct);
                Assert.Equal("practice_opened:" + practiceId.ToString("D"), await ScalarAsync(connection,
                    $"SELECT once_key FROM config.investigation_journal WHERE request_id = '{practiceId}' AND event = 'opened'", ct));
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(mcp,
                    JournalRow(practiceId, InvestigationEvents.Opened, reason: "practice of " + id.ToString("D")), ct))).SqlState);
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
            var history = new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
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

    private static string JournalRow(Guid request, string journalEvent, string? onceKey = null, string time = "now()", string? reason = null,
        string columns = "event_time, request_id, route_id, server_id, event, actor, reason, once_key", string? idValues = null) =>
        $"INSERT INTO config.investigation_journal ({columns}) {(idValues is null ? "" : "OVERRIDING SYSTEM VALUE ")}"
        + $"VALUES ({idValues}{time}, '{request}', 1, {ServerId}, '{journalEvent}', 'agent:forger', "
        + (reason is null ? "NULL, " : $"'{reason}', ")
        + (onceKey is null ? "NULL)" : $"'{onceKey}')");

    /// <summary>Every insert a non-owner journal writer must not make, each refused with 42501.</summary>
    private static async Task AssertForgeriesRefusedAsync(NpgsqlConnection writer, Guid id, Guid practiceId, CancellationToken ct)
    {
        var forged = new List<string>();
        foreach (var kind in InvestigationEvents.Kinds.Append(InvestigationEvents.Delivered).Append(InvestigationEvents.FallbackSent))
        {
            forged.Add(JournalRow(id, kind));
        }

        /* Who changed a server action switch is the owner's alone (#6269): the managed trigger and the script's copy both refuse it from any other login. */
        forged.Add(JournalRow(id, ServerActionSwitches.JournalEvent));

        /* The three rows of an agent action are the owner's alone (#6569): the Agent changes list offers Undo on action_called, so neither mcp nor admin
           (the writer role here holds the column list both hold) may write one. */
        foreach (var kind in AgentActionEvents.OwnerOnly)
        {
            forged.Add(JournalRow(id, kind));
        }

        /* A practice request's opened row with a reason that is not the exact sentence. */
        forged.Add(JournalRow(practiceId, InvestigationEvents.Opened, reason: "because I said so"));
        forged.Add(JournalRow(practiceId, InvestigationEvents.Opened, reason: "practice of " + Guid.NewGuid().ToString("D")));
        /* A once key the service uses, and any other prefix the tools do not write. */
        foreach (var prefix in new[] { "delivered", "fallback", "resolved", "expired", "evidence_ready", "lease_expired", "limit_reached", "practice_opened", "anything" })
        {
            forged.Add(JournalRow(id, "claimed", onceKey: prefix + ":" + id.ToString("D")));
        }

        /* The identity and the event id are the service's: naming either is a column the role holds no INSERT on. */
        forged.Add(JournalRow(id, "claimed", columns: "journal_id, event_time, request_id, route_id, server_id, event, actor, reason, once_key", idValues: "9000000000, "));
        forged.Add(JournalRow(id, "claimed", columns: "event_id, event_time, request_id, route_id, server_id, event, actor, reason, once_key", idValues: $"'{Guid.NewGuid()}', "));

        foreach (var sql in forged)
        {
            var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(writer, sql, ct));
            Assert.True(refused.SqlState == "42501", sql + " -> " + refused.SqlState);
        }
    }

    /// <summary>A practice request's event carries <c>practice_of</c> in its detail, on the delivery read and the long-poll read, so
    /// a receiver can tell a replay from a live request; a live request's event does not.</summary>
    [Fact]
    public async Task APracticeRequestsEvent_CarriesPracticeOf_AndALiveRequestsDoesNot()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            var practiceId = Guid.NewGuid();
            await ExecAsync(connection,
                $"CREATE TEMP TABLE practice AS SELECT * FROM config.investigation_requests WHERE request_id = '{id}'; "
                + $"UPDATE practice SET request_id = '{practiceId}', practice_of = '{id}'; "
                + "INSERT INTO config.investigation_requests SELECT * FROM practice; "
                + $"INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason) "
                + $"VALUES ('{T0:O}', '{practiceId}', 1, {ServerId}, 'opened', 'person:viewer', 'practice of {id}')", ct);

            var pending = await store.PendingDeliveriesAsync(T0.AddMinutes(-1), 100, ct);
            var polled = await store.OpenedSinceAsync(T0.AddMinutes(-1), null, 100, ct);
            Assert.Equal(id.ToString("D"), (string)JsonNode.Parse(pending.Single(p => p.RequestId == practiceId).DetailJson!)!["practice_of"]!);
            Assert.Equal(id.ToString("D"), (string)JsonNode.Parse(polled.Single(p => p.RequestId == practiceId).DetailJson!)!["practice_of"]!);
            Assert.DoesNotContain("practice_of", pending.Single(p => p.RequestId == id).DetailJson ?? "", StringComparison.Ordinal);
            Assert.DoesNotContain("practice_of", polled.Single(p => p.RequestId == id).DetailJson ?? "", StringComparison.Ordinal);
        });
    }

    /// <summary>
    /// The alert links (managed provisioning's <see cref="DarlingManagedRoles.BuildRequestAlertLinkRulesSql"/>): a role holding
    /// admin's INSERT on them is refused a link to a live request and takes one to a practice request.
    /// </summary>
    [Fact]
    public async Task TheAlertLinkTrigger_RefusesALinkToALiveRequest_FromAdmin_AndTakesAPracticeRequestsCopy()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            var practiceId = Guid.NewGuid();
            await ExecAsync(connection,
                $"CREATE TEMP TABLE practice AS SELECT * FROM config.investigation_requests WHERE request_id = '{id}'; "
                + $"UPDATE practice SET request_id = '{practiceId}', practice_of = '{id}'; "
                + "INSERT INTO config.investigation_requests SELECT * FROM practice;", ct);
            await ExecAsync(connection, DarlingManagedRoles.BuildRequestAlertLinkRulesSql("config"), ct);

            var roleName = "inv_lnk_" + Guid.NewGuid().ToString("N")[..8];
            var rolePassword = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
            await ExecAsync(connection,
                $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{rolePassword}'; GRANT USAGE ON SCHEMA config TO {roleName}; "
                + $"GRANT SELECT ON config.investigation_requests, config.investigation_request_alerts TO {roleName}; "
                + $"GRANT INSERT ON config.investigation_request_alerts TO {roleName};", ct);
            try
            {
                var asAdmin = new NpgsqlConnectionStringBuilder(connection.ConnectionString) { Username = roleName, Password = rolePassword }.ConnectionString;
                await using var admin = new NpgsqlConnection(asAdmin);
                await admin.OpenAsync(ct);
                string Link(Guid request) =>
                    "INSERT INTO config.investigation_request_alerts (request_id, alert_time, server_id, metric_name) "
                    + $"VALUES ('{request}', now(), {ServerId}, 'Memory Pressure')";

                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(admin, Link(id), ct))).SqlState);
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(admin, Link(Guid.NewGuid()), ct))).SqlState);
                await ExecAsync(admin, Link(practiceId), ct);

                Assert.Equal("1|0", await ScalarAsync(connection,
                    $"SELECT (SELECT count(*) FROM config.investigation_request_alerts WHERE request_id = '{practiceId}' AND metric_name = 'Memory Pressure') || '|' || "
                    + $"(SELECT count(*) FROM config.investigation_request_alerts WHERE request_id = '{id}' AND metric_name = 'Memory Pressure')", ct));
            }
            finally
            {
                await ExecAsync(connection, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", ct);
            }
        });
    }

    /// <summary>
    /// F: the self-managed script's own text (provision-roles.sql step 3i, run here as its DO block) refuses every event kind
    /// and both marks from a role with the script's column grant, so a kind added to <see cref="InvestigationEvents.Kinds"/>
    /// without the script's list is caught here rather than forgeable on a self-managed store.
    /// </summary>
    [Fact]
    public async Task TheScriptsJournalTrigger_RefusesEveryEventKindAndMark_FromTheMcpRole()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var id = (await store.OpenOrJoinAsync(Alert(T0, "High CPU"), ct)).RequestId!.Value;
            var practiceId = Guid.NewGuid();
            await ExecAsync(connection,
                $"CREATE TEMP TABLE practice AS SELECT * FROM config.investigation_requests WHERE request_id = '{id}'; "
                + $"UPDATE practice SET request_id = '{practiceId}', practice_of = '{id}'; "
                + "INSERT INTO config.investigation_requests SELECT * FROM practice;", ct);
            var script = System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql"));
            var start = script.IndexOf("-- 3i (continued)", StringComparison.Ordinal);
            var end = script.IndexOf("-- 3j.", StringComparison.Ordinal);
            Assert.True(start > 0 && end > start, "the script's journal rules block was not found");
            await ExecAsync(connection, script[start..end], ct);
            Assert.Equal("2", await ScalarAsync(connection,
                "SELECT count(*) FROM pg_trigger WHERE tgname IN ('trg_investigation_journal_owner_rows', 'trg_investigation_request_alerts_owner_rows')", ct));

            var roleName = "inv_scr_" + Guid.NewGuid().ToString("N")[..8];
            var rolePassword = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
            await ExecAsync(connection,
                $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{rolePassword}'; GRANT USAGE ON SCHEMA config TO {roleName}; "
                + $"GRANT SELECT ON config.investigation_requests, config.investigation_journal TO {roleName}; "
                + $"GRANT INSERT ({DarlingManagedRoles.JournalWriterColumns}) ON config.investigation_journal TO {roleName};", ct);
            try
            {
                var asMcp = new NpgsqlConnectionStringBuilder(connection.ConnectionString) { Username = roleName, Password = rolePassword }.ConnectionString;
                await using var mcp = new NpgsqlConnection(asMcp);
                await mcp.OpenAsync(ct);
                await AssertForgeriesRefusedAsync(mcp, id, practiceId, ct);
                await ExecAsync(mcp, JournalRow(id, "claimed"), ct);
                await ExecAsync(mcp, JournalRow(practiceId, InvestigationEvents.Opened, reason: "practice of " + id.ToString("D")), ct);
            }
            finally
            {
                await ExecAsync(connection, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", ct);
            }
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
            var wait = Wait(cursor, DarlingMcpInvestigationTools.MaxWaitSeconds);
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
                new CustomAlertRuleStore(_dataSource!, StorageCommandDeadlines.McpReadSeconds), new CustomAlertStateStore(_dataSource!, StorageCommandDeadlines.McpReadSeconds), _dataSource!, deliverer,
                isAlertMuted: _ => false, alertsEnabled: static () => true, new PgAlertHistoryStore(_dataSource!, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance),
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
