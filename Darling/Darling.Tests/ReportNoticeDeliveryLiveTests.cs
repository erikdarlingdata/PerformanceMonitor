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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The report notice on a live store (#6979): a report made in the last 30 minutes on a route that asks for a notice goes once to the
/// route's Slack and Teams destinations (the settings' channel when the route has none), the mark is written before the send and a
/// failed send is not retried, <c>needs_person_or_change</c> takes a <c>needs_person</c> verdict or a request with a change and
/// <c>all</c> takes every report, the change list and the masking follow the request, and an older report sends nothing.
/// </summary>
public sealed class ReportNoticeDeliveryLiveTests
{
    private const string RouteSlack = "https://hooks.example/route-slack";
    private const string ParentSlack = "https://hooks.example/parent-slack";

    private static readonly DateTime T0 = new(DateTime.UtcNow.AddHours(-1).Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond,
        DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private DateTime _now = T0;

    private sealed class FakeNotices : IReportNoticeSends
    {
        public List<(string Channel, string Destination, ReportNoticeContent Content)> Sent { get; } = [];

        public string? Error { get; set; }

        public Task<string?> TrySendReportNoticeAsync(string channel, string destination, ReportNoticeContent content, CancellationToken cancellationToken = default)
        {
            Sent.Add((channel, destination, content));
            return Task.FromResult(Error);
        }
    }

    /// <summary>The agent-route sends, which these facts do not look at.</summary>
    private sealed class QuietSends : IInvestigationEventSends
    {
        public Task<string?> PostEventAsync(int routeId, string url, string payload, string eventId, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);

        public Task<IReadOnlyList<string>> SendNoticeAsync(IReadOnlyList<(string Channel, string Destination)> targets, string title,
            string serverName, string currentValue, string detail, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<string>>([]);

        public Task<bool> RaiseSelfAlertAsync(Guid requestId, string serverName, int serverId, string shortMessage, string detail, CancellationToken cancellationToken) =>
            Task.FromResult(true);
    }

    private sealed class Settings(string slackUrl = "", string teamsUrl = "") : IAlertSettings
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
        public bool TeamsWebhookEnabled => teamsUrl.Length > 0;
        public string TeamsWebhookUrl => teamsUrl;
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => slackUrl.Length > 0;
        public string SlackWebhookUrl => slackUrl;
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

    private static NotificationRoute Route(int routeId, string slack = "", string teams = "", bool enabled = true, string[]? off = null) =>
        new(routeId, "High CPU", teams, slack, "", "", "", Enabled: enabled)
        {
            AgentLevel = NotificationRoute.AgentLevelInvestigate, AgentMaxPerHour = 50, AgentMaxPerDay = 500, OffChannels = off,
        };

    private InvestigationEventDelivery Delivery(InvestigationRequestStore store, FakeNotices notices, IReadOnlyList<NotificationRoute> routes,
        IAlertSettings? settings = null, McpDataDetail dataDetail = McpDataDetail.Full) =>
        new(store, () => routes, new QuietSends(), null, NullLogger.Instance, () => _now,
            notices, settings ?? new Settings(), dataDetail, () => "https://dash.example");

    private static InvestigationAlert Alert(DateTime at, int routeId, int serverId) =>
        new(routeId, serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), "resources", "High CPU", at, Array.Empty<string>(), null, 50, 500,
            LeaseMinutes: 15, ExpiryMinutes: 240);

    /// <summary>Opens, claims and reports a request at the clock's time, on <paramref name="routeId"/> and its own server, then moves the
    /// clock a minute on (the pass runs a minute after the report).</summary>
    private async Task<Guid> ReportedAsync(InvestigationRequestStore store, NpgsqlConnection connection, int routeId, string setting,
        string verdict, string rootCause, CancellationToken ct)
    {
        await ExecAsync(connection,
            $"INSERT INTO config.config_notification_routes (route_id, metric_match) OVERRIDING SYSTEM VALUE VALUES ({routeId}, 'High CPU') ON CONFLICT DO NOTHING; "
            + $"UPDATE config.config_notification_routes SET report_notice = '{setting}' WHERE route_id = {routeId};", ct);
        var opened = await store.OpenOrJoinAsync(Alert(_now, routeId, 100 + routeId), ct);
        var request = opened.RequestId!.Value;
        var claim = await store.ClaimAsync(request, "agent-a", ct);
        await store.SubmitAsync(new InvestigationReportSubmission(request, claim.ClaimId!.Value, "agent-a", 1, verdict, rootCause, verdict != "needs_person", "summary",
            "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"1\"}]"), ct);
        _now = _now.AddMinutes(1);
        return request;
    }

    /// <summary>Journals one agent change the way the gate does (the call, then its result), as the store owner.</summary>
    private static async Task AddChangeAsync(NpgsqlConnection connection, Guid request, int routeId, string tool, string targetId, string? database, string result, CancellationToken ct)
    {
        var target = database is null ? $"{{\"id\":\"{targetId}\"}}" : $"{{\"id\":\"{targetId}\",\"database\":\"{database}\"}}";
        var detail = $"{{\"tool\":\"{tool}\",\"kind\":\"mute\",\"target\":{target}}}".Replace("'", "''", StringComparison.Ordinal);
        var called = await ScalarAsync(connection,
            "INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail) "
            + $"VALUES (now(), '{request}', {routeId}, {100 + routeId}, 'action_called', 'agent:agent-a', 'why', '{detail}'::jsonb) RETURNING journal_id", ct);
        if (result != "unknown")
        {
            await ExecAsync(connection,
                "INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, detail) "
                + $"VALUES (now(), '{request}', {routeId}, {100 + routeId}, '{result}', 'agent:agent-a', '{{\"called_id\":{called}}}'::jsonb)", ct);
        }
    }

    [Fact]
    public async Task ANeedsPersonReport_SendsOneNotice_AndASecondPassSendsNone()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var request = await ReportedAsync(store, connection, 1, "needs_person_or_change", "needs_person", "A person must look.\nSecond line.", ct);
            var notices = new FakeNotices();
            var delivery = Delivery(store, notices, [Route(1, slack: RouteSlack)]);

            Assert.Equal(1, (await delivery.RunOnceAsync(ct)).Notices);
            var (channel, destination, content) = Assert.Single(notices.Sent);
            Assert.Equal("Slack", channel);
            Assert.Equal(RouteSlack, destination);
            Assert.Equal(request, content.RequestId);
            Assert.Equal("needs_person", content.Verdict);
            Assert.Equal("srv-101", content.ServerName);
            Assert.Equal("High CPU", content.Metric);
            Assert.Equal("A person must look.", content.RootCauseFirstLine);
            Assert.Empty(content.Changes);
            Assert.Equal("https://dash.example", content.BaseUrl);

            _now = _now.AddMinutes(1);
            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Notices);
            Assert.Single(notices.Sent);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'report_notice_sent' AND once_key = 'report_notice:{request}'", ct));
        });
    }

    [Fact]
    public async Task ARealReportWithNoChange_SendsNothingUnderNeedsPersonOrChange_AndOneUnderAll_AndNothingWhenOff()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ReportedAsync(store, connection, 1, "needs_person_or_change", "real", "cause", ct);
            await ReportedAsync(store, connection, 2, "all", "real", "cause", ct);
            await ReportedAsync(store, connection, 3, "off", "needs_person", "cause", ct);
            var notices = new FakeNotices();
            var routes = new[] { Route(1, slack: "https://hooks.example/one"), Route(2, slack: "https://hooks.example/two"), Route(3, slack: "https://hooks.example/three") };

            await Delivery(store, notices, routes).RunOnceAsync(ct);

            var sent = Assert.Single(notices.Sent);
            Assert.Equal("https://hooks.example/two", sent.Destination);
        });
    }

    [Fact]
    public async Task AReportWithOneChange_CarriesTheChange_AndQualifiesUnderNeedsPersonOrChange()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var request = await ReportedAsync(store, connection, 1, "needs_person_or_change", "real", "cause", ct);
            await AddChangeAsync(connection, request, 1, "create_mute_rule", "dbo.MyProc", "Sales", "action_done", ct);
            await AddChangeAsync(connection, request, 1, "set_threshold_override", "77", null, "action_failed", ct);
            await AddChangeAsync(connection, request, 1, "force_plan", "9", "Sales", "unknown", ct);
            var notices = new FakeNotices();

            await Delivery(store, notices, [Route(1, slack: RouteSlack)]).RunOnceAsync(ct);

            var content = Assert.Single(notices.Sent).Content;
            Assert.Equal(
                [("create_mute_rule", "dbo.MyProc in Sales", "done"), ("set_threshold_override", "77", "failed"), ("force_plan", "9 in Sales", "result unknown")],
                content.Changes.Select(c => (c.Tool, c.Object, c.Result)).ToArray());
        });
    }

    [Fact]
    public async Task AReportOlderThanThirtyMinutes_SendsNothing_WhenItsRouteIsSwitchedOnLater()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ReportedAsync(store, connection, 1, "off", "needs_person", "cause", ct);
            await ExecAsync(connection, "UPDATE config.config_notification_routes SET report_notice = 'all' WHERE route_id = 1", ct);
            _now = _now.AddMinutes(31);
            var notices = new FakeNotices();

            Assert.Equal(0, (await Delivery(store, notices, [Route(1, slack: RouteSlack)]).RunOnceAsync(ct)).Notices);
            Assert.Empty(notices.Sent);

            /* The same report, inside the window, is sent: the window is the only thing that held it back. */
            _now = _now.AddMinutes(-20);
            Assert.Equal(1, (await Delivery(store, notices, [Route(1, slack: RouteSlack)]).RunOnceAsync(ct)).Notices);
        });
    }

    [Fact]
    public async Task ARouteWithNoSlackUrl_SendsToTheParentSlackChannel_AndBothChannelsWhenBothAreSet()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ReportedAsync(store, connection, 1, "all", "real", "cause", ct);
            var notices = new FakeNotices();

            await Delivery(store, notices, [Route(1, teams: "https://hooks.example/route-teams")], new Settings(slackUrl: ParentSlack, teamsUrl: "https://hooks.example/parent-teams")).RunOnceAsync(ct);

            Assert.Equal(
                [("Teams", "https://hooks.example/route-teams"), ("Slack", ParentSlack)],
                notices.Sent.Select(s => (s.Channel, s.Destination)).ToArray());
        });
    }

    [Theory]
    [InlineData("disabled")]
    [InlineData("slack_off")]
    [InlineData("route_gone")]
    [InlineData("nowhere")]
    public async Task ADisabledRoute_AChannelTurnedOff_AGoneRoute_OrNoDestination_SendsNothing(string why)
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ReportedAsync(store, connection, 1, "all", "needs_person", "cause", ct);
            var notices = new FakeNotices();
            IReadOnlyList<NotificationRoute> routes = why switch
            {
                "disabled" => [Route(1, slack: RouteSlack, enabled: false)],
                "slack_off" => [Route(1, slack: RouteSlack, off: ["Slack"])],
                "route_gone" => [],
                _ => [Route(1)],
            };

            /* The slack_off route also has a parent Slack channel: IsOffFor keeps the notice off it (an alert would not go there either). */
            var settings = why is "slack_off" ? new Settings(slackUrl: ParentSlack) : new Settings();
            Assert.Equal(0, (await Delivery(store, notices, routes, settings).RunOnceAsync(ct)).Notices);
            Assert.Empty(notices.Sent);
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'report_notice_sent'", ct));
        });
    }

    [Theory]
    [InlineData(McpDataDetail.Full, "Blocked by 'alice' on 42 rows")]
    [InlineData(McpDataDetail.MaskLiterals, "Blocked by '?' on ? rows")]
    [InlineData(McpDataDetail.MetadataOnly, null)]
    public async Task TheRootCauseAndTheChangeObjects_FollowTheDataDetail(McpDataDetail mode, string? expectedLine)
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var request = await ReportedAsync(store, connection, 1, "all", "real", "Blocked by 'alice' on 42 rows\nsecond line 'bob'", ct);
            await AddChangeAsync(connection, request, 1, "create_plan_guide", "guide 'x' 5", "Sales", "action_done", ct);
            var notices = new FakeNotices();

            await Delivery(store, notices, [Route(1, slack: RouteSlack)], dataDetail: mode).RunOnceAsync(ct);

            var content = Assert.Single(notices.Sent).Content;
            Assert.Equal(expectedLine, content.RootCauseFirstLine);
            var maskedObject = Assert.Single(content.Changes).Object;
            Assert.Equal(mode == McpDataDetail.Full ? "guide 'x' 5 in Sales" : "guide '?' ? in Sales", maskedObject);
            if (mode != McpDataDetail.Full)
            {
                Assert.DoesNotContain("alice", System.Text.Json.JsonSerializer.Serialize(content));
            }
        });
    }

    [Fact]
    public async Task AFailedSend_IsNotRetried_AndTheMarkStaysWritten()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var request = await ReportedAsync(store, connection, 1, "all", "real", "cause", ct);
            var notices = new FakeNotices { Error = "webhook answered 500" };
            var delivery = Delivery(store, notices, [Route(1, slack: RouteSlack)]);

            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Notices);
            Assert.Single(notices.Sent);

            notices.Error = null;
            _now = _now.AddMinutes(1);
            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Notices);
            Assert.Single(notices.Sent);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE once_key = 'report_notice:{request}'", ct));
        });
    }

    [Fact]
    public async Task APassThatLosesTheMarkRace_SendsNothing()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var request = await ReportedAsync(store, connection, 1, "all", "real", "cause", ct);
            await ExecAsync(connection,
                "INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, once_key) "
                + $"VALUES (now(), '{request}', 1, 101, 'report_notice_sent', 'service', 'report_notice:{request}')", ct);
            var notices = new FakeNotices();

            Assert.Equal(0, (await Delivery(store, notices, [Route(1, slack: RouteSlack)]).RunOnceAsync(ct)).Notices);
            Assert.Empty(notices.Sent);
        });
    }

    [Theory]
    [InlineData("42", null, "42")]
    [InlineData("dbo.Proc1", "Sales2024", "dbo.Proc1 in Sales2024")]
    [InlineData("name 'a' 7", null, "name '?' ?")]
    [InlineData("0x1F", "Sales", "? in Sales")]
    public void AChangeObject_KeepsAnAssignedIdAndNames_AndMasksLiteralsBelowFull(string id, string? database, string expected)
    {
        Assert.Equal(expected, InvestigationEventDelivery.ChangeObject(id, database, McpDataDetail.MaskLiterals));
        Assert.Equal(expected, InvestigationEventDelivery.ChangeObject(id, database, McpDataDetail.MetadataOnly));
        Assert.Equal(database is null ? id : id + " in " + database, InvestigationEventDelivery.ChangeObject(id, database, McpDataDetail.Full));
    }

    [Fact]
    public void TheRootCauseFirstLine_IsTheFirstNonBlankLine_AndAQuotedLiteralOverALineBreakIsMaskedWhole()
    {
        Assert.Equal("first", InvestigationEventDelivery.RootCauseFirstLine("\r\n  first  \nsecond", McpDataDetail.Full));
        Assert.Null(InvestigationEventDelivery.RootCauseFirstLine("   ", McpDataDetail.Full));
        var masked = InvestigationEventDelivery.RootCauseFirstLine("Query WHERE name = 'ali\nce' ran", McpDataDetail.MaskLiterals);
        Assert.DoesNotContain("ali", masked);
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the report notice live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
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
