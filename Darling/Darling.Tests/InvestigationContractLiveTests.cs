/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests.AgentContract;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The investigation contract end to end (#5632 Phase A, L8): an alert or a finding goes through the real deliverer or
/// finding sender, the dispatcher opens a request on the live store, and <see cref="FakeInvestigationAgent"/> drives the
/// REAL MCP host (<see cref="StatementFilterCensus.BuildLiveHostAsync"/>: the live store, <c>mcp.allowInvestigations</c>
/// on, <c>mcp.allowWrites</c> off, a bearer token) to list, claim, read every next call and report. The journal holds
/// opened, claimed, reported in that order, and the viewer partial reads the report. Plus the edges: a muted and a
/// recovery alert open nothing, the 21st request in an hour is refused with <c>limit_reached</c> journaled once, the
/// sweep expires an unclaimed request, and a claim after that answers <c>not_open</c>.
/// </summary>
public sealed class InvestigationContractLiveTests
{
    private const int ServerId = 42;

    /* A string literal and a numeric literal planted in the alert's detail: the brief, composed under maskLiterals,
       must hold neither. */
    private const string LiteralCanary = "zq-lit-5632";
    private const string NumberCanary = "731900417";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private TimeSpan _shift = TimeSpan.Zero;

    private sealed record Harness(
        DarlingAlertDeliverer Deliverer, DarlingFindingAlertSender Sender, InvestigationRequestStore Store,
        NpgsqlConnection Connection, NpgsqlDataSource Source, string StoreString, TestServer Host);

    private static NotificationRoute AgentRoute(int routeId, string match) =>
        new(routeId, match, "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelInvestigate };

    private static AlertOutcome Outcome(string metric, int serverId = ServerId, bool muted = false, string detail = "detail") =>
        new(serverId.ToString(CultureInfo.InvariantCulture), serverId == ServerId ? "srv" : "srv-" + serverId.ToString(CultureInfo.InvariantCulture), metric,
            "3", "1", null, detail, NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: muted, Severity: null);

    [Fact]
    public async Task AThresholdAlert_ReachesTheAgent_ThroughTheRealHost_AndTheViewerReadsItsReport()
    {
        await RunAsync(McpDataDetail.MaskLiterals, async (h, ct) =>
        {
            /* The token gates the host: a call without it is refused before any tool runs. */
            Assert.Equal(401, await StatementFilterCensus.CallStatusAsync(h.Host, "list_investigation_requests", bearerToken: null));

            await h.Deliverer.DeliverAsync(Outcome("Blocking Detected",
                detail: $"Head blocker ran SELECT a FROM dbo.t WHERE b = '{LiteralCanary}' AND c = {NumberCanary}"), ct);

            var agent = new FakeInvestigationAgent(h.Host, StatementFilterCensus.LiveHostToken, "contract-agent");
            string requestId = await ListOneAsync(agent);

            /* The canaries are in the brief's source: composed at full, the brief holds them. */
            string fullBrief = (await new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(h.Source),
                new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools).ComposeAsync(Guid.Parse(requestId), ct))!;
            Assert.Contains(LiteralCanary, fullBrief, StringComparison.Ordinal);
            Assert.Contains(NumberCanary, fullBrief, StringComparison.Ordinal);

            /* The agent copies a statement into its inferred evidence item: it is free text the report keeps as submitted. */
            var run = await agent.InvestigateAsync(requestId,
                $"Head blocker ran SELECT a FROM dbo.t WHERE b = '{LiteralCanary}' AND c = {NumberCanary}");

            await AssertTheRunAsync(h, agent, run, requestId, ct);
            /* #6004: the report is served through the real host at maskLiterals, so its evidence lists hold neither canary. */
            var served = await agent.CallAsync("get_investigation_reports",
                new System.Text.Json.Nodes.JsonObject { ["request_id"] = requestId });
            Assert.Equal(1, served.GetProperty("reports")[0].GetProperty("evidence_inferred").GetArrayLength());
            Assert.DoesNotContain(LiteralCanary, served.GetRawText(), StringComparison.Ordinal);
            Assert.DoesNotContain(NumberCanary, served.GetRawText(), StringComparison.Ordinal);
            using (var brief = JsonDocument.Parse(run.BriefJson))
            {
                Assert.Equal("maskLiterals", brief.RootElement.GetProperty("data_detail").GetString());
            }

            Assert.DoesNotContain(LiteralCanary, run.BriefJson, StringComparison.Ordinal);
            Assert.DoesNotContain(NumberCanary, run.BriefJson, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AnAnalysisFinding_ReachesTheAgent_ThroughTheSamePath()
    {
        await RunAsync(McpDataDetail.Full, async (h, ct) =>
        {
            await h.Sender.SendFindingAlertAsync(InvestigationDispatcherTests.Finding(
                "Analysis: blocking [bbbb2222]", FindingRoute.Page, ServerId.ToString(CultureInfo.InvariantCulture),
                hash: "bbbb2222cccc3333dddd4444"));

            var agent = new FakeInvestigationAgent(h.Host, StatementFilterCensus.LiveHostToken, "contract-agent");
            string requestId = await ListOneAsync(agent);
            var run = await agent.InvestigateAsync(requestId);

            await AssertTheRunAsync(h, agent, run, requestId, ct);
            using var brief = JsonDocument.Parse(run.BriefJson);
            Assert.Equal("bbbb2222cccc3333dddd4444", brief.RootElement.GetProperty("request").GetProperty("story_path_hash").GetString());
        });
    }

    [Fact]
    public async Task AMutedAlert_AndARecovery_OpenNothing()
    {
        await RunAsync(McpDataDetail.Full, async (h, ct) =>
        {
            await h.Deliverer.DeliverAsync(Outcome("Blocking Detected", muted: true), ct);
            await h.Deliverer.DeliverAsync(Outcome("Server Restored"), ct);

            Assert.Equal("2|0", await ScalarAsync(h.Connection,
                "SELECT (SELECT count(*) FROM config_alert_log) || '|' || (SELECT count(*) FROM config.investigation_requests)", ct));
            var list = await new FakeInvestigationAgent(h.Host, StatementFilterCensus.LiveHostToken, "contract-agent").ListAsync(state: null);
            Assert.Equal("empty", list.GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task TheTwentyFirstRequestInAnHour_IsRefused_WithLimitReachedJournaledOnce()
    {
        await RunAsync(McpDataDetail.Full, async (h, ct) =>
        {
            /* One server per alert, so each opens its own request on the one route (a join would not count). */
            for (int server = 1; server <= 22; server++)
            {
                await h.Deliverer.DeliverAsync(Outcome("Blocking Detected", serverId: 100 + server), ct);
            }

            Assert.Equal("20|1", await ScalarAsync(h.Connection,
                "SELECT (SELECT count(*) FROM config.investigation_requests) || '|' || " +
                "(SELECT count(*) FROM config.investigation_journal WHERE event = 'limit_reached')", ct));
        });
    }

    /// <summary>#5632 acceptance test: a route at its hourly limit opens no new request, and the alert still goes to its
    /// other channels. The agent route carries its own generic webhook and inherits Slack from the settings row; 22 alerts
    /// on 22 servers (the 21st and 22nd are past the limit of 20) each post to both, and the history row is written
    /// for every one of them.</summary>
    [Fact]
    public async Task AnAlertPastTheHourlyLimit_OpensNoRequest_AndStillReachesTheRoutesOtherChannels()
    {
        using var slack = new CapturingWebhookEndpoint();
        using var generic = new CapturingWebhookEndpoint();
        await RunAsync(McpDataDetail.Full, async (h, ct) =>
        {
            for (int server = 1; server <= 22; server++)
            {
                await h.Deliverer.DeliverAsync(Outcome("Blocking Detected", serverId: 100 + server), ct);
            }

            Assert.Equal("20|1|22", await ScalarAsync(h.Connection,
                "SELECT (SELECT count(*) FROM config.investigation_requests) || '|' || " +
                "(SELECT count(*) FROM config.investigation_journal WHERE event = 'limit_reached') || '|' || " +
                "(SELECT count(*) FROM config_alert_log)", ct));

            /* The two past the limit reached both channels, as the twenty before them did. */
            Assert.Equal(22, slack.Bodies.Count);
            Assert.Equal(22, generic.Bodies.Count);
            foreach (var past in new[] { "srv-121", "srv-122" })
            {
                Assert.Contains(slack.Bodies, b => b.Contains(past, StringComparison.Ordinal));
                Assert.Contains(generic.Bodies, b => b.Contains(past, StringComparison.Ordinal));
            }
        }, config =>
        {
            config.Webhooks.SlackUrl = slack.Url;
            config.NotificationRoutes = new[] { AgentRoute(1, AlertFamily.Performance) with { GenericUrl = generic.Url } };
        });
    }

    [Fact]
    public async Task TheSweep_ExpiresAnUnclaimedRequest_AndAClaimAfterThatIsNotOpen()
    {
        await RunAsync(McpDataDetail.Full, async (h, ct) =>
        {
            await h.Deliverer.DeliverAsync(Outcome("Blocking Detected"), ct);
            string requestId = (await ScalarAsync(h.Connection, "SELECT request_id FROM config.investigation_requests", ct))!;

            /* Past the request's expiry (240 minutes by default). */
            _shift = TimeSpan.FromMinutes(300);
            Assert.Equal(1, (await h.Store.SweepAsync(ct)).Expired);
            Assert.Equal("expired|1", await ScalarAsync(h.Connection,
                "SELECT r.state || '|' || (SELECT count(*) FROM config.investigation_journal WHERE event = 'expired') " +
                "FROM config.investigation_requests AS r", ct));

            var claim = await new FakeInvestigationAgent(h.Host, StatementFilterCensus.LiveHostToken, "contract-agent").ClaimAsync(requestId);
            Assert.Equal("not_open", claim.GetProperty("status").GetString());
        });
    }

    /// <summary>The one open request the agent lists, by id.</summary>
    private static async Task<string> ListOneAsync(FakeInvestigationAgent agent)
    {
        var list = await agent.ListAsync();
        Assert.Equal("ok", list.GetProperty("status").GetString());
        var requests = list.GetProperty("requests").EnumerateArray().ToList();
        Assert.Single(requests);
        return requests[0].GetProperty("request_id").GetString()!;
    }

    /// <summary>What every leg proves after the agent's run: it read every next call, its report was accepted, the
    /// journal holds opened, claimed, reported in that order, the note waits as pending, and the viewer reads the
    /// report.</summary>
    private static async Task AssertTheRunAsync(
        Harness h, FakeInvestigationAgent agent, InvestigationRun run, string requestId, CancellationToken ct)
    {
        Assert.NotEmpty(run.Calls);
        Assert.All(run.Calls, c => Assert.Contains(c.Status,
            new[] { "ok", "empty", "unavailable", "not_collected", "precondition", "insufficient_data" }));
        /* It only reads: the investigation pair is all it writes through, and no read it made is a write tool. */
        Assert.DoesNotContain(run.ToolsCalled, t => PerformanceMonitor.Darling.Service.Mcp.DarlingMcpWritePolicy.IsWrite(t));
        Assert.Equal("reported", run.Submit.GetProperty("status").GetString());

        Assert.Equal("opened,claimed,reported", await ScalarAsync(h.Connection,
            $"SELECT string_agg(event, ',' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{requestId}' " +
            "AND event IN ('opened', 'claimed', 'reported')", ct));
        Assert.Equal("pending|true", await ScalarAsync(h.Connection,
            "SELECT state || '|' || (author LIKE '%contract-agent%')::text FROM config.investigation_notes", ct));

        /* The report reads back through the tool, and through the viewer partial. */
        var reports = await agent.CallAsync("get_investigation_reports",
            new System.Text.Json.Nodes.JsonObject { ["request_id"] = requestId });
        var served = reports.GetProperty("reports")[0];
        Assert.Equal("real", served.GetProperty("verdict").GetString());
        /* #6004: Darling serves the evidence by basis, never as one `evidence` list. */
        Assert.Equal(run.Calls.Count, served.GetProperty("evidence_verified").GetArrayLength());
        Assert.Equal(1, served.GetProperty("evidence_inferred").GetArrayLength());
        Assert.False(served.TryGetProperty("evidence", out _));

        await using var viewer = new ViewerDataService(h.StoreString);
        var detail = await viewer.GetInvestigationRequestDetailAsync(Guid.Parse(requestId), ct);
        Assert.Single(detail.Reports);
        Assert.Equal("real", detail.Reports[0].Verdict);
        Assert.NotNull(detail.BriefJson);
        Assert.Contains(detail.Journal, j => j.Event == "reported" && j.Actor.Contains(agent.AgentId, StringComparison.Ordinal));
    }

    private async Task RunAsync(McpDataDetail dataDetail, Func<Harness, CancellationToken, Task> body, Action<DarlingConfig>? configure = null)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the investigation contract live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(storeString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        /* The server the brief's next calls name must resolve, as a registered one does. */
        await ScalarAsync(connection, "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) " +
            "VALUES (" + ServerId.ToString(CultureInfo.InvariantCulture) + ", 'srv', 'srv', TRUE, 16, now(), now())", ct);

        await using var historySource = NpgsqlDataSource.Create(storeString);
        await using var investigationSource = NpgsqlDataSource.Create(InvestigationDispatcher.WithLockTimeout(storeString));
        var store = new InvestigationRequestStore(investigationSource, 30, () => DateTime.UtcNow + _shift);

        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.Summary;
        config.NotificationRoutes = new[] { AgentRoute(1, AlertFamily.Performance) };
        configure?.Invoke(config);
        var settings = new DarlingAlertSettings(config);
        var history = new PgAlertHistoryStore(historySource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var dispatcher = new InvestigationDispatcher(store, settings, NullLogger.Instance);

        using var host = await StatementFilterCensus.BuildLiveHostAsync(storeString, dataDetail);
        try
        {
            await body(new Harness(
                new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, investigationDispatcher: dispatcher),
                new DarlingFindingAlertSender(settings, history, webhooks, NullLogger.Instance, dispatcher),
                store, connection, historySource, storeString, host), ct);
        }
        finally
        {
            NpgsqlConnection.ClearAllPools();
        }
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
