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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The proposal notice on a live store (#7109): a new proposal sends one notice to its request's route, a retry that returns the same
/// proposal sends none, a second pass sends none, the notice carries the kind, the server and the link but nothing the agent wrote, it
/// carries the nobody-can-approve sentence only while nobody can approve, and a route with no channel gets the mark and no send.
/// </summary>
public sealed class ProposalNoticeLiveTests
{
    private const string AgentReason = "AGENT-REASON-TEXT drop table";
    private const string TargetText = "TARGET-TEXT-9917";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class RecordingSends : IInvestigationEventSends
    {
        public List<(IReadOnlyList<(string Channel, string Destination)> Targets, string Title, string Server, string Current, string Detail)> Notices { get; } = [];

        public Task<string?> PostEventAsync(int routeId, string url, string payload, string eventId, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);

        public Task<IReadOnlyList<string>> SendNoticeAsync(IReadOnlyList<(string Channel, string Destination)> targets, string title,
            string serverName, string currentValue, string detail, CancellationToken cancellationToken)
        {
            Notices.Add((targets, title, serverName, currentValue, detail));
            return Task.FromResult<IReadOnlyList<string>>(["Slack"]);
        }

        public Task<bool> RaiseSelfAlertAsync(Guid requestId, string serverName, int serverId, string shortMessage, string detail, CancellationToken cancellationToken) =>
            Task.FromResult(true);
    }

    private static NotificationRoute Route(string slack, string teams = "", string email = "") =>
        new(1, "High CPU", teams, slack, "", "", email, Enabled: true)
        {
            AgentLevel = NotificationRoute.AgentLevelServerAct, AgentMaxPerHour = 50, AgentMaxPerDay = 500,
        };

    private DateTime _now = DateTime.UtcNow;

    /// <summary>Opens and claims a request on route 1 and server 101, and has the agent propose a kill on it. Answers the proposal.</summary>
    private async Task<(InvestigationRequestStore Store, ActionProposalCreated Created, ActionProposalDraft Draft)> ProposeAsync(
        InvestigationRequestStore store, NpgsqlConnection connection, bool singlePerson, CancellationToken ct)
    {
        await using (var command = new NpgsqlCommand(
            "INSERT INTO config.config_notification_routes (route_id, metric_match, agent_level, action_approval, approval_single_person) OVERRIDING SYSTEM VALUE "
            + $"VALUES (1, 'High CPU', 3, 'required', {(singlePerson ? "TRUE" : "FALSE")}) ON CONFLICT DO NOTHING; "
            + "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, engine_kind, created_date, modified_date) "
            + "VALUES (101, 'srv-101', 'srv-101', TRUE, 16, 'sqlserver', now(), now()) ON CONFLICT DO NOTHING", connection))
        {
            await command.ExecuteNonQueryAsync(ct);
        }

        var alert = new InvestigationAlert(1, 101, "srv-101", "resources", "High CPU", _now, Array.Empty<string>(), null, 50, 500,
            LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 3 };
        var opened = await store.OpenOrJoinAsync(alert, ct);
        var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
        var draft = new ActionProposalDraft(opened.RequestId.Value, 1, "agent-a", "kill_session", 101,
            new JsonObject { ["session_id"] = 55, ["note"] = TargetText }, new JsonObject { ["session_id"] = 55 }, AgentReason);
        return (store, await store.CreateProposalAsync(draft, ct), draft);
    }

    private InvestigationEventDelivery Delivery(InvestigationRequestStore store, RecordingSends sends, NotificationRoute route, string baseUrl) =>
        new(store, () => [route], sends, null, NullLogger.Instance, () => _now, baseUrl: () => baseUrl);

    [Fact]
    public async Task ANewProposal_SendsOneNotice_ARetrySendsNone_AndASecondPassSendsNone()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (_, created, draft) = await ProposeAsync(store, connection, singlePerson: false, ct);
            var retry = await store.CreateProposalAsync(draft, ct);
            Assert.False(retry.Created);
            Assert.Equal(created.ProposalId, retry.ProposalId);

            var sends = new RecordingSends();
            var delivery = Delivery(store, sends, Route("https://hooks.example/slack", email: "ops@example.com"), "https://dash.example");
            Assert.Equal(1, (await delivery.RunOnceAsync(ct)).Notices);
            var notice = Assert.Single(sends.Notices);
            Assert.Contains(notice.Targets, t => t.Channel == "Slack");
            Assert.Contains(notice.Targets, t => t.Channel == "Email");
            Assert.Equal("srv-101", notice.Server);
            Assert.Equal("kill a session", notice.Current);
            Assert.Contains($"https://dash.example/#/proposals?proposal={created.ProposalId:D}", notice.Detail, StringComparison.Ordinal);

            _now = _now.AddMinutes(1);
            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Notices);
            Assert.Single(sends.Notices);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'proposal_notice_sent' AND once_key = 'proposal_notice:{created.ProposalId}'", ct));
        });
    }

    [Fact]
    public async Task TheNotice_CarriesNoReason_NoTarget_AndNoStatement()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ProposeAsync(store, connection, singlePerson: false, ct);
            var sends = new RecordingSends();
            await Delivery(store, sends, Route("https://hooks.example/slack"), "https://dash.example").RunOnceAsync(ct);

            var notice = Assert.Single(sends.Notices);
            var all = string.Join("|", notice.Title, notice.Server, notice.Current, notice.Detail);
            Assert.DoesNotContain("AGENT-REASON-TEXT", all, StringComparison.Ordinal);
            Assert.DoesNotContain(TargetText, all, StringComparison.Ordinal);
            Assert.DoesNotContain("55", all, StringComparison.Ordinal);
            Assert.DoesNotContain("drop table", all, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public async Task TheSentence_IsInTheNotice_WhileNobodyCanApprove_AndGoneOnceAPersonCan_OrOnASinglePersonRoute()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (_, created, _) = await ProposeAsync(store, connection, singlePerson: false, ct);
            var sends = new RecordingSends();
            await Delivery(store, sends, Route("https://hooks.example/slack"), "https://dash.example").RunOnceAsync(ct);
            Assert.Contains(ActionApprovalRules.NobodyCanApproveMessage, Assert.Single(sends.Notices).Detail, StringComparison.Ordinal);

            /* A mapped person with the grant: the next proposal's notice has no sentence. */
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host, port, encrypt_mode, trust_server_certificate) "
                + "VALUES (101, 'srv-101', 'h', 1433, 'Optional', TRUE) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(connection,
                "WITH p AS (INSERT INTO config.person (display_name, created_by, updated_by) VALUES ('Approver', 't', 't') RETURNING person_id), "
                + "i AS (INSERT INTO config.person_identity (person_id, kind, external_id, created_by) SELECT person_id, 'web', repeat('A', 43), 't' FROM p) "
                + "INSERT INTO config.person_grant (person_id, scope_server_id, capability, kinds, created_by) SELECT person_id, 101, 'approve', ARRAY['kill_session'], 't' FROM p", ct);
            var other = await store.CreateProposalAsync(new ActionProposalDraft(
                (await GetRequestAsync(connection, created.ProposalId, ct)), 1, "agent-a", "kill_session", 101,
                new JsonObject { ["session_id"] = 56 }, new JsonObject { ["session_id"] = 56 }, null), ct);
            Assert.True(other.Created);
            Assert.Equal(1, (await Delivery(store, sends, Route("https://hooks.example/slack"), "https://dash.example").RunOnceAsync(ct)).Notices);
            Assert.DoesNotContain(ActionApprovalRules.NobodyCanApproveMessage, sends.Notices[1].Detail, StringComparison.Ordinal);
        });

        await RunAsync(async (store, connection, ct) =>
        {
            await ProposeAsync(store, connection, singlePerson: true, ct);
            var sends = new RecordingSends();
            await Delivery(store, sends, Route("https://hooks.example/slack"), "https://dash.example").RunOnceAsync(ct);
            Assert.DoesNotContain(ActionApprovalRules.NobodyCanApproveMessage, Assert.Single(sends.Notices).Detail, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task WithNoBaseUrl_TheNoticeSaysToOpenTheProposalInTheWebOrTheViewer_AndCarriesNoLink()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ProposeAsync(store, connection, singlePerson: false, ct);
            var sends = new RecordingSends();
            await Delivery(store, sends, Route("https://hooks.example/slack"), "").RunOnceAsync(ct);

            var detail = Assert.Single(sends.Notices).Detail;
            Assert.Contains(ProposalNotice.NoLinkLine, detail, StringComparison.Ordinal);
            Assert.DoesNotContain("http", detail, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ARouteWithNoNoticeChannel_GetsTheMark_AndNoSend()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (_, created, _) = await ProposeAsync(store, connection, singlePerson: false, ct);
            var sends = new RecordingSends();
            var delivery = Delivery(store, sends, Route(""), "https://dash.example");

            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Notices);
            Assert.Empty(sends.Notices);
            Assert.Equal("no_destination", await ScalarAsync(connection,
                $"SELECT reason FROM config.investigation_journal WHERE once_key = 'proposal_notice:{created.ProposalId}'", ct));
            Assert.Equal(0, (await delivery.RunOnceAsync(ct)).Notices);
        });
    }

    private static async Task<Guid> GetRequestAsync(NpgsqlConnection connection, Guid proposalId, CancellationToken ct) =>
        Guid.Parse((await ScalarAsync(connection, $"SELECT request_id FROM config.agent_action_proposals WHERE proposal_id = '{proposalId}'", ct))!);

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the proposal notice live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
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
