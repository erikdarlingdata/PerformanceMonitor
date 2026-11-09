/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #8473: an agent token reaches the four mute rule tools by two gates, the claim path (an <c>agent_action</c>) and the claim-free path
/// (none). Leaving the argument out must never widen what a mute can silence: a mute the claim path refuses for a protected metric or a
/// missing server is refused for the same reason on the claim-free path. A digest keeps the claim-free path's scope (#7414). The same
/// class holds the two neighbouring facts: with an owner source that has not published, the path answers refused rather than failing on
/// the journal insert, and the hourly write cap is counted per agent.
/// </summary>
public sealed class AgentMuteDirectScopeLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingMcpWritePolicy.Effective AgentPolicy = new(false, false, Investigations: true, TokenName: "agent-a");
    private static readonly DarlingAgentActionSeat Agent = new(AgentPolicy);
    private static readonly DarlingAgentActionSeat Legacy = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, string Arg, Guid Request, CancellationToken Ct);

    /// <summary>Two servers, a Level 2 route, and a request on srv-7 that agent-a has claimed, so the same seat can call with or without the claim.</summary>
    private static async Task RunAsync(Func<Ctx, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the agent mute scope live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        foreach (var id in new[] { 7, 8 })
        {
            await ExecAsync(connection, $@"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ({id}, 'srv-{id}', 'srv-{id}', TRUE, 15, now(), now())", ct);
        }

        await ExecAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', 2)", ct);
        var store = new InvestigationRequestStore(source, 30);
        var alert = new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100,
            LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 2 };
        var opened = await store.OpenOrJoinAsync(alert, ct);
        var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
        var arg = AgentActionArgumentTests.Arg(opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString());
        await body(new Ctx(source, connection, arg, opened.RequestId.Value, ct));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(Ctx c, string sql)
    {
        await using var command = new NpgsqlCommand(sql, c.Connection);
        return Convert.ToString(await command.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture);
    }

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static string Iso(DateTime utc) => utc.ToString("o", CultureInfo.InvariantCulture);

    private static Task<string?> RuleCountAsync(Ctx c) => ScalarAsync(c, "SELECT count(*) FROM config_mute_rules");

    /// <summary>The claim-free path's journal rows: the claim path's own refusals carry the request id and are not counted here.</summary>
    private static Task<string?> DirectJournalCountAsync(Ctx c) =>
        ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE request_id IS NULL AND event LIKE 'action_%'");

    /// <summary>A rule an operator wrote straight into the table, with an expiry inside the claim-free path's bound.</summary>
    private static async Task<string> SeedAsync(Ctx c, bool enabled, int? serverId, string metric, string action = "mute")
    {
        var id = Guid.NewGuid().ToString();
        await ExecAsync(c.Connection, $@"INSERT INTO config_mute_rules (id, enabled, created_at_utc, server_id, server_name, metric_name, reason, expires_at_utc, action)
VALUES ('{id}', {(enabled ? "TRUE" : "FALSE")}, now() AT TIME ZONE 'utc', {(serverId is null ? "NULL" : serverId.ToString())},
{(serverId is null ? "NULL" : $"'srv-{serverId}'")}, '{metric}', 'operator rule', '{Iso(DateTime.UtcNow.AddDays(2))}', '{action}')", c.Ct);
        return id;
    }

    /// <summary>The claim path: the same tool, with the claimed request's <c>agent_action</c>.</summary>
    private static Task<string> ClaimCreate(Ctx c, string? metric, int? serverId) =>
        DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: metric, server_id: serverId, reason: "known batch", agent_action: c.Arg, agentSeat: Agent);

    /// <summary>The claim-free path: the same tool and seat, no <c>agent_action</c>, with the reason and expiry that path requires.</summary>
    private static Task<string> DirectCreate(Ctx c, DarlingAgentActionSeat seat, string? metric, int? serverId, string action = "mute") =>
        DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: metric, server_id: serverId, reason: "known batch",
            expires_at: Iso(DateTime.UtcNow.AddDays(1)), action: action, agentSeat: seat);

    private static string Changes(string field, string value) =>
        "{\"reason\":\"narrowing\",\"expires_at_utc\":\"" + Iso(DateTime.UtcNow.AddDays(1)) + "\",\"" + field + "\":" + value + "}";

    [Theory]
    [InlineData("Collection Stopped", 7, DarlingMcpAgentActions.SelfMonitorMetric)]
    [InlineData("Store Disk Pressure", 7, DarlingMcpAgentActions.SelfMonitorMetric)]
    /* A spelling that differs only in case still matches the protected alert (mute rules match the metric case-insensitively), so it is protected too. */
    [InlineData("collection stopped", 7, DarlingMcpAgentActions.SelfMonitorMetric)]
    [InlineData("COLLECTOR COST REGRESSION", 7, DarlingMcpAgentActions.SelfMonitorMetric)]
    [InlineData("Blocking Detected", null, DarlingMcpAgentActions.MuteNeedsMetric)]
    public async Task ACreateTheClaimPathRefuses_ForAProtectedMetricOrNoServer_IsRefusedForTheSameReason_ReSentAsAMuteWithoutAgentAction(
        string metric, int? serverId, string code)
    {
        await RunAsync(async c =>
        {
            var claim = Parse(await ClaimCreate(c, metric, serverId));
            Assert.Equal("refused", (string?)claim["status"]);
            Assert.Equal(code, (string?)claim["reason"]);

            var direct = Parse(await DirectCreate(c, Agent, metric, serverId));
            Assert.True("refused" == (string?)direct["status"], direct.ToJsonString());
            Assert.Equal((string?)claim["reason"], (string?)direct["reason"]);
            Assert.Contains("Nothing was written", (string?)direct["message"], StringComparison.Ordinal);

            Assert.Equal("0", await RuleCountAsync(c));
            Assert.Equal("0", await DirectJournalCountAsync(c));
        });
    }

    [Fact]
    public async Task AnUpdateOrEnableTheClaimPathRefuses_ForAProtectedMetric_IsRefusedForTheSameReason_WithoutAgentAction()
    {
        await RunAsync(async c =>
        {
            /* An update that moves an agent-muteable rule onto a protected metric. */
            var rule = await SeedAsync(c, enabled: true, serverId: 7, metric: "High CPU");
            var claim = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule, "{\"reason\":\"x\",\"metric_name\":\"Collection Stopped\"}", c.Arg, Agent));
            Assert.Equal(DarlingMcpAgentActions.SelfMonitorMetric, (string?)claim["reason"]);

            var direct = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule, Changes("metric_name", "\"Collection Stopped\""), agentSeat: Agent));
            Assert.True("refused" == (string?)direct["status"], direct.ToJsonString());
            Assert.Equal((string?)claim["reason"], (string?)direct["reason"]);
            Assert.Equal("High CPU", await ScalarAsync(c, $"SELECT metric_name FROM config_mute_rules WHERE id = '{rule}'"));

            /* The same move with a spelling that differs only in case: the rule would still match the protected alert, so both paths refuse it. */
            var claimLower = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule, "{\"reason\":\"x\",\"metric_name\":\"collection stopped\"}", c.Arg, Agent));
            Assert.True(DarlingMcpAgentActions.SelfMonitorMetric == (string?)claimLower["reason"], claimLower.ToJsonString());
            var directLower = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule, Changes("metric_name", "\"collection stopped\""), agentSeat: Agent));
            Assert.True("refused" == (string?)directLower["status"], directLower.ToJsonString());
            Assert.Equal(DarlingMcpAgentActions.SelfMonitorMetric, (string?)directLower["reason"]);
            Assert.Equal("High CPU", await ScalarAsync(c, $"SELECT metric_name FROM config_mute_rules WHERE id = '{rule}'"));

            /* Enabling an operator's disabled mute of a protected metric. */
            var protectedRule = await SeedAsync(c, enabled: false, serverId: 7, metric: "Collection Stopped");
            var claimEnable = Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, protectedRule, true, c.Arg, Agent));
            Assert.Equal(DarlingMcpAgentActions.SelfMonitorMetric, (string?)claimEnable["reason"]);

            var directEnable = Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, protectedRule, true, agentSeat: Agent, reason: "turn it on"));
            Assert.True("refused" == (string?)directEnable["status"], directEnable.ToJsonString());
            Assert.Equal((string?)claimEnable["reason"], (string?)directEnable["reason"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{protectedRule}'"));

            Assert.Equal("0", await DirectJournalCountAsync(c));
        });
    }

    [Fact]
    public async Task AMuteOnAllServers_IsRefusedOnUpdateAndEnable_WithTheClaimPathsReasonWord_WhileADigestThereIsAllowed_AndADisableOrDeleteStillIs()
    {
        await RunAsync(async c =>
        {
            /* The latitude a digest keeps (#7414): all servers and one of the service's own alerts. */
            var digest = Parse(await DirectCreate(c, Agent, "Collection Stopped", serverId: null, action: "digest"));
            Assert.True("created" == (string?)digest["status"], digest.ToJsonString());
            var digestId = (string)digest["mute_rule"]!["id"]!;

            /* Turning that digest into a mute would silence the alert on every server: refused with the claim path's word, and the rule stays a digest. */
            var toMute = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, digestId, Changes("action", "\"mute\""), agentSeat: Agent));
            Assert.True("refused" == (string?)toMute["status"], toMute.ToJsonString());
            Assert.Equal(DarlingMcpAgentActions.MuteNeedsMetric, (string?)toMute["reason"]);
            Assert.Contains("digest", (string?)toMute["message"], StringComparison.Ordinal);
            Assert.Equal("digest", await ScalarAsync(c, $"SELECT action FROM config_mute_rules WHERE id = '{digestId}'"));

            /* Enabling an operator's disabled all-servers mute is refused the same way. */
            var allServers = await SeedAsync(c, enabled: false, serverId: null, metric: "Blocking Detected");
            var enable = Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, allServers, true, agentSeat: Agent, reason: "turn it on"));
            Assert.True("refused" == (string?)enable["status"], enable.ToJsonString());
            Assert.Equal(DarlingMcpAgentActions.MuteNeedsMetric, (string?)enable["reason"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{allServers}'"));

            /* Disabling or deleting a wide mute can only make the operator hear more, so both stay allowed. */
            var wide = await SeedAsync(c, enabled: true, serverId: null, metric: "Blocking Detected");
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, wide, false, agentSeat: Agent, reason: "storm over"))["status"]);
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, wide, agentSeat: Agent, reason: "stale"))["status"]);

            /* The digest create, the disable and the delete each left one journal pair; the refusals left none. */
            Assert.Equal("6", await DirectJournalCountAsync(c));
        });
    }

    [Fact]
    public async Task WithAnOwnerSourceThatHasNotPublished_EveryVerbAnswersRefused_AndWritesNothing_AndAPublishedOneWrites()
    {
        await RunAsync(async c =>
        {
            var owner = new StoreOwnerSource();
            var seat = new DarlingAgentActionSeat(AgentPolicy, owner);
            var rule = await SeedAsync(c, enabled: true, serverId: 7, metric: "High CPU");

            var answers = new[]
            {
                Parse(await DirectCreate(c, seat, "Deadlocks", serverId: 7)),
                Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule, Changes("database_pattern", "\"sales\""), agentSeat: seat)),
                Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, rule, false, agentSeat: seat, reason: "storm over")),
                Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, rule, agentSeat: seat, reason: "stale")),
            };
            foreach (var answer in answers)
            {
                Assert.True("refused" == (string?)answer["status"], answer.ToJsonString());
                Assert.Equal(DarlingMcpPlanForceTools.NoOwnerSource, (string?)answer["reason"]);
                Assert.Contains("Nothing was written", (string?)answer["message"], StringComparison.Ordinal);
            }

            Assert.Equal("1", await RuleCountAsync(c));
            Assert.Equal("True", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{rule}'"));
            Assert.Equal("operator rule", await ScalarAsync(c, $"SELECT reason FROM config_mute_rules WHERE id = '{rule}'"));
            Assert.Equal("0", await DirectJournalCountAsync(c));

            /* Once the worker has published its source, the same seat writes, and the journal row goes with the rule. */
            owner.Publish(c.Source);
            var created = Parse(await DirectCreate(c, seat, "Deadlocks", serverId: 7));
            Assert.True("created" == (string?)created["status"], created.ToJsonString());
            Assert.Equal("2", await RuleCountAsync(c));
            Assert.Equal("2", await DirectJournalCountAsync(c));
        });
    }

    [Fact]
    public async Task TheHourlyCap_IsCountedPerAgent_SoOneAgentsWritesDoNotUseUpAnothers()
    {
        await RunAsync(async c =>
        {
            /* Twenty writes by another agent in the last hour leave this agent's twenty untouched. */
            for (var i = 0; i < DarlingMcpAgentMuteDirect.MaxWritesPerHour; i++)
                await ExecAsync(c.Connection, "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail) " +
                    "VALUES (now(), NULL, NULL, 'action_called', 'agent:other', 'x', '{}'::jsonb)", c.Ct);
            var first = Parse(await DirectCreate(c, Agent, "High CPU", serverId: 7, action: "digest"));
            Assert.True("created" == (string?)first["status"], first.ToJsonString());

            /* That write is this agent's first of the hour. Nineteen more and it is at its cap, while the legacy token still has its own. */
            for (var i = 0; i < DarlingMcpAgentMuteDirect.MaxWritesPerHour - 1; i++)
                await ExecAsync(c.Connection, "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail) " +
                    "VALUES (now(), NULL, NULL, 'action_called', 'agent:agent-a', 'x', '{}'::jsonb)", c.Ct);
            var capped = Parse(await DirectCreate(c, Agent, "Deadlocks", serverId: 7, action: "digest"));
            Assert.True("refused" == (string?)capped["status"], capped.ToJsonString());
            Assert.Equal(DarlingMcpAgentMuteDirect.RateCode, (string?)capped["reason"]);
            Assert.Contains("20", (string?)capped["message"], StringComparison.Ordinal);

            var other = Parse(await DirectCreate(c, Legacy, "Deadlocks", serverId: 7, action: "digest"));
            Assert.True("created" == (string?)other["status"], other.ToJsonString());
            Assert.Equal("2", await RuleCountAsync(c));
        });
    }
}
