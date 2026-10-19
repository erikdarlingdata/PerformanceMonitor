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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Level 3 (#5632, step 5), without a store: the route resolver answers a Level 3 route like a Level 2 one, the brief of a Level 3
/// request keeps every Level 2 rule and adds one sentence, the credential slots are classified by prefix only, and darling.json cannot
/// carry a route level or a switch into the config.
/// </summary>
public sealed class ServerActionLevelThreeTests
{
    [Theory]
    [InlineData(NotificationRoute.AgentLevelInvestigate)]
    [InlineData(NotificationRoute.AgentLevelAct)]
    [InlineData(NotificationRoute.AgentLevelServerAct)]
    public void TheAgentResolver_ReturnsEveryAgentLevel_AndLevelThreeIsOneOfThem(int level)
    {
        var route = new NotificationRoute(2, "Blocking", "", "", "", "", "", Enabled: true) { AgentLevel = level };
        Assert.Same(route, NotificationRouter.ResolveAgent("Blocking", new[] { route }));
    }

    [Fact]
    public void AnOffRoute_StillStopsTheLookup_AndAnUnknownLevelAnswersNothing()
    {
        var off = new NotificationRoute(1, "Blocking", "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelOff };
        var three = new NotificationRoute(2, "Blocking", "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelServerAct };
        Assert.Null(NotificationRouter.ResolveAgent("Blocking", new[] { off, three }));
        var four = new NotificationRoute(3, "Blocking", "", "", "", "", "", Enabled: true) { AgentLevel = 4 };
        Assert.Null(NotificationRouter.ResolveAgent("Blocking", new[] { four }));
    }

    private static InvestigationBriefSources Sources(int level) => new(
        new InvestigationBriefRequest(Guid.NewGuid(), 7, "SQLPROD01", false, "blocking", "claimed",
            DateTime.UtcNow.AddHours(4), DateTime.UtcNow.AddMinutes(15), 1, "Blocking", null, level),
        Array.Empty<InvestigationBriefAlert>(), Array.Empty<InvestigationBriefFinding>(), Array.Empty<InvestigationEarlierReport>(),
        Array.Empty<InvestigationRecentChange>(), Array.Empty<InvestigationBriefNote>(), Array.Empty<InvestigationProfileText>());

    [Fact]
    public void ALevelThreeBrief_KeepsEveryLevelTwoRule_AndNamesThePlanForceTools()
    {
        var two = JsonNode.Parse(InvestigationBriefComposer.Compose(Sources(2), DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, DateTime.UtcNow))!.AsObject();
        var three = JsonNode.Parse(InvestigationBriefComposer.Compose(Sources(3), DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, DateTime.UtcNow))!.AsObject();
        Assert.Equal(3, (int)three["autonomy_level"]!);
        Assert.Equal(2, (int)two["autonomy_level"]!);
        Assert.Equal(
            two["allowed_actions"]!.AsArray().Select(n => (string)n!),
            three["allowed_actions"]!.AsArray().Select(n => (string)n!));
        var twoRules = two["read_rules"]!.AsArray().Select(n => (string)n!).ToList();
        var threeRules = three["read_rules"]!.AsArray().Select(n => (string)n!).ToList();
        Assert.Equal(twoRules.Count, threeRules.Count);
        Assert.Contains(threeRules, r => r.Contains("writes refuse after submit", StringComparison.Ordinal));
        var sentence = Assert.Single(threeRules, r => r.Contains("Level 3", StringComparison.Ordinal));
        Assert.Contains("force_query_store_plan and unforce_query_store_plan can change the monitored server", sentence, StringComparison.Ordinal);
        Assert.Contains("switch and remediation credential", sentence, StringComparison.Ordinal);
        Assert.DoesNotContain("not built yet", sentence, StringComparison.Ordinal);
        Assert.DoesNotContain(twoRules, r => r.Contains("Level 3", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(null, null, ServerCredentialState.Missing)]
    [InlineData("svc_fix", null, ServerCredentialState.Missing)]
    [InlineData("svc_fix", "  ", ServerCredentialState.Missing)]
    [InlineData(null, "env:FIX_PASSWORD", ServerCredentialState.Missing)]
    [InlineData("svc_fix", "env:FIX_PASSWORD", ServerCredentialState.Reference)]
    [InlineData("svc_fix", "file:/run/secrets/fix", ServerCredentialState.Reference)]
    [InlineData("svc_fix", "sealed:AQAAANCM", ServerCredentialState.StoredPassword)]
    [InlineData("svc_fix", "AQAAANCMnd8BFdERjHoAwE", ServerCredentialState.StoredPassword)]
    [InlineData("svc_fix", "xenv:FIX_PASSWORD", ServerCredentialState.StoredPassword)]
    public void TheRemediationSlots_AreClassifiedByPrefix_AndANonReferenceIsAStoredPassword(string? login, string? stored, ServerCredentialState expected) =>
        Assert.Equal(expected, InvestigationRequestStore.ClassifyCredential(login, stored));

    [Fact]
    public void DarlingJson_CannotCarryARouteLevelOrASwitchIntoTheConfig()
    {
        var config = DarlingConfig.Parse("""
            {
              "notificationRoutes": [ { "routeId": 1, "metricMatch": "Blocking", "agentLevel": 3, "agent_level": 3 } ],
              "serverActions": [ { "serverId": 7, "action": "force_plan", "enabled": true } ],
              "servers": [ { "name": "srv", "host": "h", "serverActions": { "force_plan": true }, "forcePlanBotEnabled": true } ]
            }
            """);
        Assert.Empty(config.NotificationRoutes);
        Assert.All(config.Servers, s => Assert.False(s.PlanForceBotEnabled));
    }
}

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The server-action gate against a live store (#5632, step 5): each of the four conditions refuses with its own reason when it alone is
/// missing, all four together pass, a Level 2 request is refused with its own sentence, the secret is classified and never resolved, a
/// refusal is journaled once per request, tool, code and hour, and a Level 3 request passes every Level 2 gate check.
/// </summary>
public sealed class ServerActionGateLiveTests
{
    private static readonly DateTime T0 = new(2026, 10, 18, 12, 0, 0, DateTimeKind.Utc);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private DateTime _now = T0;

    private static InvestigationAlert Alert(DateTime at, int level) =>
        new(1, 7, "srv-7", "blocking", "Blocking", at, Array.Empty<string>(), null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = level };

    private static ServerActionCall Call(Guid request, Guid claim, string action = ServerActionNames.ForcePlan, int serverId = 7) =>
        new(action, action, new AgentAction(request, claim, "agent-a", "the plan regressed",
            "[{\"tool\":\"get_query_store_top\",\"arguments\":{},\"key_numbers\":\"12\"}]"),
            new AgentActionTarget(serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), null, null));

    private static ServerActionHooks Hooks(InvestigationRequestStore store, NpgsqlDataSource source, int? resolved = 3, Action? onCredential = null) => new()
    {
        LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, int?>>((_, _) => resolved),
        ReadCredential = (serverId, ct) =>
        {
            onCredential?.Invoke();
            return store.ReadServerCredentialStateAsync(source, serverId, ct);
        },
    };

    private async Task<(Guid Request, Guid Claim)> OpenAndClaimAsync(InvestigationRequestStore store, int level)
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await store.OpenOrJoinAsync(Alert(_now, level), ct);
        var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
        return (opened.RequestId!.Value, claim.ClaimId!.Value);
    }

    private static async Task ArmAsync(NpgsqlConnection connection, bool switchOn, string? login, string? stored, CancellationToken ct)
    {
        await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        await ExecAsync(connection, "DELETE FROM config.config_monitored_servers WHERE server_id = 7", ct);
        await ExecAsync(connection,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, remediation_username, remediation_encrypted_password) VALUES (7, 'srv-7', 'host-7', "
            + (login is null ? "NULL" : $"'{login}'") + ", " + (stored is null ? "NULL" : $"'{stored}'") + ")", ct);
        if (switchOn)
            await ExecAsync(connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'force_plan', true, 'test') "
                + "ON CONFLICT (server_id, action) DO UPDATE SET enabled = true", ct);
    }

    [Fact]
    public async Task AllFourConditionsTogether_Pass_AndTheSecretIsNeverRead()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            await ArmAsync(connection, switchOn: true, "svc_fix", "env:FIX_PASSWORD", ct);
            Assert.Null(await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source), ct));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused'", ct));
            /* It writes nothing but a refusal: no action row, no switch change. */
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", ct));
        });
    }

    [Fact]
    public async Task EachConditionRefusesWithItsOwnReason_WhenItAloneIsMissing_AndJournalsOneRefusedRowAnHour()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            async Task Refuses(string code, ServerActionHooks hooks, string action = ServerActionNames.ForcePlan)
            {
                /* The clock moves an hour at a time to reach a new journal hour; the claim's lease moves with it. */
                await ExecAsync(connection, FormattableString.Invariant(
                    $"UPDATE config.investigation_requests SET lease_until = '{_now:yyyy-MM-dd HH:mm:ss}+00'::timestamptz + interval '10 minutes', expires_at = '{_now:yyyy-MM-dd HH:mm:ss}+00'::timestamptz + interval '4 hours'"), ct);
                var before = long.Parse((await ScalarAsync(connection,
                    $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{code}' AND request_id = '{request}'", ct))!, CultureInfo.InvariantCulture);
                for (var i = 0; i < 3; i++)
                    Assert.Equal(code, (await store.CheckServerActionAsync(Call(request, claim, action), hooks, ct))?.Reason);
                /* Three refusals in one hour journal one row. */
                Assert.Equal((before + 1).ToString(CultureInfo.InvariantCulture), await ScalarAsync(connection,
                    $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{code}' AND request_id = '{request}'", ct));
            }

            /* Condition 3 alone missing: the credential and the level are fine, the switch has no row (no row is off). */
            await ArmAsync(connection, switchOn: false, "svc_fix", "env:FIX_PASSWORD", ct);
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_server_actions", ct));
            await Refuses(AgentActionCodes.SwitchOff, Hooks(store, source));

            /* A row that says off refuses the same way. */
            await ExecAsync(connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'force_plan', false, 'test')", ct);
            _now = T0.AddMinutes(61);
            await Refuses(AgentActionCodes.SwitchOff, Hooks(store, source));

            /* The switch is per action: force_plan on does not turn evict_plan on. */
            await ExecAsync(connection, "UPDATE config.config_server_actions SET enabled = true", ct);
            Assert.Null(await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source), ct));
            await Refuses(AgentActionCodes.SwitchOff, Hooks(store, source), ServerActionNames.EvictPlan);

            /* Condition 4 alone missing: no login, then a login with no secret. */
            await ArmAsync(connection, switchOn: true, null, null, ct);
            _now = T0.AddMinutes(122);
            await Refuses(AgentActionCodes.NoCredential, Hooks(store, source));
            await ArmAsync(connection, switchOn: true, "svc_fix", null, ct);
            _now = T0.AddMinutes(183);
            await Refuses(AgentActionCodes.NoCredential, Hooks(store, source));

            /* A stored password is refused with its own reason, and an env: reference passes. */
            await ArmAsync(connection, switchOn: true, "svc_fix", "sealed:AQAAANCMnd8BFdERjHoAwE", ct);
            _now = T0.AddMinutes(244);
            await Refuses(AgentActionCodes.CredentialNotReference, Hooks(store, source));
            await ArmAsync(connection, switchOn: true, "svc_fix", "file:/run/secrets/fix", ct);
            Assert.Null(await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source), ct));

            /* Condition 1 alone missing. */
            await ExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = false", ct);
            _now = T0.AddMinutes(305);
            await Refuses(AgentActionCodes.ActionsOff, Hooks(store, source));
            await ExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = true", ct);
            Assert.Null(await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source), ct));
        });
    }

    [Fact]
    public async Task ALevelTwoRequest_IsRefused_WithTheSentenceThatLevelTwoChangesOnlyTheMonitoringStore()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store, 2);
            await ArmAsync(connection, switchOn: true, "svc_fix", "env:FIX_PASSWORD", ct);
            var refusal = await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source, resolved: 2), ct);
            Assert.Equal(AgentActionCodes.NotLevel3, refusal?.Reason);
            Assert.StartsWith("Level 2 changes only the monitoring store", refusal!.Message, StringComparison.Ordinal);
            /* Stamped 2, the request is refused even where the routes now say 3: the stamp is the ceiling. */
            Assert.Equal(AgentActionCodes.NotLevel3, (await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source, resolved: 3), ct))?.Reason);
        });
    }

    [Fact]
    public async Task ARequestStampedThree_WhoseRoutesNowAnswerLess_IsRefused_AndARouteDeletedAfterTheOpenFailsClosed()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            await ArmAsync(connection, switchOn: true, "svc_fix", "env:FIX_PASSWORD", ct);
            Assert.Null(await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source, resolved: 3), ct));
            /* An operator lowered a route to 2: the lowest level wins. */
            Assert.Equal(AgentActionCodes.NotLevel3, (await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source, resolved: 2), ct))?.Reason);
            /* A route deleted after the open resolves to nothing and fails closed. */
            Assert.Equal(AgentActionCodes.NotLevel3, (await store.CheckServerActionAsync(Call(request, claim), Hooks(store, source, resolved: null), ct))?.Reason);
        });
    }

    [Fact]
    public async Task TheClaimThePracticeFlagAndTheServerAreChecked_AndNoCredentialIsReadForARefusedCall()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            await ArmAsync(connection, switchOn: true, "svc_fix", "env:FIX_PASSWORD", ct);
            var reads = 0;
            var hooks = Hooks(store, source, onCredential: () => reads++);

            Assert.Equal(AgentActionCodes.ClaimNotLive, (await store.CheckServerActionAsync(Call(request, Guid.NewGuid()), hooks, ct))?.Reason);
            Assert.Equal(AgentActionCodes.OtherServer, (await store.CheckServerActionAsync(Call(request, claim, serverId: 8), hooks, ct))?.Reason);
            Assert.Equal(AgentActionCodes.UnknownServerAction, (await store.CheckServerActionAsync(Call(request, claim, "drop_database"), hooks, ct))?.Reason);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET practice_of = gen_random_uuid() WHERE request_id = '{request}'", ct);
            Assert.Equal(AgentActionCodes.Practice, (await store.CheckServerActionAsync(Call(request, claim), hooks, ct))?.Reason);
            Assert.Equal(0, reads);
        });
    }

    [Fact]
    public async Task ALevelThreeRequest_PassesTheLevelTwoGate_ThatALevelTwoRequestPasses()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            foreach (var level in new[] { 2, 3 })
            {
                var (request, claim) = await OpenAndClaimAsync(store, level);
                var writes = 0;
                var hooks = new AgentActionHooks
                {
                    LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, int?>>((_, _) => level),
                    ReadBefore = (_, _, _) => Task.FromResult<string?>("{\"reason\":\"old\"}"),
                    Write = (_, _, _) => { writes++; return Task.FromResult<string?>("{\"reason\":\"new\"}"); },
                };
                var call = new AgentActionCall("create_mute_rule", "mute", Call(request, claim).Action, Call(request, claim).Target);
                var result = await store.RunAgentActionAsync(call, hooks, ct);
                Assert.True(result.Done, $"Level {level} passes the Level 2 gate");
                Assert.Equal(1, writes);
                await store.ReleaseAsync(request, claim, ct);
                _now = _now.AddMinutes(1);
                await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'closed' WHERE request_id = '{request}'", ct);
            }
        });
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the server action gate live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => _now);
        await body(store, connection, dataSource, ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
