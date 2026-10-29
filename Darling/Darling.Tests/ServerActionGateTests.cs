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
[Trait("Stage", "Guard")]
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
    public void ALevelThreeBrief_KeepsEveryLevelTwoRule_AndNamesEveryPlanChangingTool()
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
        foreach (var tool in new[] { "force_query_store_plan", "unforce_query_store_plan", "evict_cached_plan", "kill_session" })
            Assert.Contains(tool, sentence, StringComparison.Ordinal);
        Assert.Contains("can change the monitored server", sentence, StringComparison.Ordinal);
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
        LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => resolved),
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
                    LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => level),
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

    private int _writes;

    /// <summary>The write of a plan force: <c>ReadBefore</c> runs in the gate's transaction, after the gate's checks passed and before the
    /// write's own transaction opens, which is where a person changes something mid-flight.</summary>
    private AgentActionHooks WriteHooks(Func<int> resolved, Func<CancellationToken, Task> inFlight) => new()
    {
        LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => resolved()),
        ReadBefore = async (_, _, token) =>
        {
            await inFlight(token);
            return "{\"reason\":\"old\"}";
        },
        Write = (_, _, _) => { _writes++; return Task.FromResult<string?>("{\"reason\":\"new\"}"); },
    };

    private static AgentActionCall WriteCall(Guid request, Guid claim, string kind = AgentChangeKinds.PlanForce)
    {
        var call = Call(request, claim);
        return new AgentActionCall("force_plan", kind, call.Action, call.Target);
    }

    private async Task<string?> EventsAsync(NpgsqlConnection connection, Guid request, CancellationToken ct) =>
        await ScalarAsync(connection,
            $"SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{request}' AND event LIKE 'action_%'", ct);

    [Fact]
    public async Task TheWrite_ReChecksTheServersSwitch_RefusesSwitchOff_JournalsActionFailed_AndNothingReachesTheServer()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            /* The gate has passed this server's switch; a person turns it off now, before the write's own transaction. */
            var hooks = WriteHooks(() => 3, token => ExecAsync(connection,
                "UPDATE config.config_server_actions SET enabled = false WHERE server_id = 7 AND action = 'force_plan'", token));
            var result = await store.RunAgentActionAsync(WriteCall(request, claim), hooks, ct);
            Assert.Equal(AgentActionCodes.SwitchOff, result.Refusal?.Reason);
            Assert.NotNull(result.CalledId);
            Assert.Equal(0, _writes);
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task TheWrite_ReChecksLevelThree_RefusesARouteLoweredToLevelTwo_JournalsActionFailed_AndNothingReachesTheServer()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            var level = 3;
            /* The routes are read again by the write: a person lowers the route to Level 2 after the gate read it as 3. */
            var hooks = WriteHooks(() => level, token => { level = 2; return Task.CompletedTask; });
            var result = await store.RunAgentActionAsync(WriteCall(request, claim), hooks, ct);
            Assert.Equal(AgentActionCodes.NotLevel3, result.Refusal?.Reason);
            Assert.NotNull(result.CalledId);
            Assert.Equal(0, _writes);
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task TheWrite_WaitsForASwitchWriterHoldingTheSwitchLock_ThenRefuses()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            /* The table's bump trigger updates config_service, which the write also waits on; it is off here so only the advisory lock is under test. */
            await ExecAsync(connection, "ALTER TABLE config.config_server_actions DISABLE TRIGGER trg_bump_server_actions", ct);
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            Task? commit = null;
            /* A switch writer: takes the exclusive form of the lock, turns the switch off and commits a second later. The write must wait
               for that commit and then see the switch off; without the lock it reads the committed "on" and writes. */
            var hooks = WriteHooks(() => 3, async token =>
            {
                var writer = await source.OpenConnectionAsync(token);
                var transaction = await writer.BeginTransactionAsync(token);
                await ServerActionSwitchLock.AcquireExclusiveAsync(writer, transaction, 7, ServerActionNames.ForcePlan, 30, token);
                await using (var update = new NpgsqlCommand(
                    "UPDATE config.config_server_actions SET enabled = false WHERE server_id = 7 AND action = 'force_plan'", writer, transaction))
                {
                    await update.ExecuteNonQueryAsync(token);
                }

                commit = Task.Run(async () =>
                {
                    await Task.Delay(1000, CancellationToken.None);
                    await transaction.CommitAsync(CancellationToken.None);
                    await transaction.DisposeAsync();
                    await writer.DisposeAsync();
                }, CancellationToken.None);
            });
            var result = await store.RunAgentActionAsync(WriteCall(request, claim), hooks, ct);
            await commit!;
            Assert.Equal(AgentActionCodes.SwitchOff, result.Refusal?.Reason);
            Assert.Equal(0, _writes);
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task TheWrite_WaitsForTheServiceSwitchToBeChanged_ThenRefuses()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            await ExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = true", ct);
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            Task? commit = null;
            /* The service-wide switch is turned off by a change that has not committed when the write reads it. The row is read FOR SHARE,
               so the read waits for that commit and sees off; a plain read sees the committed "on". */
            var hooks = WriteHooks(() => 3, async token =>
            {
                var writer = await source.OpenConnectionAsync(token);
                var transaction = await writer.BeginTransactionAsync(token);
                await using (var update = new NpgsqlCommand("UPDATE config.config_service SET agent_actions_enabled = false", writer, transaction))
                {
                    await update.ExecuteNonQueryAsync(token);
                }

                commit = Task.Run(async () =>
                {
                    await Task.Delay(1000, CancellationToken.None);
                    await transaction.CommitAsync(CancellationToken.None);
                    await transaction.DisposeAsync();
                    await writer.DisposeAsync();
                }, CancellationToken.None);
            });
            var result = await store.RunAgentActionAsync(WriteCall(request, claim), hooks, ct);
            await commit!;
            Assert.Equal(AgentActionCodes.ActionsOff, result.Refusal?.Reason);
            Assert.Equal(0, _writes);
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
        });
    }

    /* A person's route or tag edit made while a write is in its transaction waits for that write, and the next write reads it. The write does not
       lock the route and tag rows: every writer of them fires the version-bump trigger, which UPDATEs the service row the write holds. */
    [Fact]
    public async Task ARouteEditAndATagEdit_MadeDuringTheWrite_WaitForIt_ThenTheNextWriteSeesEach()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification_routes (route_id, metric_match, agent_level) OVERRIDING SYSTEM VALUE VALUES (4001, 'Blocking', 3)", ct);
            await ExecAsync(connection, "INSERT INTO config.server_tags (id, name) OVERRIDING SYSTEM VALUE VALUES (9101, 'prod')", ct);
            await ExecAsync(connection, "INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, 9101)", ct);
            var (request, claim) = await OpenAndClaimAsync(store, 3);

            /* The level the write reads: the route's own while the server holds its tag, 2 once it does not. */
            AgentActionHooks Hooks(Func<CancellationToken, Task>? duringWrite) => new()
            {
                LoadLevels = async (c, t, token) =>
                {
                    await using var read = new NpgsqlCommand(
                        "SELECT CASE WHEN EXISTS (SELECT 1 FROM config.server_tag_map WHERE server_id = 7) THEN agent_level ELSE 2 END FROM config.config_notification_routes WHERE route_id = 4001", c, t);
                    var level = Convert.ToInt32(await read.ExecuteScalarAsync(token), CultureInfo.InvariantCulture);
                    return (_, _, _) => level;
                },
                ReadBefore = (_, _, _) => Task.FromResult<string?>("{\"reason\":\"old\"}"),
                Write = async (_, _, token) =>
                {
                    _writes++;
                    if (duringWrite is not null)
                        await duringWrite(token);
                    return "{\"reason\":\"new\"}";
                },
            };

            async Task EditDuringAWriteAsync(string editSql, string restoreSql)
            {
                Task? edit = null;
                var during = await store.RunAgentActionAsync(WriteCall(request, claim), Hooks(async token =>
                {
                    /* The edit is the stores' own statement, started inside this write's transaction. Its version bump cannot get the service
                       row until the write ends. */
                    edit = EditAsync(source, editSql, ct);
                    await Task.Delay(1500, token);
                    Assert.False(edit.IsCompleted, "the edit finished while the write was still in its transaction");
                }), ct);
                Assert.True(during.Done, during.Refusal?.Message);
                await edit!;

                var next = await store.RunAgentActionAsync(WriteCall(request, claim), Hooks(null), ct);
                Assert.Equal(AgentActionCodes.NotLevel3, next.Refusal?.Reason);
                await ExecAsync(connection, restoreSql, ct);
            }

            await EditDuringAWriteAsync("UPDATE config.config_notification_routes SET agent_level = 2 WHERE route_id = 4001",
                "UPDATE config.config_notification_routes SET agent_level = 3 WHERE route_id = 4001");
            await EditDuringAWriteAsync("DELETE FROM config.server_tag_map WHERE server_id = 7",
                "INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, 9101)");
            Assert.Equal(2, _writes);
        });
    }

    /* A pause writes the service row too, on the command plane (5 seconds for its other writes). It waits for an agent change that holds the row
       for longer than that, and then succeeds. */
    [Fact]
    public async Task APause_WaitsForAnAgentChangeHoldingTheServiceRow_PastTheCommandPlanesFiveSeconds_ThenSucceeds()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            await using var holder = await source.OpenConnectionAsync(ct);
            await using var held = await holder.BeginTransactionAsync(ct);
            await using (var lockRow = new NpgsqlCommand("SELECT 1 FROM config.config_service WHERE id = 1 FOR SHARE", holder, held))
            {
                await lockRow.ExecuteScalarAsync(ct);
            }

            var release = Task.Run(async () =>
            {
                await Task.Delay(6500, CancellationToken.None);
                await held.CommitAsync(CancellationToken.None);
            }, CancellationToken.None);
            var plan = DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(1, "pause", null, null, null));
            await DarlingCommandExecutor.ExecuteStoreWriteAsync(source, plan, ct);
            await release;
            Assert.Equal("True", await ScalarAsync(connection, "SELECT paused FROM config.config_service WHERE id = 1", ct));
        });
    }

    private static async Task EditAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var editor = await source.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(sql, editor);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static AgentActionCall MuteCall(Guid request, Guid claim)
    {
        var call = Call(request, claim);
        return new AgentActionCall("create_mute_rule", AgentChangeKinds.Mute, call.Action, call.Target);
    }

    /* Two Level 2 store writes at once (create_mute_rule twice): each writes a row whose version-bump trigger UPDATEs the service row. The write
       reads that row for the whole transaction, so a share lock would leave both holding it and both waiting to update it (40P01). */
    [Fact]
    public async Task TwoStoreWritesAtOnce_QueueOnTheServiceRow_AndBothSucceed()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            var reached = 0;
            var bothReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            AgentActionHooks Hooks(string key) => new()
            {
                LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => 3),
                ReadBefore = (_, _, _) => Task.FromResult<string?>(null),
                Write = async (c, t, token) =>
                {
                    /* Hold here until the other write is also in its transaction (or two seconds pass, when it is queued behind this one). */
                    if (Interlocked.Increment(ref reached) == 2)
                        bothReached.TrySetResult();
                    await Task.WhenAny(bothReached.Task, Task.Delay(2000, token));
                    await using var insert = new NpgsqlCommand(
                        $"INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (7, '{key}', 1, 'agent')", c, t);
                    await insert.ExecuteNonQueryAsync(token);
                    return "{}";
                },
            };

            var a = store.RunAgentActionAsync(MuteCall(request, claim), Hooks("blocking.a"), ct);
            var b = store.RunAgentActionAsync(MuteCall(request, claim), Hooks("blocking.b"), ct);
            var results = await Task.WhenAll(a, b);
            Assert.All(results, r => Assert.True(r.Done, r.Refusal?.Message));
            Assert.Equal("2", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides WHERE server_id = 7", ct));
        });
    }

    /* A slow monitored server: the connect, the read and the write share one deadline, and its expiry is an unknown outcome (the called row
       stands with no result), with an answer for the caller instead of a bare cancellation. */
    [Fact]
    public async Task TheRemoteWork_HasOneDeadline_AndItsExpiryIsAnUnknownOutcome()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            var shortStore = new InvestigationRequestStore(source, 30, () => _now) { RemoteWorkDeadline = TimeSpan.FromMilliseconds(800) };
            var signal = new ServerChangeSignal();
            var hooks = new AgentActionHooks
            {
                ServerChange = signal,
                LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => 3),
                ReadBefore = (_, _, _) => Task.FromResult<string?>("{\"reason\":\"old\"}"),
                Write = async (_, _, token) =>
                {
                    /* A server that never answers: the connect or the read waits on the token and nothing else. */
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    return null;
                },
            };
            var run = shortStore.RunAgentActionAsync(WriteCall(request, claim), hooks, ct);
            var finished = await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(20), ct));
            Assert.Same(run, finished);
            var ex = await Assert.ThrowsAsync<TimeoutException>(() => run);
            Assert.Contains("unknown", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.True(signal.OutcomeUnknown);
            Assert.Equal("action_called", await EventsAsync(connection, request, ct));
        });
    }

    /* The operator's off switch waits for an agent change in progress and then succeeds, though its own command timeout (the Viewer's is about
       15 seconds; here one second) is shorter than the wait. */
    [Fact]
    public async Task TurningAgentChangesOff_WaitsForAWriteInProgress_ThenSucceeds()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, switchOn: true, "svc", "env:X", ct);
            await ExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = true", ct);
            var (request, claim) = await OpenAndClaimAsync(store, 3);
            var inTransaction = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var finishWrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var hooks = new AgentActionHooks
            {
                ServerChange = new ServerChangeSignal(),
                LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => 3),
                ReadBefore = (_, _, _) => Task.FromResult<string?>("{\"reason\":\"old\"}"),
                Write = async (_, _, token) =>
                {
                    inTransaction.SetResult();
                    await finishWrite.Task.WaitAsync(token);
                    return "{\"reason\":\"new\"}";
                },
            };
            var write = store.RunAgentActionAsync(WriteCall(request, claim), hooks, ct);
            await inTransaction.Task;

            var turnOff = new AgentChangeUndo(source, 1).SetEnabledAsync(false, "alice", ct);
            await Task.Delay(2500, ct);
            Assert.False(turnOff.IsCompleted, "the switch gave up (or went through) while the write was in its transaction");
            finishWrite.SetResult();
            await turnOff;
            Assert.True((await write).Done);
            Assert.Equal("False", await ScalarAsync(connection, "SELECT agent_actions_enabled FROM config.config_service WHERE id = 1", ct));
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
