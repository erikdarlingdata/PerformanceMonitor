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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5912: which arm offers a Level 2 action tool, pinned by behaviour per tool name. The action tool set is empty until
/// the mute, custom-rule and override tools add to it, so the arm is exercised through <c>Offers(name, set)</c> with a set
/// a later slice could ship.
/// </summary>
public sealed class AgentActionPolicyTests
{
    private static readonly DarlingMcpWritePolicy.Effective AgentArm = new(Writes: false, ServerAdmin: false, Investigations: true);
    private static readonly DarlingMcpWritePolicy.Effective WritesArm = new(Writes: true, ServerAdmin: false);
    private static readonly IReadOnlySet<string> ActionToolNames = new HashSet<string>(InvestigationRules.AgentActionToolNames, StringComparer.Ordinal);
    private static readonly IReadOnlySet<string> Actions = new HashSet<string>(StringComparer.Ordinal) { "create_mute_rule", "update_alert_settings", "edit_server" };

    [Theory]
    [InlineData("create_mute_rule")]
    public void AnActionTool_IsOfferedUnderTheAgentArm_AndTheWritesArm_AndNotWithBothOff(string tool)
    {
        Assert.True(AgentArm.Offers(tool, Actions));
        Assert.True(WritesArm.Offers(tool, Actions));
        Assert.False(DarlingMcpWritePolicy.Effective.Off.Offers(tool, Actions));
    }

    [Theory]
    [InlineData("update_alert_settings")]
    [InlineData("mute_analysis_finding")]
    [InlineData("unmute_analysis_finding")]
    [InlineData("undo_plan_change")]
    [InlineData("set_database_state_expected")]
    [InlineData("write_investigation_note")]
    [InlineData("approve_investigation_note")]
    [InlineData("reject_investigation_note")]
    [InlineData("close_investigation_request")]
    [InlineData("start_practice_request")]
    [InlineData("save_environment_profile")]
    [InlineData("record_investigation_feedback")]
    [InlineData("delete_notification_route")]
    [InlineData("set_notification_route_enabled")]
    [InlineData("set_notification_route_fallback")]
    [InlineData("create_custom_view")]
    [InlineData("delete_server_tag")]
    [InlineData("assign_server_tag")]
    [InlineData("edit_server")]
    [InlineData("add_servers")]
    public void ANeverAgentTool_IsNotOfferedUnderTheAgentArm_EvenWhenListed(string tool)
    {
        Assert.False(AgentArm.Offers(tool, Actions));
        Assert.False(AgentArm.Offers(tool));
    }

    [Theory]
    [InlineData("create_mute_rule")]
    [InlineData("update_custom_alert_rule")]
    [InlineData("delete_mute_rule")]
    [InlineData("update_alert_settings")]
    [InlineData("edit_server")]
    public void EveryWrite_IsOffUnderTheAgentArm_UnlessItIsALevel2ActionTool(string tool) =>
        Assert.Equal(ActionToolNames.Contains(tool), AgentArm.Offers(tool));

    [Theory]
    [InlineData("claim_investigation_request", true, false)]
    [InlineData("submit_investigation_report", true, false)]
    [InlineData("get_alert_history", true, true)]
    [InlineData("create_mute_rule", true, true)]
    [InlineData("update_alert_settings", false, true)]
    public void BothArms_OfferALevel2ActionToolUnderTheAgentArm_AndEveryOtherWriteOnlyUnderTheWritesArm(string tool, bool agentArm, bool writesArm)
    {
        Assert.Equal(agentArm, AgentArm.Offers(tool));
        Assert.Equal(writesArm, WritesArm.Offers(tool));
    }
}

/// <summary>#5912, check 1: the <c>agent_action</c> argument, its fields and limits.</summary>
public sealed class AgentActionArgumentTests
{
    internal static string Arg(string? requestId = null, string? claimId = null, string agentId = "agent-a", string reason = "why",
        string evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"12 blocked\"}]") =>
        new JsonObject
        {
            ["request_id"] = requestId ?? Guid.NewGuid().ToString(),
            ["claim_id"] = claimId ?? Guid.NewGuid().ToString(),
            ["agent_id"] = agentId,
            ["reason"] = reason,
            ["evidence"] = JsonNode.Parse(evidence),
        }.ToJsonString();

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("null")]
    public void Missing_IsRequired(string? arg)
    {
        Assert.Equal(AgentActionCodes.Required, InvestigationToolCore.ParseAgentAction(arg, out var action)!.Reason);
        Assert.Null(action);
    }

    [Fact]
    public void EachBadField_IsInvalid_AndTheMessageNamesIt()
    {
        void Bad(string arg, string field)
        {
            var refusal = InvestigationToolCore.ParseAgentAction(arg, out _)!;
            Assert.Equal(AgentActionCodes.Invalid, refusal.Reason);
            var lead = field == "agent_action" ? "agent_action: " : "agent_action." + field + ": ";
            Assert.StartsWith(lead, refusal.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(field + ": " + field + ":", refusal.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("agent_action.agent_action", refusal.Message, StringComparison.Ordinal);
        }

        Bad("[1]", "agent_action");
        Bad(Arg(requestId: "x"), "request_id");
        Bad(Arg(claimId: "x"), "claim_id");
        Bad(Arg(agentId: "bad id"), "agent_id");
        Bad(Arg(reason: ""), "reason");
        Bad(Arg(reason: new string('r', InvestigationToolCore.MaxActionReasonChars + 1)), "reason");
        Bad(Arg(evidence: "[]"), "evidence");
        Bad(Arg(evidence: "[{\"tool\":\"t\",\"arguments\":{},\"key_numbers\":\"" + new string('k', 1001) + "\"}]"), "evidence");
        /* The cap text reads "5 items, over 4", once: the field is named by the lead, not again by the cap. */
        var tooMany = "[" + string.Join(",", Enumerable.Repeat("{\"tool\":\"t\",\"arguments\":{},\"key_numbers\":\"1\"}", InvestigationToolCore.MaxEvidenceItems + 1)) + "]";
        Bad(Arg(evidence: tooMany), "evidence");
        Assert.Contains($"{InvestigationToolCore.MaxEvidenceItems + 1} items, over {InvestigationToolCore.MaxEvidenceItems}.",
            InvestigationToolCore.ParseAgentAction(Arg(evidence: tooMany), out _)!.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AValidArgument_Parses_WithTheReasonAtItsLimit()
    {
        var requestId = Guid.NewGuid();
        Assert.Null(InvestigationToolCore.ParseAgentAction(
            Arg(requestId: requestId.ToString(), reason: new string('r', InvestigationToolCore.MaxActionReasonChars)), out var action));
        Assert.Equal(requestId, action!.RequestId);
        Assert.Equal("agent-a", action.AgentId);
    }
}

/// <summary>#5912: the one journal masking helper, the MCP read and the web read; and the Level 2 route level.</summary>
public sealed class AgentActionJournalMaskingTests
{
    private const string Detail = "{\"tool\":\"create_mute_rule\",\"kind\":\"mute\",\"target\":{\"id\":\"9\",\"server_id\":7,\"database\":null},"
        + "\"claim_no\":1,\"evidence\":[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"WHERE id = 4711\"}],"
        + "\"before\":null,\"after\":{\"reason\":\"mute WHERE id = 4711\",\"query_text_pattern\":\"SELECT * FROM t WHERE x = 4711\",\"enabled\":true}}";

    [Fact]
    public void Full_ReturnsTheDetailAsStored()
    {
        var node = InvestigationFieldMasking.JournalDetail(Detail, McpDataDetailMasker.ForAnswer(McpDataDetail.Full))!;
        Assert.Contains("4711", node.ToJsonString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(McpDataDetail.MaskLiterals)]
    [InlineData(McpDataDetail.MetadataOnly)]
    public void ImageTextAndEvidence_AreMasked_AndTheStructureKept(McpDataDetail mode)
    {
        var node = InvestigationFieldMasking.JournalDetail(Detail, McpDataDetailMasker.ForAnswer(mode))!.AsObject();
        Assert.DoesNotContain("4711", node.ToJsonString(), StringComparison.Ordinal);
        Assert.Equal("create_mute_rule", (string?)node["tool"]);
        Assert.Equal(1, (int?)node["claim_no"]);
        Assert.Equal(true, (bool?)node["after"]!["enabled"]);
        if (mode == McpDataDetail.MaskLiterals)
            Assert.Contains("SELECT", (string?)node["after"]!["query_text_pattern"], StringComparison.Ordinal);
        else
            Assert.DoesNotContain("SELECT", node["after"]!["query_text_pattern"]?.ToJsonString() ?? "", StringComparison.Ordinal);
    }

    [Fact]
    public void ResolveAgent_AnswersLevelTwo_AndAnOffRouteStillStopsTheLookup()
    {
        var act = new NotificationRoute(2, "Blocking", "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelAct };
        Assert.Equal(2, NotificationRouter.ResolveAgent("Blocking", new[] { act })!.AgentLevel);
        var off = new NotificationRoute(1, "Blocking", "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelOff };
        Assert.Null(NotificationRouter.ResolveAgent("Blocking", new[] { off, act }));
    }
}

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5912: the gate against a live store. Each refusal code refuses, changes nothing and journals at most one refused row
/// an hour; reported, released and lapsed claims and another agent id refuse; no row holds the claim id; a pass journals
/// action_called then action_done; the request stamps the lowest level among its routes, and Level 1 for an alert of a
/// custom rule an agent last wrote.
/// </summary>
public sealed class AgentActionGateLiveTests
{
    private static readonly DateTime T0 = new(2026, 10, 16, 12, 0, 0, DateTimeKind.Utc);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private DateTime _now = T0;
    private int _writes;

    private static InvestigationAlert Alert(DateTime at, int level = 2, string metric = "Blocking", int serverId = 7) =>
        new(1, serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), "blocking", metric, at, Array.Empty<string>(), null,
            20, 100, LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = level };

    private AgentActionHooks Hooks(int? resolved = 2, AgentActionRefusal? kind = null) => new()
    {
        LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => resolved),
        KindRule = kind is null ? null : (_, _, _, _) => Task.FromResult<AgentActionRefusal?>(kind),
        ReadBefore = (_, _, _) => Task.FromResult<string?>("{\"reason\":\"old\"}"),
        Write = (_, _, _) => { _writes++; return Task.FromResult<string?>("{\"reason\":\"new\"}"); },
    };

    private static AgentActionCall Call(Guid requestId, Guid claimId, string agentId = "agent-a", int serverId = 7) =>
        new("create_mute_rule", "mute", new AgentAction(requestId, claimId, agentId, "the chain is a known batch",
            "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"12\"}]"),
            new AgentActionTarget(serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), null, null));

    private async Task<(Guid Request, Guid Claim)> OpenAndClaimAsync(InvestigationRequestStore store, int level = 2)
    {
        var ct = TestContext.Current.CancellationToken;
        var opened = await store.OpenOrJoinAsync(Alert(_now, level), ct);
        var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
        return (opened.RequestId!.Value, claim.ClaimId!.Value);
    }

    [Fact]
    public async Task ALiveClaim_Passes_JournalsCalledThenDone_AndNoRowHoldsTheClaimId()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            var result = await store.RunAgentActionAsync(Call(request, claim), Hooks(), ct);
            Assert.True(result.Done);
            Assert.Equal(1, _writes);
            Assert.Equal("action_called|action_done", await ScalarAsync(connection,
                $"SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{request}' AND event LIKE 'action_%'", ct));
            Assert.Equal("agent:agent-a", await ScalarAsync(connection,
                $"SELECT DISTINCT actor FROM config.investigation_journal WHERE request_id = '{request}' AND event LIKE 'action_%'", ct));
            Assert.Equal("0", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE coalesce(detail::text, '') || coalesce(reason, '') LIKE '%{claim}%'", ct));
            Assert.Equal(result.CalledId!.Value.ToString(CultureInfo.InvariantCulture), await ScalarAsync(connection,
                $"SELECT detail->>'called_id' FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'action_done'", ct));
        });
    }

    [Fact]
    public async Task EachCode_Refuses_ChangesNothing_AndJournalsOneRefusedRowAnHour()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            async Task Refuses(string code, Func<Task<AgentActionResult>> run, Guid request)
            {
                var before = await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused'", ct);
                for (var i = 0; i < 3; i++)
                {
                    var result = await run();
                    Assert.Equal(code, result.Refusal?.Reason);
                }

                Assert.Equal(0, _writes);
                Assert.Equal("1", await ScalarAsync(connection,
                    $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{code}' AND (request_id = '{request}' OR request_id IS NULL)", ct));
                Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", ct));
                Assert.NotEqual(before, await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused'", ct));
            }

            var (request, claim) = await OpenAndClaimAsync(store);

            await Refuses(AgentActionCodes.ClaimNotLive, () => store.RunAgentActionAsync(Call(request, Guid.NewGuid()), Hooks(), ct), request);
            await Refuses(AgentActionCodes.ClaimNotLive, () => store.RunAgentActionAsync(Call(Guid.NewGuid(), claim), Hooks(), ct), Guid.Empty);
            await Refuses(AgentActionCodes.NotLevel2, () => store.RunAgentActionAsync(Call(request, claim), Hooks(resolved: 1), ct), request);
            await Refuses(AgentActionCodes.OtherServer, () => store.RunAgentActionAsync(Call(request, claim, serverId: 8), Hooks(), ct), request);
            await Refuses("kind_rule", () => store.RunAgentActionAsync(Call(request, claim), Hooks(kind: new("kind_rule", "no")), ct), request);

            await ExecAsync(connection, $"UPDATE config.investigation_requests SET practice_of = gen_random_uuid() WHERE request_id = '{request}'", ct);
            await Refuses(AgentActionCodes.Practice, () => store.RunAgentActionAsync(Call(request, claim), Hooks(), ct), request);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET practice_of = NULL WHERE request_id = '{request}'", ct);

            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = false", ct);
            await Refuses(AgentActionCodes.ActionsOff, () => store.RunAgentActionAsync(Call(request, claim), Hooks(), ct), request);
            await ExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = true", ct);

            /* The next hour journals the refusal again, once. */
            _now = T0.AddMinutes(61);
            await store.RecordAgentActionRefusalAsync(request, "agent-a", "create_mute_rule", AgentActionCodes.Invalid, null, ct);
            await store.RecordAgentActionRefusalAsync(request, "agent-a", "create_mute_rule", AgentActionCodes.Invalid, null, ct);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{AgentActionCodes.Invalid}'", ct));
        });
    }

    [Fact]
    public async Task ALevelOneRequest_RefusesNotLevel2()
    {
        await RunAsync(async (store, _, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store, level: 1);
            Assert.Equal(AgentActionCodes.NotLevel2, (await store.RunAgentActionAsync(Call(request, claim), Hooks(), ct)).Refusal?.Reason);
        });
    }

    [Theory]
    [InlineData("released")]
    [InlineData("reported")]
    [InlineData("lapsed")]
    [InlineData("other_agent")]
    public async Task ADeadClaim_OrAnotherAgent_RefusesClaimNotLive_WithTheOneMessage(string how)
    {
        await RunAsync(async (store, _, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            var agent = "agent-a";
            switch (how)
            {
                case "released":
                    Assert.True(await store.ReleaseAsync(request, claim, ct));
                    break;
                case "reported":
                    await store.SubmitAsync(new InvestigationReportSubmission(request, claim, "agent-a", 1, "real", "cause", true, "summary",
                        "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"1\"}]"), ct);
                    break;
                case "lapsed":
                    _now = T0.AddMinutes(16);
                    break;
                default:
                    agent = "agent-b";
                    break;
            }

            var refusal = (await store.RunAgentActionAsync(Call(request, claim, agent), Hooks(), ct)).Refusal!;
            Assert.Equal(AgentActionCodes.ClaimNotLive, refusal.Reason);
            Assert.Equal(AgentActionCodes.ClaimNotLiveMessage, refusal.Message);
            Assert.DoesNotContain(claim.ToString(), refusal.Message, StringComparison.Ordinal);
            Assert.Equal(0, _writes);
        });
    }

    [Fact]
    public async Task TheWrite_ReChecksTheClaim_AndJournalsActionFailed()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            var calls = 0;
            var hooks = new AgentActionHooks
            {
                /* Live in the gate's transaction, lowered before the write's re-check. */
                LoadLevels = (_, _, _) =>
                {
                    var level = ++calls == 1 ? 2 : 1;
                    return Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => level);
                },
                ReadBefore = (_, _, _) => Task.FromResult<string?>(null),
                Write = (_, _, _) => { _writes++; return Task.FromResult<string?>(null); },
            };
            var result = await store.RunAgentActionAsync(Call(request, claim), hooks, ct);
            Assert.Equal(AgentActionCodes.NotLevel2, result.Refusal?.Reason);
            Assert.NotNull(result.CalledId);
            Assert.Equal(0, _writes);
            Assert.Equal("action_called|action_failed", await ScalarAsync(connection,
                $"SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{request}' AND event LIKE 'action_%'", ct));
        });
    }

    [Fact]
    public async Task TheWrite_ReChecksTheAgentActionsSwitch_RefusesActionsOff_JournalsActionFailed_AndNothingReachesTheServer()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = true", ct);
            var hooks = new AgentActionHooks
            {
                LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => 2),
                /* Runs in the gate's transaction after the switch check passed and before action_called commits: a person turns the switch
                   off now, so only the write's re-check can see it. */
                ReadBefore = async (_, _, token) =>
                {
                    await ExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = false", token);
                    return "{\"reason\":\"old\"}";
                },
                Write = (_, _, _) => { _writes++; return Task.FromResult<string?>("{\"reason\":\"new\"}"); },
            };
            var result = await store.RunAgentActionAsync(Call(request, claim), hooks, ct);
            Assert.Equal(AgentActionCodes.ActionsOff, result.Refusal?.Reason);
            Assert.NotNull(result.CalledId);
            Assert.Equal(0, _writes);
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
            Assert.Equal(AgentActionCodes.ActionsOff, await ScalarAsync(connection,
                $"SELECT detail->>'error' FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'action_failed'", ct));
        });
    }

    [Fact]
    public async Task AFullAgentJournal_RefusesJournalCapped()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            await ExecAsync(connection, $@"INSERT INTO config.investigation_journal (request_id, event, actor)
                SELECT '{request}', 'filler', 'agent:agent-a' FROM generate_series(1, {InvestigationRules.MaxAgentJournalRows - 2})", ct);
            Assert.Equal(AgentActionCodes.JournalCapped, (await store.RunAgentActionAsync(Call(request, claim), Hooks(), ct)).Refusal?.Reason);
            Assert.Equal(0, _writes);
        });
    }

    [Fact]
    public async Task TheLowestLevelWins_OnAJoin_AndAnAgentWrittenRuleOpensLevelOne()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var opened = await store.OpenOrJoinAsync(Alert(T0, level: 2), ct);
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '{opened.RequestId}'", ct));
            _now = T0.AddMinutes(5);
            var joined = await store.OpenOrJoinAsync(Alert(_now, level: 1, metric: "Blocking Detected"), ct);
            Assert.Equal(opened.RequestId, joined.RequestId);
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '{opened.RequestId}'", ct));

            var ruleId = await ScalarAsync(connection,
                "INSERT INTO config.custom_alert_rules (name, definition, updated_by) VALUES ('r', '{}'::jsonb, 'agent:agent-a') RETURNING id", ct);
            var byAgent = await store.OpenOrJoinAsync(Alert(_now, level: 2, metric: "Custom:" + ruleId, serverId: 9), ct);
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '{byAgent.RequestId}'", ct));

            await ExecAsync(connection, $"UPDATE config.custom_alert_rules SET updated_by = 'person:erik' WHERE id = {ruleId}", ct);
            var byPerson = await store.OpenOrJoinAsync(Alert(_now, level: 2, metric: "Custom:" + ruleId, serverId: 10), ct);
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '{byPerson.RequestId}'", ct));
        });
    }

    [Fact]
    public async Task ARouteDeletedAfterOpen_LowersTheRequestToNothing_AndRefusesNotLevel2()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', 2)", ct);
            var (request, claim) = await OpenAndClaimAsync(store);
            var arg = AgentActionArgumentTests.Arg(request.ToString(), claim.ToString());
            var agentArm = new DarlingMcpWritePolicy.Effective(false, false, Investigations: true);
            await using var dataSource = NpgsqlDataSource.Create(connection.ConnectionString);

            Task<AgentActionGateOutcome> Run() => DarlingAgentActionGate.RunAsync(
                agentArm, dataSource, store, NullLogger.Instance, "create_mute_rule", "mute", arg, new AgentActionTarget(7, "srv-7", null, null), null,
                (_, _, _) => Task.FromResult<string?>(null), (_, _, _) => { _writes++; return Task.FromResult<string?>("{}"); }, ct);

            /* The stamp on the request still says 2; the route that sent it there is gone. No route means level 0, not "unchanged". */
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT autonomy_level FROM config.investigation_requests WHERE request_id = '{request}'", ct));
            await ExecAsync(connection, "DELETE FROM config.config_notification_routes", ct);
            var refused = await Run();
            Assert.Equal(AgentActionCodes.NotLevel2, refused.Refusal?.Reason);
            Assert.Null(refused.CalledId);
            Assert.Equal(0, _writes);
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{request}' AND event LIKE 'action_%'", ct));
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'refused'", ct));
        });
    }

    [Fact]
    public async Task TheRoutesAreReadOncePerGatePass_NotOncePerMetric()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            _now = T0.AddMinutes(5);
            var joined = await store.OpenOrJoinAsync(Alert(_now, level: 2, metric: "Blocking Detected"), ct);
            Assert.Equal(request, joined.RequestId);
            var loads = 0;
            var seen = new List<string>();
            var hooks = new AgentActionHooks
            {
                LoadLevels = (_, _, _) =>
                {
                    loads++;
                    return Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((metric, _, _) => { seen.Add(metric); return 2; });
                },
                ReadBefore = (_, _, _) => Task.FromResult<string?>(null),
                Write = (_, _, _) => { _writes++; return Task.FromResult<string?>(null); },
            };
            Assert.True((await store.RunAgentActionAsync(Call(request, claim), hooks, ct)).Done);

            /* Two metrics, two passes (the gate's transaction and the write's re-check): two reads, four resolutions. */
            Assert.Equal(2, loads);
            Assert.Equal(4, seen.Count);
            Assert.Equal(2, seen.Distinct().Count());
        });
    }

    [Fact]
    public async Task TheServiceGate_ReResolvesOverTheStoredRoutes_AndIgnoresTheArgumentUnderTheWritesArm()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', 2)", ct);
            var (request, claim) = await OpenAndClaimAsync(store);
            var arg = AgentActionArgumentTests.Arg(request.ToString(), claim.ToString());
            var agentArm = new DarlingMcpWritePolicy.Effective(false, false, Investigations: true);
            var target = new AgentActionTarget(7, "srv-7", null, null);
            await using var dataSource = NpgsqlDataSource.Create(connection.ConnectionString);

            Task<AgentActionGateOutcome> Run(DarlingMcpWritePolicy.Effective policy, string? a) => DarlingAgentActionGate.RunAsync(
                policy, dataSource, store, NullLogger.Instance, "create_mute_rule", "mute", a, target, null,
                (_, _, _) => Task.FromResult<string?>(null), (_, _, _) => { _writes++; return Task.FromResult<string?>("{}"); }, ct);

            Assert.True((await Run(new DarlingMcpWritePolicy.Effective(true, false), null)).IsUngated);
            Assert.Equal(AgentActionCodes.Required, (await Run(agentArm, null)).Refusal?.Reason);
            Assert.NotNull((await Run(agentArm, arg)).CalledId);
            Assert.Equal(1, _writes);

            await ExecAsync(connection, "UPDATE config.config_notification_routes SET agent_level = 1", ct);
            Assert.Equal(AgentActionCodes.NotLevel2, (await Run(agentArm, arg)).Refusal?.Reason);
            Assert.Contains("\"status\":\"refused\"", DarlingAgentActionGate.RefusalAnswer(new("x", "y")), StringComparison.Ordinal);
        });
    }

    /* #5632, step 13. The store's commit is broken by a DEFERRED constraint trigger: it raises at COMMIT for an action_done row
       that is not marked recorded_late, so the insert inside the write's transaction succeeds and only the commit fails, which is
       the window the step closes. The late row is written outside a transaction block, so it passes. The "late write fails too" case
       uses an immediate trigger on every action_done row, so the normal insert and both late attempts are refused. */
    private const string FailCommitSql = @"
CREATE FUNCTION config.test_fail_done() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    IF NEW.event = 'action_done' AND NEW.detail->>'recorded_late' IS NULL THEN
        RAISE EXCEPTION 'commit refused by test';
    END IF;
    RETURN NULL;
END $f$;
CREATE CONSTRAINT TRIGGER test_fail_done AFTER INSERT ON config.investigation_journal
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION config.test_fail_done();";

    private const string FailEveryDoneSql = @"
CREATE FUNCTION config.test_fail_every_done() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    RAISE EXCEPTION 'store refused by test';
END $f$;
CREATE TRIGGER test_fail_every_done BEFORE INSERT ON config.investigation_journal
    FOR EACH ROW WHEN (NEW.event = 'action_done') EXECUTE FUNCTION config.test_fail_every_done();";

    private AgentActionHooks WriteHooks(ServerChangeSignal? signal, bool mark, Func<CancellationToken, Task>? then = null) => new()
    {
        ServerChange = signal,
        LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => 2),
        ReadBefore = (_, _, _) => Task.FromResult<string?>("{\"state\":\"old\"}"),
        Write = async (_, _, token) =>
        {
            _writes++;
            if (mark)
                signal!.MarkServerChanged();
            if (then is not null)
                await then(token);
            return "{\"state\":\"new\"}";
        },
    };

    private static Task<string?> EventsAsync(NpgsqlConnection connection, Guid request, CancellationToken ct) => ScalarAsync(connection,
        $"SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{request}' AND event LIKE 'action_%'", ct);

    private async Task<AgentChangeRow> ListedAsync(NpgsqlConnection connection, long calledId, CancellationToken ct)
    {
        await using var dataSource = NpgsqlDataSource.Create(connection.ConnectionString);
        return (await new AgentChangeUndo(dataSource, 30, () => _now).ReadAsync(50, cancellationToken: ct)).Changes.Single(c => c.CalledId == calledId);
    }

    [Fact]
    public async Task AServerChange_WhoseCommitFails_IsJournaledDoneAndLate_NeverFailed()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ExecAsync(connection, FailCommitSql, ct);
            var (request, claim) = await OpenAndClaimAsync(store);
            var result = await store.RunAgentActionAsync(Call(request, claim), WriteHooks(new ServerChangeSignal(), mark: true), ct);

            Assert.True(result.Done);
            Assert.True(result.RecordedLate);
            Assert.False(result.JournalMissing);
            Assert.Equal("{\"state\":\"new\"}", result.AfterJson);
            Assert.Equal(1, _writes);
            Assert.Equal("action_called|action_done", await EventsAsync(connection, request, ct));
            Assert.Equal("true", await ScalarAsync(connection,
                $"SELECT detail->>'recorded_late' FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'action_done'", ct));
            Assert.Equal(result.CalledId!.Value.ToString(CultureInfo.InvariantCulture), await ScalarAsync(connection,
                $"SELECT detail->>'called_id' FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'action_done'", ct));
            Assert.Equal("new", await ScalarAsync(connection,
                $"SELECT detail->'after'->>'state' FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'action_done'", ct));
            Assert.Contains("commit refused by test", await ScalarAsync(connection,
                $"SELECT detail->>'error' FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'action_done'", ct), StringComparison.Ordinal);
            Assert.Equal(AgentChangeResult.Done, (await ListedAsync(connection, result.CalledId.Value, ct)).Result);
        });
    }

    [Fact]
    public async Task AWriteThatFailsBeforeTheServerAccepted_IsStillJournaledFailed_AndThrows()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            var signal = new ServerChangeSignal();
            var hooks = WriteHooks(signal, mark: false, _ => throw new InvalidOperationException("executor said it did not write"));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunAgentActionAsync(Call(request, claim), hooks, ct));
            Assert.Contains("did not write", ex.Message, StringComparison.Ordinal);
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task AnUnmarkedWrite_WhoseCommitFails_IsJournaledFailed_AsBefore()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ExecAsync(connection, FailCommitSql, ct);
            var (request, claim) = await OpenAndClaimAsync(store);
            await Assert.ThrowsAsync<PostgresException>(() => store.RunAgentActionAsync(Call(request, claim), WriteHooks(new ServerChangeSignal(), mark: false), ct));
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task AStoreOnlyHook_WhoseCommitFails_IsJournaledFailed_BecauseItsChangeRolledBack()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ExecAsync(connection, FailCommitSql, ct);
            var (request, claim) = await OpenAndClaimAsync(store);
            /* No signal at all: the mute, signature merge and threshold override hooks. */
            await Assert.ThrowsAsync<PostgresException>(() => store.RunAgentActionAsync(Call(request, claim), WriteHooks(null, mark: false), ct));
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task AServerChange_ThenACancellation_IsRecordedDone_AndTheCancellationPropagates()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var hooks = WriteHooks(new ServerChangeSignal(), mark: true, token =>
            {
                cts.Cancel();
                token.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            });
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.RunAgentActionAsync(Call(request, claim), hooks, cts.Token));
            Assert.Equal("action_called|action_done", await EventsAsync(connection, request, ct));
            Assert.Equal("true", await ScalarAsync(connection,
                $"SELECT detail->>'recorded_late' FROM config.investigation_journal WHERE request_id = '{request}' AND event = 'action_done'", ct));
        });
    }

    [Fact]
    public async Task ACancellationBeforeTheServerAccepted_IsNotRecordedAsDone()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var (request, claim) = await OpenAndClaimAsync(store);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var hooks = WriteHooks(new ServerChangeSignal(), mark: false, token =>
            {
                cts.Cancel();
                token.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            });
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.RunAgentActionAsync(Call(request, claim), hooks, cts.Token));
            Assert.Equal("action_called", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task AServerChange_WhoseStoreCannotBeWrittenAtAll_HasNoResultRow_AndTheListReadsUnknown()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            await ExecAsync(connection, FailEveryDoneSql, ct);
            var (request, claim) = await OpenAndClaimAsync(store);
            var result = await store.RunAgentActionAsync(Call(request, claim), WriteHooks(new ServerChangeSignal(), mark: true), ct);

            Assert.True(result.JournalMissing);
            Assert.False(result.RecordedLate);
            Assert.Equal("action_called", await EventsAsync(connection, request, ct));
            Assert.Equal(AgentChangeResult.Unknown, (await ListedAsync(connection, result.CalledId!.Value, ct)).Result);
        });
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the agent action gate live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => _now);
        await body(store, connection, ct);
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
