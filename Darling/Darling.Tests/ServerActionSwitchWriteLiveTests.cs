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
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6269: a person (or an MCP client under <c>mcp.allowServerAdmin</c>) turns a server's action switch on and off. One service
/// method writes <c>config.config_server_actions</c> as the store owner and journals the change; the Viewer command, the web route and
/// the MCP tool each reach it. The grants on the table are untouched, so the tests also show the admin, viewer and mcp roles still
/// cannot write it themselves.
/// </summary>
public sealed class ServerActionSwitchWriteLiveTests
{
    private const string RolePassword = "switch-write-live-pw";
    private const int SqlServerId = 1;
    private const int PostgresId = 2;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheService_TurnsASwitchOnThenOff_JournalingEachChangeWithThePersonSurfaceAndBeforeAfter_AndAnUnchangedCallWritesNothing()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var on = await ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", true, "person:alice", ServerActionSurfaces.Viewer, ct);
            Assert.True(on.Succeeded);
            Assert.Equal("set", on.Status);
            Assert.Equal("True", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));
            Assert.Equal("person:alice", await ScalarAsync(owner, "SELECT set_by FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));

            var again = await ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", true, "person:alice", ServerActionSurfaces.Viewer, ct);
            Assert.Equal("unchanged", again.Status);
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));

            var off = await ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", false, "person:bob", ServerActionSurfaces.Web, ct);
            Assert.Equal("set", off.Status);
            Assert.Equal("False", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));

            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched' AND actor NOT LIKE 'person:%'", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", ct));
            var first = await ScalarAsync(owner, "SELECT detail::text FROM config.investigation_journal WHERE actor = 'person:alice'", ct);
            var detail = JsonNode.Parse(first!)!;
            Assert.Equal("force_plan", (string)detail["action"]!);
            Assert.Equal("viewer", (string)detail["surface"]!);
            Assert.Equal("off", (string)detail["before"]!);
            Assert.Equal("on", (string)detail["after"]!);
            var second = JsonNode.Parse((await ScalarAsync(owner, "SELECT detail::text FROM config.investigation_journal WHERE actor = 'person:bob'", ct))!)!;
            Assert.Equal("web", (string)second["surface"]!);
            Assert.Equal("on", (string)second["before"]!);
            Assert.Equal("off", (string)second["after"]!);
        });
    }

    [Fact]
    public async Task ADryRunRefusedForWantOfTheClaim_ReadsTheSwitchWithoutOne_AndSaysOnOrOff()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            /* #6455: no claim, no agent_action. The plan tools read the switch for a refused dry run through ServerActionSwitches.ListAsync. */
            var off = await DarlingMcpPlanForceTools.ReadSwitchStateAsync(owner, null, SqlServerId, "srv-1", "force_plan", ct);
            Assert.Contains("is currently off", off, StringComparison.Ordinal);
            /* The per-server switch answers server_action_off; actions_off is the service-wide switch. */
            Assert.Contains($"refused as {AgentActionCodes.SwitchOff} ", off, StringComparison.Ordinal);
            Assert.DoesNotContain(AgentActionCodes.ActionsOff, off, StringComparison.Ordinal);

            await ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", true, "person:alice", ServerActionSurfaces.Viewer, ct);
            var on = await DarlingMcpPlanForceTools.ReadSwitchStateAsync(owner, null, SqlServerId, "srv-1", "force_plan", ct);
            Assert.Contains("is currently on.", on, StringComparison.Ordinal);

            /* An unknown server has no switch to describe, so the refusal goes out without the note. */
            Assert.Null(await DarlingMcpPlanForceTools.ReadSwitchStateAsync(owner, null, 9999, "nowhere", "force_plan", ct));
        });
    }

    [Fact]
    public async Task TheService_RefusesAnUnknownAction_AnActionTheEngineCannotRun_AndAnUnknownServer_WritingAndJournalingNothing()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var unknown = await ServerActionSwitches.SetAsync(owner, SqlServerId, "drop_database", true, "person:alice", ServerActionSurfaces.Viewer, ct);
            Assert.Equal("unknown_action", unknown.Code);
            Assert.Equal("unknown_action", (await ServerActionSwitches.SetAsync(owner, SqlServerId, null, true, "person:alice", ServerActionSurfaces.Viewer, ct)).Code);

            var wrongEngine = await ServerActionSwitches.SetAsync(owner, PostgresId, "force_plan", true, "person:alice", ServerActionSurfaces.Viewer, ct);
            Assert.Equal("not_for_engine", wrongEngine.Code);
            Assert.Equal("not_for_engine", (await ServerActionSwitches.SetAsync(owner, PostgresId, "evict_plan", true, "person:alice", ServerActionSurfaces.Viewer, ct)).Code);

            Assert.Equal("unknown_server", (await ServerActionSwitches.SetAsync(owner, 404, "force_plan", true, "person:alice", ServerActionSurfaces.Viewer, ct)).Code);

            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));

            /* A session kill runs on PostgreSQL too. */
            Assert.Equal("set", (await ServerActionSwitches.SetAsync(owner, PostgresId, "kill_session", true, "person:alice", ServerActionSurfaces.Viewer, ct)).Status);

            var list = await ServerActionSwitches.ListAsync(owner, ct);
            var pg = list.Single(s => s.ServerId == PostgresId);
            Assert.False(pg.Actions.Single(a => a.Action == "force_plan").Valid);
            Assert.True(pg.Actions.Single(a => a.Action == "kill_session").Enabled);
            Assert.DoesNotContain(list.Single(s => s.ServerId == SqlServerId).Actions, a => a.Enabled);
        });
    }

    [Fact]
    public async Task TheGate_ReadsTheNewState_OnTheNextAction_RefusedOff_PassedOn_RefusedOffAgain()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            await ExecAsync(owner, "UPDATE config.config_monitored_servers SET remediation_username = 'svc_fix', remediation_encrypted_password = 'env:FIX_PASSWORD' WHERE server_id = 1", ct);
            var now = new DateTime(2026, 10, 21, 12, 0, 0, DateTimeKind.Utc);
            var store = new InvestigationRequestStore(owner, 30, () => now);
            var alert = new InvestigationAlert(1, SqlServerId, "alpha-sql-01", "blocking", "Blocking", now, Array.Empty<string>(), null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 3 };
            var opened = await store.OpenOrJoinAsync(alert, ct);
            var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
            var call = new ServerActionCall("force_query_store_plan", ServerActionNames.ForcePlan,
                new AgentAction(opened.RequestId!.Value, claim.ClaimId!.Value, "agent-a", "the plan regressed", "[{\"tool\":\"get_query_store_top\",\"arguments\":{},\"key_numbers\":\"12\"}]"),
                new AgentActionTarget(SqlServerId, "alpha-sql-01", null, null));
            var hooks = new ServerActionHooks
            {
                LoadRoutes = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, PerformanceMonitor.Notifications.NotificationRoute?>>((_, _, _) => TestRouteHooks.AtLevel(3)),
                ReadCredential = (serverId, token) => store.ReadServerCredentialStateAsync(owner, serverId, token),
            };

            var realRefusal = await store.CheckServerActionAsync(call, hooks, ct);
            Assert.Equal(AgentActionCodes.SwitchOff, realRefusal?.Reason);

            /* #6455: the code a dry run predicts is the code the real call gets for this server. */
            var predicted = await DarlingMcpPlanForceTools.ReadSwitchStateAsync(owner, null, SqlServerId, "alpha-sql-01", ServerActionNames.ForcePlan, ct);
            Assert.Contains($"refused as {realRefusal!.Reason} ", predicted, StringComparison.Ordinal);

            await ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", true, "person:alice", ServerActionSurfaces.Viewer, ct);
            Assert.Null(await store.CheckServerActionAsync(call, hooks, ct));
            await ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", false, "person:alice", ServerActionSurfaces.Viewer, ct);
            Assert.Equal(AgentActionCodes.SwitchOff, (await store.CheckServerActionAsync(call, hooks, ct))?.Reason);
        });
    }

    [Fact]
    public async Task TheViewerCommand_SetsTheSwitchAsThePersonInRequestedBy_AndRefusesABadLoginABadBodyAndAnUnknownAction()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var executor = new DarlingCommandExecutor(owner, new NoCommandHost(), "test-instance", null);

            var good = await RunCommandAsync(owner, executor, SqlServerId, "{\"action\":\"kill_session\",\"enabled\":true}", "alice", ct);
            Assert.Equal("succeeded", good.Status);
            Assert.Equal("set", (string)JsonNode.Parse(good.Result!)!["status"]!);
            Assert.Equal("True", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'kill_session'", ct));
            Assert.Equal("person:alice", await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
            Assert.Equal("viewer", await ScalarAsync(owner, "SELECT detail->>'surface' FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));

            var off = await RunCommandAsync(owner, executor, SqlServerId, "{\"action\":\"kill_session\",\"enabled\":false}", "alice", ct);
            Assert.Equal("succeeded", off.Status);
            Assert.Equal("False", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'kill_session'", ct));
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));

            /* Refused: a name the journal's person rule refuses, a non-boolean, no action, no server, an unknown action, a wrong engine. */
            foreach (var (server, args, by) in new (int?, string, string)[]
            {
                (SqlServerId, "{\"action\":\"force_plan\",\"enabled\":true}", "bad;name,with\"punctuation"),
                (SqlServerId, "{\"action\":\"force_plan\",\"enabled\":\"yes\"}", "alice"),
                (SqlServerId, "{\"enabled\":true}", "alice"),
                (null, "{\"action\":\"force_plan\",\"enabled\":true}", "alice"),
                (SqlServerId, "{\"action\":\"drop_database\",\"enabled\":true}", "alice"),
                (PostgresId, "{\"action\":\"force_plan\",\"enabled\":true}", "alice"),
            })
            {
                var refused = await RunCommandAsync(owner, executor, server, args, by, ct);
                Assert.Equal("failed", refused.Status);
            }

            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions WHERE action = 'force_plan'", ct));
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
        });
    }

    [Fact]
    public async Task TheMcpTool_SetsTheSwitch_JournaledByTheMcpCallerNeverAnAgentOrAPerson_AndRefusesWithoutTheOwnerSourceAndForAnUnknownServer()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            const string actor = "mcp:token 3fa9c1d2, client says: claude-desktop 1.2.3";
            var set = JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-sql-01", "evict_plan", true, actor, ct))!;
            Assert.Equal("set", (string)set["status"]!);
            Assert.Equal("off", (string)set["before"]!);
            Assert.Equal("on", (string)set["after"]!);
            Assert.False((bool)set["undo"]!["arguments"]!["enabled"]!);
            Assert.Equal(actor, await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
            Assert.Equal("mcp", await ScalarAsync(owner, "SELECT detail->>'surface' FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));

            var back = JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-sql-01", "evict_plan", false, actor, ct))!;
            Assert.Equal("False", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'evict_plan'", ct));
            Assert.Equal("set", (string)back["status"]!);

            Assert.Equal("no_owner_source", (string)JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, null, null, "alpha-sql-01", "evict_plan", true, actor, ct))!["code"]!);
            Assert.Equal("unknown_action", (string)JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-sql-01", "nope", true, actor, ct))!["code"]!);
            Assert.Equal("not_for_engine", (string)JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-pg-01", "force_plan", true, actor, ct))!["code"]!);
            var unknownServer = JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "no-such-server", "evict_plan", true, actor, ct))!;
            Assert.NotEqual("set", (string?)unknownServer["status"]);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
        });
    }

    [Fact]
    public async Task TheMcpTool_JournalsTheLegacyTokenAsLegacyToken_AndANamedTokenByItsName_NeverTheFingerprintTheLogCarries()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            try
            {
                ReadCaller.SetAmbientMcpCaller("claude-desktop 1.2.3");
                ReadCaller.SetAmbientMcpToken(ReadCaller.OfMcpToken("a-legacy-bearer-token"));
                Assert.Matches("^token [0-9a-f]{8}, client says: ", ReadCaller.WriteIdentity());
                Assert.Equal("set", (string)JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-sql-01", "evict_plan", true, DarlingMcpServerActionTools.ActorOf(), ct))!["status"]!);
                var legacy = await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'server_action_switched'", ct);
                Assert.Equal("mcp:legacy token, client says: claude-desktop 1.2.3", legacy);
                Assert.DoesNotMatch("[0-9a-f]{8}", legacy);

                ReadCaller.SetAmbientMcpToken("named token ops");
                Assert.Equal("set", (string)JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-sql-01", "evict_plan", false, DarlingMcpServerActionTools.ActorOf(), ct))!["status"]!);
                Assert.Equal("mcp:named token ops, client says: claude-desktop 1.2.3",
                    await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'server_action_switched' ORDER BY journal_id DESC LIMIT 1", ct));
            }
            finally
            {
                ReadCaller.SetAmbientMcpToken(null);
                ReadCaller.SetAmbientMcpCaller(null);
            }
        });
    }

    [Fact]
    public void TheMcpTool_IsServedOnlyUnderAllowServerAdmin_AndNeverToAnInvestigationAgentEvenWhenItsToolSetNamesIt()
    {
        const string tool = "set_server_action";
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        Assert.Contains(tools, t => t.ProtocolTool.Name == tool);
        Assert.Contains(tools, t => t.ProtocolTool.Name == "get_server_actions");

        bool Served(McpConfig config, string token = "write-gate-token-value")
        {
            var effective = DarlingMcpWritePolicy.ResolveForStart(config, token, out _);
            var collection = new McpServerPrimitiveCollection<McpServerTool>();
            foreach (var t in tools)
                collection.Add(t);
            return DarlingMcpWritePolicy.FilterToolCollection(collection, effective).Any(t => t.ProtocolTool.Name == tool);
        }

        Assert.False(Served(new McpConfig()));
        Assert.False(Served(new McpConfig { AllowWrites = true }));
        Assert.False(Served(new McpConfig { AllowServerAdmin = true }));
        Assert.False(Served(new McpConfig { AllowWrites = true, AllowServerAdmin = true }, token: string.Empty));
        Assert.True(Served(new McpConfig { AllowWrites = true, AllowServerAdmin = true }));

        /* An investigation-token caller (writes off, investigations on) cannot reach it, even if the agent tool set named it. */
        var investigator = DarlingMcpWritePolicy.Resolve(new McpConfig { AllowInvestigations = true }, tokenPresent: true);
        Assert.True(investigator.Investigations);
        Assert.False(investigator.Offers(tool));
        Assert.False(investigator.Offers(tool, new HashSet<string>(StringComparer.Ordinal) { tool }));
    }

    [Fact]
    public async Task TheWebRoutes_AsAdmin_ListAndSetSwitches_JournaledWithThePerson_AndRefuseBadInput()
    {
        await WithWebAsync("admin", async (owner, login, Send, ct) =>
        {
            var list = await Send("GET", "/api/server-actions", null);
            Assert.Equal(200, list.Status);
            Assert.True((bool)list.Body!["can_decide"]!);
            Assert.True((bool)list.Body["signed_in_admin"]!);
            Assert.Equal(2, list.Body["servers"]!.AsArray().Count);

            var on = await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "{\"enabled\":true}");
            Assert.Equal(200, on.Status);
            Assert.Equal("set", (string)on.Body!["status"]!);
            Assert.Equal("True", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));
            Assert.Equal("web", await ScalarAsync(owner, "SELECT detail->>'surface' FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
            Assert.StartsWith("person:", await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'server_action_switched'", ct), StringComparison.Ordinal);

            var read = await Send("GET", "/api/server-actions", null);
            var row = read.Body!["servers"]!.AsArray().Single(s => (int)s!["server_id"]! == 1)!["actions"]!.AsArray().Single(a => (string?)a!["action"] == "force_plan")!;
            Assert.True((bool)row["enabled"]!);

            Assert.Equal(200, (await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "{\"enabled\":false}")).Status);
            Assert.Equal("False", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));

            Assert.Equal(400, (await Send("PUT", "/api/servers/1/actions/drop_database", "application/json", "{\"enabled\":true}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "{\"enabled\":\"yes\"}")).Status);
            Assert.Equal(400, (await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "[1]")).Status);
            Assert.Equal(409, (await Send("PUT", "/api/servers/2/actions/force_plan", "application/json", "{\"enabled\":true}")).Status);
            Assert.Equal(404, (await Send("PUT", "/api/servers/99/actions/force_plan", "application/json", "{\"enabled\":true}")).Status);
            Assert.Equal(415, (await Send("PUT", "/api/servers/1/actions/force_plan", "text/plain", "{\"enabled\":true}")).Status);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));

            /* The admin login itself still cannot write the table: the route had to use the service's owner connection. */
            var direct = await Assert.ThrowsAsync<PostgresException>(async () =>
                await ExecAsync(login, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (1, 'kill_session', true, 'x')", ct));
            Assert.Equal("42501", direct.SqlState);
        });
    }

    /* #6600: the managed store's shape. The web reads as the viewer role; a signed-in admin's access check goes through the arm, so the page can
       decide, and the switch change is journaled by the person. */
    [Fact]
    public async Task OnTheManagedStore_ASignedInAdminSeat_ChangesASwitchThroughTheArm_AndCanDecideIsTrue()
    {
        await WithWebAsync("web", async (owner, _, Send, ct) =>
        {
            var list = await Send("GET", "/api/server-actions", null);
            Assert.Equal(200, list.Status);
            Assert.True((bool)list.Body!["can_decide"]!);
            Assert.True((bool)list.Body["signed_in_admin"]!);

            Assert.Equal(200, (await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "{\"enabled\":true}")).Status);
            Assert.Equal("True", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));
            Assert.Equal("person:alice", await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
        }, seat: new DarlingWebSeat("alice", true), arm: AdminArmShape.Admin, managedStore: true);
    }

    /* #6600: an OIDC viewer seat never opens the arm (its login is refused here, so opening it would fail): it lists, cannot decide, and a
       change answers with the sign-in sentence, as today. */
    [Fact]
    public async Task OnTheManagedStore_AViewerSeat_NeverOpensTheArm_AndAnswersAsToday()
    {
        await WithWebAsync("web", async (owner, _, Send, ct) =>
        {
            var list = await Send("GET", "/api/server-actions", null);
            Assert.Equal(200, list.Status);
            Assert.False((bool)list.Body!["can_decide"]!);
            Assert.False((bool)list.Body["signed_in_admin"]!);

            var refused = await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "{\"enabled\":true}");
            Assert.Equal(403, refused.Status);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions", ct));
        }, seat: new DarlingWebSeat("viewer-person", false), arm: AdminArmShape.Refused, managedStore: true);
    }

    /* #6600: with the switch off (no arm on a managed store) the signed-in admin lists only, and the 403 names the switch. */
    [Fact]
    public async Task OnTheManagedStore_WithTheSwitchOff_TheSignedInAdminStillListsOnly_NamingTheSwitch()
    {
        await WithWebAsync("web", async (owner, _, Send, ct) =>
        {
            Assert.False((bool)(await Send("GET", "/api/server-actions", null)).Body!["can_decide"]!);
            var refused = await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "{\"enabled\":true}");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.ServerActionRefusedMessage(true, adminWritesOff: true), (string)refused.Body!["error"]!);
            Assert.Contains("web.allowAdminWrites", (string)refused.Body["error"]!, StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions", ct));
        }, seat: new DarlingWebSeat("alice", true), managedStore: true);
    }

    [Fact]
    public async Task TheWebRoutes_AsViewer_ListButRefuseEveryChangeWith403_ChangingNothing()
    {
        await WithWebAsync("web", async (owner, _, Send, ct) =>
        {
            var list = await Send("GET", "/api/server-actions", null);
            Assert.Equal(200, list.Status);
            Assert.False((bool)list.Body!["can_decide"]!);

            var refused = await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "{\"enabled\":true}");
            Assert.Equal(403, refused.Status);
            Assert.Equal(DarlingWebEndpoints.ServerActionRefusedMessage(false), (string)refused.Body!["error"]!);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
        });
    }

    [Fact]
    public async Task OnAnUnsignedLoopbackDashboard_TheSwitchPutAnswers403WithTheSignInSentence_WritesAndJournalsNothing_AndTheListOnlyLists()
    {
        /* The web's store login is the admin role, yet nobody signed in: the store login says nothing about the caller. */
        await WithWebAsync("admin", async (owner, _, Send, ct) =>
        {
            var list = await Send("GET", "/api/server-actions", null);
            Assert.Equal(200, list.Status);
            Assert.False((bool)list.Body!["can_decide"]!);
            Assert.False((bool)list.Body["signed_in_admin"]!);

            foreach (var payload in new[] { "{\"enabled\":true}", "{\"enabled\":false}" })
            {
                var refused = await Send("PUT", "/api/servers/1/actions/kill_session", "application/json", payload);
                Assert.Equal(403, refused.Status);
                Assert.Equal(DarlingWebEndpoints.InvestigationWriteSignInMessage, (string)refused.Body!["error"]!);
            }

            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
        }, signedIn: false);
    }

    [Fact]
    public async Task TheWebSwitch_StampsOnePersonPrefix_AndAnOidcNameOverFiftySevenCharactersIsRecorded_NotRefused()
    {
        var longName = string.Concat(Enumerable.Repeat("a", 60)) + "@example.com";
        await WithWebAsync("admin", async (owner, _, Send, ct) =>
        {
            var on = await Send("PUT", "/api/servers/1/actions/force_plan", "application/json", "{\"enabled\":true}");
            Assert.Equal(200, on.Status);
            var actor = await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'server_action_switched'", ct);
            Assert.Equal("person:" + longName, actor);
            Assert.Equal(actor, await ScalarAsync(owner, "SELECT set_by FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));
        }, seat: new DarlingWebSeat(longName, true));

        await WithWebAsync("admin", async (owner, _, Send, ct) =>
        {
            Assert.Equal(200, (await Send("PUT", "/api/servers/1/actions/kill_session", "application/json", "{\"enabled\":true}")).Status);
            Assert.Equal("person:web", await ScalarAsync(owner, "SELECT actor FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ASwitchChange_WaitsForAnAgentWriteHoldingTheSharedLock_ThenSucceeds_OnAndOffAlike(bool turnOn)
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            if (!turnOn)
            {
                await ExecAsync(owner, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (1, 'force_plan', true, 'person:alice')", ct);
            }

            await using var agent = await owner.OpenConnectionAsync(ct);
            await using var agentTransaction = await agent.BeginTransactionAsync(ct);
            await ServerActionSwitchLock.AcquireSharedAsync(agent, agentTransaction, SqlServerId, "force_plan", 30, ct);

            var change = Task.Run(() => ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", turnOn, "person:bob", ServerActionSurfaces.Web, ct), ct);
            Assert.NotSame(change, await Task.WhenAny(change, Task.Delay(TimeSpan.FromSeconds(2), ct)));
            Assert.Equal(turnOn ? "0" : "1", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions WHERE enabled", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));

            await agentTransaction.CommitAsync(ct);
            var result = await change.WaitAsync(TimeSpan.FromSeconds(30), ct);
            Assert.Equal("set", result.Status);
            Assert.Equal(turnOn ? "1" : "0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions WHERE enabled", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
        });
    }

    [Fact]
    public async Task ASwitchChange_ThatStillWaitsOnAnAgentWriteWhenItsWaitEnds_RefusesSwitchBusy_ChangingAndJournalingNothing()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            await using var agent = await owner.OpenConnectionAsync(ct);
            await using var agentTransaction = await agent.BeginTransactionAsync(ct);
            await ServerActionSwitchLock.AcquireSharedAsync(agent, agentTransaction, SqlServerId, "force_plan", 30, ct);

            var refused = await ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", true, "person:bob", ServerActionSurfaces.Web, ct, lockWaitSeconds: 1);
            Assert.False(refused.Succeeded);
            Assert.Equal("switch_busy", refused.Code);
            Assert.Equal(
                "The switch was not changed: an agent change on this server was still running after 60 seconds. Try again in a minute.",
                refused.Message);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));

            /* The refused change left nothing held: once the agent write ends, the same change goes through. */
            await agentTransaction.CommitAsync(ct);
            Assert.Equal("set", (await ServerActionSwitches.SetAsync(owner, SqlServerId, "force_plan", true, "person:bob", ServerActionSurfaces.Web, ct, lockWaitSeconds: 5)).Status);
        });
    }

    [Fact]
    public async Task AServerRegisteredWithAnEngineAlias_PgAuroraOrMixedCase_IsAPostgreSqlTarget_RefusingForcePlanAndEvictPlanOn_AndListingThemInvalid()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var aliases = new[] { (3, "alpha-alias-pg", "pg"), (4, "alpha-alias-aurora", "aurora"), (5, "alpha-alias-aurora-pg", "Aurora-PostgreSQL"), (6, "alpha-alias-mixed", " PostgreSQL ") };
            foreach (var (id, name, engine) in aliases)
            {
                await ExecAsync(owner, $"INSERT INTO config.config_monitored_servers (server_id, name, host, engine) VALUES ({id}, '{name}', '{name}.example.test', '{engine}')", ct);
            }

            foreach (var (id, _, _) in aliases)
            {
                foreach (var action in new[] { "force_plan", "evict_plan" })
                {
                    var refused = await ServerActionSwitches.SetAsync(owner, id, action, true, "person:alice", ServerActionSurfaces.Viewer, ct);
                    Assert.False(refused.Succeeded);
                    Assert.Equal("not_for_engine", refused.Code);
                }

                Assert.True((await ServerActionSwitches.SetAsync(owner, id, "kill_session", true, "person:alice", ServerActionSurfaces.Viewer, ct)).Succeeded);
            }

            var listed = await ServerActionSwitches.ListAsync(owner, ct);
            foreach (var (id, _, _) in aliases)
            {
                var actions = listed.Single(r => r.ServerId == id).Actions;
                Assert.False(actions.Single(a => a.Action == "force_plan").Valid);
                Assert.True(actions.Single(a => a.Action == "kill_session").Valid);
            }

            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions WHERE action IN ('force_plan', 'evict_plan')", ct));
        });
    }

    [Fact]
    public async Task TheWebList_AnswersEachServersEngineKindFromTheSharedParser_SoPgAndAuroraAreLabelledPostgreSql()
    {
        await WithWebAsync("admin", async (owner, _, Send, ct) =>
        {
            await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host, engine) VALUES (3, 'beta-pg-01', 'beta-pg-01.example.test', 'pg')", ct);
            await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host, engine) VALUES (4, 'beta-aurora-01', 'beta-aurora-01.example.test', 'Aurora')", ct);

            var list = await Send("GET", "/api/server-actions", null);
            Assert.Equal(200, list.Status);
            var kinds = list.Body!["servers"]!.AsArray().ToDictionary(s => (string)s!["server_name"]!, s => (string)s!["engine_kind"]!);
            Assert.Equal("sqlserver", kinds["alpha-sql-01"]);
            Assert.Equal("postgresql", kinds["alpha-pg-01"]);
            Assert.Equal("postgresql", kinds["beta-pg-01"]);
            Assert.Equal("postgresql", kinds["beta-aurora-01"]);
        });
    }

    [Fact]
    public async Task AnOnSwitchTheEngineCannotRun_IsListedByTheWebAsInvalidAndOn_AnOffPutClearsIt_AndAnOnPutIsStillRefused()
    {
        await WithWebAsync("admin", async (owner, _, Send, ct) =>
        {
            /* force_plan was on for server 2 (alpha-pg-01) when its engine was edited to PostgreSQL: the row is left behind. */
            await ExecAsync(owner, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (2, 'force_plan', true, 'person:alice')", ct);

            JsonNode StuckRow(JsonNode body) => body["servers"]!.AsArray().Single(s => (int)s!["server_id"]! == 2)!["actions"]!.AsArray().Single(a => (string?)a!["action"] == "force_plan")!;
            var before = StuckRow((await Send("GET", "/api/server-actions", null)).Body!);
            Assert.False((bool)before["valid"]!);
            Assert.True((bool)before["enabled"]!);

            var refused = await Send("PUT", "/api/servers/2/actions/force_plan", "application/json", "{\"enabled\":true}");
            Assert.Equal(409, refused.Status);
            Assert.Equal("True", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 2 AND action = 'force_plan'", ct));

            var off = await Send("PUT", "/api/servers/2/actions/force_plan", "application/json", "{\"enabled\":false}");
            Assert.Equal(200, off.Status);
            Assert.Equal("set", (string)off.Body!["status"]!);
            Assert.Equal("False", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 2 AND action = 'force_plan'", ct));
            Assert.False((bool)StuckRow((await Send("GET", "/api/server-actions", null)).Body!)["enabled"]!);
        });
    }

    [Fact]
    public async Task TurningASwitchOff_OnAServerWhoseEngineCannotRunIt_StillWorks_ButTurningItOnIsRefused()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            /* force_plan was on for server 1 when its engine was edited to postgres: the row is left behind. */
            await ExecAsync(owner, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (1, 'force_plan', true, 'person:alice')", ct);
            await ExecAsync(owner, "UPDATE config.config_monitored_servers SET engine = 'postgres' WHERE server_id = 1", ct);

            var off = await ServerActionSwitches.SetAsync(owner, 1, "force_plan", false, "person:bob", ServerActionSurfaces.Mcp, ct);
            Assert.True(off.Succeeded);
            Assert.Equal("set", off.Status);
            Assert.Equal("False", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched' AND detail->>'after' = 'off'", ct));

            var again = await ServerActionSwitches.SetAsync(owner, 1, "force_plan", true, "person:bob", ServerActionSurfaces.Mcp, ct);
            Assert.Equal("not_for_engine", again.Code);
            Assert.Equal("False", await ScalarAsync(owner, "SELECT enabled FROM config.config_server_actions WHERE server_id = 1 AND action = 'force_plan'", ct));
        });
    }

    /* #6727: the registry says "sqlserver" for an Azure SQL target too; the probed edition in collect.servers is what the plan guide and eviction tools refuse on. */
    [Fact]
    public async Task AnAzureSqlTarget_RefusesTheSwitchesItsToolsAlwaysRefuse_OnAndListsThemInvalid_WhileOffAndTheOtherActionsStillWork()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            async Task<IReadOnlyDictionary<string, bool>> ValidAsync()
            {
                var rows = await ServerActionSwitches.ListAsync(owner, ct);
                return rows.Single(r => r.ServerId == 1).Actions.ToDictionary(a => a.Action, a => a.Valid, StringComparer.Ordinal);
            }

            /* Edition 5, an Azure SQL Database: no eviction and no plan guides. */
            await ExecAsync(owner, "UPDATE collect.servers SET sql_engine_edition = 5 WHERE server_id = 1", ct);
            var evict = await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.EvictPlan, true, "person:alice", ServerActionSurfaces.Mcp, ct);
            Assert.False(evict.Succeeded);
            Assert.Equal("not_for_engine", evict.Code);
            Assert.Contains("Azure SQL Database", evict.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("a SQL Server target", evict.Message, StringComparison.Ordinal);
            Assert.Equal("not_for_engine", (await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.CreatePlanGuide, true, "person:alice", ServerActionSurfaces.Mcp, ct)).Code);
            Assert.Equal("not_for_engine", (await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.DropPlanGuide, true, "person:alice", ServerActionSurfaces.Mcp, ct)).Code);
            var valid = await ValidAsync();
            Assert.False(valid[ServerActionNames.EvictPlan]);
            Assert.False(valid[ServerActionNames.CreatePlanGuide]);
            Assert.False(valid[ServerActionNames.DropPlanGuide]);
            Assert.True(valid[ServerActionNames.ForcePlan]);
            Assert.True(valid[ServerActionNames.KillSession]);
            Assert.True(valid[ServerActionNames.RecompileModule]);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions WHERE server_id = 1", ct));
            Assert.True((await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.KillSession, true, "person:alice", ServerActionSurfaces.Mcp, ct)).Succeeded);

            /* Edition 8, a Managed Instance: no plan guides, but eviction runs. */
            await ExecAsync(owner, "UPDATE collect.servers SET sql_engine_edition = 8 WHERE server_id = 1", ct);
            var guide = await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.CreatePlanGuide, true, "person:alice", ServerActionSurfaces.Mcp, ct);
            Assert.Equal("not_for_engine", guide.Code);
            Assert.Contains("Azure SQL Managed Instance", guide.Message, StringComparison.Ordinal);
            Assert.Equal("not_for_engine", (await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.DropPlanGuide, true, "person:alice", ServerActionSurfaces.Mcp, ct)).Code);
            Assert.True((await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.EvictPlan, true, "person:alice", ServerActionSurfaces.Mcp, ct)).Succeeded);
            valid = await ValidAsync();
            Assert.True(valid[ServerActionNames.EvictPlan]);
            Assert.False(valid[ServerActionNames.CreatePlanGuide]);
            Assert.False(valid[ServerActionNames.DropPlanGuide]);

            /* A switch left on before the edition was probed as Azure SQL Database can still be turned off. */
            await ExecAsync(owner, "UPDATE collect.servers SET sql_engine_edition = 5 WHERE server_id = 1", ct);
            var off = await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.EvictPlan, false, "person:alice", ServerActionSurfaces.Mcp, ct);
            Assert.True(off.Succeeded);
            Assert.Equal("set", off.Status);

            /* No edition probed yet, and no collect.servers row at all: answers as an ordinary SQL Server, as before. */
            await ExecAsync(owner, "UPDATE collect.servers SET sql_engine_edition = NULL WHERE server_id = 1", ct);
            Assert.True((await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.CreatePlanGuide, true, "person:alice", ServerActionSurfaces.Mcp, ct)).Succeeded);
            await ExecAsync(owner, "DELETE FROM collect.servers WHERE server_id = 1", ct);
            Assert.True((await ServerActionSwitches.SetAsync(owner, 1, ServerActionNames.EvictPlan, true, "person:alice", ServerActionSurfaces.Mcp, ct)).Succeeded);
            valid = await ValidAsync();
            Assert.All(ServerActionNames.All, a => Assert.True(valid[a], a));
        });
    }

    [Theory]
    [InlineData("admin")]
    [InlineData("mcp")]
    public async Task TheAdminAndMcpLogins_CannotInsertAServerActionSwitchedJournalRow_42501_WhileTheOwnersSwitchStillJournals(string role)
    {
        await WithWebAsync(role, async (owner, login, _, ct) =>
        {
            var forged = await Assert.ThrowsAsync<PostgresException>(async () => await ExecAsync(login,
                "INSERT INTO config.investigation_journal (event_time, server_id, event, actor, detail) "
                + "VALUES (now(), 1, 'server_action_switched', 'person:someone', '{\"after\":\"on\"}')", ct));
            Assert.Equal("42501", forged.SqlState);

            /* The owner's own write is not held to the rule. */
            Assert.True((await ServerActionSwitches.SetAsync(owner, 1, "kill_session", true, "person:alice", ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal WHERE event = 'server_action_switched'", ct));
        });
    }

    [Fact]
    public async Task TheMcpTool_TakesOneServerByItsExactName_RefusesAPartialNameListingTheNearMatches_AndFindsADisabledServerLikeEditServerDoes()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (3, 'alpha-sql-010', 'alpha-sql-010.example.test', true)", ct);
            await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (4, 'alpha-sql-off', 'alpha-sql-off.example.test', false)", ct);
            const string actor = "mcp:token 3fa9c1d2, client: test";

            foreach (var partial in new[] { "alpha-sql", "alpha-sql-0", "sql-010" })
            {
                var refused = JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, partial, "kill_session", true, actor, ct))!;
                Assert.Equal("refused", (string)refused["status"]!);
                Assert.Equal("ambiguous", (string)refused["code"]!);
            }

            var listing = (string)JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-sql", "kill_session", true, actor, ct))!["message"]!;
            Assert.Contains("alpha-sql-01", listing, StringComparison.Ordinal);
            Assert.Contains("alpha-sql-010", listing, StringComparison.Ordinal);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.config_server_actions", ct));
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));

            /* The exact name picks the one server, though another server's name contains it. */
            var exact = JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-sql-01", "kill_session", true, actor, ct))!;
            Assert.Equal("set", (string)exact["status"]!);
            Assert.Equal(1, (int)exact["server_id"]!);

            /* A disabled server is a definition: edit_server and remove_server find it by its exact name, and so does this tool. */
            var disabled = JsonNode.Parse(await DarlingMcpServerActionTools.RunAsync(owner, owner, null, "alpha-sql-off", "kill_session", true, actor, ct))!;
            Assert.Equal("set", (string)disabled["status"]!);
            Assert.Equal(4, (int)disabled["server_id"]!);
        });
    }

    [Fact]
    public async Task TheViewerCommand_LogsOnlyAnActionFromTheFixedList_NeverTheCallersText()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var logger = new CapturingTestLogger();
            var executor = new DarlingCommandExecutor(owner, new NoCommandHost(), "test-instance", logger);
            await RunCommandAsync(owner, executor, SqlServerId, "{\"action\":\"evict_plan\\r\\n[Information] fake line from nowhere\",\"enabled\":true}", "alice", ct);
            await RunCommandAsync(owner, executor, SqlServerId, "{\"action\":\"evict_plan\",\"enabled\":true}", "alice", ct);

            var lines = logger.Joined;
            Assert.DoesNotContain("fake line", lines, StringComparison.Ordinal);
            Assert.Contains("unknown action", lines, StringComparison.Ordinal);
            Assert.Contains("evict_plan on server", lines, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task TheGrantsAreUnchanged_NoManagedRoleCanWriteTheSwitchTable_ButEachCanReadIt()
    {
        foreach (var role in new[] { "admin", "viewer", "mcp" })
        {
            await WithWebAsync(role, async (owner, login, _, ct) =>
            {
                Assert.Equal("0", await ScalarAsync(login, "SELECT count(*) FROM config.config_server_actions", ct));
                foreach (var sql in new[]
                {
                    "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (1, 'kill_session', true, 'x')",
                    "UPDATE config.config_server_actions SET enabled = true",
                    "DELETE FROM config.config_server_actions",
                })
                {
                    var ex = await Assert.ThrowsAsync<PostgresException>(async () => await ExecAsync(login, sql, ct));
                    Assert.Equal("42501", ex.SqlState);
                }
            });
        }
    }

    // ---- harness ------------------------------------------------------------------------------------------------

    private delegate Task<(int Status, JsonNode? Body)> Sender(string method, string path, string? contentType, string? payload = null);

    private static async Task SeedAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (1, 'alpha-sql-01', 'alpha-sql-01.example.test')", ct);
        await ExecAsync(owner, "INSERT INTO config.config_monitored_servers (server_id, name, host, engine) VALUES (2, 'alpha-pg-01', 'alpha-pg-01.example.test', 'postgresql')", ct);
        await ExecAsync(owner, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
            + "VALUES (1, 'alpha-sql-01', 'alpha-sql-01', TRUE, 15, now(), now()), (2, 'alpha-pg-01', 'alpha-pg-01', TRUE, 15, now(), now())", ct);
    }

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the server action switch live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(owner, ct);
            await body(owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task WithWebAsync(string managedRole, Func<NpgsqlDataSource, NpgsqlDataSource, Sender, CancellationToken, Task> body, bool signedIn = true, DarlingWebSeat? seat = null,
        AdminArmShape arm = AdminArmShape.None, bool managedStore = false)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the server action switch live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "switch_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(owner, ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildJournalEventRulesSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, managedRole))
            {
                await ExecAsync(owner, statement, ct);
            }

            var asSeat = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                SearchPath = "config,public",
                Pooling = false,
            }.ConnectionString;
            await using var webLogin = NpgsqlDataSource.Create(asSeat);
            using var adminArm = await AdminArmLive.BuildAsync(owner, scratch, roleName, arm, ct);

            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
                WebRootPath = "wwwroot",
            });
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(webLogin);
            await using var app = builder.Build();
            if (signedIn)
            {
                /* What the sign-in middleware stores for a request that holds a valid session. A dashboard with no sign-in stores nothing (#6269). */
                app.Use((context, next) =>
                {
                    context.Items[DarlingWebSeat.HttpContextItemKey] = seat ?? DarlingWebSeat.SharedToken;
                    return next(context);
                });
            }

            var ownerSeam = new StoreOwnerSource();
            ownerSeam.Publish(owner);
            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(), ownerSource: ownerSeam,
                postgresConfig: managedStore ? new PostgresConfig { Managed = true } : null, adminArm: adminArm);
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, JsonNode? Body)> Send(string method, string path, string? contentType, string? payload = null)
            {
                var context = await server.SendAsync(request =>
                {
                    request.Request.Method = method;
                    request.Request.Path = path;
                    request.Request.Headers.Host = "localhost";
                    if (contentType is not null)
                    {
                        request.Request.ContentType = contentType;
                    }

                    request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(payload ?? (method == "PUT" ? "{}" : "")));
                });
                using var reader = new StreamReader(context.Response.Body);
                var text = await reader.ReadToEndAsync(ct);
                return (context.Response.StatusCode, string.IsNullOrWhiteSpace(text) ? null : JsonNode.Parse(text));
            }

            await body(owner, webLogin, new Sender(Send), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (connection, token) =>
            {
                NpgsqlConnection.ClearAllPools();
                await using var drop = new NpgsqlCommand(AdminArmLive.DropRolesSql(roleName), connection);
                await drop.ExecuteNonQueryAsync(token);
            });
        }
    }

    private static async Task<(string Status, string? Result)> RunCommandAsync(
        NpgsqlDataSource owner, DarlingCommandExecutor executor, int? server, string args, string requestedBy, CancellationToken ct)
    {
        long id;
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status) VALUES ($1, 'set_server_action', $2, $3::jsonb, 'pending') RETURNING command_id"))
        {
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = requestedBy, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            insert.Parameters.Add(new NpgsqlParameter<int?> { TypedValue = server, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = args, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            await using var connection = await owner.OpenConnectionAsync(ct);
            insert.Connection = connection;
            id = Convert.ToInt64(await insert.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
        }

        Assert.True(await executor.PollOnceAsync(ct));
        return (
            (await ScalarAsync(owner, $"SELECT status FROM config.config_command WHERE command_id = {id}", ct))!,
            await ScalarAsync(owner, $"SELECT result_json::text FROM config.config_command WHERE command_id = {id}", ct));
    }

    private sealed class NoCommandHost : IDarlingCommandHost
    {
        private static Task<CommandOutcome> No() => throw new InvalidOperationException("the switch command does not use the host");

        public Task<CommandOutcome> SnapshotNowAsync(int serverId, IReadOnlyList<string>? collectors, CancellationToken cancellationToken) => No();

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken) => No();

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken) => No();

        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken) => No();

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken) => No();

        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken) => No();

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken) => No();
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
