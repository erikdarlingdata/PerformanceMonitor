/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// The approval outcome of a Level 3 server action against a live store (#7109, slice 4a): the gate reports it and never refuses for it, a route
/// set to <c>required</c> after the gate makes the agent's write refuse <c>approval_required</c> with nothing run, a chat request needs a press
/// on a route that is off, a route that is off still runs the action, and <see cref="InvestigationRequestStore.NobodyCanApproveAsync"/> follows the
/// #7107 people and grants.
/// </summary>
public sealed class ActionApprovalStoreLiveTests
{
    private const string Admin = "person:alice";
    private static readonly DateTime T0 = new(2026, 10, 18, 12, 0, 0, DateTimeKind.Utc);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static NotificationRoute RouteAt(string approval) =>
        new(1, "*", "", "", "", "", "", Enabled: true) { AgentLevel = 3, ActionApproval = approval };

    private static ServerActionHooks GateHooks(InvestigationRequestStore store, NpgsqlDataSource source, Func<NotificationRoute?> route) => new()
    {
        LoadRoutes = (_, _, _) => Task.FromResult<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>((_, _, _) => route()),
        ReadCredential = (serverId, ct) => store.ReadServerCredentialStateAsync(source, serverId, ct),
    };

    private int _writes;

    private AgentActionHooks WriteHooks(Func<NotificationRoute?> route, Func<CancellationToken, Task>? inFlight = null) => new()
    {
        LoadRoutes = (_, _, _) => Task.FromResult<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>((_, _, _) => route()),
        ReadBefore = async (_, _, token) =>
        {
            if (inFlight is not null)
                await inFlight(token);
            return "{\"reason\":\"old\"}";
        },
        Write = (_, _, _) => { _writes++; return Task.FromResult<string?>("{\"reason\":\"new\"}"); },
    };

    private static ServerActionCall Call(Guid request, Guid claim, string action = ServerActionNames.ForcePlan) =>
        new(action, action, new AgentAction(request, claim, "agent-a", "the plan regressed",
            "[{\"tool\":\"get_query_store_top\",\"arguments\":{},\"key_numbers\":\"12\"}]"),
            new AgentActionTarget(7, "srv-7", null, null));

    private static AgentActionCall WriteCall(Guid request, Guid claim)
    {
        var call = Call(request, claim);
        return new AgentActionCall("force_plan", AgentChangeKinds.PlanForce, call.Action, call.Target);
    }

    private async Task<(Guid Request, Guid Claim)> OpenAndClaimAsync(InvestigationRequestStore store, NpgsqlConnection connection, bool chat, CancellationToken ct)
    {
        var alert = new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", T0, Array.Empty<string>(), null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240)
        { AutonomyLevel = 3 };
        var opened = await store.OpenOrJoinAsync(alert, ct);
        var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
        if (chat)
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET trigger_kind = '{InvestigationTriggerKinds.Chat}' WHERE request_id = '{opened.RequestId}'", ct);
        return (opened.RequestId.Value, claim.ClaimId!.Value);
    }

    private static async Task ArmAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
        await ExecAsync(connection,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, remediation_username, remediation_encrypted_password) VALUES (7, 'srv-7', 'host-7', 'svc', 'env:X')", ct);
        await ExecAsync(connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'force_plan', true, 'test')", ct);
    }

    private async Task<string?> EventsAsync(NpgsqlConnection connection, Guid request, CancellationToken ct) =>
        await ScalarAsync(connection,
            $"SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE request_id = '{request}' AND event LIKE 'action_%'", ct);

    [Fact]
    public async Task TheGate_ReportsApproval_NeverRefusesForIt_AndAnOffRouteStillRunsTheWrite()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, ct);
            var (request, claim) = await OpenAndClaimAsync(store, connection, chat: false, ct);

            var required = await store.AuthorizeServerActionAsync(Call(request, claim), GateHooks(store, source, () => RouteAt(NotificationRouteKinds.ActionApprovalRequired)), ct);
            Assert.Null(required.Refusal);
            Assert.NotNull(required.Permit);
            Assert.True(required.NeedsApproval);
            Assert.False(required.Approval!.SinglePerson);
            Assert.Equal(1, required.Approval.ClaimNo);

            var off = await store.AuthorizeServerActionAsync(Call(request, claim), GateHooks(store, source, () => RouteAt(NotificationRouteKinds.ActionApprovalOff)), ct);
            Assert.Null(off.Refusal);
            Assert.False(off.NeedsApproval);

            /* A gate that reports does not write: no refused row, no action row. */
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' OR event LIKE 'action_%'", ct));

            /* A route that is off runs the write as it did before approval existed. */
            var result = await store.RunAgentActionAsync(WriteCall(request, claim), WriteHooks(() => RouteAt(NotificationRouteKinds.ActionApprovalOff)), ct);
            Assert.True(result.Done);
            Assert.Equal(1, _writes);
            Assert.Equal("action_called|action_done", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task ARouteSetToRequiredAfterTheGate_MakesTheWriteRefuseApprovalRequired_AndNothingReachesTheServer()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, ct);
            var (request, claim) = await OpenAndClaimAsync(store, connection, chat: false, ct);
            var approval = NotificationRouteKinds.ActionApprovalOff;

            var gate = await store.AuthorizeServerActionAsync(Call(request, claim), GateHooks(store, source, () => RouteAt(approval)), ct);
            Assert.False(gate.NeedsApproval);

            /* A person sets the route to required after the gate, before the write's own transaction. */
            var hooks = WriteHooks(() => RouteAt(approval), _ => { approval = NotificationRouteKinds.ActionApprovalRequired; return Task.CompletedTask; });
            var result = await store.RunAgentActionAsync(WriteCall(request, claim), hooks, ct);
            Assert.Equal(AgentActionCodes.ApprovalRequired, result.Refusal?.Reason);
            Assert.NotNull(result.CalledId);
            Assert.Equal(0, _writes);
            Assert.Equal("action_called|action_failed", await EventsAsync(connection, request, ct));
        });
    }

    [Fact]
    public async Task AChatRequest_NeedsAPress_OnARouteThatIsOff_AtTheGate_AndTheWriteRefuses()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, ct);
            var (request, claim) = await OpenAndClaimAsync(store, connection, chat: true, ct);
            var off = () => RouteAt(NotificationRouteKinds.ActionApprovalOff);

            var gate = await store.AuthorizeServerActionAsync(Call(request, claim), GateHooks(store, source, off), ct);
            Assert.Null(gate.Refusal);
            Assert.True(gate.NeedsApproval);

            var result = await store.RunAgentActionAsync(WriteCall(request, claim), WriteHooks(off), ct);
            Assert.Equal(AgentActionCodes.ApprovalRequired, result.Refusal?.Reason);
            Assert.Equal(0, _writes);
        });
    }

    [Fact]
    public async Task NobodyCanApprove_FollowsTheMappedPeopleAndTheirGrants()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (8, 'srv-8', 'host-8')", ct);
            const string kind = ServerActionNames.KillSession;

            /* Nobody holds the grant. */
            Assert.True(await store.NobodyCanApproveAsync(7, kind, singlePerson: false, ct));
            /* Single-person approval is the other way to approve: nobody-can-approve is never true then. */
            Assert.False(await store.NobodyCanApproveAsync(7, kind, singlePerson: true, ct));

            var ann = (await PersonDirectory.AddPersonAsync(source, "Ann", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            var grant = (await PersonDirectory.AddGrantAsync(source, ann, 7, null, "approve", [kind], Admin, ServerActionSurfaces.Viewer, ct)).GrantId!.Value;
            /* A grant without an identity is not a mapped person. */
            Assert.True(await store.NobodyCanApproveAsync(7, kind, singlePerson: false, ct));

            var key = DarlingWebSeat.ComputeOwnerKey("https://issuer.example.test", "sub-ann")!;
            Assert.True((await PersonDirectory.AddIdentityAsync(source, ann, "web", key, Admin, ServerActionSurfaces.Web, ct)).Succeeded);
            Assert.False(await store.NobodyCanApproveAsync(7, kind, singlePerson: false, ct));
            Assert.False(await store.NobodyCanApproveAsync(7, kind, singlePerson: true, ct));

            /* The grant is for that server and that kind only. */
            Assert.True(await store.NobodyCanApproveAsync(8, kind, singlePerson: false, ct));
            Assert.True(await store.NobodyCanApproveAsync(7, ServerActionNames.ForcePlan, singlePerson: false, ct));

            /* A disabled person approves nothing, and approves again once enabled. */
            Assert.True((await PersonDirectory.SetPersonEnabledAsync(source, ann, false, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.True(await store.NobodyCanApproveAsync(7, kind, singlePerson: false, ct));
            Assert.True((await PersonDirectory.SetPersonEnabledAsync(source, ann, true, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.False(await store.NobodyCanApproveAsync(7, kind, singlePerson: false, ct));

            /* A removed grant leaves nobody. */
            Assert.True((await PersonDirectory.RemoveGrantAsync(source, grant, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.True(await store.NobodyCanApproveAsync(7, kind, singlePerson: false, ct));

            /* A fleet grant covers every server, and a tag grant covers the servers that carry the tag. */
            Assert.True((await PersonDirectory.AddGrantAsync(source, ann, null, null, "approve", [kind], Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.False(await store.NobodyCanApproveAsync(7, kind, singlePerson: false, ct));
            Assert.False(await store.NobodyCanApproveAsync(8, kind, singlePerson: false, ct));
        });
    }

    [Fact]
    public async Task NobodyCanApprove_ATagGrantCountsOnlyForServersCarryingTheTag()
    {
        await RunAsync(async (store, connection, source, ct) =>
        {
            await ArmAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (8, 'srv-8', 'host-8')", ct);
            var tag = Convert.ToInt32(await ScalarAsync(connection, "INSERT INTO config.server_tags (name) VALUES ('oltp') RETURNING id", ct), CultureInfo.InvariantCulture);
            await ExecAsync(connection, $"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (7, {tag})", ct);
            var ann = (await PersonDirectory.AddPersonAsync(source, "Ann", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            var key = DarlingWebSeat.ComputeOwnerKey("https://issuer.example.test", "sub-ann")!;
            Assert.True((await PersonDirectory.AddIdentityAsync(source, ann, "web", key, Admin, ServerActionSurfaces.Web, ct)).Succeeded);
            Assert.True((await PersonDirectory.AddGrantAsync(source, ann, null, tag, "approve", [ServerActionNames.KillSession], Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);

            Assert.False(await store.NobodyCanApproveAsync(7, ServerActionNames.KillSession, singlePerson: false, ct));
            Assert.True(await store.NobodyCanApproveAsync(8, ServerActionNames.KillSession, singlePerson: false, ct));
        });
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the approval live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var store = new InvestigationRequestStore(dataSource, 30, () => T0);
            await body(store, connection, dataSource, ct);
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
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
