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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#7109: the parts of the proposal store that need no store.</summary>
public sealed class ActionProposalRulesTests
{
    [Fact]
    public void TheSameTarget_InAnyKeyOrder_HasTheSameKey_AndAnotherPlanHasAnother()
    {
        var a = InvestigationRequestStore.CanonicalJson(new JsonObject { ["plan_id"] = 5, ["database"] = "Sales", ["query_id"] = 9 });
        var b = InvestigationRequestStore.CanonicalJson(new JsonObject { ["query_id"] = 9, ["database"] = "Sales", ["plan_id"] = 5 });
        var c = InvestigationRequestStore.CanonicalJson(new JsonObject { ["query_id"] = 9, ["database"] = "Sales", ["plan_id"] = 7 });
        Assert.Equal(a, b);
        Assert.Equal(InvestigationRequestStore.TargetKey(a), InvestigationRequestStore.TargetKey(b));
        Assert.NotEqual(InvestigationRequestStore.TargetKey(a), InvestigationRequestStore.TargetKey(c));
        Assert.Matches("^[0-9a-f]{64}$", InvestigationRequestStore.TargetKey(a));
    }

    [Fact]
    public void EveryProposalEvent_IsInTheJournalsOwnerOnlyList()
    {
        var list = DarlingManagedRoles.JournalOwnerEventList.Split(", ");
        foreach (var kind in ProposalEvents.OwnerOnly.Append(InvestigationRequestStore.ProposalNoticeSent).Append(InvestigationRequestStore.AskerJournalEvent))
            Assert.Contains("'" + kind + "'", list);
    }

    [Fact]
    public void AnApprovedProposal_EndsOnlyAsRunFailedUnknownOrRefused()
    {
        foreach (var state in ProposalStates.AfterApproved)
            Assert.StartsWith("proposal_", ProposalEvents.ForState(state), StringComparison.Ordinal);
        Assert.Throws<ArgumentOutOfRangeException>(() => ProposalEvents.ForState(ProposalStates.Pending));
    }
}

/// <summary>
/// #7109 on a live store: a create returns the same id for the same request, tool and target; each state change runs only from its
/// from-state and journals its event; the sweep voids, expires and marks unknown; the rung's backfill sets off only on Level 3 routes.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class ActionProposalStoreLiveTests
{
    private static readonly Guid s_request = Guid.Parse("71090000-0000-0000-0000-000000000001");
    private static readonly Guid s_closed = Guid.Parse("71090000-0000-0000-0000-000000000002");
    private static readonly Guid s_resolved = Guid.Parse("71090000-0000-0000-0000-000000000003");

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the action proposal live tests.");
        return connectionString!;
    }

    internal static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
    }

    internal static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static Task AddRequestAsync(NpgsqlConnection connection, Guid id, string state, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at, state) "
            + $"VALUES ('{id}', 1, 7, 'srv-7', 'blocking', 'blocking', now(), now(), now(), now() + interval '4 hours', '{state}')", ct);

    internal static ActionProposalDraft Draft(Guid request, string tool, JsonObject target) =>
        new(request, 1, "agent-a", tool, 7, target, new JsonObject { ["statement"] = "EXEC x;" }, "the plan regressed");

    private static JsonObject Force(int planId) => new() { ["database"] = "Sales", ["query_id"] = 9, ["plan_id"] = planId };

    private static async Task WithStoreAsync(Func<NpgsqlConnection, ScratchPostgres, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await body(connection, scratch, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ARetry_GetsTheSameId_ANewPlanIsANewProposal_AndAnEndedOneDoesNotBlockANewOne()
    {
        await WithStoreAsync(async (connection, scratch, ct) =>
        {
            await AddRequestAsync(connection, s_request, "claimed", ct);
            var store = new InvestigationRequestStore(NpgsqlDataSource.Create(scratch.ConnectionString), 30);

            var first = await store.CreateProposalAsync(Draft(s_request, "force_query_store_plan", Force(5)), ct);
            Assert.True(first.Created);
            var retry = await store.CreateProposalAsync(Draft(s_request, "force_query_store_plan",
                new JsonObject { ["plan_id"] = 5, ["query_id"] = 9, ["database"] = "Sales" }), ct);
            Assert.False(retry.Created);
            Assert.Equal(first.ProposalId, retry.ProposalId);
            var plan7 = await store.CreateProposalAsync(Draft(s_request, "force_query_store_plan", Force(7)), ct);
            Assert.True(plan7.Created);
            Assert.NotEqual(first.ProposalId, plan7.ProposalId);
            var unforce = await store.CreateProposalAsync(Draft(s_request, "unforce_query_store_plan", Force(5)), ct);
            Assert.NotEqual(first.ProposalId, unforce.ProposalId);

            Assert.Equal("3", await ScalarAsync(connection, "SELECT count(*) FROM config.agent_action_proposals", ct));
            Assert.Equal("3", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'proposal_created' AND actor = 'agent-a'", ct));

            var row = (await store.GetProposalAsync(first.ProposalId, ct))!;
            Assert.Equal(ProposalStates.Pending, row.State);
            Assert.Equal(TimeSpan.FromMinutes(ProposalRules.ExpiryMinutes), row.ExpiresAt - row.ProposedAt);
            Assert.Equal(1, row.ClaimNo);
            Assert.Equal("the plan regressed", row.Reason);
            Assert.Equal(3, (await store.ListProposalsAsync(s_request, ProposalStates.Pending, 10, ct)).Count);

            /* Once the proposal has ended, the same call makes a new one rather than answering a dead id. */
            await ExecAsync(connection, $"UPDATE config.agent_action_proposals SET state = 'expired' WHERE proposal_id = '{first.ProposalId}'", ct);
            var again = await store.CreateProposalAsync(Draft(s_request, "force_query_store_plan", Force(5)), ct);
            Assert.True(again.Created);
            Assert.NotEqual(first.ProposalId, again.ProposalId);
        });
    }

    [Fact]
    public async Task EachStateChange_RunsOnlyFromItsFromState_ByThePersonOfTheFirstPress_AndJournalsItsEvent()
    {
        await WithStoreAsync(async (connection, scratch, ct) =>
        {
            await AddRequestAsync(connection, s_request, "claimed", ct);
            var store = new InvestigationRequestStore(NpgsqlDataSource.Create(scratch.ConnectionString), 30);
            var id = (await store.CreateProposalAsync(Draft(s_request, "force_query_store_plan", Force(5)), ct)).ProposalId;
            var facts = new JsonObject { ["statement"] = "EXEC x;" };

            /* No first press yet, so no confirming press. */
            Assert.False(await store.ApproveProposalAsync(id, "person:ann", null, ct));
            Assert.False(await store.FinishProposalAsync(id, ProposalStates.Run, "service", null, ct));
            Assert.True(await store.RecordFirstPressAsync(id, "person:ann", facts, new JsonObject { ["surface"] = "web" }, ct));
            Assert.False(await store.ApproveProposalAsync(id, "person:bob", null, ct));
            Assert.True(await store.ApproveProposalAsync(id, "person:ann", null, ct));

            /* A double press, a late first press and a supersede find no pending row. */
            Assert.False(await store.ApproveProposalAsync(id, "person:ann", null, ct));
            Assert.False(await store.RecordFirstPressAsync(id, "person:bob", facts, null, ct));
            Assert.False(await store.SupersedeProposalAsync(id, "person:ann", new JsonObject { ["changed"] = "statement" }, ct));

            Assert.True(await store.FinishProposalAsync(id, ProposalStates.Run, "agent-a", new JsonObject { ["called_id"] = 41 }, ct));
            Assert.False(await store.FinishProposalAsync(id, ProposalStates.Failed, "agent-a", null, ct));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.FinishProposalAsync(id, ProposalStates.Expired, "service", null, ct));

            var row = (await store.GetProposalAsync(id, ct))!;
            Assert.Equal(ProposalStates.Run, row.State);
            Assert.Equal("person:ann", row.FirstPressBy);
            Assert.Equal("person:ann", row.DecidedBy);
            Assert.NotNull(row.DecidedAt);
            Assert.NotNull(row.FinishedAt);
            Assert.Contains("41", row.OutcomeJson, StringComparison.Ordinal);

            Assert.Equal("proposal_created,proposal_first_press,proposal_approved,proposal_run", await ScalarAsync(connection,
                "SELECT string_agg(event, ',' ORDER BY journal_id) FROM config.investigation_journal WHERE event LIKE 'proposal%'", ct));
            Assert.Equal("person:ann|web|" + id, await ScalarAsync(connection,
                "SELECT actor || '|' || (detail->>'surface') || '|' || (detail->>'proposal_id') FROM config.investigation_journal WHERE event = 'proposal_first_press'", ct));

            /* Superseded from pending, with what changed. */
            var other = (await store.CreateProposalAsync(Draft(s_request, "recompile_module", new JsonObject { ["database"] = "Sales", ["module"] = "dbo.p" }), ct)).ProposalId;
            Assert.True(await store.SupersedeProposalAsync(other, "person:ann", new JsonObject { ["changed"] = "statement" }, ct));
            Assert.Equal(ProposalStates.Superseded, (await store.GetProposalAsync(other, ct))!.State);
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'proposal_superseded' AND detail->>'changed' = 'statement'", ct));
        });
    }

    [Fact]
    public async Task TheSweep_VoidsOnClose_VoidsOnlyAResolvedKill_Expires_AndMarksAStaleApprovalUnknown()
    {
        await WithStoreAsync(async (connection, scratch, ct) =>
        {
            await AddRequestAsync(connection, s_request, "claimed", ct);
            await AddRequestAsync(connection, s_closed, "closed", ct);
            await AddRequestAsync(connection, s_resolved, "reported", ct);
            await ExecAsync(connection, $"INSERT INTO config.investigation_journal (request_id, event, actor) VALUES ('{s_resolved}', 'resolved', 'service')", ct);

            var clock = DateTime.UtcNow;
            var store = new InvestigationRequestStore(NpgsqlDataSource.Create(scratch.ConnectionString), 30, () => clock);
            var kill = new JsonObject { ["session_id"] = 87 };
            var onClosed = (await store.CreateProposalAsync(Draft(s_closed, "force_query_store_plan", Force(5)), ct)).ProposalId;
            var resolvedKill = (await store.CreateProposalAsync(Draft(s_resolved, "kill_session", kill), ct)).ProposalId;
            var resolvedForce = (await store.CreateProposalAsync(Draft(s_resolved, "force_query_store_plan", Force(5)), ct)).ProposalId;
            var live = (await store.CreateProposalAsync(Draft(s_request, "kill_session", kill), ct)).ProposalId;
            var approved = (await store.CreateProposalAsync(Draft(s_request, "force_query_store_plan", Force(5)), ct)).ProposalId;
            Assert.True(await store.RecordFirstPressAsync(approved, "person:ann", new JsonObject(), null, ct));
            Assert.True(await store.ApproveProposalAsync(approved, "person:ann", null, ct));

            var first = await store.SweepAsync(ct);
            Assert.Equal(2, first.Proposals);
            Assert.Equal(ProposalStates.Voided, (await store.GetProposalAsync(onClosed, ct))!.State);
            Assert.Equal(ProposalStates.Voided, (await store.GetProposalAsync(resolvedKill, ct))!.State);
            Assert.Equal(ProposalStates.Pending, (await store.GetProposalAsync(resolvedForce, ct))!.State);
            Assert.Equal(ProposalStates.Pending, (await store.GetProposalAsync(live, ct))!.State);
            Assert.Equal(ProposalStates.Approved, (await store.GetProposalAsync(approved, ct))!.State);
            Assert.Equal("alert_resolved", await ScalarAsync(connection, $"SELECT outcome->>'reason' FROM config.agent_action_proposals WHERE proposal_id = '{resolvedKill}'", ct));

            /* Ten minutes after the approval with no outcome: unknown. The pending ones are still inside their hour. */
            clock = clock.AddMinutes(ProposalRules.ApprovedOutcomeMinutes + 1);
            Assert.Equal(1, (await store.SweepAsync(ct)).Proposals);
            Assert.Equal(ProposalStates.Unknown, (await store.GetProposalAsync(approved, ct))!.State);

            /* Past the hour, both pending proposals expire, and a press then finds nothing. */
            clock = clock.AddMinutes(ProposalRules.ExpiryMinutes);
            Assert.Equal(2, (await store.SweepAsync(ct)).Proposals);
            Assert.Equal(ProposalStates.Expired, (await store.GetProposalAsync(live, ct))!.State);
            Assert.Equal(ProposalStates.Expired, (await store.GetProposalAsync(resolvedForce, ct))!.State);
            Assert.False(await store.RecordFirstPressAsync(live, "person:ann", new JsonObject(), null, ct));
            Assert.Equal(0, (await store.SweepAsync(ct)).Proposals);

            Assert.Equal("proposal_expired:2,proposal_unknown:1,proposal_voided:2", await ScalarAsync(connection,
                "SELECT string_agg(event || ':' || n, ',' ORDER BY event) FROM (SELECT event, count(*) AS n FROM config.investigation_journal "
                + "WHERE event IN ('proposal_expired', 'proposal_unknown', 'proposal_voided') AND actor = 'service' GROUP BY event) AS e", ct));
        });
    }

    [Fact]
    public async Task TheRung_SetsOffOnlyOnLevel3Routes_AndARerunKeepsAPersonsRequired()
    {
        await WithStoreAsync(async (connection, scratch, ct) =>
        {
            /* Back behind the rung, with one route at each level. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 237, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 238", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('L0', 0), ('L1', 1), ('L2', 2), ('L3', 3), ('none', NULL)", ct);

            Assert.Equal(StorageVersion.SchemaVersion - 237, await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal("L0=required/false,L1=required/false,L2=required/false,L3=off/false,none=required/false", await ScalarAsync(connection,
                "SELECT string_agg(metric_match || '=' || action_approval || '/' || approval_single_person, ',' ORDER BY metric_match) FROM config.config_notification_routes", ct));
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.agent_action_proposals') IS NOT NULL", ct));

            /* A new route, and one a person raised to Level 3 later, get approval. */
            await ExecAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('new', 3)", ct);
            Assert.Equal("required", await ScalarAsync(connection, "SELECT action_approval FROM config.config_notification_routes WHERE metric_match = 'new'", ct));

            /* A person sets approval on the Level 3 route; a re-run of the rung does not turn it off again. */
            await ExecAsync(connection, "UPDATE config.config_notification_routes SET action_approval = 'required' WHERE metric_match = 'L3'", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 238", ct);
            Assert.Equal(StorageVersion.SchemaVersion - 237, await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal("required", await ScalarAsync(connection, "SELECT action_approval FROM config.config_notification_routes WHERE metric_match = 'L3'", ct));

            var bad = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(connection, "UPDATE config.config_notification_routes SET action_approval = 'maybe'", ct));
            Assert.Equal("23514", bad.SqlState);
        });
    }
}

/// <summary>
/// #7109: only the store owner writes the proposals table and journals the proposal events, in both grant sources (the managed batch and
/// the bring-your-own <c>provision-roles.sql</c> step 3j4) and from the rung alone, as the real <c>admin</c> and <c>mcp</c> logins. The
/// fixed-name roles are cluster-wide, so the class is in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class ActionProposalGrantLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/mcp roles are why the class
       sits in the pg-cluster-roles collection. */
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];
    private static readonly Guid s_request = Guid.Parse("71090000-0000-0000-0000-0000000000a1");

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the action proposal grant live tests.");
        return connectionString!;
    }

    private static async Task<NpgsqlConnection> LoginAsync(ScratchPostgres scratch, string role, string password, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ActionProposalStoreLiveTests.ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    /// <summary>admin, viewer and web read the table and mcp does not, none writes it, and neither login can write a proposal or any proposal event.</summary>
    private static async Task AssertOwnerOnlyAsync(NpgsqlConnection owner, ScratchPostgres scratch, string adminPassword, string mcpPassword, CancellationToken ct)
    {
        foreach (var role in s_cleanupRoles)
        {
            var held = string.Join(",", new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" }
                .Where(verb => ActionProposalStoreLiveTests.ScalarAsync(owner,
                    $"SELECT has_table_privilege('{role}', 'config.agent_action_proposals', '{verb}')", ct).GetAwaiter().GetResult() == "True"));
            Assert.Equal(role == "mcp" ? "" : "SELECT", held);
        }

        await ActionProposalStoreLiveTests.AddRequestAsync(owner, s_request, "claimed", ct);
        var store = new InvestigationRequestStore(NpgsqlDataSource.Create(scratch.ConnectionString), 30);
        var id = (await store.CreateProposalAsync(ActionProposalStoreLiveTests.Draft(s_request, "kill_session", new JsonObject { ["session_id"] = 87 }), ct)).ProposalId;
        var journalBefore = await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);

        foreach (var (role, password) in new[] { ("admin", adminPassword), ("mcp", mcpPassword) })
        {
            await using var login = await LoginAsync(scratch, role, password, ct);
            await AssertDeniedAsync(login,
                "INSERT INTO config.agent_action_proposals (proposal_id, request_id, claim_no, agent, tool, server_id, target, target_key, binding, proposed_at, expires_at) "
                + $"VALUES (gen_random_uuid(), '{s_request}', 1, 'eve', 'kill_session', 7, '{{}}', repeat('a', 64), '{{}}', now(), now() + interval '1 hour')", ct);
            await AssertDeniedAsync(login, $"UPDATE config.agent_action_proposals SET state = 'approved', decided_by = 'person:eve' WHERE proposal_id = '{id}'", ct);
            await AssertDeniedAsync(login, "DELETE FROM config.agent_action_proposals", ct);
            if (role == "mcp")
                await AssertDeniedAsync(login, "SELECT count(*) FROM config.agent_action_proposals", ct);
            else
                Assert.Equal("1", await ActionProposalStoreLiveTests.ScalarAsync(login, "SELECT count(*) FROM config.agent_action_proposals", ct));

            /* The journal trigger keeps every proposal event to the owner, so neither login can write a false proposal_approved row, nor
               the notice mark first, which would stop the proposal's notice, nor an asker row, which would block an approver. */
            foreach (var kind in ProposalEvents.OwnerOnly.Append(InvestigationRequestStore.ProposalNoticeSent).Append(InvestigationRequestStore.AskerJournalEvent))
            {
                var journal = await Assert.ThrowsAsync<PostgresException>(() => ActionProposalStoreLiveTests.ExecAsync(login,
                    $"INSERT INTO config.investigation_journal (request_id, event, actor, detail) VALUES ('{s_request}', '{kind}', 'person:eve', '{{}}'::jsonb)", ct));
                Assert.Equal("42501", journal.SqlState);
                Assert.Contains("Only the Darling service journals the request event " + kind, journal.MessageText, StringComparison.Ordinal);
            }

            /* The store on the login's own data source is refused too, and changes nothing. */
            await using var source = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString);
            var refused = await Assert.ThrowsAsync<PostgresException>(() => new InvestigationRequestStore(source, 30).ApproveProposalAsync(id, "person:eve", null, ct));
            Assert.Equal("42501", refused.SqlState);
        }

        Assert.Equal("pending", await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT string_agg(state, ',') FROM config.agent_action_proposals", ct));
        Assert.Equal(journalBefore, await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
    }

    private static async Task<NpgsqlConnection> MigratedOwnerAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        var exist = await ActionProposalStoreLiveTests.ScalarAsync(owner,
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
        Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this cluster; the grants would adopt it.");
        return owner;
    }

    private static Task ManagedBatchAsync(NpgsqlConnection owner, ScratchPostgres scratch, CancellationToken ct) =>
        ActionProposalStoreLiveTests.ExecAsync(owner,
            DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All,
                ProvisioningTarget.ComposeStore(new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling", scratch.DatabaseName)), ct);

    private static async Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "proposal7109none", cleanupCt));

    [Fact]
    public async Task TheManagedBatch_LeavesTheProposalsToTheOwner()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await MigratedOwnerAsync(scratch, ct);
            await ManagedBatchAsync(owner, scratch, ct);
            await AssertOwnerOnlyAsync(owner, scratch, ProvisioningTestSecrets.AdminPassword, ProvisioningTestSecrets.McpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheBringYourOwnScript_LeavesTheProposalsToTheOwner_AndSkipsOnAStoreBehindTheRung()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await MigratedOwnerAsync(scratch, ct);
            Assert.SkipUnless(await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct) == "True",
                "The script names the store owner 'darling', and this cluster has no such role.");

            const string AdminPassword = "ScriptAdmin7109", ViewerPassword = "ScriptViewer7109", WebPassword = "ScriptWeb7109", McpPassword = "ScriptMcp7109";
            var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
                    System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
                .Replace("ON DATABASE darling", $"ON DATABASE \"{scratch.DatabaseName}\"", StringComparison.Ordinal)
                .Replace("CHANGE_ME_ADMIN_PASSWORD", AdminPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_VIEWER_PASSWORD", ViewerPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_WEB_PASSWORD", WebPassword, StringComparison.Ordinal)
                .Replace("CHANGE_ME_MCP_PASSWORD", McpPassword, StringComparison.Ordinal);

            /* Behind the rung the script warns, skips step 3j4 and still runs to the end. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, 237, ct);
            await ActionProposalStoreLiveTests.ExecAsync(owner, script, ct);
            Assert.Equal("4", await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp')", ct));

            await ActionProposalStoreLiveTests.ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version >= 238", ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await ActionProposalStoreLiveTests.ExecAsync(owner, script, ct);
            await AssertOwnerOnlyAsync(owner, scratch, AdminPassword, McpPassword, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>The rung takes the writes back itself: on a store whose roles and default privileges exist but whose provisioning has not run
    /// again since V238, no role holds more than SELECT on the table, and mcp holds none.</summary>
    [Fact]
    public async Task TheRung_TakesTheWritesBack_BeforeProvisioningRunsAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await MigratedOwnerAsync(scratch, ct);
            await ManagedBatchAsync(owner, scratch, ct);
            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, 237, ct);
            await ActionProposalStoreLiveTests.ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version >= 238", ct);
            await PgMigrations.MigrateAsync(owner, ct);

            foreach (var role in s_cleanupRoles)
            {
                foreach (var verb in new[] { "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" })
                    Assert.Equal("False", await ActionProposalStoreLiveTests.ScalarAsync(owner,
                        $"SELECT has_table_privilege('{role}', 'config.agent_action_proposals', '{verb}')", ct));
            }

            Assert.Equal("False", await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT has_table_privilege('mcp', 'config.agent_action_proposals', 'SELECT')", ct));
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }
}
