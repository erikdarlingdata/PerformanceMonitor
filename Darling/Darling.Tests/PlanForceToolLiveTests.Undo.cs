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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* The undo half of the plan force live tests (#5632, step 9): the same store and SQL Server setup as the force tool's tests (a partial class, so the
   private setup is shared). Each test makes its own scratch store and SQL Server database. */
public sealed partial class PlanForceToolLiveTests
{
    /* A write needs the statement that was shown. A confirm here without one shows it first, as the page and the Viewer do, then confirms
       that statement. A refusal at the first step is the answer. RawUndoAsync sends exactly what it is given. */
    private static async Task<PlanUndoAnswer> UndoAsync(Ctx c, long calledId, bool confirm, string login = "alice", string? shown = null)
    {
        if (confirm && shown is null)
        {
            var first = await RawUndoAsync(c, calledId, confirm: false, login, shown: null);
            if (first.Status != PlanUndoAnswer.Confirm)
                return first;
            shown = first.Statement;
        }

        return await RawUndoAsync(c, calledId, confirm, login, shown);
    }

    private static Task<PlanUndoAnswer> RawUndoAsync(Ctx c, long calledId, bool confirm, string login, string? shown) =>
        DarlingMcpPlanForceTools.UndoPlanChangeAsync(new AgentChangeUndo(c.Source, 30), c.Source, c.Log, calledId, login, confirm, shown, c.Ct);

    private static async Task<long> LastCalledIdAsync(Ctx c) =>
        long.Parse(await PgScalarAsync(c.Connection, "SELECT max(journal_id) FROM config.investigation_journal WHERE event = 'action_called'", c.Ct) ?? "0", CultureInfo.InvariantCulture);

    /// <summary>Journals a call by hand, as the tool would, with the state the agent read before it wrote. <paramref name="result"/> is done, failed or none (unknown).</summary>
    private static async Task<long> SeedCallAsync(Ctx c, string kind, long queryId, long planId, long? beforePlan, string beforeType, string result)
    {
        var request = await PgScalarAsync(c.Connection, "SELECT request_id FROM config.investigation_requests LIMIT 1", c.Ct);
        var before = new JsonObject
        {
            ["database"] = c.Sql.Database, ["query_id"] = queryId, ["plan_id"] = planId,
            ["forced_plan"] = beforePlan is null ? null : new JsonObject { ["plan_id"] = beforePlan, ["forcing_type"] = beforeType },
        };
        var detail = new JsonObject
        {
            ["tool"] = kind == ForceKind ? "force_query_store_plan" : "unforce_query_store_plan", ["kind"] = kind,
            ["target"] = new JsonObject { ["id"] = planId.ToString(CultureInfo.InvariantCulture), ["server_id"] = 7, ["database"] = c.Sql.Database },
            ["before"] = before,
        };
        var id = await InsertJournalAsync(c, request!, "action_called", "agent:agent-a", detail.ToJsonString());
        if (result != "none")
            await InsertJournalAsync(c, request!, "action_" + result, "agent:agent-a", new JsonObject { ["called_id"] = id }.ToJsonString());
        return id;
    }

    private static async Task<long> InsertJournalAsync(Ctx c, string request, string journalEvent, string actor, string detail)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, detail) VALUES (now(), $1::uuid, 7, $2, $3, $4::jsonb) RETURNING journal_id", c.Connection);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = request, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = journalEvent, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = actor, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = detail, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        return (long)(await command.ExecuteScalarAsync(c.Ct))!;
    }

    private static async Task AssertUndoneRowAsync(Ctx c, long calledId, string kind)
    {
        var row = Assert.Single(await RowsAsync(c, "action_undone"));
        Assert.Equal(calledId, (long)row["called_id"]!);
        Assert.Equal(JsonValueKind.Number, row["called_id"]!.GetValueKind());
        Assert.Equal(kind, (string?)row["kind"]);
        Assert.NotNull(row["replaced"]);
        Assert.NotNull(row["statement"]);
        Assert.Equal(await PgScalarAsync(c.Connection, "SELECT request_id::text FROM config.investigation_journal WHERE journal_id = " + calledId.ToString(CultureInfo.InvariantCulture), c.Ct),
            await PgScalarAsync(c.Connection, "SELECT request_id::text FROM config.investigation_journal WHERE event = 'action_undone'", c.Ct));
        Assert.Equal("7", await PgScalarAsync(c.Connection, "SELECT server_id FROM config.investigation_journal WHERE event = 'action_undone'", c.Ct));
        Assert.Equal("person:alice", await PgScalarAsync(c.Connection, "SELECT actor FROM config.investigation_journal WHERE event = 'action_undone'", c.Ct));
    }

    [Fact]
    public async Task UndoOfAForce_WithNothingForcedBefore_ShowsTheStatementFirst_ThenUnforces_AndTheListAndStepEightReadIt()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pu1", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);

            var shown = await UndoAsync(c, called, confirm: false);
            Assert.Equal("confirm", shown.Status);
            Assert.Equal($"EXEC [{c.Sql.Database}].sys.sp_query_store_unforce_plan @query_id = {query}, @plan_id = {planA};", shown.Statement);
            Assert.Equal(planA, (long)shown.State!["forced_plan"]!["plan_id"]!);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));

            /* A confirm without the shown statement writes nothing: it answers the confirm step again. */
            var bare = await RawUndoAsync(c, called, confirm: true, "alice", shown: null);
            Assert.Equal("confirm", bare.Status);
            Assert.Equal(shown.Statement, bare.Statement);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));

            var undone = await UndoAsync(c, called, confirm: true, shown: shown.Statement);
            Assert.Equal("undone", undone.Status);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            await AssertUndoneRowAsync(c, called, ForceKind);
            Assert.Null((await RowsAsync(c, "action_undone"))[0]["restored_as"]);

            var row = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct)).Changes);
            Assert.Equal("person:alice", row.UndoneBy);
            Assert.False(row.CanTryUndo(DateTime.UtcNow));
            Assert.False(row.ForceReview!.Running, "step 8's review mark stops once the force is undone");

            var again = await UndoAsync(c, called, confirm: true);
            Assert.Equal("already_undone", again.Code);
            Assert.Single(await RowsAsync(c, "action_undone"));
            AssertNoSecret(c, shown.Message, undone.Message, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task UndoOfAForce_ThatReplacedAnotherPlan_ForcesTheOldPlanAgain()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, planB) = await c.Sql.TwoPlansAsync("pu2", c.Ct);
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planB, dryRun: false))["status"]);
            Assert.Equal(planB, await c.Sql.ForcedPlanAsync(query, c.Ct));

            var answer = await UndoAsync(c, await LastCalledIdAsync(c), confirm: true);
            Assert.Equal("undone", answer.Status);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Contains("sp_query_store_force_plan", answer.Statement, StringComparison.Ordinal);
            var detail = Assert.Single(await RowsAsync(c, "action_undone"));
            Assert.Equal(planB, (long)detail["replaced"]!["forced_plan"]!["plan_id"]!);
            Assert.Equal(planA, (long)detail["restored"]!["forced_plan"]!["plan_id"]!);
        });
    }

    [Fact]
    public async Task UndoOfAForce_ThatReplacedAnAutomaticForce_ComesBackAsAManualForce_AndSaysSo()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, planB) = await c.Sql.TwoPlansAsync("pu3", c.Ct);
            await c.Sql.ForceDirectlyAsync(query, planB, c.Ct);
            var called = await SeedCallAsync(c, ForceKind, query, planB, planA, "AUTO", "done");

            var answer = await UndoAsync(c, called, confirm: true);
            Assert.Equal("undone", answer.Status);
            Assert.Equal("manual", answer.RestoredAs);
            Assert.Contains("manual", answer.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal("manual", (string?)(await RowsAsync(c, "action_undone"))[0]["restored_as"]);
        });
    }

    [Fact]
    public async Task UndoOfAnUnforce_ForcesThePlanAgain_OnlyWhileNoPlanOfTheQueryIsForced()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, planB) = await c.Sql.TwoPlansAsync("pu4", c.Ct);
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: false, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);

            /* Another plan forced since: refused, and the words name it. */
            await c.Sql.ForceDirectlyAsync(query, planB, c.Ct);
            var refused = await UndoAsync(c, called, confirm: true);
            Assert.Equal("changed_since", refused.Code);
            Assert.Contains($"plan {planB}", refused.Message, StringComparison.Ordinal);
            Assert.Equal(planB, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND actor = 'person:alice' AND reason = 'changed_since'", c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));

            await using (var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
                await ExecAsync(admin, $"EXEC sys.sp_query_store_unforce_plan @query_id = {query}, @plan_id = {planB};", c.Ct);
            var undone = await UndoAsync(c, called, confirm: true);
            Assert.Equal("undone", undone.Status);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            await AssertUndoneRowAsync(c, called, UnforceKind);
        });
    }

    [Fact]
    public async Task UndoOfAForce_WhenTheForceWasRemovedByHand_IsRefusedChangedSince_AndWritesNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pu5", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            await using (var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
                await ExecAsync(admin, $"EXEC sys.sp_query_store_unforce_plan @query_id = {query}, @plan_id = {planA};", c.Ct);
            var answer = await UndoAsync(c, await LastCalledIdAsync(c), confirm: true);
            Assert.Equal("refused", answer.Status);
            Assert.Equal("changed_since", answer.Code);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));
        });
    }

    [Fact]
    public async Task AFailedRow_WhoseWriteTookEffect_CanBeUndone_AndOneWhoseWriteDidNotHappen_IsNothingToUndo()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pu6", c.Ct);
            var failed = await SeedCallAsync(c, ForceKind, query, planA, null, "", "failed");
            var unknown = await SeedCallAsync(c, ForceKind, query, planA, null, "", "none");

            var nothing = await UndoAsync(c, failed, confirm: true);
            Assert.Equal("nothing_to_undo", nothing.Code);
            Assert.Equal("nothing_to_undo", (await UndoAsync(c, unknown, confirm: true)).Code);
            Assert.Empty(await RowsAsync(c, "action_undone"));

            /* The store commit failed after the server took the force: the journal says failed, the server says forced. */
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            var worked = await UndoAsync(c, failed, confirm: true);
            Assert.Equal("undone", worked.Status);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal(AgentChangeResult.Failed, (await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct)).Changes.Single(x => x.CalledId == failed).Result);
        });
    }

    [Fact]
    public async Task WithoutConfirm_NothingIsWritten_AndAMovedStatementIsShownAgain()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pu7", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var stale = await UndoAsync(c, called, confirm: true, shown: "EXEC something else;");
            Assert.Equal("confirm", stale.Status);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));
            Assert.Equal("0", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE actor = 'person:alice'", c.Ct));
        });
    }

    [Fact]
    public async Task WithTheServersSwitchOff_TheUndoIsRefusedWithWordsThatSayHowToTurnItOn_JournaledWithThePerson_AndNothingIsWritten()
    {
        await RunAsync(Without.Switch, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pu8", c.Ct);
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            var called = await SeedCallAsync(c, ForceKind, query, planA, null, "", "done");
            var answer = await UndoAsync(c, called, confirm: true);
            Assert.Equal("server_action_off", answer.Code);
            Assert.Contains("config_server_actions", answer.Message, StringComparison.Ordinal);
            Assert.Contains("SQL Server", answer.Message, StringComparison.Ordinal);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND actor = 'person:alice' AND reason = 'server_action_off'", c.Ct));
            AssertNoSecret(c, answer.Message, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task WithoutTheCredentialSlot_TheUndoIsRefused_AndNothingIsWritten()
    {
        await RunAsync(Without.CredentialSlot, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pu9", c.Ct);
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            var called = await SeedCallAsync(c, ForceKind, query, planA, null, "", "done");
            var answer = await UndoAsync(c, called, confirm: true);
            Assert.Equal("no_remediation_credential", answer.Code);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND actor = 'person:alice' AND reason = 'no_remediation_credential'", c.Ct));
        });
    }

    [Fact]
    public async Task APersonsUndo_DoesNotNeedTheAgentConditions_NoLevelThreeRoute_AndAgentChangesSwitchedOff()
    {
        await RunAsync(Without.Level, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pu10", c.Ct);
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            await PgExecAsync(c.Connection, "UPDATE config.config_service SET agent_actions_enabled = false", c.Ct);
            var called = await SeedCallAsync(c, ForceKind, query, planA, null, "", "done");
            Assert.Equal("undone", (await UndoAsync(c, called, confirm: true)).Status);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
        });
    }

    [Fact]
    public async Task AnUndo_OfAnotherKind_OrAnUnknownCall_OrABadLogin_IsRefusedWithoutTouchingTheServer()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            Assert.Equal("not_found", (await UndoAsync(c, 987654, confirm: true)).Code);
            /* A journal row this test makes, so the login refusal is checked whatever ran before it (a fresh store has no row 1). The login holds a
               control character, which a person's name never does ("bad login!" is a valid name: inner spaces and ! are allowed). */
            var seeded = await SeedCallAsync(c, ForceKind, 1, 1, null, "", "done");
            Assert.Equal("bad_login", (await UndoAsync(c, seeded, confirm: true, login: "bad\u0001login")).Code);
            var request = await PgScalarAsync(c.Connection, "SELECT request_id FROM config.investigation_requests LIMIT 1", c.Ct);
            var mute = await InsertJournalAsync(c, request!, "action_called", "agent:agent-a", new JsonObject { ["kind"] = "mute" }.ToJsonString());
            Assert.Equal("not_undoable", (await UndoAsync(c, mute, confirm: true)).Code);
        });
    }

    /// <summary>The Viewer's path (#5632, step 9, rule 8): a queued undo_plan_change row, claimed and run by the service's executor, answers in result_json.</summary>
    [Fact]
    public async Task TheViewersCommand_RunsTheSameUndo_AsThePerson_ShowingTheStatementFirst_AndWritesOnlyOnTheSecondCommand()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pu11", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var executor = new PerformanceMonitor.Darling.Service.DarlingCommandExecutor(c.Source, new NoCommandHost(), "test-instance", c.Log);

            var first = await RunCommandAsync(c, executor, "{\"called_id\": " + called + ", \"confirm\": false}", "alice");
            Assert.Equal("confirm", first.Status);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));

            var second = await RunCommandAsync(c, executor,
                new JsonObject { ["called_id"] = called, ["confirm"] = true, ["statement"] = first.Statement }.ToJsonString(), "alice");
            Assert.Equal("undone", second.Status);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            await AssertUndoneRowAsync(c, called, ForceKind);

            var again = await RunCommandAsync(c, executor, "{\"called_id\": " + called + ", \"confirm\": true}", "alice");
            Assert.Equal("already_undone", again.Code);
            AssertNoSecret(c, await PgScalarAsync(c.Connection, "SELECT coalesce(string_agg(result_json::text, ' '), '') FROM config.config_command", c.Ct) ?? "", await JournalTextAsync(c));
        });
    }

    private static async Task<PlanUndoAnswer> RunCommandAsync(Ctx c, PerformanceMonitor.Darling.Service.DarlingCommandExecutor executor, string args, string requestedBy)
    {
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO config.config_command (requested_by, command_type, args_json, status) VALUES ($1, 'undo_plan_change', $2::jsonb, 'pending')", c.Connection))
        {
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = requestedBy, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = args, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            await insert.ExecuteNonQueryAsync(c.Ct);
        }

        Assert.True(await executor.PollOnceAsync(c.Ct));
        var json = await PgScalarAsync(c.Connection, "SELECT result_json::text FROM config.config_command ORDER BY command_id DESC LIMIT 1", c.Ct);
        return PlanUndoAnswer.TryRead(json) ?? throw new InvalidOperationException("The command left no answer: " + json);
    }

    private sealed class NoCommandHost : PerformanceMonitor.Darling.Service.IDarlingCommandHost
    {
        private static Task<PerformanceMonitor.Darling.Service.CommandOutcome> No() => throw new InvalidOperationException("the undo does not use the host");

        public Task<PerformanceMonitor.Darling.Service.CommandOutcome> SnapshotNowAsync(int serverId, IReadOnlyList<string>? collectors, CancellationToken cancellationToken) => No();

        public Task<PerformanceMonitor.Darling.Service.CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken) => No();

        public Task<PerformanceMonitor.Darling.Service.CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken) => No();

        public Task<PerformanceMonitor.Darling.Service.CommandOutcome> FetchPlanAsync(int serverId, PerformanceMonitor.Darling.Service.PlanFetchRequest request, CancellationToken cancellationToken) => No();

        public Task<PerformanceMonitor.Darling.Service.CommandOutcome> ExecuteActualPlanAsync(int serverId, PerformanceMonitor.Darling.Service.ActualPlanRequest request, CancellationToken cancellationToken) => No();

        public Task<PerformanceMonitor.Darling.Service.CommandOutcome> TestHypotheticalIndexAsync(int serverId, PerformanceMonitor.Darling.Service.HypotheticalIndexRequest request, CancellationToken cancellationToken) => No();

        public Task<PerformanceMonitor.Darling.Service.CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken) => No();
    }

    [Fact]
    public void TheDecision_FollowsTheServersState_NotTheJournalsResult()
    {
        var facts = new PlanChangeFacts(1, Guid.NewGuid(), 7, "srv-7", ForceKind, "db", 10, 20, 30, "MANUAL", AgentChangeResult.Failed, null);
        PerformanceMonitor.Darling.Remediation.PlanForceState State(long? forced) => new("db", 10, 20, null,
            forced is null ? null : new PerformanceMonitor.Darling.Remediation.ForcedPlanState(forced.Value, "MANUAL", null, null));
        Assert.True(DarlingMcpPlanForceTools.PlanUndoDecision.Decide(facts, State(20)).Step!.Force);
        Assert.Equal(30, DarlingMcpPlanForceTools.PlanUndoDecision.Decide(facts, State(20)).Step!.PlanId);
        Assert.Equal("nothing_to_undo", DarlingMcpPlanForceTools.PlanUndoDecision.Decide(facts, State(30)).Refusal!.Reason);
        Assert.Equal("changed_since", DarlingMcpPlanForceTools.PlanUndoDecision.Decide(facts, State(99)).Refusal!.Reason);
        Assert.Equal("changed_since", DarlingMcpPlanForceTools.PlanUndoDecision.Decide(facts with { Result = AgentChangeResult.Done }, State(null)).Refusal!.Reason);
    }
}
