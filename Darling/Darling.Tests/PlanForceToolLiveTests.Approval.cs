/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* The approval path of the SQL Server tools (#7109, slice 4c), on the plan force live setup (a partial class, so the private setup is shared).
   Each tool: the agent's call stores a proposal, a person presses twice, and the change is made on the test server. One case each shows a
   change between the proposal and the press ends the proposal superseded with nothing written. The route is single-person, so an
   administrator path approves and no person rows are needed. These need SQL Server and run in CI. */
public sealed partial class PlanForceToolLiveTests
{
    private static readonly ProposalApprover Approver = ProposalApprover.Administrator("web_token");

    /// <summary>Turns single-person approval on for the setup's route, so the administrator path may approve.</summary>
    private static Task SinglePersonAsync(Ctx c) =>
        PgExecAsync(c.Connection, "UPDATE config.config_notification_routes SET approval_single_person = TRUE", c.Ct);

    private static ActionProposalPress Press(Ctx c) => new(c.Source, c.Log);

    private static Guid ProposalIdOf(string answer)
    {
        var json = Parse(answer);
        Assert.True("proposed" == (string?)json["status"], answer);
        return Guid.Parse((string)json["proposal_id"]!);
    }

    private static async Task<string?> ProposalStateAsync(Ctx c, Guid proposalId) =>
        await PgScalarAsync(c.Connection, $"SELECT state FROM config.agent_action_proposals WHERE proposal_id = '{proposalId}'", c.Ct);

    /// <summary>Both presses, and the confirm's state: <c>run</c> when the write was made.</summary>
    private static async Task<ProposalPressResult> ApproveAsync(Ctx c, Guid proposalId)
    {
        var press = Press(c);
        var first = await press.FirstPressAsync(proposalId, Approver, c.Ct);
        Assert.True("first_pressed" == first.Status, first.Code + " " + first.Message);
        return await press.ConfirmAsync(proposalId, Approver, c.Ct);
    }

    /// <summary>A first press after the facts changed: <c>superseded</c>, the proposal ended, and no <c>action_</c> row.</summary>
    private static async Task AssertSupersededAsync(Ctx c, Guid proposalId)
    {
        var first = await Press(c).FirstPressAsync(proposalId, Approver, c.Ct);
        Assert.True("superseded" == first.Status, first.Code + " " + first.Message);
        Assert.Equal("superseded", await ProposalStateAsync(c, proposalId));
        Assert.Equal(0, await JournalActionRowsAsync(c));
    }

    private static async Task<JsonObject> CalledAsync(Ctx c)
    {
        var called = Assert.Single(await RowsAsync(c, "action_called"));
        Assert.NotNull(called["approved_by"]);
        return called;
    }

    [Fact]
    public async Task AnApprovedForce_PlacesThePlan_AndJournalsThePlanStateTheUndoReads()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pa1", c.Ct);
            var proposalId = ProposalIdOf(await c.Tool(force: true, query, planA, dryRun: false));
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));

            var confirmed = await ApproveAsync(c, proposalId);
            Assert.True("run" == confirmed.Status, confirmed.Code + " " + confirmed.Message);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal("run", await ProposalStateAsync(c, proposalId));

            /* The journal's before image is the plan state, as the agent's own force journals it, so the undo and the post-force checks read it. */
            var called = await CalledAsync(c);
            Assert.Equal(query, (long)called["before"]!["query_id"]!);
            Assert.Null(called["before"]!["forced_plan"]);
            var done = Assert.Single(await RowsAsync(c, "action_done"));
            Assert.Equal(planA, (long)done["after"]!["state"]!["forced_plan"]!["plan_id"]!);
            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct)).Changes);
            Assert.True(change.CanTryUndo(DateTime.UtcNow), "an approved force can be undone from the list");
        }, approval: "required");
    }

    [Fact]
    public async Task AnApprovedUnforce_RemovesTheForce()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pa2", c.Ct);
            await using (var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
                await ExecAsync(admin, $"EXEC sys.sp_query_store_force_plan @query_id = {query}, @plan_id = {planA};", c.Ct);
            var proposalId = ProposalIdOf(await c.Tool(force: false, query, planA, dryRun: false));
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));

            var confirmed = await ApproveAsync(c, proposalId);
            Assert.True("run" == confirmed.Status, confirmed.Code + " " + confirmed.Message);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
        }, approval: "required");
    }

    [Fact]
    public async Task AForceProposal_WhoseQueryGotAnotherForcedPlan_EndsSuperseded_AndForcesNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var (query, planA, planB) = await c.Sql.TwoPlansAsync("pa3", c.Ct);
            var proposalId = ProposalIdOf(await c.Tool(force: true, query, planA, dryRun: false));
            await using (var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
                await ExecAsync(admin, $"EXEC sys.sp_query_store_force_plan @query_id = {query}, @plan_id = {planB};", c.Ct);

            await AssertSupersededAsync(c, proposalId);
            Assert.Equal(planB, await c.Sql.ForcedPlanAsync(query, c.Ct));
        }, approval: "required");
    }

    [Fact]
    public async Task AnApprovedEvict_RemovesTheHandle_AndAnEvictOfAHandleThatLeftTheCache_EndsSuperseded()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var marker = "ev4" + Guid.NewGuid().ToString("N")[..8];
            await RunMarkerAsync(c.Sql, marker, 3, c.Ct);
            var cached = await ReadCachedAsync(marker, c.Ct);
            await SeedStatsAsync(c, cached, DateTime.UtcNow.AddMinutes(-5), cached.Executions);
            var proposalId = ProposalIdOf(await EvictAsync(c, cached.PlanHandle, dryRun: false));
            Assert.True(await IsCachedAsync(c.Sql, cached.PlanHandle, c.Ct));

            var confirmed = await ApproveAsync(c, proposalId);
            Assert.True("run" == confirmed.Status, confirmed.Code + " " + confirmed.Message);
            Assert.False(await IsCachedAsync(c.Sql, cached.PlanHandle, c.Ct));
            Assert.Equal(cached.PlanHandle, (string?)(await CalledAsync(c))["target"]!["id"], StringComparer.OrdinalIgnoreCase);
        }, approval: "required");

        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var marker = "ev5" + Guid.NewGuid().ToString("N")[..8];
            await RunMarkerAsync(c.Sql, marker, 3, c.Ct);
            var cached = await ReadCachedAsync(marker, c.Ct);
            await SeedStatsAsync(c, cached, DateTime.UtcNow.AddMinutes(-5), cached.Executions);
            var proposalId = ProposalIdOf(await EvictAsync(c, cached.PlanHandle, dryRun: false));
            await using (var admin = await OpenAsync(c.Sql.Admin(), c.Ct))
                await ExecAsync(admin, $"DBCC FREEPROCCACHE ({cached.PlanHandle}) WITH NO_INFOMSGS;", c.Ct);

            await AssertSupersededAsync(c, proposalId);
        }, approval: "required");
    }

    [Fact]
    public async Task AnApprovedRecompile_DropsTheCachedPlan_AndARecompileOfADroppedModule_EndsSuperseded()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnRecompileOnAsync(c);
            var proposalId = ProposalIdOf(await RecompileAsync(c, $"[dbo].[{name}]", dryRun: false));
            Assert.Equal(3, await ProcedureExecutionsAsync(c, name));

            /* The permit's id is the module in its normal form: the executor refuses permit_mismatch without it. */
            var confirmed = await ApproveAsync(c, proposalId);
            Assert.True("run" == confirmed.Status, confirmed.Code + " " + confirmed.Message);
            Assert.Equal(0, await ProcedureExecutionsAsync(c, name));
            var called = await CalledAsync(c);
            Assert.Equal($"[dbo].[{name}]", (string?)called["target"]!["id"]);
            Assert.Equal(name, (string?)called["before"]!["name"]);
        }, approval: "required");

        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnRecompileOnAsync(c);
            var proposalId = ProposalIdOf(await RecompileAsync(c, $"[dbo].[{name}]", dryRun: false));
            await AdminExecAsync(c, $"DROP PROCEDURE dbo.[{name}];");

            await AssertSupersededAsync(c, proposalId);
        }, approval: "required");
    }

    [Fact]
    public async Task AnApprovedCreateGuide_MakesTheGuide_AndAGuideNameTakenSinceTheProposal_EndsSuperseded()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);
            var proposalId = ProposalIdOf(await CreateGuideAsync(c, guide, dryRun: false, moduleOrBatch: $"[dbo].[{name}]"));
            Assert.Null(await GuideColumnAsync(c, guide, "hints"));

            var confirmed = await ApproveAsync(c, proposalId);
            Assert.True("run" == confirmed.Status, confirmed.Code + " " + confirmed.Message);
            Assert.Equal("OPTION (RECOMPILE)", await GuideColumnAsync(c, guide, "hints"));
            Assert.Equal(guide, (string?)(await CalledAsync(c))["target"]!["id"]);
        }, approval: "required");

        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);
            var proposalId = ProposalIdOf(await CreateGuideAsync(c, guide, dryRun: false, moduleOrBatch: $"[dbo].[{name}]"));
            await MakeObjectGuideAsync(c, guide, name, hints: "OPTION (MAXDOP 1)");

            await AssertSupersededAsync(c, proposalId);
            Assert.Equal("OPTION (MAXDOP 1)", await GuideColumnAsync(c, guide, "hints"));
        }, approval: "required");
    }

    [Fact]
    public async Task AnApprovedDropGuide_DropsIt_AndAGuideDisabledSinceTheProposal_EndsSupersededNamingDisabled()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, guide, name);
            await TurnDropOnAsync(c);
            var proposalId = ProposalIdOf(await DropGuideAsync(c, guide, dryRun: false));
            Assert.NotNull(await GuideRowAsync(c, guide));

            var confirmed = await ApproveAsync(c, proposalId);
            Assert.True("run" == confirmed.Status, confirmed.Code + " " + confirmed.Message);
            Assert.Null(await GuideRowAsync(c, guide));
            var called = await CalledAsync(c);
            Assert.Equal(guide, (string?)called["target"]!["id"]);
            Assert.Equal(guide, (string?)called["before"]!["guide"]!["name"]);
        }, approval: "required");

        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, guide, name);
            await TurnDropOnAsync(c);
            var proposalId = ProposalIdOf(await DropGuideAsync(c, guide, dryRun: false));
            await AdminExecAsync(c, $"EXEC sys.sp_control_plan_guide N'DISABLE', N'{guide}';");

            var first = await Press(c).FirstPressAsync(proposalId, Approver, c.Ct);
            Assert.True("superseded" == first.Status, first.Code + " " + first.Message);
            Assert.Contains("disabled", first.Changed ?? []);
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.NotNull(await GuideRowAsync(c, guide));
        }, approval: "required");
    }

    [Fact]
    public async Task AnApprovedKill_EndsTheSession_AndASessionThatRanANewBatchSinceTheProposal_EndsSupersededNamingItsLastEndTime()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            await using var _ = victim.Connection;
            var proposalId = ProposalIdOf(await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun: false));
            Assert.True(await SessionAliveAsync(c, victim));

            var confirmed = await ApproveAsync(c, proposalId);
            Assert.True("run" == confirmed.Status, confirmed.Code + " " + confirmed.Message);
            Assert.False(await SessionAliveAsync(c, victim));
            Assert.Equal(victim.SessionId.ToString(System.Globalization.CultureInfo.InvariantCulture), (string?)(await CalledAsync(c))["target"]!["id"]);
        }, approval: "required");

        await RunAsync(Without.Nothing, async c =>
        {
            await SinglePersonAsync(c);
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            await using var _ = victim.Connection;
            var proposalId = ProposalIdOf(await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun: false));
            /* The session runs a new batch: its last_request_end_time moves, so the binding no longer matches. */
            await Task.Delay(50, c.Ct);
            await ScalarAsync(victim.Connection, "SELECT 1;", c.Ct);

            var first = await Press(c).FirstPressAsync(proposalId, Approver, c.Ct);
            Assert.True("superseded" == first.Status, first.Code + " " + first.Message);
            Assert.Contains("last_request_end_time", first.Changed ?? []);
            Assert.True(await SessionAliveAsync(c, victim));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        }, approval: "required");
    }
}
