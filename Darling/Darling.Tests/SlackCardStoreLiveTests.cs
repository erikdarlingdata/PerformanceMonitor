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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Chat;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7658 on a live store: the card table (V253 <c>config.investigation_slack_posts</c>) round-trips a delivered card by its channel and ts,
/// keeps the first writer's row on both keys, marks a card decided once, and goes with its request; the Slack person lookup and the grant
/// check read the people tables; a press journals as <c>slack_press</c>; a person's refusal moves a pending proposal once; the thread
/// pass's pending read returns a request-bound pending note as a card and leaves a fleet note out; and a request's opened row and the
/// note written from it, read in one batch, post the parent first and the note card in its thread.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class SlackCardStoreLiveTests
{
    private static readonly Guid s_request = Guid.Parse("76580000-0000-0000-0000-0000000000b1");
    private static readonly Guid s_other = Guid.Parse("76580000-0000-0000-0000-0000000000b2");
    private const string Actor = "person:admin";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the Slack card store live tests.");
        return connectionString!;
    }

    internal static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static Task AddRequestAsync(NpgsqlConnection connection, Guid id, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at, state) "
            + $"VALUES ('{id}', 1, 7, 'srv-7', 'blocking', 'blocking', now(), now(), now(), now() + interval '4 hours', 'claimed')", ct);

    /* The store is opened through its own data source on the scratch database, so every session carries the store's search_path. */
    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, InvestigationRequestStore, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await AddRequestAsync(connection, s_request, ct);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, source, new InvestigationRequestStore(source, 30), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheRung_MakesTheCardTable_TheProbeSeesIt_AndARewindDropsItForTheRungToMakeAgain()
    {
        await WithStoreAsync(async (connection, _, _, ct) =>
        {
            Assert.Equal(StorageVersion.SchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync(connection, "SELECT max(version) FROM darling_schema_version", ct));
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.investigation_slack_posts') IS NOT NULL", ct));
            Assert.Equal("SELECT", await ScalarAsync(connection,
                "SELECT string_agg(privilege_type, ',') FROM information_schema.role_table_grants WHERE table_schema = 'config' AND table_name = 'investigation_slack_posts' AND grantee = 'admin'", ct) ?? "SELECT");

            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 252, ct);
            Assert.Equal("False", await ScalarAsync(connection, "SELECT to_regclass('config.investigation_slack_posts') IS NOT NULL", ct));
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version > 252", ct);
            Assert.Equal(StorageVersion.SchemaVersion - 252, await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.investigation_slack_posts') IS NOT NULL", ct));
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
        });
    }

    [Fact]
    public async Task ACard_IsFoundByItsChannelAndTs_TheFirstWriterWinsOnBothKeys_AndADecisionMarksItOnce()
    {
        await WithStoreAsync(async (connection, _, store, ct) =>
        {
            ISlackCardStore cards = store;
            await cards.SaveCardAsync(new SlackCard(s_request, "proposal_created:100", "C07658A1", "1700000000.000200", SlackCardKinds.Proposal, "a1", 7), ct);
            /* The same request and post key again under another ts, and another post key under the same channel and ts: both stay as they were. */
            await cards.SaveCardAsync(new SlackCard(s_request, "proposal_created:100", "C07658A1", "1700000000.000201", SlackCardKinds.Proposal, "a2", 7), ct);
            await cards.SaveCardAsync(new SlackCard(s_request, "reported:101", "C07658A1", "1700000000.000200", SlackCardKinds.Report, "r1", 7), ct);

            var found = await cards.FindCardAsync("C07658A1", "1700000000.000200", ct);
            Assert.NotNull(found);
            Assert.Equal((s_request, "proposal_created:100", SlackCardKinds.Proposal, "a1", 7, SlackCardStates.Live), (found.RequestId, found.PostKey, found.Kind, found.RecordId, found.ServerId, found.State));
            Assert.Null(found.DecidedBy);
            Assert.Null(await cards.FindCardAsync("C07658A1", "1700000000.000201", ct));
            Assert.Null(await cards.FindCardAsync("C07658A1", "1700000000.000999", ct));
            Assert.Null(await cards.FindCardAsync("", "1700000000.000200", ct));
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_slack_posts", ct));

            Assert.True(await cards.MarkCardDecidedAsync("C07658A1", "1700000000.000200", "person:7", ct));
            Assert.False(await cards.MarkCardDecidedAsync("C07658A1", "1700000000.000200", "person:8", ct));
            var decided = await cards.FindCardAsync("C07658A1", "1700000000.000200", ct);
            Assert.Equal((SlackCardStates.Decided, "person:7"), (decided!.State, decided.DecidedBy));
            Assert.NotNull(decided.DecidedAtUtc);
            Assert.False(await cards.MarkCardDecidedAsync("C07658A1", "1700000000.000999", "person:7", ct));
        });
    }

    [Fact]
    public async Task DeletingTheRequest_RemovesItsCards()
    {
        await WithStoreAsync(async (connection, _, store, ct) =>
        {
            await AddRequestAsync(connection, s_other, ct);
            ISlackCardStore cards = store;
            await cards.SaveCardAsync(new SlackCard(s_request, "reported:1", "C07658A1", "1700000000.000300", SlackCardKinds.Report, s_request.ToString("D"), 7), ct);
            await cards.SaveCardAsync(new SlackCard(s_other, "reported:2", "C07658A1", "1700000000.000301", SlackCardKinds.Report, s_other.ToString("D"), 7), ct);
            await ExecAsync(connection, $"DELETE FROM config.investigation_requests WHERE request_id = '{s_request}'", ct);
            Assert.Null(await cards.FindCardAsync("C07658A1", "1700000000.000300", ct));
            Assert.NotNull(await cards.FindCardAsync("C07658A1", "1700000000.000301", ct));
        });
    }

    [Fact]
    public async Task ThePoster_SavesACardWithButtons_UnderTheTsSlackGave_AndNoCardForAPostWithoutButtons()
    {
        await WithStoreAsync(async (_, _, store, ct) =>
        {
            await store.SaveAsync(new SlackRequestThread(s_request, "C07658A1", "1700000000.000100"), ct);
            var posts = new ScriptedPosts();
            var poster = new SlackThreadPoster(store, posts, Microsoft.Extensions.Logging.Abstractions.NullLogger<SlackThreadPoster>.Instance, cards: store);
            var target = new SlackThreadTarget(s_request, s_request, 1, 7, "C07658A1", Practice: false);
            var proposal = Guid.Parse("76580000-0000-0000-0000-0000000000a1");

            posts.NextTs = "1700000000.000400";
            var card = SlackThreadPosts.ProposalPost("force_query_store_plan", "srv-7", "pending", null, null, PerformanceMonitor.Common.McpDataDetail.Full, null, proposal);
            Assert.Equal(SlackThreadPostOutcome.Posted, await poster.PostAsync(target, "proposal_created:100", card, ct));
            posts.NextTs = "1700000000.000401";
            Assert.Equal(SlackThreadPostOutcome.Posted, await poster.PostAsync(target, "claimed:101", SlackThreadPosts.ClaimPost("agent-a", 1), ct));

            ISlackCardStore cards = store;
            var saved = await cards.FindCardAsync("C07658A1", "1700000000.000400", ct);
            Assert.NotNull(saved);
            Assert.Equal((s_request, "proposal_created:100", SlackCardKinds.Proposal, proposal.ToString("D"), 7), (saved.RequestId, saved.PostKey, saved.Kind, saved.RecordId, saved.ServerId));
            Assert.Null(await cards.FindCardAsync("C07658A1", "1700000000.000401", ct));
        });
    }

    private sealed class ScriptedPosts : ISlackBotPosts
    {
        public string NextTs { get; set; } = "1700000000.000001";
        public readonly List<(string Channel, string Text, string? ThreadTs)> Posts = new();

        public Task<SlackPostResult> PostAsync(string channel, string text, System.Collections.Generic.IReadOnlyList<object>? blocks, string? threadTs, CancellationToken cancellationToken)
        {
            Posts.Add((channel, text, threadTs));
            return Task.FromResult(new SlackPostResult(true, NextTs, channel, null));
        }
    }

    [Fact]
    public async Task TheSlackPerson_IsFoundByTeamAndUser_WithTheIdentityUsed_AndTheGrantCheck_ReadsTheFleetScope_AndTheEnabledFlag()
    {
        await WithStoreAsync(async (connection, source, store, ct) =>
        {
            var person = await PersonDirectory.AddPersonAsync(source, "Erik", Actor, "web", ct);
            Assert.True(person.Succeeded, person.Message);
            var identity = await PersonDirectory.AddIdentityAsync(source, person.PersonId!.Value, PersonIdentityKinds.Slack, "T0TEAM:U0PERSON", Actor, "web", ct);
            Assert.True(identity.Succeeded, identity.Message);

            ISlackPressStore presses = store;
            var found = await presses.FindSlackPersonAsync("T0TEAM", "U0PERSON", ct);
            Assert.NotNull(found);
            Assert.Equal((person.PersonId.Value, identity.IdentityId!.Value, "Erik", true), (found.PersonId, found.IdentityId, found.DisplayName, found.Enabled));
            Assert.Null(await presses.FindSlackPersonAsync("T0TEAM", "U0NOBODY", ct));
            Assert.Null(await presses.FindSlackPersonAsync("T0OTHER", "U0PERSON", ct));

            Assert.False(await presses.HasGrantAsync(found.PersonId, PersonCapabilities.Undo, 7, ct));
            var grant = await PersonDirectory.AddGrantAsync(source, found.PersonId, null, null, PersonCapabilities.Undo, null, Actor, "web", ct);
            Assert.True(grant.Succeeded, grant.Message);
            Assert.True(await presses.HasGrantAsync(found.PersonId, PersonCapabilities.Undo, 7, ct));
            Assert.True(await presses.HasGrantAsync(found.PersonId, PersonCapabilities.Undo, null, ct));
            Assert.False(await presses.HasGrantAsync(found.PersonId, PersonCapabilities.Feedback, 7, ct));
            Assert.False(await presses.HasGrantAsync(found.PersonId + 1000, PersonCapabilities.Undo, 7, ct));

            var disabled = await PersonDirectory.SetPersonEnabledAsync(source, found.PersonId, false, Actor, "web", ct);
            Assert.True(disabled.Succeeded, disabled.Message);
            Assert.False((await presses.FindSlackPersonAsync("T0TEAM", "U0PERSON", ct))!.Enabled);
            Assert.False(await presses.HasGrantAsync(found.PersonId, PersonCapabilities.Undo, 7, ct));
        });
    }

    [Fact]
    public async Task APress_JournalsAsSlackPress_WithThePersonTheActionAndTheIdentity()
    {
        await WithStoreAsync(async (connection, _, store, ct) =>
        {
            ISlackPressStore presses = store;
            await presses.JournalSlackPressAsync(s_request, 7, "person:7", SlackCardActions.ProposalApproveFirst,
                new JsonObject { ["identity"] = "slack:T0TEAM:U0PERSON", ["outcome"] = "first_pressed" }, ct);
            Assert.Equal("person:7|proposal_approve_1|slack:T0TEAM:U0PERSON|first_pressed|7",
                await ScalarAsync(connection,
                    $"SELECT actor || '|' || reason || '|' || (detail->>'identity') || '|' || (detail->>'outcome') || '|' || server_id::text FROM config.investigation_journal WHERE event = '{InvestigationRequestStore.SlackPressed}' AND request_id = '{s_request}'", ct));
            Assert.Contains(InvestigationRequestStore.SlackPressed, DarlingManagedRoles.JournalOwnerEventList, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task APersonsRefusal_MovesAPendingProposalToRefusedOnce_AndJournalsThePerson()
    {
        await WithStoreAsync(async (connection, source, store, ct) =>
        {
            var created = await store.CreateProposalAsync(new ActionProposalDraft(s_request, 1, "agent-a", "force_query_store_plan", 7,
                new JsonObject { ["plan_id"] = 5, ["query_id"] = 9, ["database"] = "Sales" }, new JsonObject { ["statement"] = "EXEC x;" }, "the plan regressed"), ct);
            Assert.True(created.Created);
            var approver = ProposalApprover.Person(7, 70, ServerActionSurfaces.Slack);

            await using (var connection2 = await source.OpenConnectionAsync(ct))
            await using (var transaction = await connection2.BeginTransactionAsync(ct))
            {
                Assert.True(await store.RefuseProposalRowAsync(connection2, transaction, created.ProposalId, approver, "refused from Slack", ct));
                await transaction.CommitAsync(ct);
            }

            var row = await store.GetProposalAsync(created.ProposalId, ct);
            Assert.Equal((ProposalStates.Refused, "person:7"), (row!.State, row.DecidedBy));
            Assert.NotNull(row.DecidedAt);
            Assert.NotNull(row.FinishedAt);
            Assert.Contains("refused_by_person", row.OutcomeJson);
            Assert.Equal("person:7|slack|refused from Slack",
                await ScalarAsync(connection, $"SELECT actor || '|' || (detail->'approver'->>'surface') || '|' || reason FROM config.investigation_journal WHERE event = '{ProposalEvents.Refused}' AND request_id = '{s_request}'", ct));

            await using (var connection3 = await source.OpenConnectionAsync(ct))
            await using (var transaction = await connection3.BeginTransactionAsync(ct))
            {
                Assert.False(await store.RefuseProposalRowAsync(connection3, transaction, created.ProposalId, approver, "again", ct));
                await transaction.CommitAsync(ct);
            }

            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE event = '{ProposalEvents.Refused}'", ct));
            /* The checked refusal on a proposal that is not pending answers the state and changes nothing. */
            var hooks = new ApprovedActionHooks { LoadRoutes = (_, _, _) => throw new InvalidOperationException("not reached"), ReadCredential = (_, _) => throw new InvalidOperationException("not reached") };
            var refusal = await store.RefuseProposalAsync(created.ProposalId, approver, hooks, "again", ct);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.NotPending, refusal!.Reason);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.NotFound, (await store.RefuseProposalAsync(Guid.NewGuid(), approver, hooks, null, ct))!.Reason);
        });
    }

    [Fact]
    public async Task AnUndoConfirm_RunsOnlyTheStatementThePersonWasShown_AndIsRefusedWhenTheServerWouldNowRunAnotherOne()
    {
        await WithStoreAsync(async (connection, source, store, ct) =>
        {
            /* A plan force the agent made, journaled as the tool journals it: a plan change, whose undo reads the server before it decides. */
            var detail = new JsonObject
            {
                ["tool"] = "force_query_store_plan", ["kind"] = AgentChangeKinds.PlanForce,
                ["target"] = new JsonObject { ["id"] = "5", ["server_id"] = 7, ["database"] = "Sales" },
                ["before"] = new JsonObject { ["database"] = "Sales", ["query_id"] = 9, ["plan_id"] = 5, ["forced_plan"] = null },
            };
            var calledId = long.Parse((await ScalarAsync(connection,
                $"INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, detail) VALUES (now(), '{s_request}', 7, 'action_called', 'agent:agent-a', '{detail.ToJsonString()}'::jsonb) RETURNING journal_id", ct))!,
                CultureInfo.InvariantCulture);

            /* The monitored server, stood in at the one seam that reads it: the statement the undo would run now, and every read and write asked of it. */
            const string unforce = "EXEC [Sales].sys.sp_query_store_unforce_plan @query_id = 9, @plan_id = 5;";
            const string reforce = "EXEC [Sales].sys.sp_query_store_force_plan @query_id = 9, @plan_id = 3;";
            var statementNow = unforce;
            var server = new List<(long CalledId, string Login, bool Confirm, string? Shown)>();
            var commands = new StoreSlackPressCommands(source, store, NullLogger.Instance, null, (id, login, confirm, shown, _) =>
            {
                server.Add((id, login, confirm, shown));
                return Task.FromResult(confirm
                    ? new PlanUndoAnswer(PlanUndoAnswer.Undone, null, "Undone.", statementNow)
                    : new PlanUndoAnswer(PlanUndoAnswer.Confirm, null, "This unforces the plan.", statementNow));
            });

            var shown = await commands.UndoAsync(calledId, "7", confirm: false, null, ct);
            Assert.Equal((SlackUndoStep.Confirm, unforce), (shown.Status, shown.Statement));
            Assert.Equal([false], server.Select(s => s.Confirm));
            var hash = SlackConfirmValue.HashOf(unforce);

            /* The server changed between the show and the Confirm, so the undo would run another statement: the press is refused and nothing runs. */
            statementNow = reforce;
            var stale = await commands.UndoAsync(calledId, "7", confirm: true, hash, ct);
            Assert.Equal((SlackUndoStep.Refused, "statement_changed"), (stale.Status, stale.Code));
            Assert.Equal(reforce, stale.Statement);
            Assert.DoesNotContain(server, s => s.Confirm);

            /* A Confirm that carries no hash is refused the same way. */
            var bare = await commands.UndoAsync(calledId, "7", confirm: true, null, ct);
            Assert.Equal((SlackUndoStep.Refused, "statement_changed"), (bare.Status, bare.Code));
            Assert.DoesNotContain(server, s => s.Confirm);
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_undone'", ct));

            /* The server would run the statement the person saw: the write goes to the server with that statement and no other. */
            statementNow = unforce;
            var done = await commands.UndoAsync(calledId, "7", confirm: true, hash, ct);
            Assert.Equal(SlackUndoStep.Undone, done.Status);
            var write = Assert.Single(server, s => s.Confirm);
            Assert.Equal((calledId, "7", unforce), (write.CalledId, write.Login, write.Shown));
        });
    }

    [Fact]
    public async Task ThePendingRead_ReturnsARequestBoundPendingNoteAsACard_LeavesAFleetNoteOut_AndStopsOnceItIsMarked()
    {
        await WithStoreAsync(async (connection, _, store, ct) =>
        {
            var routeId = await ScalarAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, slack_url, slack_bot_channel, enabled) VALUES ('cpu', 'sealed-cpu', 'C07658A1', true) RETURNING route_id", ct);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET route_id = {routeId} WHERE request_id = '{s_request}'", ct);
            await ExecAsync(connection, $"INSERT INTO config.investigation_notes (server_id, metric_name, note_text, author, state, source_request_id, expires_at) VALUES (7, 'cpu', 'Index ix_a is unused', 'agent:a', 'pending', '{s_request}', now() + interval '30 days')", ct);
            await ExecAsync(connection, $"INSERT INTO config.investigation_notes (server_id, metric_name, note_text, author, state, source_request_id, expires_at) VALUES (NULL, NULL, 'Fleet-wide note', 'agent:a', 'pending', '{s_request}', now() + interval '30 days')", ct);
            await ExecAsync(connection, "INSERT INTO config.investigation_notes (server_id, metric_name, note_text, author, state, source_request_id, expires_at) VALUES (7, 'cpu', 'No request', 'person:erik', 'pending', NULL, now() + interval '30 days')", ct);
            await ExecAsync(connection, $"INSERT INTO config.investigation_notes (server_id, metric_name, note_text, author, state, source_request_id, expires_at) VALUES (7, 'cpu', 'Already approved', 'agent:a', 'approved', '{s_request}', now() + interval '30 days')", ct);

            ISlackThreadRowSource rows = store;
            var now = DateTime.UtcNow;
            var pending = await rows.PendingSlackThreadRowsAsync(SlackThreadPass.PostedEvents, now.AddDays(-7), now.AddMinutes(-10), 100, ct);
            var note = Assert.Single(pending, r => r.Event == InvestigationRequestStore.NotePending);
            Assert.Equal("Index ix_a is unused", note.Reason);
            Assert.Equal("agent:a", note.Actor);
            Assert.Equal(s_request, note.RequestId);
            Assert.Equal("note:" + note.JournalId, note.PostKey);
            Assert.Equal("cpu", JsonNode.Parse(note.DetailJson!)!["metric"]!.GetValue<string>());
            Assert.Equal(7, note.ServerId);
            var card = SlackThreadPass.Content(note, [], PerformanceMonitor.Common.McpDataDetail.Full, "");
            Assert.Equal(new SlackCardFacts(SlackCardKinds.Note, note.JournalId.ToString()), card.Card);

            ISlackRequestThreadStore threads = store;
            Assert.True(await threads.MarkOnceAsync(s_request, 1, 7, note.PostKey, SlackThreadPosts.OnceKey(s_request, note.PostKey), ct));
            var after = await rows.PendingSlackThreadRowsAsync(SlackThreadPass.PostedEvents, now.AddDays(-7), now.AddMinutes(-10), 100, ct);
            Assert.DoesNotContain(after, r => r.Event == InvestigationRequestStore.NotePending);
        });
    }

    /* A route with a bot channel, the request's opened row, and a pending note written from the request, so one batch reads both. The
       journal's identity is moved ahead first, as it stands on any store that journaled before its first note, so the note's id is the
       smaller of the two. */
    private static async Task<(int RouteId, long OpenedId, long NoteId)> SeedOpenedRequestWithANoteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var routeId = int.Parse((await ScalarAsync(connection, "INSERT INTO config.config_notification_routes (metric_match, slack_url, slack_bot_channel, enabled) VALUES ('cpu', 'sealed-cpu', 'C07658A1', true) RETURNING route_id", ct))!, CultureInfo.InvariantCulture);
        await ExecAsync(connection, $"UPDATE config.investigation_requests SET route_id = {routeId} WHERE request_id = '{s_request}'", ct);
        await ExecAsync(connection, "SELECT setval(pg_get_serial_sequence('config.investigation_journal', 'journal_id'), 1000)", ct);
        var openedId = long.Parse((await ScalarAsync(connection,
            $"INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason) VALUES (now() - interval '5 minutes', '{s_request}', {routeId}, 7, 'opened', 'service', 'cpu') RETURNING journal_id", ct))!, CultureInfo.InvariantCulture);
        var noteId = long.Parse((await ScalarAsync(connection,
            $"INSERT INTO config.investigation_notes (server_id, metric_name, note_text, author, state, source_request_id, expires_at) VALUES (7, 'cpu', 'Index ix_a is unused', 'agent:a', 'pending', '{s_request}', now() + interval '30 days') RETURNING note_id", ct))!, CultureInfo.InvariantCulture);
        Assert.True(noteId < openedId, $"the note's id ({noteId}) should be the smaller; the opened row's is {openedId}");
        return (routeId, openedId, noteId);
    }

    [Fact]
    public async Task ThePendingRead_PutsARequestsOpenedRowBeforeThePendingNoteWrittenFromIt_WhateverTheirIds()
    {
        await WithStoreAsync(async (connection, _, store, ct) =>
        {
            var (_, openedId, noteId) = await SeedOpenedRequestWithANoteAsync(connection, ct);
            ISlackThreadRowSource rows = store;
            var now = DateTime.UtcNow;
            var pending = await rows.PendingSlackThreadRowsAsync(SlackThreadPass.PostedEvents, now.AddDays(-7), now.AddMinutes(-10), 100, ct);
            Assert.Equal([("opened", openedId), (InvestigationRequestStore.NotePending, noteId)], pending.Select(r => (r.Event, r.JournalId)));
        });
    }

    [Fact]
    public async Task ThePass_PostsTheParentFirst_AndTheNoteCardFollowsInItsThread_WhenBothAreReadInOneBatch()
    {
        await WithStoreAsync(async (connection, _, store, ct) =>
        {
            var (routeId, _, noteId) = await SeedOpenedRequestWithANoteAsync(connection, ct);
            var posts = new ScriptedPosts { NextTs = "1700000000.000500" };
            var poster = new SlackThreadPoster(store, posts, NullLogger<SlackThreadPoster>.Instance, cards: store);
            var routes = new List<NotificationRoute> { new(routeId, "cpu", "", "", "", "", "", Enabled: true) { SlackBotChannel = "C07658A1" } };
            var pass = new SlackThreadPass(store, store, poster, poster.Throttle, () => routes, NullLogger.Instance, PerformanceMonitor.Common.McpDataDetail.Full, () => "https://pm.example");

            /* The parent and the card land in the same pass; the next pass is the most the card may take. */
            await pass.RunOnceAsync(ct);
            if (posts.Posts.Count < 2)
            {
                await pass.RunOnceAsync(ct);
            }

            Assert.Equal(2, posts.Posts.Count);
            Assert.Null(posts.Posts[0].ThreadTs);
            Assert.Contains("Investigation request", posts.Posts[0].Text);
            Assert.Equal("1700000000.000500", posts.Posts[1].ThreadTs);
            Assert.StartsWith("Note proposed by agent:a", posts.Posts[1].Text, StringComparison.Ordinal);
            ISlackCardStore cards = store;
            var card = await cards.FindCardAsync("C07658A1", "1700000000.000500", ct);
            Assert.NotNull(card);
            Assert.Equal((SlackCardKinds.Note, noteId.ToString(CultureInfo.InvariantCulture)), (card.Kind, card.RecordId));
            Assert.Equal(0, await pass.RunOnceAsync(ct));
            Assert.Equal(2, posts.Posts.Count);
        });
    }
}

/// <summary>
/// #7658 as the real logins: the card table is the owner's. The rung revokes what the default privileges gave and grants admin SELECT alone;
/// the managed provisioning batch does the same after its blanket grants; and no login but the owner writes a <c>slack_press</c> journal row. The
/// roles are cluster-wide, so the class is in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class SlackCardGrantLiveTests
{
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];
    private static readonly Guid s_request = Guid.Parse("76580000-0000-0000-0000-0000000000c1");
    private const string Table = "config.investigation_slack_posts";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the Slack card grant live tests.");
        return connectionString!;
    }

    private static string LoginConnectionString(ScratchPostgres scratch, string role, string password) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString;

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => SlackCardStoreLiveTests.ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    /// <summary>The catalog's answer, role by verb: admin holds SELECT and nothing else, and the other three roles hold nothing.</summary>
    private static async Task AssertCatalogAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        foreach (var role in s_cleanupRoles)
        {
            var held = new System.Collections.Generic.List<string>();
            foreach (var verb in new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" })
            {
                if (await SlackCardStoreLiveTests.ScalarAsync(owner, $"SELECT has_table_privilege('{role}', '{Table}', '{verb}')", ct) == "True")
                    held.Add(verb);
            }

            var acl = await SlackCardStoreLiveTests.ScalarAsync(owner, $"SELECT relacl::text FROM pg_class WHERE oid = '{Table}'::regclass", ct);
            Assert.True((role == "admin" ? "SELECT" : "") == string.Join(",", held), $"{role} holds [{string.Join(",", held)}] on {Table}; acl {acl}");
        }
    }

    /// <summary>The rung on a store whose roles exist before it runs, then the managed batch after it: the catalog reads owner-only both times, and as
    /// the real logins admin reads and cannot write, the others cannot read, and no login journals a press.</summary>
    [Fact]
    public async Task TheRungAndTheManagedBatch_LeaveTheCardsToTheOwner_AndTheReadToAdmin()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var exist = await SlackCardStoreLiveTests.ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
            Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this cluster; the grants would adopt it.");

            /* The managed batch first: it makes the four roles, gives its blanket grants and takes the card table back (section 10e6). */
            await PgMigrations.MigrateAsync(owner, ct);
            await SlackCardStoreLiveTests.ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 15, PasswordReassert.All,
                ProvisioningTarget.ComposeStore(new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling", scratch.DatabaseName)), ct);
            await AssertCatalogAsync(owner, ct);

            /* Then back behind V253 and through it again with the roles and their default privileges in place, as on an upgraded store: the rung
               makes the table under the default privileges the batch left and takes it back itself. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, 252, ct);
            await SlackCardStoreLiveTests.ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version > 252", ct);
            Assert.Equal(StorageVersion.SchemaVersion - 252, await PgMigrations.MigrateAsync(owner, ct));
            await AssertCatalogAsync(owner, ct);

            await SlackCardStoreLiveTests.ExecAsync(owner,
                "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at, state) "
                + $"VALUES ('{s_request}', 1, 7, 'srv-7', 'blocking', 'blocking', now(), now(), now(), now() + interval '4 hours', 'claimed')", ct);
            await using var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await new InvestigationRequestStore(ownerSource, 30).SaveCardAsync(new SlackCard(s_request, "reported:1", "C07658A1", "1700000000.000500", SlackCardKinds.Report, s_request.ToString("D"), 7), ct);
            var journalBefore = await SlackCardStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);

            foreach (var (role, password) in new[] { ("admin", ProvisioningTestSecrets.AdminPassword), ("viewer", ProvisioningTestSecrets.ViewerPassword), ("mcp", ProvisioningTestSecrets.McpPassword) })
            {
                await using var login = new NpgsqlConnection(LoginConnectionString(scratch, role, password));
                await login.OpenAsync(ct);
                await AssertDeniedAsync(login, $"INSERT INTO {Table} (request_id, post_key, channel_id, message_ts, kind, record_id, server_id) VALUES ('{s_request}', 'x', 'C07658EVE', '1700000000.000009', 'report', 'r', 7)", ct);
                await AssertDeniedAsync(login, $"UPDATE {Table} SET state = 'decided' WHERE channel_id = 'C07658A1'", ct);
                await AssertDeniedAsync(login, $"DELETE FROM {Table}", ct);
                if (role == "admin")
                    Assert.Equal("live", await SlackCardStoreLiveTests.ScalarAsync(login, $"SELECT state FROM {Table} WHERE channel_id = 'C07658A1'", ct));
                else
                    await AssertDeniedAsync(login, $"SELECT count(*) FROM {Table}", ct);

                /* The press row is the owner's: admin and mcp hold the journal's INSERT and the owner-row trigger refuses them by the event's name;
                   viewer holds no INSERT on the journal at all. */
                var journal = await Assert.ThrowsAsync<PostgresException>(() => SlackCardStoreLiveTests.ExecAsync(login,
                    $"INSERT INTO config.investigation_journal (request_id, event, actor, reason, detail) VALUES ('{s_request}', '{InvestigationRequestStore.SlackPressed}', 'person:eve', 'proposal_approve_2', '{{}}'::jsonb)", ct));
                Assert.Equal("42501", journal.SqlState);
                if (role is "admin" or "mcp")
                    Assert.Contains("Only the Darling service journals the request event " + InvestigationRequestStore.SlackPressed, journal.MessageText, StringComparison.Ordinal);

                await using var source = NpgsqlDataSource.Create(LoginConnectionString(scratch, role, password));
                ISlackCardStore store = new InvestigationRequestStore(source, 30);
                if (role == "admin")
                    Assert.Equal("reported:1", (await store.FindCardAsync("C07658A1", "1700000000.000500", ct))!.PostKey);
                else
                    Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => store.FindCardAsync("C07658A1", "1700000000.000500", ct))).SqlState);
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => store.MarkCardDecidedAsync("C07658A1", "1700000000.000500", "person:eve", ct))).SqlState);
            }

            Assert.Equal("live", await SlackCardStoreLiveTests.ScalarAsync(owner, $"SELECT state FROM {Table} WHERE channel_id = 'C07658A1'", ct));
            Assert.Equal(journalBefore, await SlackCardStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "slackcard7658", cleanupCt));
        }
    }
}
