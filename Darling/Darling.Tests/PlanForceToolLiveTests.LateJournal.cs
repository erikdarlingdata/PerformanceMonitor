/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #5632, step 13: a real force whose store commit fails after the server forced the plan. A deferred constraint trigger refuses the commit of
   an action_done row that is not marked recorded_late, so the write's transaction fails at COMMIT, after the server accepted the force. */
public sealed partial class PlanForceToolLiveTests
{
    private const string FailCommitOfDoneSql = @"
CREATE FUNCTION config.test_fail_done() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    IF NEW.event = 'action_done' AND NEW.detail->>'recorded_late' IS NULL THEN
        RAISE EXCEPTION 'commit refused by test';
    END IF;
    RETURN NULL;
END $f$;
CREATE CONSTRAINT TRIGGER test_fail_done AFTER INSERT ON config.investigation_journal
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION config.test_fail_done();";

    [Fact]
    public async Task AForce_WhoseStoreCommitFailsAfterTheServerForced_AnswersDoneWithAWarning_JournalsDoneLate_AndCanBeUndone()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf13", c.Ct);
            await PgExecAsync(c.Connection, FailCommitOfDoneSql, c.Ct);

            var answer = await c.Tool(force: true, query, planA, dryRun: false);
            var json = Parse(answer);

            Assert.Equal("done", (string?)json["status"]);
            Assert.Contains("The change was made on the server. The store recorded it late.", (string?)json["notice"], StringComparison.Ordinal);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_failed"));
            var done = Assert.Single(await RowsAsync(c, "action_done"));
            Assert.True((bool)done["recorded_late"]!);
            Assert.Equal(planA, (long)done["after"]!["state"]!["forced_plan"]!["plan_id"]!);
            Assert.Equal((long)json["journal_id"]!, (long)done["called_id"]!);

            var list = await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct);
            var change = Assert.Single(list.Changes);
            Assert.Equal(AgentChangeResult.Done, change.Result);
            Assert.True(change.CanTryUndo(DateTime.UtcNow));

            var undone = await UndoAsync(c, change.CalledId, confirm: true);
            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    /* A person's undo (#5632, step 13): the same shape, for action_undone. The deferred trigger refuses the COMMIT of an action_undone row that
       is not marked recorded_late; the immediate one refuses its INSERT; the last refuses every action_undone row, the late one too. */
    private const string FailCommitOfUndoneSql = @"
CREATE FUNCTION config.test_fail_undone() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    IF NEW.event = 'action_undone' AND NEW.detail->>'recorded_late' IS NULL THEN
        RAISE EXCEPTION 'undo commit refused by test';
    END IF;
    RETURN NULL;
END $f$;
CREATE CONSTRAINT TRIGGER test_fail_undone AFTER INSERT ON config.investigation_journal
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION config.test_fail_undone();";

    private const string FailInsertOfUndoneSql = @"
CREATE FUNCTION config.test_fail_undone_insert() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    IF NEW.detail->>'recorded_late' IS NULL THEN
        RAISE EXCEPTION 'undo insert refused by test';
    END IF;
    RETURN NEW;
END $f$;
CREATE TRIGGER test_fail_undone_insert BEFORE INSERT ON config.investigation_journal
    FOR EACH ROW WHEN (NEW.event = 'action_undone') EXECUTE FUNCTION config.test_fail_undone_insert();";

    private const string FailEveryUndoneSql = @"
CREATE FUNCTION config.test_fail_every_undone() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    RAISE EXCEPTION 'undo store refused by test';
END $f$;
CREATE TRIGGER test_fail_every_undone BEFORE INSERT ON config.investigation_journal
    FOR EACH ROW WHEN (NEW.event = 'action_undone') EXECUTE FUNCTION config.test_fail_every_undone();";

    [Theory]
    [InlineData(FailCommitOfUndoneSql, "undo commit refused by test")]
    [InlineData(FailInsertOfUndoneSql, "undo insert refused by test")]
    public async Task AnUndo_WhoseStoreWriteFailsAfterTheServerTookIt_AnswersUndone_JournalsUndoneLate_AndTheListNoLongerOffersIt(string failSql, string storeError)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf13u", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var shown = await UndoAsync(c, called, confirm: false);
            await PgExecAsync(c.Connection, failSql, c.Ct);

            var undone = await UndoAsync(c, called, confirm: true, shown: shown.Statement);

            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.Contains("The store recorded it late.", undone.Message, StringComparison.Ordinal);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            var row = Assert.Single(await RowsAsync(c, "action_undone"));
            Assert.True((bool)row["recorded_late"]!);
            Assert.Contains(storeError, (string?)row["error"], StringComparison.Ordinal);
            Assert.Equal(called, (long)row["called_id"]!);
            Assert.Equal(ForceKind, (string?)row["kind"]);

            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct)).Changes);
            Assert.Equal("person:alice", change.UndoneBy);
            Assert.False(change.CanTryUndo(DateTime.UtcNow));
            Assert.Equal("already_undone", (await UndoAsync(c, called, confirm: true)).Code);
            Assert.Single(await RowsAsync(c, "action_undone"));
        });
    }

    [Fact]
    public async Task AnUndo_WhoseStoreRefusesEvenTheLateRow_StillAnswersUndone_AndSaysTheStoreCouldNotRecordIt()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf13v", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var shown = await UndoAsync(c, called, confirm: false);
            await PgExecAsync(c.Connection, FailEveryUndoneSql, c.Ct);

            var undone = await UndoAsync(c, called, confirm: true, shown: shown.Statement);

            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.Contains("the store could not record it", undone.Message, StringComparison.Ordinal);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));
        });
    }
}
