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
            AssertNoSecret(c, undone.Message, await JournalTextAsync(c));
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
            AssertNoSecret(c, undone.Message, await JournalTextAsync(c));
        });
    }

    /* #5632, step 13: an undo is journaled undone only when the server confirmed the write. Here the store would record a late row for any
       undo it believed the server took, so a row that appears means the undo claimed a write that did not happen. */
    [Fact]
    public async Task AnUndo_WhoseServerWriteIsRefused_WritesNoUndoneRow_EvenWhenTheStoreFailsToo()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf13w", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var shown = await UndoAsync(c, called, confirm: false);
            await PgExecAsync(c.Connection, FailCommitOfUndoneSql, c.Ct);
            /* The real procedure, aimed at a plan that does not exist: the server raises an error on an open connection. */
            PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = (command, token) =>
            {
                command.Parameters["@plan_id"].Value = 987654321L;
                return command.ExecuteNonQueryAsync(token);
            };
            PlanUndoAnswer undone;
            try
            {
                undone = await UndoAsync(c, called, confirm: true, shown: shown.Statement);
            }
            finally
            {
                PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = null;
            }

            Assert.Equal(PlanUndoAnswer.Failed, undone.Status);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));
            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct)).Changes);
            Assert.Null(change.UndoneBy);
            Assert.True(change.CanTryUndo(DateTime.UtcNow));
            AssertNoSecret(c, undone.Message, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task AnUndo_WhoseServerPlanChangedSinceItWasShown_WritesNoUndoneRow_EvenWhenTheStoreFailsToo()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, planB) = await c.Sql.TwoPlansAsync("pf13x", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var shown = await UndoAsync(c, called, confirm: false);
            await c.Sql.ForceDirectlyAsync(query, planB, c.Ct);
            await PgExecAsync(c.Connection, FailCommitOfUndoneSql, c.Ct);

            var undone = await UndoAsync(c, called, confirm: true, shown: shown.Statement);

            Assert.Equal(PlanUndoAnswer.RefusedStatus, undone.Status);
            Assert.Equal(planB, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));
            AssertNoSecret(c, undone.Message, await JournalTextAsync(c));
        });
    }

    /* The write was sent and the reply lost, or the call was cancelled while the server ran it: the plan may have changed. The journal says
       neither undone (the server did not confirm it) nor refused (it was not), the person is told to look, and the change stays on the list. */
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnUndo_WhoseReplyIsLostOrCancelledMidWrite_AnswersOutcomeUnknown_JournalsNeitherUndoneNorRefused_AndStaysOffered(bool cancelled)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf13y", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var shown = await UndoAsync(c, called, confirm: false);
            var refusedBefore = (await RowsAsync(c, "refused")).Count;
            /* The real procedure runs and unforces the plan; then the reply is lost, or the token fires, the way a dropped connection or a cancelled call would. */
            PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = async (command, token) =>
            {
                await command.ExecuteNonQueryAsync(token);
                if (cancelled)
                    throw new OperationCanceledException();
                throw new System.IO.IOException("The connection was reset.");
            };
            PlanUndoAnswer undone;
            try
            {
                undone = await UndoAsync(c, called, confirm: true, shown: shown.Statement);
            }
            finally
            {
                PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = null;
            }

            Assert.Equal(PlanUndoAnswer.OutcomeUnknown, undone.Status);
            Assert.Equal("outcome_unknown", undone.Code);
            Assert.Contains("may or may not have changed", undone.Message, StringComparison.Ordinal);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Empty(await RowsAsync(c, "action_undone"));
            Assert.Equal(refusedBefore, (await RowsAsync(c, "refused")).Count);
            /* The person's attempt is on the list as a call with no result; the change it tried to undo is still offered. */
            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct)).Changes, x => x.CalledId == called);
            Assert.Null(change.UndoneBy);
            Assert.True(change.CanTryUndo(DateTime.UtcNow));

            /* A second try reads the server first: the plan is already unforced, so there is nothing to undo and no row is written. */
            var again = await UndoAsync(c, called, confirm: true);
            Assert.Equal(PlanUndoAnswer.RefusedStatus, again.Status);
            Assert.Empty(await RowsAsync(c, "action_undone"));
            AssertNoSecret(c, undone.Message, again.Message, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task AForce_WhoseReplyIsLost_AnswersOutcomeUnknown_JournalsNoFailure_AndTheListSaysResultUnknown()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf13z", c.Ct);
            /* The reply is lost before the procedure ran, so the plan is not forced: the tool reads it again and leaves the call with no result row.
               (When the plan is forced, the call gets a late action_done: PlanForceToolLiveTests.UndoJournal.) */
            PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = (_, _) =>
                throw new System.IO.IOException("The connection was reset.");
            string answer;
            try
            {
                answer = await c.Tool(force: true, query, planA, dryRun: false);
            }
            finally
            {
                PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = null;
            }

            Assert.Equal("outcome_unknown", (string?)Parse(answer)["status"]);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Empty(await RowsAsync(c, "action_failed"));
            Assert.Empty(await RowsAsync(c, "action_done"));
            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct)).Changes);
            Assert.Equal(AgentChangeResult.Unknown, change.Result);
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    /* The commit fails after the server took the undo, and the failure takes a moment (the trigger waits before it refuses). A second undo of the
       same change arrives meanwhile and waits for the first one's lock. The late row is written before that lock is let go, so the second reads
       the change as already undone, not as changed since. */
    private const string SlowFailCommitOfUndoneSql = @"
CREATE FUNCTION config.test_slow_fail_undone() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    IF NEW.event = 'action_undone' AND NEW.detail->>'recorded_late' IS NULL THEN
        PERFORM pg_sleep(4);
        RAISE EXCEPTION 'undo commit refused by test';
    END IF;
    RETURN NULL;
END $f$;
CREATE CONSTRAINT TRIGGER test_slow_fail_undone AFTER INSERT ON config.investigation_journal
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION config.test_slow_fail_undone();";

    [Fact]
    public async Task TwoUndos_RacingAcrossAFailedCommit_WriteOneUndoneRow_AndTheSecondReadsAlreadyUndone()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf13r", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var shown = await UndoAsync(c, called, confirm: false);
            await PgExecAsync(c.Connection, SlowFailCommitOfUndoneSql, c.Ct);

            var first = Task.Run(() => UndoAsync(c, called, confirm: true, login: "alice", shown: shown.Statement), c.Ct);
            /* The first undo is inside its commit (the trigger is sleeping) once the server shows a backend in pg_sleep. */
            for (var i = 0; i < 300 && await PgScalarAsync(c.Connection, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event = 'PgSleep'", c.Ct) == "0"; i++)
                await Task.Delay(100, c.Ct);
            var second = await UndoAsync(c, called, confirm: true, login: "bob", shown: shown.Statement);
            var firstAnswer = await first;

            Assert.Equal(PlanUndoAnswer.Undone, firstAnswer.Status);
            Assert.Contains("The store recorded it late.", firstAnswer.Message, StringComparison.Ordinal);
            Assert.Equal(PlanUndoAnswer.RefusedStatus, second.Status);
            Assert.Equal("already_undone", second.Code);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            var row = Assert.Single(await RowsAsync(c, "action_undone"));
            Assert.True((bool)row["recorded_late"]!);
            AssertNoSecret(c, firstAnswer.Message, second.Message, await JournalTextAsync(c));
        });
    }
}
