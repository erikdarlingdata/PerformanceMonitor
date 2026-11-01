/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #5632: how a person's undo and a forward force journal what the store cannot take on the first try, or cannot know. */
public sealed partial class PlanForceToolLiveTests
{
    /* The first late row fails (a statement timeout or a deadlock would do the same) and leaves the session's connection open. */
    private const string FailFirstLateUndoneSql = @"
CREATE SEQUENCE config.test_late_attempts;
CREATE FUNCTION config.test_fail_first_late() RETURNS trigger LANGUAGE plpgsql AS $f$
BEGIN
    IF nextval('config.test_late_attempts') = 1 THEN
        RAISE EXCEPTION 'first late attempt refused by test';
    END IF;
    RETURN NEW;
END $f$;
CREATE TRIGGER test_fail_first_late BEFORE INSERT ON config.investigation_journal
    FOR EACH ROW WHEN (NEW.event = 'action_undone' AND NEW.detail->>'recorded_late' IS NOT NULL) EXECUTE FUNCTION config.test_fail_first_late();";

    [Fact]
    public async Task AnUndo_WhoseFirstLateRowFailsOnAnOpenConnection_RetriesOnIt_AndWritesTheRowWithoutWaitingOnItsOwnLock()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf5632a", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var shown = await UndoAsync(c, called, confirm: false);
            await PgExecAsync(c.Connection, FailCommitOfUndoneSql, c.Ct);
            await PgExecAsync(c.Connection, FailFirstLateUndoneSql, c.Ct);

            var undone = await UndoAsync(c, called, confirm: true, shown: shown.Statement);

            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.Contains("The store recorded it late.", undone.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("could not record", undone.Message, StringComparison.Ordinal);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            var row = Assert.Single(await RowsAsync(c, "action_undone"));
            Assert.True((bool)row["recorded_late"]!);
            Assert.Equal(called, (long)row["called_id"]!);
            /* The first late attempt was refused and the second wrote the row. */
            Assert.Equal("2", await PgScalarAsync(c.Connection, "SELECT last_value FROM config.test_late_attempts", c.Ct));
            AssertNoSecret(c, undone.Message, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task APersonsUndo_WaitsForTheServersSwitchLock_HeldExclusivelyByASwitchWriter()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf5632b", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);

            await using var writer = await c.Source.OpenConnectionAsync(c.Ct);
            await using var transaction = await writer.BeginTransactionAsync(c.Ct);
            await ServerActionSwitchLock.AcquireExclusiveAsync(writer, transaction, 7, ServerActionNames.ForcePlan, 30, c.Ct);

            var undo = Task.Run(() => UndoAsync(c, called, confirm: false), c.Ct);
            var finished = await Task.WhenAny(undo, Task.Delay(2000, c.Ct));
            Assert.NotSame(undo, finished);

            await transaction.RollbackAsync(c.Ct);
            var answer = await undo.WaitAsync(TimeSpan.FromSeconds(20), c.Ct);
            Assert.Equal(PlanUndoAnswer.Confirm, answer.Status);
        });
    }

    [Fact]
    public async Task AnUndo_WhoseReplyIsLost_LeavesAJournalRowForThePersonsAttempt_ThatTheListReadsAsResultUnknown()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf5632c", c.Ct);
            Assert.Equal("done", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["status"]);
            var called = await LastCalledIdAsync(c);
            var shown = await UndoAsync(c, called, confirm: false);
            PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = async (command, token) =>
            {
                await command.ExecuteNonQueryAsync(token);
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
            var attempt = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct)).Changes, x => x.CalledId != called);
            Assert.Equal("person:alice", attempt.Actor);
            Assert.Equal(AgentChangeKinds.PlanUndo, attempt.Kind);
            Assert.Equal(AgentChangeResult.Unknown, attempt.Result);
            Assert.False(attempt.CanTryUndo(DateTime.UtcNow));
            Assert.Equal(called, (long)System.Text.Json.Nodes.JsonNode.Parse(attempt.DetailJson)!["called_id"]!);
            /* The change it tried to undo is still on the list and still offered. */
            var original = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct)).Changes, x => x.CalledId == called);
            Assert.True(original.CanTryUndo(DateTime.UtcNow));
            AssertNoSecret(c, undone.Message, await JournalTextAsync(c));
        });
    }

    /* A force whose reply was lost, after the real procedure ran: the plan is forced and nothing was journaled as done, so no post-force check
       would ever open. The tool reads the plan again and journals action_done, marked late. */
    [Fact]
    public async Task AForce_WhoseReplyIsLostAfterItTookEffect_JournalsDoneLate_SoItsChecksRun()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf5632d", c.Ct);
            PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.PlanWriteSeam.Value = async (command, token) =>
            {
                await command.ExecuteNonQueryAsync(token);
                throw new System.IO.IOException("The connection was reset.");
            };
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
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            var done = Assert.Single(await RowsAsync(c, "action_done"));
            Assert.True((bool)done["recorded_late"]!);
            Assert.Equal(planA, (long)done["after"]!["state"]!["forced_plan"]!["plan_id"]!);
            Assert.Empty(await RowsAsync(c, "action_failed"));
            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct)).Changes);
            Assert.Equal(AgentChangeResult.Done, change.Result);
            Assert.NotNull(change.ForceReview);
            Assert.True(change.ForceReview!.Running);
            /* The sweep's own query now lists the force as due for its checks. */
            await using var due = new NpgsqlCommand(InvestigationRequestStore.DueForceChecksSql, c.Connection);
            due.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz });
            await using var dueRows = await due.ExecuteReaderAsync(c.Ct);
            Assert.True(await dueRows.ReadAsync(c.Ct));
            Assert.Equal(change.CalledId, dueRows.GetInt64(0));
            await dueRows.CloseAsync();
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }
}
