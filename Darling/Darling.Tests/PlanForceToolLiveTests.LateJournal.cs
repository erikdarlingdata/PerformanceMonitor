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
}
