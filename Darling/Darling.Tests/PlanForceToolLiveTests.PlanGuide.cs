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
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* The plan guide half of the plan force live tests (#6569, slice 2a): the same store and SQL Server setup (a partial class, so the private setup is shared).
   Each test makes its own scratch store and SQL Server database, creates a throwaway procedure in it, and drops the database after. */
public sealed partial class PlanForceToolLiveTests
{
    private const string GuideKind = "plan_guide_create";
    private const string GuideStatement = "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1";

    private static Task<string> CreateGuideAsync(Ctx c, string name, bool dryRun, string type = "OBJECT", string? statement = null, string? moduleOrBatch = null, string hints = "OPTION (RECOMPILE)",
        string? parameters = null, string server = "srv-7", string? database = null) =>
        DarlingMcpPlanForceTools.RunCreatePlanGuideAsync(c.Source, c.Source, c.Log, server, database ?? c.Sql.Database, name, statement ?? GuideStatement, type, moduleOrBatch, parameters, hints, c.Arg, dryRun, c.Ct);

    private static Task TurnGuideOnAsync(Ctx c) =>
        PgExecAsync(c.Connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'create_plan_guide', true, 'test')", c.Ct);

    private static async Task<string?> GuideColumnAsync(Ctx c, string name, string column)
    {
        await using var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
        await using var command = new Microsoft.Data.SqlClient.SqlCommand($"SELECT CONVERT(nvarchar(max), {column}) FROM sys.plan_guides WHERE name = @name;", admin);
        command.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@name", System.Data.SqlDbType.NVarChar, 128) { Value = name });
        var value = await command.ExecuteScalarAsync(c.Ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private static async Task AdminExecAsync(Ctx c, string sql)
    {
        await using var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
        await ExecAsync(admin, sql, c.Ct);
    }

    [Fact]
    public async Task ADryRunGuide_ShowsTheStatementAndTheDefinition_CreatesNothing_AndJournalsNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);

            var answer = await CreateGuideAsync(c, "g_" + name, dryRun: true, moduleOrBatch: "dbo." + name);
            var json = Parse(answer);
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.StartsWith($"USE [{c.Sql.Database}]; EXEC sys.sp_create_plan_guide @name = N'g_{name}', @stmt = N'{GuideStatement}', @type = N'OBJECT', @module_or_batch = N'[dbo].[{name}]', @params = NULL, @hints = N'OPTION (RECOMPILE)';",
                (string?)json["statement"], StringComparison.Ordinal);
            Assert.Null(await GuideColumnAsync(c, "g_" + name, "name"));
            Assert.Equal(0, await JournalActionRowsAsync(c));
            var parameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.CreatePlanGuide))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)parameter.DefaultValue!);
            AssertNoSecret(c, answer);
        });
    }

    [Fact]
    public async Task ARealObjectGuide_IsCreated_JournalsTheWholeDefinition_AndAPersonsUndoDropsIt()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);

            var answer = await CreateGuideAsync(c, guide, dryRun: false, moduleOrBatch: $"[dbo].[{name}]");
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.Equal("OPTION (RECOMPILE)", await GuideColumnAsync(c, guide, "hints"));
            Assert.Equal("OBJECT", await GuideColumnAsync(c, guide, "scope_type_desc"));

            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(GuideKind, (string?)called["kind"]);
            Assert.Equal("create_plan_guide", (string?)called["tool"]);
            Assert.Equal(guide, (string?)called["target"]!["id"]);
            Assert.Equal(c.Sql.Database, (string?)called["before"]!["database"]);
            var journaled = called["before"]!["guide"]!;
            Assert.Equal(guide, (string?)journaled["name"]);
            Assert.Equal("OBJECT", (string?)journaled["type"]);
            Assert.Equal(GuideStatement, (string?)journaled["statement"]);
            Assert.Equal($"[dbo].[{name}]", (string?)journaled["module_or_batch"]);
            Assert.Equal("OPTION (RECOMPILE)", (string?)journaled["hints"]);
            Assert.Single(await RowsAsync(c, "action_done"));

            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct)).Changes);
            Assert.Equal(GuideKind, change.Kind);
            Assert.True(change.CanTryUndo(DateTime.UtcNow), "a plan guide create can be undone");

            /* The undo shows the statement first and writes nothing. */
            var calledId = (long)json["journal_id"]!;
            var shown = await UndoAsync(c, calledId, confirm: false);
            Assert.Equal(PlanUndoAnswer.Confirm, shown.Status);
            Assert.Equal($"USE [{c.Sql.Database}]; EXEC sys.sp_control_plan_guide N'DROP', N'{guide}';", shown.Statement);
            Assert.Equal(guide, await GuideColumnAsync(c, guide, "name"));

            var undone = await UndoAsync(c, calledId, confirm: true);
            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.Null(await GuideColumnAsync(c, guide, "name"));
            var undoneRow = Assert.Single(await RowsAsync(c, "action_undone"));
            Assert.Equal(calledId, (long)undoneRow["called_id"]!);
            Assert.Equal("person:alice", await PgScalarAsync(c.Connection, "SELECT actor FROM config.investigation_journal WHERE event = 'action_undone'", c.Ct));

            var again = await UndoAsync(c, calledId, confirm: true);
            Assert.Equal("already_undone", again.Code);
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task TheUndoOfAGuideThatIsAlreadyGone_SaysSo_AndChangesNothing_AndOneSomebodyReplacedIsNotDropped()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);
            var gone = (long)Parse(await CreateGuideAsync(c, guide, dryRun: false, moduleOrBatch: "dbo." + name))["journal_id"]!;
            await AdminExecAsync(c, $"EXEC sys.sp_control_plan_guide N'DROP', N'{guide}';");

            var goneAnswer = await UndoAsync(c, gone, confirm: true);
            Assert.Equal(PlanUndoAnswer.RefusedStatus, goneAnswer.Status);
            Assert.Equal("guide_not_found", goneAnswer.Code);
            Assert.Contains("already gone", goneAnswer.Message, StringComparison.Ordinal);
            Assert.Empty(await RowsAsync(c, "action_undone"));

            /* The guide comes back under the same name with other hints: the undo does not drop what it did not create. */
            await AdminExecAsync(c, $"EXEC sys.sp_create_plan_guide @name = N'{guide}', @stmt = N'{GuideStatement}', @type = N'OBJECT', @module_or_batch = N'dbo.{name}', @params = NULL, @hints = N'OPTION (MAXDOP 1)';");
            var changedAnswer = await UndoAsync(c, gone, confirm: true);
            Assert.Equal("guide_changed", changedAnswer.Code);
            Assert.Equal("OPTION (MAXDOP 1)", await GuideColumnAsync(c, guide, "hints"));
            Assert.Empty(await RowsAsync(c, "action_undone"));
        });
    }

    [Fact]
    public async Task ASqlGuideWithNoCachedMatch_IsCreated_AndTheAnswerSaysItMatchesNothingCached()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var guide = "g_" + Guid.NewGuid().ToString("N")[..8];
            await TurnGuideOnAsync(c);
            var statement = "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a = " + Random.Shared.Next(1000, 99999);

            var dry = Parse(await CreateGuideAsync(c, guide, dryRun: true, type: "SQL", statement: statement));
            Assert.Equal("dry_run", (string?)dry["status"]);
            Assert.Equal(0, (long)dry["matches"]!);

            var json = Parse(await CreateGuideAsync(c, guide, dryRun: false, type: "SQL", statement: statement));
            Assert.Equal("done", (string?)json["status"]);
            Assert.Equal(0, (long)json["matches"]!);
            Assert.Contains("matches nothing cached", (string?)json["message"] ?? "", StringComparison.Ordinal);
            Assert.Equal("SQL", await GuideColumnAsync(c, guide, "scope_type_desc"));
        });
    }

    [Fact]
    public async Task ASqlGuideOnACachedBatch_ReportsTheCount()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var guide = "g_" + Guid.NewGuid().ToString("N")[..8];
            await TurnGuideOnAsync(c);
            var statement = "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 2";
            await AdminExecAsync(c, statement + ";");

            var json = Parse(await CreateGuideAsync(c, guide, dryRun: true, type: "SQL", statement: statement));
            Assert.True((long)json["matches"]! >= 1);
        });
    }

    [Fact]
    public async Task AStatementWithAQuote_GoesThroughAsAParameter_AndIsStoredExactly()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var guide = "g_" + Guid.NewGuid().ToString("N")[..8];
            await TurnGuideOnAsync(c);
            const string Statement = "SELECT q = 'it''s'; DROP TABLE dbo.t; --' + N''";

            var json = Parse(await CreateGuideAsync(c, guide, dryRun: false, type: "SQL", statement: Statement));
            /* Whether the server accepts the text as a guide or not, nothing in it ran as batch text. */
            await using var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
            Assert.Equal("3", Convert.ToString(await ScalarAsync(admin, "SELECT COUNT_BIG(*) FROM dbo.t;", c.Ct), CultureInfo.InvariantCulture));
            if ((string?)json["status"] == "done")
                Assert.Equal(Statement, await GuideColumnAsync(c, guide, "query_text"));
            Assert.Contains("it''s", (string?)json["statement"] ?? "", StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AGuideThatFailsValidation_IsNotLeftBehind_AndIsJournaledFailed()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);
            /* A plan for another statement: a USE PLAN hint the guide's statement cannot use. */
            await AdminExecAsync(c, "SELECT a, b FROM dbo.t WHERE a = 1 AND b = 1 OPTION (MAXDOP 1, MAXRECURSION 7);");
            string plan;
            await using (var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
                plan = Convert.ToString(await ScalarAsync(admin,
                    "SELECT TOP (1) CONVERT(nvarchar(max), qp.query_plan) FROM sys.dm_exec_cached_plans AS cp CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) AS st CROSS APPLY sys.dm_exec_query_plan(cp.plan_handle) AS qp WHERE st.text LIKE N'SELECT a, b FROM dbo.t WHERE a = 1 AND b = 1 OPTION (MAXDOP 1, MAXRECURSION 7)%' AND st.text NOT LIKE N'%dm_exec_cached_plans%';", c.Ct), CultureInfo.InvariantCulture)!;
            Assert.False(string.IsNullOrEmpty(plan));

            var json = Parse(await CreateGuideAsync(c, guide, dryRun: false, moduleOrBatch: "dbo." + name, hints: "OPTION (USE PLAN N'" + plan.Replace("'", "''", StringComparison.Ordinal) + "')"));
            Assert.Equal("failed", (string?)json["status"]);
            Assert.Null(await GuideColumnAsync(c, guide, "name"));
            Assert.Single(await RowsAsync(c, "action_failed"));
            Assert.Empty(await RowsAsync(c, "action_done"));
        });
    }

    [Fact]
    public async Task AStatementNotInTheModule_AndATakenName_AndATemplate_AreRefused_AndNothingRuns()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);

            foreach (var dryRun in new[] { true, false })
            {
                var missing = Parse(await CreateGuideAsync(c, guide, dryRun, statement: "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 99", moduleOrBatch: "dbo." + name));
                Assert.Equal("refused", (string?)missing["status"]);
                Assert.Equal("statement_not_in_module", (string?)missing["reason"]);
                var table = Parse(await CreateGuideAsync(c, guide, dryRun, moduleOrBatch: "dbo.t"));
                Assert.Equal("not_a_module", (string?)table["reason"]);
                var template = Parse(await CreateGuideAsync(c, guide, dryRun, type: "TEMPLATE"));
                Assert.Equal("bad_guide_type", (string?)template["reason"]);
            }

            Assert.Null(await GuideColumnAsync(c, guide, "name"));
            Assert.Equal(0, await JournalActionRowsAsync(c));

            await AdminExecAsync(c, $"EXEC sys.sp_create_plan_guide @name = N'{guide}', @stmt = N'{GuideStatement}', @type = N'OBJECT', @module_or_batch = N'dbo.{name}', @params = NULL, @hints = N'OPTION (MAXDOP 1)';");
            foreach (var dryRun in new[] { true, false })
            {
                var taken = Parse(await CreateGuideAsync(c, guide, dryRun, moduleOrBatch: "dbo." + name));
                Assert.Equal("refused", (string?)taken["status"]);
                Assert.Equal("guide_exists", (string?)taken["reason"]);
            }

            Assert.Equal("OPTION (MAXDOP 1)", await GuideColumnAsync(c, guide, "hints"));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task ADatabaseTheServersEntryExcludes_IsRefusedForAGuide_ForADryRunAndAWrite()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);
            await PgExecAsync(c.Connection, $"UPDATE config.config_monitored_servers SET excluded_databases = ARRAY['{c.Sql.Database.ToUpperInvariant()}'] WHERE server_id = 7", c.Ct);

            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await CreateGuideAsync(c, "g_" + name, dryRun, moduleOrBatch: "dbo." + name));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("database_excluded", (string?)json["reason"]);
            }

            Assert.Null(await GuideColumnAsync(c, "g_" + name, "name"));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task AGuideOnAPostgreSqlTarget_IsRefusedWithItsOwnReason()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var json = Parse(await CreateGuideAsync(c, "g_x", dryRun: true, moduleOrBatch: "dbo.p", server: "srv-8"));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("postgres_target", (string?)json["reason"]);
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task WithoutTheGuideSwitch_TheCreateIsRefused_ForADryRunAndAWrite_AndNothingIsCreated()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            /* The other actions are on in this setup; create_plan_guide has no row, so it is off. */
            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await CreateGuideAsync(c, "g_" + name, dryRun, moduleOrBatch: "dbo." + name));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("server_action_off", (string?)json["reason"]);
            }

            Assert.Null(await GuideColumnAsync(c, "g_" + name, "name"));
        });
    }

    [Fact]
    public Task WithoutALevelThreeRoute_TheCreateIsRefused_AndNothingIsCreated() => AssertGuideRefusedAsync(Without.Level, "not_level_3");

    [Fact]
    public Task WithAgentActionsOff_TheCreateIsRefused_AndNothingIsCreated() => AssertGuideRefusedAsync(Without.ActionsEnabled, "actions_off");

    private static Task AssertGuideRefusedAsync(Without without, string reason) =>
        RunAsync(without, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnGuideOnAsync(c);
            var json = Parse(await CreateGuideAsync(c, "g_" + name, dryRun: false, moduleOrBatch: "dbo." + name));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(reason, (string?)json["reason"]);
            Assert.Null(await GuideColumnAsync(c, "g_" + name, "name"));
        });
}
