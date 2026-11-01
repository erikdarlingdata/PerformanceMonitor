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

/* The plan guide drop half of the plan force live tests (#6569, slice 2b): the same store and SQL Server setup as the create tests (a partial class, so the private setup is shared).
   Each test makes its own scratch store and SQL Server database, creates a throwaway procedure and guides in it, and drops the database after. */
public sealed partial class PlanForceToolLiveTests
{
    private const string DropKind = "plan_guide_drop";

    private static Task<string> DropGuideAsync(Ctx c, string name, bool dryRun, string server = "srv-7", string? database = null) =>
        DarlingMcpPlanForceTools.RunDropPlanGuideAsync(c.Source, c.Source, c.Log, server, database ?? c.Sql.Database, name, c.Arg, dryRun, c.Ct);

    private static Task TurnDropOnAsync(Ctx c) =>
        PgExecAsync(c.Connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'drop_plan_guide', true, 'test')", c.Ct);

    /// <summary>Makes an OBJECT guide on the procedure and a SQL guide on a parameterized batch (the second one disabled when asked), straight on the server.</summary>
    private static async Task MakeObjectGuideAsync(Ctx c, string guide, string procedure, string hints = "OPTION (RECOMPILE)", bool disabled = false)
    {
        await AdminExecAsync(c, $"EXEC sys.sp_create_plan_guide @name = N'{guide}', @stmt = N'{GuideStatement}', @type = N'OBJECT', @module_or_batch = N'dbo.{procedure}', @params = NULL, @hints = N'{hints}';"
            + (disabled ? $" EXEC sys.sp_control_plan_guide N'DISABLE', N'{guide}';" : ""));
    }

    private static async Task MakeSqlGuideAsync(Ctx c, string guide, bool disabled)
    {
        await AdminExecAsync(c, $"EXEC sys.sp_create_plan_guide @name = N'{guide}', @stmt = N'SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= @p', @type = N'SQL', @module_or_batch = NULL, @params = N'@p int', @hints = N'OPTION (OPTIMIZE FOR (@p = 1))';"
            + (disabled ? $" EXEC sys.sp_control_plan_guide N'DISABLE', N'{guide}';" : ""));
    }

    /// <summary>Every column of the guide's <c>sys.plan_guides</c> row that a re-create must reproduce, as one string; null when there is no such guide.</summary>
    private static async Task<string?> GuideRowAsync(Ctx c, string name)
    {
        var parts = new System.Collections.Generic.List<string>();
        foreach (var column in new[] { "query_text", "scope_type", "scope_batch", "parameters", "hints", "is_disabled", "scope_object_id" })
        {
            var value = await GuideColumnAsync(c, name, column);
            if (value is null && column == "query_text")
                return null;
            parts.Add(column + "=" + (value ?? "<null>"));
        }

        return string.Join("|", parts);
    }

    [Fact]
    public async Task ADryRunDrop_ShowsTheStatementAndTheDefinition_DropsNothing_AndJournalsNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, guide, name);
            await TurnDropOnAsync(c);

            var answer = await DropGuideAsync(c, guide, dryRun: true);
            var json = Parse(answer);
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.Equal($"USE [{c.Sql.Database}]; EXEC sys.sp_control_plan_guide N'DROP', N'{guide}';", (string?)json["statement"]);
            var shown = json["before"]!["guide"]!;
            Assert.Equal("OBJECT", (string?)shown["type"]);
            Assert.Equal(GuideStatement, (string?)shown["statement"]);
            Assert.Equal($"[dbo].[{name}]", (string?)shown["module_or_batch"]);
            Assert.Equal("OPTION (RECOMPILE)", (string?)shown["hints"]);
            Assert.False((bool)shown["is_disabled"]!);
            Assert.NotNull(await GuideRowAsync(c, guide));
            Assert.Equal(0, await JournalActionRowsAsync(c));
            var parameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.DropPlanGuide))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)parameter.DefaultValue!);
            AssertNoSecret(c, answer);
        });
    }

    [Theory]
    [InlineData("OBJECT", false)]
    [InlineData("SQL", true)]
    public async Task ADroppedGuide_IsGone_JournalsTheWholeDefinition_AndAPersonsUndoReCreatesItEqualToTheOriginal(string type, bool disabled)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            if (type == "OBJECT")
                await MakeObjectGuideAsync(c, guide, name, disabled: disabled);
            else
                await MakeSqlGuideAsync(c, guide, disabled);
            await TurnDropOnAsync(c);
            var original = await GuideRowAsync(c, guide);
            Assert.NotNull(original);
            Assert.Contains("is_disabled=" + (disabled ? "1" : "0"), original, StringComparison.Ordinal);

            var answer = await DropGuideAsync(c, guide, dryRun: false);
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.Null(await GuideRowAsync(c, guide));

            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(DropKind, (string?)called["kind"]);
            Assert.Equal("drop_plan_guide", (string?)called["tool"]);
            Assert.Equal(guide, (string?)called["target"]!["id"]);
            Assert.Equal(c.Sql.Database, (string?)called["before"]!["database"]);
            var journaled = called["before"]!["guide"]!;
            Assert.Equal(type, (string?)journaled["type"]);
            Assert.Equal(GuideStatement.Replace("a >= 1", type == "SQL" ? "a >= @p" : "a >= 1", StringComparison.Ordinal), (string?)journaled["statement"]);
            Assert.Equal(disabled, (bool)journaled["is_disabled"]!);
            if (type == "OBJECT")
                Assert.Equal($"[dbo].[{name}]", (string?)journaled["module_or_batch"]);
            else
                Assert.Equal("@p int", (string?)journaled["params"]);
            Assert.Single(await RowsAsync(c, "action_done"));

            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct)).Changes);
            Assert.Equal(DropKind, change.Kind);
            Assert.True(change.CanTryUndo(DateTime.UtcNow), "a plan guide drop can be undone");

            /* The undo shows the statement first and writes nothing. */
            var calledId = (long)json["journal_id"]!;
            var shown = await UndoAsync(c, calledId, confirm: false);
            Assert.Equal(PlanUndoAnswer.Confirm, shown.Status);
            Assert.StartsWith($"USE [{c.Sql.Database}]; EXEC sys.sp_create_plan_guide @name = N'{guide}'", shown.Statement, StringComparison.Ordinal);
            Assert.Equal(disabled, shown.Statement!.EndsWith($"EXEC sys.sp_control_plan_guide N'DISABLE', N'{guide}';", StringComparison.Ordinal));
            Assert.Null(await GuideRowAsync(c, guide));

            var undone = await UndoAsync(c, calledId, confirm: true);
            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.Equal(original, await GuideRowAsync(c, guide));
            var undoneRow = Assert.Single(await RowsAsync(c, "action_undone"));
            Assert.Equal(calledId, (long)undoneRow["called_id"]!);
            Assert.Equal("person:alice", await PgScalarAsync(c.Connection, "SELECT actor FROM config.investigation_journal WHERE event = 'action_undone'", c.Ct));

            var again = await UndoAsync(c, calledId, confirm: true);
            Assert.Equal("already_undone", again.Code);
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task TheUndoOfADrop_WhileAGuideWithThatNameExists_IsRefused_AndChangesNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, guide, name);
            await TurnDropOnAsync(c);
            var calledId = (long)Parse(await DropGuideAsync(c, guide, dryRun: false))["journal_id"]!;

            /* Somebody made a guide under the same name since. */
            await MakeObjectGuideAsync(c, guide, name, hints: "OPTION (MAXDOP 1)");
            var replacement = await GuideRowAsync(c, guide);

            var answer = await UndoAsync(c, calledId, confirm: true);
            Assert.Equal(PlanUndoAnswer.RefusedStatus, answer.Status);
            Assert.Equal("guide_exists", answer.Code);
            Assert.Equal(replacement, await GuideRowAsync(c, guide));
            Assert.Empty(await RowsAsync(c, "action_undone"));
        });
    }

    [Fact]
    public async Task AGuideThatIsNotThere_IsRefused_AndATemplateGuide_IsDropped_AndUndone()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var template = "t_" + name;
            await MakeProcedureAsync(c, name);
            await TurnDropOnAsync(c);
            await AdminExecAsync(c, "DECLARE @s nvarchar(max), @p nvarchar(max); EXEC sys.sp_get_query_template N'SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1', @s OUTPUT, @p OUTPUT; "
                + $"EXEC sys.sp_create_plan_guide @name = N'{template}', @stmt = @s, @type = N'TEMPLATE', @module_or_batch = NULL, @params = @p, @hints = N'OPTION (PARAMETERIZATION FORCED)';");
            var original = await GuideRowAsync(c, template);
            Assert.NotNull(original);

            foreach (var dryRun in new[] { true, false })
            {
                var missing = Parse(await DropGuideAsync(c, "g_" + name, dryRun));
                Assert.Equal("refused", (string?)missing["status"]);
                Assert.Equal("guide_not_found", (string?)missing["reason"]);
            }

            var dry = Parse(await DropGuideAsync(c, template, dryRun: true));
            Assert.Equal("dry_run", (string?)dry["status"]);
            Assert.Equal("TEMPLATE", (string?)dry["before"]!["guide"]!["type"]);
            Assert.Equal(original, await GuideRowAsync(c, template));
            Assert.Equal(0, await JournalActionRowsAsync(c));

            var dropped = await RealDropAsync(c, template);
            Assert.Null(await GuideRowAsync(c, template));
            var undone = await UndoAsync(c, dropped, confirm: true);
            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.Equal(original, await GuideRowAsync(c, template));
        });
    }

    /// <summary>
    /// #6569 final review: the Undo re-creates a dropped guide through the create's own checks, so a guide that would not pass them (here, hints over the
    /// 4,000,000-character cap, which the server took) is refused on a dry run and on a write, and nothing is dropped or journaled as a change.
    /// </summary>
    [Fact]
    public async Task AGuideTheUndoCouldNotReCreate_IsRefusedOnADryRunAndOnAWrite_AndDropsNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var guide = "g_" + name;
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, guide, name, hints: "OPTION (RECOMPILE /*" + new string('x', PerformanceMonitor.Darling.Remediation.PlanGuideDefinition.MaxText) + "*/)");
            await TurnDropOnAsync(c);
            var original = await GuideRowAsync(c, guide);
            Assert.NotNull(original);

            foreach (var dryRun in new[] { true, false })
            {
                var answer = Parse(await DropGuideAsync(c, guide, dryRun));
                Assert.Equal("refused", (string?)answer["status"]);
                Assert.Equal(DarlingMcpPlanForceTools.GuideNotUndoable, (string?)answer["reason"]);
                Assert.Equal(original, await GuideRowAsync(c, guide));
                Assert.Equal(0, await JournalActionRowsAsync(c));
            }
        });
    }

    [Fact]
    public async Task ALoginWithAlterOnTheProcedureOnly_DropsAnObjectGuide_AndIsRefusedASqlGuideWithThePermissionSentence()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            var objectGuide = "g_" + name;
            var sqlGuide = "s_" + name;
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, objectGuide, name);
            await MakeSqlGuideAsync(c, sqlGuide, disabled: false);
            await TurnDropOnAsync(c);
            var sqlRow = await GuideRowAsync(c, sqlGuide);
            /* The test login is db_owner; narrow it to what Microsoft documents for an OBJECT guide: ALTER on the referenced object. */
            await AdminExecAsync(c, $"ALTER ROLE db_owner DROP MEMBER [{c.Sql.Login}]; GRANT ALTER ON OBJECT::dbo.[{name}] TO [{c.Sql.Login}];");

            foreach (var dryRun in new[] { true, false })
            {
                var refused = Parse(await DropGuideAsync(c, sqlGuide, dryRun));
                Assert.Equal("refused", (string?)refused["status"]);
                Assert.Equal("guide_permission_denied", (string?)refused["reason"]);
                Assert.Contains("does not hold ALTER on the database", (string?)refused["message"] ?? "", StringComparison.Ordinal);
            }

            Assert.Equal(sqlRow, await GuideRowAsync(c, sqlGuide));

            var dropped = Parse(await DropGuideAsync(c, objectGuide, dryRun: false));
            Assert.Equal("done", (string?)dropped["status"]);
            Assert.Null(await GuideRowAsync(c, objectGuide));

            /* The undo follows the create split: ALTER on the object is enough to re-create an OBJECT guide. */
            var undone = await UndoAsync(c, (long)dropped["journal_id"]!, confirm: true);
            Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
            Assert.NotNull(await GuideRowAsync(c, objectGuide));
        });
    }

    [Fact]
    public async Task WithoutTheDropSwitch_TheDropIsRefused_ForADryRunAndAWrite_AndNothingIsDropped()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, "g_" + name, name);
            /* The other actions are on in this setup; drop_plan_guide has no row, so it is off. */
            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await DropGuideAsync(c, "g_" + name, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("server_action_off", (string?)json["reason"]);
            }

            Assert.NotNull(await GuideRowAsync(c, "g_" + name));
        });
    }

    [Fact]
    public Task WithoutALevelThreeRoute_TheDropIsRefused_AndNothingIsDropped() => AssertDropRefusedAsync(Without.Level, "not_level_3");

    [Fact]
    public Task WithAgentActionsOff_TheDropIsRefused_AndNothingIsDropped() => AssertDropRefusedAsync(Without.ActionsEnabled, "actions_off");

    private static Task AssertDropRefusedAsync(Without without, string reason) =>
        RunAsync(without, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, "g_" + name, name);
            await TurnDropOnAsync(c);
            var json = Parse(await DropGuideAsync(c, "g_" + name, dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(reason, (string?)json["reason"]);
            Assert.NotNull(await GuideRowAsync(c, "g_" + name));
        });

    [Fact]
    public async Task ADatabaseTheServersEntryExcludes_IsRefusedForADrop_ForADryRunAndAWrite()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, "g_" + name, name);
            await TurnDropOnAsync(c);
            await PgExecAsync(c.Connection, $"UPDATE config.config_monitored_servers SET excluded_databases = ARRAY['{c.Sql.Database.ToUpperInvariant()}'] WHERE server_id = 7", c.Ct);

            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await DropGuideAsync(c, "g_" + name, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("database_excluded", (string?)json["reason"]);
            }

            Assert.NotNull(await GuideRowAsync(c, "g_" + name));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task ADropOnAPostgreSqlTarget_IsRefusedWithItsOwnReason()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var json = Parse(await DropGuideAsync(c, "g_x", dryRun: true, server: "srv-8"));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("postgres_target", (string?)json["reason"]);
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Theory]
    [InlineData(5)]
    [InlineData(8)]
    public async Task ADropOnAnAzureSqlTarget_IsRefusedAsEngineNotSupported_ForADryRunAndAWrite(int engineEdition)
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await MakeObjectGuideAsync(c, "g_" + name, name);
            await TurnDropOnAsync(c);
            await PgExecAsync(c.Connection, $"UPDATE collect.servers SET sql_engine_edition = {engineEdition.ToString(CultureInfo.InvariantCulture)} WHERE server_id = 7", c.Ct);

            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await DropGuideAsync(c, "g_" + name, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("engine_not_supported", (string?)json["reason"]);
            }

            Assert.NotNull(await GuideRowAsync(c, "g_" + name));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }
}
