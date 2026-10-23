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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* The recompile half of the plan force live tests (#6569): the same store and SQL Server setup (a partial class, so the private setup is shared).
   Each test makes its own scratch store and SQL Server database, creates a throwaway procedure in it, and drops the database after. */
public sealed partial class PlanForceToolLiveTests
{
    private const string RecompileKind = "module_recompile";

    private static Task<string> RecompileAsync(Ctx c, string module, bool dryRun, string server = "srv-7", string? database = null) =>
        DarlingMcpPlanForceTools.RunRecompileAsync(c.Source, c.Source, c.Log, server, database ?? c.Sql.Database, module, c.Arg, dryRun, c.Ct);

    private static Task TurnRecompileOnAsync(Ctx c) =>
        PgExecAsync(c.Connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'recompile_module', true, 'test')", c.Ct);

    /// <summary>Makes a procedure and runs it, so its plan is cached. The recompile marker is that plan: <c>sp_recompile</c> removes it from the cache.</summary>
    private static async Task MakeProcedureAsync(Ctx c, string name)
    {
        await using var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
        await ExecAsync(admin, $"CREATE PROCEDURE dbo.[{name}] AS SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1;", c.Ct);
        for (var i = 0; i < 3; i++)
            await ExecAsync(admin, $"EXEC dbo.[{name}];", c.Ct);
    }

    private static async Task<long> ProcedureExecutionsAsync(Ctx c, string name)
    {
        await using var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
        await using var command = new Microsoft.Data.SqlClient.SqlCommand(
            "SELECT ISNULL(SUM(ps.execution_count), 0) FROM sys.dm_exec_procedure_stats AS ps WHERE ps.database_id = DB_ID() AND ps.object_id = OBJECT_ID(@name);", admin);
        command.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@name", System.Data.SqlDbType.NVarChar, 256) { Value = "dbo.[" + name + "]" });
        return Convert.ToInt64(await command.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task ADryRunRecompile_ShowsTheStatementAndTheObject_RunsNothing_AndJournalsNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnRecompileOnAsync(c);

            var answer = await RecompileAsync(c, "dbo." + name, dryRun: true);
            var json = Parse(answer);
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.Equal($"USE [{c.Sql.Database}]; EXEC sys.sp_recompile @objname = N'[dbo].[{name}]';", (string?)json["statement"]);
            Assert.Equal("SQL_STORED_PROCEDURE", (string?)json["before"]!["object_type"]);
            Assert.Equal(3, await ProcedureExecutionsAsync(c, name));
            Assert.Equal(0, await JournalActionRowsAsync(c));
            var parameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.RecompileModule))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)parameter.DefaultValue!);
            AssertNoSecret(c, answer);
        });
    }

    [Fact]
    public async Task ARealRecompile_RunsSpRecompile_JournalsCalledAndDone_AnswersTheJournalId_AndHasNoUndo()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnRecompileOnAsync(c);
            Assert.Equal(3, await ProcedureExecutionsAsync(c, name));

            var answer = await RecompileAsync(c, $"[dbo].[{name}]", dryRun: false);
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.True(json.ContainsKey("undo"), "a recompile answers undo, and it is null");
            Assert.Null(json["undo"]);

            /* The marker: sp_recompile drops the procedure's cached plan, so the server no longer counts its three runs. */
            Assert.Equal(0, await ProcedureExecutionsAsync(c, name));

            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(RecompileKind, (string?)called["kind"]);
            Assert.Equal("recompile_module", (string?)called["tool"]);
            Assert.Equal($"[dbo].[{name}]", (string?)called["target"]!["id"]);
            Assert.Equal(c.Sql.Database, (string?)called["target"]!["database"]);
            Assert.Equal(7, (int)called["target"]!["server_id"]!);
            Assert.Equal("SQL_STORED_PROCEDURE", (string?)called["before"]!["object_type"]);
            var done = Assert.Single(await RowsAsync(c, "action_done"));
            Assert.Contains("sp_recompile", (string?)done["after"]!["statement"], StringComparison.Ordinal);
            Assert.Equal((long)json["journal_id"]!, (long)done["called_id"]!);

            var list = await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct);
            var change = Assert.Single(list.Changes);
            Assert.Equal(RecompileKind, change.Kind);
            Assert.Equal(AgentChangeResult.Done, change.Result);
            Assert.False(change.CanTryUndo(DateTime.UtcNow), "a recompile has no undo");
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task ANameWithAQuoteOrABracket_ResolvesThroughTheParameter_AndRunsNothingElse()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await TurnRecompileOnAsync(c);
            foreach (var hostile in new[] { "dbo.x'; DROP TABLE t; --", "[dbo].[x]]; DROP TABLE dbo.t; --]", "dbo.x'' OR 1=1" })
            {
                foreach (var dryRun in new[] { true, false })
                {
                    var json = Parse(await RecompileAsync(c, hostile, dryRun));
                    Assert.Equal("refused", (string?)json["status"]);
                    Assert.Equal("module_not_found", (string?)json["reason"]);
                }
            }

            await using var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
            Assert.Equal("3", Convert.ToString(await ScalarAsync(admin, "SELECT COUNT_BIG(*) FROM dbo.t;", c.Ct), CultureInfo.InvariantCulture));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task ATableName_IsRefusedNotAModule_AndAMissingOneModuleNotFound_BeforeAnythingRuns()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await TurnRecompileOnAsync(c);
            foreach (var dryRun in new[] { true, false })
            {
                var table = Parse(await RecompileAsync(c, "dbo.t", dryRun));
                Assert.Equal("refused", (string?)table["status"]);
                Assert.Equal("not_a_module", (string?)table["reason"]);
                var missing = Parse(await RecompileAsync(c, "dbo.no_such_proc", dryRun));
                Assert.Equal("module_not_found", (string?)missing["reason"]);
                var otherDatabase = Parse(await RecompileAsync(c, "dbo.t", dryRun, database: "master"));
                Assert.Equal("module_not_found", (string?)otherDatabase["reason"]);
            }

            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'not_a_module'", c.Ct));
        });
    }

    [Fact]
    public async Task ADatabaseTheServersEntryExcludes_IsRefused_ForADryRunAndAWrite_AndNothingIsReadOrRun()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnRecompileOnAsync(c);
            await PgExecAsync(c.Connection, $"UPDATE config.config_monitored_servers SET excluded_databases = ARRAY['{c.Sql.Database.ToUpperInvariant()}'] WHERE server_id = 7", c.Ct);

            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await RecompileAsync(c, "dbo." + name, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("database_excluded", (string?)json["reason"]);
                Assert.Contains("is excluded in this server's entry, so Darling does not act in it", (string?)json["message"], StringComparison.Ordinal);
            }

            /* Nothing ran on the server (the procedure kept its three cached runs), and nothing was journaled as a call. */
            Assert.Equal(3, await ProcedureExecutionsAsync(c, name));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task AValidTwoPartNameLongerThan256Characters_Resolves_AndRuns_AndTheJournalNamesTheResolvedObject()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            /* Two parts of 128 characters: the normal form [schema].[name] is 261 characters, past the 256 the parameter used to hold. */
            var schema = "s" + new string('a', 127);
            var name = "p" + new string('b', 127);
            await using (var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
            {
                await ExecAsync(admin, $"CREATE SCHEMA [{schema}];", c.Ct);
                await ExecAsync(admin, $"CREATE PROCEDURE [{schema}].[{name}] AS SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1;", c.Ct);
                for (var i = 0; i < 3; i++)
                    await ExecAsync(admin, $"EXEC [{schema}].[{name}];", c.Ct);
            }

            await TurnRecompileOnAsync(c);
            var typed = $"{schema}.{name}";
            Assert.True($"[{schema}].[{name}]".Length > 256);

            var dryText = await RecompileAsync(c, typed, dryRun: true);
            var dry = Parse(dryText);
            Assert.True("dry_run" == (string?)dry["status"], dryText);
            Assert.Equal(schema, (string?)dry["before"]!["schema"]);
            Assert.Equal(name, (string?)dry["before"]!["name"]);

            var done = Parse(await RecompileAsync(c, typed, dryRun: false));
            Assert.Equal("done", (string?)done["status"]);
            Assert.Equal(schema, (string?)done["before"]!["schema"]);
            Assert.Equal(name, (string?)done["before"]!["name"]);
            var doneRow = Assert.Single(await RowsAsync(c, "action_done"));
            Assert.Contains($"[{schema}].[{name}] was marked for recompile", (string?)doneRow["after"]!["message"], StringComparison.Ordinal);

            /* The journal row names the schema and name the server resolved. */
            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(schema, (string?)called["before"]!["schema"]);
            Assert.Equal(name, (string?)called["before"]!["name"]);
            Assert.Equal($"[{schema}].[{name}]", (string?)called["before"]!["module"]);
        });
    }

    [Fact]
    public async Task ANameOverTheLimit_IsRefusedBeforeTheServerIsReached()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            await TurnRecompileOnAsync(c);
            var json = Parse(await RecompileAsync(c, "dbo." + new string('x', 129), dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("bad_module_name", (string?)json["reason"]);
        });
    }

    [Fact]
    public async Task ABadName_IsRefusedBadModuleName_BeforeTheGate_AndJournaled()
    {
        await RunAsync(Without.Switch, async c =>
        {
            /* The switch is off, yet the answer is the name's own refusal: a name that cannot do anything is refused before the gate. */
            var json = Parse(await RecompileAsync(c, "one_part_only", dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("bad_module_name", (string?)json["reason"]);
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'bad_module_name'", c.Ct));
        });
    }

    [Fact]
    public async Task ALoginWithoutAlterOnTheObject_IsRefusedRecompilePermissionDenied_AndTheProcedureKeepsItsPlan()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnRecompileOnAsync(c);
            await using (var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
                await ExecAsync(admin, $"DENY ALTER ON OBJECT::dbo.[{name}] TO [{c.Sql.Login}];", c.Ct);

            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await RecompileAsync(c, "dbo." + name, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("recompile_permission_denied", (string?)json["reason"]);
            }

            Assert.Equal(3, await ProcedureExecutionsAsync(c, name));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task ARecompileOnAPostgreSqlTarget_IsRefusedWithItsOwnReason()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var json = Parse(await RecompileAsync(c, "dbo.p", dryRun: true, server: "srv-8"));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("postgres_target", (string?)json["reason"]);
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task WithoutTheServersSwitch_TheRecompileIsRefused_ForADryRunAndAWrite_AndRunsNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            /* force_plan, evict_plan and kill_session are on in this setup; recompile_module has no row, so it is off. */
            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await RecompileAsync(c, "dbo." + name, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("server_action_off", (string?)json["reason"]);
            }

            Assert.Equal(3, await ProcedureExecutionsAsync(c, name));
        });
    }

    [Fact]
    public Task WithoutALevelThreeRoute_TheRecompileIsRefused_AndRunsNothing() => AssertRecompileRefusedAsync(Without.Level, "not_level_3");

    [Fact]
    public Task WithAgentActionsOff_TheRecompileIsRefused_AndRunsNothing() => AssertRecompileRefusedAsync(Without.ActionsEnabled, "actions_off");

    [Fact]
    public Task WithoutTheCredentialSlot_TheRecompileIsRefused_AndRunsNothing() => AssertRecompileRefusedAsync(Without.CredentialSlot, "no_remediation_credential");

    private static Task AssertRecompileRefusedAsync(Without without, string reason) =>
        RunAsync(without, async c =>
        {
            var name = "p_" + Guid.NewGuid().ToString("N")[..8];
            await MakeProcedureAsync(c, name);
            await TurnRecompileOnAsync(c);
            var json = Parse(await RecompileAsync(c, "dbo." + name, dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(reason, (string?)json["reason"]);
            Assert.Equal(3, await ProcedureExecutionsAsync(c, name));
        });
}
