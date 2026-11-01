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
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the store fact mints its own scratch database through ScratchPostgres and never touches another test's rows, so the
   class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The Level 3 plan guide create (#6569, slice 2a): the argument checks, the executor's refusals and their order, the statement text, the action's place
/// in the switch lists, and the store rung that lets <c>config.config_server_actions</c> hold both plan guide switches. The tool and the real server are
/// covered by <see cref="PlanForceToolLiveTests"/> (the plan guide half).
/// </summary>
public sealed class PlanGuideTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Statement = "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1";

    [Fact]
    public void AnObjectGuide_IsNormalized_AndASqlGuideKeepsItsBatchAndParams()
    {
        var (obj, code, _) = PlanGuideDefinition.TryCreate("  g1 ", Statement, "object", "dbo.p", null, "  OPTION (RECOMPILE) ");
        Assert.Null(code);
        Assert.Equal(new PlanGuideDefinition("g1", "OBJECT", Statement, "[dbo].[p]", null, "OPTION (RECOMPILE)"), obj);

        var (sql, _, _) = PlanGuideDefinition.TryCreate("g2", Statement, "SQL", null, "", "option(maxdop 1)");
        Assert.Equal(new PlanGuideDefinition("g2", "SQL", Statement, null, null, "option(maxdop 1)"), sql);
        var (batch, _, _) = PlanGuideDefinition.TryCreate("g3", Statement, "SQL", "SELECT 1; " + Statement, "@p int", "OPTION (OPTIMIZE FOR (@p = 1))");
        Assert.Equal("SELECT 1; " + Statement, batch!.ModuleOrBatch);
        Assert.Equal("@p int", batch.Params);
    }

    [Theory]
    [InlineData("g", Statement, "TEMPLATE", "dbo.p", "@0 int", "OPTION (PARAMETERIZATION FORCED)", "bad_guide_argument")]
    [InlineData("g", Statement, "template", "SELECT 1", "@0 int", "OPTION (PARAMETERIZATION FORCED)", "bad_guide_argument")]
    [InlineData("g", Statement, "TEMPLATE", null, null, "OPTION (PARAMETERIZATION FORCED)", "bad_guide_argument")]
    [InlineData("g", Statement, "TEMPLATE", null, "  ", "OPTION (PARAMETERIZATION FORCED)", "bad_guide_argument")]
    [InlineData("g", Statement, "TEMPLATES", null, "@0 int", "OPTION (PARAMETERIZATION FORCED)", "bad_guide_type")]
    [InlineData("g", Statement, "OTHER", "dbo.p", null, "OPTION (RECOMPILE)", "bad_guide_type")]
    [InlineData("g", Statement, null, "dbo.p", null, "OPTION (RECOMPILE)", "bad_guide_type")]
    [InlineData("", Statement, "SQL", null, null, "OPTION (RECOMPILE)", "bad_guide_name")]
    [InlineData("#g", Statement, "SQL", null, null, "OPTION (RECOMPILE)", "bad_guide_name")]
    [InlineData("g\u0001", Statement, "SQL", null, null, "OPTION (RECOMPILE)", "bad_guide_name")]
    [InlineData("g", "", "SQL", null, null, "OPTION (RECOMPILE)", "bad_guide_argument")]
    [InlineData("g", Statement, "SQL", null, null, "<", "bad_guide_argument")]
    [InlineData("g", Statement, "SQL", null, null, "<?xml version=\"1.0\"?>", "bad_guide_argument")]
    [InlineData("g", Statement, "SQL", null, null, "<?xml version=\"1.0\"?> OPTION (RECOMPILE)", "bad_guide_argument")]
    [InlineData("g", Statement, "SQL", null, null, "RECOMPILE", "bad_guide_argument")]
    [InlineData("g", Statement, "SQL", null, null, "DROP TABLE t; --", "bad_guide_argument")]
    [InlineData("g", Statement, "OBJECT", "dbo.p", "@p int", "OPTION (RECOMPILE)", "bad_guide_argument")]
    [InlineData("g", Statement, "OBJECT", null, null, "OPTION (RECOMPILE)", "bad_module_name")]
    [InlineData("g", Statement, "OBJECT", "p", null, "OPTION (RECOMPILE)", "bad_module_name")]
    public void AnArgumentThatCannotMakeAGuide_IsRefused_WithItsOwnReason(string? name, string? statement, string? type, string? module, string? parameters, string? hints, string reason)
    {
        var (definition, code, message) = PlanGuideDefinition.TryCreate(name, statement, type, module, parameters, hints);
        Assert.Null(definition);
        Assert.Equal(reason, code);
        Assert.EndsWith("Nothing was run.", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"></ShowPlanXML>")]
    [InlineData("  <?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<ShowPlanXML></ShowPlanXML>  ")]
    public void AnXmlShowplanHint_IsTaken_ForAnObjectGuideAndASqlGuide(string hints)
    {
        var (sql, code, _) = PlanGuideDefinition.TryCreate("g", Statement, "SQL", null, null, hints);
        Assert.Null(code);
        Assert.Equal(hints.Trim(), sql!.Hints);
        var (obj, objCode, _) = PlanGuideDefinition.TryCreate("g", Statement, "OBJECT", "dbo.p", null, hints);
        Assert.Null(objCode);
        Assert.Equal(hints.Trim(), obj!.Hints);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void NoHints_AreTaken_AsNull_ForEachGuideType(string? hints)
    {
        Assert.Null(PlanGuideDefinition.TryCreate("g", Statement, "SQL", null, null, hints).Definition!.Hints);
        Assert.Null(PlanGuideDefinition.TryCreate("g", Statement, "OBJECT", "dbo.p", null, hints).Definition!.Hints);
        Assert.Null(PlanGuideDefinition.TryCreate("g", Statement, "TEMPLATE", null, "@0 int", hints).Definition!.Hints);
    }

    [Fact]
    public void ATemplateGuide_HasNoModule_KeepsItsParams_AndPrintsItsNullHintsAsNull()
    {
        var (template, code, _) = PlanGuideDefinition.TryCreate(" t1 ", "SELECT n FROM dbo.t WHERE a = @0", "template", "  ", "@0 int", "OPTION (PARAMETERIZATION FORCED)");
        Assert.Null(code);
        Assert.Equal(new PlanGuideDefinition("t1", "TEMPLATE", "SELECT n FROM dbo.t WHERE a = @0", null, "@0 int", "OPTION (PARAMETERIZATION FORCED)"), template);

        var statement = PlanGuideStatement.Create("Sales", PlanGuideDefinition.TryCreate("g", Statement, "SQL", null, null, null).Definition!);
        Assert.EndsWith(", @params = NULL, @hints = NULL;", statement, StringComparison.Ordinal);
        Assert.Contains("@type = N'SQL'", statement, StringComparison.Ordinal);
    }

    [Fact]
    public void ATemplateGuide_ReportsNoCachedBatchCount_AndTheTextSaysSo()
    {
        var template = new PlanGuideDefinition("t", "TEMPLATE", "SELECT n FROM dbo.t WHERE a = @0", null, "@0 int", "OPTION (PARAMETERIZATION FORCED)");
        var words = SqlServerRemediationExecutor.MatchWords(template, 5);
        Assert.Contains("TEMPLATE", words, StringComparison.Ordinal);
        Assert.DoesNotContain("5 cached", words, StringComparison.Ordinal);
    }

    [Fact]
    public void AGuideNameOver128Characters_IsRefused()
    {
        Assert.Equal("bad_guide_name", PlanGuideDefinition.TryCreate(new string('g', 129), Statement, "SQL", null, null, "OPTION (RECOMPILE)").Code);
        Assert.NotNull(PlanGuideDefinition.TryCreate(new string('g', 128), Statement, "SQL", null, null, "OPTION (RECOMPILE)").Definition);
    }

    [Fact]
    public void AReCreateOfADisabledGuide_ShowsTheDisableStatementAfterTheCreate_AndAnEnabledOneShowsNone()
    {
        var guide = new PlanGuideDefinition("g1", "SQL", "SELECT 1", null, null, "OPTION (RECOMPILE)");
        var enabled = PlanGuideStatement.Create("d", guide);
        var disabled = PlanGuideStatement.Create("d", guide, disabled: true);
        Assert.DoesNotContain("DISABLE", enabled, StringComparison.Ordinal);
        Assert.Equal(enabled + " EXEC sys.sp_control_plan_guide N'DISABLE', N'g1';", disabled);
    }

    [Fact]
    public void TheGuideNameCheck_IsTheSameForADropAsForACreate()
    {
        Assert.Equal("g1", PlanGuideDefinition.TryName("  g1 "));
        Assert.Null(PlanGuideDefinition.TryName(""));
        Assert.Null(PlanGuideDefinition.TryName("#temp"));
        Assert.Null(PlanGuideDefinition.TryName("a\nb"));
        Assert.Null(PlanGuideDefinition.TryName(new string('g', 129)));
        Assert.Equal(new string('g', 128), PlanGuideDefinition.TryName(new string('g', 128)));
    }

    [Fact]
    public void TheStatements_NameTheDatabase_DoubleAQuote_AndTheBatchTextHoldsNoValue()
    {
        var guide = new PlanGuideDefinition("g'1", "SQL", "SELECT 'x'; DROP TABLE t; --", null, "@p int", "OPTION (RECOMPILE)");
        Assert.Equal("USE [Sa]]les]; EXEC sys.sp_create_plan_guide @name = N'g''1', @stmt = N'SELECT ''x''; DROP TABLE t; --', @type = N'SQL', @module_or_batch = NULL, @params = N'@p int', @hints = N'OPTION (RECOMPILE)';",
            PlanGuideStatement.Create("Sa]les", guide));
        Assert.Equal("USE [d]; EXEC sys.sp_control_plan_guide N'DROP', N'g''1';", PlanGuideStatement.Drop("d", "g'1"));
        /* The executor sends every value as a typed parameter: the batch text it runs holds none. */
        Assert.DoesNotContain("DROP TABLE", SqlServerRemediationExecutor.CreatePlanGuideBatchSql, StringComparison.Ordinal);
        Assert.DoesNotContain("DROP TABLE", SqlServerRemediationExecutor.DropPlanGuideBatchSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRefusalForAnObjectGuideOnATrigger_NamesTheTriggersTable_AndOneOnAProcedureNamesTheProcedure()
    {
        var onTrigger = new PlanGuideDefinition("g", "OBJECT", Statement, "[dbo].[trg]", null, "OPTION (RECOMPILE)");
        var trigger = SqlServerRemediationExecutor.CreatePlanGuideRefusal("create_plan_guide", true, onTrigger, "d", false, true, "TR", true, true, false, "stmt", "[dbo].[orders]");
        Assert.Equal(RemediationResult.Reasons.GuidePermissionDenied, trigger?.Reason);
        Assert.Contains("ALTER on the table [dbo].[orders]", trigger!.Message, StringComparison.Ordinal);
        Assert.Contains("[dbo].[trg]", trigger.Message, StringComparison.Ordinal);

        var onProcedure = new PlanGuideDefinition("g", "OBJECT", Statement, "[dbo].[p]", null, "OPTION (RECOMPILE)");
        var procedure = SqlServerRemediationExecutor.CreatePlanGuideRefusal("create_plan_guide", true, onProcedure, "d", false, true, "P", true, true, false, "stmt");
        Assert.Contains("ALTER on [dbo].[p]", procedure!.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("table", procedure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCreateRefusals_AreInTheOrderTheFactsAreGathered()
    {
        var obj = new PlanGuideDefinition("g", "OBJECT", Statement, "[dbo].[p]", null, "OPTION (RECOMPILE)");
        var sql = new PlanGuideDefinition("g", "SQL", Statement, null, null, "OPTION (RECOMPILE)");
        static string? Reason(PlanGuideDefinition guide, bool taken, bool found, string? type, bool inModule, bool db, bool module) =>
            SqlServerRemediationExecutor.CreatePlanGuideRefusal("create_plan_guide", true, guide, "d", taken, found, type, inModule, db, module, "stmt")?.Reason;

        Assert.Equal(RemediationResult.Reasons.GuideExists, Reason(obj, true, false, null, false, false, false));
        Assert.Equal(RemediationResult.Reasons.ModuleNotFound, Reason(obj, false, false, null, false, true, true));
        foreach (var other in new[] { "U", "V", "IF", "PC" })
            Assert.Equal(RemediationResult.Reasons.NotAModule, Reason(obj, false, true, other, true, true, true));
        Assert.Equal(RemediationResult.Reasons.StatementNotInModule, Reason(obj, false, true, "P", false, true, true));
        /* Microsoft's grants: an OBJECT guide needs ALTER on the module alone; a SQL guide ALTER on the database alone. */
        Assert.Null(Reason(obj, false, true, "P", true, false, true));
        Assert.Equal(RemediationResult.Reasons.GuidePermissionDenied, Reason(obj, false, true, "P", true, true, false));
        foreach (var module in SqlServerRemediationExecutor.PlanGuideModuleTypes)
            Assert.Null(Reason(obj, false, true, module, true, true, true));
        /* A SQL guide reads no module: the module facts are ignored. */
        Assert.Null(Reason(sql, false, false, null, false, true, true));
        Assert.Equal(RemediationResult.Reasons.GuidePermissionDenied, Reason(sql, false, false, null, false, false, true));
        Assert.Null(Reason(sql, false, false, null, false, true, false));
    }

    [Fact]
    public void TheMatchWords_SayWhatAGuideAppliesTo()
    {
        var obj = new PlanGuideDefinition("g", "OBJECT", Statement, "[dbo].[p]", null, "OPTION (RECOMPILE)");
        var sql = new PlanGuideDefinition("g", "SQL", Statement, null, null, "OPTION (RECOMPILE)");
        Assert.Contains("found in the module's definition", SqlServerRemediationExecutor.MatchWords(obj, null), StringComparison.Ordinal);
        Assert.Contains("No cached batch in this database matches", SqlServerRemediationExecutor.MatchWords(sql, 0), StringComparison.Ordinal);
        Assert.Equal("1 cached batch in this database equal this guide's text right now.", SqlServerRemediationExecutor.MatchWords(sql, 1));
        Assert.Equal("4 cached batches in this database equal this guide's text right now.", SqlServerRemediationExecutor.MatchWords(sql, 4));
        Assert.Contains("could not count", SqlServerRemediationExecutor.MatchWords(sql, null), StringComparison.Ordinal);
    }

    [Fact]
    public async Task APermitForAnotherActionGuideOrServer_IsRefused_AndNothingIsRun()
    {
        var executor = new SqlServerRemediationExecutor(7, new RemediationServer("no-such-host.invalid", null, null, true, false, 0), "login", "env:NEVER_SET", null);
        var guide = new PlanGuideDefinition("g", "SQL", Statement, null, null, "OPTION (RECOMPILE)");
        var ct = TestContext.Current.CancellationToken;
        foreach (var permit in new[] { Permit(ServerActionNames.RecompileModule, 7, "g", "d"), Permit(ServerActionNames.CreatePlanGuide, 8, "g", "d"), Permit(ServerActionNames.CreatePlanGuide, 7, "other", "d"), null })
        {
            var create = await executor.CreatePlanGuideAsync(permit!, guide, dryRun: false, ct);
            Assert.Equal(RemediationOutcome.Refused, create.Outcome);
            Assert.Equal(RemediationResult.Reasons.PermitMismatch, create.Reason);
            Assert.False(create.Wrote);
            var drop = await executor.DropPlanGuideAsync(permit!, "g", null, dryRun: false, ct);
            Assert.Equal(RemediationResult.Reasons.PermitMismatch, drop.Reason);
        }

        var noDatabase = await executor.CreatePlanGuideAsync(Permit(ServerActionNames.CreatePlanGuide, 7, "g", null), guide, dryRun: false, ct);
        Assert.Equal(RemediationResult.Reasons.BadDatabase, noDatabase.Reason);
    }

    private static ServerActionPermit Permit(string action, int serverId, string id, string? database) =>
        (ServerActionPermit)Activator.CreateInstance(typeof(ServerActionPermit), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public,
            null, [action, serverId, "srv", id, database, Guid.NewGuid(), "person:test"], CultureInfo.InvariantCulture)!;

    [Fact]
    public void TheActions_AreInEverySwitchList_ForSqlServerOnly_AndTheCreateIsAnAgentToolWithAnUndoableJournalKind()
    {
        foreach (var action in new[] { ServerActionNames.CreatePlanGuide, ServerActionNames.DropPlanGuide })
        {
            Assert.Contains(action, ServerActionNames.All);
            Assert.True(ServerActionNames.IsValidForEngine(action, postgres: false, sqlEngineEdition: null));
            Assert.False(ServerActionNames.IsValidForEngine(action, postgres: true, sqlEngineEdition: null));
        }

        Assert.Equal("Create plan guides", ServerActionNames.Label(ServerActionNames.CreatePlanGuide));
        Assert.Equal("Drop plan guides", ServerActionNames.Label(ServerActionNames.DropPlanGuide));
        Assert.Contains("create_plan_guide", InvestigationRules.AgentActionToolNames);
        Assert.Contains("create_plan_guide", DarlingMcpWritePolicy.AgentActionTools);
        Assert.True(DarlingMcpWritePolicy.IsWrite("create_plan_guide"));
        Assert.Equal(ServerActionNames.CreatePlanGuide, InvestigationRequestStore.ServerActionForKind(AgentChangeKinds.PlanGuideCreate));
        /* Slice 2b: the drop tool is read by the same lists, and its journal kind keys the drop switch. */
        Assert.Contains("drop_plan_guide", InvestigationRules.AgentActionToolNames);
        Assert.Contains("drop_plan_guide", DarlingMcpWritePolicy.AgentActionTools);
        Assert.True(DarlingMcpWritePolicy.IsWrite("drop_plan_guide"));
        Assert.Equal(ServerActionNames.DropPlanGuide, InvestigationRequestStore.ServerActionForKind(AgentChangeKinds.PlanGuideDrop));
    }

    [Fact]
    public async Task TheRung_LetsTheSwitchTableHoldBothGuideActions_StillRefusesAnUnknownOne_AndASecondRunChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the plan guide switch store fact.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var store = new NpgsqlConnection(scratch.ConnectionString);
        await store.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            var rung = PgMigrations.Scripts.Single(m => m.Name == "server-action-recompile-module");
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (5691, 'srv-a', 'host-a')", ct);
            foreach (var action in new[] { "create_plan_guide", "drop_plan_guide", "recompile_module" })
                await ExecAsync(store, $"INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5691, '{action}', 'erik')", ct);
            var unknown = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5691, 'drop_database', 'erik')", ct));
            Assert.Equal("ck_config_server_actions_action", unknown.ConstraintName);

            /* A store one rung back: the old CHECK refuses both words, and the rung puts the new one in place. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(store, rung.Version - 1, ct);
            foreach (var action in new[] { "create_plan_guide", "drop_plan_guide" })
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(() =>
                    ExecAsync(store, $"INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5691, '{action}', 'erik')", ct));
                Assert.Equal("ck_config_server_actions_action", refused.ConstraintName);
            }

            await ExecAsync(store, rung.Sql, ct);
            foreach (var action in new[] { "create_plan_guide", "drop_plan_guide" })
                await ExecAsync(store, $"INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5691, '{action}', 'erik')", ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
