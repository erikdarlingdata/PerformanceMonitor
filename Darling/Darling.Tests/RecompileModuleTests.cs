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
/// The Level 3 module recompile (#6569): the name's one normal form, the executor's refusals, the action's place in the switch lists, and the
/// store rung that lets <c>config.config_server_actions</c> hold its switch. The tool and the real server are covered by
/// <see cref="PlanForceToolLiveTests"/> (the recompile half).
/// </summary>
public sealed class RecompileModuleTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Theory]
    [InlineData("dbo.MyProc", "[dbo].[MyProc]")]
    [InlineData("[dbo].[MyProc]", "[dbo].[MyProc]")]
    [InlineData("  dbo.[My Proc]  ", "[dbo].[My Proc]")]
    [InlineData("[sales].[a.b]", "[sales].[a.b]")]
    [InlineData("dbo.x'y", "[dbo].[x'y]")]
    [InlineData("[dbo].[a]]b]", "[dbo].[a]]b]")]
    public void ATwoPartName_IsNormalizedToBracketedParts(string name, string expected)
    {
        Assert.True(ModuleRecompileStatement.TryNormalize(name, out var normal));
        Assert.Equal(expected, normal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("MyProc")]
    [InlineData("db.dbo.MyProc")]
    [InlineData("dbo.")]
    [InlineData(".MyProc")]
    [InlineData("[dbo].[MyProc")]
    [InlineData("[dbo]x.MyProc")]
    [InlineData("dbo.My\nProc")]
    public void ANameThatIsNotExactlyTwoParts_IsRefused(string? name)
    {
        Assert.False(ModuleRecompileStatement.TryNormalize(name, out _));
    }

    [Fact]
    public void ANamePartOver128Characters_IsRefused()
    {
        Assert.False(ModuleRecompileStatement.TryNormalize("dbo." + new string('a', 129), out _));
        Assert.True(ModuleRecompileStatement.TryNormalize("dbo." + new string('a', 128), out _));
    }

    [Fact]
    public void TheStatement_NamesTheDatabase_AndDoublesAQuoteInTheName()
    {
        Assert.True(ModuleRecompileStatement.TryNormalize("dbo.x'; DROP TABLE t; --", out var normal));
        Assert.Equal("USE [Sales]; EXEC sys.sp_recompile @objname = N'[dbo].[x''; DROP TABLE t; --]';", ModuleRecompileStatement.For("Sales", normal));
        /* The executor sends the name as a parameter: the batch text it runs holds no name at all. */
        Assert.DoesNotContain("DROP", SqlServerRemediationExecutor.RecompileBatchSql, StringComparison.Ordinal);
        /* The parameter holds the longest normal form: two 128-character parts, every character a doubled bracket. */
        var doubled = new string(']', 256);
        Assert.True(ModuleRecompileStatement.TryNormalize("[" + doubled + "].[" + doubled + "]", out var longest));
        Assert.Equal(ModuleRecompileStatement.MaxNormalLength, longest.Length);
    }

    [Fact]
    public void TheRefusals_AreInTheOrderTheFactsAreGathered_AndOnlyAProcedureFunctionOrTriggerPasses()
    {
        const string Statement = "USE [d]; EXEC sys.sp_recompile @objname = N'[dbo].[p]';";
        static RemediationResult? Refusal(bool found, string? type, bool granted) =>
            SqlServerRemediationExecutor.RecompileRefusal("recompile_module", true, "[dbo].[p]", "d", found, type, type is null ? null : "KIND", granted, Statement);

        Assert.Equal(RemediationResult.Reasons.ModuleNotFound, Refusal(false, null, false)!.Reason);
        foreach (var other in new[] { "U", "V", "SO", "D", "SN" })
            Assert.Equal(RemediationResult.Reasons.NotAModule, Refusal(true, other, true)!.Reason);
        foreach (var module in SqlServerRemediationExecutor.RecompilableTypes)
        {
            Assert.Equal(RemediationResult.Reasons.RecompilePermissionDenied, Refusal(true, module, false)!.Reason);
            Assert.Null(Refusal(true, module, true));
        }

        Assert.Equal(Statement, Refusal(false, null, false)!.Statement);
    }

    [Fact]
    public async Task APermitForAnotherActionObjectOrServer_IsRefused_AndNothingIsRun()
    {
        var executor = new SqlServerRemediationExecutor(7, new RemediationServer("no-such-host.invalid", null, null, true, false, 0), "login", "env:NEVER_SET", null);
        var forOther = Permit(ServerActionNames.EvictPlan, 7, "[dbo].[p]", "d");
        var forOtherServer = Permit(ServerActionNames.RecompileModule, 8, "[dbo].[p]", "d");
        var forOtherObject = Permit(ServerActionNames.RecompileModule, 7, "[dbo].[q]", "d");
        foreach (var permit in new[] { forOther, forOtherServer, forOtherObject, null })
        {
            var result = await executor.RecompileModuleAsync(permit!, "dbo.p", dryRun: false, TestContext.Current.CancellationToken);
            Assert.Equal(RemediationOutcome.Refused, result.Outcome);
            Assert.Equal(RemediationResult.Reasons.PermitMismatch, result.Reason);
            Assert.False(result.Wrote);
        }

        var bad = await executor.RecompileModuleAsync(Permit(ServerActionNames.RecompileModule, 7, "[dbo].[p]", "d"), "dbo.p.extra", dryRun: false, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationResult.Reasons.BadModuleName, bad.Reason);
        var noDatabase = await executor.RecompileModuleAsync(Permit(ServerActionNames.RecompileModule, 7, "[dbo].[p]", null), "dbo.p", dryRun: false, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationResult.Reasons.BadDatabase, noDatabase.Reason);
    }

    private static ServerActionPermit Permit(string action, int serverId, string id, string? database) =>
        (ServerActionPermit)Activator.CreateInstance(typeof(ServerActionPermit), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public,
            null, [action, serverId, "srv", id, database, Guid.NewGuid(), "person:test"], CultureInfo.InvariantCulture)!;

    [Fact]
    public void TheAction_IsInEverySwitchList_ForSqlServerOnly_AndIsAnAgentTool_WithAJournalKindAndNoUndo()
    {
        Assert.Contains(ServerActionNames.RecompileModule, ServerActionNames.All);
        Assert.True(ServerActionNames.IsValidForEngine(ServerActionNames.RecompileModule, postgres: false, sqlEngineEdition: null));
        Assert.False(ServerActionNames.IsValidForEngine(ServerActionNames.RecompileModule, postgres: true, sqlEngineEdition: null));
        Assert.Equal("Recompile procedures, functions and triggers", ServerActionNames.Label(ServerActionNames.RecompileModule));
        Assert.Contains("recompile_module", InvestigationRules.AgentActionToolNames);
        Assert.Contains("recompile_module", DarlingMcpWritePolicy.AgentActionTools);
        Assert.True(DarlingMcpWritePolicy.IsWrite("recompile_module"));
        Assert.Equal(ServerActionNames.RecompileModule, InvestigationRequestStore.ServerActionForKind(AgentChangeKinds.ModuleRecompile));
        Assert.DoesNotContain(AgentChangeKinds.ModuleRecompile, AgentChangeUndo.UndoableKinds);
    }

    [Fact]
    public async Task TheRung_LetsTheSwitchTableHoldTheAction_StillRefusesAnUnknownOne_AndASecondRunChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the recompile switch store fact.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var store = new NpgsqlConnection(scratch.ConnectionString);
        await store.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            var rung = PgMigrations.Scripts.Single(m => m.Name == "server-action-recompile-module");
            Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (5690, 'srv-a', 'host-a')", ct);
            await ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5690, 'recompile_module', 'erik')", ct);
            Assert.Equal("False", await ScalarAsync(store, "SELECT enabled FROM config.config_server_actions WHERE server_id = 5690 AND action = 'recompile_module'", ct));
            var unknown = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5690, 'drop_database', 'erik')", ct));
            Assert.Equal("ck_config_server_actions_action", unknown.ConstraintName);

            /* Every action the service knows is storable, and the three earlier ones still are. */
            foreach (var action in ServerActionNames.All.Where(a => a != "recompile_module"))
                await ExecAsync(store, $"INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5690, '{action}', 'erik')", ct);

            /* A second run replaces no constraint. */
            const string Identity = "SELECT c.oid::text FROM pg_constraint c WHERE c.conname = 'ck_config_server_actions_action'";
            var before = await ScalarAsync(store, Identity, ct);
            await ExecAsync(store, rung.Sql, ct);
            Assert.Equal(before, await ScalarAsync(store, Identity, ct));

            /* A store one rung back: the old CHECK refuses the word, and the rung puts the new one in place. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(store, rung.Version - 1, ct);
            var refusedBefore = await Assert.ThrowsAsync<PostgresException>(() =>
                ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5690, 'recompile_module', 'erik')", ct));
            Assert.Equal("ck_config_server_actions_action", refusedBefore.ConstraintName);
            await ExecAsync(store, rung.Sql, ct);
            await ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, set_by) VALUES (5690, 'recompile_module', 'erik')", ct);
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

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }
}
