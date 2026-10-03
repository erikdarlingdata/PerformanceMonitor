/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The <c>--set-collector-run-at</c> verb (#4938), without a store: its spelling, its strict grammar, the command the
/// verb stands in for and the plan the executor makes of it (so the verb owns no SQL), the usage text, and the line
/// the read-back prints for a row that carries a run time. The verb run end to end is
/// <see cref="CollectorRunTimeCliVerbLiveTests"/>.
/// </summary>
public sealed class CollectorRunTimeCliVerbTests
{
    private const string Verb = "--set-collector-run-at";

    private static bool Parse(string[] rest, out string? collector, out string? runAt, out string? server, out string? config, out string? error) =>
        DarlingCliCommands.TryParseCollectorRunAtArgs(Verb, rest, out collector, out runAt, out server, out config, out error);

    [Fact]
    public void TheVerb_IsRecognisedBySpelling_CaseInsensitively_AndNoOtherVerbIs()
    {
        Assert.True(DarlingCliCommands.IsSetCollectorRunAtVerb("--set-collector-run-at"));
        Assert.True(DarlingCliCommands.IsSetCollectorRunAtVerb("--SET-COLLECTOR-RUN-AT"));
        Assert.False(DarlingCliCommands.IsSetCollectorRunAtVerb("--set-collector-run"));
        Assert.False(DarlingCliCommands.IsSetCollectorRunAtVerb("--enable-collector"));
        Assert.False(DarlingCliCommands.IsEnableCollectorVerb("--set-collector-run-at"));
        Assert.False(DarlingCliCommands.IsDisableCollectorVerb("--set-collector-run-at"));
    }

    [Fact]
    public void TheGrammar_IsACollectorAndATime_WithAnOptionalServerAndConfig_InAnyOrder()
    {
        Assert.True(Parse(new[] { "index_object_stats", "02:00" }, out var collector, out var runAt, out var server, out var config, out var error));
        Assert.Equal("index_object_stats", collector);
        Assert.Equal("02:00", runAt);
        Assert.Null(server);
        Assert.Null(config);
        Assert.Null(error);

        Assert.True(Parse(new[] { "--server", "sql01", "index_object_stats", "none", "--config", "darling.json" }, out collector, out runAt, out server, out config, out error));
        Assert.Equal("index_object_stats", collector);
        Assert.Equal("none", runAt);
        Assert.Equal("sql01", server);
        Assert.Equal("darling.json", config);

        Assert.True(Parse(new[] { "index_object_stats", "default", "--server", "sql01" }, out _, out runAt, out server, out _, out _));
        Assert.Equal("default", runAt);
        Assert.Equal("sql01", server);
    }

    [Theory]
    [InlineData(new string[0], "needs a collector name")]
    [InlineData(new[] { "index_object_stats" }, "needs a time")]
    [InlineData(new[] { "index_object_stats", "02:00", "extra" }, "takes ONE collector name and ONE time")]
    [InlineData(new[] { "index_object_stats", "02:00", "--server" }, "--server needs a server name")]
    [InlineData(new[] { "index_object_stats", "02:00", "--server", "--config", "x" }, "--server needs a server name")]
    [InlineData(new[] { "index_object_stats", "02:00", "--config" }, "--config needs a path")]
    [InlineData(new[] { "index_object_stats", "02:00", "--bogus" }, "Unknown option")]
    public void TheGrammar_RefusesAnythingElse_NamingTheProblem(string[] rest, string expected)
    {
        Assert.False(Parse(rest, out _, out _, out _, out _, out var error));
        Assert.Contains(expected, error, StringComparison.Ordinal);
        Assert.Contains(Verb, error, StringComparison.Ordinal);
    }

    [Fact]
    public void TheUsageText_NamesTheThreeValues_TheScopes_AndTheCollectors()
    {
        var text = DarlingCliCommands.CollectorRunAtUsageText();

        Assert.Contains("--set-collector-run-at <collector> <HH:MM|none|default> [--server <server>]", text, StringComparison.Ordinal);
        Assert.Contains("HH:MM", text, StringComparison.Ordinal);
        Assert.Contains("none", text, StringComparison.Ordinal);
        Assert.Contains("default", text, StringComparison.Ordinal);
        Assert.Contains("server time", text, StringComparison.Ordinal);
        Assert.Contains("index_object_stats", text, StringComparison.Ordinal);
    }

    private static CommandPlan Plan(string collector, string runAt, int? serverId) =>
        DarlingCliCommands.PlanCollectorRunAt(collector, runAt, serverId);

    [Fact]
    public void TheVerbsCommand_IsTheCommandPlanesSetCollectorRunAt_Row()
    {
        var command = DarlingCliCommands.BuildCollectorRunAtCommand("index_object_stats", "02:00", 7);

        Assert.Equal("set_collector_run_at", command.CommandType);
        Assert.Equal(7, command.TargetServerId);
        Assert.Equal("cli", command.RequestedBy);
        using var args = JsonDocument.Parse(command.ArgsJson!);
        Assert.Equal("index_object_stats", args.RootElement.GetProperty("collector_name").GetString());
        Assert.Equal("02:00", args.RootElement.GetProperty("run_at").GetString());

        /* The verb's plan is the executor's plan for that command, nothing else: one write path. */
        var viaExecutor = DarlingCommandExecutor.ResolvePlan(command);
        var viaVerb = Plan("index_object_stats", "02:00", 7);
        Assert.Equal(viaExecutor.Kind, viaVerb.Kind);
        Assert.Equal(viaExecutor.Sql, viaVerb.Sql);
        Assert.Equal(viaExecutor.Parameters, viaVerb.Parameters);
    }

    [Fact]
    public void AFleetTime_UpsertsTheFleetRow_TouchingOnlyTheRunTime_AndInsertingTheCodeDefaultEnabledState()
    {
        var plan = Plan("index_object_stats", "02:00", serverId: null);

        Assert.Equal(CommandKind.StoreWrite, plan.Kind);
        Assert.Contains("INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, run_at_minute) VALUES (NULL, $1, $2, $3)", plan.Sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE SET run_at_minute = EXCLUDED.run_at_minute", plan.Sql, StringComparison.Ordinal);
        Assert.Equal(3, plan.Parameters!.Length);
        Assert.Equal("index_object_stats", plan.Parameters[0]);
        Assert.Equal(true, plan.Parameters[1]);
        Assert.Equal((short)120, plan.Parameters[2]);
        Assert.Equal("collector run time set (fleet-wide)", plan.SuccessStatus);

        /* A collector that ships OFF is inserted OFF: the verb must not turn it on as a side effect. */
        var optIn = Plan("long_query_completions", "02:00", serverId: null);
        Assert.Equal(false, optIn.Parameters![1]);
    }

    [Fact]
    public void AServerTime_UpsertsTheServersRow_TouchingOnlyTheRunTime_AndInsertingTheEffectiveEnabledState()
    {
        var plan = Plan("index_object_stats", "04:30", serverId: 7);

        Assert.Equal(CommandKind.StoreWrite, plan.Kind);
        Assert.Contains("INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, run_at_minute) VALUES ($1, $2, COALESCE((SELECT f.enabled FROM config.config_collector_schedules f WHERE f.server_id IS NULL AND f.collector_name = $2), $3), $4)", plan.Sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE SET run_at_minute = EXCLUDED.run_at_minute", plan.Sql, StringComparison.Ordinal);
        Assert.Equal(new object?[] { 7, "index_object_stats", true, (short)270 }, plan.Parameters);
        Assert.Equal("collector run time set", plan.SuccessStatus);
    }

    [Fact]
    public void NoneOnAServer_WritesMinusOne_AndOnTheFleet_ClearsTheColumn()
    {
        var server = Plan("index_object_stats", "none", serverId: 7);
        Assert.Contains("ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL", server.Sql, StringComparison.Ordinal);
        Assert.Equal(new object?[] { 7, "index_object_stats", true, (short)-1 }, server.Parameters);

        var fleet = Plan("index_object_stats", "NONE", serverId: null);
        Assert.Equal("UPDATE config.config_collector_schedules SET run_at_minute = NULL WHERE server_id IS NULL AND collector_name = $1", fleet.Sql);
        Assert.Equal(new object?[] { "index_object_stats" }, fleet.Parameters);
        Assert.Equal("collector run time cleared (fleet-wide)", fleet.SuccessStatus);
    }

    [Fact]
    public void Default_ClearsARowToNull_WithoutCreatingOne()
    {
        var server = Plan("index_object_stats", "default", serverId: 7);
        Assert.Equal("UPDATE config.config_collector_schedules SET run_at_minute = NULL WHERE server_id = $1 AND collector_name = $2", server.Sql);
        Assert.Equal(new object?[] { 7, "index_object_stats" }, server.Parameters);
        Assert.Equal("collector run time cleared", server.SuccessStatus);

        var fleet = Plan("index_object_stats", "Default", serverId: null);
        Assert.Equal("UPDATE config.config_collector_schedules SET run_at_minute = NULL WHERE server_id IS NULL AND collector_name = $1", fleet.Sql);
    }

    [Theory]
    [InlineData("24:00")]
    [InlineData("2:5")]
    [InlineData("noon")]
    [InlineData("")]
    public void ABadTime_IsRefusedByThePlan_WithTheSharedText(string runAt)
    {
        var plan = Plan("index_object_stats", runAt, serverId: null);

        Assert.Equal(CommandKind.Fail, plan.Kind);
        Assert.Equal(CollectorRunTime.InvalidRunAtMessage, plan.FailReason);
    }

    [Fact]
    public void AnUnknownCollector_AndAMissingArgument_AreRefusedByThePlan()
    {
        Assert.Equal(CommandKind.Fail, Plan("not_a_collector", "02:00", serverId: null).Kind);
        Assert.Contains("unknown collector", Plan("not_a_collector", "02:00", serverId: null).FailReason, StringComparison.Ordinal);

        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(
            new ClaimedCommand(0, "set_collector_run_at", null, "{\"collector_name\":\"index_object_stats\"}", "cli")).Kind);
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(
            new ClaimedCommand(0, "set_collector_run_at", null, "{\"run_at\":\"02:00\"}", "cli")).Kind);
    }

    [Fact]
    public void TheReadBack_PrintsTheRunTime_OnlyForARowThatCarriesOne()
    {
        var rows = new List<DarlingCliCommands.CollectorScheduleReadbackRow>
        {
            new(null, "index_object_stats", null, null, true, null, null, 120),
            new(7, "index_object_stats", null, null, true, null, "sql01", -1),
            new(8, "index_object_stats", 2880, 45, false, null, "sql02", 1439),
            new(9, "index_object_stats", null, null, true, null, "sql03", null),
        };

        var lines = DarlingCliCommands.FormatCollectorScheduleRows("index_object_stats", rows);

        Assert.Contains("  fleet-wide: enabled=true  frequency=(default)  retention=(default)  run_at=02:00 server time", lines);
        Assert.Contains("  sql01 (server_id 7): enabled=true  frequency=(default)  retention=(default)  run_at=none (no fixed time on this server)", lines);
        Assert.Contains("  sql02 (server_id 8): enabled=false  frequency=every 2880 min  retention=45 days  run_at=23:59 server time", lines);
        /* A row with no run time prints exactly what the toggle verbs have always printed. */
        Assert.Contains("  sql03 (server_id 9): enabled=true  frequency=(default)  retention=(default)", lines);
    }

    /// <summary>The read-back for a store that has no run-time column yet is the same read with the column swapped for a
    /// NULL smallint in the same position, so one reader serves both stores and a row prints no <c>run_at=</c>.</summary>
    [Fact]
    public void TheReadBackForAStoreWithoutTheColumn_IsTheSameReadWithANullRunTimeInPlace()
    {
        var full = DarlingCliCommands.CollectorScheduleReadbackSql;
        var older = DarlingCliCommands.CollectorScheduleReadbackWithoutRunAtSql;

        Assert.Contains("cs.run_at_minute", full, StringComparison.Ordinal);
        Assert.DoesNotContain("cs.run_at_minute", older, StringComparison.Ordinal);
        Assert.Equal(full.Replace("cs.run_at_minute", "NULL::smallint AS run_at_minute", StringComparison.Ordinal), older);
        Assert.DoesNotContain("INSERT", older, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("UPDATE", older, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DELETE", older, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// <c>--set-collector-run-at</c> run end to end (#4938) through <see cref="DarlingCliCommands.SetCollectorRunAtAsync"/>
/// with a bring-your-own darling.json pointed at a scratch store: parse, plan, connect, check, write, read back,
/// print. It asserts the rows the service will resolve, that the verb touches only the run time, and that a bad time or
/// a collector that runs more often than daily is refused with the shared text and a non-zero exit code.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class CollectorRunTimeCliVerbLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the run-time verb end to end (each fact mints its own scratch database).";

    private const string Daily = "index_object_stats";
    private const int Sql01 = 4101;
    private const int Sql02 = 4102;

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required string ConfigPath { get; init; }
        public required string Directory { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
            try { System.IO.Directory.Delete(Directory, recursive: true); } catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
        }
    }

    private static async Task<Rig> OpenAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var directory = Path.Combine(Path.GetTempPath(), "darling-runat-" + Guid.NewGuid().ToString("N"));
        System.IO.Directory.CreateDirectory(directory);
        var configPath = Path.Combine(directory, "darling.json");
        File.WriteAllText(configPath, $$"""
            {
              "postgres": {
                "managed": false,
                "connectionString": {{JsonSerializer.Serialize(scratch.ConnectionString)}}
              },
              "servers": []
            }
            """);

        foreach (var (id, name) in new[] { (Sql01, "sql01"), (Sql02, "sql02") })
        {
            await ExecAsync(connection,
                "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, created_date, modified_date) " +
                $"VALUES ({id}, '{name}', '{name}', TRUE, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')", ct);
        }

        return new Rig { Scratch = scratch, Connection = connection, ConfigPath = configPath, Directory = directory };
    }

    private static async Task<(int Exit, string Output, string Error)> RunAsync(Rig rig, CancellationToken ct, params string[] args)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingCliCommands.SetCollectorRunAtAsync(args.Concat(new[] { "--config", rig.ConfigPath }).ToArray(), output, error, ct);
        return (exit, output.ToString(), error.ToString());
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static string Scope(int? serverId) => serverId is int id ? $"server_id = {id}" : "server_id IS NULL";

    private static async Task<long> RowsAsync(NpgsqlConnection connection, int? serverId, string collector, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection,
            $"SELECT COUNT(*) FROM config.config_collector_schedules WHERE {Scope(serverId)} AND collector_name = '{collector}'", ct));

    private static async Task<int?> RunAtAsync(NpgsqlConnection connection, int? serverId, string collector, CancellationToken ct)
    {
        var value = await ScalarAsync(connection,
            $"SELECT run_at_minute FROM config.config_collector_schedules WHERE {Scope(serverId)} AND collector_name = '{collector}'", ct);
        return value is null or DBNull ? null : Convert.ToInt32(value);
    }

    [Fact]
    public async Task TheFleetRow_IsWritten_TheNameIsCanonicalised_AndTheRowIsReadBack()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            var (exit, output, error) = await RunAsync(rig, ct, "Index_Object_Stats", "02:00");

            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(string.Empty, error);
            Assert.Contains("[SET] index_object_stats", output, StringComparison.Ordinal);
            Assert.Contains("fleet-wide: enabled=true", output, StringComparison.Ordinal);
            Assert.Contains("run_at=02:00 server time", output, StringComparison.Ordinal);
            Assert.Contains("no restart is needed", output, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(120, await RunAtAsync(rig.Connection, null, Daily, ct));
            /* The dictionary spelling, so it is the row the viewer's editor writes too. */
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(rig.Connection,
                "SELECT COUNT(*) FROM config.config_collector_schedules WHERE server_id IS NULL AND lower(collector_name) = 'index_object_stats'", ct)));

            /* A second time replaces the first. */
            (exit, _, error) = await RunAsync(rig, ct, Daily, "03:15");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(195, await RunAtAsync(rig.Connection, null, Daily, ct));
            Assert.Equal(1L, await RowsAsync(rig.Connection, null, Daily, ct));

            /* `none` on the fleet row clears it to NULL: the fleet has no time to stop. */
            (exit, output, error) = await RunAsync(rig, ct, Daily, "none");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Contains("[CLEARED] index_object_stats", output, StringComparison.Ordinal);
            Assert.Null(await RunAtAsync(rig.Connection, null, Daily, ct));
            Assert.Equal(1L, await RowsAsync(rig.Connection, null, Daily, ct));

            /* `default` on the fleet row does the same, and with no row there is nothing to create. */
            (exit, _, error) = await RunAsync(rig, ct, "pg_column_stats", "default");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(0L, await RowsAsync(rig.Connection, null, "pg_column_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AServerRow_IsWritten_KeepsItsOtherColumns_AndANewOneTakesTheEffectiveEnabledState()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* sql01 already has a row with its own cadence, retention and an OFF flag: the verb writes one column. */
            await ExecAsync(rig.Connection,
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) " +
                $"VALUES ({Sql01}, '{Daily}', 2880, 45, FALSE)", ct);

            var (exit, output, error) = await RunAsync(rig, ct, Daily, "04:30", "--server", "sql01");

            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Contains("[SET] index_object_stats - sql01", output.Replace('—', '-'), StringComparison.Ordinal);
            Assert.Contains("sql01 (server_id 4101): enabled=false  frequency=every 2880 min  retention=45 days  run_at=04:30 server time", output, StringComparison.Ordinal);
            await using (var read = new NpgsqlCommand(
                $"SELECT run_at_minute, frequency_minutes, retention_days, enabled FROM config.config_collector_schedules WHERE server_id = {Sql01} AND collector_name = '{Daily}'", rig.Connection))
            await using (var reader = await read.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(270, Convert.ToInt32(reader.GetValue(0)));
                Assert.Equal(2880, reader.GetInt32(1));
                Assert.Equal(45, reader.GetInt32(2));
                Assert.False(reader.GetBoolean(3));
                Assert.False(await reader.ReadAsync(ct), "exactly one row for the server");
            }

            /* sql02 has no row, and the fleet row says OFF: the row the verb creates must say OFF too. A column default
               of TRUE would switch the collector back on for that one server. */
            await ExecAsync(rig.Connection,
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES (NULL, '{Daily}', FALSE)", ct);
            (exit, _, error) = await RunAsync(rig, ct, Daily, "01:00", "--server", "sql02");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(60, await RunAtAsync(rig.Connection, Sql02, Daily, ct));
            Assert.Equal(false, await ScalarAsync(rig.Connection,
                $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {Sql02} AND collector_name = '{Daily}'", ct));

            /* And with no fleet row, the code default (ON for this collector). */
            (exit, _, error) = await RunAsync(rig, ct, "pg_column_stats", "05:00", "--server", "sql02");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(true, await ScalarAsync(rig.Connection,
                $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {Sql02} AND collector_name = 'pg_column_stats'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task NoneOnAServer_WritesMinusOneOverAFleetTime_AndDefaultClearsTheServerRowToNull()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            var (exit, _, error) = await RunAsync(rig, ct, Daily, "02:00");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");

            /* none on a server: -1, the fleet's time stays and the service resolves no time for that server only. */
            string output;
            (exit, output, error) = await RunAsync(rig, ct, Daily, "none", "--server", "sql01");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(-1, await RunAtAsync(rig.Connection, Sql01, Daily, ct));
            Assert.Equal(120, await RunAtAsync(rig.Connection, null, Daily, ct));
            Assert.Contains("run_at=none (no fixed time on this server)", output, StringComparison.Ordinal);
            Assert.Contains("run_at=02:00 server time", output, StringComparison.Ordinal);

            var overrides = new List<ScheduleOverride>
            {
                new(null, Daily, null, null, true, null, 120),
                new(Sql01, Daily, null, null, true, null, -1),
            };
            Assert.Null(StoreConfigProvider.ResolveSchedule(Daily, Sql01, overrides).RunAtMinute);
            Assert.Equal(120, StoreConfigProvider.ResolveSchedule(Daily, Sql02, overrides).RunAtMinute);

            /* default on the server's row: NULL, so it falls through to the fleet's time again. The row stays. */
            (exit, output, error) = await RunAsync(rig, ct, Daily, "default", "--server", "sql01");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Contains("[CLEARED] index_object_stats", output, StringComparison.Ordinal);
            Assert.Null(await RunAtAsync(rig.Connection, Sql01, Daily, ct));
            Assert.Equal(1L, await RowsAsync(rig.Connection, Sql01, Daily, ct));

            /* default on a server with no row creates none. */
            (exit, _, error) = await RunAsync(rig, ct, Daily, "default", "--server", "sql02");
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(0L, await RowsAsync(rig.Connection, Sql02, Daily, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ABadTime_AndAnHourlyCollector_AreRefusedWithTheSharedText_ANonZeroExit_AndNothingWritten()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* A time that is not a 24-hour HH:MM time. */
            foreach (var bad in new[] { "25:00", "2:5", "noon" })
            {
                var (exit, output, error) = await RunAsync(rig, ct, Daily, bad);
                Assert.Equal(1, exit);
                Assert.Contains("Run at must be a 24-hour time from 00:00 to 23:59, such as 02:00.", error, StringComparison.Ordinal);
                Assert.Contains("HH:MM|none|default", output, StringComparison.Ordinal);
            }

            /* A collector that runs more often than once a day. */
            var (hourlyExit, _, hourlyError) = await RunAsync(rig, ct, "wait_stats", "02:00");
            Assert.Equal(1, hourlyExit);
            Assert.Contains(CollectorRunTime.IntervalRefusalMessage("wait_stats", 1), hourlyError, StringComparison.Ordinal);

            /* The interval judged is the target's own: a daily collector that the server runs every hour is refused for
               that server, and the fleet-wide time on the same collector is still fine. */
            await ExecAsync(rig.Connection,
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES ({Sql01}, '{Daily}', 60)", ct);
            var (serverExit, _, serverError) = await RunAsync(rig, ct, Daily, "02:00", "--server", "sql01");
            Assert.Equal(1, serverExit);
            Assert.Contains(CollectorRunTime.IntervalRefusalMessage(Daily, 60), serverError, StringComparison.Ordinal);
            var (fleetExit, _, fleetError) = await RunAsync(rig, ct, Daily, "02:00");
            Assert.True(fleetExit == 0, $"exit {fleetExit}; stderr: {fleetError}");

            /* And a fleet row that makes the collector hourly is refused for the fleet. */
            await ExecAsync(rig.Connection,
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES (NULL, 'pg_column_stats', 90)", ct);
            var (fleetHourlyExit, _, fleetHourlyError) = await RunAsync(rig, ct, "pg_column_stats", "02:00");
            Assert.Equal(1, fleetHourlyExit);
            Assert.Contains(CollectorRunTime.IntervalRefusalMessage("pg_column_stats", 90), fleetHourlyError, StringComparison.Ordinal);

            /* Nothing was written by any refusal: only the one accepted fleet time exists. */
            Assert.Equal(0L, await RowsAsync(rig.Connection, null, "wait_stats", ct));
            Assert.Null(await RunAtAsync(rig.Connection, Sql01, Daily, ct));
            Assert.Null(await RunAtAsync(rig.Connection, null, "pg_column_stats", ct));
            Assert.Equal(120, await RunAtAsync(rig.Connection, null, Daily, ct));

            /* Clearing is the remedy the refusal names, so it is never refused: none and default on an hourly collector. */
            var (noneExit, _, noneError) = await RunAsync(rig, ct, "wait_stats", "none");
            Assert.True(noneExit == 0, $"exit {noneExit}; stderr: {noneError}");
            var (defaultExit, _, defaultError) = await RunAsync(rig, ct, Daily, "default", "--server", "sql01");
            Assert.True(defaultExit == 0, $"exit {defaultExit}; stderr: {defaultError}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AnUnknownServerOrCollector_IsRefused_AndNothingIsWritten()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            var (exit, _, error) = await RunAsync(rig, ct, Daily, "02:00", "--server", "no-such-server");
            Assert.Equal(1, exit);
            Assert.Contains("Could not resolve server", error, StringComparison.Ordinal);
            Assert.Contains("Nothing was changed.", error, StringComparison.Ordinal);

            (exit, _, error) = await RunAsync(rig, ct, "not_a_collector", "02:00");
            Assert.Equal(1, exit);
            Assert.Contains("unknown collector", error, StringComparison.Ordinal);

            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(rig.Connection,
                "SELECT COUNT(*) FROM config.config_collector_schedules", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The toggle verbs share the read-back, which now selects the run time. A store the service has not migrated to
    /// the run-time column yet must still read back after the write (the CLI works against a store older than its binary), and
    /// the rows print without a <c>run_at=</c>: the column is gone here, with its check, as it is on such a store.</summary>
    [Fact]
    public async Task TheToggleVerbs_ReadBackOnAStoreWithoutTheRunTimeColumn_AndPrintNoRunAt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(rig.Connection, "ALTER TABLE config.config_collector_schedules DROP COLUMN run_at_minute", ct);
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(rig.Connection,
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_schedules' AND column_name = 'run_at_minute'", ct)));
            await ExecAsync(rig.Connection,
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES ({Sql01}, '{Daily}', FALSE)", ct);

            /* --enable-collector, fleet-wide: the write commits and the read-back prints the row. */
            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: true, new[] { "long_query_completions", "--config", rig.ConfigPath }, output, error, ct);
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(string.Empty, error.ToString());
            Assert.Contains("[ENABLED] long_query_completions", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("  fleet-wide: enabled=true  frequency=(default)  retention=(default)", output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("run_at=", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("no restart is needed", output.ToString(), StringComparison.OrdinalIgnoreCase);

            /* --disable-collector for one server's row, beside a fleet row the same collector does not have. */
            output = new StringWriter();
            error = new StringWriter();
            exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: false, new[] { Daily, "--server", "sql01", "--config", rig.ConfigPath }, output, error, ct);
            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(string.Empty, error.ToString());
            Assert.Contains($"  sql01 (server_id {Sql01}): enabled=false  frequency=(default)  retention=(default)", output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("run_at=", output.ToString(), StringComparison.Ordinal);

            Assert.Equal(true, await ScalarAsync(rig.Connection,
                "SELECT enabled FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'long_query_completions'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
