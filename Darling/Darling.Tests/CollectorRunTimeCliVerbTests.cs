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

    private const string FleetUpsert =
        "INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute) VALUES (NULL, $1, $2) " +
        "ON CONFLICT (collector_name) WHERE server_id IS NULL DO UPDATE SET run_at_minute = EXCLUDED.run_at_minute";

    private const string ServerUpsert =
        "INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute) VALUES ($1, $2, $3) " +
        "ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL DO UPDATE SET run_at_minute = EXCLUDED.run_at_minute";

    [Fact]
    public void ATimeOrNone_UpsertsOneRunTimeRow_AndTouchesNoScheduleRow()
    {
        var fleet = Plan("index_object_stats", "02:00", serverId: null);
        Assert.Equal(CommandKind.StoreWrite, fleet.Kind);
        Assert.Equal(FleetUpsert, fleet.Sql);
        Assert.Equal(new object?[] { "index_object_stats", (short)120 }, fleet.Parameters);
        Assert.Equal("collector run time set (fleet-wide)", fleet.SuccessStatus);

        var server = Plan("index_object_stats", "02:00", serverId: 7);
        Assert.Equal(ServerUpsert, server.Sql);
        Assert.Equal(new object?[] { 7, "index_object_stats", (short)120 }, server.Parameters);
        Assert.Equal("collector run time set", server.SuccessStatus);

        /* None on a server is -1 in the same upsert. */
        var none = Plan("index_object_stats", "none", serverId: 7);
        Assert.Equal(ServerUpsert, none.Sql);
        Assert.Equal(new object?[] { 7, "index_object_stats", (short)-1 }, none.Parameters);

        /* The run time has its own table: no plan names a schedule row or its enabled flag, so a collector that ships OFF
           is never switched on as a side effect, and the schedule rows are neither read nor written. */
        foreach (var plan in new[] { fleet, server, none, Plan("long_query_completions", "02:00", serverId: null) })
        {
            Assert.DoesNotContain("config_collector_schedules", plan.Sql, StringComparison.Ordinal);
            Assert.DoesNotContain("enabled", plan.Sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void NoneOnTheFleet_AndDefault_DeleteTheScopesRow_WithoutCreatingOne()
    {
        const string FleetDelete = "DELETE FROM config.config_collector_run_times WHERE server_id IS NULL AND collector_name = $1";
        const string ServerDelete = "DELETE FROM config.config_collector_run_times WHERE server_id = $1 AND collector_name = $2";

        /* The fleet has no time to stop, so none there is the same as default: the row goes. */
        foreach (var word in new[] { "none", "default", "Default", "NONE" })
        {
            var fleet = Plan("index_object_stats", word, serverId: null);
            Assert.Equal(FleetDelete, fleet.Sql);
            Assert.Equal(new object?[] { "index_object_stats" }, fleet.Parameters);
            Assert.Equal("collector run time cleared (fleet-wide)", fleet.SuccessStatus);
        }

        var server = Plan("index_object_stats", "default", serverId: 7);
        Assert.Equal(ServerDelete, server.Sql);
        Assert.Equal(new object?[] { 7, "index_object_stats" }, server.Parameters);
        Assert.Equal("collector run time cleared", server.SuccessStatus);
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
    public void TheReadBack_PrintsTheRunTimeRows_UnderTheirOwnHeading_AndNothingForACollectorWithNone()
    {
        var schedule = new[] { new DarlingCliCommands.CollectorScheduleReadbackRow(null, "index_object_stats", null, null, true, null, null) };

        /* No run times: the read-back is exactly the schedule read-back it always was. */
        var plain = DarlingCliCommands.FormatCollectorScheduleRows("index_object_stats", schedule);
        Assert.DoesNotContain(plain, l => l.Contains("run_at=", StringComparison.Ordinal) || l.Contains("Run times", StringComparison.Ordinal));
        Assert.Equal(plain, DarlingCliCommands.FormatCollectorScheduleRows(
            "index_object_stats", schedule, Array.Empty<DarlingCliCommands.CollectorRunTimeReadbackRow>()));

        var runTimes = new[]
        {
            new DarlingCliCommands.CollectorRunTimeReadbackRow(null, "index_object_stats", 120, null),
            new DarlingCliCommands.CollectorRunTimeReadbackRow(7, "index_object_stats", -1, "sql01"),
            new DarlingCliCommands.CollectorRunTimeReadbackRow(8, "index_object_stats", 195, null),
        };

        /* A run time needs no schedule row, so the run-time lines follow the "no override rows" line as well. */
        var lines = DarlingCliCommands.FormatCollectorScheduleRows("index_object_stats", Array.Empty<DarlingCliCommands.CollectorScheduleReadbackRow>(), runTimes);
        Assert.Contains(lines, l => l.Contains("(none", StringComparison.Ordinal));
        var from = lines.ToList().IndexOf("Run times in the store for index_object_stats:");
        Assert.True(from >= 0, "no run-time heading");
        Assert.Equal("  fleet-wide: run_at=02:00 server time", lines[from + 1]);
        Assert.Equal("  sql01 (server_id 7): run_at=none (no fixed time on this server)", lines[from + 2]);
        Assert.Equal("  server_id 8 (not in the servers registry): run_at=03:15 server time", lines[from + 3]);
    }

    [Fact]
    public void TheScheduleReadBack_NamesNoRunTime_AndTheRunTimeReadBackReadsItsOwnTable()
    {
        Assert.DoesNotContain("run_at_minute", DarlingCliCommands.CollectorScheduleReadbackSql, StringComparison.Ordinal);
        Assert.Contains("FROM config.config_collector_run_times rt", DarlingCliCommands.CollectorRunTimeReadbackSql, StringComparison.Ordinal);
        Assert.Contains("lower(rt.collector_name) = lower($1)", DarlingCliCommands.CollectorRunTimeReadbackSql, StringComparison.Ordinal);
        Assert.Contains("run_at_minute", DarlingCliCommands.CollectorRunTimeReadbackSql, StringComparison.Ordinal);
        foreach (var write in new[] { "INSERT", "UPDATE", "DELETE" })
        {
            Assert.DoesNotContain(write, DarlingCliCommands.CollectorRunTimeReadbackSql, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void TheMissingTableMessage_NamesTheTable_AndSaysToStartTheServiceOnce()
    {
        Assert.Equal(
            "The store has not been upgraded to the run-time table yet. Start the service once to migrate it, then run --set-collector-run-at again.",
            DarlingCliCommands.RunTimeTableMissingMessage("--set-collector-run-at"));
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

    /// <summary>Every stored run time as one text: <c>fleet/collector=minute</c> or <c>serverId/collector=minute</c>.</summary>
    private static async Task<string> RunTimesAsync(NpgsqlConnection connection, CancellationToken ct) =>
        (string)(await ScalarAsync(connection,
            "SELECT COALESCE(string_agg(COALESCE(server_id::text, 'fleet') || '/' || collector_name || '=' || run_at_minute, ',' ORDER BY server_id NULLS FIRST, collector_name), '') FROM config.config_collector_run_times", ct))!;

    private static async Task<long> ScheduleRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Convert.ToInt64(await ScalarAsync(connection, "SELECT COUNT(*) FROM config.config_collector_schedules", ct));

    [Fact]
    public async Task TheFleetRunTime_IsWrittenToItsTable_TheNameIsCanonicalised_AndTheRowsAreReadBack()
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
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(rig.Connection, ct));
            Assert.Equal(0L, await ScheduleRowsAsync(rig.Connection, ct));
            Assert.Contains("Run times in the store for index_object_stats:", output, StringComparison.Ordinal);
            Assert.Contains("fleet-wide: run_at=02:00 server time", output, StringComparison.Ordinal);

            /* A second time replaces the row; default and none clear it. */
            Assert.Equal(0, (await RunAsync(rig, ct, Daily, "03:30")).Exit);
            Assert.Equal($"fleet/{Daily}=210", await RunTimesAsync(rig.Connection, ct));
            Assert.Equal(0, (await RunAsync(rig, ct, Daily, "default")).Exit);
            Assert.Equal("", await RunTimesAsync(rig.Connection, ct));
            Assert.Equal(0, (await RunAsync(rig, ct, Daily, "none")).Exit);
            Assert.Equal("", await RunTimesAsync(rig.Connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AServerRunTime_IsItsOwnRow_NoneIsMinusOne_DefaultDeletesIt_AndTheServersScheduleRowIsUntouched()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(rig.Connection,
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES ({Sql01}, '{Daily}', 2880, 45, FALSE)", ct);
            const string Schedule = "SELECT frequency_minutes || '|' || retention_days || '|' || enabled FROM config.config_collector_schedules WHERE server_id = " + "4101";

            Assert.Equal(0, (await RunAsync(rig, ct, Daily, "02:00", "--server", "sql01")).Exit);
            Assert.Equal($"{Sql01}/{Daily}=120", await RunTimesAsync(rig.Connection, ct));
            Assert.Equal(0, (await RunAsync(rig, ct, Daily, "01:00")).Exit);
            Assert.Equal(0, (await RunAsync(rig, ct, Daily, "none", "--server", "sql01")).Exit);
            Assert.Equal($"fleet/{Daily}=60,{Sql01}/{Daily}=-1", await RunTimesAsync(rig.Connection, ct));

            Assert.Equal(0, (await RunAsync(rig, ct, Daily, "default", "--server", "sql01")).Exit);
            Assert.Equal($"fleet/{Daily}=60", await RunTimesAsync(rig.Connection, ct));

            /* The schedule row the server already had is exactly as it was: the verb never wrote it. */
            Assert.Equal("2880|45|false",(string)(await ScalarAsync(rig.Connection, Schedule, ct))!);
            Assert.Equal(1L, await ScheduleRowsAsync(rig.Connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ABadTimeAnHourlyCollectorAnUnknownNameOrServer_AreRefused_WithANonZeroExit_AndNothingWritten()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(rig.Connection,
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES (NULL, '{Daily}', 60)", ct);

            var bad = await RunAsync(rig, ct, Daily, "25:00");
            Assert.Equal(1, bad.Exit);
            Assert.Contains(CollectorRunTime.InvalidRunAtMessage, bad.Error, StringComparison.Ordinal);

            var hourly = await RunAsync(rig, ct, Daily, "02:00");
            Assert.Equal(1, hourly.Exit);
            Assert.Contains(CollectorRunTime.IntervalRefusalMessage(Daily, 60), hourly.Error, StringComparison.Ordinal);

            Assert.Equal(1, (await RunAsync(rig, ct, "no_such_collector", "02:00")).Exit);
            Assert.Equal(1, (await RunAsync(rig, ct, "server_properties", "02:00", "--server", "no_such_server")).Exit);
            Assert.Equal("", await RunTimesAsync(rig.Connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>A store the service has not migrated to the run-time table yet (below V160). The verb says so in plain words,
    /// exits 1 and writes nothing, for a time, none and default alike; and the toggle verbs' read-back reads such a store as having
    /// no run times, so it does not fail after a write that committed.</summary>
    [Theory]
    [InlineData("02:00", null)]
    [InlineData("none", "sql01")]
    [InlineData("default", null)]
    public async Task OnAStoreWithoutTheRunTimeTable_TheVerbRefusesWithAPlainMessage_ExitsOne_AndWritesNothing(string runAt, string? server)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(rig.Connection, "DROP TABLE config.config_collector_run_times", ct);
            var scheduleRowsBefore = await ScheduleRowsAsync(rig.Connection, ct);

            var args = server is null ? new[] { Daily, runAt } : new[] { Daily, runAt, "--server", server };
            var (exit, _, error) = await RunAsync(rig, ct, args);

            Assert.Equal(1, exit);
            Assert.Contains(DarlingCliCommands.RunTimeTableMissingMessage("--set-collector-run-at"), error, StringComparison.Ordinal);
            Assert.Contains("Nothing was changed.", error, StringComparison.Ordinal);
            Assert.DoesNotContain("relation", error, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(scheduleRowsBefore, await ScheduleRowsAsync(rig.Connection, ct));

            await using var dataSource = NpgsqlDataSource.Create(rig.Scratch.ConnectionString);
            Assert.Empty(await DarlingCliCommands.ReadCollectorRunTimeRowsAsync(dataSource, Daily, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
