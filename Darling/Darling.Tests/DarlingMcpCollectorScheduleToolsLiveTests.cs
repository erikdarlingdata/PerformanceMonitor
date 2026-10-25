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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// The collector schedule MCP tools (#6227) called through their tool methods against a real store. A disable and an enable are
/// checked against what the runner schedules by (<see cref="StoreConfigProvider.LoadViewAsync"/> and
/// <see cref="StoreConfigProvider.ResolveSchedule"/>), a frequency, retention and run time are set, read back and cleared, each answer's
/// <c>undo</c> is called to put the previous state back, and the refusals change nothing. The write gate is pinned in
/// <see cref="DarlingMcpCollectorScheduleGateTests"/>.
/// </summary>
public sealed class DarlingMcpCollectorScheduleToolsLiveTests
{
    private const int ServerA = 7401;
    private const int ServerB = 7402;
    private const string Daily = "index_object_stats";
    private const string Delta = "wait_stats";
    private const string ShipsOff = "long_query_completions";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the collector schedule tool pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var owner = new NpgsqlConnection(connectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        await ExecAsync(owner, $"INSERT INTO servers (server_id, server_name) VALUES ({ServerA}, 'sched-alpha'), ({ServerB}, 'sched-beta')", ct);
        await using var source = NpgsqlDataSource.Create(connectionString);

        var bodySucceeded = false;
        try
        {
            await body(source, owner, ct);
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

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    /// <summary>What the runner's schedule read resolves for one collector on one server: the store read as the worker reads it, then
    /// <see cref="StoreConfigProvider.ResolveSchedule"/>.</summary>
    private static async Task<EffectiveSchedule> RunnerSeesAsync(NpgsqlDataSource source, string collector, int serverId, CancellationToken ct)
    {
        var view = await new StoreConfigProvider(source).LoadViewAsync(new DarlingConfig(), ct);
        Assert.NotNull(view);
        return StoreConfigProvider.ResolveSchedule(collector, serverId, view.ScheduleOverrides);
    }

    /// <summary>Calls the tool an answer's <c>undo</c> names, with the arguments it gives: the exact call that puts the state back.</summary>
    private static Task<string> CallUndoAsync(NpgsqlDataSource source, JsonObject answer, CancellationToken ct)
    {
        var undo = (JsonObject)answer["undo"]!;
        var args = (JsonObject)undo["arguments"]!;
        var collector = (string)args["collector_name"]!;
        var server = (string?)args["server_name"];
        return (string)undo["tool"]! switch
        {
            "enable_collector" => DarlingMcpCollectorScheduleTools.EnableCollector(source, collector, server, cancellationToken: ct),
            "disable_collector" => DarlingMcpCollectorScheduleTools.DisableCollector(source, collector, server, cancellationToken: ct),
            "set_collector_frequency" => DarlingMcpCollectorScheduleTools.SetCollectorFrequency(
                source, collector, (int?)args["frequency_minutes"], (bool?)args["clear"] ?? false, server, cancellationToken: ct),
            "set_collector_retention" => DarlingMcpCollectorScheduleTools.SetCollectorRetention(
                source, collector, (int?)args["retention_days"], (bool?)args["clear"] ?? false, server, cancellationToken: ct),
            "set_collector_run_at" => DarlingMcpCollectorScheduleTools.SetCollectorRunAt(source, collector, (string)args["run_at"]!, server, cancellationToken: ct),
            var other => throw new InvalidOperationException($"unexpected undo tool {other}"),
        };
    }

    [Fact]
    public async Task ADisable_MakesTheRunnersScheduleSkipTheCollector_AnEnable_BringsItBack_AndTheUndoRestoresIt()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            Assert.True((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);

            var disabled = Parse(await DarlingMcpCollectorScheduleTools.DisableCollector(source, "WAIT_STATS", null, cancellationToken: ct));
            Assert.Equal("changed", (string?)disabled["status"]);
            Assert.Equal(Delta, (string?)disabled["collector"]);
            Assert.Equal("fleet-wide", (string?)disabled["scope"]);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerB, ct)).Enabled);
            Assert.Equal("enable_collector", (string?)disabled["undo"]!["tool"]);

            /* A server's own row wins over the fleet row, and the fleet-wide answer names no server for it. */
            var serverOn = Parse(await DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, "sched-alpha", cancellationToken: ct));
            Assert.Equal("changed", (string?)serverOn["status"]);
            Assert.True((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerB, ct)).Enabled);

            /* The undo of the server enable puts the server back to what it resolved to before: off. */
            var undone = Parse(await CallUndoAsync(source, serverOn, ct));
            Assert.Equal("changed", (string?)undone["status"]);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);

            var enabled = Parse(await DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, null, cancellationToken: ct));
            Assert.True((await RunnerSeesAsync(source, Delta, ServerB, ct)).Enabled);

            /* The undo of the fleet enable is a disable, and the one of the fleet disable put it back on. */
            Assert.Equal("disable_collector", (string?)enabled["undo"]!["tool"]);
            await CallUndoAsync(source, enabled, ct);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerB, ct)).Enabled);
            await CallUndoAsync(source, disabled, ct);
            Assert.True((await RunnerSeesAsync(source, Delta, ServerB, ct)).Enabled);

            /* The write touched the enabled column and nothing else. */
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules WHERE frequency_minutes IS NOT NULL OR retention_days IS NOT NULL", ct));
        });
    }

    [Fact]
    public async Task FrequencyAndRetention_SetClearAndRoundTrip_AreReadBackBySchedules_AndNeverFlipTheEnabledFlag()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            /* A collector that ships OFF stays off when a frequency or retention is set on it, fleet-wide or per server. */
            var freq = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, ShipsOff, 7, false, null, cancellationToken: ct));
            Assert.Equal("changed", (string?)freq["status"]);
            Assert.Null((int?)freq["previous"]!["frequency_minutes"]);
            Assert.Equal(7, (int?)freq["current"]!["frequency_minutes"]);
            /* A retention for one server is refused (the purge reads the fleet-wide value only), so the row an older build stored is seeded directly. */
            await ExecAsync(owner, $"INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, retention_days) VALUES ({ServerB}, '{ShipsOff}', NULL, 11)", ct);

            var runnerFleet = await RunnerSeesAsync(source, ShipsOff, ServerA, ct);
            Assert.False(runnerFleet.Enabled);
            Assert.Equal(7, runnerFleet.FrequencyMinutes);
            var runnerServer = await RunnerSeesAsync(source, ShipsOff, ServerB, ct);
            Assert.False(runnerServer.Enabled);
            Assert.Equal(11, runnerServer.RetentionDays);
            Assert.Equal(7, runnerServer.FrequencyMinutes);

            /* A server row created under a fleet row that is OFF is created OFF too. */
            await DarlingMcpCollectorScheduleTools.DisableCollector(source, Delta, null, cancellationToken: ct);
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Delta, 5, false, "sched-alpha", cancellationToken: ct);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);

            /* The read lists what is stored, fleet-wide and per server, with the code default beside it. */
            var read = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(source, ShipsOff, null, ct));
            Assert.Equal("ok", (string?)read["status"]);
            var rows = ((JsonArray)read["overrides"]!).Cast<JsonObject>().ToList();
            Assert.Equal(2, rows.Count);
            var fleet = rows.Single(r => (string?)r["scope"] == "fleet-wide");
            Assert.Equal(7, (int?)fleet["frequency_minutes"]);
            Assert.Equal("inherited", (string?)fleet["enabled"]);
            Assert.Equal(11, (int?)rows.Single(r => (string?)r["scope"] == "server")["retention_days"]);
            Assert.Equal(false, (bool?)read["code_default"]!["enabled"]);

            /* A server filter keeps the fleet rows and that server's rows only. */
            var alpha = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(source, ShipsOff, "sched-alpha", ct));
            Assert.Single((JsonArray)alpha["overrides"]!);

            /* The undo of each answer puts the previous value back; a previous "none" is a clear. */
            await DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, ShipsOff, null, true, "sched-beta", cancellationToken: ct);
            await CallUndoAsync(source, freq, ct);
            var afterUndo = await RunnerSeesAsync(source, ShipsOff, ServerB, ct);
            Assert.Equal(CollectorScheduleDefaults.All[ShipsOff].FrequencyMinutes, afterUndo.FrequencyMinutes);
            Assert.Equal(CollectorScheduleDefaults.All[ShipsOff].RetentionDays, afterUndo.RetentionDays);
            Assert.False(afterUndo.Enabled);

            /* Clear is its own call, and a clear with nothing stored writes nothing. */
            var cleared = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Delta, null, true, "sched-beta", cancellationToken: ct));
            Assert.Equal("unchanged", (string?)cleared["status"]);
            Assert.Equal(0, await ScalarAsync(owner, $"SELECT count(*) FROM config.config_collector_schedules WHERE server_id = {ServerB} AND collector_name = '{Delta}'", ct));
        });
    }

    [Fact]
    public async Task FleetDisable_ReachesAServerWithAToolSetFrequency_AndInheritReturnsAServerToTheFleetValue()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            /* A frequency set on a server stores no enabled flag, so a later fleet-wide disable still reaches that server (V206). */
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Delta, 5, false, "sched-alpha", cancellationToken: ct);
            Assert.Equal(1, await ScalarAsync(owner, $"SELECT count(*) FROM config.config_collector_schedules WHERE server_id = {ServerA} AND enabled IS NULL", ct));
            await DarlingMcpCollectorScheduleTools.DisableCollector(source, Delta, null, cancellationToken: ct);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);
            Assert.Equal(5, (await RunnerSeesAsync(source, Delta, ServerA, ct)).FrequencyMinutes);

            /* An explicit server value wins over the fleet; inherit=true puts the server back on the fleet value, and its undo restores the explicit one. */
            await DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, "sched-alpha", cancellationToken: ct);
            Assert.True((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);
            var inherit = Parse(await DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, "sched-alpha", inherit: true, cancellationToken: ct));
            Assert.Equal("changed", (string?)inherit["status"]);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);
            Assert.Equal("enable_collector", (string?)inherit["undo"]!["tool"]);
            await CallUndoAsync(source, inherit, ct);
            Assert.True((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);

            var read = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(source, Delta, "sched-beta", ct));
            Assert.Contains(((JsonArray)read["overrides"]!).Cast<JsonObject>(), r => (string?)r["scope"] == "fleet-wide");
        });
    }

    [Fact]
    public async Task Undo_NamesTheServerByItsStorageName_NotByItsDisplayLabel()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            await ExecAsync(owner, $"UPDATE servers SET display_name = 'Alpha Box' WHERE server_id = {ServerA}", ct);
            var disabled = Parse(await DarlingMcpCollectorScheduleTools.DisableCollector(source, Delta, "Alpha Box", cancellationToken: ct));
            Assert.Equal("sched-alpha", (string?)disabled["undo"]!["arguments"]!["server_name"]);
            Assert.True((bool?)disabled["undo"]!["arguments"]!["inherit"]);
            Assert.False((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);
            await CallUndoAsync(source, disabled, ct);
            Assert.True((await RunnerSeesAsync(source, Delta, ServerA, ct)).Enabled);
        });
    }

    [Fact]
    public async Task TheNotesNameOtherServersByTheStorageNameTheResolverAccepts_NotByTheDisplayLabel()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            await ExecAsync(owner, $"UPDATE servers SET display_name = 'Alpha Box' WHERE server_id = {ServerA}", ct);
            await DarlingMcpCollectorScheduleTools.EnableCollector(source, Delta, "sched-alpha", cancellationToken: ct);
            await ExecAsync(owner, $"INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES ({ServerA}, '{Daily}', 30)", ct);

            var enabled = Parse(await DarlingMcpCollectorScheduleTools.DisableCollector(source, Delta, null, cancellationToken: ct));
            var retention = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, Daily, 90, false, null, cancellationToken: ct));
            foreach (var answer in new[] { enabled, retention })
            {
                var note = Assert.Single(((JsonArray)answer["notes"]!).Select(n => (string?)n), n => n!.StartsWith("These servers have their own", StringComparison.Ordinal));
                Assert.Contains("sched-alpha", note, StringComparison.Ordinal);
                Assert.DoesNotContain("Alpha Box", note, StringComparison.Ordinal);
            }

            /* The name the note gave is one the resolver takes: the follow-up call the note describes lands. */
            var back = Parse(await DarlingMcpCollectorScheduleTools.DisableCollector(source, Delta, "sched-alpha", inherit: true, cancellationToken: ct));
            Assert.Equal("changed", (string?)back["status"]);
        });
    }

    [Fact]
    public async Task WhenThePreviousValueIsAboveTodaysCap_TheUndoClearsTheOverride_AndTheNoteSaysTheOldValueCannotBePutBack()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            /* A row from before the limits, or written straight into the store: the checks are dropped on the scratch store to seed it. */
            var names = new System.Collections.Generic.List<string>();
            await using (var list = new NpgsqlCommand(
                "SELECT conname FROM pg_constraint WHERE conrelid = 'config.config_collector_schedules'::regclass AND contype = 'c'", owner))
            await using (var reader = await list.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    names.Add(reader.GetString(0));
                }
            }

            foreach (var name in names)
            {
                await ExecAsync(owner, $"ALTER TABLE config.config_collector_schedules DROP CONSTRAINT \"{name}\"", ct);
            }

            await ExecAsync(owner,
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days) VALUES ({ServerA}, '{Daily}', 600000, 5000)", ct);

            var frequency = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Daily, 60, false, "sched-alpha", cancellationToken: ct));
            Assert.Equal("changed", (string?)frequency["status"]);
            var frequencyUndo = (JsonObject)frequency["undo"]!["arguments"]!;
            Assert.True((bool?)frequencyUndo["clear"]);
            Assert.False(frequencyUndo.ContainsKey("frequency_minutes"));
            Assert.Contains(((JsonArray)frequency["notes"]!).Select(n => (string?)n), n => n!.Contains("600000", StringComparison.Ordinal) && n.Contains("cannot be put back", StringComparison.Ordinal));
            var undone = Parse(await CallUndoAsync(source, frequency, ct));
            Assert.Equal("changed", (string?)undone["status"]);
            Assert.Equal(1, await ScalarAsync(owner, $"SELECT count(*) FROM config.config_collector_schedules WHERE server_id = {ServerA} AND collector_name = '{Daily}' AND frequency_minutes IS NULL", ct));

            /* A retention stored above today's limit is a fleet-wide one now: a per-server value is refused. */
            await ExecAsync(owner, $"INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES (NULL, '{Daily}', 5000)", ct);
            var retention = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, Daily, 30, false, null, cancellationToken: ct));
            var retentionUndo = (JsonObject)retention["undo"]!["arguments"]!;
            Assert.True((bool?)retentionUndo["clear"]);
            Assert.False(retentionUndo.ContainsKey("retention_days"));
            Assert.Contains(((JsonArray)retention["notes"]!).Select(n => (string?)n), n => n!.Contains("5000", StringComparison.Ordinal) && n.Contains("cannot be put back", StringComparison.Ordinal));
        });
    }

    /* The purge reads one retention per collector (the fleet-wide row), so a retention for one server would be stored and never used: the
       tool refuses it and writes nothing. A fleet-wide retention still sets, and a clear with a server still removes a value an older
       build stored there. */
    [Fact]
    public async Task Retention_ForOneServer_IsRefused_FleetWideStillSets_AndAServerClearRemovesAStoredValue()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var refused = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, Daily, 30, false, "sched-alpha", cancellationToken: ct));
            Assert.Equal("refused", (string?)refused["status"]);
            Assert.Equal(DarlingCliCommands.PerServerRetentionRefusalText, (string?)refused["message"]);
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules", ct));

            var fleet = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, Daily, 90, false, null, cancellationToken: ct));
            Assert.Equal("changed", (string?)fleet["status"]);
            Assert.Equal(90, await ScalarAsync(owner, $"SELECT retention_days FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = '{Daily}'", ct));

            await ExecAsync(owner, $"INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES ({ServerA}, '{Daily}', 30)", ct);
            var cleared = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, Daily, null, true, "sched-alpha", cancellationToken: ct));
            Assert.Equal("changed", (string?)cleared["status"]);
            Assert.Null(cleared["undo"]);
            Assert.Equal(0, await ScalarAsync(owner, $"SELECT count(*) FROM config.config_collector_schedules WHERE server_id = {ServerA} AND retention_days IS NOT NULL", ct));
        });
    }

    [Fact]
    public async Task RunAt_SetNoneAndClear_RoundTrip_AndTheReadShowsIt()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var fleet = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(source, Daily, "02:30", null, cancellationToken: ct));
            Assert.Equal("changed", (string?)fleet["status"]);
            Assert.Equal("default", (string?)fleet["undo"]!["arguments"]!["run_at"]);
            Assert.Equal(150, (await RunnerSeesAsync(source, Daily, ServerA, ct)).RunAtMinute);

            var server = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(source, Daily, "none", "sched-beta", cancellationToken: ct));
            Assert.Equal("changed", (string?)server["status"]);
            Assert.Null((await RunnerSeesAsync(source, Daily, ServerB, ct)).RunAtMinute);
            Assert.Equal(150, (await RunnerSeesAsync(source, Daily, ServerA, ct)).RunAtMinute);

            var read = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(source, Daily, null, ct));
            var rows = ((JsonArray)read["overrides"]!).Cast<JsonObject>().ToList();
            Assert.Equal("02:30", (string?)rows.Single(r => (string?)r["scope"] == "fleet-wide")["run_at"]);
            Assert.Equal("none", (string?)rows.Single(r => (string?)r["scope"] == "server")["run_at"]);

            /* A run time writes no schedule row: it cannot switch a collector on or off. */
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules", ct));

            /* The undo of the fleet set is the clear; the undo of the server set puts the default back (no row). */
            await CallUndoAsync(source, server, ct);
            Assert.Equal(150, (await RunnerSeesAsync(source, Daily, ServerB, ct)).RunAtMinute);
            await CallUndoAsync(source, fleet, ct);
            Assert.Null((await RunnerSeesAsync(source, Daily, ServerA, ct)).RunAtMinute);
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_run_times", ct));

            /* A second set to the same time says so. */
            await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(source, Daily, "04:00", null, cancellationToken: ct);
            var again = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(source, Daily, "04:00", null, cancellationToken: ct));
            Assert.Equal("unchanged", (string?)again["status"]);
        });
    }

    [Fact]
    public async Task AnUnknownCollector_AnOutOfBoundsValue_ABadRunAt_AndAnUnknownServer_AreRefusedInPlainWords_AndChangeNothing()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var unknown = Parse(await DarlingMcpCollectorScheduleTools.DisableCollector(source, "no_such_collector", null, cancellationToken: ct));
            var negative = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Daily, -1, false, null, cancellationToken: ct));
            var pastCap = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Delta, CollectorDeltaCalculator.MaxDeltaFrequencyMinutes + 1, false, null, cancellationToken: ct));
            var neither = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Daily, null, false, null, cancellationToken: ct));
            var both = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, Daily, 3, true, null, cancellationToken: ct));
            var zeroDays = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRetention(source, Daily, 0, false, null, cancellationToken: ct));
            var badTime = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(source, Daily, "25:99", null, cancellationToken: ct));
            var tooOften = Parse(await DarlingMcpCollectorScheduleTools.SetCollectorRunAt(source, Delta, "02:00", null, cancellationToken: ct));
            var noServer = Parse(await DarlingMcpCollectorScheduleTools.EnableCollector(source, Daily, "no-such-server", cancellationToken: ct));
            var readUnknown = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(source, "no_such_collector", null, ct));

            foreach (var refusal in new[] { unknown, negative, pastCap, neither, both, zeroDays, badTime, tooOften, noServer, readUnknown })
            {
                Assert.Equal("refused", (string?)refusal["status"]);
                Assert.False((bool?)refusal["changed"]);
                Assert.False(string.IsNullOrWhiteSpace((string?)refusal["message"]));
            }

            Assert.Contains("no_such_collector", (string?)unknown["message"], StringComparison.Ordinal);
            Assert.Contains("Nothing was changed", (string?)unknown["message"], StringComparison.Ordinal);
            Assert.Contains("negative", (string?)negative["message"], StringComparison.Ordinal);
            Assert.Contains("can't exceed", (string?)pastCap["message"], StringComparison.Ordinal);
            Assert.Contains("at least 1", (string?)zeroDays["message"], StringComparison.Ordinal);
            Assert.Contains("Could not resolve server", (string?)noServer["message"], StringComparison.Ordinal);
            Assert.Contains("sched-alpha", (string?)noServer["message"], StringComparison.Ordinal);
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_schedules", ct));
            Assert.Equal(0, await ScalarAsync(owner, "SELECT count(*) FROM config.config_collector_run_times", ct));
        });
    }

    [Fact]
    public async Task TheViewersScheduleSave_StillWritesFrequencyAndRetention_ThroughTheSharedStatements_AndTheToolsReadThem()
    {
        await WithStoreAsync(async (source, owner, ct) =>
        {
            var connectionString = new NpgsqlConnectionStringBuilder(owner.ConnectionString).ConnectionString;
            await using var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService(connectionString);
            await viewer.ReplaceFleetSchedulesAsync(
                new[] { new PerformanceMonitor.Darling.Viewer.CollectorScheduleRow(null, Daily, 720, 45, true, null) }, ct);

            var read = Parse(await DarlingMcpCollectorScheduleTools.GetCollectorSchedules(source, Daily, null, ct));
            var row = ((JsonArray)read["overrides"]!).Cast<JsonObject>().Single();
            Assert.Equal(720, (int?)row["frequency_minutes"]);
            Assert.Equal(45, (int?)row["retention_days"]);

            /* A tool write after the viewer's keeps the viewer's other column. */
            await DarlingMcpCollectorScheduleTools.SetCollectorFrequency(source, Daily, 60, false, null, cancellationToken: ct);
            var runner = await RunnerSeesAsync(source, Daily, ServerA, ct);
            Assert.Equal(60, runner.FrequencyMinutes);
            Assert.Equal(45, runner.RetentionDays);
        });
    }
}
