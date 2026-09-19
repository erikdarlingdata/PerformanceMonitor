/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store half of the collector toggle verbs (#3752), against the shared <c>DARLING_TEST_PG</c> store: the
/// verb run END TO END through <see cref="DarlingCliCommands.ToggleCollectorAsync"/> with a bring-your-own
/// darling.json pointed at the test store — parse, plan, connect, write through the executor's static store-write,
/// read back, print — for the fleet scope and for a sentinel server, asserting the row the service will resolve
/// and that a frequency override already on the row survives the toggle (the plan touches only <c>enabled</c>).
///
/// <para>Its own file, by the shape the collection's hygiene guidance prescribes: the pure pins live in
/// <c>DarlingCliCommandsTests.cs</c> beside the other CLI verbs' pure pins, and the ONE live test sits here so
/// that applying <c>[Collection("live-postgres")]</c> does not pull that whole file — and its twenty-nine
/// file-teardown <c>finally</c> blocks — into the #1902 teardown ratchet. Serialized with the other live classes
/// because it writes rows the shared store's other tests read.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCollectorToggleVerbLivePostgresTests
{
    /// <summary>Distinctive sentinel — a real server_id is a storage-name hash, never this; distinct from
    /// <c>DarlingCommandExecutorTests</c>' sentinel so the two classes never clean each other's rows.</summary>
    private const int SentinelServerId = -375200;
    private const string SentinelServerName = "lane-3752-sentinel:xedb1";
    private const string SentinelDisplayName = "lane-3752 sentinel";

    [Fact]
    public async Task Toggle_FleetAndServer_WritesTheExecutorsRow_ReadsItBack_PreservesOverrides_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the collector toggle verb end to end.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var directory = Path.Combine(Path.GetTempPath(), "darling-3752-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var configPath = Path.Combine(directory, "darling.json");
        File.WriteAllText(configPath, $$"""
            {
              "postgres": {
                "managed": false,
                "connectionString": {{JsonSerializer.Serialize(connectionString)}}
              },
              "servers": []
            }
            """);

        var bodySucceeded = false;
        try
        {
            /* A clean slate first, through the same teardown the finally runs — a previous run that died
               mid-body may have left the sentinel behind. */
            await CleanupAsync(connection, ct);

            /* A registry row for the sentinel, so --server can resolve it by display name, and a PRE-EXISTING
               per-server row carrying a frequency override with enabled = FALSE — the thing the toggle must not
               erase. (The Viewer's editor writes every column; the executor's toggle writes one.) */
            await ExecAsync(connection,
                "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, created_date, modified_date) " +
                $"VALUES ({SentinelServerId}, '{SentinelServerName}', '{SentinelDisplayName}', TRUE, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) " +
                $"VALUES ({SentinelServerId}, 'long_query_completions', 5, NULL, FALSE)", ct);

            /* 1. Fleet-wide enable, typed in the wrong case: exit 0, the canonical name written, the row read back. */
            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: true, new[] { "Long_Query_Completions", "--config", configPath }, output, error, ct);

            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Equal(string.Empty, error.ToString());
            var text = output.ToString();
            Assert.Contains("[ENABLED] long_query_completions — fleet-wide (collector enabled (fleet-wide)).", text, StringComparison.Ordinal);
            Assert.Contains("  fleet-wide: enabled=true  frequency=(default)  retention=(default)", text, StringComparison.Ordinal);
            /* The sentinel's own row is in the read-back too — still disabled, frequency override intact. */
            Assert.Contains($"  {SentinelDisplayName} (server_id {SentinelServerId}): enabled=false  frequency=every 5 min  retention=(default)", text, StringComparison.Ordinal);
            Assert.Contains("no restart is needed", text, StringComparison.OrdinalIgnoreCase);

            Assert.Equal(true, await ScalarAsync(connection,
                "SELECT enabled FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'long_query_completions'", ct));
            /* The dictionary spelling, not the typed one — one fleet row, the same row the Viewer writes. */
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT COUNT(*) FROM config.config_collector_schedules WHERE server_id IS NULL AND lower(collector_name) = 'long_query_completions'", ct));

            /* 2. Per-server enable by DISPLAY name: the pre-existing row's enabled flips, its frequency survives. */
            output = new StringWriter();
            error = new StringWriter();
            exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: true, new[] { "long_query_completions", "--server", SentinelDisplayName, "--config", configPath }, output, error, ct);

            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Contains($"[ENABLED] long_query_completions — {SentinelDisplayName} ({SentinelServerName}) (collector enabled).", output.ToString(), StringComparison.Ordinal);
            Assert.Contains($"  {SentinelDisplayName} (server_id {SentinelServerId}): enabled=true  frequency=every 5 min  retention=(default)", output.ToString(), StringComparison.Ordinal);

            await using (var read = new NpgsqlCommand(
                $"SELECT enabled, frequency_minutes FROM config.config_collector_schedules WHERE server_id = {SentinelServerId} AND collector_name = 'long_query_completions'", connection))
            await using (var reader = await read.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(reader.GetBoolean(0));
                Assert.Equal(5, reader.GetInt32(1));
                Assert.False(await reader.ReadAsync(ct), "exactly one per-server row");
            }

            /* 3. Disable fleet-wide: the fleet row flips back; the per-server row is untouched (it is its own scope). */
            output = new StringWriter();
            error = new StringWriter();
            exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: false, new[] { "long_query_completions", "--config", configPath }, output, error, ct);

            Assert.True(exit == 0, $"exit {exit}; stderr: {error}");
            Assert.Contains("[DISABLED] long_query_completions — fleet-wide (collector disabled (fleet-wide)).", output.ToString(), StringComparison.Ordinal);
            Assert.Equal(false, await ScalarAsync(connection,
                "SELECT enabled FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'long_query_completions'", ct));
            Assert.Equal(true, await ScalarAsync(connection,
                $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {SentinelServerId} AND collector_name = 'long_query_completions'", ct));

            /* 4. A server nobody has: refused with the resolver's listing, exit 1, nothing written. */
            output = new StringWriter();
            error = new StringWriter();
            exit = await DarlingCliCommands.ToggleCollectorAsync(
                enable: true, new[] { "wait_stats", "--server", "no-such-server-3752", "--config", configPath }, output, error, ct);

            Assert.Equal(1, exit);
            Assert.Contains("Could not resolve server", error.ToString(), StringComparison.Ordinal);
            Assert.Contains("Nothing was changed.", error.ToString(), StringComparison.Ordinal);
            Assert.Equal(0L, await ScalarAsync(connection,
                $"SELECT COUNT(*) FROM config.config_collector_schedules WHERE server_id = {SentinelServerId} AND collector_name = 'wait_stats'", ct));

            bodySucceeded = true;
        }
        finally
        {
            /* #1902: the store teardown runs on ITS OWN connection through LiveStoreCleanup, so a body failure
               that closed this test's connection cannot turn into a throw-from-finally that replaces the real
               exception. The temp config folder is file teardown and needs no store. */
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
            try { Directory.Delete(directory, recursive: true); } catch (IOException) { }
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* The fleet row for long_query_completions is deleted too: this class is the only live test that writes it,
           and leaving it would change what a later test's ResolveSchedule sees for the opt-in collector. */
        await ExecAsync(connection,
            $"DELETE FROM config.config_collector_schedules WHERE server_id = {SentinelServerId}; " +
            "DELETE FROM config.config_collector_schedules WHERE server_id IS NULL AND lower(collector_name) = 'long_query_completions'; " +
            $"DELETE FROM collect.servers WHERE server_id = {SentinelServerId}", ct);
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
}
