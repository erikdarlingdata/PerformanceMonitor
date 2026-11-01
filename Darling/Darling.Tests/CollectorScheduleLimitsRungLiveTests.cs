/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another database; cleanup
   goes through LiveStoreCleanup. The one fact that creates a login-less role gives it a random name and drops it in the cleanup. */

/// <summary>
/// #6227, V206: the collector schedule table takes only what the schedule tools write. Behaviour against a real store: the rung over
/// a store that holds mixed-case, duplicate-case, junk-named and over-cap rows from before it, the raw writes the constraints refuse,
/// the clearing of schedule rows held for a server id the service has never seen when that server is first registered (as a role that
/// cannot delete schedule rows), a removed server that is added again keeping its rows, the Viewer's save of a server's schedules over the upgraded rows, and the rollback statement that gives every inherited
/// NULL the value it inherits.
/// </summary>
public sealed class CollectorScheduleLimitsRungLiveTests
{
    private const int ServerA = 8101;
    private const int ServerB = 8102;
    private const int NotYetRegistered = 8103;
    private const int ReAdded = 8104;
    private const int ObservedOnly = 8105;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string where, CancellationToken ct) =>
        (long)(await ScalarAsync(connection, "SELECT count(*) FROM config.config_collector_schedules WHERE " + where, ct))!;

    private static async Task WithStoreAsync(Func<NpgsqlConnection, CancellationToken, Task> body, Func<NpgsqlConnection, CancellationToken, Task>? cleanup = null)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V206 collector schedule limit facts (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanupConnection, cleanupCt) =>
            {
                if (cleanup is not null)
                {
                    await cleanup(cleanupConnection, cleanupCt);
                }
            });
        }
    }

    /// <summary>Puts the store back at V205 (the rewind removes the rung's NOT NULL change, constraints and trigger) and takes the
    /// V206 row out of the version table, so the next migrate runs the rung's own SQL over whatever rows the test then seeds.</summary>
    private static async Task RewindToV205Async(NpgsqlConnection connection, CancellationToken ct)
    {
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 205, ct);
        await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 206", ct);
    }

    private static async Task SeedLegacyRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, @"
INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES
    (8101, 'wait_stats',  5, 14, FALSE),     -- a lower-case row beside a differently cased twin: the lower-case row stays
    (8101, 'WAIT_STATS',  9, 90, TRUE),
    (8101, 'Query_Stats', 7, NULL, TRUE),    -- mixed case with no twin: becomes lower-case
    (8101, 'Latch_Stats', 2, NULL, TRUE),    -- two differently cased rows, neither lower-case: one stays, in lower-case
    (8101, 'LATCH_STATS', 3, NULL, TRUE),
    (8101, 'wait stats',  1, NULL, TRUE),    -- names no collector and is not an identifier: goes
    (8101, 'memory_stats', 999999, 99999, TRUE), -- over the caps: set to the caps
    (8102, 'file_io_stats', 4, NULL, TRUE),
    (NULL, 'FILE_IO_STATS', 6, NULL, TRUE),  -- a fleet-wide mixed-case row
    (8103, 'query_store', 12, NULL, FALSE)", ct);   // a row for a server nothing has registered
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // The rung over rows from before it
    // ---------------------------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheRung_TidiesRowsFromBeforeIt_KeepsTheLowerCaseTwin_ClampsTheCaps_AndRunsAgainUnchanged()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await RewindToV205Async(connection, ct);
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'config.config_collector_schedules'::regclass AND conname LIKE 'ck_config_collector_schedules_%'", ct));
            await SeedLegacyRowsAsync(connection, ct);

            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal((long)StorageVersion.SchemaVersion, await ScalarAsync(connection, "SELECT max(version)::bigint FROM darling_schema_version", ct));
            await AssertTidiedRowsAsync(connection, ct);

            /* Run again, the rung's own SQL included: nothing changes and nothing fails. */
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 206", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await AssertTidiedRowsAsync(connection, ct);
            Assert.Equal(3L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_constraint WHERE conrelid = 'config.config_collector_schedules'::regclass AND conname LIKE 'ck_config_collector_schedules_%'", ct));
        });
    }

    private static async Task AssertTidiedRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var rows = new List<(int? Server, string Name, int? Frequency, int? Retention, bool? Enabled)>();
        await using (var command = new NpgsqlCommand(
            "SELECT server_id, collector_name, frequency_minutes, retention_days, enabled FROM config.config_collector_schedules ORDER BY server_id NULLS FIRST, collector_name", connection))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add((
                    reader.IsDBNull(0) ? null : reader.GetInt32(0), reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetInt32(2), reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    reader.IsDBNull(4) ? null : reader.GetBoolean(4)));
            }
        }

        Assert.All(rows, r => Assert.Equal(r.Name.ToLowerInvariant(), r.Name));
        Assert.Equal(7, rows.Count);

        /* The lower-case twin stayed with its own values; the upper-case twin went. */
        var wait = Assert.Single(rows, r => r.Server == ServerA && r.Name == "wait_stats");
        Assert.Equal((5, 14, false), (wait.Frequency, wait.Retention, wait.Enabled));
        /* A mixed-case name with no twin is the same row in lower-case. */
        var query = Assert.Single(rows, r => r.Server == ServerA && r.Name == "query_stats");
        Assert.Equal(7, query.Frequency);
        /* Two cases of one name, neither lower-case: exactly one row is left. */
        Assert.Single(rows, r => r.Server == ServerA && r.Name == "latch_stats");
        /* A name that is not a lower-case identifier names no collector and went. */
        Assert.DoesNotContain(rows, r => r.Name == "wait stats");
        /* A value over the cap is the cap. */
        var memory = Assert.Single(rows, r => r.Server == ServerA && r.Name == "memory_stats");
        Assert.Equal((525600, 3650), (memory.Frequency, memory.Retention));
        /* The fleet-wide row is in lower-case, and another server's row and the unregistered server's row are untouched. */
        Assert.Single(rows, r => r.Server is null && r.Name == "file_io_stats" && r.Frequency == 6);
        Assert.Single(rows, r => r.Server == ServerB && r.Name == "file_io_stats" && r.Frequency == 4);
        Assert.Single(rows, r => r.Server == NotYetRegistered && r.Name == "query_store");
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // Raw writes after the rung
    // ---------------------------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task ARawWrite_OfAnUpperCaseName_AnOverLongOrOddName_OrAValueOverTheCap_IsRefused_AndTheLimitsThemselvesAreAccepted()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var longestName = new string('a', 64);
            await ExecAsync(connection,
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days) VALUES ({ServerA}, '{longestName}', 525600, 3650)", ct);

            foreach (var refused in new[]
            {
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES ({ServerA}, 'WAIT_STATS', FALSE)",
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES ({ServerA}, 'wait-stats', FALSE)",
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES ({ServerA}, '', FALSE)",
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES ({ServerA}, '{longestName}b', FALSE)",
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES ({ServerA}, 'wait_stats', 525601)",
                $"INSERT INTO config.config_collector_schedules (server_id, collector_name, retention_days) VALUES ({ServerA}, 'wait_stats', 3651)",
                $"UPDATE config.config_collector_schedules SET collector_name = 'Renamed' WHERE collector_name = '{longestName}'",
                $"UPDATE config.config_collector_schedules SET frequency_minutes = 600000 WHERE collector_name = '{longestName}'",
                $"UPDATE config.config_collector_schedules SET retention_days = 4000 WHERE collector_name = '{longestName}'",
            })
            {
                var failure = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, refused, ct));
                Assert.Equal("23514", failure.SqlState);
            }

            Assert.Equal(1L, await CountAsync(connection, "true", ct));
        });
    }

    [Fact]
    public async Task ARowHeldForAnUnregisteredServer_IsClearedWhenThatServerIsRegistered_ByARoleThatCannotDeleteSchedules_AndAnExistingServersRowsAreKept()
    {
        var roleName = "sched_limits_" + Guid.NewGuid().ToString("N")[..8];
        await WithStoreAsync(async (connection, ct) =>
        {
            /* A schedule row for an id nothing has registered is a normal write (the viewer and the tools resolve a server from the
               observed registry), so no foreign key refuses it. */
            await ExecAsync(connection, $@"
INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES
    ({NotYetRegistered}, 'wait_stats', FALSE), ({NotYetRegistered}, 'query_stats', FALSE), ({ServerA}, 'wait_stats', FALSE), (NULL, 'wait_stats', TRUE)", ct);
            Assert.Equal(4L, await CountAsync(connection, "true", ct));

            /* A role that may register a server and may not touch schedule rows: the clearing still runs, as the function's owner. */
            await ExecAsync(connection, $@"
CREATE ROLE {roleName} NOLOGIN;
GRANT USAGE ON SCHEMA config TO {roleName};
GRANT INSERT ON config.config_monitored_servers TO {roleName};
GRANT SELECT, UPDATE ON config.config_service TO {roleName};", ct);
            Assert.False((bool)(await ScalarAsync(connection, $"SELECT has_table_privilege('{roleName}', 'config.config_collector_schedules', 'DELETE')", ct))!);

            await ExecAsync(connection, $"SET ROLE {roleName}", ct);
            try
            {
                await ExecAsync(connection, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({NotYetRegistered}, 'late-registered', 'late-registered')", ct);
            }
            finally
            {
                await ExecAsync(connection, "RESET ROLE", ct);
            }

            Assert.Equal(0L, await CountAsync(connection, $"server_id = {NotYetRegistered}", ct));
            Assert.Equal(1L, await CountAsync(connection, $"server_id = {ServerA}", ct));
            Assert.Equal(1L, await CountAsync(connection, "server_id IS NULL", ct));

            /* A server that already has a definition keeps its rows when the same id is written again (an upsert, the seed). */
            await ExecAsync(connection, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({ServerA}, 'first-registered', 'first-registered')", ct);
            Assert.Equal(0L, await CountAsync(connection, $"server_id = {ServerA}", ct));
            await ExecAsync(connection, $"INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES ({ServerA}, 'wait_stats', FALSE)", ct);
            await ExecAsync(connection,
                $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({ServerA}, 'first-registered', 'first-registered') ON CONFLICT (server_id) DO NOTHING", ct);
            Assert.Equal(1L, await CountAsync(connection, $"server_id = {ServerA}", ct));
        },
        async (cleanup, cleanupCt) =>
        {
            await ExecAsync(cleanup, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName}", cleanupCt);
        });
    }

    [Fact]
    public async Task AServerTheServiceHasSeen_RemovedFromTheRegistryAndAddedAgain_KeepsItsScheduleRows_AnIdItHasNeverSeenStillHasItsRowsCleared()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            /* A server with an observed row (collect.servers keeps it after the definition goes) and a definition, whose operator
               switched a heavy collector off and set a frequency on another. */
            await ExecAsync(connection, $"INSERT INTO servers (server_id, server_name) VALUES ({ReAdded}, 'readd-alpha'), ({ObservedOnly}, 'observed-only')", ct);
            await ExecAsync(connection, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({ReAdded}, 'readd-alpha', 'readd-alpha')", ct);
            await ExecAsync(connection, $@"
INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, frequency_minutes) VALUES
    ({ReAdded}, 'query_store', FALSE, NULL), ({ReAdded}, 'wait_stats', NULL, 30),
    ({ObservedOnly}, 'query_store', FALSE, NULL),
    ({NotYetRegistered}, 'query_store', FALSE, NULL), (NULL, 'query_store', TRUE, NULL)", ct);

            /* The operator removes the server and adds it again: its schedule rows are exactly as they were. */
            await ExecAsync(connection, $"DELETE FROM config.config_monitored_servers WHERE server_id = {ReAdded}", ct);
            await ExecAsync(connection, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({ReAdded}, 'readd-alpha', 'readd-alpha')", ct);
            Assert.Equal(2L, await CountAsync(connection, $"server_id = {ReAdded}", ct));
            Assert.False((bool)(await ScalarAsync(connection,
                $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {ReAdded} AND collector_name = 'query_store'", ct))!);
            Assert.Equal(30, Convert.ToInt32(await ScalarAsync(connection,
                $"SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id = {ReAdded} AND collector_name = 'wait_stats'", ct), CultureInfo.InvariantCulture));

            /* A server the observed table holds but that was never registered keeps its rows when it is first registered. */
            await ExecAsync(connection, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({ObservedOnly}, 'observed-only', 'observed-only')", ct);
            Assert.Equal(1L, await CountAsync(connection, $"server_id = {ObservedOnly}", ct));

            /* An id that no observed row and no definition holds has its rows cleared when it is registered; the fleet-wide row stays. */
            await ExecAsync(connection, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({NotYetRegistered}, 'never-seen', 'never-seen')", ct);
            Assert.Equal(0L, await CountAsync(connection, $"server_id = {NotYetRegistered}", ct));
            Assert.Equal(1L, await CountAsync(connection, "server_id IS NULL", ct));
        });
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // The Viewer's save over the upgraded rows
    // ---------------------------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheViewersSave_OfAServersSchedules_StillWorks_OverRowsTheRungTidied()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await ExecAsync(connection, $"INSERT INTO servers (server_id, server_name) VALUES ({ServerA}, 'limits-alpha')", ct);
            await RewindToV205Async(connection, ct);
            await SeedLegacyRowsAsync(connection, ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await using var viewer = new ViewerDataService(connection.ConnectionString);
            var rows = await viewer.GetCollectorSchedulesAsync(ct);
            var runTimes = await viewer.GetCollectorRunTimesAsync(ct);
            var items = CollectorScheduleOverlay.BuildEffectiveSchedule(rows, runTimes, ServerA);
            var wait = items.Single(i => i.Name == "wait_stats");
            wait.Enabled = true;

            var plan = CollectorScheduleOverlay.BuildSavePlan(items, runTimes, ServerA, usesDefault: false, resetToDefaults: false, schedulesRead: true, runTimesRead: true);
            Assert.Null(plan.Refusal);
            await viewer.SaveCollectorScheduleAsync(ServerA, plan.Rows, plan.RunTimeChanges, plan.ClearRunTimes, ct);

            Assert.True((bool)(await ScalarAsync(connection,
                $"SELECT enabled FROM config.config_collector_schedules WHERE server_id = {ServerA} AND collector_name = 'wait_stats'", ct))!);
            /* The tidied over-cap row was written back inside the limits. */
            Assert.Equal(525600, Convert.ToInt32(await ScalarAsync(connection,
                $"SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id = {ServerA} AND collector_name = 'memory_stats'", ct), CultureInfo.InvariantCulture));
            Assert.Equal(0L, await CountAsync(connection, "collector_name <> lower(collector_name)", ct));
        });
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // The rollback statement
    // ---------------------------------------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheRollbackStatement_GivesEveryInheritedRowTheValueItInherited_AndLeavesExplicitRowsAlone()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            /* The fleet row of wait_stats is off and the fleet row of latch_stats itself inherits; long_query_completions ships off and has
               no fleet row yet. Every server row below stores NULL, as the tools and the Viewer leave it. */
            await ExecAsync(connection, $@"
INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, enabled) VALUES
    (NULL, 'wait_stats', NULL, FALSE),
    (NULL, 'latch_stats', 3, NULL),
    ({ServerA}, 'wait_stats', 10, NULL),                -- inherits the fleet row: off
    ({ServerA}, 'long_query_completions', 10, NULL),    -- a default-off collector with no fleet row: off
    ({ServerA}, 'query_stats', 10, NULL),               -- a default-on collector with no fleet row: on
    ({ServerA}, 'latch_stats', 10, NULL),               -- the fleet row inherits too: the shipped default, on
    ({ServerB}, 'wait_stats', 10, TRUE)                 -- explicit: untouched", ct);

            await ExecAsync(connection, RungRewindArtifacts.ResolveInheritedEnabledSql, ct);

            Assert.Equal(0L, await CountAsync(connection, "enabled IS NULL", ct));
            async Task<bool> EnabledAsync(int? server, string name) => (bool)(await ScalarAsync(connection,
                $"SELECT enabled FROM config.config_collector_schedules WHERE {(server is int id ? "server_id = " + id : "server_id IS NULL")} AND collector_name = '{name}'", ct))!;

            Assert.False(await EnabledAsync(ServerA, "wait_stats"));
            Assert.False(await EnabledAsync(ServerA, "long_query_completions"));
            Assert.True(await EnabledAsync(ServerA, "query_stats"));
            Assert.True(await EnabledAsync(ServerA, "latch_stats"));
            Assert.True(await EnabledAsync(null, "latch_stats"));
            Assert.False(await EnabledAsync(null, "wait_stats"));
            Assert.True(await EnabledAsync(ServerB, "wait_stats"));

            /* A fleet row for the default-off collector that is on: a server row that inherited it is on. */
            await ExecAsync(connection, $@"
INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled) VALUES
    (NULL, 'long_query_completions', TRUE), ({ServerB}, 'long_query_completions', NULL)", ct);
            await ExecAsync(connection, RungRewindArtifacts.ResolveInheritedEnabledSql, ct);
            Assert.True(await EnabledAsync(ServerB, "long_query_completions"));
            Assert.False(await EnabledAsync(ServerA, "long_query_completions"));

            /* The whole rewind runs over the same rows: NOT NULL is back and the V205 shape holds. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 205, ct);
            Assert.Equal("NO", await ScalarAsync(connection,
                "SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_collector_schedules' AND column_name = 'enabled'", ct));
        });
    }
}
