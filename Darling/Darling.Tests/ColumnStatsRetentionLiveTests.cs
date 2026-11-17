/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8798 on a live store: the current column statistics rows and their cursor stay out of the age purge. They go only when no server's
/// cursor has seen their (history key, database) inside the window, so an availability group's rows stay while any of its servers still
/// sees the database, and the run log (a catalogue table) is purged by age like any other.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class ColumnStatsRetentionLiveTests
{
    private static readonly DateTime s_now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
    private static readonly Guid s_group = Guid.Parse("66666666-6666-6666-6666-666666666666");

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the column stats retention live tests.");
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static string Key(int serverId, Guid? group) =>
        group is { } g ? "ag:" + g.ToString("D") : "server:" + serverId.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /* One current row of the history (server key, or group key when a group is given), 60 days old. */
    private static async Task SeedRowAsync(
        NpgsqlConnection c, int serverId, string database, int objectId, CancellationToken ct, Guid? group = null)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.column_stats (server_id, history_key, database_name, object_id, schema_name, object_name, stats_id,
                                  stats_name, origin, prefix_length, columns, includes_clustering_key, all_density, rows, rows_sampled, last_updated, last_updated_utc, read_at)
VALUES ($1, $2, $3, $4, 'dbo', 'T', 1, 'ix', 'index', 1, ARRAY['Id'], false, 0.5, 100, 100, $5, $5, $5)", c);
        insert.Parameters.Add(new NpgsqlParameter { Value = serverId });
        insert.Parameters.Add(new NpgsqlParameter { Value = Key(serverId, group) });
        insert.Parameters.Add(new NpgsqlParameter { Value = database });
        insert.Parameters.Add(new NpgsqlParameter { Value = objectId });
        insert.Parameters.Add(new NpgsqlParameter { Value = s_now.AddDays(-60), NpgsqlDbType = NpgsqlDbType.Timestamp });
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedStateAsync(
        NpgsqlConnection c, int serverId, string database, DateTime lastSeen, CancellationToken ct, Guid? group = null)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.column_stats_state (server_id, database_name, history_key, last_seen_in_scope_at)
VALUES ($1, $2, $3, $4)", c);
        insert.Parameters.Add(new NpgsqlParameter { Value = serverId });
        insert.Parameters.Add(new NpgsqlParameter { Value = database });
        insert.Parameters.Add(new NpgsqlParameter { Value = Key(serverId, group) });
        insert.Parameters.Add(new NpgsqlParameter { Value = lastSeen, NpgsqlDbType = NpgsqlDbType.Timestamp });
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection c, string table, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM " + table, c);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task CurrentRow_OlderThanRetention_SurvivesTheSweep()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedStateAsync(c, 1, "SalesDb", s_now, ct);
            await SeedRowAsync(c, 1, "SalesDb", 10, ct);

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct);

            Assert.Equal(1L, await CountAsync(c, "collect.column_stats", ct));
            Assert.Equal(1L, await CountAsync(c, "collect.column_stats_state", ct));
        });
    }

    [Fact]
    public async Task AgKey_RowsStay_WhileAnotherServerStillSeesTheDatabase()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            /* Server 1 stopped seeing the database 40 days ago; server 2, in the same group, saw it now. The rows were read by server 1. */
            await SeedStateAsync(c, 1, "SalesDb", s_now.AddDays(-40), ct, s_group);
            await SeedStateAsync(c, 2, "SalesDb", s_now, ct, s_group);
            await SeedRowAsync(c, 1, "SalesDb", 10, ct, s_group);

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct);

            Assert.Equal(1L, await CountAsync(c, "collect.column_stats", ct));
            Assert.Equal(1L, await CountAsync(c, "collect.column_stats_state", ct));
        });
    }

    [Fact]
    public async Task RowsGo_WhenNoServerSeesTheDatabaseAnyMore()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedStateAsync(c, 1, "SalesDb", s_now.AddDays(-40), ct, s_group);
            await SeedStateAsync(c, 2, "SalesDb", s_now.AddDays(-35), ct, s_group);
            await SeedRowAsync(c, 1, "SalesDb", 10, ct, s_group);
            /* Another database of server 3 is still seen and keeps its row. */
            await SeedStateAsync(c, 3, "OpsDb", s_now, ct);
            await SeedRowAsync(c, 3, "OpsDb", 11, ct);

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct);

            Assert.Equal(1L, await CountAsync(c, "collect.column_stats", ct));
            Assert.Equal(1L, await CountAsync(c, "collect.column_stats_state", ct));
            await using var remaining = new NpgsqlCommand("SELECT database_name FROM collect.column_stats", c);
            Assert.Equal("OpsDb", (string)(await remaining.ExecuteScalarAsync(ct))!);
        });
    }

    [Fact]
    public async Task RunsTable_IsPurgedByAge()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.column_stats_runs (collection_id, collection_time, server_id, server_name, database_name)
VALUES (1, $1, 1, 's', 'SalesDb'), (2, $2, 1, 's', 'SalesDb')", c);
            insert.Parameters.Add(new NpgsqlParameter { Value = s_now.AddDays(-60), NpgsqlDbType = NpgsqlDbType.Timestamp });
            insert.Parameters.Add(new NpgsqlParameter { Value = s_now.AddDays(-1), NpgsqlDbType = NpgsqlDbType.Timestamp });
            await insert.ExecuteNonQueryAsync(ct);

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct);

            Assert.Equal(1L, await CountAsync(c, "collect.column_stats_runs", ct));
        });
    }

    [Fact]
    public async Task StampWindow_IsTheRetentionHorizon_AServerDownForDaysKeepsItsRows()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedStateAsync(c, 1, "FreshDb", s_now.AddDays(-29), ct);
            await SeedRowAsync(c, 1, "FreshDb", 10, ct);
            await SeedStateAsync(c, 2, "StaleDb", s_now.AddDays(-31), ct);
            await SeedRowAsync(c, 2, "StaleDb", 11, ct);

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct);

            await using var remaining = new NpgsqlCommand("SELECT string_agg(database_name, ',') FROM collect.column_stats", c);
            Assert.Equal("FreshDb", (string)(await remaining.ExecuteScalarAsync(ct))!);
            Assert.Equal(1L, await CountAsync(c, "collect.column_stats_state", ct));
        });
    }

    [Fact]
    public async Task StampWindow_FollowsTheConfiguredRetention()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedStateAsync(c, 1, "FreshDb", s_now.AddDays(-6), ct);
            await SeedRowAsync(c, 1, "FreshDb", 10, ct);
            await SeedStateAsync(c, 2, "StaleDb", s_now.AddDays(-8), ct);
            await SeedRowAsync(c, 2, "StaleDb", 11, ct);

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, null, ct, retentionDaysFor: _ => 7);

            await using var remaining = new NpgsqlCommand("SELECT string_agg(database_name, ',') FROM collect.column_stats", c);
            Assert.Equal("FreshDb", (string)(await remaining.ExecuteScalarAsync(ct))!);
            Assert.Equal(1L, await CountAsync(c, "collect.column_stats_state", ct));
        });
    }
}
