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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="ViewerDataService.GetPgDataStartAsync"/> against a real store with TimescaleDB where it is present (#4966): the seeded
/// start of two PostgreSQL tables comes back, a window of 90 minutes or less runs no query, and a table the probe refuses answers null.
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to create and drop its own database through ScratchPostgres. */
public sealed class ViewerPostgresDataStartLiveTests
{
    private const int LockServerId = -496701;
    private const int DeadlockServerId = -496702;

    [Fact]
    public async Task TheSeededStart_ComesBack_ForTwoTables_AndNullForANarrowWindowOrAnUnknownTable_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL data-start test.");
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var end = QueryGridSeed.NowToTheMinute();

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            if (await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct))
            {
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
                await using var stopWorkers = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
                await stopWorkers.ExecuteNonQueryAsync(ct);
            }

            /* Added a day ago, its first run and first lock row together; a second server added 2 days ago does the same for deadlocks. */
            await EventGridSeed.AddServerAsync(connection, LockServerId, "pg-lock-start", end.AddDays(-1), "pg_lock_stats", end, clockOffsetMinutes: null, ct);
            await InsertAsync(connection, "pg_lock_stats", LockServerId, "pg-lock-start", end.AddDays(-1), ct);
            await EventGridSeed.AddServerAsync(connection, DeadlockServerId, "pg-deadlock-start", end.AddDays(-2), "pg_deadlocks", end, clockOffsetMinutes: null, ct);
            await InsertAsync(connection, "pg_deadlocks", DeadlockServerId, "pg-deadlock-start", end.AddDays(-2), ct);
        }

        var viewer = new ViewerDataService(scratch.ConnectionString);
        var start = end.AddDays(-7);

        Assert.Equal(end.AddDays(-1), await viewer.GetPgDataStartAsync("pg_lock_stats", LockServerId, start, end, ct));
        Assert.Equal(end.AddDays(-2), await viewer.GetPgDataStartAsync("pg_deadlocks", DeadlockServerId, start, end, ct));
        Assert.Null(await viewer.GetPgDataStartAsync("pg_lock_stats", LockServerId, end.AddMinutes(-90), end, ct));
        Assert.Null(await viewer.GetPgDataStartAsync("not_a_collector_table", LockServerId, start, end, ct));
    }

    private static async Task InsertAsync(NpgsqlConnection connection, string table, int serverId, string serverName, DateTime firstUtc, CancellationToken ct)
    {
        var extra = table == "pg_deadlocks" ? "occurred_at, deadlock_hash" : "database_name";
        var value = table == "pg_deadlocks" ? "t, 'h'" : "'d'";
        await using var insert = new NpgsqlCommand($"""
            INSERT INTO collect.{table} (collection_id, collection_time, server_id, server_name, {extra})
            SELECT row_number() OVER () + $4, t, $1, $2, {value}
            FROM generate_series($3::timestamp, $3::timestamp + interval '3 hours', interval '1 hour') AS t
            """, connection);
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstUtc, DateTimeKind.Unspecified));
        insert.Parameters.AddWithValue(Math.Abs((long)serverId) * 1_000L);
        await insert.ExecuteNonQueryAsync(ct);
    }
}
