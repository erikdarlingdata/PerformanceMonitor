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

/// <summary>The latch and spinlock snapshot reads against a real store return the collection time of the snapshot they rendered (#4966).</summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own scratch database. */
[Collection("viewer-time-statics")]
public sealed class ViewerSnapshotTimeLiveTests
{
    private const int ServerId = -496660;

    [Fact]
    public async Task TheLatchAndSpinlockSnapshots_CarryTheNewestCollectionTime_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await QueryGridSeed.OpenScratchAsync(ct);
        var end = QueryGridSeed.NowToTheMinute();
        var older = end.AddHours(-3);
        var newest = end.AddHours(-1);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await EventGridSeed.AddServerAsync(connection, ServerId, "snapshot-time", end.AddDays(-2), "latch_stats", end, clockOffsetMinutes: null, ct);
            long id = 1;
            foreach (var at in new[] { older, newest })
            {
                await ExecAsync(connection, $"INSERT INTO latch_stats (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms, delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds) VALUES ({id++}, @t, {ServerId}, 'snapshot-time', 'BUFFER', 10, 20, 5, 1, 2, 1, 60)", at, ct);
                await ExecAsync(connection, $"INSERT INTO spinlock_stats (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs, delta_collisions, delta_spins, delta_sleep_time, delta_backoffs, sample_interval_seconds) VALUES ({id++}, @t, {ServerId}, 'snapshot-time', 'LOCK_HASH', 10, 20, 2, 0, 0, 1, 2, 0, 0, 60)", at, ct);
            }
        }

        var viewer = new ViewerDataService(scratch.ConnectionString);
        var latch = await viewer.GetLatchStatsSnapshotAsync(ServerId, end.AddDays(-7), end, ct);
        var spinlock = await viewer.GetSpinlockStatsSnapshotAsync(ServerId, end.AddDays(-7), end, ct);

        Assert.Single(latch);
        Assert.Equal(newest, latch[0].CollectionTime);
        Assert.Single(spinlock);
        Assert.Equal(newest, spinlock[0].CollectionTime);
        Assert.Empty(await viewer.GetLatchStatsSnapshotAsync(ServerId + 1, end.AddDays(-7), end, ct));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, DateTime at, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("t", DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }
}
