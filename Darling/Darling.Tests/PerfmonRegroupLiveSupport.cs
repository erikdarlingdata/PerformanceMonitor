/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class holds no test; it is the fixture code that PerfmonRegroupLiveTests and
   PerfmonRegroupTimingLiveTests share, and every test that uses it mints its own scratch database through
   ScratchPostgres, so neither class touches the shared store. */

/// <summary>
/// The scratch-store helpers that the perfmon_stats re-group live tests share (#5574): the old-grouping store builder, the
/// chunk catalog read, and the held chunk lock. <see cref="PerfmonRegroupLiveTests"/> keeps the tests that judge the
/// drain's results; <see cref="PerfmonRegroupTimingLiveTests"/> holds the two that judge a time against a limit and so run
/// in the <c>timing</c> collection.
/// </summary>
internal static class PerfmonRegroupLiveSupport
{

    internal const string OldChunkSegmentBy = "server_id";
    internal const long IdBase = -495_200_000_000L;

    /// <summary>"Now" for every day the tests seed, name and compare, captured ONCE: a run that crosses midnight UTC between a
    /// seed and a lookup must not move the day a chunk is looked up by. The database's own clock decides the reach, which has
    /// days of slack on both sides of the seeded chunks in every test but <c>TheReach_IsClampedToTheRetentionLessADay</c>; that
    /// one reads the chunks in its reach from <c>now()</c> itself.</summary>
    internal static readonly DateTime RunStartUtc = DateTime.UtcNow;

    internal sealed record ChunkRow(string Name, bool IsCompressed, string SegmentBy, DateTime Start);

    internal static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the perfmon_stats re-group tests (each mints its own scratch database).");
        return connectionString!;
    }

    /// <summary>
    /// perfmon_stats as every build before #5574 left it: a hypertable compressed by <c>server_id</c> alone, five daily
    /// chunks inside the reach (yesterday back to five days ago) and one 45 days back, all compressed. Skips on a TimescaleDB
    /// without the per-chunk settings view (before 2.14.1), which these tests read.
    /// </summary>
    internal static async Task BuildOldStoreAsync(NpgsqlConnection connection, string connectionString, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(connectionString, ct), "TimescaleDB is not available on this cluster.");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        Assert.SkipUnless(
            await ScalarAsync<bool>(connection, "SELECT to_regclass('timescaledb_information.chunk_compression_settings') IS NOT NULL", ct),
            "this TimescaleDB has no timescaledb_information.chunk_compression_settings (before 2.14.1).");

        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql(TimescaleSupport.PerfmonStatsTable, "collection_time"), ct);
        await ExecAsync(connection, "ALTER TABLE perfmon_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        var today = DateTime.SpecifyKind(RunStartUtc.Date, DateTimeKind.Unspecified);
        foreach (var daysBack in new[] { 1, 2, 3, 4, 5, 45 })
        {
            await SeedDayAsync(connection, today.AddDays(-daysBack), daysBack, ct);
        }

        var chunks = new List<string>();
        using (var list = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks "
            + "WHERE hypertable_schema = 'collect' AND hypertable_name = 'perfmon_stats' AND NOT is_compressed AND range_end <= now()", connection))
        await using (var reader = await list.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        foreach (var chunk in chunks)
        {
            await ExecAsync(connection, $"SELECT compress_chunk('{chunk}')", ct);
        }
    }

    /// <summary>Two servers, four counters, two instances each, a sample every 30 minutes: 768 rows for the day.</summary>
    internal static async Task SeedDayAsync(NpgsqlConnection connection, DateTime day, int salt, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
              SELECT $1 - $3 * 100000 - row_number() OVER (), g.t AT TIME ZONE 'UTC', s.id, 'REGROUP-SRV', 'SQLServer:Test', 'Counter ' || c.n,
                     CASE WHEN i.i = 0 THEN '' ELSE 'inst' || i.i END,
                     (extract(epoch FROM g.t)::bigint / 60 + s.id + c.n * 7 + i.i) % 100000, (extract(epoch FROM g.t)::bigint / 60 + c.n) % 50, 60, 65792
              FROM generate_series($2::timestamp, $2::timestamp + INTERVAL '23 hours 30 minutes', INTERVAL '30 minutes') AS g(t)
              CROSS JOIN generate_series(1, 2) AS s(id)
              CROSS JOIN generate_series(1, 4) AS c(n)
              CROSS JOIN generate_series(0, 1) AS i(i)", connection);
        command.Parameters.AddWithValue(IdBase);
        command.Parameters.AddWithValue(day);
        command.Parameters.AddWithValue(salt);
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static async Task<List<ChunkRow>> ReadChunksAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"SELECT format('%I.%I', c.chunk_schema, c.chunk_name), c.is_compressed, coalesce(s.segmentby, ''), (c.range_start AT TIME ZONE 'UTC')
              FROM timescaledb_information.chunks AS c
              LEFT JOIN timescaledb_information.chunk_compression_settings AS s ON s.chunk = to_regclass(format('%I.%I', c.chunk_schema, c.chunk_name))
              WHERE c.hypertable_schema = 'collect' AND c.hypertable_name = 'perfmon_stats'
              ORDER BY c.range_start DESC", connection);
        var chunks = new List<ChunkRow>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(new ChunkRow(reader.GetString(0), reader.GetBoolean(1), reader.GetString(2), DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc)));
        }

        return chunks;
    }

    internal static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return (T)Convert.ChangeType(value!, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }

    internal static ChunkRow ChunkOfDay(List<ChunkRow> chunks, int daysBack) =>
        chunks.Single(chunk => chunk.Start.Date == RunStartUtc.Date.AddDays(-daysBack));

    /// <summary>Another session holds ROW EXCLUSIVE on the chunk inside an open transaction, which conflicts with the
    /// EXCLUSIVE lock both halves of the re-group ask for.</summary>
    internal static async Task<HeldLock> HoldLockAsync(string connectionString, string chunk, CancellationToken ct)
    {
        var holder = new NpgsqlConnection(connectionString);
        await holder.OpenAsync(ct);
        await ExecAsync(holder, "BEGIN", ct);
        await ExecAsync(holder, $"LOCK TABLE {chunk} IN ROW EXCLUSIVE MODE", ct);
        return new HeldLock(holder);
    }

    /// <summary>The session that holds a chunk's lock; released by <see cref="ReleaseAsync"/> or, at the latest, when
    /// the test's scope ends.</summary>
    internal sealed class HeldLock : IAsyncDisposable
    {
        private NpgsqlConnection? _connection;

        public HeldLock(NpgsqlConnection connection) => _connection = connection;

        public async Task ReleaseAsync(CancellationToken ct)
        {
            var connection = Interlocked.Exchange(ref _connection, null);
            if (connection is not null)
            {
                await ExecAsync(connection, "ROLLBACK", ct);
                await connection.DisposeAsync();
            }
        }

        public async ValueTask DisposeAsync()
        {
            var connection = Interlocked.Exchange(ref _connection, null);
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }
        }
    }

    internal static async Task RequirePerChunkSettingsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Assert.SkipUnless(
            await ScalarAsync<bool>(connection, "SELECT to_regclass('timescaledb_information.chunk_compression_settings') IS NOT NULL", ct),
            "this TimescaleDB has no timescaledb_information.chunk_compression_settings (before 2.14.1).");
}
