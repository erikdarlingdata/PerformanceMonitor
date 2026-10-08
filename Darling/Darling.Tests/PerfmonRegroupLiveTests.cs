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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5574: the perfmon_stats re-group loop against a real TimescaleDB store. Each test mints its own scratch database, like
/// <see cref="CollectionLogSegmentByLiveTests"/>, which this class mirrors. The old store is built with the statements
/// every build before #5574 ran (create_hypertable, then the enable ALTER with <c>server_id</c> alone), with its chunks
/// compressed under those settings. The expected groupings are literals on purpose: they pin the value the store ends up
/// with, so they must not move when the product's constant does.
/// </summary>
[Collection("live-postgres")]
public sealed class PerfmonRegroupLiveTests
{
    private const string WantedChunkSegmentBy = "server_id,counter_name";
    private const string OldChunkSegmentBy = "server_id";
    private const long IdBase = -495_200_000_000L;

    private sealed record ChunkRow(string Name, bool IsCompressed, string SegmentBy, DateTime Start);

    /// <summary>
    /// The upgraded store converges: with the hypertable already on the new grouping, the drain re-groups the chunks inside
    /// the 30-day reach newest first, leaves the chunk outside it alone, changes no row (count and a checksum per counter),
    /// pauses after every chunk but the last, and a second run is a no-op.
    /// </summary>
    [Fact]
    public async Task AnUpgradedStore_IsReGroupedNewestFirst_KeepsEveryRow_LeavesOutOfReachAlone_AndASecondRunIsANoOp()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await BuildOldStoreAsync(connection, scratch.ConnectionString, ct);

        var before = await ReadChunksAsync(connection, ct);
        Assert.Equal(6, before.Count);
        Assert.All(before, chunk => Assert.Equal(OldChunkSegmentBy, chunk.SegmentBy));
        var fingerprint = await FingerprintAsync(connection, ct);

        /* The hypertable still has the old grouping: the gate keeps the drain from writing the old grouping again. */
        var gated = new CapturingTestLogger();
        Assert.Equal(0, await NewDrain(scratch.ConnectionString, gated, TimeSpan.Zero, new List<TimeSpan>()).RunAsync(ct));
        Assert.DoesNotContain(gated.Lines, line => line.Contains("re-grouped perfmon_stats chunk", StringComparison.Ordinal));

        await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(TimescaleSupport.PerfmonStatsTable), ct);
        var inReachNewestFirst = before.Where(chunk => chunk.Start > DateTime.UtcNow.AddDays(-30)).OrderByDescending(chunk => chunk.Start).Select(chunk => chunk.Name).ToList();
        Assert.Equal(5, inReachNewestFirst.Count);

        var logger = new CapturingTestLogger();
        var pauses = new List<TimeSpan>();
        Assert.Equal(5, await NewDrain(scratch.ConnectionString, logger, TimeSpan.FromSeconds(90), pauses).RunAsync(ct));

        var after = await ReadChunksAsync(connection, ct);
        Assert.All(after.Where(chunk => inReachNewestFirst.Contains(chunk.Name)), chunk => Assert.Equal(WantedChunkSegmentBy, chunk.SegmentBy));
        Assert.Equal(OldChunkSegmentBy, after.Single(chunk => !inReachNewestFirst.Contains(chunk.Name)).SegmentBy);
        Assert.Equal(inReachNewestFirst, logger.Lines
            .Select(line => Regex.Match(line, @"re-grouped perfmon_stats chunk (\S+) "))
            .Where(match => match.Success).Select(match => match.Groups[1].Value).ToList());
        Assert.Equal(fingerprint, await FingerprintAsync(connection, ct));

        /* Four pauses for five chunks, none after the last, each the floor (a chunk this small takes far less). */
        Assert.Equal(4, pauses.Count);
        Assert.All(pauses, pause => Assert.Equal(TimeSpan.FromSeconds(90), pause));

        var second = new CapturingTestLogger();
        Assert.Equal(0, await NewDrain(scratch.ConnectionString, second, TimeSpan.Zero, new List<TimeSpan>()).RunAsync(ct));
        Assert.DoesNotContain(second.Lines, line => line.StartsWith("Information:", StringComparison.Ordinal));
        Assert.Equal(after, await ReadChunksAsync(connection, ct));
    }

    /* ---------------- fixture ---------------- */

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the perfmon_stats re-group tests (each mints its own scratch database).");
        return connectionString!;
    }

    private static PerfmonRegroupDrain NewDrain(string connectionString, CapturingTestLogger logger, TimeSpan minPause, List<TimeSpan> pauses) =>
        new(async token =>
            {
                var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(token);
                return connection;
            },
            logger, 30, minPause,
            (pause, token) =>
            {
                pauses.Add(pause);
                return Task.CompletedTask;
            });

    /// <summary>
    /// perfmon_stats as every build before #5574 left it: a hypertable compressed by <c>server_id</c> alone, five daily
    /// chunks inside the reach (yesterday back to five days ago) and one 45 days back, all compressed. Skips on a TimescaleDB
    /// without the per-chunk settings view (before 2.14.1), which these tests read.
    /// </summary>
    private static async Task BuildOldStoreAsync(NpgsqlConnection connection, string connectionString, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(connectionString, ct), "TimescaleDB is not available on this cluster.");
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        Assert.SkipUnless(
            await ScalarAsync<bool>(connection, "SELECT to_regclass('timescaledb_information.chunk_compression_settings') IS NOT NULL", ct),
            "this TimescaleDB has no timescaledb_information.chunk_compression_settings (before 2.14.1).");

        await ExecAsync(connection, TimescaleSupport.CreateHypertableSql(TimescaleSupport.PerfmonStatsTable, "collection_time"), ct);
        await ExecAsync(connection, "ALTER TABLE perfmon_stats SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

        var today = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
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
    private static async Task SeedDayAsync(NpgsqlConnection connection, DateTime day, int salt, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
              SELECT $1 - $3 * 100000 - row_number() OVER (), g.t, s.id, 'REGROUP-SRV', 'SQLServer:Test', 'Counter ' || c.n,
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

    private static async Task<List<ChunkRow>> ReadChunksAsync(NpgsqlConnection connection, CancellationToken ct)
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

    /// <summary>One md5 over the row count and a hash sum per (server, counter): unchanged data gives the same value.</summary>
    private static Task<string> FingerprintAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<string>(connection,
            @"SELECT md5(string_agg(concat_ws('|', server_id, counter_name, n, h), ',' ORDER BY server_id, counter_name))
              FROM (SELECT server_id, counter_name, count(*) AS n,
                           sum(hashtextextended(concat_ws('|', collection_id, collection_time, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type), 0)::numeric) AS h
                    FROM perfmon_stats GROUP BY server_id, counter_name) AS t", ct);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return (T)Convert.ChangeType(value!, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }
}
