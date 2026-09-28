/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4539 end-to-end against a REAL TimescaleDB: the rollup floor cache does not re-sort a compressed
/// materialization chunk's <c>min(bucket)</c> a second time when that chunk has not changed, and it DOES
/// re-measure once the chunk identity moves.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database through <see cref="ScratchPostgres"/>
/// rather than sharing the live fixture, so it never races the shared store: everything below reads and
/// writes only this database's own rows.</para>
/// </summary>
public sealed class RollupFloorCacheLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -453453;

    private const int QueriesPerBucket = 3;

    [Fact]
    public async Task DetectRollupCoverageAsync_ReusesACompressedChunksFloor_UntilTheChunkItselfChanges()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4539 rollup floor cache test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the live rollup floor cache test");

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* No background worker racing the fixture's own refresh/compress calls below. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var oldest = now.AddDays(-3);

        /* ── 1. Plant three days of collect.query_store_stats and materialize the interval-honest hourly
               rollup over all of it. ── */
        await SeedHourlyQueryStoreStatsAsync(connection, oldest, now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* Narrow the materialization to ONE-DAY chunks (TimescaleDB defaults a fresh aggregate's chunk
           interval far wider than that) BEFORE the refresh below creates any — set_chunk_time_interval only
           affects chunks created after the call, so this must run first, or the whole 3-day materialization
           lands in a single chunk and step (c) below would have nothing narrower to drop. */
        await using (var setInterval = new NpgsqlCommand(
            TimescaleSupport.SetMaterializationChunkIntervalSql(TimescaleSupport.QueryStoreStatsIntervalHourlyView), connection))
        {
            await setInterval.ExecuteNonQueryAsync(ct);
        }

        await using (var refresh = new NpgsqlCommand(
            TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.QueryStoreStatsIntervalHourlyView, force: true), connection))
        {
            refresh.Parameters.AddWithValue(oldest);
            await refresh.ExecuteNonQueryAsync(ct);
        }

        var rawRows = await CountAsync(connection,
            $"SELECT count(*) FROM collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView}", ct);
        Assert.True(rawRows > 0, "the rollup must actually hold materialized rows or nothing below tests anything");

        /* ── 2. Compress the materialization's chunks, so a fresh min(bucket) is the expensive sorted scan
               #4539 is about, not a cheap heap read. ── */
        await using (var alter = new NpgsqlCommand(
            $"ALTER MATERIALIZED VIEW collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView} SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", connection))
        {
            await alter.ExecuteNonQueryAsync(ct);
        }

        await CompressMaterializationChunksAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);

        var (oldestChunkBefore, oldestChunkRangeEndBefore) = await OldestMaterializationChunkAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);
        Assert.NotNull(oldestChunkBefore);

        var loggerFactory = new CommandCountingLoggerFactory();
        await using var dataSource = new NpgsqlDataSourceBuilder(scratch.ConnectionString)
            .UseLoggerFactory(loggerFactory)
            .Build();
        var availability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(availability.Has(TimescaleSupport.QueryStoreStatsIntervalHourlyView),
            "the interval-honest hourly rollup should exist after the ensure sweep");

        /* ── (a) Two calls on the SAME data source return the same floor as a fresh min(bucket). ── */
        var truth = await ScalarInstantAsync(connection,
            $"SELECT min(bucket) FROM collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView}", ct);
        Assert.NotNull(truth);

        loggerFactory.Provider.Reset();
        var firstCall = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, ct);
        var floorFirst = firstCall.FloorOf(TimescaleSupport.QueryStoreStatsIntervalHourlyView);
        Assert.Equal(truth, floorFirst);

        /* ── (b) The second call returns the SAME floor without needing a fresh min(bucket).
               pg_stat_user_tables' seq_scan/idx_scan on the compressed chunk was tried as the "no re-read"
               proof and measured UNRELIABLE here: it read identically (unchanged) on BOTH this branch and
               dev, because a compressed chunk's min(bucket) plan does not register as a seq_scan/idx_scan on
               the chunk relation the way an ordinary heap read would — so it cannot tell a skipped read from
               a re-run one. pg_stat_statements is not in this rig's shared_preload_libraries either. Instead,
               this counts Npgsql's own "Executing command" debug log lines whose text names the
               materialization view's min(bucket) probe (#4197's counting-logger seam): across BOTH calls
               combined, exactly ONE such statement executes on this branch (only the first call's catalog
               miss triggers it) — RED on dev is TWO, because dev has no cache and issues a fresh min(bucket)
               on every call. ── */
        var secondCall = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, ct);
        var floorSecond = secondCall.FloorOf(TimescaleSupport.QueryStoreStatsIntervalHourlyView);
        Assert.Equal(floorFirst, floorSecond);

        var minBucketExecutions = loggerFactory.Provider.CountContaining(
            $"min(bucket) FROM collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView}");
        Assert.Equal(1, minBucketExecutions);

        /* ── (c) drop_chunks the oldest materialization chunk: the floor must RE-measure and move later.
               drop_chunks is called against the AGGREGATE VIEW (the only relation name that stays stable
               across stores), with older_than the oldest chunk's own range_end so only that one chunk goes. ── */
        await using (var drop = new NpgsqlCommand(
            $"SELECT drop_chunks('collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView}', older_than => $1::timestamp)", connection))
        {
            drop.Parameters.AddWithValue(oldestChunkRangeEndBefore);
            await drop.ExecuteNonQueryAsync(ct);
        }

        var (oldestChunkAfter, _) = await OldestMaterializationChunkAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);
        Assert.NotEqual(oldestChunkBefore, oldestChunkAfter);

        var truthAfterDrop = await ScalarInstantAsync(connection,
            $"SELECT min(bucket) FROM collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView}", ct);
        Assert.NotNull(truthAfterDrop);
        Assert.True(truthAfterDrop > truth, "dropping the oldest chunk must move the true floor LATER");

        var thirdCall = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, ct);
        var floorThird = thirdCall.FloorOf(TimescaleSupport.QueryStoreStatsIntervalHourlyView);
        Assert.Equal(truthAfterDrop, floorThird);
        Assert.True(floorThird > floorSecond, "the cache must re-measure and move the floor later once the chunk it was keyed on is gone");
    }

    private static async Task SeedHourlyQueryStoreStatsAsync(
        NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
SELECT
    (extract(epoch FROM g)::bigint * 100 + q),
    g,
    $3,
    'floor-cache-e2e',
    'TestDb',
    'dbo.Proc' || q::text,
    '0x' || lpad(to_hex(q), 16, '0'),
    q,
    q + 1000,
    'Regular',
    'PRIMARY',
    extract(epoch FROM date_trunc('hour', g))::bigint,
    date_trunc('hour', g),
    date_trunc('hour', g),
    q * 10,
    1200,
    600,
    9000,
    4000
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g
CROSS JOIN generate_series(1, $4::int) AS q", connection);

        insert.Parameters.AddWithValue(from);
        insert.Parameters.AddWithValue(to);
        insert.Parameters.AddWithValue(TestServerId);
        insert.Parameters.AddWithValue(QueriesPerBucket);
        await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task CompressMaterializationChunksAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        var chunks = await MaterializationChunksAsync(connection, view, ct);
        foreach (var chunk in chunks)
        {
            await using var compress = new NpgsqlCommand($"SELECT compress_chunk('{chunk}'::regclass, if_not_compressed => true)", connection);
            await compress.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<List<string>> MaterializationChunksAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand($@"
SELECT format('%I.%I', c.chunk_schema, c.chunk_name)
FROM timescaledb_information.chunks AS c
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  c.hypertable_schema = ca.materialization_hypertable_schema
  AND c.hypertable_name = ca.materialization_hypertable_name
WHERE ca.view_schema = 'collect' AND ca.view_name = '{view}'
ORDER BY c.range_start", connection);
        var chunks = new List<string>();
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(reader.GetString(0));
        }
        return chunks;
    }

    private static async Task<(string? ChunkName, DateTime RangeEnd)> OldestMaterializationChunkAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand($@"
SELECT format('%I.%I', c.chunk_schema, c.chunk_name), c.range_end
FROM timescaledb_information.chunks AS c
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  c.hypertable_schema = ca.materialization_hypertable_schema
  AND c.hypertable_name = ca.materialization_hypertable_name
WHERE ca.view_schema = 'collect' AND ca.view_name = '{view}'
ORDER BY c.range_start
LIMIT 1", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return (null, default);
        }

        return (reader.GetString(0), reader.GetDateTime(1));
    }

    private static async Task<DateTime?> ScalarInstantAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync(ct);
        return result is DBNull or null ? null : Convert.ToDateTime(result);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }
}
