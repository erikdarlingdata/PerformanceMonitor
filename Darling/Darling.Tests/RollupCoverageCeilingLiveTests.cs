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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605 part 2 (LA-1b) end-to-end against a REAL TimescaleDB: <see cref="RollupCoverage.CeilingOf"/>
/// matches the engine's own materialization watermark, advances once the rollup is refreshed further, and
/// stays put for a cycle where the floor (and therefore the ceiling) is reused from cache rather than
/// re-read.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database through <see cref="ScratchPostgres"/>
/// rather than sharing the live fixture.</para>
/// </summary>
public sealed class RollupCoverageCeilingLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -460560;

    private const int QueriesPerBucket = 3;

    [Fact]
    public async Task DetectRollupCoverageAsync_CeilingOf_MatchesTheEngineWatermark_AndAdvancesOnRefresh_AndHoldsWhileCached()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4605 ceiling test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the live #4605 ceiling test");

        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* No background worker racing the fixture's own refresh calls below. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var oldest = now.AddHours(-6);
        var view = TimescaleSupport.QueryStoreStatsIntervalHourlyView;

        /* ── 1. Plant 6 hours of raw query_store_stats and materialize the hourly rollup only up to
               now - 2h, the way the product's own refresh cadence leaves the newest buckets unmaterialized. ── */
        await SeedHourlyQueryStoreStatsAsync(connection, oldest, now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var refreshTo = now.AddHours(-2);
        await using (var setInterval = new NpgsqlCommand(TimescaleSupport.SetMaterializationChunkIntervalSql(view), connection))
        {
            await setInterval.ExecuteNonQueryAsync(ct);
        }

        await using (var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp, true)", connection))
        {
            refresh.Parameters.AddWithValue(oldest);
            refresh.Parameters.AddWithValue(refreshTo);
            await refresh.ExecuteNonQueryAsync(ct);
        }

        var rowsAfterFirstRefresh = await CountAsync(connection, $"SELECT count(*) FROM collect.{view}", ct);
        Assert.True(rowsAfterFirstRefresh > 0, "the rollup must actually hold materialized rows or nothing below tests anything");

        await using var dataSource = new NpgsqlDataSourceBuilder(scratch.ConnectionString).Build();
        var availability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(availability.Has(view), "the interval-honest hourly rollup should exist after the ensure sweep");

        /* ── 2. The product's own watermark read, for comparison against what the coverage probe reports. ── */
        var engineWatermark = await RollupMaterializationWatermark.GetAsync(
            dataSource, view, TimescaleSupport.HourlyBucket, commandTimeoutSeconds: 30, ct);
        Assert.NotNull(engineWatermark);

        var firstCall = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, now, ct);
        Assert.Equal(engineWatermark, firstCall.CeilingOf(view));

        /* ── 3. Refresh one more hour and force a re-measure the way the #4539/#4553 cache decides one is
               needed: the oldest chunk identity is unchanged here, but crossing the cache's TTL forces a
               fresh read regardless — the same "identity alone is not always enough" safety net
               RollupFloorMaxReuse documents. ── */
        var refreshToLater = now.AddHours(-1);
        await using (var refreshAgain = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp, true)", connection))
        {
            refreshAgain.Parameters.AddWithValue(oldest);
            refreshAgain.Parameters.AddWithValue(refreshToLater);
            await refreshAgain.ExecuteNonQueryAsync(ct);
        }

        var laterWatermark = await RollupMaterializationWatermark.GetAsync(
            dataSource, view, TimescaleSupport.HourlyBucket, commandTimeoutSeconds: 30, ct);
        Assert.NotNull(laterWatermark);
        Assert.True(laterWatermark > engineWatermark, "the second refresh must move the engine's own watermark later");

        var pastTtl = now + TimescaleSupport.RollupFloorMaxReuse + TimeSpan.FromMinutes(1);
        var secondCall = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, pastTtl, ct);
        Assert.Equal(laterWatermark, secondCall.CeilingOf(view));
        Assert.True(secondCall.CeilingOf(view) > firstCall.CeilingOf(view), "the ceiling must advance once the rollup materializes further");

        /* ── 4. A cycle immediately after (well inside the TTL, same chunk identity) reuses the cached floor
               — and therefore must answer the SAME ceiling as the call that measured it, with no fresh
               watermark read for this view. ── */
        var thirdCall = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, pastTtl + TimeSpan.FromSeconds(1), ct);
        Assert.Equal(secondCall.CeilingOf(view), thirdCall.CeilingOf(view));
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
    'ceiling-e2e',
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

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result);
    }
}
