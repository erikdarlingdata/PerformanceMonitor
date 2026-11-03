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
/// #4605 end-to-end against a REAL TimescaleDB: <see cref="RollupCoverage.CeilingOf"/>
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

        /* #7240: the ceiling is read on every probe, so a probe that serves the cached floor (inside the hour, with the oldest
           chunk unchanged) still reports the watermark the refresh just moved, with no re-measure of the floor. */
        var insideTtl = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, now.AddMinutes(1), ct);
        Assert.Equal(firstCall.FloorOf(view), insideTtl.FloorOf(view));
        Assert.Equal(laterWatermark, insideTtl.CeilingOf(view));
        Assert.True(insideTtl.CeilingOf(view) > firstCall.CeilingOf(view), "the ceiling must advance once the rollup materializes further");

        /* #4957: past the hour the floor is served from the cache and re-measured once in the background. The ceiling is the
           same fresh watermark on that probe and after the re-measure lands. */
        var pastTtl = now + TimescaleSupport.RollupFloorMaxReuse + TimeSpan.FromMinutes(1);
        var servedFromCache = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, pastTtl, ct);
        Assert.Equal(laterWatermark, servedFromCache.CeilingOf(view));
        await TimescaleSupport.RollupFloorDrainFor(dataSource);

        var thirdCall = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, pastTtl + TimeSpan.FromSeconds(1), ct);
        Assert.Equal(laterWatermark, thirdCall.CeilingOf(view));
    }

    /// <summary>
    /// #7240: a store where the one-statement watermark read does not resolve (a TimescaleDB older than 2.12 has no
    /// <c>_timescaledb_functions</c> schema, or the role cannot use it) still reports the fresh ceiling, <c>max(bucket)</c> plus
    /// the bucket width, not the one cached with the floor. The role here has SELECT on the rollup views and no EXECUTE on
    /// <c>cagg_watermark</c> (revoked from PUBLIC in this scratch database only). Revoking USAGE on the whole schema breaks every
    /// other read the probe makes, so it would not isolate the batch statement.
    /// </summary>
    [Fact]
    public async Task DetectRollupCoverageAsync_WhenTheWatermarkFunctionIsOutOfReach_ReportsTheFreshMaxBucketCeiling()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #7240 fallback test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available on CI for the live #7240 fallback test");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var oldest = now.AddHours(-6);
        var view = TimescaleSupport.QueryStoreStatsIntervalHourlyView;

        await SeedHourlyQueryStoreStatsAsync(connection, oldest, now, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await ExecAsync(connection, TimescaleSupport.SetMaterializationChunkIntervalSql(view), ct);
        await RefreshAsync(connection, view, oldest, now.AddHours(-2), ct);

        /* Roles are cluster-wide, so the name is unique to this run and dropped below. */
        var role = "darling_7240_" + Guid.NewGuid().ToString("N")[..8];
        const string password = "Pw-7240-nofn";
        await ExecAsync(connection, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{password}'", ct);
        try
        {
            await ExecAsync(connection, $"GRANT USAGE ON SCHEMA collect TO {role}", ct);
            await ExecAsync(connection, $"GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {role}", ct);
            await ExecAsync(connection, "REVOKE EXECUTE ON FUNCTION _timescaledb_functions.cagg_watermark(integer) FROM PUBLIC", ct);

            var roleConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password }.ConnectionString;
            await using var dataSource = new NpgsqlDataSourceBuilder(roleConnectionString).Build();

            /* The setup must really put the watermark function out of reach, or nothing below tests the fallback. */
            await Assert.ThrowsAsync<PostgresException>(() => RollupMaterializationWatermark.GetManyAsync(dataSource, new[] { view }, 30, ct));

            var availability = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            Assert.True(availability.Has(view), "the role must see the hourly rollup");

            var first = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, now, ct);
            var firstCeiling = first.CeilingOf(view);
            Assert.NotNull(firstCeiling);
            Assert.NotNull(first.FloorOf(view));

            /* The rollup catches up after the floor was cached. */
            await RefreshAsync(connection, view, oldest, now.AddHours(-1), ct);
            var expected = (DateTime)(await ScalarAsync(connection, $"SELECT max(bucket) + interval '1 hour' FROM collect.{view}", ct))!;
            Assert.True(expected > firstCeiling, "the second refresh must move the newest bucket");

            var second = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, availability, now.AddMinutes(1), ct);
            Assert.Equal(first.FloorOf(view), second.FloorOf(view));
            Assert.Equal(expected, second.CeilingOf(view));
        }
        finally
        {
            await ExecAsync(connection, $"DROP OWNED BY {role}", CancellationToken.None);
            await ExecAsync(connection, $"DROP ROLE {role}", CancellationToken.None);
        }
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp, true)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
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
