/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4611 end-to-end against a REAL TimescaleDB: once a caller already holds a <see cref="RollupCoverage"/>
/// with a ceiling for the Query Store corrected hourly view, <see cref="QueryStoreTrendRouting.ResolveAsync(RollupCoverage,NpgsqlDataSource,System.Threading.CancellationToken)"/>
/// issues NO fresh <c>min(bucket)</c>/<c>max(bucket)</c> read against that view, and it reaches the same
/// routing decision the uncached probe-and-read overload would for the same store.
///
/// <para>Counted through <c>pg_stat_statements</c> against the actual product call path, the same
/// discipline <c>StoreSizeCacheLiveTests</c> uses, rather than a mock standing in for the store.</para>
///
/// <para><b>#1776 own-store</b> — mints its own scratch database through <see cref="ScratchPostgres"/>.</para>
/// </summary>
public sealed class QueryStoreTrendRoutingCachedLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -461100;

    [Fact]
    public async Task ResolveAsync_WithACachedCeiling_IssuesNoBoundsProbe_AndMatchesTheFallbackRoute()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4611 cached-route test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using (var probe = new NpgsqlConnection(baseConnectionString))
        {
            await probe.OpenAsync(ct);
            await using var preloadCmd = new NpgsqlCommand("SELECT current_setting('shared_preload_libraries')", probe);
            var preload = (string)(await preloadCmd.ExecuteScalarAsync(ct))!;
            var loaded = preload.Split(',').Select(s => s.Trim()).Contains("pg_stat_statements");
            Assert.SkipWhen(!loaded,
                $"pg_stat_statements is not in shared_preload_libraries ('{preload}') - this rig's postgresql.conf must carry it in shared_preload_libraries to run this pin.");
        }

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using (var extCmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", connection))
        {
            await extCmd.ExecuteNonQueryAsync(ct);
        }

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, TestServerId, "qs-trend-cached-e2e", ct);

        var hour10 = new DateTime(2026, 3, 4, 10, 0, 0, DateTimeKind.Unspecified);
        var hour11 = hour10.AddHours(1);
        var hour12 = hour10.AddHours(2);

        await SeedSnapshotAsync(connection, intervalId: 4200, queryId: 80, when: hour10.AddMinutes(5), executionCount: 5L, ct);
        await SeedSnapshotAsync(connection, intervalId: 4201, queryId: 81, when: hour11.AddMinutes(5), executionCount: 7L, ct);

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
        {
            await using var remove = new NpgsqlCommand(
                $"SELECT remove_continuous_aggregate_policy('collect.{view}', if_exists => true)", connection);
            await remove.ExecuteNonQueryAsync(ct);
        }

        await using (var refreshInterval = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView}', $1::timestamp, $2::timestamp)", connection))
        {
            refreshInterval.Parameters.AddWithValue(hour10);
            refreshInterval.Parameters.AddWithValue(hour12);
            await refreshInterval.ExecuteNonQueryAsync(ct);
        }

        await using (var refreshCorrected = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStoreStatsCorrectedHourlyView}', $1::timestamp, $2::timestamp)", connection))
        {
            refreshCorrected.Parameters.AddWithValue(hour10);
            refreshCorrected.Parameters.AddWithValue(hour12);
            await refreshCorrected.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* ── the fallback route, measured first as the reference answer ── */
        var fallbackRoute = await QueryStoreTrendRouting.ResolveAsync(postgres, ct);
        Assert.True(fallbackRoute.UseRollup);

        /* ── a coverage snapshot already carrying the ceiling, the way both real callers hold one ── */
        var availability = await TimescaleSupport.DetectRollupsAsync(postgres, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, availability, ct);
        Assert.NotNull(coverage.CeilingOf(TimescaleSupport.QueryStoreStatsCorrectedHourlyView));

        long dbId;
        await using (var oidConn = new NpgsqlConnection(scratch.ConnectionString))
        {
            await oidConn.OpenAsync(ct);
            using var oidCmd = new NpgsqlCommand("SELECT oid FROM pg_database WHERE datname = current_database()", oidConn);
            dbId = (uint)(await oidCmd.ExecuteScalarAsync(ct))!;

            using var resetCmd = new NpgsqlCommand("SELECT pg_stat_statements_reset(0, $1, 0)", oidConn);
            resetCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
            await resetCmd.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            /* ── THE PIN: the cached-first overload, called the same way the MCP reader and the desktop
                   viewer call it ── */
            var cachedRoute = await QueryStoreTrendRouting.ResolveAsync(coverage, postgres, ct);

            Assert.Equal(fallbackRoute.UseRollup, cachedRoute.UseRollup);
            Assert.Equal(fallbackRoute.RawStartUtc, cachedRoute.RawStartUtc);
            Assert.Equal(fallbackRoute.RollupFloorUtc, cachedRoute.RollupFloorUtc);

            long boundsCalls;
            await using (var countConn = new NpgsqlConnection(scratch.ConnectionString))
            {
                await countConn.OpenAsync(ct);
                using var countCmd = new NpgsqlCommand(
                    "SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements WHERE dbid = $1 AND query LIKE '%min(bucket)%max(bucket)%'",
                    countConn);
                countCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
                boundsCalls = (long)(await countCmd.ExecuteScalarAsync(ct))!;
            }

            Assert.Equal(0, boundsCalls);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task SeedSnapshotAsync(
        NpgsqlConnection connection, long intervalId, long queryId, DateTime when, long executionCount, System.Threading.CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
VALUES
    ((extract(epoch FROM $1)::bigint * 100000) + $2, $1, $3, 'qs-trend-cached-e2e', 'RoutingDb', 'dbo.GetOrders', '0xROUTE',
     $2, $2, 'Regular', 'PRIMARY', $2, $1, $1, $4, 100, 100, 900, 400)";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(when);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(executionCount);
        await command.ExecuteNonQueryAsync(ct);
    }
}
