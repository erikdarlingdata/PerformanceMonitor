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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for <see cref="IntervalRollupCountGuard.QueryStatsSql"/> (#4605): the per-(server, hour) raw count,
/// restart rows excluded, matches the hourly rollup's <c>sum(sample_count)</c> after a refresh, and a stray raw row
/// that the rollup has not seen makes the guard return a nonzero count.
///
/// <para><b>#1776 own-store</b>: mints a scratch database because it materializes a continuous aggregate, so it is
/// deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class IntervalRollupCountGuardLiveTests
{
    private const int ServerA = -944101;
    private const int ServerB = -944102;
    private const int ServerC = -944103;
    private const string NameA = "GuardServerA";
    private const string NameB = "GuardServerB";
    private const string NameC = "GuardServerC";

    /// <summary>Fixed anchor, never wall-clock relative: three whole hours, [H1, H2).</summary>
    private static readonly DateTime H1 = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime H2 = H1.AddHours(3);

    [Fact]
    public async Task TheGuard_CountsNonRestartRowsPerServerHour_AndAgreesWithTheRefreshedRollup()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live count-guard test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live count-guard test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, NameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, NameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerC, NameC, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var bodySucceeded = false;
        try
        {
            /* Two servers over three whole hours: ordinary rows, restart rows, one NULL-interval row. */
            for (var h = 0; h < 3; h++)
            {
                await PlantAsync(connection, ct, ServerA, NameA, H1.AddHours(h).AddMinutes(5), 3600, "0xGA1");
                await PlantAsync(connection, ct, ServerA, NameA, H1.AddHours(h).AddMinutes(25), 3600, "0xGA2");
                await PlantAsync(connection, ct, ServerB, NameB, H1.AddHours(h).AddMinutes(15), 3600, "0xGB1");
            }

            await PlantAsync(connection, ct, ServerA, NameA, H1.AddMinutes(45), 0, "0xGA1");
            await PlantAsync(connection, ct, ServerB, NameB, H1.AddHours(2).AddMinutes(45), 0, "0xGB2");
            await PlantAsync(connection, ct, ServerB, NameB, H1.AddHours(1).AddMinutes(35), null, "0xGB3");
            /* A row exactly at the exclusive end: outside [H1, H2) and outside the refresh. */
            await PlantAsync(connection, ct, ServerA, NameA, H2, 3600, "0xGA1");

            await RefreshAsync(connection, H1, H2, ct);
            var scope = new[] { NameA, NameB };

            Assert.Equal(0L, await GuardAsync(connection, scope, ct));
            Assert.Equal(0L, await GuardAsync(connection, null, ct));

            /* A NULL-interval row counts on the raw side exactly as it counts in the rollup. */
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM collect.query_stats WHERE server_id = -944102 AND sample_interval_seconds IS NULL", ct));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT sum(sample_count) FROM collect.query_stats_interval_hourly WHERE server_id = -944102 AND query_hash = '0xGB3'", ct));

            /* One more restart row in a middle hour: the rollup excludes it and so does the guard. */
            await PlantAsync(connection, ct, ServerB, NameB, H1.AddHours(1).AddMinutes(55), 0, "0xGB2");
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));

            /* One more non-restart row in a middle hour, after the refresh: one pair now disagrees. */
            await PlantAsync(connection, ct, ServerA, NameA, H1.AddHours(1).AddMinutes(50), 3600, "0xGA1");
            Assert.Equal(1L, await GuardAsync(connection, scope, ct));
            await RefreshAsync(connection, H1, H2, ct);
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));

            /* A third server, outside the scope array: invisible with the array, counted with NULL. */
            await PlantAsync(connection, ct, ServerC, NameC, H1.AddHours(1).AddMinutes(10), 3600, "0xGC1");
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));
            Assert.True(await GuardAsync(connection, null, ct) >= 1L);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    private static async Task<long> GuardAsync(NpgsqlConnection connection, string[]? servers, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(IntervalRollupCountGuard.QueryStatsSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = H1 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = H2 });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = servers is null ? DBNull.Value : servers,
        });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, int? intervalSeconds, string queryHash)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'GuardDb', $5, $5, 1000, 900, 1, $6)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DarlingMcpTestData.TruncateToSeconds(at) });
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
        insert.Parameters.AddWithValue(queryHash);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = intervalSeconds.HasValue ? intervalSeconds.Value : DBNull.Value });
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }
}
