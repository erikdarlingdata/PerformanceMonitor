/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for <see cref="IntervalRollupCountGuard.QueryStatsSql"/> (#4605): the guard compares the hour ledger
/// (<see cref="QueryStatsHourLedger"/>), not a raw scan, with the hourly rollup's <c>sum(sample_count)</c> per
/// (server, hour), and returns <c>-1</c> for a window the ledger does not cover. Every fact seeds raw with direct INSERTs
/// (which write no ledger row), then plays the writer's part with <see cref="QueryStatsHourLedger.RecountSql"/>.
///
/// <para>The cases: a match passes; a pair the rollup lacks fails; a ledger row nothing else has fails; a pair the ledger
/// lacks fails; restart rows are excluded on both sides; the window start is inclusive and the end exclusive on the ledger
/// side too; a window that starts before <c>counted_since</c> or a store with no state row returns -1, even when a
/// count would also disagree.</para>
///
/// <para><b>#1776 own-store</b>: mints a scratch database because it materializes a continuous aggregate, so it is
/// deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class IntervalRollupCountGuardLiveTests
{
    private const int ServerA = -944101;
    private const int ServerB = -944102;
    private const int ServerC = -944103;
    private const int ServerGhost = -944104;
    private const string NameA = "GuardServerA";
    private const string NameB = "GuardServerB";
    private const string NameC = "GuardServerC";
    private const string NameGhost = "GuardServerGhost";

    /// <summary>Fixed anchor, never wall-clock relative: three whole hours, [H1, H2).</summary>
    private static readonly DateTime H1 = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime H2 = H1.AddHours(3);

    [Fact]
    public async Task TheGuard_ComparesTheLedgerWithTheRefreshedRollup_PerServerHour_AndReturnsMinusOneWhenTheLedgerDoesNotCoverTheWindow()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live count-guard test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PrepareStoreAsync(connection, ct);

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
            /* A server and hour with a restart row and nothing else: neither side has a row for it. */
            await PlantAsync(connection, ct, ServerC, NameC, H1.AddHours(2).AddMinutes(20), 0, "0xGC0");
            /* A row exactly at the exclusive end: outside [H1, H2) and outside the refresh, but the recount below gives the
               ledger a row for hour H2, so the guard's end bound has something to exclude on the ledger side. */
            await PlantAsync(connection, ct, ServerA, NameA, H2, 3600, "0xGA1");

            await RefreshAsync(connection, H1, H2, ct);
            await RecountAsync(connection, ct);
            /* counted_since equal to the window start covers the window: the guard's test is <=, not <. */
            await QueryStatsLedgerSeed.SetCountedSinceAsync(connection, H1, ct);
            var scope = new[] { NameA, NameB };

            Assert.Equal(0L, await GuardAsync(connection, scope, ct));
            Assert.Equal(0L, await GuardAsync(connection, null, ct));

            /* What the ledger holds: restart rows are not counted, a NULL interval is, and a restart-only pair has no row. */
            Assert.Equal(2L, await ScalarAsync(connection, LedgerN(ServerA, H1), ct));
            Assert.Equal(2L, await ScalarAsync(connection, LedgerN(ServerB, H1.AddHours(1)), ct));
            Assert.Equal(1L, await ScalarAsync(connection, LedgerN(ServerB, H1.AddHours(2)), ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger WHERE server_id = -944103", ct));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM collect.query_stats WHERE server_id = -944102 AND sample_interval_seconds IS NULL", ct));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT sum(sample_count) FROM collect.query_stats_interval_hourly WHERE server_id = -944102 AND query_hash = '0xGB3'", ct));

            /* The ledger holds a row for hour H2 (the recount covered it), and the guard still ignores it: the end is exclusive. */
            Assert.Equal(1L, await ScalarAsync(connection, LedgerN(ServerA, H2), ct));

            /* One more restart row in a middle hour: neither side counts it, so a recount leaves the ledger as it was. */
            await PlantAsync(connection, ct, ServerB, NameB, H1.AddHours(1).AddMinutes(55), 0, "0xGB2");
            await RecountAsync(connection, ct);
            Assert.Equal(2L, await ScalarAsync(connection, LedgerN(ServerB, H1.AddHours(1)), ct));
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));

            /* One more non-restart row in a middle hour, after the refresh, counted into the ledger the way the writer
               counts it: the ledger is ahead of the rollup, and one pair disagrees. */
            await PlantAsync(connection, ct, ServerA, NameA, H1.AddHours(1).AddMinutes(50), 3600, "0xGA1");
            await RecountAsync(connection, ct);
            Assert.Equal(1L, await GuardAsync(connection, scope, ct));
            await RefreshAsync(connection, H1, H2, ct);
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));

            /* A third server, outside the scope array, whose pair the rollup has not seen: a pair the rollup lacks. Invisible
               with the array, counted with NULL. */
            await PlantAsync(connection, ct, ServerC, NameC, H1.AddHours(1).AddMinutes(10), 3600, "0xGC1");
            await RecountAsync(connection, ct);
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));
            Assert.Equal(1L, await GuardAsync(connection, null, ct));
            await RefreshAsync(connection, H1, H2, ct);
            Assert.Equal(0L, await GuardAsync(connection, null, ct));

            /* Start is inclusive. One row exactly at H1, planted after the refresh and counted: the ledger has it (>= $1), the
               rollup does not yet, so exactly one pair disagrees. A ledger side written as > $1 would not read it, and the
               guard would read 0 here. Then refresh and it matches again. */
            await PlantAsync(connection, ct, ServerB, NameB, H1, 3600, "0xGB4");
            await RecountAsync(connection, ct);
            Assert.Equal(1L, await GuardAsync(connection, scope, ct));
            await RefreshAsync(connection, H1, H2, ct);
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));

            /* A ledger row nothing else has (a server and hour with no raw rows and no rollup row) fails, but only in scope. */
            await ExecuteAsync(connection,
                $"INSERT INTO collect.query_stats_hour_ledger (server_id, server_name, bucket, n) VALUES ({ServerGhost}, '{NameGhost}', '2026-01-05 01:00:00', 5)", ct);
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));
            Assert.Equal(1L, await GuardAsync(connection, null, ct));
            Assert.Equal(1L, await GuardAsync(connection, new[] { NameGhost }, ct));

            /* A ledger count above the rollup's fails, and a recount heals both: it sets the pair to raw's count and deletes
               the ghost row, which has no raw rows. */
            await ExecuteAsync(connection, $"UPDATE collect.query_stats_hour_ledger SET n = n + 1 WHERE server_id = {ServerA} AND bucket = '2026-01-05 01:00:00'", ct);
            Assert.Equal(1L, await GuardAsync(connection, scope, ct));
            Assert.Equal(2L, await GuardAsync(connection, null, ct));
            await RecountAsync(connection, ct);
            Assert.Equal(0L, await GuardAsync(connection, null, ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats_hour_ledger WHERE server_id = -944104", ct));

            /* A pair the ledger lacks (a writer that did not count) fails: the rollup has rows the ledger never counted. */
            await ExecuteAsync(connection, $"DELETE FROM collect.query_stats_hour_ledger WHERE server_id = {ServerA} AND bucket = '2026-01-05 02:00:00'", ct);
            Assert.Equal(1L, await GuardAsync(connection, scope, ct));
            await RecountAsync(connection, ct);
            Assert.Equal(0L, await GuardAsync(connection, scope, ct));

            /* A window that starts before counted_since is not covered: -1, never a pass, and never a count even when a
               count would disagree too. A window that starts at or after it is judged as ever. */
            await QueryStatsLedgerSeed.SetCountedSinceAsync(connection, H1.AddHours(1), ct);
            Assert.Equal(-1L, await GuardAsync(connection, scope, ct));
            Assert.Equal(-1L, await GuardAsync(connection, null, ct));
            Assert.Equal(0L, await GuardAsync(connection, scope, ct, H1.AddHours(1), H2));
            await ExecuteAsync(connection,
                $"INSERT INTO collect.query_stats_hour_ledger (server_id, server_name, bucket, n) VALUES ({ServerGhost}, '{NameGhost}', '2026-01-05 02:00:00', 5)", ct);
            Assert.Equal(-1L, await GuardAsync(connection, null, ct));
            Assert.Equal(1L, await GuardAsync(connection, null, ct, H1.AddHours(1), H2));
            await ExecuteAsync(connection, $"DELETE FROM collect.query_stats_hour_ledger WHERE server_id = {ServerGhost}", ct);
            await QueryStatsLedgerSeed.SetCountedSinceAsync(connection, H1, ct);
            Assert.Equal(0L, await GuardAsync(connection, null, ct));

            /* No state row: nothing says where counting began, so the window is not covered either. */
            await ExecuteAsync(connection, "DELETE FROM collect.query_stats_hour_ledger_state", ct);
            Assert.Equal(-1L, await GuardAsync(connection, scope, ct));
            Assert.Equal(-1L, await GuardAsync(connection, null, ct));

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

    /// <summary>Migrates the scratch store, makes the hourly rollup, stops the TimescaleDB scheduler and registers the three servers.</summary>
    private static async Task PrepareStoreAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live count-guard test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, NameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, NameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerC, NameC, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
    }

    /// <summary>The guard over <c>[from, to)</c>, the three-hour window by default.</summary>
    private static async Task<long> GuardAsync(NpgsqlConnection connection, string[]? servers, CancellationToken ct, DateTime? from = null, DateTime? to = null)
    {
        await using var command = new NpgsqlCommand(IntervalRollupCountGuard.QueryStatsSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = from ?? H1 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = to ?? H2 });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = servers is null ? DBNull.Value : servers,
        });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    /// <summary>Plays the writer's part for every hour the facts plant into, the hour at the exclusive end included.</summary>
    private static Task RecountAsync(NpgsqlConnection connection, CancellationToken ct) =>
        QueryStatsLedgerSeed.RecountAsync(connection, H1, H2.AddHours(1), ct);

    private static string LedgerN(int serverId, DateTime bucket) =>
        string.Create(CultureInfo.InvariantCulture,
            $"SELECT n FROM collect.query_stats_hour_ledger WHERE server_id = {serverId} AND bucket = '{bucket:yyyy-MM-dd HH:mm:ss}'");

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
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
