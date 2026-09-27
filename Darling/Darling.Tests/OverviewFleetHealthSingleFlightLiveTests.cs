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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4477: an Overview refresh with N server cards used to issue N per-server fleet-health round trips
/// (each card's lane called <c>GetFleetCollectionHealthByServerAsync</c>, and every one of them raced the
/// same cold 20-second memo — a production session measured 40 store round trips in 4.5 minutes, one per
/// card per refresh). <see cref="ViewerDataService.GetFleetCollectionHealthByServerAsync"/> now gates a cold
/// cache behind a single in-flight <see cref="System.Threading.Tasks.Task"/>, so every racing caller inside
/// one refresh awaits the SAME fetch instead of starting its own.
///
/// <para>Its own scratch database (<c>#1776 own-store</c>).</para>
/// </summary>
public sealed class OverviewFleetHealthSingleFlightLiveTests
{
    private const int ServerCount = 12;
    private const int ServerIdBase = -447700;
    private static long s_logId = 447700_000;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The statement-count pin: N concurrent callers racing a cold cache (the Overview loader's per-card
    /// lanes) produce exactly ONE fleet-wide read, not N — counted through <c>pg_stat_statements</c>, scoped
    /// to this scratch database, against the actual product call path
    /// (<see cref="ViewerDataService.GetFleetCollectionHealthByServerAsync"/>), never a helper standing in
    /// for it.
    /// </summary>
    [Fact]
    public async Task ConcurrentOverviewCards_ShareOneFleetWideRead_NotOnePerCard()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live Overview fleet-health single-flight test.");

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

        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);

            for (var i = 0; i < ServerCount; i++)
            {
                await RegisterServerAsync(setup, ServerIdBase - i, ct);
                await InsertLogRowAsync(setup, ServerIdBase - i, ct);
            }

            using var extCmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", setup);
            await extCmd.ExecuteNonQueryAsync(ct);
        }

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
        ViewerDataService? service = null;
        try
        {
            service = new ViewerDataService(scratch.ConnectionString);

            /* The equivalence half: one server's fleet-served row must match a direct raw scan of the same
               server's rows exactly — the fleet-wide statement must not answer a different question than
               the per-server one it replaces. */
            var byServer = await service.GetFleetCollectionHealthByServerAsync(ct);
            Assert.True(byServer.TryGetValue(ServerIdBase, out var fleetRows), "the fleet-wide read must carry the seeded server's rows.");
            Assert.Single(fleetRows!);
            Assert.Equal(1, fleetRows![0].TotalRuns);
            Assert.Equal(1, fleetRows[0].SuccessCount);

            /* Reset again so the equivalence read above doesn't count toward the concurrency pin. */
            await using (var resetConn = new NpgsqlConnection(scratch.ConnectionString))
            {
                await resetConn.OpenAsync(ct);
                using var resetCmd = new NpgsqlCommand("SELECT pg_stat_statements_reset(0, $1, 0)", resetConn);
                resetCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
                await resetCmd.ExecuteNonQueryAsync(ct);
            }

            /* THE PIN: N racing callers (the per-card lanes of one Overview refresh) through the product's
               own call path, hitting a cold cache together. */
            var racers = Enumerable.Range(0, ServerCount)
                .Select(_ => service.GetFleetCollectionHealthByServerAsync(ct))
                .ToArray();
            await Task.WhenAll(racers);

            long calls;
            await using (var countConn = new NpgsqlConnection(scratch.ConnectionString))
            {
                await countConn.OpenAsync(ct);
                using var countCmd = new NpgsqlCommand(
                    "SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements WHERE dbid = $1 AND query LIKE '%v_collection_log%' AND query LIKE '%last_zero_row_streak_break_time%'",
                    countConn);
                countCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
                calls = (long)(await countCmd.ExecuteScalarAsync(ct))!;
            }

            Assert.Equal(1, calls);

            bodySucceeded = true;
        }
        finally
        {
            if (service is not null)
            {
                await service.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var deleteLog = new NpgsqlCommand("DELETE FROM collection_log WHERE server_id <= $1", cleanup);
                deleteLog.Parameters.AddWithValue(ServerIdBase);
                await deleteLog.ExecuteNonQueryAsync(cleanupCt);

                using var deleteServers = new NpgsqlCommand("DELETE FROM servers WHERE server_id <= $1", cleanup);
                deleteServers.Parameters.AddWithValue(ServerIdBase);
                await deleteServers.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("overview-fleet-health-4477-" + serverId);
        command.Parameters.AddWithValue("overview-fleet-health-4477-" + serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertLogRowAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", connection);
        command.Parameters.AddWithValue(System.Threading.Interlocked.Increment(ref s_logId));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("overview-fleet-health-4477-" + serverId);
        command.Parameters.AddWithValue("wait_stats");
        command.Parameters.AddWithValue("SUCCESS");
        command.Parameters.AddWithValue(10);
        command.Parameters.AddWithValue(5);
        await command.ExecuteNonQueryAsync(ct);
    }
}
