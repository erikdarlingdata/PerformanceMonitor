/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4300: live pins for the raw purge gate's PER-SLOT source floor — <c>collect.query_stats</c>'s two
/// consumers (<see cref="TimescaleSupport.QueryStatsIntervalHourlyView"/>, the query-grain successor, and
/// <see cref="TimescaleSupport.QueryStatsDbIntervalHourlyView"/>, the db-grain successor added by #4423) admit
/// different rows: the db-grain successor's own CREATE excludes any row with a NULL
/// <c>delta_worker_time</c>, while the query-grain successor's does not. Before this fix, both slots were
/// judged against ONE shared, filtered <c>source_oldest</c>, so a hole at raw's oldest hour that only the
/// db-grain successor's filter rejects could hold the WHOLE gate Short forever even once the query-grain
/// successor caught all the way up.
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RawGatePerSlotSourceFloorLiveTests
{
    private const string Raw = "query_stats";
    private const int SetupTimeoutSeconds = 60;

    /// <summary>Anchor hour, aligned to a bucket boundary, ten hours back — far enough back that every
    /// seeded row and every refreshed bucket falls comfortably inside this test's own probe window.</summary>
    private static readonly DateTime AnchorHour = TimescaleSupport.AlignDown(DateTime.UtcNow.AddHours(-10), TimeSpan.FromHours(1));

    private static async Task<(NpgsqlConnection Connection, ScratchPostgres Scratch)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4300 per-slot floor pins.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);

        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxAutoPrepare = 0 }.ConnectionString;
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4300 per-slot floor pins need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, default);

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await stop.ExecuteNonQueryAsync();
        }

        return (connection, scratch);
    }

    /// <summary>Hour 0 (the raw floor) holds ONLY CPU-unknown rows: <c>delta_worker_time = NULL</c>,
    /// <c>sample_interval_seconds = 60</c> so the interval-honest filter admits them, and a real
    /// <c>delta_execution_count</c>/<c>delta_elapsed_time</c> — exactly the shape #4423's collector writes for
    /// a query it could time but not attribute worker time to. Hours 1-3 hold normal rows with a real
    /// <c>delta_worker_time</c>, so both successors' own filters admit them.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection)
    {
        await using var seedFloor = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    -1,
    $1::timestamp + (n || ' minutes')::interval,
    1,
    'probe-per-slot-floor',
    'ProbeDb',
    decode(md5('per-slot-floor-null-' || n), 'hex'),
    decode(md5('per-slot-floor-null-h-' || n), 'hex'),
    NULL, 500, 3, 60
FROM generate_series(0, 50, 10) AS n", connection) { CommandTimeout = SetupTimeoutSeconds };
        seedFloor.Parameters.AddWithValue(DateTime.SpecifyKind(AnchorHour, DateTimeKind.Unspecified));
        await seedFloor.ExecuteNonQueryAsync();

        await using var seedNormal = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    -1,
    $1::timestamp + (n || ' hours')::interval,
    1,
    'probe-per-slot-floor',
    'ProbeDb',
    decode(md5('per-slot-floor-normal-' || n), 'hex'),
    decode(md5('per-slot-floor-normal-h-' || n), 'hex'),
    100, 500, 3, 60
FROM generate_series(1, 3) AS n", connection) { CommandTimeout = SetupTimeoutSeconds };
        seedNormal.Parameters.AddWithValue(DateTime.SpecifyKind(AnchorHour, DateTimeKind.Unspecified));
        await seedNormal.ExecuteNonQueryAsync();
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection) { CommandTimeout = SetupTimeoutSeconds };
        refresh.Parameters.AddWithValue(DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync();
    }

    /// <summary>The floor case (the bug): raw's oldest hour holds ONLY CPU-unknown rows. Both successors are
    /// refreshed over the whole seeded range. The db-grain successor's own filter (<c>delta_worker_time IS NOT
    /// NULL</c>) can never admit that hour, so its own floor starts at hour 1 — the first normal hour — and it
    /// covers that. Before this fix, the db-grain slot was judged against the SHARED,
    /// interval-honest-only floor (hour 0, since <c>sample_interval_seconds = 60</c> passes that filter), which
    /// the db-grain successor can never materialize, holding the whole gate Short forever. RED on
    /// <c>b476262be</c> (dev before this fix): the assertion below reads Short there.</summary>
    [Fact]
    public async Task FloorCase_DbSlotOwnFloorSkipsCpuUnknownHour_Covered()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection);
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, AnchorHour.AddHours(-1), AnchorHour.AddHours(4));
            await RefreshAsync(connection, TimescaleSupport.QueryStatsDbIntervalHourlyView, AnchorHour.AddHours(-1), AnchorHour.AddHours(4));

            var safe = await TimescaleSupport.IsRawTierDropSafeAsync(connection, Raw, default);
            Assert.True(safe, "the db-grain successor's own filter never admits the CPU-unknown floor hour, so its own floor starts at the first normal hour and it must read Covered, not Short");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
            await connection.DisposeAsync();
        }
    }

    /// <summary>The query-grain case: same seed, but the query-grain successor is deliberately NOT
    /// materialized over the CPU-unknown floor hour (refreshed from hour 1 onward only). The query-grain
    /// successor's own filter (interval-honest only) DOES admit the NULL-worker-time rows, so its own floor is
    /// the CPU-unknown hour itself — and since that hour is the one hole left open, the gate must read Short.
    /// Refreshing that one hour then flips it to Covered, proving the fix did not simply loosen the gate: the
    /// query-grain slot is still held to its OWN floor, not waved through by the db-grain slot's looser one.</summary>
    [Fact]
    public async Task QueryGrainCase_HoleAtOwnFloor_ShortThenCovered()
    {
        var (connection, scratch) = await OpenAsync();
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection);

            /* Query-grain successor refreshed from hour 1 only — the CPU-unknown floor hour (hour 0) is left
               a hole for THIS successor, even though its own filter would admit those rows. */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, AnchorHour.AddHours(1), AnchorHour.AddHours(4));
            await RefreshAsync(connection, TimescaleSupport.QueryStatsDbIntervalHourlyView, AnchorHour.AddHours(-1), AnchorHour.AddHours(4));

            var shortSafe = await TimescaleSupport.IsRawTierDropSafeAsync(connection, Raw, default);
            Assert.False(shortSafe, "the query-grain successor's own filter admits the CPU-unknown floor hour, so leaving that hour unmaterialized for it must read Short, not Covered");

            /* Materialize the missing hour and re-probe: now Covered. */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, AnchorHour.AddHours(-1), AnchorHour.AddHours(4));

            var coveredSafe = await TimescaleSupport.IsRawTierDropSafeAsync(connection, Raw, default);
            Assert.True(coveredSafe, "once the query-grain successor materializes its own floor hour, the gate must read Covered");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
            await connection.DisposeAsync();
        }
    }
}
