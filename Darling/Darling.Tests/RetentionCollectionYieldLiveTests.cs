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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5592's live test: a real purge, through the product's own sweep, whose wall budget runs out part-way through a
/// table. The pass has to leave the rest of that table's rows, say in its run record that it stopped on its budget,
/// and leave the next pass to take the rows that are left. The clock is derived from the rows deleted so far (20 s
/// per 1,000), so the stop lands on the same batch on every run. Gated on <c>DARLING_TEST_PG</c>; CI sets it, a bare
/// machine skips.
/// </summary>
/* #1776 own-store: mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection. */
public sealed class RetentionCollectionYieldLiveTests
{
    private const int SeededRows = 5_000;
    private const int BatchRows = 1_000;
    private const string MapTable = "collect.query_store_plan_map";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class NeverBehind : ICollectionPressure
    {
        public string? BehindReason() => null;
    }

    private sealed class MustNotBeRead : ICollectionPressure
    {
        public string? BehindReason() => throw new InvalidOperationException("an unpaced pass read the collection signal");
    }

    private static async Task SeedExpiredMapRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var expiredStamp = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified).AddDays(-400);
        await using var seed = new NpgsqlCommand(
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) " +
            "SELECT 1, 'db', gs, ('\\x' || lpad(to_hex(gs), 8, '0'))::bytea, 'h' || gs, $2 " +
            "FROM generate_series(1, $1) gs", connection);
        seed.Parameters.AddWithValue(SeededRows);
        seed.Parameters.AddWithValue(expiredStamp);
        await seed.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> MapRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var count = new NpgsqlCommand($"SELECT COUNT(*) FROM {MapTable}", connection);
        return (long)(await count.ExecuteScalarAsync(ct))!;
    }

    private static RetentionWalPacer NoWaitPacer() =>
        new(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask);

    [Fact]
    public async Task ABudgetStoppedPass_LeavesTheRemainingRows_AndTheNextPassRemovesThem()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #5592 live budget test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedExpiredMapRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Fake time that follows the work: 20 s per 1,000 rows deleted from the map. A 50 s budget is spent after the
           third full batch (60 s), so the fourth batch is the one the budget stops. Every table before the map is
           empty, so it costs no time, and every table after it is not reached. */
        double WorkClock()
        {
            using var count = new NpgsqlCommand($"SELECT COUNT(*) FROM {MapTable}", connection);
            var remaining = (long)count.ExecuteScalar()!;
            return (SeededRows - remaining) / (double)BatchRows * 20;
        }

        var pacerOne = NoWaitPacer();
        pacerOne.Yield = new RetentionCollectionYield(
            new NeverBehind(), TimeSpan.FromSeconds(50), logger: null,
            delay: (_, _) => Task.CompletedTask, secondsClock: WorkClock);
        var firstLog = new CapturingTestLogger();

        var first = await DarlingRetention.PurgeWithPacerAsync(
            postgres, timescaleAvailable: false, firstLog, ct,
            retentionDaysFor: null, planContentRetentionDays: 0,
            livenessTouchedTablePruneRowCap: BatchRows, walPacer: pacerOne);

        Assert.Equal(3 * BatchRows, first.RowsDeleted);
        Assert.Equal(SeededRows - 3 * BatchRows, await MapRowsAsync(connection, ct));
        Assert.True(pacerOne.Yield.StoppedOnBudget, firstLog.Joined);
        Assert.Equal("collect.query_store_plan_map", pacerOne.Yield.FirstStoppedTable);
        Assert.Contains(firstLog.Lines, l => l.Contains("Retention purge stopped at its", StringComparison.Ordinal)
            && l.Contains("query_store_plan_map", StringComparison.Ordinal));

        await using (var record = new NpgsqlCommand(
            "SELECT error_message FROM collect.collection_log WHERE collector_name = $1 ORDER BY collection_time DESC LIMIT 1",
            connection))
        {
            record.Parameters.AddWithValue("data_retention");
            var text = (string?)await record.ExecuteScalarAsync(ct);
            Assert.NotNull(text);
            Assert.Contains("time budget in collect.query_store_plan_map", text, StringComparison.Ordinal);
            Assert.Contains("the next pass continues from the rows left", text, StringComparison.Ordinal);
        }

        /* The next pass: a fresh gate with room to finish. It takes exactly the rows that were left. */
        var pacerTwo = NoWaitPacer();
        pacerTwo.Yield = new RetentionCollectionYield(
            new NeverBehind(), TimeSpan.FromHours(1), logger: null,
            delay: (_, _) => Task.CompletedTask);
        var secondLog = new CapturingTestLogger();

        var second = await DarlingRetention.PurgeWithPacerAsync(
            postgres, timescaleAvailable: false, secondLog, ct,
            retentionDaysFor: null, planContentRetentionDays: 0,
            livenessTouchedTablePruneRowCap: BatchRows, walPacer: pacerTwo);

        Assert.Equal(SeededRows - 3 * BatchRows, second.RowsDeleted);
        Assert.Equal(0L, await MapRowsAsync(connection, ct));
        Assert.False(pacerTwo.Yield.StoppedOnBudget, secondLog.Joined);
    }

    /// <summary>
    /// A paced <see cref="DarlingRetention.PurgeAsync"/> with a signal and a spent budget attaches the rule to the
    /// pacer it builds: nothing is deleted and the record says why. The same call with <c>paceWal</c> false ignores the
    /// signal altogether and deletes everything, without reading it (the unpaced callers keep today's behavior).
    /// </summary>
    [Fact]
    public async Task PurgeAsync_AttachesTheRuleOnAPacedRun_AndIgnoresItOnAnUnpacedOne()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #5592 live wiring test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedExpiredMapRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var log = new CapturingTestLogger();

        /* Paced, budget already spent: the first table is not started, so nothing is deleted. */
        var stopped = await DarlingRetention.PurgeAsync(
            postgres, timescaleAvailable: false, log, ct,
            retentionDaysFor: null, planContentRetentionDays: 0,
            paceWal: true, collectionPressure: new NeverBehind(), wallBudget: TimeSpan.Zero);

        /* The caveats prune is a direct statement, not a paced batch, so the budget does not gate it. It is the one table
           this pass purged: every gated table the spent budget kept from starting is taken back out of the count (#5595
           L4: without that subtraction the count is the 80-odd tables the loop walked, and without the entry skip the
           record would say the pass stopped "in" the first table instead of before it). */
        Assert.Equal(0, stopped.RowsDeleted);
        Assert.Equal((long)SeededRows, await MapRowsAsync(connection, ct));
        Assert.Equal(1, stopped.TablesPurged);
        Assert.Contains(log.Lines, l => l.Contains("Retention purge stopped at its", StringComparison.Ordinal)
            && l.Contains("time budget before ", StringComparison.Ordinal));

        /* Unpaced: the same signal and budget are ignored, and the signal is never read. */
        var unpaced = await DarlingRetention.PurgeAsync(
            postgres, timescaleAvailable: false, log, ct,
            retentionDaysFor: null, planContentRetentionDays: 0,
            paceWal: false, collectionPressure: new MustNotBeRead(), wallBudget: TimeSpan.Zero);

        Assert.Equal(SeededRows, unpaced.RowsDeleted);
        Assert.Equal(0L, await MapRowsAsync(connection, ct));
    }
}
