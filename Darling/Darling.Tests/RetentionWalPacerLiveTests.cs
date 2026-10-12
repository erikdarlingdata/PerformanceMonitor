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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4823's live test: a real purge, through the product's own sweep, with a pacer whose rate is far below what
/// the deletes write, so the run has to measure WAL and has to wait. The delay is injected and never sleeps.
/// Gated on <c>DARLING_TEST_PG</c>; CI sets it, a bare machine skips.
/// </summary>
/* #1776 own-store: mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection. */
public sealed class RetentionWalPacerLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// Seeds expired rows in <c>collect.query_store_plan_map</c>, purges with a 1 KiB/s pacer, and checks that
    /// the purge measured WAL, waited (in waits of at most 30 s), still deleted every expired row, and said so
    /// in its summary line.
    /// </summary>
    [Fact]
    public async Task PurgeWithPacer_MeasuresTheWalItWrites_AndWaitsForIt()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4823 live pacing test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var expiredStamp = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified).AddDays(-400);

        await using (var seed = new NpgsqlCommand(
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) " +
            "SELECT 1, 'db', gs, ('\\x' || lpad(to_hex(gs), 8, '0'))::bytea, 'h' || gs, $2 " +
            "FROM generate_series(1, $1) gs", connection))
        {
            seed.Parameters.AddWithValue(3_000);
            seed.Parameters.AddWithValue(expiredStamp);
            await seed.ExecuteNonQueryAsync(ct);
        }

        var waits = new List<TimeSpan>();
        var pacer = new RetentionWalPacer(
            rateBytesPerSecond: 1_024,
            delay: (wait, _) => { waits.Add(wait); return Task.CompletedTask; });

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var purgeLog = new CapturingTestLogger();

        await DarlingRetention.PurgeWithPacerAsync(
            postgres, timescaleAvailable: false, purgeLog, ct,
            retentionDaysFor: null, planContentRetentionDays: 0,
            livenessTouchedTablePruneRowCap: DarlingRetention.LivenessTouchedTablePruneRowCap,
            walPacer: pacer);

        await using var remaining = new NpgsqlCommand("SELECT COUNT(*) FROM collect.query_store_plan_map", connection);
        Assert.Equal(0L, (long)(await remaining.ExecuteScalarAsync(ct))!);

        Assert.True(pacer.TotalWalBytes > 0, "the purge measured no WAL. " + purgeLog.Joined);
        Assert.True(pacer.TotalWaitSeconds > 0, "the purge never waited. " + purgeLog.Joined);
        Assert.NotEmpty(waits);
        Assert.All(waits, wait => Assert.InRange(wait.TotalSeconds, 0.0, 30.0));

        Assert.Contains(
            purgeLog.Lines,
            line => line.Contains("Retention purge:", StringComparison.Ordinal)
                && line.Contains("WAL", StringComparison.Ordinal));
    }
}
