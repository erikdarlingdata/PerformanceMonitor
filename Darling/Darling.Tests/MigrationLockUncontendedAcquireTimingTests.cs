/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres. */

/// <summary>
/// #5602: the one check in <see cref="MigrationLockWaitContentionTests"/> that judges a clock, in the <c>timing</c>
/// collection so it runs alone, after the parallel classes.
///
/// <para><b>Why it left the contention class.</b> That class sits in <c>live-postgres</c> (its other facts hold the real
/// migration lock on the shared store), and a <c>live-postgres</c> class runs in parallel with every non-live class, so a
/// 750 ms bar against a one second poll sleep was judged against the runner's load. This fact needs no shared store: the
/// advisory lock is per database, so a scratch database of its own has nobody to contend with, and the class can join
/// <c>timing</c> (<c>TimingCollection</c> does not carry the shared-store fixture).</para>
///
/// <para><b>What it keeps.</b> The same two-sided bar. The shipped poll interval is one second, so a ceiling below a
/// second separates "took the lock on the first attempt" from "slept once first" — which matters because moving the sleep
/// above the first attempt is a RELOCATION, and every occurrence count stays 1, so no structural check sees it and only
/// elapsed time can. The operation is a single round trip on a connection that is already open and already migrated, which
/// is sub-millisecond against a local store, so 750 ms leaves roughly three orders of magnitude of headroom.</para>
/// </summary>
[Collection("timing")]
public sealed class MigrationLockUncontendedAcquireTimingTests
{
    /// <summary>Same budget the contention class uses: long enough to be several poll intervals, so a slept-first build would have time to loop.</summary>
    private const int TestWaitBudgetSeconds = 3;

    private static readonly TimeSpan UncontendedAcquireCeiling = TimeSpan.FromMilliseconds(750);

    [Fact]
    public async Task AcquireSucceedsFirstAttempt_WhenNobodyHoldsTheLock_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live migration-lock tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The uncontended path must cost nothing: polling is only a fallback shape, and if the first
           attempt did not succeed outright then every ordinary service start pays a poll interval. */
        var started = Stopwatch.StartNew();
        Assert.True(await PgMigrations.TryAcquireMigrationLockForTestsAsync(
            connection, logger: null, TestWaitBudgetSeconds, ct));
        started.Stop();

        /* Release in a finally, not after the assertion: Npgsql pools, so disposing the connection hands the physical
           session, advisory lock and all, back to the pool rather than closing it. */
        var bodySucceeded = false;
        try
        {
            Assert.True(
                started.Elapsed < UncontendedAcquireCeiling,
                $"An uncontended acquire took {started.Elapsed.TotalMilliseconds:F0}ms, over the "
                + $"{UncontendedAcquireCeiling.TotalMilliseconds:F0}ms ceiling — it slept a poll interval "
                + "instead of taking the lock on its first attempt.");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(
                bodySucceeded,
                async () =>
                {
                    using var command = new NpgsqlCommand("SELECT pg_advisory_unlock($1)", connection) { CommandTimeout = 30 };
                    command.Parameters.AddWithValue(PgMigrations.MigrationLockKeyForTests);
                    await command.ExecuteNonQueryAsync(CancellationToken.None);
                });
        }
    }
}
