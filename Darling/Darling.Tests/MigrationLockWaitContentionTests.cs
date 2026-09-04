/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2894 residual 2: what the migration advisory-lock wait does when it runs out, under REAL contention.
///
/// <para><b>Why these are live-store tests and not source pins.</b> The behaviour under test is a
/// negotiation between two PostgreSQL sessions over a session-scoped advisory lock — a second holder
/// returning FALSE from <c>pg_try_advisory_lock</c>, a budget being spent against a live backend, a stamp
/// table read on the way out. None of that has a fake that would have caught anything: the defect this
/// replaces was a client-side timeout whose failure text described a socket, and only a real socket
/// produces that text. <c>MigrationDataMovingRungCensusPins</c> covers the constant's DERIVATION from the
/// rung census; this covers what the constant DOES.</para>
///
/// <para><b>The budget is shortened and nothing else.</b> These go through
/// <c>PgMigrations.TryAcquireMigrationLockForTestsAsync</c>, which is
/// <c>TryAcquireMigrationLockAsync</c> with the wait budget as a parameter — the poll cadence, the expiry
/// split and the messages are the shipped ones. A test cannot wait out the real
/// <c>MigrationLockWaitTimeoutSeconds</c>, and a test that mocked the clock instead would stop exercising
/// the lock.</para>
///
/// <para><b>The two outcomes are the point.</b> Expiry is not one answer. Against a store already at
/// <c>StorageVersion.SchemaVersion</c> there is nothing to apply, the lock was only ever wanted to
/// establish that, and giving up is correct — the case that strands an instance in practice is an orphaned
/// backend still holding the lock after the client that took it is gone, and the store is perfectly
/// current underneath it. Against a store BELOW that version there are rungs to apply and no safe way to
/// apply them, so it must throw. A test that only covered one half would pass on a build that always did
/// that half.</para>
///
/// <para><b>Teardown goes through <c>LiveStoreCleanup.RunOwnedAsync</c>, not <c>RunAsync</c></b> (#1902).
/// <c>RunAsync</c> opens its own connection, which is the safer default and is wrong here: the release is
/// <c>pg_advisory_unlock</c>, and only the session HOLDING a session-scoped advisory lock can release it,
/// so a fresh connection would report success while leaving the lock held for every later class in the
/// collection. The stamp restore rides the same session because that is the one whose <c>search_path</c>
/// the applier set. <c>bodySucceeded</c> is the last statement of each <c>try</c> so a throw from the
/// teardown stays silent while the body's own exception is in flight.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class MigrationLockWaitContentionTests
{
    /// <summary>
    /// Short enough that the whole class runs in seconds, long enough to be several
    /// <c>MigrationLockPollIntervalSeconds</c> so the loop actually loops rather than testing a single
    /// attempt with extra steps.
    /// </summary>
    private const int TestWaitBudgetSeconds = 3;

    /// <summary>
    /// Ceiling for an UNCONTENDED acquire. Two-sided, and the upper side is what does the work: the
    /// shipped poll interval is one second, so any ceiling below a second separates "took the lock on the
    /// first attempt" from "slept once first" — which matters because moving the sleep above the first
    /// attempt is a RELOCATION, and every occurrence count stays 1, so no structural check sees it and
    /// only elapsed time can. The lower side is headroom: the operation is a single round trip on a
    /// connection that is already open and already migrated, which is sub-millisecond against a local
    /// store, so 750 ms leaves roughly three orders of magnitude before a slow runner can flake it.
    /// </summary>
    private static readonly TimeSpan UncontendedAcquireCeiling = TimeSpan.FromMilliseconds(750);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AcquireSucceedsFirstAttempt_WhenNobodyHoldsTheLock_AgainstDevPostgres()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live migration-lock tests.");

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        /* The uncontended path must cost nothing: polling is only a fallback shape, and if the first
           attempt did not succeed outright then every ordinary service start pays a poll interval. */
        var started = Stopwatch.StartNew();
        Assert.True(await PgMigrations.TryAcquireMigrationLockForTestsAsync(
            connection, logger: null, TestWaitBudgetSeconds, TestContext.Current.CancellationToken));
        started.Stop();

        /* Release in a finally, not after the assertion. Npgsql pools by default, so disposing the
           connection hands the physical session — advisory lock and all — back to the pool rather than
           closing it, and a lock leaked by a failed assertion then blocks every later class in this
           collection until the pool prunes that connection. Measured: a leak here cost a sibling test
           290 s of waiting on a lock nothing was using. */
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
                () => ReleaseAsync(connection, CancellationToken.None));
        }
    }

    [Fact]
    public async Task ExpiryReturnsFalseWithoutThrowing_WhenTheStoreIsAlreadyCurrent_AgainstDevPostgres()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live migration-lock tests.");

        await using var holder = new NpgsqlConnection(connectionString);
        await holder.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(holder, TestContext.Current.CancellationToken);
        await HoldAsync(holder, TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            await using var waiter = new NpgsqlConnection(connectionString);
            await waiter.OpenAsync(TestContext.Current.CancellationToken);

            /* What the give-up path must not do is CHANGE anything: returning false means "nothing to
               apply", so the ladder's stamps have to come back untouched. Snapshot both the high-water
               mark and the row count — the count catches a duplicate or removed stamp that leaves MAX
               alone.

               Note what is deliberately NOT asserted here: that the waiter did not also take the lock.
               PostgreSQL will not grant the same session-scoped advisory lock twice while the holder
               below still owns it, so that assertion could never fail, and an assertion that cannot fail
               is worse than no assertion — it reads as coverage. Mutual exclusion at this point is the
               database's guarantee, not this code's.

               Read through the HOLDER: MigrateAsync set that session's search_path, so the bare stamp
               table name resolves without depending on the best-effort database-default search_path
               having been grantable on this store. */
            var versionBefore = await ReadMaxVersionAsync(holder, TestContext.Current.CancellationToken);
            var stampsBefore = await ReadStampCountAsync(holder, TestContext.Current.CancellationToken);

            /* The store is current — MigrateAsync above brought it there — so the waiter has no rung to
               apply and must come back FALSE rather than taking its caller out of the collection loop. */
            Assert.False(await PgMigrations.TryAcquireMigrationLockForTestsAsync(
                waiter, logger: null, TestWaitBudgetSeconds, TestContext.Current.CancellationToken));

            Assert.Equal(versionBefore, await ReadMaxVersionAsync(holder, TestContext.Current.CancellationToken));
            Assert.Equal(stampsBefore, await ReadStampCountAsync(holder, TestContext.Current.CancellationToken));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(
                bodySucceeded,
                () => ReleaseAsync(holder, CancellationToken.None));
        }
    }

    [Fact]
    public async Task ExpiryThrowsNamingBothVersions_WhenTheStoreIsBehind_AgainstDevPostgres()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live migration-lock tests.");

        await using var holder = new NpgsqlConnection(connectionString);
        await holder.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(holder, TestContext.Current.CancellationToken);

        /* Make the store look mid-ladder by removing the top stamp, so the expiry path sees rungs still to
           apply. Restored in the finally: the rung itself was applied and stays applied, and re-stamping
           is what a real re-run would do. Naive UTC via AT TIME ZONE, matching how the applier stamps
           and every other `timestamp` column here: `now()::timestamp` would truncate in the session's
           TimeZone GUC, which nothing pins to UTC. */
        var topVersion = StorageVersion.SchemaVersion;

        /* The setup needs the store to sit exactly where this build sits, so that removing one stamp
           puts it BELOW. A store that is ahead (an older build pointed at a newer store, which is a
           legitimate state) cannot be pushed below by removing this build's top stamp, and a store
           that is behind is already mid-ladder for other reasons — either way, skip rather than fail,
           because neither says anything about the code under test. */
        var maxBefore = await ReadMaxVersionAsync(holder, TestContext.Current.CancellationToken);
        Assert.SkipWhen(
            maxBefore != topVersion,
            $"This store reports schema v{maxBefore} but this build knows v{topVersion}; the "
            + "behind-store expiry test needs them equal to stage a mid-ladder store.");

        var topName = await ReadStampNameAsync(holder, topVersion, TestContext.Current.CancellationToken);
        Assert.NotNull(topName);

        /* Everything that mutates the store happens INSIDE the try, so the finally puts it back no matter
           which assertion fails. Staging the store first and opening the try afterwards leaves a failed
           run's store un-restored for every later test in the collection — which is exactly how the first
           version of this class poisoned its own re-runs. */
        var bodySucceeded = false;
        try
        {
            await ExecuteAsync(
                holder,
                "DELETE FROM darling_schema_version WHERE version = $1",
                TestContext.Current.CancellationToken,
                topVersion);

            /* Read what the store now reports rather than assuming top-1: the ladder is not dense (V45 is
               permanently absent), so the version below the top is a fact to look up, not to compute. */
            var storeVersion = await ReadMaxVersionAsync(holder, TestContext.Current.CancellationToken);
            Assert.True(storeVersion < topVersion, "The stamp removal did not move the store's reported version.");

            await HoldAsync(holder, TestContext.Current.CancellationToken);

            await using var waiter = new NpgsqlConnection(connectionString);
            await waiter.OpenAsync(TestContext.Current.CancellationToken);

            var thrown = await Assert.ThrowsAsync<TimeoutException>(
                () => PgMigrations.TryAcquireMigrationLockForTestsAsync(
                    waiter, logger: null, TestWaitBudgetSeconds, TestContext.Current.CancellationToken));

            /* The one production caller logs ex.Message and nothing else, so the message IS the diagnostic.
               The shape it replaced said "Exception while reading from stream" — true of a socket and
               useless about a lock. Assert the facts an operator needs to act: that it was the migration
               advisory lock, where the store is, where the build is, and how to find the holder. */
            Assert.Contains("advisory lock", thrown.Message, StringComparison.Ordinal);
            Assert.Contains("v" + storeVersion.ToString(CultureInfo.InvariantCulture), thrown.Message, StringComparison.Ordinal);
            Assert.Contains("v" + topVersion.ToString(CultureInfo.InvariantCulture), thrown.Message, StringComparison.Ordinal);
            Assert.Contains("pg_locks", thrown.Message, StringComparison.Ordinal);

            /* The negative is the whole point of the change, so it gets a positive control through the
               identical form — a DoesNotContain that can only ever pass proves nothing. */
            Assert.DoesNotContain("reading from stream", thrown.Message, StringComparison.Ordinal);
            Assert.Contains("reading from stream", "Exception while reading from stream", StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await ReleaseAsync(holder, CancellationToken.None);
                await ExecuteAsync(
                    holder,
                    "INSERT INTO darling_schema_version (version, name, applied_at) "
                    + "VALUES ($1, $2, now() AT TIME ZONE 'UTC') ON CONFLICT (version) DO NOTHING",
                    CancellationToken.None,
                    topVersion,
                    topName!);
            });
        }
    }

    [Fact]
    public async Task WaitingIsCancellable_WithoutRelyingOnACancelRequestReachingTheBackend_AgainstDevPostgres()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live migration-lock tests.");

        await using var holder = new NpgsqlConnection(connectionString);
        await holder.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(holder, TestContext.Current.CancellationToken);
        await HoldAsync(holder, TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            await using var waiter = new NpgsqlConnection(connectionString);
            await waiter.OpenAsync(TestContext.Current.CancellationToken);

            /* Shutdown during a wait has to be prompt and must surface as cancellation, because the one
               production caller re-throws OperationCanceledException rather than logging it as a store
               failure. The sleep between attempts is what makes this a plain token wait instead of a
               cancel request racing a backend asleep on a lock.

               The budget is two orders of magnitude past the token's lifetime so that the token, not
               the budget, is what ends the wait — a budget near 250 ms would pass whether or not
               cancellation works. It is 60 s rather than the shipped 1500 s because the failure mode
               of a regression here is a HANG, and a minute is long enough to prove the point while
               capping what a broken build costs every CI run. */
            using var cancelSoon = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => PgMigrations.TryAcquireMigrationLockForTestsAsync(
                    waiter, logger: null, waitBudgetSeconds: 60, cancelSoon.Token));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(
                bodySucceeded,
                () => ReleaseAsync(holder, CancellationToken.None));
        }
    }

    private static async Task HoldAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        Assert.True(await TryLockOnceAsync(connection, cancellationToken),
            "Could not take the migration advisory lock to set up contention — something else on this "
            + "store is already holding it.");
    }

    private static async Task<bool> TryLockOnceAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand("SELECT pg_try_advisory_lock($1)", connection) { CommandTimeout = 30 };
        command.Parameters.AddWithValue(PgMigrations.MigrationLockKeyForTests);
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    private static async Task ReleaseAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand("SELECT pg_advisory_unlock($1)", connection) { CommandTimeout = 30 };
        command.Parameters.AddWithValue(PgMigrations.MigrationLockKeyForTests);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Binds rather than concatenates. The staging statements interpolate a version number and a rung
    /// name, both internal, but hand-rolled quote doubling in a test is a pattern the next test copies —
    /// and the rest of this class already parameterizes.
    /// </summary>
    private static async Task ExecuteAsync(
        NpgsqlConnection connection, string sql, CancellationToken cancellationToken, params object[] parameters)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 30 };
        foreach (var parameter in parameters)
        {
            command.Parameters.AddWithValue(parameter);
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<int> ReadMaxVersionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            "SELECT COALESCE(MAX(version), 0) FROM darling_schema_version", connection) { CommandTimeout = 30 };
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    private static async Task<long> ReadStampCountAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            "SELECT COUNT(*) FROM darling_schema_version", connection) { CommandTimeout = 30 };
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    private static async Task<string?> ReadStampNameAsync(NpgsqlConnection connection, int version, CancellationToken cancellationToken)
    {
        using var command = new NpgsqlCommand(
            "SELECT name FROM darling_schema_version WHERE version = $1", connection) { CommandTimeout = 30 };
        command.Parameters.AddWithValue(version);
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }
}
