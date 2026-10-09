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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582 review round 1 (M2): when a rung hits its <c>lock_timeout</c> (SQLSTATE 55P03), the applier rolls the rung back and logs who
/// holds a blocking lock on a <c>collect</c> table, so an operator can see what keeps the store-migrate loop retrying while collection
/// waits behind it. The log is throttled to once per ten minutes, and a fault in the diagnostic never replaces the rung's own failure.
/// </summary>
[Collection("live-postgres")]
public sealed class MigrationLockHolderLogTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheThrottle_ClaimsOnce_ThenAgainAfterTenMinutes()
    {
        /* No try/finally: a failed assertion here leaves a claimed throttle, and the live test below resets it before it reads. */
        PgMigrations.ResetLockHolderLogThrottleForTests();
        var t0 = new DateTime(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);
        Assert.True(PgMigrations.TryClaimLockHolderLog(t0));
        Assert.False(PgMigrations.TryClaimLockHolderLog(t0.AddMinutes(1)));
        Assert.False(PgMigrations.TryClaimLockHolderLog(t0.AddMinutes(9).AddSeconds(59)));
        Assert.True(PgMigrations.TryClaimLockHolderLog(t0.AddMinutes(10)));
        Assert.False(PgMigrations.TryClaimLockHolderLog(t0.AddMinutes(10).AddSeconds(1)));
        Assert.Equal(TimeSpan.FromMinutes(10), PgMigrations.LockHolderLogInterval);
        PgMigrations.ResetLockHolderLogThrottleForTests();
    }

    [Fact]
    public void TheApplier_RollsBackOn55P03_LogsTheHolders_AndRethrows()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs").Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = source.IndexOf("private static async Task<int> MigrateLockedAsync", StringComparison.Ordinal);
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        var applier = source[start..end];
        var catchAt = applier.IndexOf("catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)", StringComparison.Ordinal);
        Assert.True(catchAt > 0, "the 55P03 catch around the rung apply is gone");
        var rollback = applier.IndexOf("await TryRollbackAsync(transaction);", catchAt, StringComparison.Ordinal);
        var log = applier.IndexOf("await LogLockHoldersAsync(connection, logger,", catchAt, StringComparison.Ordinal);
        var rethrow = applier.IndexOf("throw;", catchAt, StringComparison.Ordinal);
        Assert.True(rollback > catchAt && log > rollback && rethrow > log, "roll back, then log the holders, then rethrow");
        Assert.True(catchAt > applier.IndexOf("new NpgsqlCommand(migration.Sql, connection, transaction)", StringComparison.Ordinal),
            "the catch belongs to the rung apply, not to the stamp");
    }

    [Fact]
    public async Task ASessionHoldingACollectTableLock_IsNamedInTheLog_WithItsFields_AndNoQueryText_AgainstDevPostgres()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-holder log test.");

        await using var observer = new NpgsqlConnection(connectionString);
        await observer.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(observer, TestContext.Current.CancellationToken);

        var holderConnectionString = new NpgsqlConnectionStringBuilder(connectionString) { ApplicationName = "lock-holder-5582" }.ConnectionString;
        await using var holder = new NpgsqlConnection(holderConnectionString);
        await holder.OpenAsync(TestContext.Current.CancellationToken);
        var holderPid = holder.ProcessID;
        var transaction = await holder.BeginTransactionAsync(TestContext.Current.CancellationToken);

        await using var reader = new NpgsqlConnection(holderConnectionString);
        await reader.OpenAsync(TestContext.Current.CancellationToken);
        var readerPid = reader.ProcessID;
        var readerTransaction = await reader.BeginTransactionAsync(TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            const string HolderQuery = "LOCK TABLE collect.darling_schema_version IN SHARE UPDATE EXCLUSIVE MODE /* 5582 holder */";
            await using (var hold = new NpgsqlCommand(HolderQuery, holder, transaction))
            {
                await hold.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            /* A plain SELECT holds ACCESS SHARE, which blocks the ACCESS EXCLUSIVE lock of most schema changes. */
            const string ReaderQuery = "SELECT 1 FROM collect.darling_schema_version /* 5582 reader */";
            await using (var read = new NpgsqlCommand(ReaderQuery, reader, readerTransaction))
            {
                await read.ExecuteScalarAsync(TestContext.Current.CancellationToken);
            }

            PgMigrations.ResetLockHolderLogThrottleForTests();
            var logger = new CapturingLogger();
            await PgMigrations.LogLockHoldersAsync(observer, logger, 173, "the rung", "canceling statement due to lock timeout", TestContext.Current.CancellationToken);

            var line = Assert.Single(logger.Lines);
            Assert.Contains("V173", line, StringComparison.Ordinal);
            Assert.Contains($"pid {holderPid} (client backend), application lock-holder-5582, state idle in transaction, transaction started ", line, StringComparison.Ordinal);
            Assert.Contains("waiting on Client: ClientRead", line, StringComparison.Ordinal);
            Assert.Contains("darling_schema_version (ShareUpdateExclusiveLock)", line, StringComparison.Ordinal);
            Assert.Contains($"pid {readerPid} ", line, StringComparison.Ordinal);
            Assert.Contains("darling_schema_version (AccessShareLock)", line, StringComparison.Ordinal);
            Assert.DoesNotContain($"pid {observer.ProcessID} ", line, StringComparison.Ordinal);

            /* A client session's query text is never logged. The line also lists any autovacuum worker that holds a lock on a collect
               table while the test runs, and that worker's entry carries its first 200 characters of query text by design (#5617), so
               "query:" is checked per entry, not on the whole line. */
            Assert.DoesNotContain("5582 holder", line, StringComparison.Ordinal);
            Assert.DoesNotContain("5582 reader", line, StringComparison.Ordinal);
            var entries = SplitHolderEntries(line);
            Assert.Contains(entries, e => e.StartsWith($"pid {holderPid} ", StringComparison.Ordinal));
            Assert.Contains(entries, e => e.StartsWith($"pid {readerPid} ", StringComparison.Ordinal));
            foreach (var entry in entries)
            {
                if (entry.StartsWith($"pid {holderPid} ", StringComparison.Ordinal) || entry.StartsWith($"pid {readerPid} ", StringComparison.Ordinal))
                {
                    Assert.DoesNotContain("query:", entry, StringComparison.Ordinal);
                }

                if (entry.Contains("query:", StringComparison.Ordinal))
                {
                    Assert.Contains("(autovacuum worker)", entry, StringComparison.Ordinal);
                }
            }

            /* The lookup's SET LOCAL limits end with its transaction and do not leak into the caller's session. */
            await using (var show = new NpgsqlCommand("SHOW statement_timeout", observer))
            {
                Assert.NotEqual("5s", (string?)await show.ExecuteScalarAsync(TestContext.Current.CancellationToken));
            }

            /* The second call inside the ten minutes says nothing. */
            await PgMigrations.LogLockHoldersAsync(observer, logger, 173, "the rung", "again", TestContext.Current.CancellationToken);
            Assert.Single(logger.Lines);

            /* A fault in the diagnostic is swallowed: a closed connection must not throw out of the log call. */
            PgMigrations.ResetLockHolderLogThrottleForTests();
            await using var closed = new NpgsqlConnection(connectionString);
            var failed = new CapturingLogger();
            await PgMigrations.LogLockHoldersAsync(closed, failed, 173, "the rung", "closed", TestContext.Current.CancellationToken);
            Assert.Empty(failed.Lines);

            /* A failed lookup does not claim the ten minutes: the next attempt on a working connection still logs. */
            await PgMigrations.LogLockHoldersAsync(observer, failed, 173, "the rung", "after the failure", TestContext.Current.CancellationToken);
            Assert.Single(failed.Lines);
            bodySucceeded = true;
        }
        finally
        {
            PgMigrations.ResetLockHolderLogThrottleForTests();
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await transaction.RollbackAsync(CancellationToken.None);
                await readerTransaction.RollbackAsync(CancellationToken.None);
            });
        }
    }

    /// <summary>Splits the holders part of a lock-holder log line into its entries, one per session (#5617). An entry starts
    /// <c>pid N (</c>; splitting on that start, not on every semicolon, keeps a semicolon inside an autovacuum query in its entry.</summary>
    private static string[] SplitHolderEntries(string line)
    {
        const string Marker = "Sessions holding a lock on a collect table: ";
        var at = line.IndexOf(Marker, StringComparison.Ordinal);
        Assert.True(at >= 0, "the lock-holder log line lost its holders list");
        return System.Text.RegularExpressions.Regex.Split(line[(at + Marker.Length)..], @"; (?=pid \d+ \()");
    }

    [Fact]
    public async Task ACancelDuringTheLookup_Propagates_AndLeavesTheThrottleUnclaimed_AgainstDevPostgres()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-holder log test.");

        await using var observer = new NpgsqlConnection(connectionString);
        await observer.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(observer, TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            PgMigrations.ResetLockHolderLogThrottleForTests();
            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();
            var logger = new CapturingLogger();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                PgMigrations.LogLockHoldersAsync(observer, logger, 173, "the rung", "shutdown", cancelled.Token));
            Assert.Empty(logger.Lines);
            Assert.True(PgMigrations.IsLockHolderLogDue(DateTime.UtcNow), "a cancelled lookup must not claim the ten minutes");
            bodySucceeded = true;
        }
        finally
        {
            PgMigrations.ResetLockHolderLogThrottleForTests();
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheLookupLimits_AreValidSetLocalStatements_AgainstDevPostgres()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-holder log test.");

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
        var bodySucceeded = false;
        try
        {
            await using (var limits = new NpgsqlCommand(PgMigrations.LockHolderLookupLimitsSql, connection, transaction))
            {
                await limits.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            await using (var show = new NpgsqlCommand("SELECT current_setting('statement_timeout') || '/' || current_setting('lock_timeout')", connection, transaction))
            {
                Assert.Equal("5s/2s", (string?)await show.ExecuteScalarAsync(TestContext.Current.CancellationToken));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () => await transaction.RollbackAsync(CancellationToken.None));
        }
    }

    [Fact]
    public void TheLookup_RunsInItsOwnTransactionWithLimits_ListsEveryMode_AndReadsQueryTextOnlyForAutovacuum()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs").Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = source.IndexOf("internal static async Task LogLockHoldersAsync(", StringComparison.Ordinal);
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        var body = source[start..end];

        var begin = body.IndexOf("connection.BeginTransactionAsync(", StringComparison.Ordinal);
        var limits = body.IndexOf("LockHolderLookupLimitsSql", StringComparison.Ordinal);
        var query = body.IndexOf("new NpgsqlCommand(LockHoldersSql, connection, lookup)", StringComparison.Ordinal);
        var commit = body.IndexOf("lookup.CommitAsync(", StringComparison.Ordinal);
        Assert.True(begin > 0 && limits > begin && query > limits && commit > query, "begin, set the limits, run the lookup, commit");
        Assert.Contains("limits.ExecuteNonQueryAsync(", body[limits..query], StringComparison.Ordinal);

        var claim = body.IndexOf("TryClaimLockHolderLog(", StringComparison.Ordinal);
        Assert.True(claim > commit, "the ten minutes are claimed after the lookup worked, not before it runs");
        Assert.Contains("ex is not OperationCanceledException and not OutOfMemoryException", body, StringComparison.Ordinal);

        Assert.DoesNotContain("l.mode IN", PgMigrations.LockHoldersSql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN a.backend_type = 'autovacuum worker' THEN left(a.query, 200) END", PgMigrations.LockHoldersSql, StringComparison.Ordinal);
        Assert.DoesNotContain("a.query", PgMigrations.LockHoldersSql.Replace("CASE WHEN a.backend_type = 'autovacuum worker' THEN left(a.query, 200) END", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
            {
                Lines.Add(formatter(state, exception));
            }
        }
    }
}
