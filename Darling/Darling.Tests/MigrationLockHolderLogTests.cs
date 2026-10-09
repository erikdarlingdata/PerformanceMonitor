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
    public async Task ASessionHoldingACollectTableLock_IsNamedInTheLog_WithItsPidStateAndQuery_AgainstDevPostgres()
    {
        var connectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live lock-holder log test.");

        await using var observer = new NpgsqlConnection(connectionString);
        await observer.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(observer, TestContext.Current.CancellationToken);

        await using var holder = new NpgsqlConnection(connectionString);
        await holder.OpenAsync(TestContext.Current.CancellationToken);
        var holderPid = holder.ProcessID;
        var transaction = await holder.BeginTransactionAsync(TestContext.Current.CancellationToken);

        var bodySucceeded = false;
        try
        {
            const string HolderQuery = "LOCK TABLE collect.darling_schema_version IN SHARE UPDATE EXCLUSIVE MODE /* 5582 holder */";
            await using (var hold = new NpgsqlCommand(HolderQuery, holder, transaction))
            {
                await hold.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            PgMigrations.ResetLockHolderLogThrottleForTests();
            var logger = new CapturingLogger();
            await PgMigrations.LogLockHoldersAsync(observer, logger, 173, "the rung", "canceling statement due to lock timeout", TestContext.Current.CancellationToken);

            var line = Assert.Single(logger.Lines);
            Assert.Contains("V173", line, StringComparison.Ordinal);
            Assert.Contains($"pid {holderPid} ", line, StringComparison.Ordinal);
            Assert.Contains("state idle in transaction", line, StringComparison.Ordinal);
            Assert.Contains("darling_schema_version (ShareUpdateExclusiveLock)", line, StringComparison.Ordinal);
            Assert.Contains(HolderQuery, line, StringComparison.Ordinal);
            Assert.DoesNotContain($"pid {observer.ProcessID} ", line, StringComparison.Ordinal);

            /* The second call inside the ten minutes says nothing. */
            await PgMigrations.LogLockHoldersAsync(observer, logger, 173, "the rung", "again", TestContext.Current.CancellationToken);
            Assert.Single(logger.Lines);

            /* A fault in the diagnostic is swallowed: a closed connection must not throw out of the log call. */
            PgMigrations.ResetLockHolderLogThrottleForTests();
            await using var closed = new NpgsqlConnection(connectionString);
            await PgMigrations.LogLockHoldersAsync(closed, new CapturingLogger(), 173, "the rung", "closed", TestContext.Current.CancellationToken);
            bodySucceeded = true;
        }
        finally
        {
            PgMigrations.ResetLockHolderLogThrottleForTests();
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () => await transaction.RollbackAsync(CancellationToken.None));
        }
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
