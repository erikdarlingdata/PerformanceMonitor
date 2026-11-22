/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.QueryStoreIntervalRetentionTestKit;
using static PerformanceMonitor.Darling.Storage.QueryStoreBackgroundIndexes;

namespace Darling.Tests;

/// <summary>
/// #9313: the partitioned background index build used to end the whole spec when one leaf's ATTACH timed out behind a
/// writer. These tests hold a write transaction open on one leaf and decide the timing through the attempt hook, never a
/// sleep: the hook runs before every attempt, so a writer is started before an attempt and committed before a later one.
/// The table is a small three-leaf list-partitioned one in its own database, the lock_timeout is 200 ms and the backoff is zero.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreAttachRetryLiveTests
{
    private const string Parent = "public.t9313";
    private const string IndexName = "public.ix_t9313_k";

    private static readonly IndexSpec Spec = new(
        IndexName, Parent, "unused", "unused", IndexDefinition: "(id)");

    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Lines.Add(formatter(state, exception));
    }

    /* A second connection whose open transaction holds a lock on one leaf that its child index ATTACH has to wait behind. */
    private sealed class Writer : IAsyncDisposable
    {
        private readonly NpgsqlConnection _connection;
        private NpgsqlTransaction? _transaction;

        private Writer(NpgsqlConnection connection) => _connection = connection;

        public static async Task<Writer> OpenAsync(string connectionString, CancellationToken ct)
        {
            var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            return new Writer(connection);
        }

        public async Task HoldAsync(string leaf, CancellationToken ct, string mode = "ACCESS EXCLUSIVE", string? thenRun = null)
        {
            _transaction = await _connection.BeginTransactionAsync(ct);
            await using var hold = new NpgsqlCommand($"LOCK TABLE {leaf} IN {mode} MODE;", _connection, _transaction);
            await hold.ExecuteNonQueryAsync(ct);
            if (thenRun is not null)
            {
                await using var run = new NpgsqlCommand(thenRun, _connection, _transaction);
                await run.ExecuteNonQueryAsync(ct);
            }
        }

        public async Task ReleaseAsync(CancellationToken ct)
        {
            if (_transaction is not null)
            {
                await _transaction.CommitAsync(ct);
                await _transaction.DisposeAsync();
                _transaction = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_transaction is not null)
            {
                await _transaction.RollbackAsync();
                await _transaction.DisposeAsync();
            }

            await _connection.DisposeAsync();
        }
    }

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "CREATE TABLE public.t9313 (id int NOT NULL, k int NOT NULL) PARTITION BY LIST (k);", ct);
        await ExecAsync(connection, "CREATE TABLE public.t9313_a PARTITION OF public.t9313 FOR VALUES IN (1);", ct);
        await ExecAsync(connection, "CREATE TABLE public.t9313_b PARTITION OF public.t9313 FOR VALUES IN (2);", ct);
        await ExecAsync(connection, "CREATE TABLE public.t9313_c PARTITION OF public.t9313 FOR VALUES IN (3);", ct);
        await ExecAsync(connection, "INSERT INTO public.t9313 SELECT g, 1 + g % 3 FROM generate_series(1, 300) AS g;", ct);
    }

    private static LockRetryPlan Plan(Func<LockAttempt, CancellationToken, Task> beforeAttempt) =>
        new(5, TimeSpan.FromMilliseconds(200), _ => TimeSpan.Zero, beforeAttempt);

    private static async Task<long> AttachedLeavesAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await CountAsync(connection, $"SELECT count(*) FROM pg_inherits WHERE inhparent = '{IndexName}'::regclass", ct);

    private static async Task<bool> ParentValidAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await CountAsync(connection, $"SELECT count(*) FROM pg_index WHERE indexrelid = '{IndexName}'::regclass AND indisvalid", ct) == 1;

    [Fact]
    public void TheProductionPlan_IsTenAttemptsOfOneSecond_WithTheBackoffBetweenThem()
    {
        var plan = LockRetryPlan.Production;
        Assert.Equal(10, plan.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(1), plan.LockTimeout);
        Assert.Equal(new[] { 2, 4, 8, 15, 15, 15 }, Enumerable.Range(1, 6).Select(n => (int)plan.BackoffAfterAttempt(n).TotalSeconds));
    }

    [Fact]
    public async Task AWriteHeldOnOneLeafAtTheFirstAttach_IsRetriedInTheSameStart_AndTheParentEndsValid()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);
        await using var writer = await Writer.OpenAsync(scratch.ConnectionString, ct);

        /* The last leaf is the one held: the build of a leaf waits for every older write transaction, so a writer opened
           before a later leaf's build would stall it. The writer starts after the held leaf's child exists, which is when
           its insert takes a lock on that child. */
        var attempts = new List<LockAttempt>();
        var logger = new CapturingLogger();
        await EnsurePartitionedAsync(connection, Spec, logger, ct, null, Plan(async (attempt, token) =>
        {
            attempts.Add(attempt);
            if (attempt.Statement == "ATTACH PARTITION" && attempt.Leaf == "t9313_c")
            {
                if (attempt.Attempt == 1)
                {
                    await writer.HoldAsync("t9313_c", token);
                }
                else
                {
                    await writer.ReleaseAsync(token);
                }
            }
        }));

        Assert.Equal(3L, await AttachedLeavesAsync(connection, ct));
        Assert.True(await ParentValidAsync(connection, ct));
        Assert.Equal(2, attempts.Count(a => a.Leaf == "t9313_c" && a.Statement == "ATTACH PARTITION"));
        Assert.Single(logger.Lines, line => line.Contains("attempt 1 of 5", StringComparison.Ordinal) && line.Contains("retrying in 0s", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ALeafHeldThroughAllItsAttempts_DoesNotStopTheLaterLeaves_AndAttachesInTheComeBackPass()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);

        /* Leaves b and c already carry a valid child (an earlier start built them), so the held first leaf does not stall a
           later leaf's build behind the open writer. */
        await ExecAsync(connection, "CREATE INDEX ix_t9313_k_b ON public.t9313_b (id);", ct);
        await ExecAsync(connection, "CREATE INDEX ix_t9313_k_c ON public.t9313_c (id);", ct);
        await using var writer = await Writer.OpenAsync(scratch.ConnectionString, ct);

        var attempts = new List<LockAttempt>();
        await EnsurePartitionedAsync(connection, Spec, new CapturingLogger(), ct, null, Plan(async (attempt, token) =>
        {
            attempts.Add(attempt);
            if (attempt.Statement == "ATTACH PARTITION" && attempt.Leaf == "t9313_a")
            {
                if (attempt.Pass == 1 && attempt.Attempt == 1)
                {
                    await writer.HoldAsync("t9313_a", token);
                }
                else if (attempt.Pass == 2)
                {
                    await writer.ReleaseAsync(token);
                }
            }
        }));

        Assert.Equal(3L, await AttachedLeavesAsync(connection, ct));
        Assert.True(await ParentValidAsync(connection, ct));

        var attaches = attempts.Where(a => a.Statement == "ATTACH PARTITION").ToList();
        Assert.Equal(5, attaches.Count(a => a.Leaf == "t9313_a" && a.Pass == 1));
        Assert.Equal(1, attaches.Count(a => a.Leaf == "t9313_a" && a.Pass == 2));

        /* The later leaves attached in the first pass, before the come-back round of the held one. */
        var comeBack = attaches.FindIndex(a => a.Pass == 2);
        Assert.Equal(1, attaches.Take(comeBack).Count(a => a.Leaf == "t9313_b" && a.Pass == 1));
        Assert.Equal(1, attaches.Take(comeBack).Count(a => a.Leaf == "t9313_c" && a.Pass == 1));
    }

    [Fact]
    public async Task ALeafHeldThroughEverything_ReportsItByName_ButTheOtherLeavesAreAttached()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);
        await ExecAsync(connection, "CREATE INDEX ix_t9313_k_b ON public.t9313_b (id);", ct);
        await ExecAsync(connection, "CREATE INDEX ix_t9313_k_c ON public.t9313_c (id);", ct);
        await using var writer = await Writer.OpenAsync(scratch.ConnectionString, ct);

        var attempts = new List<LockAttempt>();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => EnsurePartitionedAsync(
            connection, Spec, new CapturingLogger(), ct, null, Plan(async (attempt, token) =>
            {
                attempts.Add(attempt);
                if (attempt.Statement == "ATTACH PARTITION" && attempt.Leaf == "t9313_a" && attempt.Pass == 1 && attempt.Attempt == 1)
                {
                    await writer.HoldAsync("t9313_a", token);
                }
            })));

        Assert.Contains("t9313_a", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("t9313_b", ex.Message, StringComparison.Ordinal);
        Assert.Equal(2L, await AttachedLeavesAsync(connection, ct));
        Assert.False(await ParentValidAsync(connection, ct));

        /* The held leaf's child is valid and left in place, for the next start. */
        Assert.Equal(1L, await CountAsync(connection, "SELECT count(*) FROM pg_index WHERE indexrelid = 'public.ix_t9313_k_a'::regclass AND indisvalid", ct));
        Assert.Equal(5, attempts.Count(a => a.Statement == "ATTACH PARTITION" && a.Leaf == "t9313_a" && a.Pass == 1));
        Assert.Equal(5, attempts.Count(a => a.Statement == "ATTACH PARTITION" && a.Leaf == "t9313_a" && a.Pass == 2));
    }

    [Fact]
    public async Task AValidUnattachedChildFromAnEarlierStart_IsAttachedAsItIs_NotRebuilt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);
        await ExecAsync(connection, "CREATE INDEX ix_t9313_k_a ON public.t9313_a (id);", ct);
        var before = Convert.ToInt64(await new NpgsqlCommand("SELECT 'public.ix_t9313_k_a'::regclass::oid::bigint", connection).ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);

        await EnsurePartitionedAsync(connection, Spec, new CapturingLogger(), ct, null, Plan((_, _) => Task.CompletedTask));

        var after = Convert.ToInt64(await new NpgsqlCommand("SELECT 'public.ix_t9313_k_a'::regclass::oid::bigint", connection).ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
        Assert.Equal(before, after);
        Assert.Equal(3L, await AttachedLeavesAsync(connection, ct));
        Assert.True(await ParentValidAsync(connection, ct));
    }

    [Fact]
    public async Task AParentIndexThatExists_IsNotCreatedAgain_SoAWriteOnTheParentDoesNotTimeItOut()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);
        await EnsurePartitionedAsync(connection, Spec, new CapturingLogger(), ct, null, Plan((_, _) => Task.CompletedTask));
        Assert.Equal(3L, await AttachedLeavesAsync(connection, ct));

        /* An insert holds ROW EXCLUSIVE on the parent, which a SHARE lock request would wait behind. */
        await using var writer = await Writer.OpenAsync(scratch.ConnectionString, ct);
        await writer.HoldAsync(Parent, ct, "ROW EXCLUSIVE");

        var attempts = new List<LockAttempt>();
        await EnsurePartitionedAsync(connection, Spec, new CapturingLogger(), ct, null, Plan((attempt, _) =>
        {
            attempts.Add(attempt);
            return Task.CompletedTask;
        }));

        Assert.DoesNotContain(attempts, a => a.Statement == "CREATE INDEX ON ONLY");
        Assert.True(await ParentValidAsync(connection, ct));
    }

    [Fact]
    public async Task ATimedOutAttempt_NamesTheHolder_AndNeverLogsItsQueryText()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);
        var holderConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { ApplicationName = "holder9313app" }.ConnectionString;
        await using var writer = await Writer.OpenAsync(holderConnectionString, ct);

        var logger = new CapturingLogger();
        await EnsurePartitionedAsync(connection, Spec, logger, ct, null, Plan(async (attempt, token) =>
        {
            if (attempt.Statement == "ATTACH PARTITION" && attempt.Leaf == "t9313_c")
            {
                if (attempt.Attempt == 1)
                {
                    await writer.HoldAsync("t9313_c", token, thenRun: "SELECT 'secretliteral9313';");
                }
                else
                {
                    await writer.ReleaseAsync(token);
                }
            }
        }));

        var retryLine = Assert.Single(logger.Lines, line => line.Contains("attempt 1 of 5", StringComparison.Ordinal));
        Assert.Contains("held by: pid ", retryLine, StringComparison.Ordinal);
        Assert.Contains("'holder9313app'", retryLine, StringComparison.Ordinal);
        Assert.Contains("idle in transaction", retryLine, StringComparison.Ordinal);
        Assert.Contains("AccessExclusiveLock", retryLine, StringComparison.Ordinal);
        Assert.DoesNotContain(logger.Lines, line => line.Contains("secretliteral9313", StringComparison.Ordinal));
    }
}
