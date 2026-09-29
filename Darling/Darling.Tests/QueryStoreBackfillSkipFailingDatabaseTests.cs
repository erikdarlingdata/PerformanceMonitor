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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A database whose backfill slices always fail must not stop the databases behind it on the same server.
/// A slice runs at most once per server per tick and the candidate list comes back in the same order every
/// tick, so a failing database that is first in line used to be first in line forever: the throw left the
/// tick before any other database was looked at, and no state was saved to change the next tick's answer.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (via <see cref="ScratchPostgres"/>) and seeds
/// two or three databases with a pending first-contact tail. The slice body is replaced through
/// <see cref="QueryStoreBackfill.SliceOverrideForTests"/> so no SQL Server is needed and each test scripts
/// which slices fail; the tick loop, the candidate read, the stored-floor read and the failure accounting are
/// the real ones.</para>
/// </summary>
public sealed class QueryStoreBackfillSkipFailingDatabaseTests
{
    private const int TestServerId = -466201;
    private const string Failing = "aaa_failing_db";
    private const string Healthy = "bbb_healthy_db";
    private const string Third = "ccc_third_db";

    private static int Threshold => QueryStoreBackfillState.SkipAfterConsecutiveSliceFailures;

    /// <summary>What one scripted slice call gets: the database, the window the real slice would have used,
    /// which attempt this is for that database (1-based), and a way to record the drain finishing.</summary>
    private sealed record SliceCall(string Database, TimeSpan Span, int Nth, Func<Task> MarkDone);

    [Fact]
    public async Task AFailingDatabase_DoesNotStopTheDatabaseBehindIt()
    {
        var attempts = await RunTicksAsync(
            [Failing, Healthy], ticks: 6,
            slice: call => call.Database == Failing ? throw new InvalidOperationException("simulated slice failure") : Task.CompletedTask);

        Assert.True(attempts.Any(a => a.Database == Healthy),
            "the healthy database was never sliced in 6 ticks; attempts: " + string.Join(", ", attempts.Select(a => a.Database)));
    }

    [Fact]
    public async Task ASkippedDatabase_IsRetriedOnceNothingElseHasWork_AtItsOwnNarrowedWindow()
    {
        var attempts = await RunTicksAsync(
            [Failing, Healthy], ticks: Threshold + 3,
            slice: async call =>
            {
                if (call.Database == Failing)
                {
                    throw new InvalidOperationException("simulated slice failure");
                }

                await call.MarkDone();
            });

        /* Threshold failures, then the healthy database drains and finishes, then the skipped one is retried
           on the ticks where nothing else has work. */
        Assert.Equal(
            Enumerable.Repeat(Failing, Threshold).Concat([Healthy, Failing, Failing]),
            attempts.Select(a => a.Database));

        /* The failing database narrows on its OWN count: full, half, then the floor; the healthy neighbour
           starts at the full window instead of inheriting the failing one's narrowed window. */
        var full = QueryStoreBackfillState.MaxSliceSpan;
        Assert.Equal(
            Enumerable.Range(0, Threshold).Select(i => QueryStoreBackfillState.AdaptiveSpan(full, i)),
            attempts.Where(a => a.Database == Failing).Take(Threshold).Select(a => a.Span));
        Assert.Equal(full, attempts.First(a => a.Database == Healthy).Span);
        Assert.Equal(QueryStoreBackfillState.MinAdaptiveSpan, attempts.Last(a => a.Database == Failing).Span);
    }

    [Fact]
    public async Task ACompletedSlice_ClearsTheFailureCount()
    {
        /* Fail, fail, complete, then fail forever: the completion must reset the count, so the database needs
           a full Threshold of fresh failures before it is skipped. Without the reset it is skipped after the
           first Threshold failures overall and the healthy database runs a few ticks earlier. */
        var attempts = await RunTicksAsync(
            [Failing, Healthy], ticks: (2 * Threshold) + 3,
            slice: async call =>
            {
                if (call.Database == Failing && call.Nth != Threshold)
                {
                    throw new InvalidOperationException("simulated slice failure");
                }

                if (call.Database == Healthy)
                {
                    await call.MarkDone();
                }
            });

        /* Threshold attempts up to and including the completion, then Threshold fresh failures. Without the
           reset the healthy database would come one attempt after the completion instead. */
        var healthyAt = attempts.FindIndex(a => a.Database == Healthy);
        Assert.Equal(2 * Threshold, healthyAt);
    }

    [Fact]
    public async Task SeveralSkippedDatabases_TakeTurnsBeingRetried()
    {
        /* Two databases fail forever and a third is healthy. Once both are skipped and the third has drained,
           each idle tick retries whichever skipped database failed longest ago, so neither starves the other. */
        var attempts = await RunTicksAsync(
            [Failing, Healthy, Third], ticks: (3 * Threshold) + 5,
            slice: async call =>
            {
                if (call.Database == Third)
                {
                    await call.MarkDone();
                    return;
                }

                throw new InvalidOperationException("simulated slice failure");
            });

        var expected = Enumerable.Repeat(Failing, Threshold)
            .Concat(Enumerable.Repeat(Healthy, Threshold))
            .Append(Third)
            .Concat([Failing, Healthy, Failing, Healthy]);
        Assert.Equal(expected, attempts.Select(a => a.Database).Take((2 * Threshold) + 1 + 4));
    }

    [Fact]
    public async Task TheSkipWarning_LogsOncePerSkippedDatabase_NotOnEveryTick()
    {
        var log = new WarningLog();
        var attempts = await RunTicksAsync(
            [Failing, Healthy], ticks: Threshold + 6, logger: log,
            slice: async call =>
            {
                if (call.Database == Failing)
                {
                    throw new InvalidOperationException("simulated slice failure");
                }

                await call.MarkDone();
            });

        Assert.True(attempts.Count(a => a.Database == Failing) > Threshold, "the skipped database should keep being retried");
        var warning = Assert.Single(log.Warnings, w => w.Contains(Failing, StringComparison.Ordinal));
        Assert.Contains(Threshold.ToString(CultureInfo.InvariantCulture), warning, StringComparison.Ordinal);
        Assert.Contains("backfill-skip-test", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSkipThreshold_LeavesOneAttemptAtTheNarrowestAdaptiveSpan_AndNoEarlierOne()
    {
        var full = QueryStoreBackfillState.MaxSliceSpan;

        /* The Nth failure is the first one at the floor: attempt k runs at AdaptiveSpan(full, k - 1). */
        Assert.Equal(QueryStoreBackfillState.MinAdaptiveSpan, QueryStoreBackfillState.AdaptiveSpan(full, Threshold - 1));
        Assert.True(QueryStoreBackfillState.AdaptiveSpan(full, Threshold - 2) > QueryStoreBackfillState.MinAdaptiveSpan,
            "a smaller threshold would skip a database before it ever tried the narrowest window");
    }

    [Fact]
    public void TheLedger_SkipsAtTheThreshold_ResetsOnCompletion_AndOrdersRetriesLeastRecentFirst()
    {
        var ledger = new QueryStoreBackfillFailureLedger();
        for (var failures = 1; failures < Threshold; failures++)
        {
            Assert.Equal(failures, ledger.RecordFailure(1, "a"));
            Assert.False(ledger.IsSkipped(1, "a"));
        }

        Assert.Equal(Threshold, ledger.RecordFailure(1, "a"));
        Assert.True(ledger.IsSkipped(1, "a"));
        Assert.False(ledger.IsSkipped(2, "a"));
        Assert.False(ledger.IsSkipped(1, "b"));

        ledger.RecordCompletion(1, "a");
        Assert.Equal(0, ledger.Failures(1, "a"));
        Assert.False(ledger.IsSkipped(1, "a"));

        ledger.RecordFailure(1, "a");
        ledger.RecordFailure(1, "b");
        Assert.True(ledger.LastFailureTicket(1, "a") < ledger.LastFailureTicket(1, "b"));
        Assert.Equal(0, ledger.LastFailureTicket(1, "never-failed"));
    }

    /// <summary>Seeds one pending first-contact tail per database, replaces the slice body with
    /// <paramref name="slice"/>, runs <paramref name="ticks"/> real ticks (a slice that throws is caught the way
    /// the worker's outer catch does) and returns every slice attempt in order.</summary>
    private static async Task<List<SliceAttempt>> RunTicksAsync(
        string[] databases, int ticks, Func<SliceCall, Task> slice, ILogger? logger = null)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store backfill skip tests (they mint their own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            for (var i = 0; i < databases.Length; i++)
            {
                await SeedPendingTailAsync(connection, 466201L + i, databases[i], ct);
            }
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var backfill = new QueryStoreBackfill(postgres, runner, new CollectorDeltaCalculator(), logger);
        var attempts = new List<SliceAttempt>();
        backfill.SliceOverrideForTests = async (database, span) =>
        {
            attempts.Add(new SliceAttempt(database, span));
            var nth = attempts.Count(a => a.Database == database);
            await slice(new SliceCall(database, span, nth, () => runner.SaveCollectorStateAsync(
                TestServerId,
                QueryStoreBackfill.StateCollectorName,
                new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    [QueryStoreBackfillState.DoneKeyPrefix + database] = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                },
                ct)));
        };

        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "backfill-skip-test", Host = "backfill-skip-test" },
            ConnectionString = "Server=backfill-skip-test",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "backfill-skip-test",
            ServerId = TestServerId,
            EngineEdition = 3,
        };

        for (var tick = 0; tick < ticks; tick++)
        {
            try
            {
                await backfill.RunServerSliceAsync(server, ct);
            }
            catch (InvalidOperationException ex) when (ex.Message.StartsWith("simulated", StringComparison.Ordinal))
            {
                /* The worker's outer catch logs a failed slice and carries on to the next tick. */
            }
        }

        return attempts;
    }

    private sealed record SliceAttempt(string Database, TimeSpan Span);

    /// <summary>One row an hour old with an even older last_execution_time: inside the horizon, so the database
    /// is a candidate, and newer than the floor, so its stored ceiling is above it and a tail slice is pending.
    /// Relative to now because the tick computes its floor from the wall clock.</summary>
    private static async Task SeedPendingTailAsync(NpgsqlConnection connection, long collectionId, string databaseName, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us)
VALUES
    ($1, $2, $3, 'SQL01', $4, 'dbo.GetOrders', '0xABCD', 91, 111, 'Regular', 'Primary',
     1, $2, $5, $5, 1, 100, 100, 100, 100)";

        var now = DateTime.UtcNow;
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(now.AddHours(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(now.AddHours(-2), DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Keeps the Warning-and-above messages, formatted, so a test can count them.</summary>
    private sealed class WarningLog : ILogger
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }
}
