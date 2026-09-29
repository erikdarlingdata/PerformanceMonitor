/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The Lite twin of Darling's skip-a-failing-database pins. A database whose backfill slices always fail must
/// not stop the databases behind it on the same server: a slice runs at most once per server per tick and the
/// candidate list comes back in the same order every tick, so a failing database first in line used to be first
/// in line forever. The slice body is replaced through <see cref="RemoteCollectorService.SliceOverrideForTests"/>
/// so no SQL Server is needed; the tick loop, the DuckDB candidate and stored-floor reads and the failure
/// accounting are the real ones.
/// </summary>
public sealed class QueryStoreBackfillSkipFailingDatabaseTests : IClassFixture<SharedDuckDbFixture>
{
    private const string Failing = "aaa_failing_db";
    private const string Healthy = "bbb_healthy_db";
    private const string Third = "ccc_third_db";
    private const string ServerLabel = "backfill-skip-test";

    private readonly DuckDbInitializer _duckDb;

    public QueryStoreBackfillSkipFailingDatabaseTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static int Threshold => QueryStoreBackfillState.SkipAfterConsecutiveSliceFailures;

    private sealed record SliceCall(string Database, TimeSpan Span, int Nth, Func<Task> MarkDone);

    private sealed record SliceAttempt(string Database, TimeSpan Span);

    [Fact]
    public async Task AFailingDatabase_DoesNotStopTheDatabaseBehindIt()
    {
        var (attempts, _) = await RunTicksAsync(
            [Failing, Healthy], ticks: 6,
            slice: call => call.Database == Failing ? throw new InvalidOperationException("simulated slice failure") : Task.CompletedTask);

        Assert.True(attempts.Any(a => a.Database == Healthy),
            "the healthy database was never sliced in 6 ticks; attempts: " + string.Join(", ", attempts.Select(a => a.Database)));
    }

    [Fact]
    public async Task ASkippedDatabase_IsRetriedOnceNothingElseHasWork_AtItsOwnNarrowedWindow()
    {
        var (attempts, _) = await RunTicksAsync(
            [Failing, Healthy], ticks: Threshold + 3,
            slice: async call =>
            {
                if (call.Database == Failing)
                {
                    throw new InvalidOperationException("simulated slice failure");
                }

                await call.MarkDone();
            });

        Assert.Equal(
            Enumerable.Repeat(Failing, Threshold).Concat([Healthy, Failing, Failing]),
            attempts.Select(a => a.Database));

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
           a full Threshold of fresh failures before it is skipped. Without the reset the healthy database
           would come one attempt after the completion instead. */
        var (attempts, _) = await RunTicksAsync(
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

        Assert.Equal(2 * Threshold, attempts.FindIndex(a => a.Database == Healthy));
    }

    [Fact]
    public async Task SeveralSkippedDatabases_TakeTurnsBeingRetried()
    {
        var (attempts, _) = await RunTicksAsync(
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
        var (attempts, log) = await RunTicksAsync(
            [Failing, Healthy], ticks: Threshold + 6,
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
        Assert.Contains(ServerLabel, warning, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSkipThreshold_LeavesOneAttemptAtTheNarrowestAdaptiveSpan_AndNoEarlierOne()
    {
        var full = QueryStoreBackfillState.MaxSliceSpan;

        Assert.Equal(QueryStoreBackfillState.MinAdaptiveSpan, QueryStoreBackfillState.AdaptiveSpan(full, Threshold - 1));
        Assert.True(QueryStoreBackfillState.AdaptiveSpan(full, Threshold - 2) > QueryStoreBackfillState.MinAdaptiveSpan,
            "a smaller threshold would skip a database before it ever tried the narrowest window");
    }

    private sealed class Harness(DuckDbInitializer duckDb, ServerManager servers, ScheduleManager schedules, ILogger<RemoteCollectorService> logger)
        : RemoteCollectorService(duckDb, servers, schedules, logger)
    {
        public Task<bool> TickAsync(ServerConnection server) => RunQueryStoreBackfillSliceAsync(server, CancellationToken.None);

        public Task MarkDoneAsync(int serverId, string database) => SaveCollectorStateAsync(
            serverId,
            QueryStoreBackfillState.StateCollectorName,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [QueryStoreBackfillState.DoneKeyPrefix + database] = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            },
            CancellationToken.None);
    }

    /// <summary>Seeds one pending first-contact tail per database, replaces the slice body with
    /// <paramref name="slice"/>, runs <paramref name="ticks"/> real ticks (a slice that throws is caught the way
    /// the collection loop's per-server catch does) and returns every slice attempt in order.</summary>
    private async Task<(List<SliceAttempt> Attempts, WarningLog Log)> RunTicksAsync(
        string[] databases, int ticks, Func<SliceCall, Task> slice)
    {
        var configDirectory = Path.Combine(Path.GetTempPath(), "qs-backfill-skip-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(configDirectory);
        try
        {
            var log = new WarningLog();
            var harness = new Harness(_duckDb, new ServerManager(configDirectory), new ScheduleManager(configDirectory), log);
            var server = new ServerConnection { ServerName = ServerLabel, DisplayName = ServerLabel };
            var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));

            for (var i = 0; i < databases.Length; i++)
            {
                await SeedPendingTailAsync(_duckDb, -466201L - i, serverId, databases[i]);
            }

            var attempts = new List<SliceAttempt>();
            harness.SliceOverrideForTests = async (database, span) =>
            {
                attempts.Add(new SliceAttempt(database, span));
                var nth = attempts.Count(a => a.Database == database);
                await slice(new SliceCall(database, span, nth, () => harness.MarkDoneAsync(serverId, database)));
            };

            for (var tick = 0; tick < ticks; tick++)
            {
                try
                {
                    await harness.TickAsync(server);
                }
                catch (InvalidOperationException ex) when (ex.Message.StartsWith("simulated", StringComparison.Ordinal))
                {
                    /* The collection loop's per-server catch logs a failed slice and carries on to the next tick. */
                }
            }

            return (attempts, log);
        }
        finally
        {
            try
            {
                Directory.Delete(configDirectory, recursive: true);
            }
            catch (IOException)
            {
                /* Best effort: a leftover temp directory is harmless. */
            }
        }
    }

    /// <summary>One row an hour old with an even older last_execution_time: inside the horizon, so the database is
    /// a candidate, and newer than the floor, so its stored ceiling is above it and a tail slice is pending.
    /// Relative to now because the tick computes its floor from the wall clock.</summary>
    private static async Task SeedPendingTailAsync(DuckDbInitializer duckDb, long collectionId, int serverId, string databaseName)
    {
        var now = DateTime.UtcNow;
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)";
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = now.AddHours(-1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerLabel });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = now.AddHours(-2) });
        cmd.Parameters.Add(new DuckDBParameter { Value = now.AddHours(-2) });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT 1" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xTESTHASH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 2000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xTESTPLANHASH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = false });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Keeps the Warning-and-above messages, formatted, so a test can count them.</summary>
    private sealed class WarningLog : ILogger<RemoteCollectorService>
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
