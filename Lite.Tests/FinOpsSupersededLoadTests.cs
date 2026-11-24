/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9058: the sample and full text of a high-impact hash come from one pass over the window, and they are the text the two
/// per-hash lookups used to return: the hash's busiest row, a NULL count last, an empty or NULL text never.
/// </summary>
public sealed class FinOpsHighImpactSampleTextTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public FinOpsHighImpactSampleTextTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private const int ServerId = -9058;
    private const int OtherServerId = -9059;

    private async Task PlantAsync(DuckDBConnection connection, long id, int serverId, DateTime at, string hash, string? text, long? exec, DateTime? creationTime = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text, creation_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, max_grant_kb)
VALUES ($1, $2, $3, 'lite-finops-sample-text', 'Db0', $4, $5, $6, $7, 1000, 1000, 10, 0, 1024)";
        foreach (var value in new object?[] { id, at, serverId, hash, text, creationTime ?? at.AddDays(-1), exec })
        {
            command.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task GetHighImpactQueriesAsync_SampleAndFullTextComeFromTheBusiestRow()
    {
        var at = DateTime.UtcNow.AddHours(-3);
        var longText = "SELECT " + new string('x', 400) + ";";
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            var id = -9_058_000L;

            /* The most executions wins over a later and an earlier row. */
            await PlantAsync(connection, id--, ServerId, at, "0xA", "A five", 5);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(10), "0xA", "A nine", 9);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(20), "0xA", "A three", 3);

            /* A NULL count ranks last, so a row with a count wins over it even when the NULL row is newer. */
            await PlantAsync(connection, id--, ServerId, at, "0xB", "B two", 2);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(10), "0xB", "B null count", null);

            /* An empty text and a NULL text are never the sample, even from the busiest row. */
            await PlantAsync(connection, id--, ServerId, at, "0xC", "", 50);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(1), "0xC", null, 40);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(2), "0xC", "C three", 3);

            /* The only row that has a text has no count: it is still the text. */
            await PlantAsync(connection, id--, ServerId, at, "0xD", null, 4);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(5), "0xD", "D null count", null);

            /* The sample is the first 200 characters of the full text. */
            await PlantAsync(connection, id--, ServerId, at, "0xE", longText, 7);

            /* No row of the hash has a text. */
            await PlantAsync(connection, id--, ServerId, at, "0xF", null, 6);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(1), "0xF", "", 6);

            /* Another server's busier row, and a row before the window, are not candidates. */
            await PlantAsync(connection, id--, OtherServerId, at, "0xA", "A other server", 999);
            await PlantAsync(connection, id--, ServerId, at.AddDays(-3), "0xA", "A before the window", 999);
            await PlantAsync(connection, id--, ServerId, at.AddDays(-3), "0xB", "B before the window", 999);

            /* A tie on the count picks the newest of the tied rows, the same on every read. */
            await PlantAsync(connection, id--, ServerId, at, "0xG", "G older tie", 8);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(30), "0xG", "G newer tie", 8);
            await PlantAsync(connection, id--, ServerId, at.AddMinutes(15), "0xG", "G middle tie", 8);

            /* Rows of one collection (several plans or databases) share the collection time. A tie on the count then goes to
               the newest plan creation, even though that text sorts last. */
            await PlantAsync(connection, id--, ServerId, at, "0xH", "H a older plan", 8, at.AddDays(-2));
            await PlantAsync(connection, id--, ServerId, at, "0xH", "H z newer plan", 8, at.AddDays(-1));

            /* Same collection, same count, same creation: the text is the last key, whatever order the rows were written in. */
            await PlantAsync(connection, id--, ServerId, at, "0xI", "I b second", 8);
            await PlantAsync(connection, id--, ServerId, at, "0xI", "I a first", 8);
            await PlantAsync(connection, id--, ServerId, at, "0xI", "I c third", 8);
        }

        var rows = await new LocalDataService(_duckDb).GetHighImpactQueriesAsync(ServerId, 24);
        var byHash = rows.ToDictionary(r => r.QueryHash);

        Assert.Equal(9, byHash.Count);
        void Check(string hash, string sample, string full)
        {
            Assert.Equal(sample, byHash[hash].SampleQueryText);
            Assert.Equal(full, byHash[hash].FullQueryText);
        }

        Check("0xA", "A nine", "A nine");
        Check("0xB", "B two", "B two");
        Check("0xC", "C three", "C three");
        Check("0xD", "D null count", "D null count");
        Check("0xE", longText[..200], longText);
        Check("0xF", "", "");
        Check("0xG", "G newer tie", "G newer tie");
        Check("0xH", "H z newer plan", "H z newer plan");
        Check("0xI", "I a first", "I a first");
    }
}

/// <summary>
/// #9058: a whole-tab FinOps load that a newer one superseded stops its store reads and gives back their read locks. The
/// token reaches the lock wait, the open and the statement.
/// </summary>
/* One test asserts a wall-clock bound on a cancelled statement, so it runs alone with the other timing classes. */
[Collection("timing")]
public sealed class FinOpsSupersededLoadTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public FinOpsSupersededLoadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private const int ServerId = -9060;

    [Fact]
    public void WholeLoadScope_BeginCancelsThePreviousLoadAndOnlyThat()
    {
        var scope = new WholeLoadScope();

        using var first = scope.Begin();
        Assert.False(first.Token.IsCancellationRequested);

        using var second = scope.Begin();
        Assert.True(first.Token.IsCancellationRequested);
        Assert.False(second.Token.IsCancellationRequested);

        using var third = scope.Begin();
        Assert.True(second.Token.IsCancellationRequested);
        Assert.False(third.Token.IsCancellationRequested);
    }

    [Fact]
    public void WholeLoadScope_AFinishedLoadIsNotCancelledByTheNextOne_AndDisposingOutOfOrderIsSafe()
    {
        var scope = new WholeLoadScope();

        var first = scope.Begin();
        var second = scope.Begin();

        /* The older lease ends after it was superseded, the newer one ends, and a later load begins: none of it throws. */
        first.Dispose();
        second.Dispose();
        using var third = scope.Begin();
        Assert.False(third.Token.IsCancellationRequested);
    }

    /// <summary>Every FinOps read the whole-tab load starts with the load's token, by the call the tab makes.</summary>
    private static IEnumerable<(string Name, Func<LocalDataService, CancellationToken, Task> Read)> TabReads()
    {
        yield return ("GetUtilizationEfficiencyAsync", (d, t) => d.GetUtilizationEfficiencyAsync(ServerId, t));
        yield return ("GetTopResourceConsumersAsync", (d, t) => d.GetTopResourceConsumersAsync(ServerId, cancellationToken: t));
        yield return ("GetProvisioningTrendAsync", (d, t) => d.GetProvisioningTrendAsync(ServerId, TimeZoneInfo.Utc, t));
        yield return ("GetDatabaseResourceUsageAsync", (d, t) => d.GetDatabaseResourceUsageAsync(ServerId, 24, t));
        yield return ("GetApplicationConnectionsAsync", (d, t) => d.GetApplicationConnectionsAsync(ServerId, t));
        yield return ("GetWaitCategorySummaryAsync", (d, t) => d.GetWaitCategorySummaryAsync(ServerId, 24, t));
        yield return ("GetExpensiveQueriesAsync", (d, t) => d.GetExpensiveQueriesAsync(ServerId, 24, cancellationToken: t));
        yield return ("GetMemoryGrantEfficiencyAsync", (d, t) => d.GetMemoryGrantEfficiencyAsync(ServerId, dayZone: TimeZoneInfo.Utc, cancellationToken: t));
        yield return ("GetHighImpactQueriesAsync", (d, t) => d.GetHighImpactQueriesAsync(ServerId, 24, t));
    }

    [Fact]
    public async Task EachTabRead_AbandonedAfterItOpensItsConnection_ThrowsCancelledAndReleasesTheStore()
    {
        var data = new LocalDataService(_duckDb);
        var failures = new List<string>();
        foreach (var (name, read) in TabReads())
        {
            using var cts = new CancellationTokenSource();
            data.ConnectionOpened = _ => cts.Cancel();
            try
            {
                await read(data, cts.Token);
                failures.Add($"{name} finished although its token fired once the connection was open");
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                data.ConnectionOpened = null;
            }

            /* The read lock is back: an exclusive writer gets in at once. */
            try
            {
                using var writeLock = _duckDb.AcquireWriteLock(TimeSpan.FromSeconds(10));
            }
            catch (TimeoutException)
            {
                failures.Add($"{name} left its read lock held after it was cancelled");
            }
        }

        Assert.True(failures.Count == 0, string.Concat(failures.Select(f => f + "; ")));
    }

    [Fact]
    public async Task ASupersededLoadsRead_WaitingOnTheStoreLock_LeavesWhenTheNextLoadBegins_AndTheNextOneStillRuns()
    {
        var data = new LocalDataService(_duckDb);
        var scope = new WholeLoadScope();

        /* The writer holds the lock on its own thread: the lock belongs to the thread that took it. */
        var writerHasIt = new ManualResetEventSlim(false);
        var releaseWriter = new ManualResetEventSlim(false);
        var writer = Task.Run(() =>
        {
            using var write = _duckDb.AcquireWriteLock();
            writerHasIt.Set();
            releaseWriter.Wait(TimeSpan.FromSeconds(20));
        });

        try
        {
            Assert.True(writerHasIt.Wait(TimeSpan.FromSeconds(20)), "the writer never took the lock");

            using var older = scope.Begin();
            /* The older load's read queues behind the writer, as one does behind a collection write. */
            var olderRead = Task.Run(() => data.GetHighImpactQueriesAsync(ServerId, 24, older.Token));
            await Task.Delay(300);
            Assert.False(olderRead.IsCompleted);

            using var newer = scope.Begin();
            var finished = await Task.WhenAny(olderRead, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.Same(olderRead, finished);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => olderRead);
            Assert.False(newer.Token.IsCancellationRequested);

            releaseWriter.Set();
            await writer;

            /* With the writer gone, a read on the newer load's token completes. */
            var rows = await data.GetHighImpactQueriesAsync(ServerId, 24, newer.Token);
            Assert.Empty(rows);
        }
        finally
        {
            releaseWriter.Set();
            await writer;
        }
    }

    /* About 5 s uncancelled on the laptop this was calibrated on. DuckDB.NET's ExecuteReaderAsync(token) wires the token to
       the command's cancel, which interrupts the running statement and surfaces as OperationCanceledException. This pins that
       for the version Lite references: an upgrade that stopped doing it would fail here. */
    private const string LongStatement =
        "SELECT sum(hash(a.range * 1000003 + b.range)) FROM range(20000) AS a, range(20000) AS b";

    [Fact]
    public async Task ARunningStatement_IsInterruptedWhenItsTokenFires_AndTheReadLockIsFreeAgain()
    {
        var data = new LocalDataService(_duckDb);
        using var cts = new CancellationTokenSource();
        var started = System.Diagnostics.Stopwatch.StartNew();

        var read = Task.Run(async () =>
        {
            using var connection = await data.OpenConnectionAsync(null, cts.Token);
            using var command = connection.CreateCommand();
            command.CommandText = LongStatement;
            using var reader = await command.ExecuteReaderAsync(cts.Token);
            return await reader.ReadAsync(cts.Token);
        });

        cts.CancelAfter(TimeSpan.FromMilliseconds(200));
        var finished = await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(20)));
        Assert.Same(read, finished);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(2), $"the cancelled statement took {started.Elapsed.TotalSeconds:0.0} s to stop");

        /* Its connection and read lock are back: an exclusive writer gets in at once. */
        using var writeLock = _duckDb.AcquireWriteLock(TimeSpan.FromSeconds(10));
    }
}
