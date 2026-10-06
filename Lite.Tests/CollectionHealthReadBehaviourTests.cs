/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5371: the three things a slow Collection Health read lacked. Nothing in the app could say where such a read spent
/// its time (the phase line), nothing stopped a superseded one (the token), and every timer tick re-ran the seven-day
/// aggregate (the 30 second memo).
///
/// <para>The cancellation tests run against a real DuckDB through the real read, with only the statement's text
/// swapped for one that cannot finish, because the aggregate over a test-sized store takes milliseconds. The bound
/// they assert is generous against a 1e12-row cross product: an interrupt that did nothing would sit in the statement
/// for minutes, not seconds.</para>
/// </summary>
public sealed class CollectionHealthReadBehaviourTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 5371;
    private const int OtherServerId = 5372;

    /// <summary>A statement that runs for minutes: 1e12 pairs. It takes the read's two parameters, so the read binds it unchanged.</summary>
    private const string NeverFinishesSql = @"
SELECT sum(a.range * b.range) + CAST($1 AS BIGINT) + CAST(EXTRACT(epoch FROM CAST($2 AS TIMESTAMP)) AS BIGINT)
FROM range(1000000) a, range(1000000) b";

    private static readonly TimeSpan InterruptBound = TimeSpan.FromSeconds(5);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionHealthReadBehaviourTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /* ---------------------------------- phase timing ---------------------------------- */

    [Fact]
    public void PhaseLine_ASlowPhase_ProducesOneLineNamingItAndTheOtherFour()
    {
        var timer = new ReadPhaseTimer();
        timer.Add(ReadPhaseTimer.LockWait, 12);
        timer.Add(ReadPhaseTimer.Open, 3);
        timer.Add(ReadPhaseTimer.Prepare, 1);
        timer.Add(ReadPhaseTimer.Execute, 900);
        timer.Add(ReadPhaseTimer.Drain, 2);

        var lines = new List<(string Name, double Total, string Context)>();
        timer.Report("GetCollectionHealthAsync", (name, total, context) => lines.Add((name, total, context)));

        var line = Assert.Single(lines);
        Assert.Equal(918, line.Total);
        Assert.Contains("lock wait 12 ms", line.Context, StringComparison.Ordinal);
        Assert.Contains("open 3 ms", line.Context, StringComparison.Ordinal);
        Assert.Contains("prepare 1 ms", line.Context, StringComparison.Ordinal);
        Assert.Contains("execute 900 ms", line.Context, StringComparison.Ordinal);
        Assert.Contains("drain 2 ms", line.Context, StringComparison.Ordinal);
    }

    [Fact]
    public void PhaseLine_AFastRead_LogsNothing()
    {
        var timer = new ReadPhaseTimer();
        timer.Add(ReadPhaseTimer.Execute, 5);

        var lines = new List<string>();
        timer.Report("GetCollectionHealthAsync", (name, total, context) => lines.Add(context));

        Assert.Empty(lines);
    }

    [Fact]
    public async Task PhaseLine_ALiveReadBehindAHeldLock_NamesTheLockWait()
    {
        var service = new LocalDataService(_duckDb);
        var lines = new List<(double Total, string Context)>();
        service.CollectionHealthPhaseSink = (name, total, context) => { lock (lines) lines.Add((total, context)); };

        var release = new ManualResetEventSlim();
        var held = HoldWriteLock(_duckDb, release, out var acquired);
        await acquired.WaitAsync(TimeSpan.FromSeconds(30));
        var read = Task.Run(() => service.GetCollectionHealthAsync(ServerId));
        await Task.Delay(700);
        release.Set();
        await held;
        await read;

        var line = Assert.Single(lines);
        Assert.True(line.Total >= 500, $"total {line.Total} ms");
        var lockWaitMs = double.Parse(line.Context.Split("lock wait ")[1].Split(' ')[0], System.Globalization.CultureInfo.InvariantCulture);
        Assert.True(lockWaitMs >= 500, line.Context);
    }

    /* ---------------------------------- cancellation ---------------------------------- */

    [Fact]
    public async Task Cancel_MidStatement_InterruptsTheRunningQueryWithinTheBound_AndFreesTheLock()
    {
        var service = new LocalDataService(_duckDb) { CollectionHealthSqlOverride = NeverFinishesSql };
        using var cts = new CancellationTokenSource();

        /* The read blocks its caller until the engine answers, as the tab's does, so it runs on the pool like the tab's. */
        var read = Task.Run(() => service.GetCollectionHealthAsync(ServerId, cancellationToken: cts.Token));
        await Task.Delay(750);
        Assert.False(read.IsCompleted, "the statement must still be running when the token fires");

        var sw = Stopwatch.StartNew();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read.WaitAsync(TimeSpan.FromSeconds(60)));
        sw.Stop();

        Assert.True(sw.Elapsed < InterruptBound, $"interrupt took {sw.ElapsedMilliseconds} ms");
        AssertLockIsFree();
    }

    [Fact]
    public async Task Cancel_MidStatement_LeavesTheStoreReadableAfterwards()
    {
        await SeedAsync(ServerId, "query_store", MinutesAgo(5));
        var slow = new LocalDataService(_duckDb) { CollectionHealthSqlOverride = NeverFinishesSql };
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => slow.GetCollectionHealthAsync(ServerId, cancellationToken: cts.Token).WaitAsync(TimeSpan.FromSeconds(60)));

        var rows = await new LocalDataService(_duckDb).GetCollectionHealthAsync(ServerId);
        Assert.Equal("query_store", Assert.Single(rows).CollectorName);
    }

    [Fact]
    public async Task Cancel_WhileWaitingBehindAWriter_ThrowsPromptly_ForTheHealthRead_AndTheLogRead_AndFreesTheLock()
    {
        var service = new LocalDataService(_duckDb);
        var release = new ManualResetEventSlim();
        var held = HoldWriteLock(_duckDb, release, out var acquired);
        try
        {
            await acquired.WaitAsync(TimeSpan.FromSeconds(30));

            foreach (var read in new Func<CancellationToken, Task>[]
            {
                ct => service.GetCollectionHealthAsync(ServerId, cancellationToken: ct),
                ct => service.GetRecentCollectionLogAsync(ServerId, cancellationToken: ct),
            })
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
                var sw = Stopwatch.StartNew();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read(cts.Token).WaitAsync(TimeSpan.FromSeconds(60)));
                Assert.True(sw.Elapsed < TimeSpan.FromSeconds(3), $"cancel took {sw.ElapsedMilliseconds} ms");
            }
        }
        finally
        {
            release.Set();
            await held;
        }

        AssertLockIsFree();
    }

    [Fact]
    public async Task Cancel_AnAlreadyCancelledToken_ThrowsBeforeTouchingTheStore()
    {
        var service = new LocalDataService(_duckDb);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.GetCollectionHealthAsync(ServerId, cancellationToken: cts.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.GetRecentCollectionLogAsync(ServerId, cancellationToken: cts.Token));
        AssertLockIsFree();
    }

    [Fact]
    public async Task Cancel_TheCoordinatorsSupersedingRequest_ReachesTheRunningHealthRead()
    {
        var service = new LocalDataService(_duckDb) { CollectionHealthSqlOverride = NeverFinishesSql };
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        Exception? firstOutcome = null;
        var firstStopped = new Stopwatch();

        var coordinator = new RefreshCoordinator(
            async (scope, ct) =>
            {
                if (Interlocked.Increment(ref calls) > 1)
                {
                    return;
                }

                firstStarted.SetResult();
                try
                {
                    await Task.Run(() => service.GetCollectionHealthAsync(ServerId, cancellationToken: ct));
                }
                catch (Exception ex)
                {
                    firstStopped.Stop();
                    firstOutcome = ex;
                    throw;
                }
            },
            ex => throw new InvalidOperationException("the pass must not fault: " + ex));

        var first = coordinator.RequestAsync(RefreshScope.Full);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await Task.Delay(500);
        firstStopped.Start();
        var second = coordinator.RequestAsync(RefreshScope.VisibleTab);
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(60));

        Assert.IsAssignableFrom<OperationCanceledException>(firstOutcome);
        Assert.True(firstStopped.Elapsed < InterruptBound, $"the superseded read stopped after {firstStopped.ElapsedMilliseconds} ms");
        Assert.Equal(2, calls);
        AssertLockIsFree();
    }

    /* ---------------------------------- memo ---------------------------------- */

    [Fact]
    public async Task Memo_TwoTicksInsideTheWindow_RunOneQuery()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));

        var first = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        await SeedAsync(ServerId, "deadlocks", MinutesAgo(9));
        var second = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);

        /* The second tick would see two collectors if it had asked the store. */
        Assert.Single(first);
        Assert.Single(second);
    }

    [Fact]
    public async Task Memo_AManualRefresh_RunsASecondQuery()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        await SeedAsync(ServerId, "deadlocks", MinutesAgo(9));

        service.InvalidateCollectionHealthMemo();
        var afterGesture = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);

        Assert.Equal(2, afterGesture.Count);
    }

    [Fact]
    public async Task Memo_ACallThatDoesNotAskForIt_AlwaysReadsTheStore()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        await SeedAsync(ServerId, "deadlocks", MinutesAgo(9));

        var mcpStyle = await service.GetCollectionHealthAsync(ServerId);

        Assert.Equal(2, mcpStyle.Count);
    }

    [Fact]
    public async Task Memo_ExpiresAfterThirtySeconds()
    {
        var now = DateTime.UtcNow;
        var service = new LocalDataService(_duckDb) { CollectionHealthMemoClock = () => now };
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        await SeedAsync(ServerId, "deadlocks", MinutesAgo(9));

        now += TimeSpan.FromSeconds(29);
        Assert.Single(await service.GetCollectionHealthAsync(ServerId, allowMemo: true));

        now += TimeSpan.FromSeconds(2);
        Assert.Equal(2, (await service.GetCollectionHealthAsync(ServerId, allowMemo: true)).Count);
        Assert.Equal(TimeSpan.FromSeconds(30), LocalDataService.CollectionHealthMemoLifetime);
    }

    [Fact]
    public async Task Memo_IsPerServer_AnotherServersReadIsNotServedFromIt()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        await SeedAsync(OtherServerId, "deadlocks", MinutesAgo(10));
        await SeedAsync(OtherServerId, "wait_stats", MinutesAgo(10));

        var one = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        var other = await service.GetCollectionHealthAsync(OtherServerId, allowMemo: true);

        Assert.Single(one);
        Assert.Equal(2, other.Count);
    }

    [Fact]
    public async Task Memo_HandsOutCopies_SoOneCallersStampDoesNotReachTheNext()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        var first = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        first[0].CollectorName = "mutated by the first caller";

        var second = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        second[0].TotalRuns = 999;
        var third = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);

        Assert.Equal("query_store", third.Single().CollectorName);
        Assert.Equal(1, third.Single().TotalRuns);
    }

    [Fact]
    public async Task Memo_ACancelledRead_StoresNothing()
    {
        var service = new LocalDataService(_duckDb) { CollectionHealthSqlOverride = NeverFinishesSql };
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.GetCollectionHealthAsync(ServerId, allowMemo: true, cancellationToken: cts.Token).WaitAsync(TimeSpan.FromSeconds(60)));

        service.CollectionHealthSqlOverride = null;
        var rows = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);

        Assert.Single(rows);
    }

    /* ---------------------------------- the tab wires it ---------------------------------- */

    [Fact]
    public void TheServerTab_PassesThePassToken_ToBothReads_AndOnlyTheTimerReusesTheMemo()
    {
        var source = System.IO.File.ReadAllText(System.IO.Path.Combine(RepoRoot(), "Lite", "Controls", "ServerTab.Refresh.cs"));

        Assert.Contains("case 17: await RefreshCollectionHealthAsync(hoursBack, fromDate, toDate, ct); break;", source, StringComparison.Ordinal);
        Assert.Contains("GetCollectionHealthAsync(_serverId, allowMemo: true, cancellationToken: ct)", source, StringComparison.Ordinal);
        Assert.Contains("GetRecentCollectionLogAsync(_serverId, hoursBack, fromDate, toDate, cancellationToken: ct)", source, StringComparison.Ordinal);

        /* Every gesture drops the memo; the timer's tick must not. */
        var timerTick = source.IndexOf("private Task RefreshAllDataOnTimerAsync() => Refresher.PollAsync();", StringComparison.Ordinal);
        Assert.True(timerTick >= 0, "the timer tick no longer goes straight to PollAsync");
        var gesture = source[source.IndexOf("private Task RefreshAllDataAsync()", StringComparison.Ordinal)..];
        Assert.Contains("InvalidateCollectionHealthMemo", gesture[..gesture.IndexOf("RequestAsync(RefreshScope.Full)", StringComparison.Ordinal)], StringComparison.Ordinal);
        var tabSwitch = source[source.IndexOf("private Task RefreshVisibleTabOnlyAsync()", StringComparison.Ordinal)..];
        Assert.Contains("InvalidateCollectionHealthMemo", tabSwitch[..tabSwitch.IndexOf("RequestAsync(RefreshScope.VisibleTab)", StringComparison.Ordinal)], StringComparison.Ordinal);
    }

    /* ---------------------------------- helpers ---------------------------------- */

    private static string RepoRoot()
    {
        var dir = new System.IO.DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "Lite", "Controls", "ServerTab.Refresh.cs")))
        {
            dir = dir.Parent;
        }
        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }

    /// <summary>
    /// Takes the exclusive store lock on a dedicated thread and holds it until <paramref name="release"/> is set. The lock
    /// is thread-affine, so the thread that took it releases it. Timed out rather than unbounded, so a stuck test cannot
    /// wedge the suite.
    /// </summary>
    private static Task HoldWriteLock(DuckDbInitializer duckDb, ManualResetEventSlim release, out Task acquired)
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        acquired = signal.Task;
        return Task.Factory.StartNew(() =>
        {
            using var writeLock = duckDb.AcquireWriteLock(TimeSpan.FromMinutes(2));
            signal.SetResult();
            release.Wait(TimeSpan.FromMinutes(2));
        }, TaskCreationOptions.LongRunning);
    }

    private void AssertLockIsFree()
    {
        /* A write lock is only granted when no reader holds the lock, so getting one proves the read gave its lock back. */
        using var writeLock = _duckDb.AcquireWriteLock(TimeSpan.FromSeconds(10));
    }

    private static DateTime MinutesAgo(int minutes)
    {
        var t = DateTime.UtcNow.AddMinutes(-minutes);
        return new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Utc);
    }

    private async Task SeedAsync(int serverId, string collector, DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= await OpenSeedConnectionAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SUCCESS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> OpenSeedConnectionAsync()
    {
        var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        return connection;
    }
}
