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
        timer.Add(ReadPhaseTimer.Execute, 900);
        timer.Add(ReadPhaseTimer.RowRead, 2);

        var lines = new List<(string Name, double Total, string Context)>();
        timer.Report("GetCollectionHealthAsync", (name, total, context) => lines.Add((name, total, context)));

        var line = Assert.Single(lines);
        Assert.Equal(917, line.Total);
        Assert.Contains("lock wait 12 ms", line.Context, StringComparison.Ordinal);
        Assert.Contains("open 3 ms", line.Context, StringComparison.Ordinal);
        /* The log line says what the execute number contains: DuckDB.NET's Prepare() binds nothing, so the bind cost is here. */
        Assert.Contains("execute 900 ms (prepare and bind included)", line.Context, StringComparison.Ordinal);
        Assert.Contains("row read 2 ms", line.Context, StringComparison.Ordinal);
        Assert.DoesNotContain("prepare 0 ms", line.Context, StringComparison.Ordinal);
        Assert.DoesNotContain("prepare 1 ms", line.Context, StringComparison.Ordinal);
    }

    [Fact]
    public void PhaseLine_ACancelledRead_ProducesOneCancelledLine_NotASlowMethodBlock()
    {
        var timer = new ReadPhaseTimer();
        timer.Add(ReadPhaseTimer.LockWait, 4);
        timer.Add(ReadPhaseTimer.Execute, 900);

        var lines = new List<(string Name, double Total, string Context)>();
        timer.Report("GetCollectionHealthAsync", (name, total, context) => lines.Add((name, total, context)), cancelled: true);

        var line = Assert.Single(lines);
        Assert.StartsWith("GetCollectionHealthAsync cancelled after 904 ms (superseded)", line.Context, StringComparison.Ordinal);
        /* "phases:" is the SLOW METHOD block's context; a superseded read must not read as a slow one. */
        Assert.DoesNotContain("phases:", line.Context, StringComparison.Ordinal);
    }

    [Fact]
    public void PhaseLine_ACancelledReadUnderTheThreshold_LogsNothing()
    {
        var timer = new ReadPhaseTimer();
        timer.Add(ReadPhaseTimer.Execute, 40);

        var lines = new List<string>();
        timer.Report("GetCollectionHealthAsync", (name, total, context) => lines.Add(context), cancelled: true);

        Assert.Empty(lines);
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

    [Theory]
    [InlineData("GetCollectionHealthAsync")]
    [InlineData("GetRecentCollectionLogAsync")]
    public async Task PhaseLine_ALiveReadBehindAHeldLock_NamesTheLockWait(string readName)
    {
        var service = new LocalDataService(_duckDb);
        var lines = new List<(string Name, double Total, string Context)>();
        service.CollectionHealthPhaseSink = (name, total, context) => { lock (lines) lines.Add((name, total, context)); };

        var release = new ManualResetEventSlim();
        var held = HoldWriteLock(_duckDb, release, out var acquired);
        await acquired.WaitAsync(TimeSpan.FromSeconds(30));
        var waitingBefore = DuckDbInitializer.WaitingReadCountForTests;
        Task read = readName == "GetCollectionHealthAsync"
            ? Task.Run(() => service.GetCollectionHealthAsync(ServerId))
            : Task.Run(() => service.GetRecentCollectionLogAsync(ServerId));

        /* Signalled, not slept: the read is known to be parked on the lock once the store counts one more waiting reader. A fixed
           delay left a starved thread pool free to start the read after the writer let go, with nothing to wait on. The
           lock is then held a further second, so the wait the read reports cannot be shorter than that. */
        var deadline = Stopwatch.StartNew();
        while (DuckDbInitializer.WaitingReadCountForTests <= waitingBefore)
        {
            Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(30), "the read never reached the lock wait");
            await Task.Delay(5);
        }
        await Task.Delay(1000);
        release.Set();
        await held;
        await read;

        var line = Assert.Single(lines);
        Assert.Equal(readName, line.Name);
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
    public async Task Cancel_WhileWaitingBehindAWriter_ThrowsPromptly_ForTheHealthLogDurationTrendsAndProbeReads_AndFreesTheLock()
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
                ct => service.GetCollectorDurationTrendAsync(ServerId, cancellationToken: ct),
                ct => service.GetQueryWindowFloorAsync(QueryWindowRelation.CollectionLog, ServerId, DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, cancellationToken: ct),
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
    public async Task Memo_ASecondRequestInsideTheInterval_FromAnotherTrigger_RunsNoQuery()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));

        var first = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        await SeedAsync(ServerId, "deadlocks", MinutesAgo(9));
        var second = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);

        /* The second request would see two collectors if it had asked the store. */
        Assert.Single(first);
        Assert.Single(second);
    }

    /// <summary>
    /// The tick is not "the second request": the tab drops the memo before every tick (<c>RefreshAllDataOnTimerAsync</c>), so a
    /// tick at the 30, 60 or 300 second setting costs a read, however recent the last one was. What the memo absorbs is a tab
    /// switch inside the interval, which costs none.
    /// </summary>
    [Theory]
    [InlineData(30)]
    [InlineData(60)]
    [InlineData(300)]
    public async Task Memo_ConsecutiveTimerTicks_EachReadTheStore_AndATabSwitchInsideTheIntervalReadsNone(int refreshSeconds)
    {
        var now = DateTime.UtcNow;
        var interval = TimeSpan.FromSeconds(refreshSeconds);
        var service = new LocalDataService(_duckDb) { CollectionHealthMemoClock = () => now };
        await SeedAsync(ServerId, "query_store", MinutesAgo(30));

        service.InvalidateCollectionHealthMemo();
        Assert.Single(await service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: interval));

        /* The next tick, one interval later: it drops the memo first, as the tab does, so it reads. */
        await SeedAsync(ServerId, "deadlocks", MinutesAgo(29));
        now += interval;
        service.InvalidateCollectionHealthMemo();
        Assert.Equal(2, (await service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: interval)).Count);

        /* A tab switch ten seconds after that tick drops nothing, so the memo answers it. */
        await SeedAsync(ServerId, "wait_stats", MinutesAgo(28));
        now += TimeSpan.FromSeconds(10);
        Assert.Equal(2, (await service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: interval)).Count);

        /* ... and the tick after it reads again. */
        now += interval;
        service.InvalidateCollectionHealthMemo();
        Assert.Equal(3, (await service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: interval)).Count);
    }

    /// <summary>
    /// The memo is aged from when the read BEGAN. This read is held behind the write lock for over 700 ms (signalled on the
    /// read reaching the wait, then held a further 700), so it ends well over the 500 ms lifetime after it started. Stamped at
    /// its start it is already stale when it lands and the next request reads; stamped when it finished, a result that old
    /// would still read as new and answer the next request.
    /// </summary>
    [Fact]
    public async Task Memo_IsAgedFromWhenTheReadStarted_NotFromWhenItFinished()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        var lifetime = TimeSpan.FromMilliseconds(500);

        var release = new ManualResetEventSlim();
        var held = HoldWriteLock(_duckDb, release, out var acquired);
        await acquired.WaitAsync(TimeSpan.FromSeconds(30));
        var waitingBefore = DuckDbInitializer.WaitingReadCountForTests;
        var slowRead = Task.Run(() => service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: lifetime));
        var deadline = Stopwatch.StartNew();
        while (DuckDbInitializer.WaitingReadCountForTests <= waitingBefore)
        {
            Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(30), "the read never reached the lock wait");
            await Task.Delay(5);
        }
        await Task.Delay(700);
        release.Set();
        await held;
        Assert.Single(await slowRead);

        await SeedAsync(ServerId, "deadlocks", MinutesAgo(9));
        var next = await service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: lifetime);

        Assert.Equal(2, next.Count);
    }

    [Fact]
    public async Task Memo_ALifetimeTheCallerNames_ReplacesTheThirtySecondDefault()
    {
        var now = DateTime.UtcNow;
        var service = new LocalDataService(_duckDb) { CollectionHealthMemoClock = () => now };
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        await service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: TimeSpan.FromSeconds(60));
        await SeedAsync(ServerId, "deadlocks", MinutesAgo(9));

        now += TimeSpan.FromSeconds(45);
        Assert.Single(await service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: TimeSpan.FromSeconds(60)));

        now += TimeSpan.FromSeconds(15);
        Assert.Equal(2, (await service.GetCollectionHealthAsync(ServerId, allowMemo: true, memoLifetime: TimeSpan.FromSeconds(60))).Count);
    }

    [Fact]
    public async Task Memo_AHitStampsTheScheduleInForceNow_NotTheOneTheMemoWasReadUnder()
    {
        var minutes = 15;
        var service = new LocalDataService(_duckDb) { CollectorFrequencyMinutes = (serverId, collector) => minutes };
        await SeedAsync(ServerId, "query_store", MinutesAgo(10));
        var first = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);
        await SeedAsync(ServerId, "deadlocks", MinutesAgo(9));

        /* The user edits the schedule: nothing refreshes the tab, so the next request finds the memo. */
        minutes = 720;
        var second = await service.GetCollectionHealthAsync(ServerId, allowMemo: true);

        /* A hit, because the new collector is not there ... */
        Assert.Single(second);
        /* ... but it carries the new cadence, not the memoized one. */
        Assert.NotEqual(first[0].EffectiveFrequencyMinutes, second[0].EffectiveFrequencyMinutes);
        Assert.Equal(720, second[0].EffectiveFrequencyMinutes);
    }

    [Fact]
    public async Task Memo_AManualRefreshOrTimerTick_RunsASecondQuery()
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
    public void TheServerTab_PassesThePassToken_ToEveryRead_AndEndsASupersededPassBeforeItRepaints()
    {
        var source = System.IO.File.ReadAllText(System.IO.Path.Combine(RepoRoot(), "Lite", "Controls", "ServerTab.Refresh.cs"));

        Assert.Contains("case 17: await RefreshCollectionHealthAsync(hoursBack, fromDate, toDate, ct); break;", source, StringComparison.Ordinal);
        Assert.Contains("GetCollectionHealthAsync(_serverId, allowMemo: true, cancellationToken: ct, memoLifetime: TimeSpan.FromSeconds(App.AutoRefreshIntervalSeconds)), ct)", source, StringComparison.Ordinal);
        Assert.Contains("GetRecentCollectionLogAsync(_serverId, hoursBack, fromDate, toDate, cancellationToken: ct), ct)", source, StringComparison.Ordinal);
        Assert.Contains("GetCollectorDurationTrendAsync(_serverId, hoursBack, fromDate, toDate, cancellationToken: ct), ct)", source, StringComparison.Ordinal);

        /* After the three reads are joined and before anything is painted or probed, a superseded pass ends. */
        var pass = source[source.IndexOf("private async System.Threading.Tasks.Task RefreshCollectionHealthAsync(", StringComparison.Ordinal)..];
        pass = pass[..pass.IndexOf("catch (OperationCanceledException) when (ct.IsCancellationRequested)", StringComparison.Ordinal)];
        var joined = pass.IndexOf("Task.WhenAll(collectionHealthTask, collectionLogTask, collectorDurationTask)", StringComparison.Ordinal);
        var cancelCheck = pass.IndexOf("ct.ThrowIfCancellationRequested();", StringComparison.Ordinal);
        var firstPaint = pass.IndexOf("_collectionHealthFilterMgr!.UpdateData(", StringComparison.Ordinal);
        var probe = pass.IndexOf("RefreshCappedGridBannerAsync(", StringComparison.Ordinal);
        Assert.True(joined >= 0 && cancelCheck > joined, "no cancellation check after the reads are joined");
        Assert.True(firstPaint > cancelCheck, "the pass repaints before it checks it was superseded");
        Assert.True(probe > cancelCheck, "the pass runs the data-start probe before it checks it was superseded");
        /* The probe itself takes the token, so a pass superseded mid-probe stops it too. */
        Assert.Contains("row => row.CollectionTime, ct: ct)", pass, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServerTab_TimerTicksDropTheMemo_ATabSwitchDoesNot_AManualRefreshDoes()
    {
        var source = System.IO.File.ReadAllText(System.IO.Path.Combine(RepoRoot(), "Lite", "Controls", "ServerTab.Refresh.cs"));

        static string Body(string source, string signature)
        {
            var from = source.IndexOf(signature, StringComparison.Ordinal);
            Assert.True(from >= 0, signature + " is gone");
            return source[from..source.IndexOf("\n    }", from, StringComparison.Ordinal)];
        }

        var timer = Body(source, "private Task RefreshAllDataOnTimerAsync()");
        Assert.Contains("InvalidateCollectionHealthMemo", timer, StringComparison.Ordinal);
        Assert.True(timer.IndexOf("InvalidateCollectionHealthMemo", StringComparison.Ordinal) < timer.IndexOf("PollAsync", StringComparison.Ordinal), "the tick must drop the memo before it polls");

        Assert.Contains("InvalidateCollectionHealthMemo", Body(source, "private Task RefreshAllDataAsync()"), StringComparison.Ordinal);

        var tabSwitch = Body(source, "private Task RefreshVisibleTabOnlyAsync()");
        Assert.DoesNotContain("InvalidateCollectionHealthMemo();", tabSwitch, StringComparison.Ordinal);
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

/// <summary>
/// #5371: the pieces of the Collection Health pass that touch process-wide statics (the app log buffer, the method profiler)
/// or are pure statics of the tab. They share the one collection that serializes every class reading
/// <c>AppLogger.DrainBufferedLines</c>, which is destructive.
/// </summary>
[Collection("app-logger-statics")]
public sealed class CollectionHealthCancellationLogTests : IDisposable
{
    private readonly string _dir = System.IO.Directory.CreateTempSubdirectory("pm-lite-5371-profiler-").FullName;
    private readonly bool _profilerWasEnabled = PerformanceMonitorLite.Helpers.MethodProfiler.IsEnabled;
    private readonly double _profilerThreshold = PerformanceMonitorLite.Helpers.MethodProfiler.ThresholdMs;
    private readonly string _profilerDir = PerformanceMonitorLite.Helpers.MethodProfiler.GetLogDirectory();

    public void Dispose()
    {
        PerformanceMonitorLite.Helpers.MethodProfiler.SetEnabled(_profilerWasEnabled);
        PerformanceMonitorLite.Helpers.MethodProfiler.SetThresholdMs(_profilerThreshold);
        if (!string.IsNullOrEmpty(_profilerDir))
        {
            PerformanceMonitorLite.Helpers.MethodProfiler.Initialize(_profilerDir);
        }

        try { System.IO.Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public void ACancelledRead_WritesOneCancelledLineToTheAppLog_AndNothingElse()
    {
        var timer = new ReadPhaseTimer();
        timer.Add(ReadPhaseTimer.Execute, 1200);
        AppLogger.DrainBufferedLines();

        timer.Report("GetCollectionHealthAsync", cancelled: true);

        var line = Assert.Single(AppLogger.DrainBufferedLines(), l => l.Contains("GetCollectionHealthAsync", StringComparison.Ordinal));
        Assert.Contains("cancelled after 1200 ms (superseded)", line, StringComparison.Ordinal);
        Assert.DoesNotContain("SLOW METHOD", line, StringComparison.Ordinal);
    }

    /// <summary>The outer <c>MethodProfiler.TimeAsync</c> the tab wraps each read in must not write its own SLOW METHOD block for a read a newer request stopped.</summary>
    [Fact]
    public async Task TheProfilersTimeAsync_WritesNoSlowMethodBlock_ForACancelledOperation_ButStillDoesForAFailedOne()
    {
        PerformanceMonitorLite.Helpers.MethodProfiler.Initialize(_dir);
        PerformanceMonitorLite.Helpers.MethodProfiler.SetThresholdMs(0);
        PerformanceMonitorLite.Helpers.MethodProfiler.SetEnabled(true);
        var logFile = PerformanceMonitorLite.Helpers.MethodProfiler.GetCurrentLogFile();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => PerformanceMonitorLite.Helpers.MethodProfiler.TimeAsync<int>(
            "cancelled-context", async () => { await Task.Delay(5); throw new OperationCanceledException(); }));
        var afterCancelled = System.IO.File.Exists(logFile) ? System.IO.File.ReadAllText(logFile) : "";
        Assert.DoesNotContain("cancelled-context", afterCancelled, StringComparison.Ordinal);

        await Assert.ThrowsAsync<InvalidOperationException>(() => PerformanceMonitorLite.Helpers.MethodProfiler.TimeAsync<int>(
            "failed-context", async () => { await Task.Delay(5); throw new InvalidOperationException(); }));
        Assert.Contains("failed-context", System.IO.File.ReadAllText(logFile), StringComparison.Ordinal);
    }

    [Fact]
    public async Task SafeQueryAsync_RethrowsACancellationTheTokenCaused_InsteadOfSwallowingItIntoAnEmptyList()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => PerformanceMonitorLite.Controls.ServerTab.SafeQueryAsync<int>(
            () => throw new OperationCanceledException(cts.Token), cts.Token));
    }

    [Fact]
    public async Task SafeQueryAsync_StillReturnsAnEmptyList_ForARealFailure_AndForACancellationNobodyAskedFor()
    {
        using var live = new CancellationTokenSource();

        var failed = await PerformanceMonitorLite.Controls.ServerTab.SafeQueryAsync<int>(
            () => throw new InvalidOperationException("the store is unreadable"), live.Token);
        var stray = await PerformanceMonitorLite.Controls.ServerTab.SafeQueryAsync<int>(
            () => throw new OperationCanceledException(), live.Token);
        var noToken = await PerformanceMonitorLite.Controls.ServerTab.SafeQueryAsync<int>(
            () => throw new InvalidOperationException("no token at all"));

        Assert.Empty(failed);
        Assert.Empty(stray);
        Assert.Empty(noToken);
    }

    [Fact]
    public async Task TheDataStartProbe_RethrowsACancellationTheTokenCaused_ButAStrayOneIsAFailedProbe()
    {
        var start = DateTime.UtcNow.AddDays(-7);
        var end = DateTime.UtcNow;
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => PerformanceMonitorLite.Controls.ServerTab.ProbeWindowFloorOrNullAsync(
            () => throw new OperationCanceledException(cts.Token), "probe", start, end, cts.Token));

        using var live = new CancellationTokenSource();
        Assert.Null(await PerformanceMonitorLite.Controls.ServerTab.ProbeWindowFloorOrNullAsync(
            () => throw new OperationCanceledException(), "probe", start, end, live.Token));
    }
}
