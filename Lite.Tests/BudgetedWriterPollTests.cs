/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Darling.Tests;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: a budgeted writer waiting for the store lock does not hold new readers back for the length of its wait.
/// <see cref="ReaderWriterLockSlim"/> admits no new reader while a writer waits, so a writer parked for its whole budget
/// behind one long archive read would hold every tab read of that time at the door for the rest of the read. The writer
/// polls for the lock instead, and parks as a waiting writer only for the tail of its budget: a read that arrives while
/// it polls gets the shared lock at once, and the writer still has the lock within its budget, because once it parks the
/// readers in flight finish and no new one gets in ahead of it.
/// </summary>
[Collection("timing")]
public sealed class BudgetedWriterPollTests : IDisposable
{
    /* The default background writer budget, LocalDataService.DefaultWriteLockBudget's value minus two seconds: long
       enough that the writer is still polling when the new reader asks, short enough to keep the test quick. */
    private static readonly TimeSpan WriterBudget = TimeSpan.FromSeconds(3);

    /* How long a new reader may wait for the shared lock while a budgeted writer is pending behind a long read. The
       status bar's bounded read is the probe, so a parked reader answers null instead of hanging the test. */
    private static readonly TimeSpan NewReaderBound = TimeSpan.FromMilliseconds(500);

    /* A reader in the never-a-gap pair exits when the other has entered, or after this long when the other cannot
       enter: that is the writer's park, and the exit is what lets it in. */
    private static readonly TimeSpan ReaderHandoverBackstop = TimeSpan.FromMilliseconds(250);

    private static readonly TimeSpan HangBackstop = TimeSpan.FromSeconds(10);

    /* How many budgeted writers are between asking for the write lock and holding it. The lock's own waiting-writer count
       sees a parked writer only; a store whose budgeted writers poll first counts them through its pending-writer seam,
       read by reflection so this file compiles against a store without one and then falls back to the parked count. */
    private static readonly PropertyInfo? s_pendingWriters =
        typeof(DuckDbInitializer).GetProperty("PendingWriteCountForTests", BindingFlags.NonPublic | BindingFlags.Static);

    private readonly string _tempDir;
    private readonly string _dbPath;

    public BudgetedWriterPollTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static int PendingWriters() =>
        s_pendingWriters is null ? DuckDbInitializer.WaitingWriteCountForTests : (int)s_pendingWriters.GetValue(null)!;

    private static Thread StartThread(Action body)
    {
        var thread = new Thread(() => body()) { IsBackground = true };
        thread.Start();
        return thread;
    }

    [Fact]
    public void ANewReaderGetsTheLockWhileABudgetedWriterWaitsBehindALongRead()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        var longReadHeld = new ManualResetEventSlim(false);
        var releaseLongRead = new ManualResetEventSlim(false);
        var longRead = StartThread(() =>
        {
            using (initializer.AcquireReadLock())
            {
                longReadHeld.Set();
                releaseLongRead.Wait(HangBackstop);
            }
        });

        Exception? writerFailure = null;
        Thread? writer = null;
        try
        {
            Assert.True(longReadHeld.Wait(HangBackstop), "the long read never took the read lock");

            var pendingBefore = PendingWriters();
            writer = StartThread(() =>
            {
                try
                {
                    using (initializer.AcquireWriteLock(WriterBudget))
                    {
                    }
                }
                catch (Exception ex)
                {
                    writerFailure = ex;
                }
            });
            TestWait.WaitUntil(
                () => PendingWriters() > pendingBefore,
                HangBackstop,
                TimeSpan.FromMilliseconds(5),
                "the writer reached the lock wait");

            /* The tab read: it arrives while the writer is pending behind the long read, and the long read is shared. */
            using var newRead = initializer.TryAcquireReadLock(NewReaderBound);
            Assert.True(
                newRead is not null,
                $"a new reader did not get the shared lock within {NewReaderBound.TotalMilliseconds:F0} ms while a budgeted " +
                "writer waited behind a long read: the waiting writer parked it for the rest of the read");
        }
        finally
        {
            releaseLongRead.Set();
            longRead.Join(HangBackstop);
        }

        /* With the long read gone the writer gets its turn: it is still inside its budget. */
        Assert.True(writer!.Join(HangBackstop), "the writer neither got the lock nor gave up");
        Assert.True(writerFailure is null, $"the writer did not get the lock once the long read ended: {writerFailure}");
    }

    [Fact]
    public void AWriterPollingAgainstReadersThatNeverLeaveAGap_GetsTheLockWithinItsBudget_AndTheReadersFlowUntilItParks()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        using var stop = new CancellationTokenSource();
        long entries = 0;

        /* Two readers pass the read lock hand over hand, so it is never free: each exits only once the other has entered
           (or, when the other is held off by a parked writer, after the handover backstop), then enters again. */
        var aIn = new SemaphoreSlim(0);
        var bIn = new SemaphoreSlim(0);
        var readerA = StartReader(initializer, mineIn: aIn, otherIn: bIn, entersFirst: true, () => Interlocked.Increment(ref entries), stop.Token);
        var readerB = StartReader(initializer, mineIn: bIn, otherIn: aIn, entersFirst: false, () => Interlocked.Increment(ref entries), stop.Token);

        long entriesWhileWriterPending = -1;
        var waited = TimeSpan.Zero;
        Exception? writerFailure = null;
        try
        {
            TestWait.WaitUntil(() => Volatile.Read(ref entries) > 100, HangBackstop, TimeSpan.FromMilliseconds(5), "the readers are passing the lock");

            var writer = StartThread(() =>
            {
                var before = Volatile.Read(ref entries);
                var started = Stopwatch.GetTimestamp();
                try
                {
                    using (initializer.AcquireWriteLock(WriterBudget))
                    {
                        waited = Stopwatch.GetElapsedTime(started);
                        entriesWhileWriterPending = Volatile.Read(ref entries) - before;
                    }
                }
                catch (Exception ex)
                {
                    writerFailure = ex;
                }
            });
            Assert.True(writer.Join(WriterBudget + HangBackstop), "the writer neither got the lock nor gave up");
        }
        finally
        {
            stop.Cancel();
            readerA.Join(HangBackstop);
            readerB.Join(HangBackstop);
        }

        /* The bound: readers that never leave a gap cannot starve the writer past its budget. */
        Assert.True(
            writerFailure is null,
            $"the writer did not get the lock within its {WriterBudget.TotalSeconds:0} s budget against readers whose every hold ends within " +
            $"{ReaderHandoverBackstop.TotalMilliseconds:0} ms of a writer parking: {writerFailure}");
        Assert.True(waited <= WriterBudget + ReaderHandoverBackstop, $"the writer waited {waited.TotalMilliseconds:F0} ms, past its budget");

        /* The readers kept flowing after the writer asked: it polled first instead of parking them at once. */
        Assert.True(
            entriesWhileWriterPending >= 50,
            $"only {entriesWhileWriterPending} read(s) got the lock after the writer asked for it: the writer parked them at once instead of polling first");
    }

    private static Thread StartReader(DuckDbInitializer initializer, SemaphoreSlim mineIn, SemaphoreSlim otherIn, bool entersFirst, Action onEntry, CancellationToken stop)
    {
        return StartThread(() =>
        {
            IDisposable? hold = null;
            try
            {
                if (!entersFirst)
                {
                    otherIn.Wait(ReaderHandoverBackstop);
                }

                hold = initializer.AcquireReadLock();
                onEntry();
                mineIn.Release();
                while (!stop.IsCancellationRequested)
                {
                    /* Bounded: the other reader cannot enter while a writer is parked, and this exit is what lets the writer in. */
                    otherIn.Wait(ReaderHandoverBackstop);
                    hold.Dispose();
                    hold = null;
                    if (stop.IsCancellationRequested)
                    {
                        break;
                    }

                    hold = initializer.AcquireReadLock();
                    onEntry();
                    mineIn.Release();
                }
            }
            finally
            {
                hold?.Dispose();
            }
        });
    }
}
