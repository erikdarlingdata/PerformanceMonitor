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

    private static Thread StartThread(Action body)
    {
        var thread = new Thread(() => body()) { IsBackground = true };
        thread.Start();
        return thread;
    }

    /* How many times a scenario may run again because a writer from another class was parked on the shared lock while it ran.
       A parked writer, whichever initializer it belongs to, holds new readers back, so a refused probe or a stalled reader pair is
       this writer's doing only when no foreign writer was parked (#8490). The foreign count is the lock's waiting-writer count less
       this initializer's own parked count, sampled while the scenario runs. The writer here is judged on its own initializer's
       counts in every attempt, with no second chance. */
    private const int ForeignWriterAttempts = 10;

    /* A writer with the shipped tail polls for the first two of its three seconds; one that is parked this soon parked at once. */
    private static readonly TimeSpan ParkedTooSoon = TimeSpan.FromMilliseconds(500);

    private static bool ForeignWriterParked(DuckDbInitializer initializer) =>
        DuckDbInitializer.WaitingWriteCountForTests - initializer.ParkedWriterCountForTests > 0;

    /// <summary>Runs <paramref name="attempt"/> until it decides (true), up to <see cref="ForeignWriterAttempts"/> times while it
    /// reports (false) that a foreign parked writer stood in the way; after that the lock's readers could not be observed.</summary>
    private static void RunUntilNoForeignWriter(Func<bool> attempt)
    {
        for (var i = 0; i < ForeignWriterAttempts; i++)
        {
            if (attempt())
            {
                return;
            }
        }

        Assert.Skip($"a writer from another class was parked on the shared lock in each of {ForeignWriterAttempts} attempts, so the lock's readers could not be observed");
    }

    [Fact]
    public void ANewReaderGetsTheLockWhileABudgetedWriterWaitsBehindALongRead()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        /* The shipped tail: this class's subject is the poll, and the host starts every other initializer parked (#8490). */
        initializer.WriteLockParkTail = DuckDbInitializer.DefaultWriteLockParkTail;
        RunUntilNoForeignWriter(() => ANewReaderGetsInOnce(initializer));
    }

    private static bool ANewReaderGetsInOnce(DuckDbInitializer initializer)
    {
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
        var foreignSeen = false;
        IDisposable? newRead = null;
        try
        {
            Assert.True(longReadHeld.Wait(HangBackstop), "the long read never took the read lock");

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
                () => initializer.PollingWriterCountForTests + initializer.ParkedWriterCountForTests > 0,
                HangBackstop,
                TimeSpan.FromMilliseconds(5),
                "the writer reached the lock wait");

            /* The writer's own mode, from its own initializer: behind the long read it polls, and does not park before its tail. */
            Assert.Equal(0, initializer.ParkedWriterCountForTests);

            /* The tab read: it arrives while the writer is pending behind the long read, and the long read is shared. */
            foreignSeen |= ForeignWriterParked(initializer);
            newRead = initializer.TryAcquireReadLock(NewReaderBound);
            foreignSeen |= ForeignWriterParked(initializer);
            if (newRead is null && foreignSeen && initializer.ParkedWriterCountForTests == 0)
            {
                return false;
            }

            Assert.True(
                newRead is not null,
                $"a new reader did not get the shared lock within {NewReaderBound.TotalMilliseconds:F0} ms while a budgeted " +
                "writer waited behind a long read: the waiting writer parked it for the rest of the read");
        }
        finally
        {
            newRead?.Dispose();
            releaseLongRead.Set();
            longRead.Join(HangBackstop);
            if (writer is not null)
            {
                writer.Join(HangBackstop);
            }
        }

        /* With the long read gone the writer gets its turn: it is still inside its budget, unless a foreign parked writer was ahead of it. */
        foreignSeen |= ForeignWriterParked(initializer);
        if (writerFailure is StoreLockTimeoutException && foreignSeen)
        {
            return false;
        }

        Assert.False(writer!.IsAlive, "the writer neither got the lock nor gave up");
        Assert.True(writerFailure is null, $"the writer did not get the lock once the long read ended: {writerFailure}");
        return true;
    }

    [Fact]
    public void AWriterPollingAgainstReadersThatNeverLeaveAGap_GetsTheLockWithinItsBudget_AndTheReadersFlowUntilItParks()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        /* The shipped tail: this class's subject is the poll, and the host starts every other initializer parked (#8490). */
        initializer.WriteLockParkTail = DuckDbInitializer.DefaultWriteLockParkTail;
        RunUntilNoForeignWriter(() => TheReadersFlowUntilTheWriterParksOnce(initializer));
    }

    private static bool TheReadersFlowUntilTheWriterParksOnce(DuckDbInitializer initializer)
    {
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
        var foreignSeen = false;
        var parkedTooSoon = false;
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
            TestWait.WaitUntil(
                () => initializer.PollingWriterCountForTests + initializer.ParkedWriterCountForTests > 0,
                HangBackstop,
                TimeSpan.FromMilliseconds(5),
                "the writer reached the lock wait");

            /* The writer's own mode, from its own initializer: it polls first, and parks only in its tail. */
            parkedTooSoon = TestWait.TryWaitUntil(() => initializer.ParkedWriterCountForTests > 0, ParkedTooSoon, TimeSpan.FromMilliseconds(5));

            /* Any foreign parked writer while this one is pending holds the readers off too, and that is not this writer's doing. */
            while (writer.IsAlive)
            {
                foreignSeen |= ForeignWriterParked(initializer);
                if (!writer.Join(TimeSpan.FromMilliseconds(5)))
                {
                    continue;
                }
            }
        }
        finally
        {
            stop.Cancel();
            readerA.Join(HangBackstop);
            readerB.Join(HangBackstop);
        }

        Assert.False(parkedTooSoon, "the writer parked at once instead of polling first");

        var starved = writerFailure is not null || waited > WriterBudget + ReaderHandoverBackstop;
        if ((starved || entriesWhileWriterPending < 50) && foreignSeen)
        {
            return false;
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
        return true;
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
