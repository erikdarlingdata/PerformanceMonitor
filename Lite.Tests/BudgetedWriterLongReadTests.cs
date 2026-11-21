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
/// #9196: a budgeted writer gets the store lock within its budget against readers that follow each other and hold the lock for
/// seconds, as one MCP client reading tool after tool does. The writer polls and never finds a gap, then parks for the tail of
/// its budget, and a parked writer waits out the read in flight. A tail shorter than that read timed the writer out. The tail now
/// follows the longest read hold the initializer has seen, and stays inside the #7256 rule that a writer polls first so a tab read
/// is not held at the door for the writer's whole budget. The times here are the issue's, at about a seventh of its scale.
/// </summary>
[Collection("timing")]
public sealed class BudgetedWriterLongReadTests : IDisposable
{
    /* The issue's reads held 1.3 to 2.6 s against a 5 s budget: here 400 ms reads against a 1 s budget. */
    private static readonly TimeSpan ReadHold = TimeSpan.FromMilliseconds(400);
    private static readonly TimeSpan WriterBudget = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan ShortTail = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan HangBackstop = TimeSpan.FromSeconds(10);

    private readonly string _tempDir;

    public BudgetedWriterLongReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
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

    private DuckDbInitializer NewInitializer(TimeSpan tail) =>
        new(Path.Combine(_tempDir, "test.duckdb")) { WriteLockParkTail = tail };

    private static void AssertNear(TimeSpan expected, TimeSpan actual) =>
        Assert.True((expected - actual).Duration() < TimeSpan.FromMilliseconds(1), $"expected {expected.TotalSeconds:0.###} s, got {actual.TotalSeconds:0.###} s");

    private static Thread StartThread(Action body)
    {
        var thread = new Thread(() => body()) { IsBackground = true };
        thread.Start();
        return thread;
    }

    [Fact]
    public void AWriterGetsTheLockWithinItsBudget_AgainstReadsThatFollowEachOtherAndEachHoldLongerThanTheTail()
    {
        using var initializer = NewInitializer(ShortTail);
        using var stop = new CancellationTokenSource();
        long started = 0;
        long finished = 0;

        /* One client reading back to back: each read takes the lock the moment the last one lets go. */
        var reader = StartThread(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                using (initializer.AcquireReadLock())
                {
                    Interlocked.Increment(ref started);
                    TestWait.Elapse(ReadHold);
                }

                Interlocked.Increment(ref finished);
            }
        });

        TimeSpan waited = TimeSpan.Zero;
        Exception? failure = null;
        try
        {
            /* The reads before the writer asks are what the lock has to learn the read length from. */
            TestWait.WaitUntil(() => Volatile.Read(ref finished) >= 2, HangBackstop, TimeSpan.FromMilliseconds(5), "two reads finished");

            /* Ask at the start of a read: the park then falls in the middle of one that outlasts the writer's budget. */
            var seen = Volatile.Read(ref started);
            TestWait.WaitUntil(() => Volatile.Read(ref started) > seen, HangBackstop, TimeSpan.FromMilliseconds(1), "a read started");

            var writerStarted = Stopwatch.GetTimestamp();
            try
            {
                using (initializer.AcquireWriteLock(WriterBudget))
                {
                    waited = Stopwatch.GetElapsedTime(writerStarted);
                }
            }
            catch (Exception ex)
            {
                failure = ex;
            }
        }
        finally
        {
            stop.Cancel();
            reader.Join(HangBackstop);
        }

        Assert.True(failure is null, $"the writer did not get the lock within its {WriterBudget.TotalMilliseconds:0} ms budget against back-to-back {ReadHold.TotalMilliseconds:0} ms reads: {failure}");
        Assert.True(waited < WriterBudget, $"the writer waited {waited.TotalMilliseconds:F0} ms");
    }

    [Fact]
    public void AWriterStillPollsFirst_AfterLongReadsHaveLengthenedItsTail_SoANewReaderIsNotHeldAtTheDoor()
    {
        using var initializer = NewInitializer(ShortTail);
        /* A read just inside the cap on the writer's budget was seen: the tail grows to its cap and no further. */
        initializer.RecordReadHold(TimeSpan.FromMilliseconds(700));

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
                TimeSpan.FromMilliseconds(1),
                "the writer reached the lock wait");

            Assert.Equal(0, initializer.ParkedWriterCountForTests);
            using var newRead = initializer.TryAcquireReadLock(TimeSpan.FromMilliseconds(100));
            Assert.True(newRead is not null, "a new reader was held at the door while the writer was still inside the polling part of its budget");
        }
        finally
        {
            releaseLongRead.Set();
            longRead.Join(HangBackstop);
        }

        Assert.True(writer!.Join(HangBackstop), "the writer neither got the lock nor gave up");
        Assert.True(writerFailure is null, $"the writer did not get the lock once the long read ended: {writerFailure}");
    }

    [Fact]
    public void TheTailFollowsTheLongestReadSeen_BetweenTheConfiguredTailAndTheShareOfTheBudgetItMayTake()
    {
        var budget = TimeSpan.FromSeconds(5);
        using var initializer = NewInitializer(TimeSpan.FromSeconds(1));

        AssertNear(TimeSpan.FromSeconds(1), initializer.EffectiveWriteLockParkTail(budget));

        initializer.RecordReadHold(TimeSpan.FromMilliseconds(300));
        AssertNear(TimeSpan.FromSeconds(1), initializer.EffectiveWriteLockParkTail(budget));

        initializer.RecordReadHold(TimeSpan.FromSeconds(2));
        AssertNear(TimeSpan.FromSeconds(2.5), initializer.EffectiveWriteLockParkTail(budget));

        initializer.RecordReadHold(TimeSpan.FromSeconds(1));
        AssertNear(TimeSpan.FromSeconds(2.5), initializer.EffectiveWriteLockParkTail(budget));

        /* A read longer than the cap (4 s of a 5 s budget) cannot be outwaited, so it leaves the tail where the 2 s read put it. */
        initializer.RecordReadHold(TimeSpan.FromSeconds(30));
        AssertNear(TimeSpan.FromSeconds(2.5), initializer.EffectiveWriteLockParkTail(budget));

        initializer.RecordReadHold(TimeSpan.FromSeconds(3.9));
        AssertNear(TimeSpan.FromSeconds(4), initializer.EffectiveWriteLockParkTail(budget));

        /* A tail no shorter than the budget parks at once, whatever was seen. */
        initializer.WriteLockParkTail = TimeSpan.FromMinutes(2);
        AssertNear(budget, initializer.EffectiveWriteLockParkTail(budget));
    }

    [Fact]
    public void AReadLongerThanTheMemoryIsForgotten_AndARealReadHoldIsWhatTheLockLearns()
    {
        using var initializer = NewInitializer(ShortTail);
        initializer.ReadHoldMemory = TimeSpan.FromMilliseconds(300);

        using (initializer.AcquireReadLock())
        {
            TestWait.Elapse(ReadHold);
        }

        Assert.True(
            initializer.EffectiveWriteLockParkTail(TimeSpan.FromSeconds(5)) >= ReadHold,
            "a read held for the length of the memory did not lengthen the tail");

        TestWait.Elapse(ReadHold);
        AssertNear(ShortTail, initializer.EffectiveWriteLockParkTail(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void AReadLongerThanTheCapIsIgnored_SoShortReadsAfterItSizeTheTail()
    {
        var budget = TimeSpan.FromSeconds(5);
        using var initializer = NewInitializer(TimeSpan.FromSeconds(1));

        initializer.RecordReadHold(TimeSpan.FromMinutes(3));
        initializer.RecordReadHold(TimeSpan.FromSeconds(2));
        initializer.RecordReadHold(TimeSpan.FromSeconds(1.5));

        AssertNear(TimeSpan.FromSeconds(2.5), initializer.EffectiveWriteLockParkTail(budget));
        /* The cap is the writer's: against a 5 minute budget the 3 minute read fits inside it and counts. */
        AssertNear(TimeSpan.FromMinutes(3.75), initializer.EffectiveWriteLockParkTail(TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public void AReadLongerThanTheCapAlone_LeavesTheConfiguredTail()
    {
        var budget = TimeSpan.FromSeconds(5);
        using var initializer = NewInitializer(TimeSpan.FromSeconds(1));

        initializer.RecordReadHold(TimeSpan.FromMinutes(3));

        AssertNear(TimeSpan.FromSeconds(1), initializer.EffectiveWriteLockParkTail(budget));
    }
}
