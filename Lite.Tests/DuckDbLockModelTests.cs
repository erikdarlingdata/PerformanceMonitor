/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2463: the lock MODEL, rather than any one caller's use of it.
///
/// <para>Lite had two conventions for locking a DuckDB write and only one of them was written down.
/// The resolution is that they are answers to two different questions — the read lock excludes
/// MAINTENANCE, the write lock additionally excludes OTHER WRITERS OF THE SAME ROWS — and the axis
/// belongs to DuckDB's optimistic concurrency control rather than to anything in
/// <c>DuckDbInitializer</c>. These tests pin the two facts that rule rests on, and pin that it is
/// stated where a caller will actually find it.</para>
///
/// <para>Only one of the three can be red on <c>dev</c>, and that is honest rather than a gap: the
/// other two pin properties that are ALREADY TRUE and whose value is that they announce when they stop
/// being true. A test for a fact is a tripwire, not a regression test, and writing one that fails today
/// would mean breaking the thing first.</para>
/// </summary>
public sealed class DuckDbLockModelTests
{
    /// <summary>
    /// The premise the whole store layer rests on: DuckDB.NET's async completes SYNCHRONOUSLY, so a lock
    /// entered before an <c>await</c> is still owned by the entering thread when it is released after one.
    ///
    /// <para><b>This is a tripwire on the driver, not a test of our code.</b>
    /// <see cref="ReaderWriterLockSlim"/> is thread-affine — <c>ExitReadLock</c> throws from a thread that
    /// did not enter — and Lite holds that lock across <c>await</c> at every store call site (66 in
    /// <c>Lite/Analysis</c> alone per #2443), on a pass that runs under <c>Task.Run</c> with no
    /// <c>SynchronizationContext</c>, where a continuation is free to resume anywhere. Nothing in our
    /// code makes that safe. What makes it safe is that no await here ever yields, so no continuation is
    /// ever scheduled.</para>
    ///
    /// <para>If a DuckDB.NET bump makes any of these genuinely asynchronous this goes red, and the
    /// paragraph on <c>DuckDbInitializer.LockReleaser</c> says what to do about it — including why
    /// guarding <c>Dispose</c> with <c>IsReadLockHeld</c> is the wrong answer, which the test below
    /// measures.</para>
    ///
    /// <para>The insert loop is long on purpose. A single moved continuation could land back on the same
    /// pool thread by chance; two hundred of them landing there is not chance.</para>
    /// </summary>
    [Fact]
    public async Task DuckDbAsyncStillCompletesOnTheCallingThread()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pm-2463-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            /* Task.Run, so there is no SynchronizationContext to pin the continuations for us --
               the same shape the analysis pass runs in. */
            await Task.Run(async () =>
            {
                var entered = Environment.CurrentManagedThreadId;

                using var connection = new DuckDBConnection($"Data Source={Path.Combine(dir, "lockmodel.duckdb")}");
                await connection.OpenAsync();
                Assert.Equal(entered, Environment.CurrentManagedThreadId);

                using (var ddl = connection.CreateCommand())
                {
                    ddl.CommandText = "CREATE TABLE t (v INTEGER)";
                    await ddl.ExecuteNonQueryAsync();
                }
                Assert.Equal(entered, Environment.CurrentManagedThreadId);

                for (var i = 0; i < 200; i++)
                {
                    using var insert = connection.CreateCommand();
                    insert.CommandText = $"INSERT INTO t VALUES ({i})";
                    await insert.ExecuteNonQueryAsync();
                }
                Assert.Equal(entered, Environment.CurrentManagedThreadId);

                using (var read = connection.CreateCommand())
                {
                    read.CommandText = "SELECT v FROM t";
                    using var reader = await read.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                    }
                }

                Assert.Equal(entered, Environment.CurrentManagedThreadId);
            });
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>
    /// Why <c>LockReleaser.Dispose</c> is NOT guarded with <c>IsReadLockHeld</c>, kept as an executable
    /// measurement because "the obvious fix is worse" is exactly the claim a later reader will doubt.
    ///
    /// <para>The guard looks free: skip the exit when this thread did not enter, and the
    /// <see cref="SynchronizationLockException"/> goes away. What actually goes away is the diagnosis.
    /// The entry the ORIGINAL thread took is still held, nothing will ever release it, and every
    /// maintenance operation in the process — CHECKPOINT, archival, compaction — blocks on it for the
    /// life of the app. An exception is loud, attributable and survivable; a leaked reader is none of
    /// those.</para>
    /// </summary>
    [Fact]
    public void GuardingTheReleaserWouldTradeAnExceptionForAPermanentlyWedgedLock()
    {
        using var rwLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        rwLock.EnterReadLock();
        try
        {
            /* What the guard would see on a continuation that resumed elsewhere. */
            var guardWouldSkip = RunOnAnotherThread(() => !rwLock.IsReadLockHeld);
            Assert.True(guardWouldSkip, "a foreign thread does not observe the entry, so the guard skips the exit");

            /* What it is being offered instead of: loud and attributable. */
            var thrown = RunOnAnotherThread<Type?>(() =>
            {
                try
                {
                    rwLock.ExitReadLock();
                    return null;
                }
                catch (Exception ex)
                {
                    return ex.GetType();
                }
            });
            Assert.Equal(typeof(SynchronizationLockException), thrown);

            /* And what skipping costs: the entry is still held and maintenance cannot get in. */
            Assert.Equal(1, rwLock.CurrentReadCount);
            var maintenanceGotIn = RunOnAnotherThread(() =>
            {
                if (!rwLock.TryEnterWriteLock(TimeSpan.FromMilliseconds(250)))
                    return false;
                rwLock.ExitWriteLock();
                return true;
            });
            Assert.False(maintenanceGotIn, "the leaked read entry blocks every maintenance writer, permanently");
        }
        finally
        {
            rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// The rule is stated where a caller will find it, and the doc comment that used to be read as the
    /// house rule no longer is. This is the one here that goes red on <c>dev</c>.
    ///
    /// <para>A source pin because what #2463 produced is a WRITTEN RULE — no call site changed, so there
    /// is no behavior to assert. The thing that can regress is the sentence, and it regresses by being
    /// deleted or by drifting back to "use the write lock for INSERT", which is the phrasing that made
    /// eleven callers look like a house rule and <c>FindingStore</c> look like a bug.</para>
    /// </summary>
    [Fact]
    public void TheLockRuleIsWrittenWhereEveryCallerAlreadyLooks()
    {
        var initializer = ParitySource.ReadFile("Lite/Database/DuckDbInitializer.cs");

        /* The rule itself, on the lock it governs. */
        Assert.Contains("WHAT IT MUST EXCLUDE, NOT BY WHETHER IT READS OR", initializer, StringComparison.Ordinal);
        Assert.Contains("#2463", initializer, StringComparison.Ordinal);

        /* The measurement that makes it a rule rather than an opinion: DuckDB fails, rather than
           queues, the loser of a write-write collision -- and an append cannot collide. */
        Assert.Contains("Conflict on update!", initializer, StringComparison.Ordinal);
        Assert.Contains("Conflict on tuple deletion!", initializer, StringComparison.Ordinal);

        /* And the latent thread-affinity hazard, with the reason its obvious mitigation is refused. */
        Assert.Contains("IsReadLockHeld", initializer, StringComparison.Ordinal);
        Assert.Contains("bug amplifier", initializer, StringComparison.Ordinal);

        /* LocalDataService must no longer read as the house rule for the whole app. */
        var localData = ParitySource.ReadFile("Lite/Services/LocalDataService.cs");
        Assert.DoesNotContain(
            "Use for UPDATE/DELETE/INSERT operations that must not race with archival or compaction.",
            localData,
            StringComparison.Ordinal);
        Assert.Contains("#2463", localData, StringComparison.Ordinal);

        /* Every store on either side of the split points at the one rule, so whichever a reader lands
           on first is where they find it. */
        foreach (var file in new[]
                 {
                     "Lite/Analysis/FindingStore.cs",
                     "Lite/Services/DuckDbAlertHistoryStore.cs",
                     "Lite/Services/DuckDbMuteRuleStore.cs",
                 })
        {
            var source = ParitySource.ReadFile(file);
            Assert.Contains("#2463", source, StringComparison.Ordinal);
            Assert.Contains("DuckDbInitializer.s_dbLock", source, StringComparison.Ordinal);
        }
    }

    private static T RunOnAnotherThread<T>(Func<T> body)
    {
        var result = default(T)!;
        var thread = new Thread(() => result = body());
        thread.Start();
        Assert.True(thread.Join(TimeSpan.FromSeconds(10)), "the probe thread did not finish");
        return result;
    }
}
