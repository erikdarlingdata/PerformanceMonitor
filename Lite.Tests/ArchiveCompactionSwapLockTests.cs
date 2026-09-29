/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Compaction swaps a group's merged files in and then rebuilds the archive views (#4720). A view keeps the
/// globs it was built with, so between the swap and the rebuild it reads only some of the files, or none:
/// DuckDB fails a read at bind when a glob matches nothing. The swap and the rebuild therefore run together
/// under the write lock, one group at a time. The merge, which is the slow part, stays outside the lock, and
/// no lock is carried from one group to the next.
///
/// <para>The archive holds two months of <c>collection_log</c>, two per-cycle files each, and a tiny batch
/// budget makes every file its own batch, so each group compacts into part files. The views were built while
/// only the per-cycle glob matched, which is what makes them stale the moment a group is swapped.</para>
///
/// <para>The same holds when a run is killed between a swap's file moves and its deletes, and the next run
/// finishes or undoes that swap from its journal: the replay resolves the journals and rebuilds the views
/// under one write lock, and takes no lock at all when there is no journal.</para>
///
/// <para>In the reset-gate collection because the write lock is one per process: a test that holds it for half a
/// second while a reader is parked behind it should not run beside the reset and sentinel tests, some of which
/// wait on it with a timeout.</para>
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveCompactionSwapLockTests : IDisposable
{
    /* 5 hot rows, plus 10 per archive file across four files. */
    private const long TotalRows = 45;

    /* How long a probe thread is given to return. The longest wait any probe makes is 5 seconds, so a thread
       that has not returned by then is stuck, and a bound the write-lock hold census (WriteLockBudgetTests)
       can read is what lets this file take the lock with no timeout. */
    private static readonly TimeSpan ProbeJoinLimit = TimeSpan.FromSeconds(10);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveCompactionSwapLockTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
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

    private async Task ExecAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /* Writes rows [from, to) of collection_log to an archive file with the table's real schema, then clears the
       hot table, the way archival does. */
    private async Task ArchiveRowsAsync(string fileName, int from, int to)
    {
        await ExecAsync($@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range({from}, {to}) t(i)");
        var path = Path.Combine(_archiveDir, fileName).Replace("\\", "/");
        await ExecAsync($"COPY collection_log TO '{path}' (FORMAT PARQUET)");
        await ExecAsync("DELETE FROM collection_log");
    }

    private async Task<(DuckDbInitializer Initializer, ArchiveService Service)> SetUpAsync(ILogger<ArchiveService>? logger = null)
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await ArchiveRowsAsync("20260801_0000_collection_log.parquet", 0, 10);
        await ArchiveRowsAsync("20260801_0100_collection_log.parquet", 10, 20);
        await ArchiveRowsAsync("20260901_0000_collection_log.parquet", 20, 30);
        await ArchiveRowsAsync("20260901_0100_collection_log.parquet", 30, 40);
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(100, 105) t(i)");
        await initializer.CreateArchiveViewsAsync();

        var service = new ArchiveService(initializer, _archiveDir, logger)
        {
            /* Every file its own batch: each group's output is two part files, which the views built above
               have no glob for yet. */
            CompactionBatchInputBytes = 1
        };
        return (initializer, service);
    }

    /* One reader, the way the app reads: under the read lock, through the view. */
    private static (long Count, string? Error) ReadTheView(DuckDbInitializer initializer)
    {
        try
        {
            using var readLock = initializer.AcquireReadLock();
            using var connection = initializer.CreateConnection();
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM v_collection_log";
            return (Convert.ToInt64(cmd.ExecuteScalar()), null);
        }
        catch (Exception ex)
        {
            return (-1, ex.Message);
        }
    }

    /// <summary>
    /// A reader that starts while a group's files have just been swapped in must not see the archive as it is
    /// between the swap and the rebuild. Before the swap and the rebuild shared the write lock, the reader
    /// started at the first group saw 25 of 45 rows (the stale view had no glob for that group's part files),
    /// and the one started at the second group failed at bind because no file matched the view's only glob.
    /// </summary>
    [Fact]
    public async Task AReaderStartedWhileAGroupIsSwapped_SeesEveryRow_AndNeverAFailure()
    {
        var (initializer, service) = await SetUpAsync();

        var readers = new List<Task<(long Count, string? Error)>>();
        service.AfterCompactionSwapForTests = _ =>
        {
            /* Not disposed here: the parked reader still signals it after this wait times out. */
            var done = new ManualResetEventSlim();
            readers.Add(Task.Run(() =>
            {
                try { return ReadTheView(initializer); }
                finally { done.Set(); }
            }));

            /* With the lock held the reader is parked behind it for the whole wait. Without it the reader is
               through in milliseconds and has read the archive in its half-swapped state. */
            done.Wait(TimeSpan.FromMilliseconds(500));
        };

        service.CompactParquetFiles();

        Assert.Equal(2, readers.Count);
        foreach (var reader in readers)
        {
            var (count, error) = await reader;
            Assert.Null(error);
            Assert.Equal(TotalRows, count);
        }
    }

    /// <summary>
    /// Where the write lock is held, seen from a second thread that asks for the read lock. The merge of a
    /// group is outside the lock, including the group after one that just swapped, so no lock is carried
    /// across groups. The swap and the view rebuild are inside it.
    /// </summary>
    [Fact]
    public async Task TheWriteLockCoversEachGroupsSwapAndRebuild_ButNotItsMerge()
    {
        var (initializer, service) = await SetUpAsync();

        var observed = new List<string>();

        /* A thread of its own, not Task.Run(...).GetResult(): a pool thread that waits on a task it just queued
           can run it inline, and the thread that holds the write lock is always let past it. */
        bool ReaderGetsIn(TimeSpan wait)
        {
            var gotIn = false;
            var probe = new Thread(() =>
            {
                using var readLock = initializer.TryAcquireReadLock(wait);
                gotIn = readLock is not null;
            });
            probe.Start();
            Assert.True(probe.Join(ProbeJoinLimit), "the probe thread did not return");
            return gotIn;
        }

        /* The lock is one per process, so another test class can hold it for a moment while a group merges: a
           reader is given seconds to get in there, and returns the moment the lock is free. At the swap the lock
           is this compaction's own, so a short wait is enough to see a reader refused. */
        service.OnCompactionTempsReadyForTests = _ =>
            observed.Add(ReaderGetsIn(TimeSpan.FromSeconds(5)) ? "merged: reader in" : "merged: reader blocked");
        service.AfterCompactionSwapForTests = _ =>
            observed.Add(ReaderGetsIn(TimeSpan.FromMilliseconds(150)) ? "swapped: reader in" : "swapped: reader blocked");

        service.CompactParquetFiles();

        Assert.Equal(
            ["merged: reader in", "swapped: reader blocked", "merged: reader in", "swapped: reader blocked"],
            observed);

        /* And the views the compaction leaves behind read the parts. */
        var (count, error) = ReadTheView(initializer);
        Assert.Null(error);
        Assert.Equal(TotalRows, count);
    }

    /// <summary>
    /// A rebuild that throws after a group's swap finished must not turn that group into a failed one (#4720).
    /// The group's files are compacted and its inputs are gone; the views are rebuilt again when compaction ends.
    /// Before this the group's catch logged "Failed to compact" and left the group out of the totals, which read
    /// as a compaction that had not happened. The rebuild fails once, at the first group and then at the second.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task AViewRebuildThatFailsAfterASwap_IsLoggedAsThat_AndTheGroupStillCounts(int failingRebuild)
    {
        var log = new CapturingLogger();
        var (initializer, service) = await SetUpAsync(log);

        var rebuilds = 0;
        initializer.OnArchiveViewRebuildForTests = () =>
        {
            if (++rebuilds == failingRebuild)
            {
                throw new InvalidOperationException("simulated view rebuild failure");
            }
        };

        service.CompactParquetFiles();

        /* One rebuild per group, and both groups swapped: the two per-cycle files of each month are gone. */
        Assert.Equal(2, rebuilds);
        Assert.Empty(Directory.GetFiles(_archiveDir, "2026*_0*_collection_log.parquet"));

        /* The failure is logged for what it is, once, and no group is reported as a compaction that failed. */
        var errors = log.Entries.Where(e => e.Level == LogLevel.Error).ToList();
        var error = Assert.Single(errors);
        Assert.Contains("archive views could not be rebuilt", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(log.Entries, e => e.Message.Contains("Failed to compact", StringComparison.Ordinal));

        /* Both groups count, and the summary line says how many rebuilds failed. */
        var summary = Assert.Single(log.Entries, e => e.Message.StartsWith("Parquet compaction complete", StringComparison.Ordinal));
        Assert.Contains("merged 2 groups, removed 4 files", summary.Message, StringComparison.Ordinal);
        Assert.Contains("view rebuilds failed: 1", summary.Message, StringComparison.Ordinal);

        /* The refresh the callers run in a finally after CompactParquetFiles rebuilds the views. */
        await initializer.CreateArchiveViewsAsync();
        var (count, readError) = ReadTheView(initializer);
        Assert.Null(readError);
        Assert.Equal(TotalRows, count);
    }

    /// <summary>
    /// With no rebuild failing, the summary line is the one it always was: no count of failed rebuilds.
    /// </summary>
    [Fact]
    public async Task WhenNoViewRebuildFails_TheSummaryLineHasNoFailureCount()
    {
        var log = new CapturingLogger();
        var (_, service) = await SetUpAsync(log);

        service.CompactParquetFiles();

        Assert.DoesNotContain(log.Entries, e => e.Level == LogLevel.Error);
        var summary = Assert.Single(log.Entries, e => e.Message.StartsWith("Parquet compaction complete", StringComparison.Ordinal));
        Assert.Contains("merged 2 groups, removed 4 files", summary.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("rebuilds failed", summary.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every view rebuild a compaction's groups run holds the write lock (#4720). The rebuild records, at its
    /// start and on its own thread, whether that thread holds the lock. Two groups make two rebuilds; one moved
    /// out of the lock is one a reader can get in front of, which the other tests only catch when their timing
    /// happens to line up.
    /// </summary>
    [Fact]
    public async Task EveryViewRebuildOfAGroupSwap_HoldsTheWriteLock()
    {
        var (initializer, service) = await SetUpAsync();

        var heldAtRebuild = new List<bool>();
        initializer.OnArchiveViewRebuildForTests = () => heldAtRebuild.Add(DuckDbInitializer.IsWriteLockHeldForTests);

        /* The seam reads the lock state of the calling thread, so it must say "not held" where it is not. */
        Assert.False(DuckDbInitializer.IsWriteLockHeldForTests);

        service.CompactParquetFiles();

        Assert.Equal(2, heldAtRebuild.Count);
        Assert.All(heldAtRebuild, held => Assert.True(held, "a group's view rebuild ran without the write lock"));
    }

    /// <summary>
    /// The same for the replay of a killed run's swap: the rebuild after the journals are resolved holds the
    /// write lock. The run also compacts the other month, so its group's rebuild is the second one recorded.
    /// </summary>
    [Fact]
    public async Task EveryViewRebuildOfAReplayedSwap_HoldsTheWriteLock()
    {
        var (initializer, service) = await SetUpAsync();
        PlantInterruptedSwap();

        var heldAtRebuild = new List<bool>();
        initializer.OnArchiveViewRebuildForTests = () => heldAtRebuild.Add(DuckDbInitializer.IsWriteLockHeldForTests);

        service.CompactParquetFiles();

        Assert.Equal(2, heldAtRebuild.Count);
        Assert.All(heldAtRebuild, held => Assert.True(held, "a view rebuild of the run that replayed a swap ran without the write lock"));
    }

    private string P(string fileName) => Path.Combine(_archiveDir, fileName).Replace("\\", "/");

    /* What a run killed after it moved a group's part files in and before it deleted the group's inputs leaves
       behind: both part files are in place next to the two files they were merged from, and the journal names
       them all. The views were built before any of this, so they know only the per-cycle files. */
    private void PlantInterruptedSwap()
    {
        File.Copy(P("20260801_0000_collection_log.parquet"), P("202608_collection_log_pt001.parquet"));
        File.Copy(P("20260801_0100_collection_log.parquet"), P("202608_collection_log_pt002.parquet"));
        File.WriteAllLines(P("202608_collection_log.swap"),
        [
            "state|swapping",
            "output|fresh|202608_collection_log_pt001.parquet",
            "output|fresh|202608_collection_log_pt002.parquet",
            "input|20260801_0000_collection_log.parquet",
            "input|20260801_0100_collection_log.parquet"
        ]);
    }

    /* Asks for the write lock from a thread of its own, for the reason the reader probe above does. */
    private static bool WriteLockIsRefused(DuckDbInitializer initializer, TimeSpan wait)
    {
        var refused = false;
        var probe = new Thread(() =>
        {
            try
            {
                using var writeLock = initializer.AcquireWriteLock(wait);
            }
            catch (TimeoutException)
            {
                refused = true;
            }
        });
        probe.Start();
        Assert.True(probe.Join(ProbeJoinLimit), "the probe thread did not return");
        return refused;
    }

    /// <summary>
    /// The replay of a killed run's swap, at the moment its journal has been resolved and the views have not
    /// been rebuilt: a second thread must be refused the write lock there. Without the lock the replay deleted
    /// the inputs a view still had globs for, with no lock held, and a reader got the archive as it was between.
    /// </summary>
    [Fact]
    public async Task TheWriteLockCoversTheReplayOfAnInterruptedSwap()
    {
        var (initializer, service) = await SetUpAsync();
        PlantInterruptedSwap();

        var replays = 0;
        var refused = false;
        var inputsGone = false;
        service.AfterCompactionReplayForTests = () =>
        {
            replays++;
            /* The replay finished the swap: the files the part files were merged from are gone, which is what
               leaves a view built before it with nothing to read for that month. */
            inputsGone = !File.Exists(P("20260801_0000_collection_log.parquet")) && !File.Exists(P("20260801_0100_collection_log.parquet"));

            /* The lock is this replay's own, so a short wait is enough to see a probe refused. */
            refused = WriteLockIsRefused(initializer, TimeSpan.FromMilliseconds(150));
        };

        service.CompactParquetFiles();

        Assert.Equal(1, replays);
        Assert.True(inputsGone, "the replay did not finish the planted swap");
        Assert.True(refused, "a second thread was given the write lock while the replay had resolved a swap and not yet rebuilt the views");

        var (count, error) = ReadTheView(initializer);
        Assert.Null(error);
        Assert.Equal(TotalRows, count);
    }

    /// <summary>
    /// A reader that starts while a killed run's swap is being replayed must not see the archive between the
    /// deletes and the rebuild. The two files the swap replaced are deleted by the replay; the views knew only
    /// the per-cycle files, so a reader in between saw 25 of 45 rows (the part files were not in the views yet).
    /// </summary>
    [Fact]
    public async Task AReaderStartedWhileAnInterruptedSwapIsReplayed_SeesEveryRow_AndNeverAFailure()
    {
        var (initializer, service) = await SetUpAsync();
        PlantInterruptedSwap();

        Task<(long Count, string? Error)>? reader = null;
        service.AfterCompactionReplayForTests = () =>
        {
            /* Not disposed here: the parked reader still signals it after this wait times out. */
            var done = new ManualResetEventSlim();
            reader = Task.Run(() =>
            {
                try { return ReadTheView(initializer); }
                finally { done.Set(); }
            });

            /* With the lock held the reader is parked behind it for the whole wait. Without it the reader is
               through in milliseconds and has read the archive in its half-replayed state. */
            done.Wait(TimeSpan.FromMilliseconds(500));
        };

        service.CompactParquetFiles();

        Assert.NotNull(reader);
        var (count, error) = await reader;
        Assert.Null(error);
        Assert.Equal(TotalRows, count);
    }

    /// <summary>
    /// The common case is no journal at all, and then the replay must not take the write lock (or rebuild the
    /// views): the compaction of an empty archive returns while another thread holds the write lock.
    /// </summary>
    [Fact]
    public async Task WithNoJournalToReplay_TheReplayTakesNoWriteLock()
    {
        var (initializer, service) = await SetUpAsync();
        foreach (var file in Directory.GetFiles(_archiveDir))
        {
            File.Delete(file);
        }

        var replays = 0;
        service.AfterCompactionReplayForTests = () => replays++;

        var compaction = new Thread(() => service.CompactParquetFiles());
        bool finished;
        using (initializer.AcquireWriteLock())
        {
            compaction.Start();
            finished = compaction.Join(ProbeJoinLimit);
        }

        /* Released above; a compaction that was parked on the lock finishes now instead of hanging the run. */
        compaction.Join(ProbeJoinLimit);

        Assert.True(finished, "the replay waited for the write lock although there was no journal to replay");
        Assert.Equal(0, replays);
    }

    private sealed class CapturingLogger : ILogger<ArchiveService>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }
}
