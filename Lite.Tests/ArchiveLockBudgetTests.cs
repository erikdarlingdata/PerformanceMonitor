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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5731: the archive's write-lock waits have a budget.
///
/// <para>The store lock is a <c>ReaderWriterLockSlim</c>, and a waiting writer makes every NEW reader wait, so an
/// archive promote that waited without a limit behind one slow read held every collector off for as long as that
/// read ran. The promote plus DELETE, the recovery of an interrupted export, the compaction swap and the
/// swap-journal replay now wait <see cref="ArchiveService.WriteLockBudget"/> and skip their unit on a timeout; the
/// pass reports it by returning false, and <c>RunArchivalIfDueAsync</c> then retries in minutes, not an hour. The
/// size-triggered reset keeps its unbounded wait.</para>
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveLockBudgetTests : IDisposable
{
    private static readonly TimeSpan Budget = TimeSpan.FromMilliseconds(200);

    /* A pass that never gives up is stopped by this and reported as the failure it is, not left to hang the run. */
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveLockBudgetTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    private async Task<long> CountAsync(string table)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /* 50 old collection_log rows and 20 old config_alert_log rows, all far older than the 7-day cutoff. */
    private async Task<DuckDbInitializer> SeedAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(1, 51) t(i)");
        await ExecAsync(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value)
SELECT TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 1, 'S1', 'Blocking Detected', i, 10 FROM range(1, 21) t(i)");

        return initializer;
    }

    private string[] ArchiveFiles() =>
        Directory.GetFiles(_archiveDir).Select(f => Path.GetFileName(f)).Order(StringComparer.Ordinal).ToArray();

    /// <summary>A read lock held on its own thread until released: the slow UI read the writer queues behind.</summary>
    private sealed class HeldRead : IDisposable
    {
        private readonly ManualResetEventSlim _release = new(false);
        private readonly Thread _thread;

        public HeldRead(DuckDbInitializer initializer)
        {
            using var taken = new ManualResetEventSlim(false);
            _thread = new Thread(() =>
            {
                using (initializer.AcquireReadLock())
                {
                    taken.Set();
                    _release.Wait();
                }
            }) { IsBackground = true, Name = "ArchiveLockBudgetTests.HeldRead" };
            _thread.Start();
            Assert.True(taken.Wait(TimeSpan.FromSeconds(10)), "the blocking read lock was not taken");
        }

        public void Dispose()
        {
            _release.Set();
            _thread.Join(TimeSpan.FromSeconds(10));
            _release.Dispose();
        }
    }

    [Fact]
    public async Task APromoteBehindAHeldRead_GivesUpWithinItsBudget_KeepsTheRows_AndLeavesNothingBehind()
    {
        var initializer = await SeedAsync();
        var service = new ArchiveService(initializer, _archiveDir) { WriteLockBudget = Budget };

        bool complete;
        var worstReaderWait = TimeSpan.Zero;
        var elapsed = Stopwatch.StartNew();
        using (new HeldRead(initializer))
        {
            /* A reader that keeps asking while the pass runs. The writer queued behind the held read blocks every NEW
               reader, so each wait it sees is the writer's wait; once the budget is spent the reader gets in. */
            using var stop = new CancellationTokenSource();
            var probe = Task.Run(() =>
            {
                while (!stop.IsCancellationRequested)
                {
                    var waited = Stopwatch.StartNew();
                    using (initializer.AcquireReadLock())
                    {
                        waited.Stop();
                    }

                    if (waited.Elapsed > worstReaderWait)
                    {
                        worstReaderWait = waited.Elapsed;
                    }

                    stop.Token.WaitHandle.WaitOne(5);
                }
            });

            /* On the pool: DuckDB.NET's async calls complete synchronously, so a pass that waits for the lock would block
               this thread, and the guard below would never get to fire. */
            var pass = Task.Run(() => service.ArchiveOldDataAsync(hotDataDays: 7));
            var finished = await Task.WhenAny(pass, Task.Delay(HangGuard, TestContext.Current.CancellationToken));
            var hung = finished != pass;
            await stop.CancelAsync();

            /* Free the held read either way, so a hung pass is reported rather than left to hang the class. */
            if (hung)
            {
                Assert.Fail($"the archive pass was still waiting for the write lock after {HangGuard.TotalSeconds:0} s: the wait has no budget");
            }

            complete = await pass;
            await probe;
        }

        elapsed.Stop();

        Assert.False(complete);

        /* Two tables had old rows, so two waits of one budget each; a generous ceiling that an unbounded wait,
           which lasts as long as the read, cannot meet. */
        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(20), $"the pass took {elapsed.Elapsed.TotalSeconds:0.0} s");
        Assert.True(worstReaderWait < Budget + TimeSpan.FromSeconds(5),
            $"a reader taken while the promote waited waited {worstReaderWait.TotalMilliseconds:0} ms; the budget is {Budget.TotalMilliseconds:0} ms");

        /* Nothing was promoted: no parquet, no leftover temp, no journal. The rows are still in their tables. */
        Assert.Empty(ArchiveFiles());
        Assert.Equal(50, await CountAsync("collection_log"));
        Assert.Equal(20, await CountAsync("config_alert_log"));
    }

    [Fact]
    public async Task TheSkippedTables_AreArchivedOnTheNextPass_OnceTheLockIsFree()
    {
        var initializer = await SeedAsync();
        var service = new ArchiveService(initializer, _archiveDir) { WriteLockBudget = Budget };

        using (new HeldRead(initializer))
        {
            var skipped = Task.Run(() => service.ArchiveOldDataAsync(hotDataDays: 7));
            Assert.True(await Task.WhenAny(skipped, Task.Delay(HangGuard, TestContext.Current.CancellationToken)) == skipped,
                "the archive pass waited for the write lock without a budget");
            Assert.False(await skipped);
        }

        Assert.Equal(50, await CountAsync("collection_log"));

        var second = new ArchiveService(initializer, _archiveDir)
        {
            WriteLockBudget = Budget,
            TimestampForTests = DateTime.UtcNow.AddHours(1).ToString("yyyyMMdd_HHmm")
        };
        Assert.True(await second.ArchiveOldDataAsync(hotDataDays: 7));

        Assert.Equal(0, await CountAsync("collection_log"));
        Assert.Equal(0, await CountAsync("config_alert_log"));
        Assert.Equal(50, await CountAsync("v_collection_log"));
    }

    [Fact]
    public async Task ACompactionSwapBehindAHeldRead_LeavesTheGroupForTheNextPass()
    {
        var initializer = await SeedAsync();
        MakeParquet("20260901_0100_collection_log.parquet", 1, 11);
        MakeParquet("20260901_0200_collection_log.parquet", 11, 21);
        var service = new ArchiveService(initializer, _archiveDir) { WriteLockBudget = Budget };
        var before = ArchiveFiles();

        bool complete;
        using (new HeldRead(initializer))
        {
            var run = Task.Run(() => service.CompactParquetFiles());
            var finished = await Task.WhenAny(run, Task.Delay(HangGuard, TestContext.Current.CancellationToken));
            Assert.True(finished == run, "the compaction swap waited for the write lock without a budget");
            complete = await run;
        }

        Assert.False(complete);
        Assert.Equal(before, ArchiveFiles());

        /* The lock is free now: the same group compacts, into the month's file. */
        Assert.True(service.CompactParquetFiles());
        Assert.Equal(new[] { "202609_collection_log.parquet" }, ArchiveFiles());
    }

    [Fact]
    public async Task ASwapJournalReplayBehindAHeldRead_IsRetriedOnTheNextPass_AndItsGroupIsNotMerged()
    {
        var initializer = await SeedAsync();
        MakeParquet("20260901_0100_collection_log.parquet", 1, 11);
        MakeParquet("20260901_0200_collection_log.parquet", 11, 21);
        /* A journal for the group, as an earlier run that died mid-swap leaves it. It names nothing to resolve, so
           a replay that gets the lock simply removes it. */
        var journal = Path.Combine(_archiveDir, "202609_collection_log.swap");
        File.WriteAllText(journal, string.Empty);
        var service = new ArchiveService(initializer, _archiveDir) { WriteLockBudget = Budget };
        var before = ArchiveFiles();

        bool complete;
        using (new HeldRead(initializer))
        {
            var run = Task.Run(() => service.CompactParquetFiles());
            var finished = await Task.WhenAny(run, Task.Delay(HangGuard, TestContext.Current.CancellationToken));
            Assert.True(finished == run, "the swap-journal replay waited for the write lock without a budget");
            complete = await run;
        }

        Assert.False(complete);
        Assert.Equal(before, ArchiveFiles());
        Assert.True(File.Exists(journal));
    }

    private void MakeParquet(string fileName, long from, long to)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        var path = Path.Combine(_archiveDir, fileName).Replace("\\", "/");
        cmd.CommandText = $"COPY (SELECT i AS log_id, 1 AS server_id, 'S1' AS server_name, 'wait_stats' AS collector_name, TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE AS collection_time, 'SUCCESS' AS status FROM range({from}, {to}) t(i)) TO '{path}' (FORMAT PARQUET)";
        cmd.ExecuteNonQuery();
    }

    /* ---- cancellation (#5731): checked between tables and before compaction ---- */

    [Fact]
    public async Task ACancelledToken_StopsThePassBetweenTables_AndReportsItIncomplete()
    {
        var initializer = await SeedAsync();
        using var cts = new CancellationTokenSource();
        var promoted = new List<string>();
        var service = new ArchiveService(initializer, _archiveDir)
        {
            WriteLockBudget = TimeSpan.FromSeconds(30),
            /* Cancel once the first table is promoted: the pass must stop before the second. */
            AfterPromoteForTests = table =>
            {
                promoted.Add(table);
                cts.Cancel();
            }
        };

        var complete = await service.ArchiveOldDataAsync(hotDataDays: 7, cancellationToken: cts.Token);

        Assert.False(complete);
        var only = Assert.Single(promoted);

        /* The table that was promoted is archived and emptied; the other was not touched. */
        var counts = new Dictionary<string, long>
        {
            ["collection_log"] = await CountAsync("collection_log"),
            ["config_alert_log"] = await CountAsync("config_alert_log"),
        };
        Assert.Equal(0, counts[only]);
        var untouched = counts.Single(kv => kv.Key != only);
        Assert.Equal(untouched.Key == "collection_log" ? 50L : 20L, untouched.Value);
        Assert.Single(ArchiveFiles(), f => f.EndsWith(".parquet", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ATokenCancelledBeforeThePass_ArchivesNothing_AndSkipsCompaction()
    {
        var initializer = await SeedAsync();
        MakeParquet("20260901_0100_collection_log.parquet", 1, 11);
        MakeParquet("20260901_0200_collection_log.parquet", 11, 21);
        var before = ArchiveFiles();
        var service = new ArchiveService(initializer, _archiveDir);

        var complete = await service.ArchiveOldDataAsync(hotDataDays: 7, cancellationToken: new CancellationToken(canceled: true));

        Assert.False(complete);
        Assert.Equal(before, ArchiveFiles());
        Assert.Equal(50, await CountAsync("collection_log"));
        Assert.Equal(20, await CountAsync("config_alert_log"));
    }

    [Fact]
    public async Task APassThatFinishesEveryTable_ReturnsTrue()
    {
        var initializer = await SeedAsync();
        var service = new ArchiveService(initializer, _archiveDir) { WriteLockBudget = TimeSpan.FromSeconds(30) };

        Assert.True(await service.ArchiveOldDataAsync(hotDataDays: 7));
        Assert.Equal(0, await CountAsync("collection_log"));
    }

    /* ---- the retry stamp (#5731) ---- */

    [Fact]
    public void ACompletePass_StampsTheClock_AnIncompleteOneStampsAFiveMinuteRetry()
    {
        var now = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);
        var interval = CollectionBackgroundService.ArchiveInterval;

        var complete = CollectionBackgroundService.ArchiveStampAfterRun(true, now);
        Assert.Equal(now, complete);
        Assert.False(CollectionBackgroundService.HousekeepingIsDue(complete, interval, now + interval - TimeSpan.FromTicks(1)));
        Assert.True(CollectionBackgroundService.HousekeepingIsDue(complete, interval, now + interval));

        var retry = CollectionBackgroundService.ArchiveStampAfterRun(false, now);
        Assert.Equal(TimeSpan.FromMinutes(5), CollectionBackgroundService.ArchiveRetryDelay);
        Assert.False(CollectionBackgroundService.HousekeepingIsDue(retry, interval, now));
        Assert.False(CollectionBackgroundService.HousekeepingIsDue(retry, interval, now + TimeSpan.FromMinutes(5) - TimeSpan.FromTicks(1)));
        Assert.True(CollectionBackgroundService.HousekeepingIsDue(retry, interval, now + TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task RunArchivalIfDueAsync_AfterAPassThatSkippedATable_StampsARetryAboutFiveMinutesOut()
    {
        var initializer = await SeedAsync();
        var archive = new ArchiveService(initializer, _archiveDir) { WriteLockBudget = Budget };
        var service = new CollectionBackgroundService(null!, initializer, archiveService: archive);
        const BindingFlags nonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
        var stampField = typeof(CollectionBackgroundService).GetField("_lastArchiveTime", nonPublic)!;
        stampField.SetValue(service, DateTime.UtcNow.AddDays(-2));
        var step = typeof(CollectionBackgroundService).GetMethod("RunArchivalIfDueAsync", nonPublic)!;
        var args = step.GetParameters()
            .Select(p => p.ParameterType == typeof(CancellationToken) ? (object)CancellationToken.None : null)
            .ToArray();

        var started = DateTime.UtcNow;
        using (new HeldRead(initializer))
        {
            await (Task)step.Invoke(service, args)!;
        }

        var stamp = (DateTime)stampField.GetValue(service)!;
        var dueIn = stamp + CollectionBackgroundService.ArchiveInterval - DateTime.UtcNow;
        Assert.InRange(dueIn, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(5));
        Assert.True(stamp > started - CollectionBackgroundService.ArchiveInterval, "the stamp is the retry stamp, not the old one");

        /* The lock is free now. The next pass is not due for five minutes, and a pass that runs when it is
           due finishes and stamps the full interval. */
        stampField.SetValue(service, DateTime.UtcNow.AddDays(-2));
        archive.WriteLockBudget = TimeSpan.FromSeconds(30);
        archive.TimestampForTests = DateTime.UtcNow.AddHours(1).ToString("yyyyMMdd_HHmm");
        await (Task)step.Invoke(service, args)!;
        var full = (DateTime)stampField.GetValue(service)!;
        Assert.True(DateTime.UtcNow - full < TimeSpan.FromMinutes(1));
    }

    [Fact]
    public async Task RunArchivalIfDueAsync_AStopDuringThePass_EndsItBetweenTables_AndStampsTheRetry()
    {
        var initializer = await SeedAsync();
        using var cts = new CancellationTokenSource();
        var promoted = new List<string>();
        var archive = new ArchiveService(initializer, _archiveDir)
        {
            WriteLockBudget = TimeSpan.FromSeconds(30),
            /* The service stops once the first table is promoted: the pass must not reach the second. */
            AfterPromoteForTests = table =>
            {
                promoted.Add(table);
                cts.Cancel();
            }
        };
        var service = new CollectionBackgroundService(null!, initializer, archiveService: archive);
        const BindingFlags nonPublic = BindingFlags.NonPublic | BindingFlags.Instance;
        var stampField = typeof(CollectionBackgroundService).GetField("_lastArchiveTime", nonPublic)!;
        var oldStamp = DateTime.UtcNow.AddDays(-2);
        stampField.SetValue(service, oldStamp);
        var step = typeof(CollectionBackgroundService).GetMethod("RunArchivalIfDueAsync", nonPublic)!;
        var args = step.GetParameters()
            .Select(p => p.ParameterType == typeof(CancellationToken) ? (object)cts.Token : null)
            .ToArray();

        await (Task)step.Invoke(service, args)!;

        /* #5731: without the stop token reaching the pass, both tables are archived and the full stamp lands. */
        Assert.Single(promoted);
        var stamp = (DateTime)stampField.GetValue(service)!;
        var dueIn = stamp + CollectionBackgroundService.ArchiveInterval - DateTime.UtcNow;
        Assert.InRange(dueIn, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(5));
        Assert.True(stamp > oldStamp, "the stamp moved off the old one, to the retry stamp");
    }

    /* ---- the reset's recovery is not deferred (#5731 review round 1) ---- */

    /* Rows and distinct keys across every archive parquet file of one table, the count a v_<table> read would see. */
    private (long Rows, long Distinct) ArchivedRows(string table, string keyColumn)
    {
        var glob = Path.Combine(_archiveDir, "*_" + table + ".parquet").Replace("\\", "/");
        if (Directory.GetFiles(_archiveDir, "*_" + table + ".parquet").Length == 0)
        {
            return (0, 0);
        }

        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT count(*), count(DISTINCT {keyColumn}) FROM read_parquet('{glob}')";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        return (Convert.ToInt64(reader.GetValue(0)), Convert.ToInt64(reader.GetValue(1)));
    }

    /* A pass killed between the promote and the DELETE: the journal is on disk, the file holds the rows, the table still holds them too. */
    private async Task KillAPassAfterThePromoteAsync(DuckDbInitializer initializer)
    {
        var killed = new ArchiveService(initializer, _archiveDir)
        {
            AfterPromoteForTests = table =>
            {
                if (table == "collection_log")
                {
                    throw new ArchiveService.SimulatedKillException();
                }
            }
        };
        await Assert.ThrowsAsync<ArchiveService.SimulatedKillException>(() => killed.ArchiveOldDataAsync(hotDataDays: 7));
        Assert.True(File.Exists(Path.Combine(_archiveDir, "collection_log.archive-pending")), "the kill left no journal");
        Assert.Equal((50, 50), ArchivedRows("collection_log", "log_id"));
    }

    [Fact]
    public async Task TheResetsRecovery_WaitsBehindAHeldRead_SoNoRowIsArchivedTwice()
    {
        var initializer = await SeedAsync();
        await KillAPassAfterThePromoteAsync(initializer);

        var service = new ArchiveService(initializer, _archiveDir) { WriteLockBudget = Budget };
        Task reset;
        using (new HeldRead(initializer))
        {
            reset = Task.Run(() => service.ArchiveAllAndResetAsync());

            /* Four budgets: a recovery on the budget has given up by now and left the journal behind. */
            await Task.Delay(Budget * 4, TestContext.Current.CancellationToken);
            Assert.False(reset.IsCompleted, "the reset finished while a read still held the lock");
            Assert.True(File.Exists(Path.Combine(_archiveDir, "collection_log.archive-pending")), "the journal was resolved without the lock");
        }

        Assert.True(await Task.WhenAny(reset, Task.Delay(HangGuard, TestContext.Current.CancellationToken)) == reset,
            "the reset never finished after the read was released");
        await reset;

        /* Each row exactly once across the archive files: the recovery deleted the promoted rows, so the reset did not export them again. */
        Assert.Equal((50, 50), ArchivedRows("collection_log", "log_id"));
        Assert.Equal((20, 20), ArchivedRows("config_alert_log", "alert_time"));
        Assert.Equal(0, await CountAsync("collection_log"));
        Assert.Equal(50, await CountAsync("v_collection_log"));
        Assert.False(File.Exists(Path.Combine(_archiveDir, "collection_log.archive-pending")));
    }

    [Fact]
    public async Task TheReset_WithAJournalItCouldNotFinish_IsAbandoned_AndExportsNothing()
    {
        var initializer = await SeedAsync();
        await KillAPassAfterThePromoteAsync(initializer);

        /* A read-only journal: the recovery deletes the promoted rows and then cannot delete the journal, the
           generic-failure shape that leaves one behind even with the lock free. */
        var journal = Path.Combine(_archiveDir, "collection_log.archive-pending");
        File.SetAttributes(journal, FileAttributes.ReadOnly);
        try
        {
            var filesBefore = ArchiveFiles();
            var service = new ArchiveService(initializer, _archiveDir);
            await service.ArchiveAllAndResetAsync();

            Assert.True(File.Exists(journal));
            Assert.True(service.ResetRetryNotBeforeUtc > DateTime.UtcNow, "the abandoned reset did not back off");
            Assert.Equal((50, 50), ArchivedRows("collection_log", "log_id"));
            Assert.Equal(filesBefore, ArchiveFiles());
        }
        finally
        {
            File.SetAttributes(journal, FileAttributes.Normal);
        }
    }
}
