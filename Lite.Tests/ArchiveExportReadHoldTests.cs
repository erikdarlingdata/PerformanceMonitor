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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5691: the periodic archive export holds the store read lock once per table, and once per time slice of a table
/// too big for one hold, never across two. A writer that queues while the export reads waits for one hold, not for
/// the export: every background writer gives up after <see cref="ArchiveService.WriteLockBudget"/>.
///
/// <para>The holds are recorded through <see cref="ArchiveService.ExportReadHoldForTests"/>, which fires right after
/// each read hold is taken and right before it is released. The writer is a real <c>AcquireWriteLock</c> on its own
/// thread, queued from inside a hold, with the background budget.</para>
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveExportReadHoldTests : IDisposable
{
    /* The background writers' budget (LocalDataService.WriteLockBudget's default). */
    private static readonly TimeSpan WriterBudget = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(60);

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveExportReadHoldTests()
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

    private async Task<long> ScalarAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /* Every archive file of the table, by the glob the views use. */
    private string ArchiveGlob(string table) => Path.Combine(_archiveDir, $"*_{table}.parquet").Replace("\\", "/");

    private async Task<DuckDbInitializer> InitializeAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();
        return initializer;
    }

    /* Old rows, far older than the 7-day cutoff, in three tables: ids 1..rows, one minute apart. */
    private async Task SeedThreeTablesAsync(int rows)
    {
        await ExecAsync($@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(1, {rows + 1}) t(i)");
        await ExecAsync($@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value)
SELECT TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 1, 'S1', 'Blocking Detected', i, 10 FROM range(1, {rows + 1}) t(i)");
        await ExecAsync($@"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type)
SELECT i, TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 1, 'S1', 'W' || i FROM range(1, {rows + 1}) t(i)");
    }

    private sealed record HoldEvent(string Table, int Slice, bool Entered);

    /// <summary>
    /// A writer on its own thread, queued with the background budget. It records the hold events it found when it got
    /// the lock, runs <paramref name="whileHeld"/> under it, and releases.
    /// </summary>
    private static Task<HoldEvent[]> QueueWriter(DuckDbInitializer initializer, List<HoldEvent> events, Func<Task>? whileHeld = null)
    {
        return Task.Factory.StartNew(() =>
        {
            using (initializer.AcquireWriteLock(WriterBudget))
            {
                HoldEvent[] seen;
                lock (events)
                {
                    seen = events.ToArray();
                }

                whileHeld?.Invoke().GetAwaiter().GetResult();
                return seen;
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    /* Signalled, not slept: the hold stays open until the store counts the queued writer, so the writer is known to be
       parked on the lock before the export releases it. Counted from before the writer was queued: the lock is the whole
       process's, and another class's writer may be waiting too. A fixed delay let a slow runner start the writer after the hold
       ended, and it then queued behind the next one. */
    private static void WaitUntilWriterQueued(int waitingBefore)
    {
        /* Counted in polls, not timed: up to about 30 s, a hang guard only. */
        for (var polls = 0; DuckDbInitializer.WaitingWriteCountForTests <= waitingBefore; polls++)
        {
            Assert.True(polls < 6000, "the writer never reached the lock wait");
            Thread.Sleep(5);
        }
    }

    private static void RecordInto(ArchiveService service, List<HoldEvent> events, Action<HoldEvent>? onEvent = null)
    {
        service.ExportReadHoldForTests = (table, slice, entered) =>
        {
            var e = new HoldEvent(table, slice, entered);
            lock (events)
            {
                events.Add(e);
            }

            onEvent?.Invoke(e);
        };
    }

    /* Every hold is released before the next is taken, and each covers one table. */
    private static void AssertHoldsNeverOverlap(List<HoldEvent> events)
    {
        Assert.True(events.Count % 2 == 0, "a read hold was taken and never released");
        for (var i = 0; i < events.Count; i += 2)
        {
            Assert.True(events[i].Entered, $"event {i} released a hold that was not taken");
            Assert.False(events[i + 1].Entered, $"a read hold on {events[i].Table} was still open when the next was taken");
            Assert.Equal(events[i].Table, events[i + 1].Table);
            Assert.Equal(events[i].Slice, events[i + 1].Slice);
        }
    }

    private static async Task<bool> RunPassAsync(ArchiveService service)
    {
        /* On the pool: DuckDB.NET's async calls complete synchronously, so a pass blocked on the lock would block this
           thread and the guard would never fire. */
        var pass = Task.Run(() => service.ArchiveOldDataAsync(hotDataDays: 7));
        var finished = await Task.WhenAny(pass, Task.Delay(HangGuard, TestContext.Current.CancellationToken));
        Assert.True(finished == pass, $"the archive pass did not finish in {HangGuard.TotalSeconds:0} s");
        return await pass;
    }

    [Fact]
    public async Task AnExportOverSeveralTables_HoldsTheReadLockOncePerTable_AndAQueuedWriterGetsInBetweenTables()
    {
        var initializer = await InitializeAsync();
        await SeedThreeTablesAsync(rows: 50);
        var service = new ArchiveService(initializer, _archiveDir) { TimestampForTests = "20261001_0000" };

        var seeded = new[] { "collection_log", "config_alert_log", "wait_stats" };
        var first = ArchiveService.ArchivableTables.Select(t => t.Table).First(seeded.Contains);
        var events = new List<HoldEvent>();
        Task<HoldEvent[]>? writer = null;
        RecordInto(service, events, e =>
        {
            if (writer is null && e.Table == first && e.Entered)
            {
                /* Queue the writer while this table's export holds the read lock, and wait until it is parked on the lock. */
                var waitingBefore = DuckDbInitializer.WaitingWriteCountForTests;
                writer = QueueWriter(initializer, events);
                WaitUntilWriterQueued(waitingBefore);
            }
        });

        Assert.True(await RunPassAsync(service));
        Assert.NotNull(writer);
        var seenByWriter = await writer;

        AssertHoldsNeverOverlap(events);

        /* One hold per table: every archivable table is counted, and each seeded table's rows fit in that hold. */
        var holdsPerTable = events.Where(e => e.Entered).GroupBy(e => e.Table).ToDictionary(g => g.Key, g => g.Count());
        Assert.Equal(ArchiveService.ArchivableTables.Length, holdsPerTable.Count);
        Assert.All(holdsPerTable, kv => Assert.Equal(1, kv.Value));

        /* The writer got the lock once the first table's hold ended and before the next table's began: it waited for one
           table, not the export. */
        Assert.NotEmpty(seenByWriter);
        Assert.Equal(new HoldEvent(first, 0, false), seenByWriter[^1]);
        Assert.Single(seenByWriter, e => e.Entered && seeded.Contains(e.Table));

        foreach (var table in seeded)
        {
            Assert.Equal(50, await ScalarAsync($"SELECT COUNT(*) FROM read_parquet('{ArchiveGlob(table)}')"));
            Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM {table}"));
        }
    }

    [Fact]
    public async Task ATableTooBigForOneHold_IsExportedOneSlicePerHold_AQueuedWriterGetsInBetweenSlices_AndTheFileHoldsEveryRowOnce()
    {
        var initializer = await InitializeAsync();
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(1, 2001) t(i)");

        /* 300 rows per hold for 2,000 rows: the table cannot go in one hold. */
        var service = new ArchiveService(initializer, _archiveDir) { TimestampForTests = "20261001_0000", ExportSeedSliceRows = 300 };
        var events = new List<HoldEvent>();
        Task<HoldEvent[]>? writer = null;
        RecordInto(service, events, e =>
        {
            if (writer is null && e.Table == "collection_log" && e.Slice == 1 && e.Entered)
            {
                var waitingBefore = DuckDbInitializer.WaitingWriteCountForTests;
                writer = QueueWriter(initializer, events);
                WaitUntilWriterQueued(waitingBefore);
            }
        });

        Assert.True(await RunPassAsync(service));
        Assert.True(writer is not null, "the table was exported in one hold: no slice 1 was taken");
        var seenByWriter = await writer;

        AssertHoldsNeverOverlap(events);

        /* The counting hold, then at least two slices, each its own hold. */
        var sliceHolds = events.Count(e => e.Entered && e.Table == "collection_log" && e.Slice > 0);
        Assert.True(sliceHolds >= 2, $"collection_log was exported in {sliceHolds} slice hold(s)");

        /* The writer got in when slice 1's hold ended, before slice 2's began. */
        Assert.Equal(new HoldEvent("collection_log", 1, false), seenByWriter[^1]);

        /* One file, every row once, nothing left in the table, and no slice file left behind. */
        var files = Directory.GetFiles(_archiveDir, "*_collection_log.parquet");
        Assert.Single(files);
        Assert.Equal(2000, await ScalarAsync($"SELECT COUNT(*) FROM read_parquet('{ArchiveGlob("collection_log")}')"));
        Assert.Equal(2000, await ScalarAsync($"SELECT COUNT(DISTINCT log_id) FROM read_parquet('{ArchiveGlob("collection_log")}')"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));
        Assert.Empty(Directory.GetFiles(_archiveDir, "*.tmp"));
    }

    [Fact]
    public async Task ARowWrittenBetweenTwoTableExports_LandsInThisExportOrTheNext_NeverLostNorDuplicated()
    {
        var initializer = await InitializeAsync();
        await SeedThreeTablesAsync(rows: 50);
        var service = new ArchiveService(initializer, _archiveDir) { TimestampForTests = "20261001_0000" };

        var order = ArchiveService.ArchivableTables.Select(t => t.Table)
            .Where(t => t is "collection_log" or "config_alert_log" or "wait_stats").ToArray();
        var events = new List<HoldEvent>();
        Task<HoldEvent[]>? writer = null;

        /* A collector-style write, under the write lock, queued while the first table is exported: it lands after that
           table's export and before the next table's. It writes one old row into every seeded table, so one row goes into a
           table already exported and the others into tables not exported yet. */
        RecordInto(service, events, e =>
        {
            if (writer is null && e.Table == order[0] && e.Entered)
            {
                var waitingBefore = DuckDbInitializer.WaitingWriteCountForTests;
                writer = QueueWriter(initializer, events, async () =>
                {
                    await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES (51, 1, 'S1', 'wait_stats', TIMESTAMP '2026-08-15 00:00:00', 'SUCCESS')");
                    await ExecAsync(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value)
VALUES (TIMESTAMP '2026-08-15 00:00:00', 1, 'S1', 'Blocking Detected', 51, 10)");
                    await ExecAsync(@"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type)
VALUES (51, TIMESTAMP '2026-08-15 00:00:00', 1, 'S1', 'W51')");
                });
                WaitUntilWriterQueued(waitingBefore);
            }
        });

        await RunPassAsync(service);
        Assert.NotNull(writer);
        await writer;

        /* Every table not yet exported when the row came took it in this export. */
        foreach (var table in order.Skip(1))
        {
            Assert.Equal(51, await ScalarAsync($"SELECT COUNT(*) FROM read_parquet('{ArchiveGlob(table)}')"));
            Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM {table}"));
        }

        /* The table already exported kept the row (or, when the row came before its promote, all 51): the next pass
           archives what is left. */
        service.ExportReadHoldForTests = null;
        service.TimestampForTests = "20261001_0100";
        Assert.True(await RunPassAsync(service));

        foreach (var table in order)
        {
            var key = table switch { "collection_log" => "log_id", "config_alert_log" => "current_value", _ => "collection_id" };
            Assert.Equal(51, await ScalarAsync($"SELECT COUNT(*) FROM read_parquet('{ArchiveGlob(table)}')"));
            Assert.Equal(51, await ScalarAsync($"SELECT COUNT(DISTINCT {key}) FROM read_parquet('{ArchiveGlob(table)}')"));
            Assert.Equal(0, await ScalarAsync($"SELECT COUNT(*) FROM {table}"));
        }
    }

    [Fact]
    public async Task ARowWrittenBetweenTwoSlicesIntoOneAlreadyExported_SkipsTheTableThisPass_AndTheNextPassArchivesEveryRowOnce()
    {
        var initializer = await InitializeAsync();
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(1, 2001) t(i)");

        var service = new ArchiveService(initializer, _archiveDir) { TimestampForTests = "20261001_0000", ExportSeedSliceRows = 300 };
        var events = new List<HoldEvent>();
        Task<HoldEvent[]>? writer = null;

        /* Slice 1 has no lower bound, so a row older than every other is in the range it already exported when the
           writer gets in after it. Without the check under the write lock, the DELETE would remove that row unarchived. */
        RecordInto(service, events, e =>
        {
            if (writer is null && e.Table == "collection_log" && e.Slice == 1 && e.Entered)
            {
                var waitingBefore = DuckDbInitializer.WaitingWriteCountForTests;
                writer = QueueWriter(initializer, events, () => ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES (2001, 1, 'S1', 'wait_stats', TIMESTAMP '2026-08-01 00:00:00', 'SUCCESS')"));
                WaitUntilWriterQueued(waitingBefore);
            }
        });

        Assert.False(await RunPassAsync(service));
        Assert.NotNull(writer);
        await writer;

        /* Skipped: nothing promoted, nothing deleted, nothing left behind. */
        Assert.Empty(Directory.GetFiles(_archiveDir, "*_collection_log.parquet"));
        Assert.Empty(Directory.GetFiles(_archiveDir, "*.tmp"));
        Assert.Equal(2001, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));

        service.ExportReadHoldForTests = null;
        service.TimestampForTests = "20261001_0100";
        Assert.True(await RunPassAsync(service));

        Assert.Equal(2001, await ScalarAsync($"SELECT COUNT(*) FROM read_parquet('{ArchiveGlob("collection_log")}')"));
        Assert.Equal(2001, await ScalarAsync($"SELECT COUNT(DISTINCT log_id) FROM read_parquet('{ArchiveGlob("collection_log")}')"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));
    }

    [Theory]
    [InlineData("query_snapshots")]
    [InlineData("wait_stats")]
    public void TheSlicedExportsMerge_UsesCompactionsThreadsAndCopyOptionsForTheTable(string table)
    {
        /* query_snapshots is a plan-heavy daily table (one thread, 32 MiB row-group byte bound); wait_stats a plain one. */
        Assert.Equal(
            ParquetCompaction.BuildPragma(ParquetCompaction.DefaultMemoryLimit, ParquetCompaction.ThreadsFor(table), "C:/spill"),
            ArchiveService.BuildExportSliceMergePragma(table, "C:/spill"));

        var copy = ArchiveService.BuildExportSliceMergeCopy(table, ["C:/a/x.parquet.tmp.001.tmp", "C:/a/x.parquet.tmp.002.tmp"], "C:/a/x.parquet.tmp");
        Assert.EndsWith(
            $"({ParquetCompaction.BuildArchiveCopyOptions(ParquetCompaction.ArchiveRowGroupSize, ParquetCompaction.RowGroupBytesFor(table))})",
            copy, StringComparison.Ordinal);

        /* Every column: the export keeps query_plan_text, which compaction's CompactionExcludeColumns would drop. */
        Assert.StartsWith("COPY (SELECT * FROM read_parquet([", copy, StringComparison.Ordinal);
    }
}