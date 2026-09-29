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
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
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
/// <para>In the reset-gate collection because the write lock is one per process: a test that holds it for half a
/// second while a reader is parked behind it should not run beside the reset and sentinel tests, some of which
/// wait on it with a timeout.</para>
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveCompactionSwapLockTests : IDisposable
{
    /* 5 hot rows, plus 10 per archive file across four files. */
    private const long TotalRows = 45;

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

    private async Task<(DuckDbInitializer Initializer, ArchiveService Service)> SetUpAsync()
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

        var service = new ArchiveService(initializer, _archiveDir)
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
            probe.Join();
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
}
