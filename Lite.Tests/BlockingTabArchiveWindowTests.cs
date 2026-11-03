/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: the Lite Blocking tab's block-chain read (<c>GetBlockingPairRowsAsync</c>) merges in the always-on DMV snapshot
/// rows from <c>v_dmv_blocking_snapshots</c>. A view reads every archive file's footer, so a window newer than the archive
/// takes a plan first and reads the hot table alone. Each test caches the archive span with one real read, then makes the
/// archive file unreadable: a read that still opens it fails.
/// </summary>
public sealed class BlockingTabArchiveWindowTests : IDisposable
{
    private const int ServerId = 7256;

    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private long _nextId = 1;

    public BlockingTabArchiveWindowTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
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

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DateTime Whole(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private async Task ExecuteAsync(params string[] statements)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(Ct);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(Ct);
        }
    }

    /// <summary>A snapshot pair. The collector writes the event time and the collection time from one value.</summary>
    private string SnapshotRow(DateTime collected, int blocked, int blocking) =>
        $@"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, blocked_spid, blocking_spid,
     wait_time_ms, blocked_sql_text, blocking_sql_text, blocked_ecid, blocking_ecid, monitor_loop)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', {Ts(collected)}, {blocked}, {blocking}, 5000,
     'SELECT blocked_{blocked}', 'UPDATE blocking_{blocking}', 0, 0, 1)";

    /// <summary>Moves the snapshots older than <paramref name="before"/> into a Parquet file, rebuilds the views and reads the
    /// archive's span once, so it is cached. Returns the file.</summary>
    private async Task<string> ArchiveAsync(DateTime before)
    {
        var path = Path.Combine(_archiveDir, "20260101_000000_dmv_blocking_snapshots.parquet").Replace("\\", "/");
        await ExecuteAsync(
            $"COPY (SELECT * FROM dmv_blocking_snapshots WHERE collection_time < {Ts(before)}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM dmv_blocking_snapshots WHERE collection_time < {Ts(before)}");
        await _duckDb.CreateArchiveViewsAsync();
        Assert.NotNull(await _duckDb.GetArchiveSpanAsync("dmv_blocking_snapshots", "collection_time", Ct));
        return path;
    }

    [Fact]
    public async Task BlockChainRead_AWindowNewerThanTheArchive_NeverOpensTheArchiveFile()
    {
        await ExecuteAsync(SnapshotRow(Whole(NowUtc.AddDays(-10)), 61, 62), SnapshotRow(Whole(NowUtc.AddHours(-2)), 51, 52));
        var archiveFile = await ArchiveAsync(NowUtc.AddDays(-1));
        File.WriteAllText(archiveFile, "not a parquet file");

        var rows = await _service.GetBlockingPairRowsAsync(ServerId, Whole(NowUtc.AddHours(-6)), Whole(NowUtc));

        var row = Assert.Single(rows);
        Assert.Equal(51, row.BlockedSpid);
        Assert.Equal(52, row.BlockingSpid);
    }

    [Fact]
    public async Task BlockChainRead_AWindowThatReachesTheArchive_StillReadsIt()
    {
        await ExecuteAsync(SnapshotRow(Whole(NowUtc.AddDays(-10)), 61, 62), SnapshotRow(Whole(NowUtc.AddHours(-2)), 51, 52));
        await ArchiveAsync(NowUtc.AddDays(-1));

        var rows = await _service.GetBlockingPairRowsAsync(ServerId, Whole(NowUtc.AddDays(-12)), Whole(NowUtc));

        Assert.Equal(new[] { 51, 61 }, rows.Select(r => r.BlockedSpid).OrderBy(s => s).ToArray());
    }
}
