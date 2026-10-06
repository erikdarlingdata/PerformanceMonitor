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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5312: the saved per-server database filter narrows the desktop reads the web filter narrows. Per read: the filter
/// returns only the chosen databases, no filter (null or empty) leaves every row, and a parity check against the reader
/// the MCP tool uses for the same seed. The statement-shape pins at the bottom read the source text so a tab that stops
/// passing the filter into its read fails here.
/// </summary>
public sealed class DesktopDatabaseFilterReadsTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 531200;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private DuckDBConnection? _conn;
    private long _nextId = 5312000;

    public DesktopDatabaseFilterReadsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose() => _conn?.Dispose();

    private async Task ExecAsync(string sql, params object[] values)
    {
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = v });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    private static DateTime Minute(int minutesAgo)
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc).AddMinutes(-minutesAgo);
    }

    private static readonly string[] OnlyA = { "DbA" };
    private static readonly string[] AAndB = { "DbA", "DbB" };

    /* ───────────────────────── File I/O trends ───────────────────────── */

    private Task SeedFileIoAsync(string db, string file, int minutesAgo, long reads, long writes) =>
        ExecAsync(@"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms,
             sample_interval_seconds)
            VALUES ($1, $2, $3, 'Srv', $4, $5, 'ROWS', '', 100, $6, $7, $8, $9, $10, $11, 60)",
            _nextId++, Minute(minutesAgo), ServerId, db, file, reads, writes, reads * 8192, writes * 8192, reads * 2, writes * 3);

    /// <summary>DbA has one quiet file; DbC has ELEVEN louder ones, so the unfiltered top ten leaves DbA out.</summary>
    private async Task SeedBusyServerAsync()
    {
        foreach (var minutes in new[] { 20, 10 })
        {
            await SeedFileIoAsync("DbA", "a.mdf", minutes, 5, 5);
            await SeedFileIoAsync("DbB", "b.mdf", minutes, 6, 6);
            for (int i = 0; i < 11; i++)
            {
                await SeedFileIoAsync("DbC", $"c{i}.mdf", minutes, 1000 + i, 1000 + i);
            }
        }
    }

    [Fact]
    public async Task FileIoLatencyTrend_FilterRanksAndReturnsOnlyTheChosenDatabases()
    {
        await SeedBusyServerAsync();

        var unfiltered = await _service.GetFileIoLatencyTrendAsync(ServerId, 1);
        Assert.DoesNotContain(unfiltered, p => p.DatabaseName == "DbA");
        Assert.Equal(10, unfiltered.Select(p => p.FileName).Distinct().Count());

        var filtered = await _service.GetFileIoLatencyTrendAsync(ServerId, 1, databaseNames: OnlyA);
        Assert.NotEmpty(filtered);
        Assert.All(filtered, p => Assert.Equal("DbA", p.DatabaseName));

        var two = await _service.GetFileIoLatencyTrendAsync(ServerId, 1, databaseNames: AAndB);
        Assert.Equal(new[] { "DbA", "DbB" }, two.Select(p => p.DatabaseName).Distinct().OrderBy(n => n).ToArray());
    }

    [Fact]
    public async Task FileIoLatencyTrend_NoFilterIsTheUnfilteredRead()
    {
        await SeedBusyServerAsync();

        var none = await _service.GetFileIoLatencyTrendAsync(ServerId, 1, databaseNames: null);
        var empty = await _service.GetFileIoLatencyTrendAsync(ServerId, 1, databaseNames: new List<string>());
        var plain = await _service.GetFileIoLatencyTrendAsync(ServerId, 1);

        Assert.Equal(plain.Count, none.Count);
        Assert.Equal(plain.Count, empty.Count);
        Assert.Equal(LocalDataService.FileIoLatencyTrendSql, LocalDataService.FileIoLatencyTrendSqlFor(""));
    }

    [Fact]
    public async Task FileIoThroughputTrend_FilterRanksAndReturnsOnlyTheChosenDatabases()
    {
        await SeedBusyServerAsync();

        var unfiltered = await _service.GetFileIoThroughputTrendAsync(ServerId, 1);
        Assert.DoesNotContain(unfiltered, p => p.FileLabel.StartsWith("DbA.", StringComparison.Ordinal));

        var filtered = await _service.GetFileIoThroughputTrendAsync(ServerId, 1, databaseNames: OnlyA);
        Assert.NotEmpty(filtered);
        Assert.All(filtered, p => Assert.StartsWith("DbA.", p.FileLabel, StringComparison.Ordinal));

        var empty = await _service.GetFileIoThroughputTrendAsync(ServerId, 1, databaseNames: new List<string>());
        Assert.Equal(unfiltered.Count, empty.Count);
    }

    /// <summary>The desktop filter and the MCP trend's one-database scope (the same reader's ranking) name the same files.</summary>
    [Fact]
    public async Task FileIoLatencyTrend_FilterMatchesTheMcpScopeForTheSameSeed()
    {
        await SeedBusyServerAsync();

        var desktop = await _service.GetFileIoLatencyTrendAsync(ServerId, 1, databaseNames: new[] { "DbC" });
        var series = await _service.GetFileIoSeriesAsync(ServerId, 1, DateTime.UtcNow, "DbC");

        Assert.All(series, s => Assert.Equal("DbC", s.DatabaseName));
        /* The chart keeps its own top ten by activity; the MCP ranking lists every active file of the database. */
        var scopedFiles = series.Where(s => s.FileName != null).Select(s => s.FileName!).ToHashSet();
        Assert.Equal(11, scopedFiles.Count);
        Assert.Equal(10, desktop.Select(p => p.FileName).Distinct().Count());
        Assert.All(desktop, p => Assert.Contains(p.FileName, scopedFiles));
    }

    /* ───────────────────────── Database sizes ───────────────────────── */

    private Task SeedSizeAsync(string db, int minutesAgo, decimal totalMb) =>
        ExecAsync(@"INSERT INTO database_size_stats
            (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
             file_name, physical_name, total_size_mb, used_size_mb)
            VALUES ($1, $2, $3, 'Srv', $4, 5, 1, 'ROWS', $5, '', $6, 10)",
            _nextId++, Minute(minutesAgo), ServerId, db, db + ".mdf", totalMb);

    [Fact]
    public async Task DatabaseSizes_FilterReturnsOnlyTheChosenDatabases_AndKeepsTheServersNewestSnapshot()
    {
        /* An older snapshot holds DbC; the newest holds DbA and DbB only. The filter must not pick an older snapshot. */
        await SeedSizeAsync("DbC", 60, 999);
        await SeedSizeAsync("DbA", 5, 100);
        await SeedSizeAsync("DbB", 5, 200);

        var all = await _service.GetDatabaseSizeLatestAsync(ServerId);
        Assert.Equal(new[] { "DbA", "DbB" }, all.Select(r => r.DatabaseName).ToArray());

        var filtered = await _service.GetDatabaseSizeLatestAsync(ServerId, OnlyA);
        Assert.Equal(new[] { "DbA" }, filtered.Select(r => r.DatabaseName).ToArray());

        var notInSnapshot = await _service.GetDatabaseSizeLatestAsync(ServerId, new[] { "DbC" });
        Assert.Empty(notInSnapshot);

        var empty = await _service.GetDatabaseSizeLatestAsync(ServerId, new List<string>());
        Assert.Equal(all.Count, empty.Count);
    }

    [Fact]
    public async Task DatabaseSizeSummary_FilterTakesTheTopAmongTheChosenDatabases()
    {
        await SeedSizeAsync("DbA", 5, 100);
        await SeedSizeAsync("DbB", 5, 200);
        await SeedSizeAsync("DbC", 5, 300);

        var all = await _service.GetDatabaseSizeSummaryAsync(ServerId, 1);
        Assert.Equal(new[] { "DbC" }, all.Select(r => r.DatabaseName).ToArray());

        var filtered = await _service.GetDatabaseSizeSummaryAsync(ServerId, 1, OnlyA.Concat(new[] { "DbB" }).ToArray());
        Assert.Equal(new[] { "DbB" }, filtered.Select(r => r.DatabaseName).ToArray());

        var none = await _service.GetDatabaseSizeSummaryAsync(ServerId, 10, new List<string>());
        Assert.Equal(3, none.Count);
    }

    /* ───────────────────────── Persistent version store ───────────────────────── */

    private Task SeedPvsAsync(string db, int minutesAgo, double pvsMb) =>
        ExecAsync(@"INSERT INTO pvs_stats
            (collection_id, collection_time, server_id, server_name, database_name, database_id,
             is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
            VALUES ($1, $2, $3, 'Srv', $4, 5, true, $5, 1000)",
            _nextId++, Minute(minutesAgo), ServerId, db, pvsMb);

    [Fact]
    public async Task PvsStats_FilterReturnsOnlyTheChosenDatabases()
    {
        await SeedPvsAsync("DbA", 5, 10);
        await SeedPvsAsync("DbB", 5, 20);
        await SeedPvsAsync("DbC", 5, 30);

        var all = await _service.GetPvsStatsLatestAsync(ServerId);
        Assert.Equal(new[] { "DbC", "DbB", "DbA" }, all.Select(r => r.DatabaseName).ToArray());

        var filtered = await _service.GetPvsStatsLatestAsync(ServerId, AAndB);
        Assert.Equal(new[] { "DbB", "DbA" }, filtered.Select(r => r.DatabaseName).ToArray());

        var empty = await _service.GetPvsStatsLatestAsync(ServerId, new List<string>());
        Assert.Equal(3, empty.Count);
    }

    [Fact]
    public async Task PvsTrend_FilterRanksTheTopFiveAmongTheChosenDatabases()
    {
        /* Six databases larger than DbA, so the unfiltered top five leaves DbA out. */
        foreach (var minutes in new[] { 30, 5 })
        {
            await SeedPvsAsync("DbA", minutes, 1);
            for (int i = 0; i < 6; i++)
            {
                await SeedPvsAsync($"Big{i}", minutes, 100 + i);
            }
        }

        var since = DateTime.UtcNow.AddHours(-2);
        var all = await _service.GetPvsTrendAsync(ServerId, since);
        Assert.DoesNotContain(all, p => p.DatabaseName == "DbA");

        var filtered = await _service.GetPvsTrendAsync(ServerId, since, OnlyA);
        Assert.NotEmpty(filtered);
        Assert.All(filtered, p => Assert.Equal("DbA", p.DatabaseName));

        var empty = await _service.GetPvsTrendAsync(ServerId, since, new List<string>());
        Assert.Equal(all.Count, empty.Count);
    }

    /* ───────────────────────── Object locking ───────────────────────── */

    private Task SeedLockingAsync(string db, string table, int minutesAgo, long rowLockWaitMs) =>
        ExecAsync(@"INSERT INTO index_object_stats
            (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
             schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
             user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count)
            VALUES ($1, $2, $3, 'Srv', $4, $5, 7, 'dbo', 100, $6, 1, 'IX', 'NONCLUSTERED', 1, 1, 10, 0, 0, 0, 0, $7, 0)",
            _nextId++, Minute(minutesAgo), ServerId, Minute(600), db, table, rowLockWaitMs);

    private async Task SeedThreeLockingDatabasesAsync()
    {
        await SeedLockingAsync("DbA", "ta", 5, 100);
        await SeedLockingAsync("DbB", "tb", 5, 200);
        await SeedLockingAsync("DbC", "tc", 5, 300);
    }

    [Fact]
    public async Task IndexLocking_FilterAndBoxBothApply()
    {
        await SeedThreeLockingDatabasesAsync();

        var all = await _service.GetIndexLockingAsync(ServerId);
        Assert.Equal(3, all.Count);

        var filtered = await _service.GetIndexLockingAsync(ServerId, 200, null, AAndB);
        Assert.Equal(new[] { "DbB", "DbA" }, filtered.Select(r => r.DatabaseName).ToArray());

        var boxInsideFilter = await _service.GetIndexLockingAsync(ServerId, 200, "DbB", AAndB);
        Assert.Equal(new[] { "DbB" }, boxInsideFilter.Select(r => r.DatabaseName).ToArray());

        var boxOutsideFilter = await _service.GetIndexLockingAsync(ServerId, 200, "DbC", AAndB);
        Assert.Empty(boxOutsideFilter);

        var empty = await _service.GetIndexLockingAsync(ServerId, 200, null, new List<string>());
        Assert.Equal(3, empty.Count);
    }

    /// <summary>The desktop filter [DbA] answers the rows the MCP get_object_locking reader gives for database_name DbA.</summary>
    [Fact]
    public async Task IndexLocking_FilterMatchesTheMcpReaderForTheSameSeed()
    {
        await SeedThreeLockingDatabasesAsync();

        var desktop = await _service.GetIndexLockingAsync(ServerId, 200, null, OnlyA);
        var mcp = await _service.GetIndexLockingAsync(ServerId, 200, "DbA");

        Assert.Equal(
            mcp.Select(r => (r.DatabaseName, r.TableName, r.RowLockWaitInMs)).ToArray(),
            desktop.Select(r => (r.DatabaseName, r.TableName, r.RowLockWaitInMs)).ToArray());
    }

    [Fact]
    public async Task IndexLockingDatabases_ListsOnlyDatabasesInsideTheFilter()
    {
        await SeedThreeLockingDatabasesAsync();

        Assert.Equal(new[] { "DbA", "DbB", "DbC" }, await _service.GetIndexLockingDatabasesAsync(ServerId));
        Assert.Equal(new[] { "DbA", "DbB" }, await _service.GetIndexLockingDatabasesAsync(ServerId, AAndB));
        Assert.Equal(3, (await _service.GetIndexLockingDatabasesAsync(ServerId, new List<string>())).Count);
    }

    [Fact]
    public async Task OptimizedLockingNote_FilterNarrowsTheFlagsRead()
    {
        foreach (var (db, on) in new[] { ("DbA", false), ("DbB", true) })
        {
            await ExecAsync(@"INSERT INTO database_config
                (config_id, capture_time, server_id, server_name, database_name, is_optimized_locking_on)
                VALUES ($1, $2, $3, 'Srv', $4, $5)",
                _nextId++, Minute(5), ServerId, db, on);
        }

        Assert.NotNull(await _service.GetOptimizedLockingNoteAsync(ServerId));
        Assert.Null(await _service.GetOptimizedLockingNoteAsync(ServerId, null, OnlyA));
        Assert.NotNull(await _service.GetOptimizedLockingNoteAsync(ServerId, null, new[] { "DbB" }));
        Assert.NotNull(await _service.GetOptimizedLockingNoteAsync(ServerId, null, new List<string>()));
    }

    /* ───────────────────────── Source pins: each tab passes the filter into its read ───────────────────────── */

    private static string Source(params string[] parts)
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !File.Exists(Path.Combine(dir, "Lite", "PerformanceMonitorLite.csproj")))
        {
            dir = Path.GetDirectoryName(dir);
        }
        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(new[] { dir! }.Concat(parts).ToArray()));
    }

    [Fact]
    public void ServerTab_PassesTheSavedFilterIntoBothFileIoReads()
    {
        var src = Source("Lite", "Controls", "ServerTab.Refresh.cs");
        Assert.Contains("GetFileIoLatencyTrendAsync(_serverId, hoursBack, fromDate, toDate, databaseNames: SelectedDatabaseFilter)", src, StringComparison.Ordinal);
        Assert.Contains("GetFileIoThroughputTrendAsync(_serverId, hoursBack, fromDate, toDate, databaseNames: SelectedDatabaseFilter)", src, StringComparison.Ordinal);
    }

    [Fact]
    public void FinOpsTab_PassesTheSavedFilterIntoSizesPvsAndLockingReads()
    {
        var tab = Source("Lite", "Controls", "FinOpsTab.xaml.cs");
        Assert.Contains("GetDatabaseSizeLatestAsync(serverId, sizesFilter)", tab, StringComparison.Ordinal);
        Assert.Contains("GetDatabaseSizeSummaryAsync(serverId, 10, sizeChartFilter)", tab, StringComparison.Ordinal);
        Assert.Contains("GetPvsStatsLatestAsync(serverId, pvsFilter)", tab, StringComparison.Ordinal);
        Assert.Contains("GetPvsTrendAsync(serverId, DateTime.UtcNow.AddDays(-7), pvsFilter)", tab, StringComparison.Ordinal);

        var locking = Source("Lite", "Controls", "FinOpsTab.Locking.cs");
        Assert.Contains("GetIndexLockingDatabasesAsync(serverId, lockingFilter)", locking, StringComparison.Ordinal);
        Assert.Contains("GetIndexLockingAsync(serverId, 200, db, lockingFilter)", locking, StringComparison.Ordinal);
        Assert.Contains("GetOptimizedLockingNoteAsync(serverId, db, lockingFilter)", locking, StringComparison.Ordinal);
    }

    [Fact]
    public void FinOpsTab_TheHealthScoreFreeSpaceReadStaysServerWide()
    {
        var tab = Source("Lite", "Controls", "FinOpsTab.xaml.cs");
        Assert.Contains("dbSizes = await Task.Run(() => _dataService.GetDatabaseSizeLatestAsync(serverId));", tab, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseFilterOf_IsNullWhenTheServerHasNoSavedFilter()
    {
        Assert.Null(PerformanceMonitorLite.Controls.FinOpsTab.DatabaseFilterOf(null));
        Assert.Null(PerformanceMonitorLite.Controls.FinOpsTab.DatabaseFilterOf(new PerformanceMonitorLite.Models.ServerConnection()));
        var withFilter = new PerformanceMonitorLite.Models.ServerConnection { ViewFilterDatabases = new List<string> { "DbA" } };
        Assert.Equal(new[] { "DbA" }, PerformanceMonitorLite.Controls.FinOpsTab.DatabaseFilterOf(withFilter));
    }
}
