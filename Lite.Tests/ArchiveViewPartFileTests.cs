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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Compaction writes a month that is too large for one merge as <c>{month}_{table}_ptNNN.parquet</c> part files.
/// The archive views globbed only <c>*_{table}.parquet</c>, which those names do not match, so a month stored as
/// parts dropped out of the table's <c>v_</c> view, and a table with only part files got no archive branch at
/// all. The views now read both shapes.
/// </summary>
public sealed class ArchiveViewPartFileTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveViewPartFileTests()
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

    private async Task<long> ScalarAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /* Writes rows [from, to) of collection_log to an archive file with the table's real schema, then clears them
       from the hot table, the way archival does. */
    private async Task ArchiveRowsAsync(string fileName, int from, int to)
    {
        await ExecAsync($@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range({from}, {to}) t(i)");
        var path = Path.Combine(_archiveDir, fileName).Replace("\\", "/");
        await ExecAsync($"COPY collection_log TO '{path}' (FORMAT PARQUET)");
        await ExecAsync("DELETE FROM collection_log");
    }

    [Fact]
    public async Task MonthStoredOnlyAsPartFiles_IsVisibleThroughTheView()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await ArchiveRowsAsync("202609_collection_log_pt001.parquet", 0, 100);
        await ArchiveRowsAsync("202609_collection_log_pt002.parquet", 100, 150);

        await initializer.CreateArchiveViewsAsync();

        Assert.Equal(150, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
    }

    [Fact]
    public async Task PlainMonthlyAndPartFiles_AreBothVisibleThroughTheView()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await ArchiveRowsAsync("202608_collection_log.parquet", 0, 40);
        await ArchiveRowsAsync("202609_collection_log_pt001.parquet", 40, 100);
        await ArchiveRowsAsync("20260928_1400_collection_log.parquet", 100, 110);

        await initializer.CreateArchiveViewsAsync();

        Assert.Equal(110, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
        Assert.Equal(110, await ScalarAsync("SELECT COUNT(DISTINCT log_id) FROM v_collection_log"));
    }

    /// <summary>
    /// The part glob is added only while a part file exists: DuckDB refuses to bind a glob that matches nothing,
    /// which would break the view for every table rather than fix it for the ones with parts.
    /// </summary>
    [Fact]
    public async Task ATableWithoutPartFiles_KeepsASingleGlob()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await ArchiveRowsAsync("202609_collection_log.parquet", 0, 10);

        var globs = initializer.ArchiveParquetGlobs("collection_log");
        Assert.Single(globs);
        Assert.EndsWith("*_collection_log.parquet", globs[0], StringComparison.Ordinal);

        await ArchiveRowsAsync("202609_collection_log_pt001.parquet", 10, 20);

        globs = initializer.ArchiveParquetGlobs("collection_log");
        Assert.Equal(2, globs.Count);
        Assert.EndsWith("*_collection_log_pt???.parquet", globs[1], StringComparison.Ordinal);

        Assert.Empty(initializer.ArchiveParquetGlobs("wait_stats"));
    }

    /// <summary>
    /// <c>*_{table}.parquet</c> also matches another table's files when that table's name ends with
    /// <c>_{table}</c>. No archivable table may be an underscore-suffix of another, or the shorter one's view
    /// would read the longer one's files.
    /// </summary>
    [Fact]
    public void NoArchivableTableName_IsAnUnderscoreSuffixOfAnother()
    {
        var tables = DuckDbInitializer.ArchivableTables;

        var collisions = tables
            .SelectMany(shorter => tables
                .Where(longer => longer != shorter && longer.EndsWith("_" + shorter, StringComparison.OrdinalIgnoreCase))
                .Select(longer => $"{longer} ends with _{shorter}"))
            .ToList();

        Assert.True(collisions.Count == 0, "archive globs would cross tables:\n" + string.Join("\n", collisions));
    }
}
