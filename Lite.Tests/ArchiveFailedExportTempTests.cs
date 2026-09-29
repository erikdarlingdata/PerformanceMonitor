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
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// DuckDB keeps the partial file when a COPY fails partway through its query. Both archive exports write to a
/// .tmp first, and the .tmp was only removed when a LATER step failed, so an export that itself threw left its
/// partial file behind on every attempt: a size-triggered reset retries every 15 minutes, up to 96 leftovers a
/// day, on a disk that is already under size pressure. These tests make the COPY fail for real, after DuckDB has
/// created its output file, rather than throwing before it starts.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveFailedExportTempTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveFailedExportTempTests()
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

    private async Task<long> CountAsync(string table)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private string[] ArchiveFileNames() =>
        Directory.GetFiles(_archiveDir).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;

    /* collection_log holds 50 old rows and is then replaced by a view over them that fails on the row with
       log_id 40, where a status that is not a number is cast to one. COUNT(*) never reads status, so archival
       sees the rows and starts the COPY; the COPY fails after DuckDB has created its output file. */
    private async Task<DuckDbInitializer> SeedACopyThatFailsMidQueryAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(1, 51) t(i)");
        await ExecAsync(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value)
SELECT TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 1, 'S1', 'Blocking Detected', i, 10 FROM range(1, 21) t(i)");

        await ExecAsync("CREATE TABLE collection_log_rows AS SELECT * FROM collection_log");
        await ExecAsync("DROP TABLE collection_log");
        await ExecAsync(@"
CREATE VIEW collection_log AS
SELECT * REPLACE (CASE WHEN log_id = 40 THEN CAST(CAST(status AS INTEGER) AS VARCHAR) ELSE status END AS status)
FROM collection_log_rows");

        return initializer;
    }

    [Fact]
    public async Task SizeTriggeredReset_WhoseCopyFailsMidQuery_LeavesNoTempFileBehind()
    {
        var initializer = await SeedACopyThatFailsMidQueryAsync();
        var service = new ArchiveService(initializer, _archiveDir);

        await service.ArchiveAllAndResetAsync();

        Assert.True(service.ResetRetryNotBeforeUtc > DateTime.UtcNow.AddMinutes(10), "the export did not fail, so the attempt was not backed off");
        Assert.Equal(50, await CountAsync("collection_log"));
        Assert.Equal(20, await CountAsync("config_alert_log"));
        Assert.Empty(ArchiveFileNames());
    }

    [Fact]
    public async Task HourlyArchival_WhoseCopyFailsMidQuery_LeavesNoTempFileBehind()
    {
        var initializer = await SeedACopyThatFailsMidQueryAsync();
        var service = new ArchiveService(initializer, _archiveDir);

        await service.ArchiveOldDataAsync(hotDataDays: 7);

        /* The export failed, so the rows were neither archived nor deleted. */
        Assert.Equal(50, await CountAsync("collection_log"));
        Assert.DoesNotContain(ArchiveFileNames(), f => f.Contains("_collection_log", StringComparison.Ordinal));
        Assert.DoesNotContain(ArchiveFileNames(), f => f.EndsWith(".tmp", StringComparison.Ordinal));
    }
}
