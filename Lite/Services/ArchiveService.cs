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
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Archives old data from DuckDB hot tables to Parquet files and purges archived rows.
/// </summary>
public class ArchiveService
{
    private readonly DuckDbInitializer _duckDb;
    private readonly string _archivePath;
    private readonly ILogger<ArchiveService>? _logger;
    private static readonly SemaphoreSlim s_archiveLock = new(1, 1);

    /* Tables eligible for archival with their time column */
    private static readonly (string Table, string TimeColumn)[] ArchivableTables =
    [
        ("wait_stats", "collection_time"),
        ("query_stats", "collection_time"),
        ("procedure_stats", "collection_time"),
        ("query_store_stats", "collection_time"),
        ("query_snapshots", "collection_time"),
        ("cpu_utilization_stats", "collection_time"),
        ("file_io_stats", "collection_time"),
        ("memory_stats", "collection_time"),
        ("memory_clerks", "collection_time"),
        ("tempdb_stats", "collection_time"),
        ("perfmon_stats", "collection_time"),
        ("deadlocks", "collection_time"),
        ("blocked_process_reports", "collection_time"),
        ("collection_log", "collection_time")
    ];

    public ArchiveService(DuckDbInitializer duckDb, string archivePath, ILogger<ArchiveService>? logger = null)
    {
        _duckDb = duckDb;
        _archivePath = archivePath;
        _logger = logger;

        if (!Directory.Exists(_archivePath))
        {
            Directory.CreateDirectory(_archivePath);
        }
    }

    /// <summary>
    /// Archives data older than the specified number of days to Parquet files,
    /// then deletes the archived rows from the hot tables.
    /// </summary>
    public async Task ArchiveOldDataAsync(int hotDataDays = 7)
    {
        if (!await s_archiveLock.WaitAsync(TimeSpan.Zero))
        {
            _logger?.LogDebug("Archive operation already in progress, skipping");
            return;
        }

        try
        {
        var cutoffDate = DateTime.UtcNow.AddDays(-hotDataDays);
        var archiveMonth = cutoffDate.ToString("yyyy-MM");

        _logger?.LogInformation("Archiving data older than {CutoffDate} to Parquet (month: {Month})", cutoffDate, archiveMonth);

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        foreach (var (table, timeColumn) in ArchivableTables)
        {
            try
            {
                /* Check if there are rows to archive */
                var rowCount = await GetRowCountBeforeCutoff(connection, table, timeColumn, cutoffDate);
                if (rowCount == 0)
                {
                    continue;
                }

                /* Export to Parquet (append mode - UNION if file exists) */
                var parquetPath = Path.Combine(_archivePath, $"{archiveMonth}_{table}.parquet")
                    .Replace("\\", "/");

                if (File.Exists(parquetPath))
                {
                    /* Append: write to temp, then UNION with existing */
                    var tempPath = parquetPath + ".tmp";
                    await ExportToParquet(connection, table, timeColumn, cutoffDate, tempPath);

                    using var mergeCmd = connection.CreateCommand();
                    mergeCmd.CommandText = $@"
COPY (
    SELECT * FROM read_parquet('{parquetPath}')
    UNION ALL
    SELECT * FROM read_parquet('{tempPath}')
) TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD)";
                    await mergeCmd.ExecuteNonQueryAsync();

                    File.Delete(tempPath);
                }
                else
                {
                    await ExportToParquet(connection, table, timeColumn, cutoffDate, parquetPath);
                }

                /* Delete archived rows from hot table */
                using var deleteCmd = connection.CreateCommand();
                deleteCmd.CommandText = $"DELETE FROM {table} WHERE {timeColumn} < '{cutoffDate:yyyy-MM-dd HH:mm:ss}'";
                await deleteCmd.ExecuteNonQueryAsync();

                _logger?.LogInformation("Archived {Count} rows from {Table} to {Path}", rowCount, table, parquetPath);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to archive table {Table}", table);
            }
        }
        }
        finally
        {
            s_archiveLock.Release();
        }
    }

    private static async Task<long> GetRowCountBeforeCutoff(DuckDBConnection connection, string table, string timeColumn, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table} WHERE {timeColumn} < '{cutoff:yyyy-MM-dd HH:mm:ss}'";
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    private static async Task ExportToParquet(DuckDBConnection connection, string table, string timeColumn, DateTime cutoff, string filePath)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
COPY (
    SELECT * FROM {table} WHERE {timeColumn} < '{cutoff:yyyy-MM-dd HH:mm:ss}'
) TO '{filePath}' (FORMAT PARQUET, COMPRESSION ZSTD)";
        await cmd.ExecuteNonQueryAsync();
    }
}
