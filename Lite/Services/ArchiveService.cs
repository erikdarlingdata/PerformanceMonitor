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
    /// Archives data older than the specified cutoff to Parquet files,
    /// then deletes the archived rows from the hot tables.
    /// Use hotDataDays for scheduled archival (default 7), or hotDataHours
    /// for size-triggered archival when the database is under space pressure.
    /// </summary>
    public async Task ArchiveOldDataAsync(int hotDataDays = 7, int? hotDataHours = null)
    {
        if (!await s_archiveLock.WaitAsync(TimeSpan.Zero))
        {
            _logger?.LogDebug("Archive operation already in progress, skipping");
            return;
        }

        try
        {
        var cutoffDate = hotDataHours.HasValue
            ? DateTime.UtcNow.AddHours(-hotDataHours.Value)
            : DateTime.UtcNow.AddDays(-hotDataDays);
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");

        _logger?.LogInformation("Archiving data older than {CutoffDate} to Parquet (prefix: {Timestamp})", cutoffDate, timestamp);

        /* Write lock covers export + DELETE. The DELETEs modify table data, and the
           next CHECKPOINT will reorganize the file — readers must not be mid-query
           when that happens or they get "Reached the end of the file" errors. */
        using (_duckDb.AcquireWriteLock())
        {
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

                    /* Export to a uniquely-named parquet file — no merging needed.
                       Each archival cycle produces a new file with a timestamp prefix.
                       Archive views use glob (*_table.parquet) to pick up all files. */
                    var parquetPath = Path.Combine(_archivePath, $"{timestamp}_{table}.parquet")
                        .Replace("\\", "/");

                    await ExportToParquet(connection, table, timeColumn, cutoffDate, parquetPath);

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

        /* Refresh archive views outside write lock — view creation is fast and safe */
        await _duckDb.CreateArchiveViewsAsync();
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

    /// <summary>
    /// Archives ALL data from every table to parquet, then deletes and reinitializes the database.
    /// Called when the database exceeds the size threshold. Data remains queryable through archive views.
    /// </summary>
    public async Task ArchiveAllAndResetAsync()
    {
        if (!await s_archiveLock.WaitAsync(TimeSpan.Zero))
        {
            _logger?.LogDebug("Archive operation already in progress, skipping");
            return;
        }

        try
        {
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");

            _logger?.LogInformation("Archiving ALL data to Parquet (prefix: {Timestamp}) and resetting database", timestamp);

            /* Export everything under write lock */
            using (_duckDb.AcquireWriteLock())
            {
                using var connection = _duckDb.CreateConnection();
                await connection.OpenAsync();

                foreach (var (table, _) in ArchivableTables)
                {
                    try
                    {
                        /* Check row count */
                        using var countCmd = connection.CreateCommand();
                        countCmd.CommandText = $"SELECT COUNT(*) FROM {table}";
                        var rowCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
                        if (rowCount == 0) continue;

                        /* Export all rows to a uniquely-named parquet file.
                           No merging needed — each reset produces a new file.
                           Archive views use glob (*_table.parquet) to pick up all files. */
                        var parquetPath = Path.Combine(_archivePath, $"{timestamp}_{table}.parquet")
                            .Replace("\\", "/");

                        using var exportCmd = connection.CreateCommand();
                        exportCmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD)";
                        await exportCmd.ExecuteNonQueryAsync();

                        _logger?.LogInformation("Archived {Count} rows from {Table}", rowCount, table);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Failed to archive table {Table}", table);
                    }
                }
            }

            /* Nuke and reinitialize outside the using-connection scope so all handles are closed */
            _logger?.LogInformation("Deleting and reinitializing database");
            await _duckDb.ResetDatabaseAsync();

            _logger?.LogInformation("Database reset complete — archive views now serve all historical data from Parquet");
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Archive-all-and-reset failed");
        }
        finally
        {
            s_archiveLock.Release();
        }
    }

}
