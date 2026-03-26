/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
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

    /* Tables eligible for archival with their time column.
       IMPORTANT: Every table with time-series data must be listed here,
       or it will grow unbounded and push the DB past the 512 MB reset threshold. */
    internal static readonly (string Table, string TimeColumn)[] ArchivableTables =
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
        ("memory_grant_stats", "collection_time"),
        ("waiting_tasks", "collection_time"),
        ("running_jobs", "collection_time"),
        ("database_size_stats", "collection_time"),
        ("server_properties", "collection_time"),
        ("session_stats", "collection_time"),
        ("server_config", "capture_time"),
        ("database_config", "capture_time"),
        ("database_scoped_config", "capture_time"),
        ("trace_flags", "capture_time"),
        ("config_alert_log", "alert_time"),
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

        /* Compact per-cycle files into monthly parquet before refreshing views */
        CompactParquetFiles();

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

    /* Columns to exclude during compaction — dead weight from legacy archives */
    private static readonly Dictionary<string, string[]> CompactionExcludeColumns = new()
    {
        ["query_store_stats"] = ["query_plan_text"]
    };

    /// <summary>
    /// Compacts all per-cycle parquet files into monthly files (YYYYMM_tablename.parquet).
    /// This keeps the archive directory small (~75 files for 3 months of 25 tables)
    /// and dramatically improves DuckDB read_parquet glob performance.
    /// </summary>
    private void CompactParquetFiles()
    {
        if (!Directory.Exists(_archivePath))
        {
            return;
        }

        var allFiles = Directory.GetFiles(_archivePath, "*.parquet")
            .Select(f => Path.GetFileName(f))
            .ToList();

        /* Group files by (month, table). Recognized formats:
           - YYYYMMDD_HHMM_tablename.parquet  (per-cycle)
           - YYYYMMDD_tablename.parquet        (consolidated daily)
           - YYYY-MM_tablename.parquet         (legacy monthly)
           - all_tablename.parquet             (manual consolidation)
           - YYYYMM_tablename.parquet          (monthly — our target format) */
        var groups = new Dictionary<(string Month, string Table), List<string>>();

        foreach (var file in allFiles)
        {
            var name = Path.GetFileNameWithoutExtension(file);

            string? month = null;
            string? table = null;

            /* YYYYMMDD_HHMM_tablename */
            var m = Regex.Match(name, @"^(\d{8})_\d{4}_(.+)$");
            if (m.Success)
            {
                month = m.Groups[1].Value[..6]; /* YYYYMM */
                table = m.Groups[2].Value;
            }

            /* YYYYMMDD_tablename (no HHMM) */
            if (month == null)
            {
                m = Regex.Match(name, @"^(\d{8})_([a-z].+)$");
                if (m.Success)
                {
                    month = m.Groups[1].Value[..6];
                    table = m.Groups[2].Value;
                }
            }

            /* YYYY-MM_tablename (legacy monthly) */
            if (month == null)
            {
                m = Regex.Match(name, @"^(\d{4})-(\d{2})_(.+)$");
                if (m.Success)
                {
                    month = m.Groups[1].Value + m.Groups[2].Value;
                    table = m.Groups[3].Value;
                }
            }

            /* all_tablename (manual consolidation from earlier) */
            if (month == null)
            {
                m = Regex.Match(name, @"^all_(.+)$");
                if (m.Success)
                {
                    /* Put in the earliest month we can find, or current month */
                    month = "orphan";
                    table = m.Groups[1].Value;
                }
            }

            /* imported_YYYYMM_tablename (imported from previous install) */
            if (month == null)
            {
                m = Regex.Match(name, @"^imported_(\d{6})_(.+)$");
                if (m.Success)
                {
                    month = m.Groups[1].Value;
                    table = m.Groups[2].Value;
                }
            }

            /* imported_YYYYMMDD_HHMM_tablename (imported per-cycle files) */
            if (month == null)
            {
                m = Regex.Match(name, @"^imported_(\d{8})_\d{4}_(.+)$");
                if (m.Success)
                {
                    month = m.Groups[1].Value[..6];
                    table = m.Groups[2].Value;
                }
            }

            /* YYYYMM_tablename (already monthly — our target format) */
            if (month == null)
            {
                m = Regex.Match(name, @"^(\d{6})_(.+)$");
                if (m.Success)
                {
                    month = m.Groups[1].Value;
                    table = m.Groups[2].Value;
                }
            }

            if (month != null && table != null)
            {
                var key = (month, table);
                if (!groups.TryGetValue(key, out List<string>? value))
                {
                    value = [];
                    groups[key] = value;
                }

                value.Add(file);
            }
            else
            {
                _logger?.LogWarning("Unrecognized parquet file format: {File}", file);
            }
        }

        /* Compact each group that has more than one file (or any non-monthly files) */
        using var con = new DuckDBConnection("DataSource=:memory:");
        con.Open();

        var totalMerged = 0;
        var totalRemoved = 0;

        foreach (var ((month, table), files) in groups)
        {
            /* If there's exactly one file and it's already in monthly format, skip */
            if (files.Count == 1)
            {
                var name = Path.GetFileNameWithoutExtension(files[0]);
                if (Regex.IsMatch(name, @"^\d{6}_"))
                {
                    continue;
                }
            }

            /* Resolve month for orphan files — use current month */
            var targetMonth = month == "orphan"
                ? DateTime.UtcNow.ToString("yyyyMM")
                : month;

            var targetFile = $"{targetMonth}_{table}.parquet";
            var targetPath = Path.Combine(_archivePath, targetFile).Replace("\\", "/");
            var tempPath = targetPath + ".tmp";

            try
            {
                var sourcePaths = files
                    .Select(f => Path.Combine(_archivePath, f).Replace("\\", "/"))
                    .ToList();
                var pathList = string.Join(", ", sourcePaths.Select(p => $"'{p}'"));

                /* Build SELECT with column exclusions for specific tables.
                   Only exclude columns that actually exist in the source files
                   (they may have been stripped in a previous compaction). */
                var selectClause = "*";
                if (CompactionExcludeColumns.TryGetValue(table, out var excludeCols))
                {
                    using var schemaCmd = con.CreateCommand();
                    schemaCmd.CommandText = $"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([{pathList}], union_by_name=true))";
                    using var reader = schemaCmd.ExecuteReader();
                    var existingCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    while (reader.Read()) existingCols.Add(reader.GetString(0));

                    var colsToExclude = excludeCols.Where(c => existingCols.Contains(c)).ToArray();
                    if (colsToExclude.Length > 0)
                    {
                        selectClause = $"* EXCLUDE ({string.Join(", ", colsToExclude)})";
                    }
                }

                using var cmd = con.CreateCommand();
                cmd.CommandText = $"COPY (SELECT {selectClause} FROM read_parquet([{pathList}], union_by_name=true)) " +
                                  $"TO '{tempPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)";
                cmd.ExecuteNonQuery();

                /* Remove originals */
                var removed = 0;
                foreach (var f in files)
                {
                    var fullPath = Path.Combine(_archivePath, f);
                    try
                    {
                        File.Delete(fullPath);
                        removed++;
                    }
                    catch (IOException ex)
                    {
                        _logger?.LogWarning("Could not delete {File} during compaction: {Message}", f, ex.Message);
                    }
                }

                /* Rename temp to final */
                if (File.Exists(targetPath))
                {
                    File.Delete(targetPath);
                }
                File.Move(tempPath, targetPath);

                totalMerged++;
                totalRemoved += removed;

                _logger?.LogDebug("Compacted {Count} files into {Target}", files.Count, targetFile);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to compact {Month}/{Table} ({Count} files)", month, table, files.Count);

                /* Clean up temp file on failure */
                if (File.Exists(tempPath))
                {
                    try { File.Delete(tempPath); } catch { /* best effort */ }
                }
            }
        }

        if (totalMerged > 0)
        {
            var remaining = Directory.GetFiles(_archivePath, "*.parquet").Length;
            _logger?.LogInformation("Parquet compaction complete: merged {Groups} groups, removed {Removed} files, {Remaining} files remaining",
                totalMerged, totalRemoved, remaining);
        }
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

            /* Compact per-cycle files into monthly parquet files before reset.
               This runs outside the write lock using an in-memory DuckDB connection
               and only touches filesystem files — no contention with collectors. */
            _logger?.LogInformation("Compacting parquet files into monthly archives");
            CompactParquetFiles();

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
