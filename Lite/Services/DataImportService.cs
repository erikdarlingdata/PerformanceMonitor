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

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Imports historical data from a previous Lite install by flushing the old DuckDB
/// hot tables to parquet, copying parquet files to the current archive folder,
/// and refreshing archive views.
/// </summary>
public class DataImportService
{
    private readonly DuckDbInitializer _duckDb;
    private readonly string _currentArchivePath;

    public DataImportService(DuckDbInitializer duckDb, string currentArchivePath)
    {
        _duckDb = duckDb;
        _currentArchivePath = currentArchivePath;
    }

    /// <summary>
    /// Result of a data import operation.
    /// </summary>
    public sealed class ImportResult
    {
        public bool Success { get; init; }
        public int FilesImported { get; init; }
        public int TablesFlushed { get; init; }
        public string? ErrorMessage { get; init; }
    }

    /// <summary>
    /// Validates that the selected folder contains a monitor.duckdb file.
    /// </summary>
    public static bool ValidateSourceFolder(string folderPath)
    {
        return File.Exists(Path.Combine(folderPath, "monitor.duckdb"));
    }

    /// <summary>
    /// Attempts to open the old DuckDB in read-write mode to flush hot table data to parquet.
    /// Returns true if successful, false if the database is locked.
    /// </summary>
    public static (bool Locked, int TablesFlushed) FlushOldDatabase(string folderPath)
    {
        var dbPath = Path.Combine(folderPath, "monitor.duckdb");
        var archivePath = Path.Combine(folderPath, "archive");
        var tablesFlushed = 0;

        DuckDBConnection? connection = null;
        try
        {
            connection = new DuckDBConnection($"Data Source={dbPath}");
            connection.Open();
        }
        catch (Exception ex)
        {
            AppLogger.Warn("DataImport", $"Could not open old database for flushing: {ex.Message}");
            connection?.Dispose();
            return (true, 0);
        }

        try
        {
            if (!Directory.Exists(archivePath))
            {
                Directory.CreateDirectory(archivePath);
            }

            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");

            foreach (var (table, _) in ArchiveService.ArchivableTables)
            {
                try
                {
                    /* Check if the table exists in the old database */
                    using var checkCmd = connection.CreateCommand();
                    checkCmd.CommandText = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}'";
                    var exists = Convert.ToInt64(checkCmd.ExecuteScalar()) > 0;
                    if (!exists)
                    {
                        continue;
                    }

                    /* Check if the table has rows */
                    using var countCmd = connection.CreateCommand();
                    countCmd.CommandText = $"SELECT COUNT(*) FROM {table}";
                    var rowCount = Convert.ToInt64(countCmd.ExecuteScalar());
                    if (rowCount == 0)
                    {
                        continue;
                    }

                    /* Export all rows to parquet */
                    var parquetPath = Path.Combine(archivePath, $"{timestamp}_{table}.parquet")
                        .Replace("\\", "/");

                    using var exportCmd = connection.CreateCommand();
                    exportCmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD)";
                    exportCmd.ExecuteNonQuery();

                    tablesFlushed++;
                    AppLogger.Info("DataImport", $"Flushed {rowCount} rows from old {table} to {Path.GetFileName(parquetPath)}");
                }
                catch (Exception ex)
                {
                    AppLogger.Warn("DataImport", $"Skipped flushing table {table}: {ex.Message}");
                }
            }
        }
        finally
        {
            connection.Dispose();
        }

        return (false, tablesFlushed);
    }

    /// <summary>
    /// Copies all parquet files from the old install's archive folder to the current archive folder.
    /// Files are prefixed with "imported_" to avoid naming collisions.
    /// Returns the count of files copied.
    /// </summary>
    public int CopyParquetFiles(string sourceFolder)
    {
        var sourceArchive = Path.Combine(sourceFolder, "archive");
        if (!Directory.Exists(sourceArchive))
        {
            AppLogger.Info("DataImport", "No archive folder found in source — nothing to copy");
            return 0;
        }

        if (!Directory.Exists(_currentArchivePath))
        {
            Directory.CreateDirectory(_currentArchivePath);
        }

        var sourceFiles = Directory.GetFiles(sourceArchive, "*.parquet");
        if (sourceFiles.Length == 0)
        {
            AppLogger.Info("DataImport", "No parquet files found in source archive");
            return 0;
        }

        var copied = 0;
        foreach (var sourceFile in sourceFiles)
        {
            try
            {
                var fileName = Path.GetFileName(sourceFile);

                /* Prefix with imported_ (strip existing imported_ prefix if re-importing) */
                if (fileName.StartsWith("imported_", StringComparison.OrdinalIgnoreCase))
                {
                    /* Already prefixed — keep the same name to overwrite on re-import */
                }
                else
                {
                    fileName = $"imported_{fileName}";
                }

                var destPath = Path.Combine(_currentArchivePath, fileName);
                File.Copy(sourceFile, destPath, overwrite: true);
                copied++;
            }
            catch (Exception ex)
            {
                AppLogger.Warn("DataImport", $"Failed to copy {Path.GetFileName(sourceFile)}: {ex.Message}");
            }
        }

        AppLogger.Info("DataImport", $"Copied {copied} parquet files to current archive");
        return copied;
    }

    /// <summary>
    /// Refreshes archive views so the imported parquet files are picked up by glob patterns.
    /// </summary>
    public async Task RefreshViewsAsync()
    {
        await _duckDb.CreateArchiveViewsAsync();
        AppLogger.Info("DataImport", "Archive views refreshed after import");
    }

    /// <summary>
    /// Runs the full import pipeline: flush old DB, copy parquet files, refresh views.
    /// This method is designed to be called from a background thread via Task.Run.
    /// The tryLockOldDb callback handles the retry/cancel dialog on the UI thread.
    /// </summary>
    public async Task<ImportResult> RunImportAsync(string sourceFolder, Func<string, Task<bool>> tryLockOldDb)
    {
        try
        {
            AppLogger.Info("DataImport", $"Starting import from {sourceFolder}");

            /* Step 1: Flush old database */
            var (locked, tablesFlushed) = FlushOldDatabase(sourceFolder);
            while (locked)
            {
                var retry = await tryLockOldDb(sourceFolder);
                if (!retry)
                {
                    AppLogger.Info("DataImport", "Import cancelled by user (database lock)");
                    return new ImportResult { Success = false, ErrorMessage = "Import cancelled." };
                }
                (locked, tablesFlushed) = FlushOldDatabase(sourceFolder);
            }

            AppLogger.Info("DataImport", $"Flushed {tablesFlushed} tables from old database");

            /* Step 2: Copy parquet files */
            var filesCopied = CopyParquetFiles(sourceFolder);

            /* Step 3: Refresh archive views */
            await RefreshViewsAsync();

            AppLogger.Info("DataImport", $"Import complete: {tablesFlushed} tables flushed, {filesCopied} files imported");

            return new ImportResult
            {
                Success = true,
                FilesImported = filesCopied,
                TablesFlushed = tablesFlushed
            };
        }
        catch (Exception ex)
        {
            AppLogger.Error("DataImport", "Import failed", ex);
            return new ImportResult
            {
                Success = false,
                ErrorMessage = $"Import failed: {ex.Message}"
            };
        }
    }
}
