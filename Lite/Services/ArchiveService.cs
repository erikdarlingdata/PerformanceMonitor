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

    /// <summary>
    /// Indicates whether an archival operation is currently in progress.
    /// UI code can check this to warn users before dismiss or show a status indicator.
    /// Volatile-backed to ensure cross-thread visibility without locking.
    /// </summary>
    private static volatile bool s_isArchiving;
    public static bool IsArchiving
    {
        get => s_isArchiving;
        private set => s_isArchiving = value;
    }

    /* Config tables that must be preserved through ArchiveAllAndResetAsync.
       These hold user configuration (not time-series) and must survive when the
       size threshold trips a database reset. Issue #938 — permanent mute rules
       were silently lost because ResetDatabaseAsync deletes monitor.duckdb. */
    private static readonly string[] PreservedConfigTables =
    [
        "config_mute_rules",
        "dismissed_archive_alerts"
    ];

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
        ("memory_pressure_events", "collection_time"),
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

        IsArchiving = true;
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
                    deleteCmd.CommandText = $"DELETE FROM {table} WHERE {timeColumn} < $1";
                    deleteCmd.Parameters.Add(new DuckDBParameter { Value = cutoffDate });
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
            IsArchiving = false;
            s_archiveLock.Release();
        }
    }

    private static async Task<long> GetRowCountBeforeCutoff(DuckDBConnection connection, string table, string timeColumn, DateTime cutoff)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table} WHERE {timeColumn} < $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt64(result);
    }

    private static async Task ExportToParquet(DuckDBConnection connection, string table, string timeColumn, DateTime cutoff, string filePath)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
COPY (
    SELECT * FROM {table} WHERE {timeColumn} < $1
) TO '{EscapeSqlPath(filePath)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
        cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        await cmd.ExecuteNonQueryAsync();
    }

    private static string EscapeSqlPath(string path) => DuckDbInitializer.EscapeSqlPath(path);

    /* Columns to exclude during compaction — dead weight from legacy archives */
    private static readonly Dictionary<string, string[]> CompactionExcludeColumns = new()
    {
        ["query_store_stats"] = ["query_plan_text"]
    };

    /* Build the SELECT clause for a compaction COPY, excluding only the
       CompactionExcludeColumns actually present in THIS set of files.
       Detection must be per-merge-set, not global: archive files predating a
       schema change lack the column, so a globally-computed "* EXCLUDE (col)"
       fails the binder on a pair where neither file has it. query_plan_text
       was added to query_store_stats in migration v13 (2026-02-23), so a
       reporter's pre-v13 archives don't carry it. (#933) */
    private static string BuildSelectClause(string table, IReadOnlyList<string> paths)
    {
        if (!CompactionExcludeColumns.TryGetValue(table, out var excludeCols))
        {
            return "*";
        }

        using var schemaCon = new DuckDBConnection("DataSource=:memory:");
        schemaCon.Open();
        var pathList = string.Join(", ", paths.Select(p => $"'{EscapeSqlPath(p)}'"));
        using var schemaCmd = schemaCon.CreateCommand();
        schemaCmd.CommandText = $"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([{pathList}], union_by_name=true))";
        using var reader = schemaCmd.ExecuteReader();
        var existingCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (reader.Read()) existingCols.Add(reader.GetString(0));

        var colsToExclude = excludeCols.Where(c => existingCols.Contains(c)).ToArray();
        return colsToExclude.Length > 0
            ? $"* EXCLUDE ({string.Join(", ", colsToExclude)})"
            : "*";
    }

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

        /* Compact each group that has more than one file (or any non-monthly files).
           Each group gets its own DuckDB connection so memory is fully released between groups. */
        var totalMerged = 0;
        var totalRemoved = 0;

        /* Spill directory for the in-memory compaction connections. Set per #935
           so DuckDB has somewhere to page if it chooses to. In practice (see #933)
           the parquet COPY path uses allocations that bypass the buffer manager
           and never actually spill — DuckDB's own OOM guide warns about this. We
           keep the dir set for any code path that *can* spill, but memory_limit
           below has to leave real headroom on top of those un-spillable allocs.
           Co-locating with the archive keeps the write on the same volume the
           parquet files already live on. */
        var spillDir = Path.Combine(_archivePath, "duckdb_tmp");
        Directory.CreateDirectory(spillDir);
        var spillDirSql = spillDir.Replace("\\", "/");

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

                if (sourcePaths.Count <= 2)
                {
                    /* Small group — single-pass merge.

                       Pragma tuning (history per #933):
                         - memory_limit = 4GB: parquet COPY does allocations that
                           bypass the buffer manager and can't be spilled. The cap
                           is effectively a hard ceiling for those, not a spill
                           trigger. At 1GB (the prior value) the reproducer dies
                           at ~900/953 MiB used before any rows are read. 4GB
                           leaves enough headroom for query_snapshots-shaped data
                           (wide VARCHAR plan XML) and aligns with DuckDB's OOM
                           guide recommendation of 50-60% of system RAM.
                         - threads = 2: fewer per-thread row-group buffers in flight.
                         - ROW_GROUP_SIZE 8192: smaller buffered batch per group.
                         - preserve_insertion_order = false: lets DuckDB stream.
                       See tools/CompactionRepro for the stress reproducer. */
                    using var con = new DuckDBConnection("DataSource=:memory:");
                    con.Open();
                    using (var pragma = con.CreateCommand())
                    {
                        pragma.CommandText = $"SET memory_limit = '4GB'; SET threads = 2; SET preserve_insertion_order = false; SET temp_directory = '{EscapeSqlPath(spillDirSql)}';";
                        pragma.ExecuteNonQuery();
                    }

                    var selectClause = BuildSelectClause(table, sourcePaths);
                    var pathList = string.Join(", ", sourcePaths.Select(p => $"'{EscapeSqlPath(p)}'"));
                    using var cmd = con.CreateCommand();
                    cmd.CommandText = $"COPY (SELECT {selectClause} FROM read_parquet([{pathList}], union_by_name=true)) " +
                                      $"TO '{EscapeSqlPath(tempPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 8192)";
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    /* Large group — incremental merge (pairs) to keep peak memory low.
                       Sort smallest-first so early merges are cheap. */
                    var sorted = sourcePaths
                        .OrderBy(p => new FileInfo(p.Replace("/", "\\")).Length)
                        .ToList();

                    var currentPath = sorted[0];
                    var intermediateFiles = new List<string>();

                    for (var i = 1; i < sorted.Count; i++)
                    {
                        var stepOutput = i < sorted.Count - 1
                            ? targetPath + $".step{i}.tmp"
                            : tempPath;

                        using var con = new DuckDBConnection("DataSource=:memory:");
                        con.Open();
                        using (var pragma = con.CreateCommand())
                        {
                            pragma.CommandText = $"SET memory_limit = '4GB'; SET threads = 2; SET preserve_insertion_order = false; SET temp_directory = '{EscapeSqlPath(spillDirSql)}';";
                            pragma.ExecuteNonQuery();
                        }

                        var selectClause = BuildSelectClause(table, new[] { currentPath, sorted[i] });
                        var pairList = $"'{EscapeSqlPath(currentPath)}', '{EscapeSqlPath(sorted[i])}'";
                        using var cmd = con.CreateCommand();
                        cmd.CommandText = $"COPY (SELECT {selectClause} FROM read_parquet([{pairList}], union_by_name=true)) " +
                                          $"TO '{EscapeSqlPath(stepOutput)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 8192)";
                        cmd.ExecuteNonQuery();

                        /* Clean up previous intermediate file */
                        if (intermediateFiles.Count > 0)
                        {
                            var prev = intermediateFiles[^1];
                            try { File.Delete(prev); } catch { /* best effort */ }
                        }

                        intermediateFiles.Add(stepOutput);
                        currentPath = stepOutput;
                    }
                }

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

                /* Clean up temp and intermediate files on failure */
                if (File.Exists(tempPath))
                {
                    try { File.Delete(tempPath); } catch { /* best effort */ }
                }
                foreach (var stepFile in Directory.GetFiles(_archivePath, $"{targetMonth}_{table}.parquet.step*.tmp"))
                {
                    try { File.Delete(stepFile); } catch { /* best effort */ }
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

        IsArchiving = true;
        var preserveDir = Path.Combine(Path.GetTempPath(), $"pm_preserve_{Guid.NewGuid():N}");
        var preservedFiles = new Dictionary<string, string>();
        try
        {
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");

            _logger?.LogInformation("Archiving ALL data to Parquet (prefix: {Timestamp}) and resetting database", timestamp);

            Directory.CreateDirectory(preserveDir);

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
                        exportCmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{EscapeSqlPath(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
                        await exportCmd.ExecuteNonQueryAsync();

                        _logger?.LogInformation("Archived {Count} rows from {Table}", rowCount, table);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Failed to archive table {Table}", table);
                    }
                }

                /* Preserve config tables that must survive the reset (issue #938).
                   Written to a temp dir, not the archive dir — these are restored
                   into the new database, not exposed via archive views. */
                foreach (var table in PreservedConfigTables)
                {
                    try
                    {
                        using var countCmd = connection.CreateCommand();
                        countCmd.CommandText = $"SELECT COUNT(*) FROM {table}";
                        var rowCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
                        if (rowCount == 0) continue;

                        var preservePath = Path.Combine(preserveDir, $"{table}.parquet").Replace("\\", "/");
                        using var exportCmd = connection.CreateCommand();
                        exportCmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{EscapeSqlPath(preservePath)}' (FORMAT PARQUET)";
                        await exportCmd.ExecuteNonQueryAsync();
                        preservedFiles[table] = preservePath;

                        _logger?.LogInformation("Preserved {Count} rows from {Table} for restoration after reset", rowCount, table);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Failed to preserve {Table} before reset — rows will be lost", table);
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

            /* Restore preserved config rows into the freshly initialized tables. */
            var allRestoresSucceeded = true;
            if (preservedFiles.Count > 0)
            {
                using (_duckDb.AcquireWriteLock())
                {
                    using var connection = _duckDb.CreateConnection();
                    await connection.OpenAsync();
                    foreach (var (table, path) in preservedFiles)
                    {
                        try
                        {
                            using var insertCmd = connection.CreateCommand();
                            insertCmd.CommandText = $"INSERT INTO {table} SELECT * FROM read_parquet('{EscapeSqlPath(path)}')";
                            await insertCmd.ExecuteNonQueryAsync();
                            _logger?.LogInformation("Restored rows to {Table} after database reset", table);
                        }
                        catch (Exception ex)
                        {
                            allRestoresSucceeded = false;
                            _logger?.LogError(ex, "Failed to restore {Table} from {Path} — preservation files retained for manual recovery", table, path);
                        }
                    }
                }
            }

            _logger?.LogInformation("Database reset complete — archive views now serve all historical data from Parquet");

            /* Clean up temp preservation dir only if every restore succeeded.
               On failure, leave the parquet files so the user can recover manually. */
            if (allRestoresSucceeded)
            {
                try
                {
                    if (Directory.Exists(preserveDir))
                        Directory.Delete(preserveDir, recursive: true);
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Could not clean up preservation temp dir {Dir}", preserveDir);
                }
            }
            else
            {
                _logger?.LogWarning("Preservation files retained at {Dir} for manual recovery", preserveDir);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Archive-all-and-reset failed — preservation files (if any) retained at {Dir}", preserveDir);
        }
        finally
        {
            IsArchiving = false;
            s_archiveLock.Release();
        }
    }

}
