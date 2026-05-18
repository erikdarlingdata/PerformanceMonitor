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
        await WithRaisedCopyMemoryLimit(connection, async () =>
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
COPY (
    SELECT * FROM {table} WHERE {timeColumn} < $1
) TO '{EscapeSqlPath(filePath)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
            cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
            await cmd.ExecuteNonQueryAsync();
        });
    }

    private static string EscapeSqlPath(string path) => DuckDbInitializer.EscapeSqlPath(path);

    /* Resting and COPY memory_limit values for the main DuckDB connection.
       The resting value is also set in DuckDbInitializer.ConnectionString so
       newly-opened connections start at the resting cap; the COPY value is
       applied transiently around parquet COPY operations and restored after.
       See WithRaisedCopyMemoryLimit and the comment block on ConnectionString. */
    private const string MainConnectionRestingMemoryLimit = "1GB";
    private const string MainConnectionCopyMemoryLimit = "4GB";

    /// <summary>
    /// Runs <paramref name="action"/> with the connection's memory_limit raised
    /// to <see cref="MainConnectionCopyMemoryLimit"/>, restoring to
    /// <see cref="MainConnectionRestingMemoryLimit"/> after. Use around parquet
    /// COPY operations on the main connection — those hit a DuckDB
    /// pre-reservation behavior that needs more headroom than the resting cap
    /// (#933). memory_limit is instance-level; concurrent operations briefly
    /// see the raised cap.
    /// </summary>
    private static async Task WithRaisedCopyMemoryLimit(DuckDBConnection connection, Func<Task> action)
    {
        using (var raiseCmd = connection.CreateCommand())
        {
            raiseCmd.CommandText = $"SET memory_limit = '{MainConnectionCopyMemoryLimit}'";
            await raiseCmd.ExecuteNonQueryAsync();
        }

        try
        {
            await action();
        }
        finally
        {
            try
            {
                using var restoreCmd = connection.CreateCommand();
                restoreCmd.CommandText = $"SET memory_limit = '{MainConnectionRestingMemoryLimit}'";
                await restoreCmd.ExecuteNonQueryAsync();
            }
            catch
            {
                /* Best-effort restore. If this fails the connection is in a bad
                   state and will be disposed by the caller's `using` shortly. */
            }
        }
    }

    /* Columns to exclude during compaction — dead weight from legacy archives */
    private static readonly Dictionary<string, string[]> CompactionExcludeColumns = new()
    {
        ["query_store_stats"] = ["query_plan_text"]
    };

    /* Maximum total on-disk parquet bytes per compaction merge batch. Wide-VARCHAR
       tables (query_snapshots) expand 5-10x on read; this cap keeps the in-memory
       working set during a COPY well below the 4 GB compaction memory_limit even
       on the worst data shapes. Groups exceeding this budget produce multiple
       _ptNNN.parquet output files. See #933 followup — a 72-file query_snapshots
       backlog at 4 GB OOM'd on real allocation pressure during the final merge. */
    private const long MaxBatchInputBytes = 200L * 1024 * 1024; /* 200 MB */

    /* Greedily group <paramref name="sortedPaths"/> (smallest-first) into batches
       whose total on-disk bytes don't exceed <paramref name="maxBytes"/>. A single
       file larger than the cap becomes its own one-element batch — that's the
       degenerate case (the cap can't split an individual file) and the caller
       handles it as a single-file pass-through merge. */
    private static List<List<string>> BuildSizeBudgetedBatches(IReadOnlyList<string> sortedPaths, long maxBytes)
    {
        var batches = new List<List<string>>();
        var current = new List<string>();
        long currentBytes = 0;

        foreach (var p in sortedPaths)
        {
            var size = new FileInfo(p.Replace("/", "\\")).Length;
            if (currentBytes + size > maxBytes && current.Count > 0)
            {
                batches.Add(current);
                current = new List<string>();
                currentBytes = 0;
            }
            current.Add(p);
            currentBytes += size;
        }
        if (current.Count > 0)
        {
            batches.Add(current);
        }

        return batches;
    }

    /* Merge one size-budgeted batch into <paramref name="outputPath"/>. The pragma
       block matches the compaction tuning from #933:
         - memory_limit = 4GB: parquet COPY does allocations that bypass the buffer
           manager and can't be spilled. The cap is a hard ceiling for those, not
           a spill trigger. 4GB leaves real headroom for wide-VARCHAR data within
           the batch-size budget. Aligns with DuckDB's OOM guide (50-60% of RAM).
         - threads = 2: fewer per-thread row-group buffers in flight.
         - ROW_GROUP_SIZE 8192: smaller buffered batch per row group.
         - preserve_insertion_order = false: lets DuckDB stream.
       See tools/CompactionRepro for the stress reproducer. */
    private void MergeBatchToFile(string table, List<string> sourcePaths, string outputPath, string spillDirSql)
    {
        if (sourcePaths.Count <= 2)
        {
            /* Small batch — single-pass merge (also covers the degenerate 1-file case). */
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
                              $"TO '{EscapeSqlPath(outputPath)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 8192)";
            cmd.ExecuteNonQuery();
            return;
        }

        /* Larger batch — incremental pairwise merge. Caller has already sorted
           smallest-first across the whole group; within a batch we preserve that
           order so the accumulator grows steadily and small files are folded in
           early when memory is cheapest. */
        var currentPath = sourcePaths[0];
        var intermediateFiles = new List<string>();

        for (var i = 1; i < sourcePaths.Count; i++)
        {
            var stepOutput = i < sourcePaths.Count - 1
                ? outputPath + $".step{i}.tmp"
                : outputPath;

            using var con = new DuckDBConnection("DataSource=:memory:");
            con.Open();
            using (var pragma = con.CreateCommand())
            {
                pragma.CommandText = $"SET memory_limit = '4GB'; SET threads = 2; SET preserve_insertion_order = false; SET temp_directory = '{EscapeSqlPath(spillDirSql)}';";
                pragma.ExecuteNonQuery();
            }

            var selectClause = BuildSelectClause(table, new[] { currentPath, sourcePaths[i] });
            var pairList = $"'{EscapeSqlPath(currentPath)}', '{EscapeSqlPath(sourcePaths[i])}'";
            using var cmd = con.CreateCommand();
            cmd.CommandText = $"COPY (SELECT {selectClause} FROM read_parquet([{pairList}], union_by_name=true)) " +
                              $"TO '{EscapeSqlPath(stepOutput)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 8192)";
            cmd.ExecuteNonQuery();

            if (intermediateFiles.Count > 0)
            {
                var prev = intermediateFiles[^1];
                try { File.Delete(prev); } catch { /* best effort */ }
            }
            intermediateFiles.Add(stepOutput);
            currentPath = stepOutput;
        }
    }

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

            /* YYYYMM_tablename_ptNNN (multi-part monthly — must match before the
               generic YYYYMM_tablename regex below, otherwise the trailing _ptNNN
               gets captured as part of the table name and groups get split). */
            if (month == null)
            {
                m = Regex.Match(name, @"^(\d{6})_(.+)_pt\d{3}$");
                if (m.Success)
                {
                    month = m.Groups[1].Value;
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
            /* If there's exactly one file and it's already in monthly format, skip.
               This regex matches both YYYYMM_table.parquet and YYYYMM_table_ptNNN.parquet. */
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

            try
            {
                var sourcePaths = files
                    .Select(f => Path.Combine(_archivePath, f).Replace("\\", "/"))
                    .ToList();

                /* Sort smallest-first so size-budget batches fill cheaply at first. */
                var sorted = sourcePaths
                    .OrderBy(p => new FileInfo(p.Replace("/", "\\")).Length)
                    .ToList();

                /* Bucket files into size-budgeted batches. Cap each batch's on-disk
                   parquet bytes so a single COPY doesn't try to merge an unbounded
                   amount of expanded VARCHAR data. Wide-row tables (query_snapshots'
                   plan XML) expand ~5-10x in memory on read; a 72-file backlog at
                   the 4 GB compaction memory_limit OOM'd on real allocation pressure
                   (not pre-reservation) — see #933 followup. The cap is sized so
                   that even with ~10x expansion the in-memory load stays well under
                   4 GB. Narrow tables fit one batch with hundreds of files in it. */
                var batches = BuildSizeBudgetedBatches(sorted, MaxBatchInputBytes);

                /* Plan the output names. With one batch we keep the existing
                   YYYYMM_table.parquet name (backward compatible). With multiple
                   batches we emit YYYYMM_table_ptNNN.parquet — the archive views
                   already glob "*_table.parquet" so readers see them all. */
                var batchOutputs = new List<(string TempPath, string FinalPath)>();
                for (var i = 0; i < batches.Count; i++)
                {
                    var finalName = batches.Count == 1
                        ? $"{targetMonth}_{table}.parquet"
                        : $"{targetMonth}_{table}_pt{i + 1:D3}.parquet";
                    var finalPath = Path.Combine(_archivePath, finalName).Replace("\\", "/");
                    batchOutputs.Add((TempPath: finalPath + ".tmp", FinalPath: finalPath));
                }

                /* Run each batch's merge into its temp file. If any batch throws,
                   the catch below cleans up all temps and we leave the originals in
                   place for next cycle's retry. */
                for (var i = 0; i < batches.Count; i++)
                {
                    MergeBatchToFile(table, batches[i], batchOutputs[i].TempPath, spillDirSql);
                }

                /* All batches succeeded — delete originals, promote temps. */
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

                foreach (var (tempPath, finalPath) in batchOutputs)
                {
                    if (File.Exists(finalPath))
                    {
                        File.Delete(finalPath);
                    }
                    File.Move(tempPath, finalPath);
                }

                totalMerged++;
                totalRemoved += removed;

                if (batches.Count == 1)
                {
                    _logger?.LogDebug("Compacted {Count} files into {Target}", files.Count, batchOutputs[0].FinalPath);
                }
                else
                {
                    _logger?.LogInformation("Compacted {Count} files into {Parts} part files for {Month}/{Table} (input too large for single batch)",
                        files.Count, batches.Count, targetMonth, table);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to compact {Month}/{Table} ({Count} files)", month, table, files.Count);

                /* Best-effort cleanup of any temp/intermediate files. */
                foreach (var stepFile in Directory.GetFiles(_archivePath, $"{targetMonth}_{table}*.tmp"))
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

                        await WithRaisedCopyMemoryLimit(connection, async () =>
                        {
                            using var exportCmd = connection.CreateCommand();
                            exportCmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{EscapeSqlPath(parquetPath)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
                            await exportCmd.ExecuteNonQueryAsync();
                        });

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
                        await WithRaisedCopyMemoryLimit(connection, async () =>
                        {
                            using var exportCmd = connection.CreateCommand();
                            exportCmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{EscapeSqlPath(preservePath)}' (FORMAT PARQUET)";
                            await exportCmd.ExecuteNonQueryAsync();
                        });
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
