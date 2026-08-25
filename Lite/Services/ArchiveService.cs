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
using PerformanceMonitor.Collectors;
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

    /* Tables eligible for archival with their time column. Catalog-driven: every collector table
       (from CollectorCatalog, with its prefix time column — collection_time everywhere except the
       four config snapshots' capture_time) plus the two non-collector time-series tables. Adding a
       collector gives it archival for free; the former hand-maintained list could silently omit a
       new table and let it grow unbounded past the 512 MB reset threshold. Mirrors
       DuckDbInitializer.ArchivableTables (same table set); a test pins the two together. */
    internal static readonly (string Table, string TimeColumn)[] ArchivableTables =
        /* Filtered exactly as DuckDbInitializer.ArchivableTables is, and for the same reason: Lite never
           creates the PostgreSQL collectors' tables, so archiving them would target nothing. */
        DuckDbSchemaGenerator.StoredCollectors.Select(c => (c.TargetTable, c.PrefixTimeColumnName))
            .Concat([("config_alert_log", "alert_time"), ("collection_log", "collection_time")])
            .ToArray();

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

        /* Archive each table independently. Export-to-Parquet (COPY ... TO)
           only READS the database, so it runs under a read lock — concurrently
           with the UI. Only the DELETE modifies the file, so just the DELETE
           takes the exclusive write lock, and only briefly. This keeps the UI
           responsive during archival instead of freezing it for the whole
           export (issue #979).

           Exporting and deleting in separate lock scopes is safe here: the
           DELETE only removes rows older than cutoffDate, and collectors only
           ever insert rows timestamped "now", so nothing archivable can be
           written into the gap between the export and the DELETE. */
        foreach (var (table, timeColumn) in ArchivableTables)
        {
            try
            {
                /* Uniquely-named parquet file — no merging needed. Each archival
                   cycle produces a new file with a timestamp prefix; archive
                   views use glob (*_table.parquet) to pick up all files. */
                var parquetPath = Path.Combine(_archivePath, $"{timestamp}_{table}.parquet")
                    .Replace("\\", "/");
                /* Export to a .tmp first (excluded from the *_table.parquet glob), then promote.
                   A mid-COPY failure (OOM/disk-full/process kill) must not leave a truncated
                   parquet that matches the glob and breaks the archive view for the whole table. */
                var tempParquetPath = parquetPath + ".tmp";

                long rowCount;

                /* Export under a read lock — runs alongside UI queries. */
                using (_duckDb.AcquireReadLock())
                {
                    using var readConnection = _duckDb.CreateConnection();
                    await readConnection.OpenAsync();

                    rowCount = await GetRowCountBeforeCutoff(readConnection, table, timeColumn, cutoffDate);
                    if (rowCount == 0)
                    {
                        continue;
                    }

                    await ExportToParquet(readConnection, table, timeColumn, cutoffDate, tempParquetPath);
                }

                /* Promote the temp only after the COPY has fully succeeded. */
                if (File.Exists(parquetPath))
                {
                    File.Delete(parquetPath);
                }
                File.Move(tempParquetPath, parquetPath);

                /* Delete the archived rows under the write lock. The DELETE
                   modifies table data and the next CHECKPOINT reorganizes the
                   file — readers must not be mid-query when that happens or
                   they get "Reached the end of the file" errors — but the
                   DELETE itself is fast, so the UI stall is brief. */
                try
                {
                    using (_duckDb.AcquireWriteLock())
                    {
                        using var writeConnection = _duckDb.CreateConnection();
                        await writeConnection.OpenAsync();

                        using var deleteCmd = writeConnection.CreateCommand();
                        deleteCmd.CommandText = $"DELETE FROM {table} WHERE {timeColumn} < $1";
                        deleteCmd.Parameters.Add(new DuckDBParameter { Value = cutoffDate });
                        await deleteCmd.ExecuteNonQueryAsync();
                    }
                }
                catch
                {
                    /* The rows are still in the table (DELETE failed), so they aren't lost —
                       discard the archive file we just wrote so the same rows aren't counted in
                       both the table and the parquet (double-counted by v_* views and re-exported
                       next cycle). */
                    try { File.Delete(parquetPath); } catch { /* best effort */ }
                    throw;
                }

                _logger?.LogInformation("Archived {Count} rows from {Table} to {Path}", rowCount, table, parquetPath);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to archive table {Table}", table);
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
    ///
    /// <para><b>internal rather than private</b> so the #1912 archive repair reuses this exact raise/restore
    /// instead of carrying its own copy of the number. The floor is a KNOWN-BROKEN-BELOW-2GB constraint, not a
    /// tuning preference — DuckDB pre-reserves ~99% of memory_limit the moment a parquet COPY begins, so a
    /// second literal drifting downward is precisely how it gets re-broken (#942 lowered it to 1GB on sound-
    /// looking reasoning and broke compaction; #952 put it back).</para>
    /// </summary>
    internal static async Task WithRaisedCopyMemoryLimit(DuckDBConnection connection, Func<Task> action)
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
            /* Best-effort: some tables can't be merged within the memory cap and
               are skipped — their per-cycle files are left in place and pruned by
               retention (see ParquetCompaction.SkipCompactionTables and #933). */
            if (ParquetCompaction.ShouldSkipCompaction(table))
            {
                continue;
            }

            /* If every file in the group is already in final monthly/part format
               (YYYYMM_table or YYYYMM_table_ptNNN), there are no new per-cycle files to fold in,
               so skip. Otherwise a month that legitimately split into N part files (input over
               the per-batch budget) gets re-read and re-written on every archival cycle. */
            if (files.All(f => Regex.IsMatch(Path.GetFileNameWithoutExtension(f), @"^\d{6}_.+?(_pt\d{3})?$")))
            {
                continue;
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

                /* Bucket files into size-budgeted batches so a single COPY never
                   merges an unbounded amount of data. Wide query-plan-XML tables
                   that can't merge within the cap are skipped above; the tables
                   that reach here compress mildly, so the on-disk budget is a fine
                   proxy and they fit one batch with many files (#933). */
                var batches = ParquetCompaction.BuildSizeBudgetedBatches(
                    sorted, ParquetCompaction.DefaultBatchInputBytes);

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
                    ParquetCompaction.MergeBatchToFile(table, batches[i], batchOutputs[i].TempPath, spillDirSql);
                }

                /* All batches succeeded — promote temps to their final names FIRST, then delete
                   the originals. Deleting first risked PERMANENT data loss: the temps hold the
                   only merged copy of the just-deleted originals, and if a promote then failed
                   (e.g. File.Delete(finalPath) throws because a UI reader has the monthly file
                   open mid read_parquet) the catch below deletes those temps. Promoting first
                   keeps the originals as a fallback until the new files are safely in place. */
                var finalPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var (tempPath, finalPath) in batchOutputs)
                {
                    if (File.Exists(finalPath))
                    {
                        File.Delete(finalPath);
                    }
                    File.Move(tempPath, finalPath);
                    finalPaths.Add(finalPath);
                }

                var removed = 0;
                foreach (var f in files)
                {
                    var fullPath = Path.Combine(_archivePath, f).Replace("\\", "/");
                    /* A source file can share the name of a promoted output when the monthly file
                       is itself re-merged; that path now holds the freshly merged data — never
                       delete it. */
                    if (finalPaths.Contains(fullPath))
                    {
                        continue;
                    }
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

        /* Wait for in-flight collections before deleting the database (#2594). The write lock below does NOT
           exclude them: the collection path takes no lock at all, so a reset could delete monitor.duckdb
           underneath a collector that was still writing - which is what happened in the field, via a
           tab-open collection that is sequenced against nothing.

           Null means a collection outlasted the drain timeout, and the right response is to DEFER. This is
           size-triggered, the store is a little over a soft threshold, and the next tick will try again;
           resetting on schedule matters far less than not resetting under a live collector. */
        var resetScope = await CollectionResetGate.TryBeginResetAsync();

        if (resetScope is null)
        {
            _logger?.LogInformation(
                "Database reset deferred: {InFlight} collection(s) still running. It will be retried on the " +
                "next archival check.",
                CollectionResetGate.CollectionsInFlight);
            s_archiveLock.Release();
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
            try
            {
                CompactParquetFiles();
            }
            catch (Exception compactEx)
            {
                /* Compaction is best-effort (merging per-cycle parquet into monthly files); a failure
                   must not abort the archive/reset. Previously unlogged — surface it so a stuck or
                   oversized backlog is visible instead of silently degrading. */
                _logger?.LogError(compactEx, "Parquet compaction failed; continuing with archive and reset");
            }

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
            resetScope.Dispose();
            s_archiveLock.Release();
        }
    }

}
