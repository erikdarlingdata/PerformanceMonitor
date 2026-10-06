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
    /// How many days of rows the scheduled archival leaves in the hot tables: the one archival age. The
    /// scheduled run (<see cref="ArchiveOldDataAsync"/>) moves anything older to Parquet, so a hot table holds
    /// no snapshot older than this, plus up to an hour until the next run. The database-state sweep reads a
    /// newest snapshot older than this as "no current data" (see <c>LocalDataService.GetDatabaseStateDeviationsAsync</c>).
    /// </summary>
    internal const int HotDataDays = 7;

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

    /* Test seams. Production leaves each one null or at its default. */
    internal Action<IReadOnlyList<string>>? OnCompactionTempsReadyForTests { get; set; }
    internal Action<string>? BeforeTableExportForTests { get; set; }
    internal Action? BeforeDatabaseResetForTests { get; set; }

    /* Fires after the reset has cleared the tables and before the preserved config rows are put back (#4824): the
       moment a reader would find those tables empty. */
    internal Action? AfterDatabaseResetForTests { get; set; }

    /* Fires with the table name right after that table's preserved rows were put back, so a test can stand in for a
       process kill between two tables of the restore. */
    internal static Action<string>? AfterPreservedTableRestoredForTests { get; set; }

    /* Fires with the table name inside the restore loop's per-table try, before that table's rows are put back, so a
       test can fail one table's restore and leave the restore marker and the preserved copy in place. */
    internal static Action<string>? BeforePreservedTableRestoreForTests { get; set; }

    /* Fires right after the export marker is deleted and the reset has begun, before the database files are deleted:
       the point where the archive files hold the only copy of the exported rows while the database still holds them
       too. A test throws SimulatedKillException here to stand in for a process kill. */
    internal static Action? BeforeDatabaseFileResetForTests { get; set; }

    /* Test code runs here outside any lock, between the copy of the preserved tables and the reset, as a concurrent
       writer would. */
    internal static Func<Task>? BetweenPreserveCopyAndResetForTests { get; set; }
    internal long CompactionBatchInputBytes { get; set; } = ParquetCompaction.DefaultBatchInputBytes;

    /* Stand in for a process kill at the two points of the periodic export where one matters (#4720): the first
       fires after the table's journal is written and before the file is promoted, the second after the file is
       promoted and before its rows are deleted. A seam throws SimulatedKillException to abort the whole run the
       way a kill does: the per-table catch below lets it through, so nothing after the seam runs. */
    internal Action<string>? BeforePromoteForTests { get; set; }
    internal Action<string>? AfterPromoteForTests { get; set; }

    /* Fires with the table name after a compaction group's files are swapped in and before the archive views are
       rebuilt (#4720): the moment a reader would find a glob that matches nothing. */
    internal Action<string>? AfterCompactionSwapForTests { get; set; }

    /* Fires after the swap journals an earlier run left behind are resolved and before the archive views are
       rebuilt (#4720): the same moment for a replay that the seam above marks for a swap. */
    internal Action? AfterCompactionReplayForTests { get; set; }

    /* Replaces the minute-resolution file-name prefix, so a test can put two runs in different "minutes"
       without waiting for the clock. */
    internal string? TimestampForTests { get; set; }

    internal sealed class SimulatedKillException : Exception
    {
    }

    /* After a size-triggered archive-and-reset fails, the next attempt waits this long. The size check runs
       every minute, and what fails an export (a full disk, memory pressure, a held file) rarely clears in one;
       retrying every minute would only log the same failure sixty times an hour. */
    internal TimeSpan ResetRetryBackoff { get; set; } = TimeSpan.FromMinutes(15);
    internal DateTime ResetRetryNotBeforeUtc { get; set; } = DateTime.MinValue;

    /* Names the archive files a size-triggered reset promoted before it reached the database reset. If the
       process dies between the two, the next archival run removes them: the database still holds every row
       they contain, and leaving them would count the whole hot window twice (and again on each retry). */
    private const string ResetMarkerFileName = PreservedTableRestore.ResetExportMarkerFileName;

    /* Compaction replaces a month's existing file (or part files) with freshly merged ones. Those existing
       files are inputs of the merge, so they are renamed with this suffix while the new files move in, and
       deleted only once every new file is in place. The suffix keeps them out of every *.parquet scan and glob. */
    private const string ReplacedSuffix = ".replaced";

    /* One per month/table being swapped: written after every batch is merged and before any file is renamed,
       removed when the swap is complete. Found at the start of a later run, it means the previous run did
       not finish, and the run finishes or undoes that swap before merging anything. */
    private const string SwapJournalSuffix = ".swap";

    /* One per table, written once a periodic export's .tmp is complete and before it is promoted, removed after the
       rows it archived are deleted (#4720). It holds the cutoff (UTC, round-trip format) and the final file name.
       Found at the start of a later run it means the previous run died somewhere in between: with the file at its
       final name the promote happened, so the run only has to finish the DELETE; without it nothing was promoted,
       and the rows are exported afresh. Without it a kill between the promote and the DELETE exported the same
       rows again on the next run, and the archive held them twice for good. */
    private const string PendingArchiveSuffix = ".archive-pending";

    /* Configuration and the alert/collector STATE that must survive ArchiveAllAndResetAsync (the size
       threshold trips a database reset that deletes monitor.duckdb). Mute rules (#938) were the first. The
       state tables follow #1145: the alert engine seeds its gates from them at the first sweep after the
       next restart, so empty tables re-fire deadlock/blocking alerts, re-post webhooks and repeat
       failed-job toasts for events still inside the lookback window. Every table here is keyed by
       server_id (or global) plus names, never by an id into a table the reset empties, so restoring the
       rows verbatim into the fresh schema is correct. The reset holds the write lock while it restores.
         config_mute_rules / dismissed_archive_alerts : user choices, not time-series.
         config_edge_trigger_watermarks : blocking/deadlock gate watermarks and the failed-job alert's
                                          watermark_time (same table); archived events still exist.
         config_incident_occurrences    : running incident totals; a lost row restarts the count.
         config_alert_persistence_state : breach/clear streaks and the firing flag of the built-in gate.
         config_database_state_expected : expected state plus last-alerted state, the edge-trigger memory.
         collector_state                : progress that cannot be derived from collected rows (XE gate counts,
                                          default-trace file, Query Store backfill done:/hole: markers).
         analysis_muted                 : user choice (#4887), PK mute_id, one row per mute the user made; ids are
                                          max+1 in C#, safe because every row is restored.
         server_tags / server_tag_map   : user choice (#4887), PK id / (server_id, tag_id), one row per tag and
                                          per tagging; bounded by what the user created.
       The restore is BY NAME: ALTER-added columns (watermark_time, v31) sit last on old stores while the fresh
       schema may order them differently, and a positional restore would swap values or fail. */
    internal static readonly string[] PreservedConfigTables =
    [
        "config_mute_rules",
        "dismissed_archive_alerts",
        "config_edge_trigger_watermarks",
        "config_incident_occurrences",
        "config_alert_persistence_state",
        "config_database_state_expected",
        "collector_state",
        "analysis_muted",
        "server_tags",
        "server_tag_map"
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
    /// Use hotDataDays for scheduled archival (default <see cref="HotDataDays"/>), or hotDataHours
    /// for size-triggered archival when the database is under space pressure.
    /// </summary>
    public async Task ArchiveOldDataAsync(int hotDataDays = HotDataDays, int? hotDataHours = null)
    {
        if (!await s_archiveLock.WaitAsync(TimeSpan.Zero))
        {
            _logger?.LogDebug("Archive operation already in progress, skipping");
            return;
        }

        IsArchiving = true;
        try
        {
        await RemoveUnfinishedResetExportsAndRefreshViewsAsync();
        await RecoverInterruptedArchiveWorkAsync();

        var cutoffDate = hotDataHours.HasValue
            ? DateTime.UtcNow.AddHours(-hotDataHours.Value)
            : DateTime.UtcNow.AddDays(-hotDataDays);
        var timestamp = TimestampForTests ?? DateTime.UtcNow.ToString("yyyyMMdd_HHmm");

        _logger?.LogInformation("Archiving data older than {CutoffDate} to Parquet (prefix: {Timestamp})", cutoffDate, timestamp);

        /* Archive each table independently. Export-to-Parquet (COPY ... TO)
           only READS the database, so it runs under a read lock — concurrently
           with the UI. Only the DELETE modifies the file, and the promote of
           the exported file has to share its lock (#4824: a view's glob sees a
           promoted file at once), so just the promote and the DELETE take the
           exclusive write lock, together, and only briefly. This keeps the UI
           responsive during archival instead of freezing it for the whole
           export (issue #979).

           Exporting and promoting in separate lock scopes is safe here: the
           DELETE only removes rows older than cutoffDate, and collectors only
           ever insert rows timestamped "now", so nothing archivable can be
           written into the gap between the export and the DELETE. */
        foreach (var (table, timeColumn) in ArchivableTables)
        {
            try
            {
                /* A journal still here after the recovery above is one it could not finish. Exporting the table
                   anyway would write over it, and with it the record of the rows an earlier file already holds. */
                if (File.Exists(PendingArchivePath(table)))
                {
                    _logger?.LogWarning("Skipping {Table}: the journal of an earlier interrupted export is still there and could not be finished", table);
                    continue;
                }

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

                    /* DuckDB keeps the partial file when a COPY fails partway through its query, and the move
                       below never runs then, so nothing else would remove it: the hourly retry would add
                       another partial file each time. */
                    try
                    {
                        await ExportToParquet(readConnection, table, timeColumn, cutoffDate, tempParquetPath);
                    }
                    catch
                    {
                        try { File.Delete(tempParquetPath); } catch { /* best effort */ }
                        throw;
                    }
                }

                /* The promote and the DELETE share one write lock (#4824). A view is the table UNION ALL its
                   archive glob, so a promoted file is in every read at once: with the promote outside the lock
                   and the DELETE under a lock of its own, a reader in between counted the exported rows in the
                   table and in the file. What undoes a failed step (the temp, the promoted file, the journal)
                   runs inside the lock too, so a reader never finds a file the table still covers.

                   Promote the temp only after the COPY has fully succeeded. The name carries this cycle's
                   timestamp, so nothing is at the final name. A move that fails after its retries leaves the
                   rows in the table (the DELETE below never runs); the temp is removed so it cannot pile up.
                   The journal goes down first (#4720): a process that dies between the promote and the DELETE
                   below leaves the rows in the table AND in the promoted file, and the next run reads the
                   journal to delete them instead of exporting them a second time.

                   The DELETE modifies table data and the next CHECKPOINT reorganizes the file, so readers must
                   not be mid-query when that happens or they get "Reached the end of the file" errors; the move
                   and the DELETE are both fast, so the UI stall is short. */
                using (_duckDb.AcquireWriteLock())
                {
                    try
                    {
                        WritePendingArchive(table, cutoffDate, Path.GetFileName(parquetPath));
                        BeforePromoteForTests?.Invoke(table);
                        MoveWithRetry(tempParquetPath, parquetPath);
                    }
                    catch (Exception ex) when (ex is not SimulatedKillException)
                    {
                        try { File.Delete(tempParquetPath); } catch { /* best effort */ }
                        TryDeletePendingArchive(table);
                        throw;
                    }

                    AfterPromoteForTests?.Invoke(table);

                    try
                    {
                        /* Core, not DeleteArchivedRowsAsync: this thread holds the write lock, and the lock does not nest. */
                        await DeleteArchivedRowsCoreAsync(table, timeColumn, cutoffDate);
                    }
                    catch
                    {
                        /* The rows are still in the table (DELETE failed), so they aren't lost —
                           discard the archive file we just wrote so the same rows aren't counted in
                           both the table and the parquet (double-counted by v_* views and re-exported
                           next cycle). The journal goes with the file; if the file cannot be removed the
                           journal stays, and the next run finishes the DELETE against it. */
                        try
                        {
                            File.Delete(parquetPath);
                            TryDeletePendingArchive(table);
                        }
                        catch { /* best effort */ }
                        throw;
                    }
                }

                TryDeletePendingArchive(table);
                _logger?.LogInformation("Archived {Count} rows from {Table} to {Path}", rowCount, table, parquetPath);
            }
            catch (Exception ex) when (ex is not SimulatedKillException)
            {
                _logger?.LogError(ex, "Failed to archive table {Table}", table);
            }
        }

        /* Compact per-cycle files into monthly parquet before refreshing views. The refresh runs even when
           compaction throws (#4720): a compaction that got part way may already have swapped some months, and
           a view whose glob matches nothing fails every read of that table until the next hourly run. */
        try
        {
            CompactParquetFiles();
        }
        finally
        {
            /* Refresh archive views outside write lock — view creation is fast and safe */
            await _duckDb.CreateArchiveViewsAsync();
        }
        }
        finally
        {
            IsArchiving = false;
            s_archiveLock.Release();
        }
    }

    /* The DELETE of a periodic export, under the write lock: it modifies table data and the next CHECKPOINT
       reorganizes the file, so readers must not be mid-query when that happens. Returns the rows deleted.
       The export itself calls the Core form below, with its promote under the same lock (#4824); this one is for
       a caller with no promote to keep it with, the recovery of an interrupted export. */
    private async Task<int> DeleteArchivedRowsAsync(string table, string timeColumn, DateTime cutoff)
    {
        using (_duckDb.AcquireWriteLock())
        {
            return await DeleteArchivedRowsCoreAsync(table, timeColumn, cutoff);
        }
    }

    /* The body of DeleteArchivedRowsAsync, for a caller that already holds the write lock (#4824). A view reads
       a promoted parquet file through its glob at once, so the export takes the lock before it promotes its file
       and keeps it through this DELETE: released in between, a reader would count the rows in the table and in
       the file. The lock does not nest, so this takes none of its own. */
    private async Task<int> DeleteArchivedRowsCoreAsync(string table, string timeColumn, DateTime cutoff)
    {
        using var writeConnection = _duckDb.CreateConnection();
        await writeConnection.OpenAsync();

        using var deleteCmd = writeConnection.CreateCommand();
        deleteCmd.CommandText = $"DELETE FROM {table} WHERE {timeColumn} < $1";
        deleteCmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        var deleted = await deleteCmd.ExecuteNonQueryAsync();

        /* A live table just lost rows, so cached archive answers are invalid until re-read. Only when rows went:
           a no-op DELETE changes nothing a cached answer depends on. Still inside the caller's write lock. */
        if (deleted > 0)
            _duckDb.BumpArchiveViewGeneration();

        return deleted;
    }

    private string PendingArchivePath(string table) => Path.Combine(_archivePath, table + PendingArchiveSuffix);

    /* Written whole beside the journal and then renamed into place, so a reader never sees a partial journal. */
    private void WritePendingArchive(string table, DateTime cutoff, string parquetFileName)
    {
        var journalPath = PendingArchivePath(table);
        var tempPath = journalPath + ".tmp";
        File.WriteAllLines(tempPath,
        [
            $"cutoff|{cutoff.ToString("O", CultureInfo.InvariantCulture)}",
            $"file|{parquetFileName}"
        ]);
        File.Move(tempPath, journalPath, overwrite: true);
    }

    private void TryDeletePendingArchive(string table)
    {
        try
        {
            File.Delete(PendingArchivePath(table));
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Could not remove the archive journal for {Table}; the next run finishes it", table);
        }
    }

    /* Null for a journal without a readable cutoff and file name (a truncated or foreign file). */
    private static (DateTime Cutoff, string FileName)? ReadPendingArchive(string journalPath)
    {
        DateTime? cutoff = null;
        string? fileName = null;
        foreach (var line in File.ReadAllLines(journalPath))
        {
            var parts = line.Split('|');
            if (parts.Length != 2)
            {
                continue;
            }

            if (parts[0] == "cutoff" && DateTime.TryParse(parts[1], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed))
            {
                cutoff = parsed;
            }
            else if (parts[0] == "file")
            {
                /* A name only: a journal must not point outside the archive folder. */
                fileName = Path.GetFileName(parts[1]);
            }
        }

        return cutoff is null || string.IsNullOrEmpty(fileName) ? null : (cutoff.Value, fileName);
    }

    /// <summary>
    /// Finishes or discards what an earlier run left half done, before this run exports, compacts or deletes
    /// anything. Called once at the start of every entry point that does, after the unfinished-reset-export
    /// cleanup (#4720).
    /// </summary>
    private async Task RecoverInterruptedArchiveWorkAsync()
    {
        if (!Directory.Exists(_archivePath))
        {
            return;
        }

        await RecoverPendingArchivesAsync();

        /* After the swap journals are replayed: a journal the replay could not finish still names temps it needs. */
        ReplayCompactionSwapJournals();
        RemoveStaleTempFiles();
    }

    /// <summary>
    /// Deletes the <c>.tmp</c> files left in the archive folder by a process killed inside a COPY (a periodic or
    /// reset export, a compaction merge) or between writing a journal and renaming it. Nothing else removes them
    /// (#4720), and they sit on a disk that is already under size pressure. A <c>.tmp</c> that a swap journal
    /// still names is kept: an undo that could not finish needs it, and it may hold rows that exist nowhere else.
    /// Runs under the process-wide archive lock, so no export or merge of this process has a <c>.tmp</c> open.
    /// </summary>
    private void RemoveStaleTempFiles()
    {
        var named = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        try
        {
            foreach (var journalPath in Directory.GetFiles(_archivePath, "*" + SwapJournalSuffix))
            {
                var swap = ReadSwapJournal(journalPath.Replace("\\", "/"));
                if (swap is null)
                {
                    continue;
                }

                foreach (var (_, tempPath, _) in swap.Outputs)
                {
                    named.Add(Path.GetFileName(tempPath));
                }
            }
        }
        catch (Exception ex)
        {
            /* A journal that cannot be read may name any temp, so none is deleted until it can. */
            _logger?.LogWarning(ex, "Could not read a compaction swap journal; the leftover .tmp files stay until the next run");
            return;
        }

        var removed = 0;
        foreach (var path in Directory.GetFiles(_archivePath, "*.tmp"))
        {
            var name = Path.GetFileName(path);

            /* The three-letter pattern also matches longer extensions that start with it on Windows. */
            if (!name.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase) || named.Contains(name))
            {
                continue;
            }

            try
            {
                File.Delete(path);
                removed++;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                _logger?.LogWarning("Could not delete the leftover {File}; it is removed by a later run: {Message}", name, ex.Message);
            }
        }

        if (removed > 0)
        {
            _logger?.LogInformation("Removed {Count} partial .tmp file(s) that an interrupted run left in the archive folder", removed);
        }
    }

    /// <summary>
    /// One journal per table whose periodic export did not finish. If its parquet file exists the promote
    /// happened, so the rows are in the file and this run deletes them from the table (a second DELETE of the same
    /// rows is harmless). If it does not, the promote never happened and the rows are still only in the table.
    /// Either way the journal goes, so the export below neither repeats an archived table nor loses one.
    /// </summary>
    private async Task RecoverPendingArchivesAsync()
    {
        foreach (var journalPath in Directory.GetFiles(_archivePath, "*" + PendingArchiveSuffix).Order(StringComparer.Ordinal))
        {
            var journalName = Path.GetFileName(journalPath);
            var table = journalName[..^PendingArchiveSuffix.Length];
            try
            {
                var timeColumn = ArchivableTables.FirstOrDefault(t => t.Table == table).TimeColumn;
                var pending = ReadPendingArchive(journalPath);
                if (timeColumn is null || pending is null)
                {
                    _logger?.LogWarning("Removing {Journal}: it does not name an archivable table and a cutoff", journalName);
                    File.Delete(journalPath);
                    continue;
                }

                var (cutoff, fileName) = pending.Value;
                if (File.Exists(Path.Combine(_archivePath, fileName)))
                {
                    var deleted = await DeleteArchivedRowsAsync(table, timeColumn, cutoff);
                    File.Delete(journalPath);
                    _logger?.LogInformation(
                        "Recovered the interrupted archive of {Table}: {File} was already written, so {Count} row(s) older than {Cutoff:o} were deleted from the table",
                        table, fileName, deleted, cutoff);
                }
                else
                {
                    File.Delete(journalPath);
                    _logger?.LogInformation(
                        "Recovered the interrupted archive of {Table}: {File} was never written, so nothing was deleted (cutoff {Cutoff:o}, 0 rows)",
                        table, fileName, cutoff);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Could not recover the interrupted archive journal {Journal}; {Table} is skipped until it can be", journalName, table);
            }
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
) TO '{EscapeSqlPath(filePath)}' ({ParquetCompaction.ArchiveCopyOptions})";
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
    /// Finishes or undoes every compaction swap an earlier run left behind. Returns the input files that swaps
    /// folded into their outputs but could not delete, and the groups whose swap could not be resolved. With
    /// any journal to resolve, the resolving and the archive-view rebuild that follows it run under one write
    /// lock; with none, no lock is taken (#4720).
    /// </summary>
    private (HashSet<string> AlreadyFolded, HashSet<(string Month, string Table)> UnresolvedGroups) ReplayCompactionSwapJournals()
    {
        /* Finish or undo any swap a previous run left behind (a crash, a kill, or a file it could not delete)
           before grouping, so this run starts from a consistent set of files. Inputs that an earlier swap
           folded into its outputs but could not delete are already counted in those outputs: they stay out
           of this run's merge, and the groups whose swap could not be resolved stay untouched. */
        var alreadyFolded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var unresolvedGroups = new HashSet<(string Month, string Table)>();

        /* Almost every run finds no journal: return before taking a lock the run does not need. */
        var journalPaths = Directory.GetFiles(_archivePath, "*" + SwapJournalSuffix);
        if (journalPaths.Length == 0)
        {
            return (alreadyFolded, unresolvedGroups);
        }

        /* Finishing a swap deletes the files the views read and undoing one moves them, so a reader in between
           finds the same missing rows or empty glob that the per-group swap in CompactParquetFiles explains
           (#4720). The lock is held to the end of the method, past the rebuild below, so no reader gets in
           between the two. */
        using var writeLock = _duckDb.AcquireWriteLock();
        foreach (var journalPath in journalPaths)
        {
            var journalName = Path.GetFileName(journalPath);
            try
            {
                var swap = ReadSwapJournal(journalPath.Replace("\\", "/"));
                if (swap is null)
                {
                    File.Delete(journalPath);
                    continue;
                }

                _logger?.LogWarning("Resolving the compaction swap {Journal} that an earlier run did not finish", journalName);
                foreach (var leftover in ResolveCompactionSwap(swap))
                {
                    alreadyFolded.Add(Path.GetFileName(leftover));
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Could not resolve the compaction swap {Journal}; its month is left as it is until the next run", journalName);
            }

            /* A journal still present, resolved or not, keeps its month out of this run's merge: a new swap
               for the month would write over the journal and forget the files it still has to delete. */
            if (File.Exists(journalPath))
            {
                var m = Regex.Match(journalName, @"^(\d{6})_(.+)" + Regex.Escape(SwapJournalSuffix) + "$");
                if (m.Success)
                {
                    unresolvedGroups.Add((m.Groups[1].Value, m.Groups[2].Value));
                }
            }
        }

        AfterCompactionReplayForTests?.Invoke();

        /* Core, not CreateArchiveViewsAsync: this thread holds the write lock and the lock does not nest.
           A failed rebuild is logged, not thrown: the caller still needs the sets built above, and the run
           rebuilds the views again before it ends. */
        try
        {
            _duckDb.CreateArchiveViewsCoreAsync().GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Could not rebuild the archive views after resolving the compaction swaps; the rebuild at the end of the run covers them");
        }

        return (alreadyFolded, unresolvedGroups);
    }

    /// <summary>
    /// Compacts all per-cycle parquet files into monthly files (YYYYMM_tablename.parquet).
    /// This keeps the archive directory small (~75 files for 3 months of 25 tables)
    /// and dramatically improves DuckDB read_parquet glob performance.
    /// </summary>
    internal void CompactParquetFiles()
    {
        if (!Directory.Exists(_archivePath))
        {
            return;
        }

        var (alreadyFolded, unresolvedGroups) = ReplayCompactionSwapJournals();

        var allFiles = Directory.GetFiles(_archivePath, "*.parquet")
            .Select(f => Path.GetFileName(f))
            .Where(f => !alreadyFolded.Contains(f))
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

            /* all_tablename (manual consolidation from earlier). Folded into the current month's group, so
               the month's existing file is an input of the same merge rather than a file another group's
               output would silently replace. */
            if (month == null)
            {
                m = Regex.Match(name, @"^all_(.+)$");
                if (m.Success)
                {
                    month = DateTime.UtcNow.ToString("yyyyMM");
                    table = m.Groups[1].Value;
                }
            }

            /* imported_YYYYMM_tablename, or imported_YYYYMM_tablename_ptNNN (imported from a previous
               install). The optional part suffix is matched here and dropped, otherwise it would stay in the
               table name: the group would be "table_ptNNN", its output named after this install's own part
               file of that month, and the local part file replaced by the imported one. It also kept an
               imported query_snapshots part out of the compaction skip below. */
            if (month == null)
            {
                m = Regex.Match(name, @"^imported_(\d{6})_(.+?)(_pt\d{3})?$");
                if (m.Success)
                {
                    month = m.Groups[1].Value;
                    table = m.Groups[2].Value;
                }
            }

            /* imported_YYYYMMDD_HHMM_tablename (imported per-cycle files) */
            if (month == null)
            {
                m = Regex.Match(name, @"^imported_(\d{8})_\d{4}_(.+?)(_pt\d{3})?$");
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
        var viewRebuildFailures = 0;

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

            /* If every file in the group is already in final monthly/part format (YYYYMM_table or
               YYYYMM_table_ptNNN, with or without the imported_ prefix), there are no new per-cycle files
               to fold in, so skip. Otherwise a month that legitimately split into N part files (input over
               the per-batch budget) gets re-read and re-written on every archival cycle. */
            if (files.All(f => Regex.IsMatch(Path.GetFileNameWithoutExtension(f), @"^(imported_)?\d{6}_.+?(_pt\d{3})?$")))
            {
                continue;
            }

            if (unresolvedGroups.Contains((month, table)))
            {
                continue;
            }

            var batchOutputs = new List<(string TempPath, string FinalPath)>();
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
                var batches = ParquetCompaction.BuildSizeBudgetedBatches(sorted, CompactionBatchInputBytes);

                /* Plan the output names. With one batch we keep the existing YYYYMM_table.parquet name
                   (backward compatible). With multiple batches we emit YYYYMM_table_ptNNN.parquet; the
                   archive views glob both shapes, so readers see them all. */
                for (var i = 0; i < batches.Count; i++)
                {
                    var finalName = batches.Count == 1
                        ? $"{month}_{table}.parquet"
                        : $"{month}_{table}_pt{i + 1:D3}.parquet";
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

                OnCompactionTempsReadyForTests?.Invoke(batchOutputs.Select(o => o.TempPath).ToList());

                /* Every batch is merged. The month's existing file (or its part files) is an input of this
                   merge, and its rows now exist only in the temps as well, so nothing is deleted until every
                   temp is in place: SwapCompactionOutputs renames the files the outputs replace aside, moves
                   the temps in, and only then removes the inputs. A move that fails after its retries (a
                   scanner, backup agent or indexer holding the fresh file) undoes the swap, and the next
                   cycle starts from exactly the files this one found.

                   The swap and the view rebuild run together under the write lock (#4720). A view keeps the
                   globs it was built with, so a reader that got in between the two found this group's rows
                   missing (the view had no glob for the new part files) or, once the last per-cycle file was
                   gone, a glob that matched nothing, which fails the whole read at bind. The lock is taken
                   here, per group, after the merge: the merge is the slow part and readers are never held
                   behind it, and one lock across every group would keep every group's merged output on disk
                   at once. Core, not CreateArchiveViewsAsync: this thread already holds the write lock and
                   the lock does not nest. Blocking on it is safe because DuckDB.NET's async calls complete
                   synchronously, so the thread that took the lock is the one that releases it. */
                using (_duckDb.AcquireWriteLock())
                {
                    var removed = SwapCompactionOutputs(month, table, sourcePaths, batchOutputs);

                    /* The group is compacted from here on: the merged files are in place and the inputs are gone.
                       It counts now, so a rebuild that throws below is not reported as a compaction that failed. */
                    totalMerged++;
                    totalRemoved += removed;

                    AfterCompactionSwapForTests?.Invoke(table);

                    /* A failed rebuild is logged for what it is and does not reach the group's catch below, which
                       is for a merge or a swap that failed. Both callers rebuild the views again in a finally when
                       compaction ends, so the views are not left behind for good. */
                    try
                    {
                        _duckDb.CreateArchiveViewsCoreAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception rebuildEx)
                    {
                        viewRebuildFailures++;
                        _logger?.LogError(rebuildEx, "Compacted {Month}/{Table} ({Count} files), but the archive views could not be rebuilt; they are rebuilt again when compaction ends", month, table, files.Count);
                    }
                }

                if (batches.Count == 1)
                {
                    _logger?.LogDebug("Compacted {Count} files into {Target}", files.Count, batchOutputs[0].FinalPath);
                }
                else
                {
                    _logger?.LogInformation("Compacted {Count} files into {Parts} part files for {Month}/{Table} (input too large for single batch)",
                        files.Count, batches.Count, month, table);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to compact {Month}/{Table} ({Count} files)", month, table, files.Count);

                /* Best-effort cleanup of this group's temps. */
                foreach (var (tempPath, _) in batchOutputs)
                {
                    try { File.Delete(tempPath); } catch { /* best effort */ }
                }
            }
        }

        if (totalMerged > 0)
        {
            var remaining = Directory.GetFiles(_archivePath, "*.parquet").Length;
            if (viewRebuildFailures > 0)
            {
                _logger?.LogInformation("Parquet compaction complete: merged {Groups} groups, removed {Removed} files, {Remaining} files remaining, view rebuilds failed: {Failures}",
                    totalMerged, totalRemoved, remaining, viewRebuildFailures);
            }
            else
            {
                _logger?.LogInformation("Parquet compaction complete: merged {Groups} groups, removed {Removed} files, {Remaining} files remaining",
                    totalMerged, totalRemoved, remaining);
            }
        }
    }

    /* One month/table swap: the merged outputs about to replace the group's inputs. An output is "replacing"
       when a file already exists at its final name (the month's existing file or part file, itself one of the
       inputs); a "fresh" output has nothing at its name. Inputs are the group's files that are not output
       names. Everything is a full path. */
    private sealed class CompactionSwap
    {
        public required string JournalPath { get; init; }
        public bool Swapped { get; set; }
        public List<(string FinalPath, string TempPath, bool Replacing)> Outputs { get; } = [];
        public List<string> Inputs { get; } = [];
    }

    /// <summary>
    /// Moves a group's merged temps to their final names and removes the inputs, without ever having a
    /// moment where a month's rows are on disk only in a file that a failure would delete. Returns the number
    /// of input files removed.
    /// </summary>
    private int SwapCompactionOutputs(
        string month, string table, IReadOnlyList<string> sourcePaths, IReadOnlyList<(string TempPath, string FinalPath)> batchOutputs)
    {
        var outputNames = new HashSet<string>(batchOutputs.Select(o => o.FinalPath), StringComparer.OrdinalIgnoreCase);
        var swap = new CompactionSwap
        {
            JournalPath = Path.Combine(_archivePath, $"{month}_{table}{SwapJournalSuffix}").Replace("\\", "/")
        };
        foreach (var (tempPath, finalPath) in batchOutputs)
        {
            swap.Outputs.Add((finalPath, tempPath, File.Exists(finalPath)));
        }
        swap.Inputs.AddRange(sourcePaths.Where(p => !outputNames.Contains(p)));

        /* The journal goes down before the first rename, so a process that dies anywhere below leaves a
           record the next run can finish or undo. */
        WriteSwapJournal(swap);

        try
        {
            /* Every file an output replaces is set aside first, then every temp moves in. Both are renames
               within the archive folder: nothing is copied and nothing is deleted. */
            foreach (var (finalPath, _, replacing) in swap.Outputs)
            {
                if (replacing)
                {
                    MoveWithRetry(finalPath, finalPath + ReplacedSuffix);
                }
            }

            foreach (var (finalPath, tempPath, _) in swap.Outputs)
            {
                MoveWithRetry(tempPath, finalPath);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Could not move the merged files for {Month}/{Table} into place; restoring the files this cycle started with", month, table);
            ResolveCompactionSwap(swap);
            throw;
        }

        var leftovers = ResolveCompactionSwap(swap);
        return swap.Inputs.Count - leftovers.Count;
    }

    /// <summary>
    /// Finishes a swap whose outputs are all in place (removes the inputs it replaced and the files it set
    /// aside), or undoes one that is not (puts the set-aside files back and removes any output already
    /// promoted). Returns the inputs a finished swap could not delete: their rows are already in the outputs,
    /// so the caller keeps them out of the next merge. Throws when an undo cannot complete; the journal then
    /// stays for the next run.
    /// </summary>
    private List<string> ResolveCompactionSwap(CompactionSwap swap)
    {
        var promotedAll = swap.Outputs.All(o =>
            File.Exists(o.FinalPath) && (!o.Replacing || File.Exists(o.FinalPath + ReplacedSuffix)));

        if (!swap.Swapped && !promotedAll)
        {
            foreach (var (finalPath, tempPath, replacing) in swap.Outputs)
            {
                var asidePath = finalPath + ReplacedSuffix;
                if (replacing)
                {
                    /* With the aside present the file at the final name, if any, is the promoted output;
                       without it the old file was never moved and still sits at the final name. */
                    if (File.Exists(asidePath))
                    {
                        if (File.Exists(finalPath))
                        {
                            File.Delete(finalPath);
                        }
                        MoveWithRetry(asidePath, finalPath);
                    }
                }
                else if (File.Exists(finalPath))
                {
                    File.Delete(finalPath);
                }

                try { File.Delete(tempPath); } catch { /* best effort; the next merge overwrites it */ }
            }

            File.Delete(swap.JournalPath);
            return [];
        }

        /* Every output is in place, so every input's rows are in an output. Record that before deleting
           anything: a run that finds the journal in this state must only retry the deletes. */
        if (!swap.Swapped)
        {
            swap.Swapped = true;
            WriteSwapJournal(swap);
        }

        var leftovers = new List<string>();
        foreach (var input in swap.Inputs)
        {
            if (!File.Exists(input))
            {
                continue;
            }
            try
            {
                File.Delete(input);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                _logger?.LogWarning("Could not delete {File} during compaction; it is kept out of the next merge and deleted later: {Message}",
                    Path.GetFileName(input), ex.Message);
                leftovers.Add(input);
            }
        }

        var asidesLeft = 0;
        foreach (var (finalPath, _, replacing) in swap.Outputs)
        {
            var asidePath = finalPath + ReplacedSuffix;
            if (!replacing || !File.Exists(asidePath))
            {
                continue;
            }
            try
            {
                File.Delete(asidePath);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                _logger?.LogWarning("Could not delete {File} after compaction; it is deleted later: {Message}", Path.GetFileName(asidePath), ex.Message);
                asidesLeft++;
            }
        }

        if (leftovers.Count == 0 && asidesLeft == 0)
        {
            File.Delete(swap.JournalPath);
        }

        return leftovers;
    }

    private static void WriteSwapJournal(CompactionSwap swap)
    {
        var lines = new List<string> { swap.Swapped ? "state|swapped" : "state|swapping" };
        lines.AddRange(swap.Outputs.Select(o => $"output|{(o.Replacing ? "replacing" : "fresh")}|{Path.GetFileName(o.FinalPath)}"));
        lines.AddRange(swap.Inputs.Select(i => $"input|{Path.GetFileName(i)}"));

        /* Written whole then renamed over the previous version, so a reader never sees a partial journal. */
        var tempPath = swap.JournalPath + ".tmp";
        File.WriteAllLines(tempPath, lines);
        File.Move(tempPath, swap.JournalPath, overwrite: true);
    }

    /* Null for a journal that names no output (a truncated or foreign file); the caller removes it. */
    private CompactionSwap? ReadSwapJournal(string journalPath)
    {
        var swap = new CompactionSwap { JournalPath = journalPath };
        foreach (var line in File.ReadAllLines(journalPath))
        {
            var parts = line.Split('|');
            switch (parts[0])
            {
                case "state":
                    swap.Swapped = parts.Length > 1 && parts[1] == "swapped";
                    break;
                case "output" when parts.Length == 3:
                    var finalPath = Path.Combine(_archivePath, parts[2]).Replace("\\", "/");
                    swap.Outputs.Add((finalPath, finalPath + ".tmp", parts[1] == "replacing"));
                    break;
                case "input" when parts.Length == 2:
                    swap.Inputs.Add(Path.Combine(_archivePath, parts[1]).Replace("\\", "/"));
                    break;
            }
        }

        return swap.Outputs.Count == 0 ? null : swap;
    }

    /// <summary>
    /// <see cref="File.Move(string, string)"/> with a few short retries. A scanner, backup agent or indexer
    /// can hold a freshly written file for a moment, which fails the rename with a sharing violation; the
    /// hold is over within a second in practice, and a rename that fails for good still throws.
    /// </summary>
    private static void MoveWithRetry(string sourcePath, string destinationPath)
    {
        const int attempts = 5;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                File.Move(sourcePath, destinationPath);
                return;
            }
            catch (Exception ex) when (attempt < attempts && ex is IOException or UnauthorizedAccessException)
            {
                Thread.Sleep(100 * attempt);
            }
        }
    }

    private string ResetMarkerPath => Path.Combine(_archivePath, ResetMarkerFileName);
    private string RestoreMarkerPath => Path.Combine(_archivePath, PreservedTableRestore.RestoreMarkerFileName);
    private string RestoreMarkerWritingPath => Path.Combine(_archivePath, PreservedTableRestore.RestoreMarkerWritingFileName);

    /// <summary>
    /// Removes the archive files a size-triggered reset promoted without reaching its database reset (the
    /// process died in between). The database still holds every row they contain.
    /// </summary>
    private int RemoveUnfinishedResetExports()
    {
        if (!File.Exists(ResetMarkerPath))
        {
            return 0;
        }

        var removed = 0;
        foreach (var line in File.ReadAllLines(ResetMarkerPath))
        {
            var path = Path.Combine(_archivePath, line.Trim());
            if (line.Trim().Length == 0 || !File.Exists(path))
            {
                continue;
            }
            try
            {
                File.Delete(path);
                removed++;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Could not remove {File}, left by an archive-and-reset that did not finish; it duplicates rows still in the database", line);
            }
        }

        File.Delete(ResetMarkerPath);
        _logger?.LogWarning("An earlier archive-and-reset exported its files but never reset the database; removed {Count} archive file(s) that duplicated rows still in it", removed);
        return removed;
    }

    /// <summary>
    /// <see cref="RemoveUnfinishedResetExports"/>, then a rebuild of the archive views when it removed anything, under
    /// the write lock so no reader sees the gap. The views built at startup already hold a glob for those files, and
    /// with the last file behind a glob gone DuckDB fails every read of that table's view at bind. The hourly cycle
    /// only rebuilds at its end, and the size-triggered reset's failure branch never does.
    /// </summary>
    private async Task RemoveUnfinishedResetExportsAndRefreshViewsAsync()
    {
        if (!File.Exists(ResetMarkerPath))
        {
            return;
        }

        using (_duckDb.AcquireWriteLock())
        {
            var removed = RemoveUnfinishedResetExports();
            if (removed > 0)
            {
                /* Core, not CreateArchiveViewsAsync: this thread holds the write lock, and the lock does not nest. */
                await _duckDb.CreateArchiveViewsCoreAsync();
            }
        }
    }

    /// <summary>
    /// Discards everything a failed archive-and-reset attempt wrote: its temps, the files it promoted, its
    /// marker and its preserved config copies. The database was not touched, so nothing here is the only copy.
    /// </summary>
    private void DiscardResetAttempt(IEnumerable<(string TempPath, string FinalPath)> exports, IEnumerable<string> promoted, string preserveDir)
    {
        foreach (var (tempPath, _) in exports)
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* best effort */ }
        }
        foreach (var path in promoted)
        {
            try { if (File.Exists(path)) File.Delete(path); }
            catch (Exception ex) { _logger?.LogError(ex, "Could not remove {File} after a failed archive-and-reset; it duplicates rows still in the database", path); }
        }
        try { if (File.Exists(ResetMarkerPath)) File.Delete(ResetMarkerPath); } catch { /* best effort */ }
        /* The restore marker goes before the directory it names (C6), and its side file with it (C0). */
        try { if (File.Exists(RestoreMarkerPath)) File.Delete(RestoreMarkerPath); } catch { /* best effort */ }
        try { if (File.Exists(RestoreMarkerWritingPath)) File.Delete(RestoreMarkerWritingPath); } catch { /* best effort */ }
        try { if (Directory.Exists(preserveDir)) Directory.Delete(preserveDir, recursive: true); } catch { /* best effort */ }
    }

    /// <summary>
    /// Archives ALL data from every table to parquet, then deletes and reinitializes the database.
    /// Called when the database exceeds the size threshold. Data remains queryable through archive views.
    /// </summary>
    public async Task ArchiveAllAndResetAsync()
    {
        if (File.Exists(RestoreMarkerPath))
        {
            /* An earlier reset's restore is still pending: a second reset would empty the tables again and
               overwrite the copy. Back off so the size trigger does not log this on every pass. */
            ResetRetryNotBeforeUtc = DateTime.UtcNow + ResetRetryBackoff;
            _logger?.LogError(
                "Database reset deferred: an earlier reset's restore is pending ({Marker}); restart Lite to finish it before another reset",
                RestoreMarkerPath);
            return;
        }

        if (!await s_archiveLock.WaitAsync(TimeSpan.Zero))
        {
            _logger?.LogDebug("Archive operation already in progress, skipping");
            return;
        }

        /* #4732: a retry time more than one backoff ahead of the clock can only come from a wall clock that stepped
           backwards after the failed attempt stamped it (the stamp is now + ResetRetryBackoff), so it counts as due
           now; left raw it stopped every size-triggered reset until the clock caught up, while the database kept
           growing past the threshold. */
        var resetNow = DateTime.UtcNow;
        if (resetNow < CollectorCadence.ClampDue(ResetRetryNotBeforeUtc, resetNow, ResetRetryBackoff))
        {
            _logger?.LogDebug("Database reset skipped: the previous attempt failed and its retry is due at {RetryAt:u}", ResetRetryNotBeforeUtc);
            s_archiveLock.Release();
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
        /* The preserved copy sits under the archive folder, not in %TEMP%, so it survives whatever clears the
           temp folder and startup finds it next to the marker that names it. Archive scans read the folder's top
           level only (GetFiles(_archivePath, "*.parquet"); DuckDB's * does not cross a slash), so a
           subdirectory is never read as archive data. */
        var preserveDirName = $"{PreservedTableRestore.PreserveDirectoryPrefix}{Guid.NewGuid():N}";
        var preserveDir = Path.Combine(_archivePath, preserveDirName);
        var preservedFiles = new Dictionary<string, string>();
        var exports = new List<(string TempPath, string FinalPath)>();
        var promoted = new List<string>();
        var resetStarted = false;
        string? preId = null;
        try
        {
            await RemoveUnfinishedResetExportsAndRefreshViewsAsync();
            await RecoverInterruptedArchiveWorkAsync();

            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmm");

            _logger?.LogInformation("Archiving ALL data to Parquet (prefix: {Timestamp}) and resetting database", timestamp);

            /* Export everything under the write lock. Each table goes to a .tmp beside its final name, and
               nothing is promoted until every export and every config save has succeeded: a table whose
               export fails (out of memory, a full disk, an I/O error) still has its rows only in the
               database, so the reset below must not run, and a COPY the process died inside must not leave
               a truncated file that matches the archive glob. One such file fails the table's archive view
               at bind, which hides every archived month of that table. */
            var exportsSucceeded = true;
            using (_duckDb.AcquireWriteLock())
            {
                using var connection = _duckDb.CreateConnection();
                await connection.OpenAsync();

                foreach (var (table, _) in ArchivableTables)
                {
                    try
                    {
                        BeforeTableExportForTests?.Invoke(table);

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
                        var tempParquetPath = parquetPath + ".tmp";

                        /* Tracked BEFORE the COPY runs: DuckDB keeps the partial file when a COPY fails partway
                           through its query, and DiscardResetAttempt removes every tracked temp. Added after the
                           COPY, a table whose export threw left its partial file on disk on every attempt, which
                           the 15-minute backoff repeats up to 96 times a day. */
                        exports.Add((tempParquetPath, parquetPath));

                        await WithRaisedCopyMemoryLimit(connection, async () =>
                        {
                            using var exportCmd = connection.CreateCommand();
                            exportCmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{EscapeSqlPath(tempParquetPath)}' ({ParquetCompaction.ArchiveCopyOptions})";
                            await exportCmd.ExecuteNonQueryAsync();
                        });

                        _logger?.LogInformation("Archived {Count} rows from {Table}", rowCount, table);
                    }
                    catch (Exception ex)
                    {
                        exportsSucceeded = false;
                        _logger?.LogError(ex, "Failed to archive table {Table}; the database is not reset", table);
                        break;
                    }
                }
            }

            if (!exportsSucceeded)
            {
                DiscardResetAttempt(exports, promoted, preserveDir);
                ResetRetryNotBeforeUtc = DateTime.UtcNow + ResetRetryBackoff;
                _logger?.LogWarning("Database reset abandoned: an export failed, so every row stays in the database and no archive file was added. The next attempt is at {RetryAt:u}",
                    ResetRetryNotBeforeUtc);
                return;
            }

            /* Everything is on disk. Name the files about to be promoted before promoting them: if the
               process dies between here and the reset, the next archival run removes them, because the
               database still holds every row they contain. */
            File.WriteAllLines(ResetMarkerPath, exports.Select(e => Path.GetFileName(e.FinalPath)));

            if (BetweenPreserveCopyAndResetForTests is { } betweenLocks)
            {
                await betweenLocks().ConfigureAwait(false);
            }

            /* Promoting every export, clearing the tables and putting the preserved config rows back share one
               write lock (#4824). A view is the table UNION ALL its archive glob, so a promoted file is in every
               read at once: released between the promote and the reset, a reader would count every row in the
               table and in the files. Released between the reset and the restore, a reader would read the
               preserved tables, which the reset just emptied, with none of their rows. The reset is the Core form
               because the lock does not nest, and a failure before it removes the promoted files inside the lock
               too, so a reader never finds files the tables still cover. */
            var allRestoresSucceeded = true;
            using (_duckDb.AcquireWriteLock())
            {
                try
                {
                    foreach (var (tempPath, finalPath) in exports)
                    {
                        MoveWithRetry(tempPath, finalPath);
                        promoted.Add(finalPath);
                    }

                    BeforeDatabaseResetForTests?.Invoke();

                    /* Copy the preserved tables now, inside the lock the reset runs under, so a row written between
                       the export lock and this one is neither lost nor brought back after a delete. A throw here
                       is before resetStarted: the attempt is discarded and the database keeps every row (C0). */
                    Directory.CreateDirectory(preserveDir);
                    using (var copyConnection = _duckDb.CreateConnection())
                    {
                        await copyConnection.OpenAsync();
                        foreach (var table in PreservedConfigTables)
                        {
                            using var countCmd = copyConnection.CreateCommand();
                            countCmd.CommandText = $"SELECT COUNT(*) FROM {table}";
                            var rowCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
                            if (rowCount == 0) continue;

                            var preservePath = Path.Combine(preserveDir, $"{table}.parquet").Replace("\\", "/");
                            await WithRaisedCopyMemoryLimit(copyConnection, async () =>
                            {
                                using var exportCmd = copyConnection.CreateCommand();
                                exportCmd.CommandText = $"COPY (SELECT * FROM {table}) TO '{EscapeSqlPath(preservePath)}' ({ParquetCompaction.ArchiveCopyOptions})";
                                await exportCmd.ExecuteNonQueryAsync();
                            });
                            preservedFiles[table] = preservePath;

                            _logger?.LogInformation("Preserved {Count} rows from {Table} for restoration after reset", rowCount, table);
                        }

                        /* Flush the WAL into the database file, then read the file's identity. After the flush the
                           WAL holds nothing the file lacks, which is what lets the reset delete the WAL first. */
                        using (var checkpointCmd = copyConnection.CreateCommand())
                        {
                            checkpointCmd.CommandText = "CHECKPOINT";
                            await checkpointCmd.ExecuteNonQueryAsync();
                        }
                        using var identityCmd = copyConnection.CreateCommand();
                        identityCmd.CommandText = "SELECT id FROM store_identity LIMIT 1";
                        preId = await identityCmd.ExecuteScalarAsync() as string;
                    }

                    if (preId == null)
                    {
                        /* Before resetStarted: the attempt is discarded and the database keeps every row. */
                        throw new InvalidOperationException("The database has no store identity; the reset cannot be made recoverable.");
                    }

                    /* The restore marker names the directory and the tables. Until it exists no reset has started,
                       so the database holds every row and a crash leaves at most an orphan directory (C0). Written
                       before the export marker goes: both present means the reset never began, and startup drops
                       the restore without restoring (C1). */
                    PreservedTableRestore.WriteMarker(
                        _archivePath, preserveDirName, preservedFiles.Keys, preId, promoted.Select(p => Path.GetFileName(p)));

                    /* From here the archive files are the only copy, so the export marker goes first. Nuke and
                       reinitialize outside the using-connection scope so all handles are closed. A crash from here
                       to the end of the restore leaves the restore marker, and startup restores idempotently:
                       full database (C2), fresh or missing database (C3), part-restored tables (C4) and
                       fully-restored tables (C5) all converge, because every insert ignores conflicts. */
                    File.Delete(ResetMarkerPath);
                    resetStarted = true;
                    BeforeDatabaseFileResetForTests?.Invoke();
                    _logger?.LogInformation("Deleting and reinitializing database");
                    await _duckDb.ResetDatabaseCoreAsync();

                    /* Every live table just lost its rows, so cached archive answers are invalid until re-read. */
                    _duckDb.BumpArchiveViewGeneration();

                    AfterDatabaseResetForTests?.Invoke();

                    /* Restore preserved config rows into the freshly initialized tables. Still under the lock the
                       reset took, and not one of its own (#4824): the tables exist and are empty from the end of
                       the reset until their rows are back, and no reader may read them in between. resetStarted is
                       set, so a failure here is not undone: the archive files are the only copy of the rows now. */
                    if (preservedFiles.Count > 0)
                    {
                        using var connection = _duckDb.CreateConnection();
                        await connection.OpenAsync();
                        foreach (var (table, path) in preservedFiles)
                        {
                            try
                            {
                                BeforePreservedTableRestoreForTests?.Invoke(table);
                                await PreservedTableRestore.RestoreTableAsync(connection, table, path);
                                _logger?.LogInformation("Restored rows to {Table} after database reset", table);
                                AfterPreservedTableRestoredForTests?.Invoke(table);
                            }
                            catch (Exception ex) when (ex is not SimulatedKillException)
                            {
                                allRestoresSucceeded = false;
                                _logger?.LogError(ex, "Failed to restore {Table} from {Path}; the preserved copy is kept and the next start retries", table, path);
                            }
                        }
                    }

                    /* Success: the marker goes first, then the directory. A crash between the two leaves an orphan
                       directory with no marker, which startup sweeps (C6); the reverse order would leave a marker
                       naming a missing directory. On any failure both stay, and the next start retries. */
                    if (allRestoresSucceeded)
                    {
                        try
                        {
                            if (File.Exists(RestoreMarkerPath)) File.Delete(RestoreMarkerPath);
                            if (Directory.Exists(preserveDir))
                                Directory.Delete(preserveDir, recursive: true);
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogWarning(ex, "Could not clean up the preserved copy at {Dir}; the next start removes it", preserveDir);
                        }
                    }
                    else
                    {
                        _logger?.LogError(
                            "Preserved tables were not all restored. The restore marker {Marker} and the copy in {Dir} are kept; the next start retries the restore",
                            RestoreMarkerPath, preserveDir);
                    }
                }
                catch when (!resetStarted)
                {
                    /* The database still holds every row: undo the attempt before the lock is released. The handler
                       below repeats this for a failure from anywhere else in the method; every step here checks
                       that the file is there, so the second pass finds nothing left to remove. */
                    DiscardResetAttempt(exports, promoted, preserveDir);
                    throw;
                }
            }

            _logger?.LogInformation("Database reset complete — archive views now serve all historical data from Parquet");

            /* Compact per-cycle files into monthly parquet files, then refresh the views over the result.
               This runs after the reset rather than before it, so the files the marker above names still
               exist under those names until the reset is through. The merges run on an in-memory DuckDB
               connection and only touch files on disk, so they do not contend with the collectors now
               writing to the fresh database; each group's swap and view rebuild briefly hold the write
               lock (#4720). */
            _logger?.LogInformation("Compacting parquet files into monthly archives");
            try
            {
                try
                {
                    CompactParquetFiles();
                }
                finally
                {
                    /* Also when compaction throws (#4720): months it already swapped must be readable. */
                    await _duckDb.CreateArchiveViewsAsync();
                }
            }
            catch (Exception compactEx)
            {
                /* Compaction is best-effort (merging per-cycle parquet into monthly files); a failure
                   must not fail the reset. Logged so a stuck or oversized backlog is visible instead of
                   silently degrading. */
                _logger?.LogError(compactEx, "Parquet compaction failed after the database reset");
            }
        }
        catch (Exception ex) when (!resetStarted)
        {
            /* The database still holds every row. Remove what this attempt wrote so the rows are not counted
               twice, and try again after the backoff. */
            DiscardResetAttempt(exports, promoted, preserveDir);
            ResetRetryNotBeforeUtc = DateTime.UtcNow + ResetRetryBackoff;
            _logger?.LogError(ex, "Archive-all-and-reset failed before the database was reset; its archive files were removed and the next attempt is at {RetryAt:u}",
                ResetRetryNotBeforeUtc);
        }
        catch (Exception ex)
        {
            ResetRetryNotBeforeUtc = DateTime.UtcNow + ResetRetryBackoff;
            _logger?.LogError(ex, "Archive-all-and-reset failed after the reset began. The restore marker {Marker} and the copy in {Dir} are kept; the next start retries the restore",
                RestoreMarkerPath, preserveDir);
        }
        finally
        {
            IsArchiving = false;
            resetScope.Dispose();
            s_archiveLock.Release();
        }
    }

}
