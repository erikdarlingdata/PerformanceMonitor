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
using System.Text;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>The store's size against the point where Lite resets it.</summary>
/// <param name="FileMb">The size of <c>monitor.duckdb</c> on disk. The size reset tests it against twice the reset point, and against the reset point itself when the data was not read.</param>
/// <param name="WalMb">The size of the write-ahead log beside it, 0 when there is none.</param>
/// <param name="UsedMb">Data inside the file, from <c>pragma_database_size()</c>. This is the number the size reset tests against the reset point (#9238). Null when the store was busy.</param>
/// <param name="ResetAtMb">The size at which Lite archives all data and starts a fresh store.</param>
/// <param name="ResetSource">Where <paramref name="ResetAtMb"/> came from: <c>default</c> or <c>setting</c> (settings.json <c>hot_store_reset_mb</c>, #9238).</param>
public sealed record StoreSizeInfo(double FileMb, double WalMb, double? UsedMb, double ResetAtMb, string ResetSource = HotStoreResetSetting.SourceDefault)
{
    /// <summary>The data's share of the reset point, in percent. When the data was not read it is the file's share, as the size check does.</summary>
    public double DataPercentOfReset => ResetAtMb <= 0 ? 0 : (UsedMb ?? FileMb) / ResetAtMb * 100.0;

    /// <summary>The file length at which the store resets whatever the data in it: twice the reset point.</summary>
    public double FileResetAtMb => ResetAtMb * HotStoreResetLimit.FileCeilingFactor;
}

/// <summary>The archive folder: parquet files only, top level only, as the archive views read it.</summary>
/// <param name="OldestMonth">The first of the month of the oldest file name that carries a date. Null when no name does.</param>
/// <param name="NewestMonth">The same for the newest.</param>
/// <param name="UndatedFiles">Parquet files whose name carries no date. Retention never deletes these.</param>
public sealed record ArchiveFolderInfo(
    string Path, int FileCount, long TotalBytes, DateTime? OldestMonth, DateTime? NewestMonth, int RetentionMonths, int UndatedFiles);

/// <summary>One table in the store.</summary>
/// <param name="EstimatedRows">DuckDB's own row estimate for the table. It is not a COUNT(*): the read stays cheap on a large store.</param>
public sealed record StoreTableInfo(string Name, long EstimatedRows);

/// <summary>Everything the Store Health window and <c>get_store_health</c> show (#8788).</summary>
/// <param name="Tables">Largest first. Null when the store could not be read (it was busy), with the reason in <paramref name="StoreNote"/>.</param>
public sealed record StoreHealthSnapshot(
    DateTime ReadAtUtc,
    StoreSizeInfo Store,
    ArchiveFolderInfo Archives,
    IReadOnlyList<StoreHousekeepingEvent> History,
    IReadOnlyList<StoreTableInfo>? Tables,
    string? StoreNote,
    ArchiveLayoutInfo? Layout = null);

/// <summary>
/// Reads Lite's own store health (#8788): the data in <c>monitor.duckdb</c> against the reset point (512 MB unless hot_store_reset_mb says otherwise), the parquet archive
/// folder, the housekeeping history and the rows per table. It holds the store's read lock only for the two small catalog queries
/// and never the write lock, so collection and the UI go on. A read that cannot get the lock within <see cref="ReadLockBudget"/>
/// still answers with the file sizes, the archive folder and the history (none of which need the store), and says why the rest is
/// missing.
/// </summary>
public sealed class StoreHealthReader
{
    private readonly DuckDbInitializer _duckDb;
    private readonly string _archivePath;
    private readonly StoreHousekeepingHistory _history;

    /// <summary>The reset point the snapshot reports (#9238). It is the one the size check uses, <see cref="HotStoreResetLimit.Current"/>.</summary>
    internal HotStoreResetLimit HotStoreReset { get; set; } = HotStoreResetLimit.Current;

    public StoreHealthReader(DuckDbInitializer duckDb, StoreHousekeepingHistory history)
        : this(duckDb, duckDb.ArchivePath, history)
    {
    }

    internal StoreHealthReader(DuckDbInitializer duckDb, string archivePath, StoreHousekeepingHistory history)
    {
        _duckDb = duckDb;
        _archivePath = archivePath;
        _history = history;
    }

    /// <summary>How long a read waits for the store's read lock. A size reset holds the write lock while it exports every table,
    /// so a longer wait would only freeze the window for a number that is about to be replaced.</summary>
    internal TimeSpan ReadLockBudget { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long the archive layout read (#9421) may run before it reports what it covered and says it is partial.</summary>
    internal TimeSpan LayoutBudget { get; set; } = TimeSpan.FromSeconds(4);

    /* Tests only (#9421): called with a file's path just before the layout read opens it. */
    internal Action<string>? BeforeLayoutFileReadForTests { get; set; }

    /* Tests only (#9421): the merge batch budget the layout read judges a file against, null for the production budgets. */
    internal Func<string, long>? LayoutBatchBudgetForTests { get; set; }

    /// <summary>The store health without the archive layout. The Store Health window calls this on load and on every Refresh and never shows the
    /// layout, so it does not pay for the layout read, which can run for seconds (#9421). <see cref="StoreHealthSnapshot.Layout"/> is null.</summary>
    public StoreHealthSnapshot Read() => ReadSnapshot(includeLayout: false);

    /// <summary>The store health with the archive layout (#9421), for <c>get_store_health</c>, which prints it.</summary>
    public StoreHealthSnapshot ReadWithLayout() => ReadSnapshot(includeLayout: true);

    private StoreHealthSnapshot ReadSnapshot(bool includeLayout)
    {
        var fileMb = FileMegabytes(_duckDb.DatabasePath);
        var walMb = FileMegabytes(_duckDb.DatabasePath + ".wal");

        double? usedMb = null;
        List<StoreTableInfo>? tables = null;
        string? note = null;

        if (!File.Exists(_duckDb.DatabasePath))
        {
            note = "The store file does not exist yet.";
        }
        else
        {
            try
            {
                using var readLock = _duckDb.TryAcquireReadLock(ReadLockBudget);
                if (readLock is null)
                {
                    note = "The store was busy (an archive or a size reset is running). Table sizes are not shown. Refresh in a minute.";
                }
                else
                {
                    using var connection = _duckDb.CreateConnection();
                    connection.Open();
                    usedMb = ReadUsedMb(connection);
                    tables = ReadTables(connection);
                }
            }
            catch (Exception ex)
            {
                note = $"The store could not be read: {ex.Message}";
                AppLogger.Warn("StoreHealth", note);
            }
        }

        return new StoreHealthSnapshot(
            DateTime.UtcNow,
            new StoreSizeInfo(fileMb, walMb, usedMb, HotStoreReset.Mb, HotStoreReset.Source),
            ReadArchiveFolder(_archivePath),
            _history.Read(),
            tables,
            note,
            includeLayout ? ReadLayout() : null);
    }

    private ArchiveLayoutInfo? ReadLayout()
    {
        try
        {
            return new ArchiveLayoutReader(_archivePath, _duckDb.DatabasePath, _duckDb.AvailableFreeBytesProvider, LayoutBudget)
            {
                BeforeFileReadForTests = BeforeLayoutFileReadForTests,
                BatchBudgetForTests = LayoutBatchBudgetForTests
            }.Read();
        }
        catch (Exception ex)
        {
            AppLogger.Warn("StoreHealth", $"The archive layout could not be read: {ex.Message}");
            return null;
        }
    }

    /// <summary>The build history (#9425), newest first. It reads <c>build-history.json</c> beside the store and nothing else: no store lock, no archive.</summary>
    public IReadOnlyList<BuildStartEntry> ReadBuildHistory() => new BuildHistory(BuildHistory.PathFor(_duckDb.DatabasePath)).Read();

    private static double FileMegabytes(string path)
    {
        try
        {
            var info = new FileInfo(path);
            return info.Exists ? info.Length / (1024.0 * 1024.0) : 0;
        }
        catch (IOException)
        {
            return 0;
        }
    }

    private static double? ReadUsedMb(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT (used_blocks * block_size)::BIGINT FROM pragma_database_size()";
        var result = cmd.ExecuteScalar();
        return result is null or DBNull ? null : Convert.ToInt64(result, CultureInfo.InvariantCulture) / (1024.0 * 1024.0);
    }

    private static List<StoreTableInfo> ReadTables(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT table_name, estimated_size::BIGINT FROM duckdb_tables() WHERE NOT internal ORDER BY estimated_size DESC, table_name";
        using var reader = cmd.ExecuteReader();
        var tables = new List<StoreTableInfo>();
        while (reader.Read())
        {
            tables.Add(new StoreTableInfo(reader.GetString(0), reader.IsDBNull(1) ? 0 : reader.GetInt64(1)));
        }

        return tables;
    }

    /// <summary>
    /// The archive folder's files, counted as the archive views and the retention cleanup see them: <c>*.parquet</c> at the top level.
    /// The month range reads each file's date from its name with the one parser the cleanup uses
    /// (<see cref="RetentionService.TryParseArchiveFileDate"/>), so a file the cleanup would age out is a file the range counts.
    /// </summary>
    internal static ArchiveFolderInfo ReadArchiveFolder(string archivePath)
    {
        var count = 0;
        long bytes = 0;
        var undated = 0;
        DateTime? oldest = null;
        DateTime? newest = null;

        if (Directory.Exists(archivePath))
        {
            foreach (var file in Directory.EnumerateFiles(archivePath, "*.parquet"))
            {
                try
                {
                    bytes += new FileInfo(file).Length;
                    count++;
                }
                catch (IOException)
                {
                    /* Deleted between the listing and the stat: not in the folder any more. */
                    continue;
                }

                if (RetentionService.TryParseArchiveFileDate(file) is not { } date)
                {
                    undated++;
                    continue;
                }

                var month = new DateTime(date.Year, date.Month, 1, 0, 0, 0, DateTimeKind.Unspecified);
                if (oldest is null || month < oldest) oldest = month;
                if (newest is null || month > newest) newest = month;
            }
        }

        return new ArchiveFolderInfo(archivePath, count, bytes, oldest, newest, RetentionService.ArchiveRetentionMonths, undated);
    }
}

/// <summary>The plain-English sentences the window and <c>get_store_health</c> share, so the two cannot say different things.</summary>
public static class StoreHealthText
{
    public static string StoreSentence(StoreSizeInfo store) =>
        string.Create(CultureInfo.InvariantCulture,
            $"Lite archives all data and starts a fresh store when the data in the store reaches {store.ResetAtMb:0} MB ({ResetSourceText(store.ResetSource)}), or when the store file reaches {store.FileResetAtMb:0} MB, twice that.");

    /// <summary>Says where the reset point came from, in words the window and <c>get_store_health</c> share (#9238).</summary>
    public static string ResetSourceText(string source) =>
        source == HotStoreResetSetting.SourceSetting
            ? "set by hot_store_reset_mb in settings.json"
            : "the default, change it with hot_store_reset_mb in settings.json";

    public static string UndatedSentence(int files) =>
        files == 1
            ? "1 file has no date in its name and is never deleted by retention."
            : string.Create(CultureInfo.InvariantCulture, $"{files:N0} files have no date in their name and are never deleted by retention.");

    public static string ArchiveSentence(ArchiveFolderInfo archives) =>
        string.Create(CultureInfo.InvariantCulture,
            $"Archive files are deleted when their month is more than {archives.RetentionMonths} months old.");

    /// <summary>The window's header line: when the store was read, as a UTC clock time with the zone written, the zone the housekeeping history below it
    /// uses (#9603). A <see cref="DateTimeKind.Local"/> value is converted to UTC once. UTC and unspecified values are taken as the UTC instant, which is
    /// what <see cref="StoreHealthSnapshot.ReadAtUtc"/> holds, so none is shifted by the machine's zone.</summary>
    public static string ReadAt(DateTime readAtUtc)
    {
        var utc = readAtUtc.Kind == DateTimeKind.Local ? readAtUtc.ToUniversalTime() : readAtUtc;
        return string.Create(CultureInfo.InvariantCulture, $"Read at {utc:HH:mm:ss} UTC");
    }

    public static string Megabytes(double mb) => string.Create(CultureInfo.InvariantCulture, $"{mb:#,##0.0} MB");

    public static string Bytes(long bytes) => Megabytes(bytes / (1024.0 * 1024.0));

    public static string Month(DateTime? month) => month is { } m ? m.ToString("yyyy-MM", CultureInfo.InvariantCulture) : "none";

    public static string KindLabel(string kind) => kind switch
    {
        StoreHousekeepingHistory.KindArchive => "Timed archive",
        StoreHousekeepingHistory.KindSizeReset => "Size reset",
        StoreHousekeepingHistory.KindRetention => "Retention cleanup",
        StoreHousekeepingHistory.KindCompaction => "Compaction",
        _ => kind
    };

    public static string OutcomeLabel(string outcome) => outcome switch
    {
        StoreHousekeepingHistory.OutcomeOk => "OK",
        StoreHousekeepingHistory.OutcomeIncomplete => "Incomplete",
        StoreHousekeepingHistory.OutcomeFailed => "FAILED",
        _ => outcome
    };

    /// <summary>What a pass moved, in a few words: "12,345 rows in 9 files", "3 files deleted", or "nothing to do".</summary>
    public static string WorkSummary(StoreHousekeepingEvent e)
    {
        var rows = e.Rows ?? 0;
        var files = e.Files ?? 0;
        if (e.Kind == StoreHousekeepingHistory.KindCompaction && e.Compaction is { } c)
        {
            return CompactionSummary(c);
        }

        if (e.Kind == StoreHousekeepingHistory.KindRetention)
        {
            return files == 0 ? "no files expired" : string.Create(CultureInfo.InvariantCulture, $"{files:N0} files deleted");
        }

        return rows == 0 && files == 0
            ? "no rows moved"
            : string.Create(CultureInfo.InvariantCulture, $"{rows:N0} rows in {files:N0} files");
    }

    /// <summary>The Moved cell of the window's housekeeping history (#9604). It is <see cref="WorkSummary"/> except for a compaction row, which shows
    /// only the size part of its pass (<see cref="CompactionMoved"/>) so the cell stays short. The rest of the pass is in <see cref="MessageText"/>.</summary>
    public static string MovedText(StoreHousekeepingEvent e) =>
        e.Kind == StoreHousekeepingHistory.KindCompaction && e.Compaction is { } c ? CompactionMoved(c) : WorkSummary(e);

    /// <summary>The Message cell of the window's housekeeping history (#9604). A compaction row carries the rest of its pass here
    /// (<see cref="CompactionNotes"/>), then its own message after a full stop when it has one, the order <see cref="Render"/> writes them in.
    /// Every other row carries its message as it is.</summary>
    public static string MessageText(StoreHousekeepingEvent e)
    {
        if (e.Kind != StoreHousekeepingHistory.KindCompaction || e.Compaction is not { } c)
        {
            return e.Message ?? "";
        }

        var notes = CompactionNotes(c);
        return string.IsNullOrEmpty(e.Message) ? notes : notes + ". " + e.Message;
    }

    /// <summary>A re-cut skip reason in plain words (#9422). A reason this does not name yet prints as its own name.</summary>
    internal static string RecutSkipLabel(ArchiveService.RecutSkipReason reason) => reason switch
    {
        ArchiveService.RecutSkipReason.SortRanOutOfResources => "sort ran out of memory or temp space",
        ArchiveService.RecutSkipReason.RecutFailed => "re-cut failed",
        ArchiveService.RecutSkipReason.MergeSortedAtSmallGroups => "written by the merge at 2,048-row groups",
        ArchiveService.RecutSkipReason.MergeFellBackUnsorted => "written by the merge in the unsorted order",
        ArchiveService.RecutSkipReason.LockTimeout => "store busy (lock timeout)",
        ArchiveService.RecutSkipReason.ExpiresSoon => "retention deletes it within a day",
        ArchiveService.RecutSkipReason.LowDiskSpace => "free disk",
        ArchiveService.RecutSkipReason.OverBatchBudget => "over the batch budget",
        _ => reason.ToString(),
    };

    /// <summary>What one compaction pass did, in plain words (#9422, #9424): its notes with the size part in brackets after the groups merged.</summary>
    public static string CompactionSummary(CompactionPassDetail c) =>
        CompactionHead(c) + " (" + CompactionMoved(c) + ")" + CompactionTail(c);

    /// <summary>The size part of a compaction pass: the files and bytes the merges read and the files and bytes they wrote (#9604).</summary>
    public static string CompactionMoved(CompactionPassDetail c) =>
        string.Create(CultureInfo.InvariantCulture, $"{c.FilesIn:N0} files, {Bytes(c.BytesIn)} in, {c.FilesOut:N0} files, {Bytes(c.BytesOut)} out");

    /// <summary>Every part of <see cref="CompactionSummary"/> except the size part (<see cref="CompactionMoved"/>), in the same order (#9604).</summary>
    public static string CompactionNotes(CompactionPassDetail c) => CompactionHead(c) + CompactionTail(c);

    private static string CompactionHead(CompactionPassDetail c) =>
        string.Create(CultureInfo.InvariantCulture, $"started {c.StartUtc:HH:mm:ss} UTC, took {c.Seconds:0.0} s, merged {c.GroupsMerged:N0} groups");

    private static string CompactionTail(CompactionPassDetail c)
    {
        var ci = CultureInfo.InvariantCulture;
        var sb = new StringBuilder();
        if (c.SortedAtSmallGroupBatches > 0 || c.UnsortedFallbackBatches > 0)
        {
            sb.Append(ci, $"; {c.SortedAtSmallGroupBatches:N0} batches sorted again at 2,048-row groups, {c.UnsortedFallbackBatches:N0} batches fell back to unsorted");
        }

        var byReason = Enum.GetValues<ArchiveService.RecutSkipReason>()
            .Select(r => (Reason: r, Count: c.RecutSkips is { } m && m.TryGetValue(r.ToString(), out var n) ? n : 0))
            .Where(x => x.Count > 0)
            .ToList();
        var skipped = byReason.Sum(x => x.Count) + c.RecutSkippedLiveJournal + c.RecutSkippedTimeBudget;
        if (c.FilesRecut > 0 || skipped > 0)
        {
            sb.Append(ci, $"; re-cut {c.FilesRecut:N0} files");
            if (skipped > 0)
            {
                var parts = byReason.Select(x => string.Create(ci, $"{RecutSkipLabel(x.Reason)} {x.Count:N0}")).ToList();
                if (c.RecutSkippedLiveJournal > 0) parts.Add(string.Create(ci, $"live swap journal {c.RecutSkippedLiveJournal:N0}"));
                if (c.RecutSkippedTimeBudget > 0) parts.Add(string.Create(ci, $"time budget {(c.RecutSkippedTimeBudgetIsLowerBound ? "at least " : "")}{c.RecutSkippedTimeBudget:N0}"));
                sb.Append(ci, $", skipped {(c.RecutSkippedTimeBudgetIsLowerBound ? "at least " : "")}{skipped:N0} (").Append(string.Join(", ", parts)).Append(')');
            }
        }

        sb.Append(ci, $"; view rebuild {c.ViewRebuildMs:N0} ms");
        if (c.ProcessPeakWorkingSetBytes is not null || c.DuckDbPeakMemoryBytes is not null)
        {
            sb.Append("; peak memory");
            if (c.ProcessPeakWorkingSetBytes is { } proc) sb.Append(ci, $" process {Bytes(proc)}");
            if (c.DuckDbPeakMemoryBytes is { } duck) sb.Append(ci, $" DuckDB {Bytes(duck)}");
        }

        return sb.ToString();
    }

    /// <summary>The archive layout section's lines (#9421), indented under ARCHIVES.</summary>
    public static void AppendLayout(StringBuilder sb, ArchiveLayoutInfo layout, int maxTables = 40)
    {
        var ci = CultureInfo.InvariantCulture;
        sb.AppendLine(ci, $"  Layout, read from parquet footers only: {layout.FilesRead:N0} of {layout.FilesTotal:N0} files in {layout.Seconds:0.0} s");
        if (layout.Partial)
        {
            sb.AppendLine(ci, $"  PARTIAL: the layout read stopped at its {layout.BudgetSeconds:0.#} s budget. The figures below cover only the files it reached.");
        }

        if (layout.FilesUnreadable > 0)
        {
            sb.AppendLine(ci, $"  {layout.FilesUnreadable:N0} files could not be read (damaged footer or locked) and are not in the figures below.");
        }

        foreach (var t in layout.Tables.Take(maxTables))
        {
            sb.Append(ci, $"  {t.Table}: {t.Files:N0} files, {Bytes(t.Bytes)}, {t.RowGroups:N0} row groups, largest group {t.LargestGroupRows:N0} rows");
            if (t.MixedServerGroups is { } mixed)
            {
                sb.Append(ci, $", {mixed:N0} row groups mix servers");
            }

            if (t.FilesNeedingRecut > 0)
            {
                var skipped = t.RecutSkips.Sum(s => s.Files);
                sb.Append(ci, $", {t.FilesNeedingRecut:N0} files still need the re-cut");
                if (skipped > 0)
                {
                    sb.Append(ci, $", the re-cut is skipping {skipped:N0} of them (").Append(string.Join("; ", t.RecutSkips.Select(s => string.Create(ci, $"{s.Files:N0} {s.Reason}")))).Append(')');
                }
            }

            sb.AppendLine();
        }

        if (layout.Tables.Count > maxTables)
        {
            sb.AppendLine(ci, $"  ... and {layout.Tables.Count - maxTables:N0} smaller tables.");
        }

        sb.AppendLine(ci, $"  Leftover files in the archive folder: {layout.LeftoverTmp:N0} .tmp, {layout.LeftoverSwap:N0} .swap, {layout.LeftoverReplaced:N0} .replaced. A count above zero after startup cleanup means a swap did not finish.");
    }

    /// <summary>The whole snapshot as text, for <c>get_store_health</c>. <paramref name="maxTables"/> keeps the reply small on a store
    /// with the full set of collector tables.</summary>
    public static string Render(StoreHealthSnapshot snapshot, int maxTables = 25, int maxHistory = 25)
    {
        var sb = new StringBuilder();
        var ci = CultureInfo.InvariantCulture;

        sb.AppendLine("STORE");
        sb.AppendLine(StoreSentence(snapshot.Store));
        sb.AppendLine(ci, $"  Data inside the file: {(snapshot.Store.UsedMb is { } used ? Megabytes(used) : "not read")} of {snapshot.Store.ResetAtMb:0} MB reset point ({snapshot.Store.DataPercentOfReset:0}%)");
        sb.AppendLine(ci, $"  Store file on disk: {Megabytes(snapshot.Store.FileMb)} (resets at {snapshot.Store.FileResetAtMb:0} MB)");
        sb.AppendLine(ci, $"  Write-ahead log: {Megabytes(snapshot.Store.WalMb)}");
        if (snapshot.StoreNote is not null)
        {
            sb.AppendLine("  Note: " + snapshot.StoreNote);
        }

        sb.AppendLine();
        sb.AppendLine("ARCHIVES");
        var a = snapshot.Archives;
        sb.AppendLine(ci, $"  {a.FileCount:N0} parquet files, {Bytes(a.TotalBytes)} on disk");
        sb.AppendLine(ci, $"  Oldest month: {Month(a.OldestMonth)}. Newest month: {Month(a.NewestMonth)}");
        sb.AppendLine("  " + ArchiveSentence(a));
        if (a.UndatedFiles > 0)
        {
            sb.AppendLine("  " + UndatedSentence(a.UndatedFiles));
        }

        if (snapshot.Layout is { } layout)
        {
            AppendLayout(sb, layout);
        }

        sb.AppendLine();
        sb.AppendLine("HOUSEKEEPING HISTORY (newest first)");
        if (snapshot.History.Count == 0)
        {
            sb.AppendLine("  Nothing recorded yet.");
        }

        foreach (var e in snapshot.History.Take(maxHistory))
        {
            sb.Append(ci, $"  {e.TimeUtc:yyyy-MM-dd HH:mm} UTC  {KindLabel(e.Kind)}  {OutcomeLabel(e.Outcome)}  {WorkSummary(e)}");
            if (e.UsedMb is { } mb) sb.Append(ci, $", {mb:0} MB in store");
            if (e.Count > 1) sb.Append(ci, $", repeated {e.Count:N0} times");
            if (!string.IsNullOrEmpty(e.Message)) sb.Append(". ").Append(e.Message);
            sb.AppendLine();
        }

        if (snapshot.History.Count > maxHistory)
        {
            sb.AppendLine(ci, $"  ... and {snapshot.History.Count - maxHistory:N0} older entries.");
        }

        sb.AppendLine();
        sb.AppendLine("TABLES (estimated rows, largest first)");
        if (snapshot.Tables is null)
        {
            sb.AppendLine("  Not read.");
        }
        else
        {
            foreach (var t in snapshot.Tables.Take(maxTables))
            {
                sb.AppendLine(ci, $"  {t.Name}: {t.EstimatedRows:N0}");
            }

            if (snapshot.Tables.Count > maxTables)
            {
                sb.AppendLine(ci, $"  ... and {snapshot.Tables.Count - maxTables:N0} smaller tables.");
            }
        }

        return sb.ToString().TrimEnd();
    }
}
