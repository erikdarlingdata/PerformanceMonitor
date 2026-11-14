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
/// <param name="FileMb">The size of <c>monitor.duckdb</c> on disk. This is the number the size reset tests.</param>
/// <param name="WalMb">The size of the write-ahead log beside it, 0 when there is none.</param>
/// <param name="UsedMb">Data inside the file, from <c>pragma_database_size()</c>. Null when the store was busy.</param>
/// <param name="ResetAtMb">The size at which Lite archives all data and starts a fresh store.</param>
public sealed record StoreSizeInfo(double FileMb, double WalMb, double? UsedMb, double ResetAtMb)
{
    /// <summary>The file's share of the reset point, in percent.</summary>
    public double FilePercentOfReset => ResetAtMb <= 0 ? 0 : FileMb / ResetAtMb * 100.0;
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
    string? StoreNote);

/// <summary>
/// Reads Lite's own store health (#8788): the size of <c>monitor.duckdb</c> against the 512 MB reset point, the parquet archive
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

    public StoreHealthSnapshot Read()
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
            new StoreSizeInfo(fileMb, walMb, usedMb, CollectionBackgroundService.ArchiveSizeThresholdMb),
            ReadArchiveFolder(_archivePath),
            _history.Read(),
            tables,
            note);
    }

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
            $"Lite archives all data and starts a fresh store when the store file reaches {store.ResetAtMb:0} MB. " +
            $"The file is {store.FileMb:0.0} MB now, {store.FilePercentOfReset:0}% of that point.");

    public static string ArchiveSentence(ArchiveFolderInfo archives) =>
        string.Create(CultureInfo.InvariantCulture,
            $"Archive files are deleted when their month is more than {archives.RetentionMonths} months old.");

    public static string Megabytes(double mb) => string.Create(CultureInfo.InvariantCulture, $"{mb:#,##0.0} MB");

    public static string Bytes(long bytes) => Megabytes(bytes / (1024.0 * 1024.0));

    public static string Month(DateTime? month) => month is { } m ? m.ToString("yyyy-MM", CultureInfo.InvariantCulture) : "none";

    public static string KindLabel(string kind) => kind switch
    {
        StoreHousekeepingHistory.KindArchive => "Timed archive",
        StoreHousekeepingHistory.KindSizeReset => "Size reset",
        StoreHousekeepingHistory.KindRetention => "Retention cleanup",
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
        if (e.Kind == StoreHousekeepingHistory.KindRetention)
        {
            return files == 0 ? "no files expired" : string.Create(CultureInfo.InvariantCulture, $"{files:N0} files deleted");
        }

        return rows == 0 && files == 0
            ? "no rows moved"
            : string.Create(CultureInfo.InvariantCulture, $"{rows:N0} rows in {files:N0} files");
    }

    /// <summary>The whole snapshot as text, for <c>get_store_health</c>. <paramref name="maxTables"/> keeps the reply small on a store
    /// with the full set of collector tables.</summary>
    public static string Render(StoreHealthSnapshot snapshot, int maxTables = 25, int maxHistory = 25)
    {
        var sb = new StringBuilder();
        var ci = CultureInfo.InvariantCulture;

        sb.AppendLine("STORE");
        sb.AppendLine(StoreSentence(snapshot.Store));
        sb.AppendLine(ci, $"  Store file on disk: {Megabytes(snapshot.Store.FileMb)} of {snapshot.Store.ResetAtMb:0} MB reset point ({snapshot.Store.FilePercentOfReset:0}%)");
        sb.AppendLine(ci, $"  Data inside the file: {(snapshot.Store.UsedMb is { } used ? Megabytes(used) : "not read")}");
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
            sb.AppendLine(ci, $"  {a.UndatedFiles:N0} files have no date in their name and are never deleted by retention.");
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
