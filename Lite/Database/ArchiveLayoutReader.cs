/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>Files the one-time re-cut is leaving alone, with why (#9421).</summary>
public sealed record ArchiveRecutSkip(string Reason, int Files);

/// <summary>How one table's archive files are laid out (#9421).</summary>
/// <param name="Files">Parquet files of the table in the archive folder.</param>
/// <param name="Bytes">Their size on disk.</param>
/// <param name="FilesRead">Files whose footer the layout read covered. Less than <paramref name="Files"/> when the read ran out of time or a file went away.</param>
/// <param name="RowGroups">Row groups in the files read.</param>
/// <param name="LargestGroupRows">The most rows in any one row group of the files read.</param>
/// <param name="MixedServerGroups">Row groups whose server_id minimum differs from its maximum, so a read for one server cannot skip them. Null for a table that is not merged in server order.</param>
/// <param name="FilesNeedingRecut">Files still due for the one-time re-cut (#9407): sorted tables' compaction files whose row groups are all small.</param>
/// <param name="RecutSkips">Of those, the files the re-cut is skipping now, by reason. The rest wait for a pass.</param>
public sealed record ArchiveTableLayout(
    string Table, int Files, long Bytes, int FilesRead, long RowGroups, long LargestGroupRows, long? MixedServerGroups,
    int FilesNeedingRecut, IReadOnlyList<ArchiveRecutSkip> RecutSkips);

/// <summary>The archive's layout, read from parquet footers only (#9421).</summary>
/// <param name="LeftoverTmp">Files ending in .tmp at the top of the archive folder: an export or merge that did not finish or whose cleanup did not run.</param>
/// <param name="LeftoverSwap">Swap journals (.swap). One that is still there after startup means a compaction swap did not finish.</param>
/// <param name="LeftoverReplaced">Files set aside by a swap (.replaced) and not yet deleted.</param>
/// <param name="Partial">True when the read stopped at its time budget, so the per-table figures cover only <paramref name="FilesRead"/> of <paramref name="FilesTotal"/> files.</param>
public sealed record ArchiveLayoutInfo(
    IReadOnlyList<ArchiveTableLayout> Tables, int LeftoverTmp, int LeftoverSwap, int LeftoverReplaced,
    int FilesRead, int FilesTotal, int FilesUnreadable, bool Partial, double Seconds, double BudgetSeconds);

/// <summary>
/// Reads how the archive's parquet files are laid out (#9421): row groups, mixed-server groups, files the one-time re-cut still
/// owes and leftover swap files. It reads <c>parquet_metadata</c> (the footer) of each file on its own in-memory DuckDB connection
/// and never row data. It takes neither the store's write lock nor the process-wide archive lock, so collection and compaction go
/// on. A file that is gone when it is opened (a swap or retention removed it) is skipped. The read stops at its time budget and
/// says it is partial.
/// </summary>
internal sealed class ArchiveLayoutReader
{
    public const string ReasonOverBatchBudget = "larger than the table's merge batch budget";
    public const string ReasonFreeDisk = "not enough free disk space";
    public const string ReasonLiveJournal = "a swap journal for its period is still live";
    public const string ReasonSortFailed = "skipped at this build after an earlier failure or a merge that sorted it at small groups or unsorted (tried again at the next build)";
    public const string ReasonExpiresSoon = "retention deletes it within a day";

    private static readonly Regex s_tableName = new(
        @"^(imported_)?((\d{8}_\d{4}_)|(\d{8}_)|(\d{6}_)|(\d{4}-\d{2}_)|(all_))?(?<table>.+?)(_pt\d{3})?$", RegexOptions.Compiled);

    private readonly string _archivePath;
    private readonly string _databasePath;
    private readonly Func<string, long?> _freeBytes;
    private readonly TimeSpan _budget;
    private readonly string _buildId;

    /* Tests only: called with a file's path just before its footer is read, so a test can remove the file under the read. */
    internal Action<string>? BeforeFileReadForTests { get; set; }

    /* Tests only: the merge batch budget of a table, null for the production budgets. */
    internal Func<string, long>? BatchBudgetForTests { get; set; }

    public ArchiveLayoutReader(string archivePath, string databasePath, Func<string, long?> freeBytes, TimeSpan budget, string? buildId = null)
    {
        _buildId = buildId ?? ArchiveService.CurrentBuildId();
        _archivePath = archivePath;
        _databasePath = databasePath;
        _freeBytes = freeBytes;
        _budget = budget;
    }

    /// <summary>The table a parquet file belongs to, from its name, whatever its shape (per-cycle, day, month, part, imported).</summary>
    internal static string TableOf(string fileName)
    {
        var name = Path.GetFileNameWithoutExtension(fileName);
        return s_tableName.Match(name).Groups["table"].Value;
    }

    public ArchiveLayoutInfo Read()
    {
        var clock = Stopwatch.StartNew();
        var files = new List<(string Path, string Name, string Table, long Bytes)>();
        if (Directory.Exists(_archivePath))
        {
            foreach (var file in EnumerateOrEmpty("*.parquet"))
            {
                try
                {
                    var bytes = new FileInfo(file).Length;
                    var name = System.IO.Path.GetFileName(file);
                    files.Add((file.Replace("\\", "/"), name, TableOf(name), bytes));
                }
                catch (IOException)
                {
                    /* Removed between the listing and the stat. */
                }
            }
        }

        var leftoverTmp = EnumerateOrEmpty("*.tmp").Count(f => f.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase));
        var leftoverSwap = EnumerateOrEmpty("*.swap").Count(f => f.EndsWith(".swap", StringComparison.OrdinalIgnoreCase));
        var leftoverReplaced = EnumerateOrEmpty("*.replaced").Count(f => f.EndsWith(".replaced", StringComparison.OrdinalIgnoreCase));

        long? free = null;
        var reserve = 0L;
        try
        {
            free = _freeBytes(_archivePath);
            reserve = DataVolumeSpace.CompactionReserveBytes(_databasePath);
        }
        catch (Exception)
        {
            /* The free-space figure is a hint for the skip reasons: without it no file is called out for lack of space. */
        }

        var skipList = ArchiveService.ReadRecutSkipList(_archivePath);
        var perTable = new Dictionary<string, TableAcc>(StringComparer.OrdinalIgnoreCase);
        foreach (var f in files)
        {
            if (!perTable.TryGetValue(f.Table, out var acc))
            {
                perTable[f.Table] = acc = new TableAcc();
            }

            acc.Files++;
            acc.Bytes += f.Bytes;
        }

        var filesRead = 0;
        var unreadable = 0;
        var partial = false;
        if (files.Count > 0)
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();

            /* Newest first: the recent files are the ones a slow read and the re-cut care about. */
            foreach (var f in files.OrderByDescending(x => x.Name, StringComparer.Ordinal))
            {
                if (clock.Elapsed >= _budget)
                {
                    partial = true;
                    break;
                }

                var acc = perTable[f.Table];
                var serverOrdered = ParquetCompaction.ServerOrderByFor(f.Table) is not null;
                long groups, maxRows, mixed;
                bool due;
                try
                {
                    BeforeFileReadForTests?.Invoke(f.Path);
                    (groups, maxRows, mixed) = ReadFooter(connection, f.Path, serverOrdered);
                    due = DueForRecut(connection, f.Path, f.Name, f.Table, groups, maxRows);
                }
                catch (Exception) when (!File.Exists(f.Path))
                {
                    /* Swapped or deleted under the read: not in the folder any more. */
                    acc.Files--;
                    acc.Bytes -= f.Bytes;
                    continue;
                }
                catch (Exception)
                {
                    unreadable++;
                    continue;
                }

                filesRead++;
                acc.FilesRead++;
                acc.RowGroups += groups;
                acc.LargestGroupRows = Math.Max(acc.LargestGroupRows, maxRows);
                if (serverOrdered)
                {
                    acc.Mixed = (acc.Mixed ?? 0) + mixed;
                }

                if (due)
                {
                    acc.NeedingRecut++;
                    var reason = RecutSkipReason(skipList, f.Path, f.Name, f.Table, f.Bytes, free, reserve);
                    if (reason is not null)
                    {
                        acc.Skips[reason] = acc.Skips.GetValueOrDefault(reason) + 1;
                    }
                }
            }
        }

        var tables = perTable
            .Where(kv => kv.Value.Files > 0)
            .OrderByDescending(kv => kv.Value.Bytes).ThenBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => new ArchiveTableLayout(
                kv.Key, kv.Value.Files, kv.Value.Bytes, kv.Value.FilesRead, kv.Value.RowGroups, kv.Value.LargestGroupRows, kv.Value.Mixed,
                kv.Value.NeedingRecut, kv.Value.Skips.Select(s => new ArchiveRecutSkip(s.Key, s.Value)).ToList()))
            .ToList();

        return new ArchiveLayoutInfo(
            tables, leftoverTmp, leftoverSwap, leftoverReplaced, filesRead, files.Count, unreadable, partial,
            clock.Elapsed.TotalSeconds, _budget.TotalSeconds);
    }

    private static (long Groups, long MaxRows, long Mixed) ReadFooter(DuckDBConnection connection, string path, bool serverOrdered)
    {
        using var cmd = connection.CreateCommand();
        var escaped = path.Replace("'", "''");
        cmd.CommandText = serverOrdered
            ? "SELECT count(DISTINCT row_group_id), coalesce(max(row_group_num_rows), 0), " +
              "count(*) FILTER (WHERE path_in_schema = 'server_id' AND stats_min IS NOT NULL AND stats_max IS NOT NULL AND stats_min <> stats_max) " +
              $"FROM parquet_metadata('{escaped}')"
            : "SELECT count(DISTINCT row_group_id), coalesce(max(row_group_num_rows), 0), 0 " +
              $"FROM parquet_metadata('{escaped}')";
        using var reader = cmd.ExecuteReader();
        reader.Read();
        return (Convert.ToInt64(reader.GetValue(0)), Convert.ToInt64(reader.GetValue(1)), Convert.ToInt64(reader.GetValue(2)));
    }

    /* The re-cut's own candidate rule (ArchiveService.RecutSmallRowGroupFiles): a compaction file of a table that merges in
       server order, not a day file of a table that merges monthly, whose shape NeedsRecut names. NeedsRecut is the re-cut's own
       rule, the byte check included (#9455), so a file whose largest group already holds half the byte bound is not counted, and
       a file stamped with the current rule is not counted either (#9409). */
    private static bool DueForRecut(DuckDBConnection connection, string path, string fileName, string table, long groups, long maxRows)
    {
        var m = ArchiveService.s_recutFileName.Match(fileName);
        if (!m.Success || ParquetCompaction.ServerOrderByFor(table) is null)
        {
            return false;
        }

        var period = m.Groups["period"].Value;
        return !(period.Length == 8 && !ParquetCompaction.IsDailyTable(table))
            && ParquetCompaction.NeedsRecut(groups, maxRows, () => ParquetCompaction.SortedCutStamp(connection, path),
                () => ParquetCompaction.MaxRowGroupUncompressedBytes(connection, path));
    }

    private string? RecutSkipReason(Dictionary<string, string> skipList, string path, string fileName, string table, long bytes, long? free, long reserve)
    {
        var m = ArchiveService.s_recutFileName.Match(fileName);
        var period = m.Groups["period"].Value;
        if (File.Exists(System.IO.Path.Combine(_archivePath, $"{period}_{table}.swap")))
        {
            return ReasonLiveJournal;
        }

        /* The persisted skip list the re-cut keeps: an entry naming this build means the re-cut passes the file over. */
        if (skipList.TryGetValue(fileName, out var skippedAt) && string.Equals(skippedAt, _buildId, StringComparison.Ordinal))
        {
            return ReasonSortFailed;
        }

        if (RetentionService.ExpiresWithin(fileName, ArchiveService.RecutRetentionWindow, DateTime.UtcNow))
        {
            return ReasonExpiresSoon;
        }

        if (bytes > (BatchBudgetForTests?.Invoke(table) ?? ParquetCompaction.BatchInputBytesFor(table)))
        {
            return ReasonOverBatchBudget;
        }

        return ArchiveService.RecutLacksDiskSpace(bytes, free, reserve) ? ReasonFreeDisk : null;
    }

    private List<string> EnumerateOrEmpty(string pattern)
    {
        try
        {
            return Directory.Exists(_archivePath) ? Directory.EnumerateFiles(_archivePath, pattern).ToList() : [];
        }
        catch (IOException)
        {
            return [];
        }
    }

    private sealed class TableAcc
    {
        public int Files;
        public long Bytes;
        public int FilesRead;
        public long RowGroups;
        public long LargestGroupRows;
        public long? Mixed;
        public int NeedingRecut;
        public readonly Dictionary<string, int> Skips = [];
    }
}
