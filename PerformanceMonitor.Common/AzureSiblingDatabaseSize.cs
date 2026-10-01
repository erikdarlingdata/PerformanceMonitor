/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one spelling of how Database Sizes treats the OTHER databases on an Azure SQL Database server, shared by Lite
/// and Darling so both apps say the same thing. <c>DatabaseSizeStatsCollector</c> writes the row and the growth reads
/// leave the old shape of it out; this class owns the name, the old-shape test and the words.
///
/// <para>Connected to <c>master</c>, the collector reads each other database from <c>sys.resource_stats</c> and stores
/// one row for it, named <see cref="FileName"/>. Rows stored before the allocated/used fix put the database's USED
/// space in <c>total_size_mb</c> and left <c>used_size_mb</c> empty. Every other row in the store puts the ALLOCATED
/// size there. Rows stored after the fix put the allocated size in <c>total_size_mb</c> and the used space in
/// <c>used_size_mb</c>. A growth read that set an old row beside a new one would call the one-time change of meaning a
/// size jump: 119 MB then 10,240 MB for a Hyperscale database that did not grow at all.</para>
///
/// <para>So a growth read leaves the old-shape rows out (<see cref="ExcludePreFixRows"/>), at read time, so it covers
/// history collected before the change without rewriting it. A database then reads like one that was added inside the
/// window: its past size is blank and its growth is 0, until a post-fix sample is old enough to compare against.
/// Reads of a current size, and sums over the latest snapshot, keep every row.</para>
/// </summary>
public static class AzureSiblingDatabaseSize
{
    /// <summary>The <c>file_name</c> of the one row a sibling database gets: <c>sys.resource_stats</c> has no
    /// per-file breakdown, so the row says it covers the whole database instead of naming a file that does not
    /// exist. Holds no single quote, because the collector places it inside a quoted string.</summary>
    public const string FileName = "(whole database)";

    /// <summary>True for a row stored before the allocated/used fix: no file id, the <see cref="FileName"/> name,
    /// and no used space. The three together, because a real file whose used-space probe failed also has no used
    /// space, and its growth must still count. A row with no file name does not match, so it is never left out.
    /// Column names are bare, so it splices into any query whose <c>FROM</c> holds one table of the size history,
    /// whatever its alias.</summary>
    public const string PreFixRowPredicate =
        "file_id IS NULL AND COALESCE(file_name, '') = '" + FileName + "' AND used_size_mb IS NULL";

    /// <summary>The clause a growth read adds to its <c>WHERE</c>, after <c>AND</c>: it keeps every row that is not
    /// an old-shape sibling row. Both sides of the test are two-valued, so a NULL cannot make a row drop out.</summary>
    public const string ExcludePreFixRows = "NOT (" + PreFixRowPredicate + ")";

    /// <summary>What a Database Sizes row for another database says about the log. The size on such a row is data
    /// space only, and the server reports no log size for a database other than the connected one, so the log is
    /// not zero and not included: it is not known. Plain words, with no field names, because people read it on a
    /// grid or a web page.</summary>
    public const string LogNote =
        "Log size: n/a (not reported for other databases on an Azure SQL Database server)";

    /// <summary>What the caption over a Database Sizes grid says when the grid holds a row for another database: those
    /// rows hold data space only, and their log size is not reported. Names the file name those rows carry, so a
    /// reader can tell which rows it means. Both apps' grids build their caption from it, so they say the same
    /// words as <see cref="LogNote"/> says on the row.</summary>
    public const string GridCaption =
        "Rows named " + FileName + " hold data space only, and their log size is not reported.";

    /// <summary>The key of the per-row note in a <c>get_database_sizes</c> payload, the same key the file I/O payload
    /// uses for the note beside a size that is not there. A sibling row carries <see cref="LogNote"/> under it, both on
    /// its entry in <c>files</c> and on its database's entry, which is the row the web table draws. No other row has
    /// the key.</summary>
    public const string RowNoteKey = "size_note";

    /// <summary>True for the one row a sibling database has, in either shape (the old one with no used space, or the
    /// new one): no file id, and the <see cref="FileName"/> name. Both apps' size reads match a row with this and
    /// nothing else, so a real file is never taken for a sibling. The name is compared exactly, as the collector
    /// wrote it.</summary>
    public static bool IsSiblingRow(int? fileId, string? fileName) =>
        fileId is null && string.Equals(fileName, FileName, StringComparison.Ordinal);

    /// <summary>The top-level <c>note</c> of a <c>get_database_sizes</c> payload, or null when nothing in the
    /// snapshot needs one. It carries <see cref="HyperscaleLogSize.Note"/> when the snapshot holds a log file with no
    /// size, and <see cref="LogNote"/> when it holds a sibling row. When both apply the two sit side by side, the
    /// Hyperscale sentence first. Lite and Darling both build the note here, so the two payloads say the same
    /// words.</summary>
    public static string? DatabaseSizesNote(bool hasLogServiceFile, bool hasSiblingRow)
    {
        if (hasLogServiceFile && hasSiblingRow) return HyperscaleLogSize.Note + " " + LogNote;
        if (hasLogServiceFile) return HyperscaleLogSize.Note;
        return hasSiblingRow ? LogNote : null;
    }
}
