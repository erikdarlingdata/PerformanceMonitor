/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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
}
