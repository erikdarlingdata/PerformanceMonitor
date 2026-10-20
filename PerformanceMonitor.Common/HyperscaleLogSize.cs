/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The one spelling of how Database Sizes and File I/O treat the transaction log on Azure SQL Database Hyperscale,
/// shared by Lite and Darling so both apps say the same thing. <c>FileIoStatsCollector.NoSizeLabel</c> points at
/// <see cref="Display"/>; this class owns the words.
///
/// <para>On Hyperscale, <c>sys.database_files</c> reports the LOG file at about 1 TB (1,046,528 MB) while the data
/// file reports its real allocation. The log lives in Hyperscale's log service, so that figure is not storage the
/// database holds or pays for: Microsoft Learn, "What is the Hyperscale service tier?", says Hyperscale bills
/// <i>allocated data storage</i>, allocated automatically from 10 GB, and the Hyperscale FAQ calls the log
/// "practically infinite", with only its active portion capped at 1 TB. The collector therefore stores a NULL
/// <c>total_size_mb</c> for that one row, and every reader shows <see cref="Display"/> where the size would be and
/// leaves the row out of every allocated total.</para>
/// </summary>
public static class HyperscaleLogSize
{
    /// <summary>What a grid cell or web table shows where a Hyperscale log file's size would be.</summary>
    public const string Display = "n/a (log service)";

    /// <summary>What the Note column on a Storage Growth row says for a database whose log file has no size: its
    /// size is data space only, and the log is not in it. The same style as
    /// <see cref="AzureSiblingDatabaseSize.LogNote"/>, so the two kinds of row read alike.</summary>
    public const string LogNote = "Log size: " + Display;

    /// <summary>The note a Database Sizes read carries when a file in the snapshot has no size: the top-level
    /// <c>note</c> of both apps' <c>get_database_sizes</c> payload, which the web Database Sizes table shows above
    /// its rows. Plain words, with no field names, because people read it on the web page.</summary>
    public const string Note =
        "On Azure SQL Database Hyperscale, the transaction log lives in the log service, not in storage the "
        + "database holds. The log file's size is " + Display + ", and the totals leave it out. "
        + DataFileCapNote;

    /// <summary>#6178: what a Hyperscale data file's maximum size is. <c>sys.database_files</c> reports 131072 MB (128 GB)
    /// for the data file, which is the cap for that one file. The database itself grows to 128 TB. Plain words, because
    /// people read it on the web page.</summary>
    public const string DataFileCapNote =
        "The data file's maximum size is a limit for that one file, not for the database, which can grow to 128 TB.";

    /// <summary>#6178: the note on the row of a Hyperscale database's data file, under the same <c>size_note</c> key the log
    /// row and the sibling row use. It sits beside the maximum size it explains.</summary>
    public const string DataFileMaxSizeNote = "Max size: a limit for this one file. A Hyperscale database can grow to 128 TB.";

    /// <summary>#6178: true for a data file whose maximum size is a per-file cap on Hyperscale: a data file (type ROWS) with a
    /// maximum size, in a database whose log file has no size (the log service). That log row is how a Hyperscale database is
    /// told apart, so no other database gets the note.</summary>
    public static bool IsDataFileWithFileCap(string? fileType, bool databaseHasLogServiceFile, bool hasMaxSize) =>
        databaseHasLogServiceFile && hasMaxSize && string.Equals(fileType, "ROWS", System.StringComparison.OrdinalIgnoreCase);
}
