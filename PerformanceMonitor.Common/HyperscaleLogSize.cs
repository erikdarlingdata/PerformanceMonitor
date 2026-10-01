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

    /// <summary>The note a Database Sizes read carries when a file in the snapshot has no size: the top-level
    /// <c>note</c> of both apps' <c>get_database_sizes</c> payload, which the web Database Sizes table shows above
    /// its rows. Plain words, with no field names, because people read it on the web page.</summary>
    public const string Note =
        "On Azure SQL Database Hyperscale, the transaction log lives in the log service, not in storage the "
        + "database holds. The log file's size is " + Display + ", and the totals leave it out.";
}
