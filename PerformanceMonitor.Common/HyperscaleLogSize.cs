/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The one spelling of how Database Sizes treats the transaction log on Azure SQL Database Hyperscale, shared by
/// Lite and Darling so both apps say the same thing.
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

    /// <summary>The short note a Database Sizes MCP payload carries beside a null <c>total_size_mb</c>.</summary>
    public const string Note =
        "total_size_mb is null for a log file on Azure SQL Database Hyperscale: the log lives in the log service, "
        + "so its size is n/a (log service) and it is left out of every allocated total.";
}
