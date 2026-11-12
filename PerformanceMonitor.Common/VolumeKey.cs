/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// #7704: the one spelling of how a read names the disk volume a database file sits on, shared by Lite (DuckDB) and
/// Darling (PostgreSQL): both engines take this text as it stands. SQL Server on Linux returns NULL for
/// <c>volume_mount_point</c> in <c>sys.dm_os_volume_stats</c> (the total and free bytes are still reported), and the
/// collector stores the name as it comes. The reads that group by volume (the Volume Free Space alert, the DISK_SPACE
/// fact, the Daily Health Report, the Database Sizes <c>Volume</c> column) used to drop such rows, so a Linux server
/// got no disk alert, no disk card and no report row however full its disk got.
///
/// <para>The key is the stored name when there is one. A row with no name and a volume total takes the directory of its
/// file's <c>physical_name</c>, the part before the last <c>/</c> or <c>\</c> (<c>/var/opt/mssql/data</c>). The directory is
/// used rather than the volume's total size because it names the disk for the alert and the card, and two disks of one
/// size stay apart. Files in two directories on one disk then show as two volumes with the same free figure, which is
/// accepted. A row with no name and no volume total (Azure SQL Database has no volume stats) has no key, so a read that
/// filters on <c>volume_total_mb &gt; 0</c> or on the key being not null leaves it out as before. It reads the stored
/// rows, so history collected before the fix works too, and the collector is unchanged.</para>
/// </summary>
public static class VolumeKey
{
    /// <summary>The key as a SQL expression over the bare columns <c>volume_mount_point</c>, <c>volume_total_mb</c> and
    /// <c>physical_name</c>: put it in the select list (<c>... AS volume_mount_point</c>), a group-by or a window, in a query
    /// whose <c>FROM</c> holds one table of the size history. The pattern has no escapes, so the same text is a valid
    /// PostgreSQL and DuckDB string literal.</summary>
    public const string Sql =
        "COALESCE(volume_mount_point, CASE WHEN volume_total_mb > 0 THEN regexp_replace(physical_name, '[/\\\\][^/\\\\]*$', '') END)";
}
