/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One tempdb file from the newest <c>database_size_stats</c> collection, with the volume it sits on and the settings that decide its next
/// growth (#8882). The TempDB Growth Won't Fit alert reads these and nothing else: the growth settings and the volume's free space are in
/// the same row, so one read answers whether the next autogrowth fits.
/// </summary>
public class TempDbFileInfo
{
    public int FileId { get; set; }
    public string FileName { get; set; } = "";

    /// <summary><c>ROWS</c> or <c>LOG</c>, as <c>sys.master_files.type_desc</c> spells it.</summary>
    public string FileTypeDesc { get; set; } = "";

    public bool IsLog => string.Equals(FileTypeDesc, "LOG", StringComparison.OrdinalIgnoreCase);

    /// <summary>The file's current size (the in-database size, which does not lag autogrowth the way <c>sys.master_files</c> does).</summary>
    public double? TotalSizeMb { get; set; }

    /// <summary>The fixed growth step. Null when growth is by percent, and on a row stored before the column was read.</summary>
    public double? AutoGrowthMb { get; set; }

    public bool IsPercentGrowth { get; set; }

    /// <summary>The percent growth step, a whole number such as 10. Null unless <see cref="IsPercentGrowth"/>.</summary>
    public double? GrowthPct { get; set; }

    /// <summary>The growth ceiling in MB. -1 means unlimited. 0 means growth is not allowed. Null on a row that never carried one.</summary>
    public double? MaxSizeMb { get; set; }

    /// <summary>Null where the volume is not readable (Azure SQL Database, a login without the permission).</summary>
    public string? VolumeMountPoint { get; set; }

    public double? VolumeTotalMb { get; set; }
    public double? VolumeFreeMb { get; set; }

    /// <summary>
    /// Whether the data file's filegroup grows all its files together. Null on the log file, on a server before SQL Server 2016 and on a row
    /// stored before the flag was collected. Null reads as "off": the largest single growth is judged, not the sum.
    /// </summary>
    public bool? IsAutogrowAllFiles { get; set; }

    /// <summary>The <c>collection_time</c> of the collection the row belongs to. The alert's observation identity.</summary>
    public DateTime? CollectionTimeUtc { get; set; }
}
