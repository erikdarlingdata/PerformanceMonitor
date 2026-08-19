/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Alerting;

/// <summary>
/// One database file's current size, its growth over a lookback window, and the volume it sits on (#2349).
///
/// <para><b>Why this exists between the two alerts that already look at disk.</b> <c>tempdb Space</c> fires on
/// reserved ÷ (reserved + unallocated), and autogrowth adds unallocated extents — so the percentage FALLS as
/// tempdb balloons. It answers "is tempdb internally full right now", which is useful and structurally cannot
/// answer "has this file grown". <c>Volume Free Space</c> catches the consequence: it fires when the drive is
/// nearly full, by which point a restart is already overdue, and cannot attribute the space to one file.
/// Between them sits a file that has grown large but has not yet filled its disk.</para>
///
/// <para><b>Both gates come from one read.</b> The store already holds the time series, so
/// <see cref="GrowthMb"/> is measured against a sample from the lookback window rather than tracked in memory
/// — no per-file state to keep, survive a restart, or leak.</para>
/// </summary>
public class DatabaseFileGrowthInfo
{
    public string DatabaseName { get; set; } = "";
    public string FileName { get; set; } = "";
    public string PhysicalName { get; set; } = "";
    public string FileTypeDesc { get; set; } = "";

    /// <summary>Current size. Since #2169 this is the in-database current size where the probe got it, so it
    /// does not lag autogrowth — which tempdb, the motivating case, is guaranteed to do.</summary>
    public double TotalSizeMb { get; set; }

    /// <summary>Growth over the lookback window: current minus the oldest sample in it. Zero when the window
    /// holds only one sample, which reads as "no rise observed" rather than as a rise of the whole file.</summary>
    public double GrowthMb { get; set; }

    /// <summary>How wide the window actually was, so a rise can be reported as a rate rather than a bare number
    /// and a short window cannot masquerade as a slow one.</summary>
    public double GrowthWindowMinutes { get; set; }

    public string VolumeMountPoint { get; set; } = "";
    public double VolumeTotalMb { get; set; }
    public double VolumeFreeMb { get; set; }

    /// <summary>Null when growth is by PERCENT — the collector reports it that way on purpose, and a percent
    /// autogrowth on a large file is itself the misconfiguration worth surfacing.</summary>
    public double? AutoGrowthMb { get; set; }
    public bool IsPercentGrowth { get; set; }
    public double? GrowthPct { get; set; }

    /// <summary>-1 means unlimited, which the collector normalizes; carried so the payload can say so.</summary>
    public double? MaxSizeMb { get; set; }

    /// <summary>The file as a share of its volume — the self-scaling level gate. One global threshold behaves
    /// correctly across a fleet whose servers have very different normal sizes, which an absolute MB threshold
    /// cannot: set it low enough for the small instances and the large ones alert constantly.</summary>
    public double VolumePercent => VolumeTotalMb > 0 ? TotalSizeMb / VolumeTotalMb * 100 : 0;

    public double TotalSizeGb => TotalSizeMb / 1024.0;
    public double GrowthGb => GrowthMb / 1024.0;

    /// <summary>Growth per hour, for a message that distinguishes "80 GB in an hour" from "80 GB since Tuesday".</summary>
    public double GrowthMbPerHour =>
        GrowthWindowMinutes > 0 ? GrowthMb / (GrowthWindowMinutes / 60.0) : 0;
}
