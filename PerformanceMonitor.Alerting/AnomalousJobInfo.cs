/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// A currently-running SQL Agent job whose duration exceeds the anomaly threshold (Nx its
/// historical average), used by the long-running-job alert.
/// Canonical shared copy (Phase-5 A0) — Lite and the Dashboard previously carried member-identical
/// local twins; both apps now alias this type via a global using so call sites are unchanged.
/// </summary>
public class AnomalousJobInfo
{
    public string JobName { get; set; } = "";
    public string JobId { get; set; } = "";
    public long CurrentDurationSeconds { get; set; }
    public long AvgDurationSeconds { get; set; }
    public long P95DurationSeconds { get; set; }
    public decimal? PercentOfAverage { get; set; }

    /// <summary>
    /// When the run started, on the MONITORED SERVER's own clock. <c>running_jobs.start_time</c> is
    /// <c>ja.start_execution_date</c>, which <c>RunningJobsCollector</c> confirms local by taking the
    /// running duration as <c>DATEDIFF(SECOND, ja.start_execution_date, GETDATE())</c> — a local-vs-local
    /// subtraction — and which the collector stores unconverted.
    /// </summary>
    public DateTime StartTime { get; set; }

    /// <summary>
    /// The monitored server's collected UTC offset in minutes (negative west of UTC), carried on the row
    /// so <see cref="StartTimeUtc"/> can state <see cref="StartTime"/> in the frame the alert body and
    /// <c>alert_time</c> share. Null when the store holds no offset for the server, which leaves the
    /// instant unconvertible rather than assumed.
    /// <para>Carried rather than de-skewed in the read's own SQL, which is what the MCP and grid reads do:
    /// those take <c>COALESCE(utc_offset_minutes, 0)</c> and so hand back an unknown offset as UTC, and a
    /// notification has no neighbouring column for a reader to catch that against. Keeping both the raw
    /// instant and the offset is what lets the body say "server clock, offset not collected" instead.</para>
    /// </summary>
    public int? UtcOffsetMinutes { get; set; }

    /// <summary>
    /// <see cref="StartTime"/> in naive UTC, or null when <see cref="UtcOffsetMinutes"/> is absent.
    /// </summary>
    public DateTime? StartTimeUtc => AlertTimestamp.ToUtc(StartTime, UtcOffsetMinutes);
}

/// <summary>
/// The anomalous-jobs read with its evidence quality attached (#1812). <paramref name="SnapshotIsFresh"/>
/// is FALSE when the newest running_jobs snapshot is older than the freshness bound (or the store has no
/// snapshot at all): the rows describe what WAS running, not what IS, so the engine must treat the read
/// as no evidence — neither firing (the reported defect: a historical run re-alerting every cooldown,
/// forever, because the per-run cooldown key deliberately expires) nor fabricating a "jobs cleared"
/// resolution off staleness. <paramref name="Jobs"/> is empty whenever the snapshot is stale — the
/// adapters skip the row read entirely rather than hand the engine rows it must not use.
/// </summary>
public sealed record AnomalousJobsResult(bool SnapshotIsFresh, List<AnomalousJobInfo> Jobs)
{
    /// <summary>The no-evidence result: stale (or absent) snapshot, no rows.</summary>
    public static readonly AnomalousJobsResult Stale = new(false, new List<AnomalousJobInfo>());

    /// <summary>
    /// How old the newest running_jobs snapshot may be and still count as CURRENT: three missed
    /// collection cycles at the server's effective cadence, floored at 10 minutes so the fastest
    /// cadences keep slack for a slow sweep. Derived from the cadence rather than a fixed constant
    /// because the shipped profiles span 2 to 30 minutes — a fixed bound is either meaningless for
    /// the slow profile or blind for the fast one. The construction sites supply the effective
    /// per-server cadence (Lite's ScheduleManager, Darling's resolved schedule); adapters fall back
    /// to the shared default when a server has no entry.
    /// </summary>
    public static TimeSpan MaxSnapshotAge(int cadenceMinutes)
        => TimeSpan.FromMinutes(Math.Max(cadenceMinutes * 3, 10));
}
