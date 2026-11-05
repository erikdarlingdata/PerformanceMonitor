/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// #7417: the I/O volume a SQL Server target's read or write latency card needs behind it before it may
/// fire. An average of stall over a handful of I/Os is noise: an idle server's two reads at 57 ms are
/// 115 ms of total stall in the window, and the card then told the reader to tune scans, buffer pool memory
/// and storage. Darling (<c>PgFactCollector</c>, <c>PgAnomalyDetector</c>) and Lite (<c>DuckDbFactCollector</c>,
/// <c>AnomalyDetector</c>) both call these members, so the two products cannot drift.
/// <para>The figure is a count of I/O OPERATIONS (<c>delta_reads</c>, <c>delta_writes</c> summed over the same
/// rows and the same window the latency was computed from), not of file-stats samples: the anomaly read's
/// <c>read_sample_count</c> and <c>write_sample_count</c> count file rows, and one row can hold one I/O or ten
/// thousand.</para>
/// <para>The value 250 is the PostgreSQL target path's own floor
/// (<see cref="PgTargetScorer.IoBaselineBucketMinimumReads"/>, which admits a 15-minute bucket to the I/O
/// baseline only at 250 or more reads). The SQL Server window is wider than that bucket (an hour or more), so
/// the same count is the looser bar here, and a server that is really reading or logging clears it in
/// moments. It is not scaled: no SQL Server fleet measurement of window I/O counts exists to size another
/// number from.</para>
/// </summary>
public static class IoVolumeFloor
{
    /// <summary>The fewest reads (<c>SUM(delta_reads)</c> over the window) behind an <c>IO_READ_LATENCY_MS</c>
    /// fact or an <c>ANOMALY_READ_LATENCY</c> anomaly. Below it the average is a few I/Os and does not fire.</summary>
    public const long MinimumReads = 250;

    /// <summary>The fewest writes behind an <c>IO_WRITE_LATENCY_MS</c> fact or an <c>ANOMALY_WRITE_LATENCY</c>
    /// anomaly. Counted over the same files the write latency is scored on (log files and tempdb), so a busy
    /// user data file's checkpoint writes cannot lend volume to a quiet log. Equal to <see cref="MinimumReads"/>
    /// for the same reason: it is the PostgreSQL path's one number, and log flushes are batched so a real
    /// workload still clears it in a window.</summary>
    public const long MinimumWrites = 250;

    /// <summary>True when the window holds enough reads for a read latency card to fire.</summary>
    public static bool ReadsClearFloor(double reads) => reads >= MinimumReads;

    /// <summary>True when the window holds enough writes for a write latency card to fire.</summary>
    public static bool WritesClearFloor(double writes) => writes >= MinimumWrites;
}
