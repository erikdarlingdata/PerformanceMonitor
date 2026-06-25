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
/// One hourly bucket of aggregated metrics for a time-range slicer.
/// The timezone of <see cref="BucketTime"/> is defined by the producer, not this type:
/// Lite supplies UTC (matching DuckDB collection_time storage); Dashboard supplies
/// server local time (matching collect.* tables). The name is intentionally neutral
/// (no ...Utc suffix) so neither producer's data is mislabeled. Consumers within an
/// app must stay consistent with that app's convention.
/// </summary>
public class TimeSliceBucket
{
    public DateTime BucketTime { get; set; }
    public long SessionCount { get; set; }
    public double TotalCpu { get; set; }
    public double TotalElapsed { get; set; }
    public double TotalReads { get; set; }
    public double TotalLogicalReads { get; set; }
    public double TotalPhysicalReads { get; set; }
    public double TotalWrites { get; set; }

    /// <summary>The display value used by the slicer chart. Set by the caller based on sort column.</summary>
    public double Value { get; set; }
}
