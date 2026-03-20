using System;

namespace PerformanceMonitorLite.Models;

/// <summary>
/// One hourly bucket of aggregated metrics for a time-range slicer.
/// Timestamps are in UTC (matching DuckDB collection_time storage).
/// </summary>
public class TimeSliceBucket
{
    public DateTime BucketTimeUtc { get; set; }
    public long SessionCount { get; set; }
    public double TotalCpu { get; set; }
    public double TotalElapsed { get; set; }
    public double TotalReads { get; set; }
    public double TotalLogicalReads { get; set; }
    public double TotalWrites { get; set; }

    /// <summary>The display value used by the slicer chart. Set by the caller based on sort column.</summary>
    public double Value { get; set; }
}
