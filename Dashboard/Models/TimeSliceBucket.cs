using System;

namespace PerformanceMonitorDashboard.Models;

/// <summary>
/// One hourly bucket of aggregated metrics for a time-range slicer.
/// In Dashboard, timestamps are in server local time (matching collect.* tables).
/// </summary>
public class TimeSliceBucket
{
    public DateTime BucketTime { get; set; }
    public long SessionCount { get; set; }
    public double TotalCpu { get; set; }
    public double TotalElapsed { get; set; }
    public double TotalReads { get; set; }
    public double TotalLogicalReads { get; set; }
    public double TotalWrites { get; set; }

    /// <summary>The display value used by the slicer chart. Set by the caller based on sort column.</summary>
    public double Value { get; set; }
}
