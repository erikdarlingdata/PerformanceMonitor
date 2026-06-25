using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Context for an analysis run — what server, what time range.
/// </summary>
public class AnalysisContext
{
    public int ServerId { get; set; }
    public string ServerName { get; set; } = string.Empty;
    public DateTime TimeRangeStart { get; set; }
    public DateTime TimeRangeEnd { get; set; }

    /// <summary>
    /// The monitored SERVER's UTC offset (SYSDATETIME − SYSUTCDATETIME), captured once at
    /// analysis start. <see cref="TimeRangeStart"/>/<see cref="TimeRangeEnd"/> are in the
    /// server's LOCAL clock so every windowed read matches the collectors (which stamp rows
    /// with SYSDATETIME, server-local); this offset converts that window back to UTC for
    /// persistence/display. <see cref="TimeSpan.Zero"/> when the clock probe was unavailable
    /// (the window is then host-UTC — the prior behavior).
    /// </summary>
    public TimeSpan ServerUtcOffset { get; set; }

    public List<AnalysisExclusion> Exclusions { get; set; } = [];

    /// <summary>
    /// Duration of the examined period in milliseconds.
    /// </summary>
    public double PeriodDurationMs => (TimeRangeEnd - TimeRangeStart).TotalMilliseconds;
}
