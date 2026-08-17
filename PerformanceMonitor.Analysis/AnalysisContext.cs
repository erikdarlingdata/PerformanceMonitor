using System;
using System.Threading;

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
    /// The host's stopping token, observed by the pass's store reads (#2299). Default
    /// <see cref="CancellationToken.None"/> — a caller that does not plumb one (Lite, the
    /// fact-inspection paths) keeps the prior behavior exactly, because every shutdown
    /// classification requires this token to be SIGNALLED. Carried on the context rather than
    /// on thirty method signatures because the context already reaches every pipeline stage.
    /// </summary>
    public CancellationToken CancellationToken { get; set; }

    /// <summary>
    /// The monitored SERVER's UTC offset (SYSDATETIME − SYSUTCDATETIME), captured once at
    /// analysis start. <see cref="TimeRangeStart"/>/<see cref="TimeRangeEnd"/> are in the
    /// server's LOCAL clock so every windowed read matches the collectors (which stamp rows
    /// with SYSDATETIME, server-local); this offset converts that window back to UTC for
    /// persistence/display. <see cref="TimeSpan.Zero"/> when the clock probe was unavailable
    /// (the window is then host-UTC — the prior behavior).
    /// </summary>
    public TimeSpan ServerUtcOffset { get; set; }

    /// <summary>
    /// Duration of the examined period in milliseconds.
    /// </summary>
    public double PeriodDurationMs => (TimeRangeEnd - TimeRangeStart).TotalMilliseconds;
}
