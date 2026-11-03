using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The axis label a blocking chart wears when it knows which collector answered (#5244), in one place for both apps. The blocking
/// trend and severity reads draw from the blocked-process-report XE session when it holds a row in the window for the chosen
/// databases, and from the DMV snapshot only when it holds none, so a filter of [A] can draw A's DMV-only event that [A, B] drops.
/// The MCP answers already name the arm (the <c>source</c> key); this puts the same two tags on the screens. Lite builds its labels
/// in <c>ServerTab.UpdateBlockingTrendChart</c> and the stats charts, the Darling viewer in <c>ViewerServerTab.Blocking.cs</c>, so
/// the two apps cannot word it differently.
/// </summary>
internal static class BlockingSourceLabel
{
    /// <summary>The tag on rows the blocked-process-report XE session answered (the same text as <c>BlockedProcessAlertRow.XeReportSource</c>).</summary>
    internal const string BlockedProcessReport = "blocked-process-report";

    /// <summary>The tag on rows the always-on DMV snapshot answered (the same text as <c>BlockedProcessAlertRow.DmvSnapshotSource</c>).</summary>
    internal const string DmvSnapshot = "DMV snapshot";

    /// <summary>
    /// <paramref name="baseLabel"/> with the answering arm appended in parentheses, for example <c>Blocking Incidents (DMV snapshot)</c>.
    /// The reads answer from ONE arm, so the first recognized tag names the chart. No rows, or rows carrying no recognized tag (the
    /// deadlock reads leave it null), return <paramref name="baseLabel"/> unchanged: nothing answered, so nothing is named.
    /// </summary>
    /// <param name="baseLabel">The chart's usual axis label.</param>
    /// <param name="sources">The <c>Source</c> of each drawn point.</param>
    internal static string For(string baseLabel, IEnumerable<string?> sources)
    {
        ArgumentNullException.ThrowIfNull(baseLabel);
        ArgumentNullException.ThrowIfNull(sources);

        foreach (var source in sources)
        {
            if (source is BlockedProcessReport or DmvSnapshot)
                return baseLabel + " (" + source + ")";
        }

        return baseLabel;
    }
}

/// <summary>
/// #7219: the blocking trend chart counts blocked-process REPORT rows per minute (one wait can write several reports), not blocked
/// sessions, so every screen that draws it says "reports". One place for both apps, so Lite and the Darling viewer cannot word it differently.
/// </summary>
internal static class BlockingTrendLabels
{
    /// <summary>The full label: chart legend, and the axis title where the plot is tall enough to carry it.</summary>
    internal const string PerMinute = "Blocked-process reports per minute";

    /// <summary>The shorter form for a hover name or a legend with less room.</summary>
    internal const string Reports = "Blocked-process reports";

    /// <summary>The left-axis title on the viewer's shortest chart row (88 px at 9 pt holds about 16 characters, see <c>ChartAxisTitle.Fit</c>); the legend carries the full <see cref="PerMinute"/>.</summary>
    internal const string AxisShort = "Reports per min";
}
