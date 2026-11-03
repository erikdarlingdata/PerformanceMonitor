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
    /// <paramref name="baseLabel"/> with the answering arm appended in parentheses, for example <c>Block Duration (ms) (DMV snapshot)</c>.
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
/// sessions. When the window holds no reports for the chosen databases the same read counts DMV blocking SNAPSHOT rows instead
/// (<see cref="BlockingSourceLabel.DmvSnapshot"/>: the AWS RDS case and the database-filter case), and a snapshot row is not a
/// report, so every screen that draws it names the arm that answered. One place for both apps, so Lite and the Darling viewer
/// cannot word it differently. Each <c>...For</c> member takes the <c>Source</c> of the drawn points, as
/// <see cref="BlockingSourceLabel.For"/> does; no rows, or no recognized tag, read as the reports arm (nothing answered, so the
/// usual wording stands).
/// </summary>
internal static class BlockingTrendLabels
{
    /// <summary>The full label: chart legend, and the axis title where the plot is tall enough to carry it.</summary>
    internal const string PerMinute = "Blocked-process reports per minute";

    /// <summary>The shorter form for a hover name or a legend with less room.</summary>
    internal const string Reports = "Blocked-process reports";

    /// <summary>The left-axis title on the shortest chart row, in both apps (88 px at 9 pt holds about 16 characters, see <c>ChartAxisTitle.Fit</c>); the legend carries the full <see cref="PerMinute"/>.</summary>
    internal const string AxisShort = "Reports per min";

    /// <summary>The unit after a value on a hover or a crosshair lane when the reports answered.</summary>
    internal const string ReportsUnit = "reports";

    /// <summary>The full label when the DMV snapshot answered.</summary>
    internal const string SnapshotPerMinute = "Blocking snapshot rows per minute";

    /// <summary>The hover name when the DMV snapshot answered.</summary>
    internal const string SnapshotRows = "Blocking snapshot rows";

    /// <summary>The short axis title when the DMV snapshot answered (13 characters, inside the 16 <see cref="AxisShort"/> is held to).</summary>
    internal const string SnapshotAxisShort = "Snapshots/min";

    /// <summary>The unit after a value when the DMV snapshot answered.</summary>
    internal const string SnapshotUnit = "snapshot rows";

    /// <summary>True when the first recognized tag is the DMV snapshot, the same "one arm per answer" reading <see cref="BlockingSourceLabel.For"/> uses.</summary>
    internal static bool AnsweredBySnapshot(IEnumerable<string?> sources)
    {
        ArgumentNullException.ThrowIfNull(sources);
        foreach (var source in sources)
        {
            if (source is BlockingSourceLabel.BlockedProcessReport) return false;
            if (source is BlockingSourceLabel.DmvSnapshot) return true;
        }

        return false;
    }

    /// <summary>The legend entry: the full per-minute label for the arm that answered, with the arm's tag appended.</summary>
    internal static string LegendFor(IEnumerable<string?> sources) =>
        BlockingSourceLabel.For(AnsweredBySnapshot(sources) ? SnapshotPerMinute : PerMinute, sources);

    /// <summary>The short left-axis title for the arm that answered.</summary>
    internal static string AxisTitleFor(IEnumerable<string?> sources) => AnsweredBySnapshot(sources) ? SnapshotAxisShort : AxisShort;

    /// <summary>The series name on a hover for the arm that answered.</summary>
    internal static string HoverNameFor(IEnumerable<string?> sources) => AnsweredBySnapshot(sources) ? SnapshotRows : Reports;

    /// <summary>The unit after a value on a hover or a crosshair lane for the arm that answered.</summary>
    internal static string UnitFor(IEnumerable<string?> sources) => AnsweredBySnapshot(sources) ? SnapshotUnit : ReportsUnit;
}
