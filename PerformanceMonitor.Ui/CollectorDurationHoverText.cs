using System;
using System.Globalization;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The line the Duration Trends chart's hover adds for a point (#4989, #4966), in one place for both apps. A point draws its
/// bucket's slowest run, so the line says how many runs it is the slowest of and what the average run took. Lite builds it
/// from its bucket (<c>ServerTab.CollectorDurationDetail</c>) and the Darling viewer from its series (<c>CollectorDurationSeries.DetailsByX</c>),
/// so the two apps cannot word it differently. It sits beside <see cref="ChartHoverHelper"/>, which prints it as the popup's
/// fourth line when a series is registered with its per-point details.
/// </summary>
internal static class CollectorDurationHoverText
{
    /// <summary>
    /// <c>Slowest of {runs} {run|runs}; average {ms} ms</c>, in the current culture: the count with its thousands separator,
    /// and the average without a decimal when it is a whole number of milliseconds and with one when it is not.
    /// </summary>
    /// <param name="runCount">How many runs the point's bucket holds.</param>
    /// <param name="averageMs">The average duration of those runs, in milliseconds.</param>
    internal static string Detail(long runCount, double averageMs)
    {
        static string Ms(double value) => value == Math.Floor(value)
            ? value.ToString("N0", CultureInfo.CurrentCulture)
            : value.ToString("N1", CultureInfo.CurrentCulture);

        return $"Slowest of {runCount.ToString("N0", CultureInfo.CurrentCulture)} {(runCount == 1 ? "run" : "runs")}; average {Ms(averageMs)} ms";
    }
}
