using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Ui;

internal static class AxesExtensions
{
    /// <summary>Culture's short-date pattern with the year component removed (e.g. "M/d" en-US, "dd/MM" en-GB, "dd.MM" de-DE).</summary>
    private static readonly string MonthDayPattern = BuildMonthDayPattern();

    private static string BuildMonthDayPattern()
    {
        var p = CultureInfo.CurrentCulture.DateTimeFormat.ShortDatePattern;
        p = Regex.Replace(p, @"y+", "");
        p = Regex.Replace(p, @"^[\s/.\-]+|[\s/.\-]+$", "");
        p = Regex.Replace(p, @"([/.\-\s])\1+", "$1");
        return string.IsNullOrWhiteSpace(p) ? "M/d" : p;
    }

    /// <summary>
    /// Like <c>DateTimeTicksBottom()</c>, but prints the date line on only the first tick
    /// and on ticks where the date component changes. All other ticks show time-only.
    /// Date and time formats follow the current culture.
    /// </summary>
    public static void DateTimeTicksBottomDateChange(this ScottPlot.AxisManager axes)
    {
        axes.DateTimeTicksBottom();
        if (axes.Bottom.TickGenerator is ScottPlot.TickGenerators.DateTimeAutomatic gen)
        {
            DateTime? lastDate = null;
            DateTime? prevTick = null;
            var culture = CultureInfo.CurrentCulture;
            gen.LabelFormatter = dt =>
            {
                /* ScottPlot re-invokes this formatter from the leftmost tick on every render
                   pass, but lastDate persists across passes. Without resetting it, the first
                   tick stops printing its date after the first render — and a single-day window
                   then shows no date at all. Ticks are generated left-to-right, so a tick value
                   that does not increase marks the start of a new pass: reset there. */
                if (prevTick is null || dt <= prevTick.Value)
                    lastDate = null;
                prevTick = dt;

                var time = dt.ToString("t", culture);
                if (lastDate is null || dt.Date != lastDate.Value)
                {
                    lastDate = dt.Date;
                    return $"{dt.ToString(MonthDayPattern, culture)}\n{time}";
                }
                return time;
            };
        }
    }
}
