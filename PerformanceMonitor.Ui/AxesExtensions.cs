using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Ui;

internal static class AxesExtensions
{
    /// <summary>Culture's short-date pattern with the year component removed (e.g. "M/d" en-US, "dd/MM" en-GB, "dd.MM" de-DE).</summary>
    private static readonly string MonthDayPattern = BuildMonthDayPattern();

    private static string BuildMonthDayPattern() => MonthDayPatternFor(CultureInfo.CurrentCulture);

    /// <summary>The culture's short-date pattern with the year removed: the month and day in the culture's own order.</summary>
    internal static string MonthDayPatternFor(CultureInfo culture)
    {
        var p = culture.DateTimeFormat.ShortDatePattern;
        p = Regex.Replace(p, @"y+", "");
        p = Regex.Replace(p, @"^[\s/.\-]+|[\s/.\-]+$", "");
        p = Regex.Replace(p, @"([/.\-\s])\1+", "$1");
        return string.IsNullOrWhiteSpace(p) ? "M/d" : p;
    }

    /// <summary>
    /// The bottom axis for a chart whose X is the UTC instant (#4766): ticks at whole wall-clock times of the
    /// display zone, labelled in it, with the date on the first tick and at each date change
    /// (<see cref="DisplayZoneTickGenerator"/>). <paramref name="zone"/> is read on every render pass, so a
    /// display-mode switch relabels the axis on the next render and no X value moves. Unlike
    /// <see cref="DateTimeTicksBottomDateChange"/> it does not consult <see cref="UiTimeContext"/>: X is already the
    /// instant, and the zone is applied once, here.
    /// </summary>
    public static void DateTimeTicksBottomUtc(this ScottPlot.AxisManager axes, Func<TimeZoneInfo> zone)
    {
        axes.DateTimeTicksBottom();
        axes.Bottom.TickGenerator = new DisplayZoneTickGenerator(zone);
    }

    /// <summary>
    /// The bottom axis a shared chart renderer uses: the UTC-instant axis when the caller passed a display zone
    /// (<see cref="DateTimeTicksBottomUtc"/>), else exactly what the renderers drew before
    /// (<see cref="DateTimeTicksBottomDateChange"/>).
    /// </summary>
    internal static void DateTimeTicksBottomFor(this ScottPlot.AxisManager axes, Func<TimeZoneInfo>? displayZone)
    {
        if (displayZone is null)
        {
            axes.DateTimeTicksBottomDateChange();
        }
        else
        {
            axes.DateTimeTicksBottomUtc(displayZone);
        }
    }

    /// <summary>
    /// Like <c>DateTimeTicksBottom()</c>, but prints the date line on only the first tick
    /// and on ticks where the date component changes. All other ticks show time-only.
    /// Date and time formats follow the current culture, and the label converts through
    /// <see cref="UiTimeContext.ConvertForDisplay"/> so the axis honors the app's Local/Server/UTC
    /// display mode (#1831).
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
                /* #1831: for an app whose charts PLOT X in server time, the display-mode conversion
                   happens at render, here — the same split the crosshair/tooltips already use
                   (CorrelatedCrosshairManager → UiTimeContext.ConvertForDisplay). This was the one
                   render surface that skipped the conversion, so the axis under every chart showed
                   server time no matter what the toggle said. The lambda reads the hook at label
                   time, so a mode flip takes effect on the next render with no re-plot needed.
                   Apps that plot the naive-UTC instant and pass their display zone to the tick
                   generator (the Darling Viewer, DateTimeTicksBottomUtc) leave UiTimeContext at its
                   identity default, making this a no-op there — do NOT also wire the hook in such
                   an app, that double-converts. Lite's converter follows the server's clock by date,
                   so the conversion is not one fixed shift, and converted values can decrease: across
                   the spring gap ToUtc(02:30) is 07:30Z and ToUtc(03:00) is 07:00Z, because a time
                   inside the skipped hour converts with the offset from before the gap. The pass-reset
                   comparison below treats an equal or lower value as "did not increase", so a tick that
                   repeats or falls behind the one before it starts a new pass and prints its date again. */
                dt = UiTimeContext.ConvertForDisplay(dt);

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
