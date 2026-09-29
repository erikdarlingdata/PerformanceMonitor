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
                /* #1831: charts PLOT X in server time everywhere; the display-mode conversion
                   happens at render, here — the same split the crosshair/tooltips already use
                   (CorrelatedCrosshairManager → UiTimeContext.ConvertForDisplay). This was the one
                   render surface that skipped the conversion, so the axis under every chart showed
                   server time no matter what the toggle said. The lambda reads the hook at label
                   time, so a mode flip takes effect on the next render with no re-plot needed.
                   Apps that pre-convert plotted X (the Darling Viewer) leave UiTimeContext at its
                   identity default, making this a no-op there — do NOT also wire the hook in such
                   an app, that double-converts. Lite's converter follows the server's clock by date,
                   so the conversion is not one fixed shift: converted values never decrease, but they
                   are not strictly increasing, because server-local times inside a skipped hour convert
                   to the same instant. The pass-reset comparison below counts an equal value as "did
                   not increase", so a tick that repeats the one before it starts a new pass and prints
                   its date again. */
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
