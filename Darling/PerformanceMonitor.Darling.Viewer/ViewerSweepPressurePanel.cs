/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One line of the Sweep Pressure panel: a measure and its value, worded as the web's stat tiles word them.</summary>
/// <param name="Label">The web tile's label.</param>
/// <param name="Value">The figure, formatted as the web formats it.</param>
internal sealed record SweepStatRow(string Label, string Value);

/// <summary>One row of the Heaviest Collectors grid (#6384). The text columns are what the web's table shows; the numbers are what each
/// column sorts by.</summary>
internal sealed record HeaviestCollectorGridRow(HeaviestCollectorRow Source)
{
    public string Collector => Source.Collector;

    public string Avg => SweepPressurePanel.FormatMs(Source.AvgDurationMs);

    public string P95 => SweepPressurePanel.FormatMs(Source.P95DurationMs);

    public string Max => SweepPressurePanel.FormatMs(Source.MaxDurationMs);

    public string EveryMinutes => SweepPressurePanel.FormatNumber(Source.FrequencyMinutes);

    public string MsPerMinute => SweepPressurePanel.FormatNumber(Source.AmortizedMsPerMinute);

    public string PctOfRunBudget => SweepPressurePanel.FormatNumber(Source.PctOfSweepBudgetPerRun);

    public double AvgMs => Source.AvgDurationMs;

    public double P95Ms => Source.P95DurationMs;

    public double MaxMs => Source.MaxDurationMs;

    public double Every => Source.FrequencyMinutes;

    public double PerMinute => Source.AmortizedMsPerMinute;

    public double PctBudget => Source.PctOfSweepBudgetPerRun;
}

/// <summary>
/// The Collection Health tab's Sweep Pressure and Heaviest Collectors panels (#6384): the same figures, in the same order, units and
/// wording, the web's panels show from <c>get_collection_health</c> (<c>SWEEP_STATS</c> and <c>HEAVIEST_COLUMNS</c> in server-tabs.js).
/// The data is <see cref="ViewerDataService.GetSweepPressureAsync"/>; this class only words it.
/// </summary>
internal static class SweepPressurePanel
{
    /// <summary>The web panels' subtitle: both read the trailing seven days, whatever the toolbar window is.</summary>
    internal const string WindowText = "trailing 7 days";

    /// <summary>The Heaviest Collectors empty state, the web's own sentence.</summary>
    internal const string NoTimings = "No per-collector timings recorded yet.";

    /// <summary>What both panels say when the server has no collection log rows in the window (the web answers "unavailable" then).</summary>
    internal const string NoLogRows = "No collection log rows for this server yet.";

    /// <summary>What both panels say when their read failed: nothing is known, which is not the all-clear.</summary>
    internal const string Unreadable = "The sweep pressure read could not be completed.";

    /// <summary>The Sweep Pressure panel's lines, in the web's order (<c>SWEEP_STATS</c>).</summary>
    internal static List<SweepStatRow> StatRows(SweepPressure pressure)
    {
        ArgumentNullException.ThrowIfNull(pressure);
        return
        [
            new("Verdict", pressure.Verdict),
            new("Sweep busy %", FormatNumber(Math.Round(pressure.BusyPercent, 1))),
            new("Busy ms/min", FormatInteger(Math.Round(pressure.BusyMsPerMinute, 0))),
            new("Peak cycle", FormatMs(Math.Round(pressure.PeakCycleMs, 0))),
            new("Peak cycle %", FormatNumber(Math.Round(pressure.PeakCyclePercent, 1))),
            new("Peak risk", pressure.PeakCycleRisk),
        ];
    }

    /// <summary>The Heaviest Collectors grid's rows, worst first, as the report ranks them.</summary>
    internal static List<HeaviestCollectorGridRow> GridRows(SweepPressureReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return report.Heaviest.Select(r => new HeaviestCollectorGridRow(r)).ToList();
    }

    /// <summary>The web's one-decimal number (<c>num1</c>).</summary>
    internal static string FormatNumber(double value) =>
        value.ToString("N1", CultureInfo.CurrentCulture);

    /// <summary>The web's whole number (<c>int</c>).</summary>
    internal static string FormatInteger(double value) =>
        value.ToString("N0", CultureInfo.CurrentCulture);

    /// <summary>The web's duration (<c>ms</c>, util.js <c>fmtMs</c>): milliseconds under a second, then seconds, minutes and hours, one decimal.</summary>
    internal static string FormatMs(double value)
    {
        var abs = Math.Abs(value);
        if (abs < 1000)
        {
            return value.ToString("N0", CultureInfo.CurrentCulture) + " ms";
        }

        if (abs < 60_000)
        {
            return (value / 1000).ToString("N1", CultureInfo.CurrentCulture) + " s";
        }

        if (abs < 3_600_000)
        {
            return (value / 60_000).ToString("N1", CultureInfo.CurrentCulture) + " min";
        }

        return (value / 3_600_000).ToString("N1", CultureInfo.CurrentCulture) + " h";
    }
}
