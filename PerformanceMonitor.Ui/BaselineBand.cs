/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Ui;

/// <summary>
/// The shaded normal range (mean plus or minus two standard deviations) the Overview lanes draw behind a series.
/// </summary>
/// <remarks>
/// #7433: the lanes used to call <c>Plot.Add.HorizontalSpan(lower, upper)</c>, and in ScottPlot 5 a HorizontalSpan
/// takes a range on the X axis. <c>lower</c> and <c>upper</c> are Y values, so the band sat at dates near 1899 and
/// never drew; only the dashed mean line showed. A <c>VerticalSpan</c> is the Y-axis range. One helper keeps the six
/// lane call sites (three in the Darling Viewer, three in Lite) from drifting apart again.
/// <para>
/// The span is added BEFORE the lane's series so it paints behind them. It does not move the Y axis: every lane sets
/// its Y limits explicitly after the plottables are added, and the lanes drop all pan, zoom and autoscale input, so a
/// band whose <c>upper</c> is far above the series is clipped at the lane's edge and the series stays readable.
/// </para>
/// </remarks>
public static class BaselineBand
{
    /// <summary>Fill opacity of the band, out of 255. Low enough that the series on top reads through it.</summary>
    public const byte FillAlpha = 40;

    /// <summary>Adds the band (Y range <paramref name="lower"/> to <paramref name="upper"/>) in <paramref name="hexColor"/>.</summary>
    public static ScottPlot.Plottables.VerticalSpan Add(ScottPlot.Plot plot, double lower, double upper, string hexColor)
    {
        var band = plot.Add.VerticalSpan(lower, upper);
        band.FillStyle.Color = ScottPlot.Color.FromHex(hexColor).WithAlpha(FillAlpha);
        band.LineStyle.Width = 0;
        return band;
    }
}
