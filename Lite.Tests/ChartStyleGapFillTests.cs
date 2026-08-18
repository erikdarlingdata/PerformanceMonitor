/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The #2324 field regression: a series carrying #1944's injected NaN gap markers must never take the
/// gradient area fill. ScottPlot 5.1.59's <c>Scatter.Render</c> builds the fill's clip/gradient rect as
/// <c>new PixelRect(linePixels)</c> over pixels that still contain the NaN markers — the rect is
/// NaN-poisoned, the axis-gradient shader built for it is invalid, and Skia falls back to the base fill
/// paint, which ScottPlot hardcodes to <c>Colors.Black</c> whenever <c>ColorPositions</c> is in use. On
/// 3.4.0 (the first release carrying gap markers) every gapped chart rendered its ribbon as an opaque
/// black polygon burying the other series; the reporter's one healthy tab was the one whose data had no
/// gaps. Lines and markers handle NaN contours fine, so line-only is the honest degradation for a gapped
/// series — and a gapless series must KEEP the ribbon, or the fix would quietly repeal the feature.
/// </summary>
public class ChartStyleGapFillTests
{
    [Fact]
    public void GapMarkedSeries_GetsNoFill_LineOnly()
    {
        var plot = new ScottPlot.Plot();
        // The #1944 shape: a real gap carries a mid-gap point with a real X and a NaN Y.
        var sc = plot.Add.Scatter(
            new double[] { 1, 2, 3, 4, 5 },
            new double[] { 0, 5, double.NaN, 7, 10 });
        sc.Color = ScottPlot.Color.FromHex("#4E79A7");

        ChartStyle.StyleScatter(sc);

        Assert.False(sc.FillY);
        Assert.Empty(sc.ColorPositions);
        Assert.Equal(2f, sc.LineWidth);   // still styled as a line — only the ribbon is withheld
    }

    [Fact]
    public void GaplessSeries_KeepsTheGradientRibbon()
    {
        var plot = new ScottPlot.Plot();
        var sc = plot.Add.Scatter(
            new double[] { 1, 2, 3, 4, 5 },
            new double[] { 0, 5, 6, 7, 10 });
        sc.Color = ScottPlot.Color.FromHex("#4E79A7");

        ChartStyle.StyleScatter(sc);

        Assert.True(sc.FillY);
        Assert.Equal(2, sc.ColorPositions.Count);
    }
}
