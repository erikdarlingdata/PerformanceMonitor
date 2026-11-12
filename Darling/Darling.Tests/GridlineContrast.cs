/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace Darling.Tests;

/// <summary>Samples a rendered plot for its background and gridline colours and gives each one's contrast ratio against the other.</summary>
internal static class GridlineContrast
{
    /// <summary>Renders <paramref name="plot"/> with fixed limits, scans one row of its data area between two horizontal gridlines, and takes the
    /// commonest colour as the plot background and the colour farthest from it in luminance as the gridline.</summary>
    public static (double Grid, double Text, string Background, string Line) Measure(ScottPlot.Plot plot, ScottPlot.Color text)
    {
        plot.Axes.SetLimits(0, 10, 0, 10);
        var image = plot.GetImage(800, 400);
        var rgb = image.GetArrayRGB();
        var rect = plot.RenderManager.LastRender.DataRect;
        var y = (int)(rect.Top + rect.Height * 0.37);
        var counts = new Dictionary<int, int>();
        for (var x = (int)rect.Left + 2; x < (int)rect.Right - 2; x++)
        {
            var key = (rgb[y, x, 0] << 16) | (rgb[y, x, 1] << 8) | rgb[y, x, 2];
            counts[key] = counts.GetValueOrDefault(key) + 1;
        }

        var background = counts.OrderByDescending(kv => kv.Value).First().Key;
        var backgroundLuminance = Luminance(background);
        var line = counts.Keys.OrderByDescending(k => Math.Abs(Luminance(k) - backgroundLuminance)).First();
        var textKey = (text.R << 16) | (text.G << 8) | text.B;
        return (Ratio(backgroundLuminance, Luminance(line)), Ratio(backgroundLuminance, Luminance(textKey)), Hex(background), Hex(line));
    }

    private static string Hex(int rgb) => $"#{rgb:X6}";

    private static double Ratio(double a, double b) => (Math.Max(a, b) + 0.05) / (Math.Min(a, b) + 0.05);

    private static double Luminance(int rgb)
    {
        static double Channel(int v)
        {
            var c = v / 255.0;
            return c <= 0.03928 ? c / 12.92 : Math.Pow((c + 0.055) / 1.055, 2.4);
        }

        return 0.2126 * Channel((rgb >> 16) & 0xFF) + 0.7152 * Channel((rgb >> 8) & 0xFF) + 0.0722 * Channel(rgb & 0xFF);
    }
}
