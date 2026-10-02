/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Hue / saturation / value in the ranges a slider wants — hue in degrees [0, 360), saturation and value
/// in [0, 1] — with the two conversions the color picker in <see cref="ThemeColorsPanel"/> needs (#3577).
///
/// <para>WPF ships no color picker, and the picker here is deliberately the smallest honest one: three
/// sliders over this model plus a live swatch, opened from the row's button, writing hex back into the
/// row's text box. A hue wheel, an eyedropper or a saved-swatches strip would each be more control than
/// the twelve rows justify; the text box takes any <c>#RRGGBB</c> an operator brings from elsewhere.</para>
/// </summary>
public readonly record struct HsvColor(double Hue, double Saturation, double Value)
{
    /// <summary>The HSV decomposition of an RGB color; alpha is dropped. A grey has hue 0 by convention.</summary>
    public static HsvColor FromColor(Color color)
    {
        var r = color.R / 255.0;
        var g = color.G / 255.0;
        var b = color.B / 255.0;

        var max = Math.Max(r, Math.Max(g, b));
        var min = Math.Min(r, Math.Min(g, b));
        var delta = max - min;

        double hue;
        if (delta <= 0)
        {
            hue = 0;
        }
        else if (max == r)
        {
            hue = 60 * (((g - b) / delta) % 6);
        }
        else if (max == g)
        {
            hue = 60 * (((b - r) / delta) + 2);
        }
        else
        {
            hue = 60 * (((r - g) / delta) + 4);
        }

        if (hue < 0)
        {
            hue += 360;
        }

        var saturation = max <= 0 ? 0 : delta / max;
        return new HsvColor(hue, saturation, max);
    }

    /// <summary>The opaque RGB color for this HSV triple. Out-of-range inputs are clamped, not thrown.</summary>
    public Color ToColor()
    {
        var h = Hue % 360;
        if (h < 0)
        {
            h += 360;
        }

        var s = Math.Clamp(Saturation, 0, 1);
        var v = Math.Clamp(Value, 0, 1);

        var c = v * s;
        var x = c * (1 - Math.Abs(((h / 60) % 2) - 1));
        var m = v - c;

        double r, g, b;
        if (h < 60) { r = c; g = x; b = 0; }
        else if (h < 120) { r = x; g = c; b = 0; }
        else if (h < 180) { r = 0; g = c; b = x; }
        else if (h < 240) { r = 0; g = x; b = c; }
        else if (h < 300) { r = x; g = 0; b = c; }
        else { r = c; g = 0; b = x; }

        return Color.FromRgb(Channel(r + m), Channel(g + m), Channel(b + m));
    }

    private static byte Channel(double unit) => (byte)Math.Clamp(Math.Round(unit * 255), 0, 255);
}
