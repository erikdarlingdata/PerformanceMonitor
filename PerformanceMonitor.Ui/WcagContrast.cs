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
/// Where a measured contrast ratio lands against the WCAG 2.x text thresholds. The bands are the three
/// colors the Settings window's Colors section paints its readout in (#3577): green for a pair that clears
/// AA text (4.5:1), amber for one that clears only the large-text / non-text mark (3:1), red for one that
/// clears neither.
/// </summary>
public enum ContrastBand
{
    /// <summary>Below 3:1 — fails even the non-text marker target.</summary>
    Fail,

    /// <summary>3:1 up to (but not including) 4.5:1 — legible as a marker or large text, not as body text.</summary>
    Marginal,

    /// <summary>4.5:1 or better — WCAG AA for text.</summary>
    Pass,
}

/// <summary>
/// WCAG 2.x relative luminance and contrast ratio, as pure functions over WPF <see cref="Color"/>s.
///
/// <para>Arm (A) of #3577 (PR #3589) measured every fix it shipped with a twenty-line Python script and kept
/// the script out of the tree to keep that diff to the defect. Arm (B) — user-maintainable colors — needs
/// the same arithmetic INSIDE the app, because the whole point of the readout beside each color row is
/// that the operator sees the ratio they are about to ship before they ship it. So the script's math lives
/// here now, once, and the tests pin it to the numbers the arm-A PR body published (2.71:1 for the Cool
/// Breeze tab defect, 5.39:1 for its fix, 1.99 → 7.52 for Dark's) so the two arms cannot disagree about
/// what a pair measures.</para>
///
/// <para>The formula is the one in WCAG 2.1 §1.4.3 / the "relative luminance" definition: each sRGB channel
/// is linearised (c/12.92 below 0.03928, else ((c+0.055)/1.055)^2.4), the luminance is the 0.2126 / 0.7152
/// / 0.0722 weighted sum, and the ratio is (L1 + 0.05) / (L2 + 0.05) with the lighter color on top. Alpha
/// is ignored: the ratio is defined for opaque colors, and the twelve keys this feature exposes are all
/// painted opaque (the translucent row-mark brushes from arm (A) are derived from them, not among them).</para>
/// </summary>
public static class WcagContrast
{
    /// <summary>The WCAG AA minimum for ordinary text.</summary>
    public const double TextMinimum = 4.5;

    /// <summary>The WCAG AA minimum for large text and non-text markers (borders, icons, row tints).</summary>
    public const double MarkerMinimum = 3.0;

    /// <summary>Relative luminance of <paramref name="color"/> in [0, 1], alpha ignored.</summary>
    public static double RelativeLuminance(Color color)
    {
        return 0.2126 * Linearise(color.R) + 0.7152 * Linearise(color.G) + 0.0722 * Linearise(color.B);
    }

    /// <summary>
    /// The contrast ratio between two colors, in [1, 21]. Order does not matter — the lighter color is
    /// always the numerator.
    /// </summary>
    public static double Ratio(Color a, Color b)
    {
        var la = RelativeLuminance(a);
        var lb = RelativeLuminance(b);
        var lighter = Math.Max(la, lb);
        var darker = Math.Min(la, lb);
        return (lighter + 0.05) / (darker + 0.05);
    }

    /// <summary>Which of the three readout bands a ratio falls in.</summary>
    public static ContrastBand Band(double ratio)
    {
        if (ratio >= TextMinimum)
        {
            return ContrastBand.Pass;
        }

        return ratio >= MarkerMinimum ? ContrastBand.Marginal : ContrastBand.Fail;
    }

    /// <summary>
    /// The ratio the way the arm-A tables and the Settings readout print it: two decimals and a ":1", so
    /// "5.39:1". Two decimals because that is the precision the published numbers carry; one would round
    /// 4.56 (Cool Breeze's hover ink) to 4.6 and hide how close to the line it sits.
    /// </summary>
    public static string Format(double ratio) =>
        ratio.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture) + ":1";

    private static double Linearise(byte channel)
    {
        var c = channel / 255.0;
        return c <= 0.03928 ? c / 12.92 : Math.Pow((c + 0.055) / 1.055, 2.4);
    }
}
