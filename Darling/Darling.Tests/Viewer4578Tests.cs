using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4578: the wait-category chart colours port PerformanceStudio dev's contrast-checked palette
/// (erikdarlingdata/PerformanceStudio@161785f) into <see cref="ChartPalette.WaitColor"/> byte for
/// byte. Golden pin on every category's hex, plus a WCAG contrast pin against the dark chart
/// background PS measured against.
/// </summary>
public class Viewer4578Tests
{
    // PS DarkTheme.axaml's WaitCategory.* brushes at 161785f (unchanged through PS dev 85492a1).
    private static readonly Dictionary<string, string> ExpectedDarkHex =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["Unknown"] = "#9CA1A5",
            ["CPU"] = "#3EBD50",
            ["Worker Thread"] = "#72E1A9",
            ["Lock"] = "#FB6640",
            ["Latch"] = "#FEB5ED",
            ["Buffer Latch"] = "#E8469D",
            ["Buffer IO"] = "#238CFA",
            ["Compilation"] = "#A1A3FB",
            ["SQL CLR"] = "#63C5C6",
            ["Mirroring"] = "#30907E",
            ["Transaction"] = "#DB9027",
            ["Preemptive"] = "#FFA8C0",
            ["Service Broker"] = "#FF9289",
            ["Tran Log IO"] = "#8AC6FF",
            ["Network IO"] = "#BBE2FE",
            ["Parallelism"] = "#9A84FE",
            ["Memory"] = "#FFE6B1",
            ["Tracing"] = "#BDC8CE",
            ["Full Text Search"] = "#D8F2FC",
            ["Other Disk IO"] = "#0FB3E2",
            ["Replication"] = "#5DF9ED",
            ["Log Rate Governor"] = "#FFCACA",
            ["Others"] = "#676E73",
            // PM-ahead: PS's ~161785f diff shows no "Batch Mode" category (PM classifies batch-mode
            // hash/bitmap waits separately; PS folds them elsewhere). Not part of the port, kept as-is.
        };

    // PS's plan-viewer background brush (BackgroundBrush, DarkTheme.axaml) that PS measured
    // 3:1 contrast against when laying the palette out.
    private const string DarkChartBackground = "#1A1D23";

    [Theory]
    [MemberData(nameof(CategoryNames))]
    public void WaitColor_MatchesPerformanceStudioDarkPalette(string category)
    {
        var expected = ExpectedDarkHex[category];
        var actual = ChartPalette.WaitColor(category, lightBackground: false);

        Assert.Equal(expected, actual, ignoreCase: true);
    }

    [Theory]
    [MemberData(nameof(CategoryNames))]
    public void WaitColor_ClearsWcagContrastFloorAgainstDarkChartBackground(string category)
    {
        var hex = ChartPalette.WaitColor(category, lightBackground: false);

        var ratio = ContrastRatio(hex, DarkChartBackground);

        Assert.True(ratio >= 3.0, $"{category} = {hex} contrast {ratio:F2} against {DarkChartBackground}, below the 3:1 floor");
    }

    public static IEnumerable<object[]> CategoryNames()
    {
        foreach (var name in ExpectedDarkHex.Keys)
            yield return new object[] { name };
    }

    // WCAG 2.x relative-luminance contrast ratio (sRGB).
    private static double ContrastRatio(string hexA, string hexB)
    {
        var lumA = RelativeLuminance(hexA);
        var lumB = RelativeLuminance(hexB);
        var lighter = Math.Max(lumA, lumB);
        var darker = Math.Min(lumA, lumB);
        return (lighter + 0.05) / (darker + 0.05);
    }

    private static double RelativeLuminance(string hex)
    {
        hex = hex.TrimStart('#');
        var r = Convert.ToInt32(hex.Substring(0, 2), 16);
        var g = Convert.ToInt32(hex.Substring(2, 2), 16);
        var b = Convert.ToInt32(hex.Substring(4, 2), 16);

        static double Linearize(int channel)
        {
            var c = channel / 255.0;
            return c <= 0.03928 ? c / 12.92 : Math.Pow((c + 0.055) / 1.055, 2.4);
        }

        return 0.2126 * Linearize(r) + 0.7152 * Linearize(g) + 0.0722 * Linearize(b);
    }
}
