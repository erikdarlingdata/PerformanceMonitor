/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3609: the stock palette must clear the contrast readout's own floor.
///
/// <para>#3606 put a live WCAG ratio beside every palette color in Settings, measured against the surface the
/// color renders on — for the four status colors, <c>BackgroundColor</c>. On a stock Light install that readout
/// found two of its own rows red: <c>WarningColor</c> at 2.47:1 and <c>InfoColor</c> at 2.32:1, both under the
/// 3:1 non-text floor, and Cool Breeze's <c>WarningColor</c> at 2.09:1. Those values were re-derived, and this
/// file is what keeps them there. It reads the six shipped theme files as TEXT — the same
/// <see cref="ThemeXamlRewriter.DeclaredColors"/> the regeneration path reads, so the pin and the readout
/// cannot see different palettes — and measures with <see cref="WcagContrast"/>, so the pin and the readout
/// cannot disagree about a ratio either. No WPF object is constructed; the file runs anywhere the tests build.</para>
///
/// <para>Three facts are pinned, at three different bars, because the colors do three different jobs:</para>
/// <list type="bullet">
/// <item><description><b>Every status color, 3:1 against both page surfaces.</b> That is the readout's
/// <see cref="ContrastBand.Fail"/> line and the WCAG floor for a non-text marker — the status dot in the server
/// list, the calendar cell, the card border. A stock palette that a fresh install's own Settings page paints
/// red is the defect this issue was opened for; the floor is pinned for all four colors so it cannot come back
/// through a different one.</description></item>
/// <item><description><b>The three re-derived colors, 4.5:1 against both page surfaces.</b> <c>WarningBrush</c>
/// and <c>InfoBrush</c> are not only markers — the viewer paints fleet counts, the seat state and the coverage
/// note in <c>WarningBrush</c> as text, and the Settings legend paints Info as text — so the fix had to reach the
/// text bar, not just the marker floor. The card surface (<c>BackgroundLightColor</c>) is included because
/// status text lands there too.</description></item>
/// <item><description><b>The ink on a status fill, 4.5:1 against every status color.</b> Making a status color
/// dark enough to read against the page makes it too dark for the dark text that used to sit on it (the
/// Recommendations severity badge carried a literal <c>#1A1A1A</c>). No single amber satisfies both — 4.5:1 on
/// Light's page needs a luminance at or below 0.17, 4.5:1 under <c>#1A1A1A</c> needs at or above 0.22 — so the
/// ink became a theme key, <c>StatusForegroundBrush</c>, the way #3589 gave the accent
/// <c>AccentForegroundColor</c>. It is a brush with a literal color rather than a nineteenth <c>&lt;Color&gt;</c>
/// because <see cref="ThemeColorOverrideTests"/> pins the palette at exactly eighteen and this is ink, not a
/// palette slot the operator should tune independently of the fills it sits on.</description></item>
/// </list>
/// </summary>
public class ThemeStatusContrastTests
{
    private static readonly string[] StatusColorKeys = { "SuccessColor", "WarningColor", "ErrorColor", "InfoColor" };
    private static readonly string[] PageSurfaceKeys = { "BackgroundColor", "BackgroundLightColor" };

    /// <summary>
    /// <c>&lt;SolidColorBrush x:Key="StatusForegroundBrush" Color="#RRGGBB"/&gt;</c> — a literal, not a
    /// <c>{StaticResource}</c>, for the reason in the class remarks.
    /// </summary>
    private static readonly Regex StatusInkDeclaration = new(
        @"<SolidColorBrush\s+x:Key=""StatusForegroundBrush""\s+Color=""(?<value>#[0-9A-Fa-f]{6,8})""",
        RegexOptions.CultureInvariant);

    public static IEnumerable<object[]> ThemeFiles()
    {
        yield return new object[] { new[] { "Lite", "Themes", "DarkTheme.xaml" } };
        yield return new object[] { new[] { "Lite", "Themes", "LightTheme.xaml" } };
        yield return new object[] { new[] { "Lite", "Themes", "CoolBreezeTheme.xaml" } };
        yield return new object[] { new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "Themes", "DarkTheme.xaml" } };
        yield return new object[] { new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "Themes", "LightTheme.xaml" } };
        yield return new object[] { new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "Themes", "CoolBreezeTheme.xaml" } };
    }

    [Theory]
    [MemberData(nameof(ThemeFiles))]
    public void EveryStatusColor_ClearsTheMarkerFloor_OnBothPageSurfaces(string[] relativePath)
    {
        var palette = Palette(relativePath);
        var file = string.Join("/", relativePath);

        var failures = new List<string>();
        foreach (var status in StatusColorKeys)
        {
            foreach (var surface in PageSurfaceKeys)
            {
                var ratio = WcagContrast.Ratio(palette[status], palette[surface]);
                if (ratio < WcagContrast.MarkerMinimum)
                {
                    failures.Add($"  {status} {ThemeColorOverrides.ToHex(palette[status])} on {surface} {ThemeColorOverrides.ToHex(palette[surface])}: {WcagContrast.Format(ratio)}");
                }
            }
        }

        Assert.True(failures.Count == 0,
            $"{file}: status colors below the {WcagContrast.MarkerMinimum}:1 marker floor the Settings contrast readout paints red (#3609):\n" +
            string.Join("\n", failures));
    }

    /// <summary>
    /// The three values #3609 re-derived, at the bar their TEXT uses need. Dark's are included because they
    /// already clear it (6.9:1 and 12.3:1 against its page) and the pin should say so rather than exempt them.
    /// </summary>
    [Theory]
    [MemberData(nameof(ThemeFiles))]
    public void WarningAndInfo_ReadAsText_OnBothPageSurfaces(string[] relativePath)
    {
        var palette = Palette(relativePath);
        var file = string.Join("/", relativePath);
        var isCoolBreeze = relativePath[^1].StartsWith("CoolBreeze", StringComparison.Ordinal);

        var failures = new List<string>();
        foreach (var status in new[] { "WarningColor", "InfoColor" })
        {
            // Cool Breeze's InfoColor is its AccentColor (#1E6FA8) and measures 4.25:1 on its page — above the
            // marker floor the first fact pins, below this text bar, and NOT one of the values this issue
            // re-derived (the issue named the three under 3:1). It is reported, not hidden: the exemption is
            // spelled here so the day it is fixed the line comes out and the pin tightens.
            if (isCoolBreeze && status == "InfoColor")
            {
                continue;
            }

            foreach (var surface in PageSurfaceKeys)
            {
                var ratio = WcagContrast.Ratio(palette[status], palette[surface]);
                if (ratio < WcagContrast.TextMinimum)
                {
                    failures.Add($"  {status} {ThemeColorOverrides.ToHex(palette[status])} on {surface} {ThemeColorOverrides.ToHex(palette[surface])}: {WcagContrast.Format(ratio)}");
                }
            }
        }

        Assert.True(failures.Count == 0,
            $"{file}: WarningColor / InfoColor are painted as text (viewer fleet counts, seat state, the Settings legend) " +
            $"and must clear {WcagContrast.TextMinimum}:1 (#3609):\n" + string.Join("\n", failures));
    }

    [Theory]
    [MemberData(nameof(ThemeFiles))]
    public void TheStatusInk_ReadsAsText_OnEveryStatusFill(string[] relativePath)
    {
        var xaml = ReadRepoFile(relativePath);
        var file = string.Join("/", relativePath);

        var match = StatusInkDeclaration.Match(xaml);
        Assert.True(match.Success,
            $"{file}: no <SolidColorBrush x:Key=\"StatusForegroundBrush\" Color=\"#…\"/> — the ink the severity badges " +
            "paint their text in (#3609). It must be a literal, not a {StaticResource}: see the class remarks.");
        Assert.True(ThemeColorOverrides.TryParseHex(match.Groups["value"].Value, out var ink), $"{file}: StatusForegroundBrush is not a hex color.");

        var palette = Palette(relativePath);
        var failures = new List<string>();
        foreach (var status in StatusColorKeys)
        {
            var ratio = WcagContrast.Ratio(ink, palette[status]);
            if (ratio < WcagContrast.TextMinimum)
            {
                failures.Add($"  {ThemeColorOverrides.ToHex(ink)} on {status} {ThemeColorOverrides.ToHex(palette[status])}: {WcagContrast.Format(ratio)}");
            }
        }

        Assert.True(failures.Count == 0,
            $"{file}: StatusForegroundBrush is the text on the Recommendations severity badge and the viewer's alert badges " +
            $"and must clear {WcagContrast.TextMinimum}:1 on every status fill (#3609):\n" + string.Join("\n", failures));
    }

    /// <summary>
    /// The three consumers spell the key the way the themes declare it. A DynamicResource to a key no theme
    /// defines paints nothing and throws nothing — the badge text would simply vanish.
    /// </summary>
    [Theory]
    [InlineData("Lite", "Controls", "RecommendationsTab.xaml")]
    [InlineData("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml")]
    [InlineData("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.AlertBadges.cs")]
    public void TheBadges_PaintTheirText_WithTheStatusInkKey(string first, string second, string third)
    {
        var relativePath = new[] { first, second, third };
        var text = ReadRepoFile(relativePath);
        var file = string.Join("/", relativePath);

        Assert.Contains("StatusForegroundBrush", text);
        Assert.DoesNotContain("Foreground=\"#1A1A1A\"", text);
        Assert.False(Regex.IsMatch(text, @"Foreground\s*=\s*(""White""|Brushes\.White)"),
            $"{file}: a badge still carries a hard-coded White ink; on Dark's amber that measured 1.41:1 (#3609).");
    }

    /// <summary>
    /// The four status colors and two page surfaces of a theme file, parsed exactly as the regeneration path
    /// parses them (<see cref="ThemeXamlRewriter.DeclaredColors"/> → <see cref="ThemeColorOverrides.TryParseHex"/>).
    /// </summary>
    private static Dictionary<string, Color> Palette(string[] relativePath)
    {
        var declared = ThemeXamlRewriter.DeclaredColors(ReadRepoFile(relativePath));
        var palette = new Dictionary<string, Color>(StringComparer.Ordinal);
        foreach (var key in StatusColorKeys.Concat(PageSurfaceKeys))
        {
            Assert.True(declared.TryGetValue(key, out var hex), $"{string.Join("/", relativePath)}: no <Color x:Key=\"{key}\">.");
            Assert.True(ThemeColorOverrides.TryParseHex(hex, out var color), $"{string.Join("/", relativePath)}: {key} = '{hex}' is not a hex color.");
            palette[key] = color;
        }

        return palette;
    }

    private static string ReadRepoFile(string[] relativePath) =>
        File.ReadAllText(Path.Combine(new[] { RepoRoot() }.Concat(relativePath).ToArray()));

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
