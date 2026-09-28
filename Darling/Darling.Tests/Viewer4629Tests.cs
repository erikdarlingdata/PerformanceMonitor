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
using System.Windows.Media;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4629 — the plan viewer's orange text (the root node's "N warnings" badge, the missing-index
/// impact %, the per-thread skew text, and the node-level cost/elapsed/CPU/row-estimate text) used
/// three fixed brushes that never changed with the theme: <c>OrangeBrush</c> (#FFB347, 1.78:1 on
/// Light's white), <c>Brushes.Orange</c> (#FFA500, 1.97:1) and <c>Brushes.OrangeRed</c> (#FF4500,
/// 3.44:1 on Light, 4.46:1 on Dark's node background) — all under WCAG AA's 4.5:1 floor for text.
///
/// <para>The fix reuses the existing theme-token <c>WarningBrush</c> for the "warning" tier (badge,
/// impact %, skew, cost 25-49%) and adds one new theme token, <c>PlanCriticalOrangeBrush</c>, for the
/// "critical" tier (cost &gt;= 50%, elapsed/CPU &gt;= 1s, row estimate off by 10x+) — still orange,
/// still one tier more severe than the warning brush, tuned per theme. This reads every theme's real
/// declared hex, the same way <c>ThemeStatusContrastTests</c> (#3609) does, and measures with the
/// shared <see cref="WcagContrast"/> helper, so the pin and any future Settings readout cannot
/// disagree about a ratio.</para>
///
/// <para>Only Light and Dark are held to the 4.5:1 floor here, matching the issue's own two named
/// themes. Cool Breeze gets an existence-only check: its own <c>WarningColor</c> (unchanged by this
/// fix, pre-existing) measures 4.41:1 against the properties panel background — a hair under the
/// floor the issue didn't ask this PR to also close.</para>
/// </summary>
public class Viewer4629Tests
{
    // The node card background (PlanViewerControl.Rendering.cs BuildNode: FindResource("BackgroundLightBrush")) —
    // what the badge and every node-level orange text (cost/elapsed/CPU/row estimate) actually sits on.
    private const string NodeBackgroundKey = "BackgroundLightColor";

    // The Properties panel's own background (PlanViewerControl.xaml PropertiesPanel Border:
    // DynamicResource BackgroundDarkBrush) — what the skew text and missing-index impact % sit on.
    private const string PanelBackgroundKey = "BackgroundDarkColor";

    private static readonly string[] LightDarkFiles =
    {
        "Lite/Themes/LightTheme.xaml",
        "Lite/Themes/DarkTheme.xaml",
        "deprecated/Dashboard/Themes/LightTheme.xaml",
        "deprecated/Dashboard/Themes/DarkTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/LightTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/DarkTheme.xaml",
    };

    private static readonly string[] AllNineFiles =
    {
        "Lite/Themes/LightTheme.xaml",
        "Lite/Themes/DarkTheme.xaml",
        "Lite/Themes/CoolBreezeTheme.xaml",
        "deprecated/Dashboard/Themes/LightTheme.xaml",
        "deprecated/Dashboard/Themes/DarkTheme.xaml",
        "deprecated/Dashboard/Themes/CoolBreezeTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/LightTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/DarkTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/CoolBreezeTheme.xaml",
    };

    public static IEnumerable<object[]> LightDarkThemeFiles() =>
        LightDarkFiles.Select(f => new object[] { f });

    public static IEnumerable<object[]> EveryThemeFile() =>
        AllNineFiles.Select(f => new object[] { f });

    [Theory]
    [MemberData(nameof(LightDarkThemeFiles))]
    public void WarningBrush_ReadsAsTextOnNodeAndPanelBackgrounds(string relativePath)
    {
        var declared = ThemeXamlRewriter.DeclaredColors(ReadRepoFile(relativePath));
        AssertClearsTextFloor(relativePath, declared, "WarningColor", NodeBackgroundKey);
        AssertClearsTextFloor(relativePath, declared, "WarningColor", PanelBackgroundKey);
    }

    [Theory]
    [MemberData(nameof(LightDarkThemeFiles))]
    public void PlanCriticalOrangeBrush_ReadsAsTextOnNodeAndPanelBackgrounds(string relativePath)
    {
        var declared = ThemeXamlRewriter.DeclaredColors(ReadRepoFile(relativePath));
        AssertClearsTextFloor(relativePath, declared, "PlanCriticalOrangeColor", NodeBackgroundKey);
        AssertClearsTextFloor(relativePath, declared, "PlanCriticalOrangeColor", PanelBackgroundKey);
    }

    /// <summary>Golden pin: every theme family's <c>PlanCriticalOrangeColor</c> value, so a future edit is deliberate.</summary>
    [Theory]
    [InlineData("Lite/Themes/LightTheme.xaml", "#A83A0D")]
    [InlineData("Lite/Themes/DarkTheme.xaml", "#FF7043")]
    [InlineData("Lite/Themes/CoolBreezeTheme.xaml", "#A83A0D")]
    [InlineData("deprecated/Dashboard/Themes/LightTheme.xaml", "#A83A0D")]
    [InlineData("deprecated/Dashboard/Themes/DarkTheme.xaml", "#FF7043")]
    [InlineData("deprecated/Dashboard/Themes/CoolBreezeTheme.xaml", "#A83A0D")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/LightTheme.xaml", "#A83A0D")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/DarkTheme.xaml", "#FF7043")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/CoolBreezeTheme.xaml", "#A83A0D")]
    public void PlanCriticalOrangeColor_MatchesPinnedHex(string relativePath, string expectedHex)
    {
        var declared = ThemeXamlRewriter.DeclaredColors(ReadRepoFile(relativePath));
        Assert.True(declared.TryGetValue("PlanCriticalOrangeColor", out var hex), $"{relativePath}: no PlanCriticalOrangeColor.");
        Assert.Equal(expectedHex, hex, ignoreCase: true);
    }

    /// <summary>
    /// Every theme this control's host apps ship — including Cool Breeze, which the strict floor tests
    /// above skip — must at least declare both keys, so <c>FindResource</c> can never throw for a
    /// missing key regardless of which theme is active.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryThemeFile))]
    public void EveryTheme_DeclaresBothOrangeKeys(string relativePath)
    {
        var declared = ThemeXamlRewriter.DeclaredColors(ReadRepoFile(relativePath));
        Assert.True(declared.ContainsKey("WarningColor"), $"{relativePath}: no WarningColor.");
        Assert.True(declared.ContainsKey("PlanCriticalOrangeColor"), $"{relativePath}: no PlanCriticalOrangeColor.");
    }

    private static void AssertClearsTextFloor(string relativePath, IReadOnlyDictionary<string, string> declared, string colorKey, string backgroundKey)
    {
        Assert.True(declared.TryGetValue(colorKey, out var fgHex), $"{relativePath}: no {colorKey}.");
        Assert.True(declared.TryGetValue(backgroundKey, out var bgHex), $"{relativePath}: no {backgroundKey}.");
        Assert.True(ThemeColorOverrides.TryParseHex(fgHex, out var fg), $"{relativePath}: {colorKey} = '{fgHex}' is not a hex color.");
        Assert.True(ThemeColorOverrides.TryParseHex(bgHex, out var bg), $"{relativePath}: {backgroundKey} = '{bgHex}' is not a hex color.");

        var ratio = WcagContrast.Ratio(fg, bg);
        Assert.True(ratio >= WcagContrast.TextMinimum,
            $"{relativePath}: {colorKey} {fgHex} on {backgroundKey} {bgHex} is {WcagContrast.Format(ratio)}, below the {WcagContrast.TextMinimum}:1 text floor (#4629).");
    }

    private static string ReadRepoFile(string relativePath) =>
        File.ReadAllText(Path.Combine(RepoRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar)));

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
