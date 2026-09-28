/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
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
/// impact %, skew, cost 25-49%) and adds one new theme token for the "critical" tier (cost &gt;= 50%,
/// elapsed/CPU &gt;= 1s, row estimate off by 10x+) — still orange, still one tier more severe than the
/// warning brush, tuned per theme. That token is <c>CriticalTextBrush</c> (#4632: renamed from the
/// original <c>PlanCriticalOrangeColor</c>/<c>PlanCriticalOrangeBrush</c> pair) — a literal-hex
/// <c>SolidColorBrush</c> with no backing <c>&lt;Color&gt;</c> key, the same shape as
/// <c>WarningTextBrush</c>, so it sits outside the eighteen-key user-overridable palette
/// <c>ThemeColorOverrideTests</c> pins. This reads every theme's real declared hex, the same way
/// <c>ThemeStatusContrastTests</c> (#3609) does, and measures with the shared
/// <see cref="WcagContrast"/> helper, so the pin and any future Settings readout cannot disagree
/// about a ratio.</para>
///
/// <para>Lite and the Darling Viewer only. The deprecated Dashboard's three theme files do not carry
/// <c>CriticalTextBrush</c> — out of scope, #4632 — so <c>PlanViewerControl</c> falls back to
/// <c>Brushes.OrangeRed</c> there instead of a per-theme shade.</para>
///
/// <para>Only Light and Dark are held to the 4.5:1 floor here, matching the issue's own two named
/// themes. Cool Breeze gets an existence-only check: its own <c>WarningColor</c> (unchanged by this
/// fix, pre-existing) measures 4.41:1 against the properties panel background — a hair under the
/// floor the issue didn't ask this PR to also close.</para>
///
/// <para>#4635 added a fourth background to the floor: the Lite and Darling Viewer status bar's
/// <c>CollectorHealthText</c>, which also takes <c>CriticalTextBrush</c> when collectors are
/// erroring or capture is down. Both apps' status bar sits directly on <c>BackgroundLightColor</c>
/// with nothing between it and the text — the same key already checked as
/// <see cref="NodeBackgroundKey"/>, kept as its own <see cref="StatusBarBackgroundKey"/> constant and
/// assertion rather than left to that coincidence. No per-theme value changed for #4635: the existing
/// #4629 hex already clears this background too.</para>
/// </summary>
public class Viewer4629Tests
{
    // The node card background (PlanViewerControl.Rendering.cs BuildNode: FindResource("BackgroundLightBrush")) —
    // what the badge and every node-level orange text (cost/elapsed/CPU/row estimate) actually sits on.
    private const string NodeBackgroundKey = "BackgroundLightColor";

    // The Properties panel's own background (PlanViewerControl.xaml PropertiesPanel Border:
    // DynamicResource BackgroundDarkBrush) — what the skew text and missing-index impact % sit on.
    private const string PanelBackgroundKey = "BackgroundDarkColor";

    // #4635: Lite's status bar (MainWindow.xaml Border Grid.Row="2": Background="{DynamicResource
    // BackgroundLightBrush}") and the Darling Viewer's status row, which sets no Background of its own and so
    // shows through to the window's (MainWindow.xaml Window: Background="{DynamicResource
    // BackgroundLightBrush}") — what CollectorHealthText sits on when it takes CriticalTextBrush for
    // "N erroring" / "Capture down". Equal to NodeBackgroundKey today; its own constant and assertion so a
    // future change to either resource is caught on its own.
    private const string StatusBarBackgroundKey = "BackgroundLightColor";

    private static readonly string[] LightDarkFiles =
    {
        "Lite/Themes/LightTheme.xaml",
        "Lite/Themes/DarkTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/LightTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/DarkTheme.xaml",
    };

    // #4632: Lite and the Darling Viewer only, three themes each. The deprecated Dashboard's theme files are
    // out of scope (see the class remarks) and do not carry CriticalTextBrush.
    private static readonly string[] AllSixFiles =
    {
        "Lite/Themes/LightTheme.xaml",
        "Lite/Themes/DarkTheme.xaml",
        "Lite/Themes/CoolBreezeTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/LightTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/DarkTheme.xaml",
        "Darling/PerformanceMonitor.Darling.Viewer/Themes/CoolBreezeTheme.xaml",
    };

    public static IEnumerable<object[]> LightDarkThemeFiles() =>
        LightDarkFiles.Select(f => new object[] { f });

    public static IEnumerable<object[]> EveryThemeFile() =>
        AllSixFiles.Select(f => new object[] { f });

    [Theory]
    [MemberData(nameof(LightDarkThemeFiles))]
    public void WarningBrush_ReadsAsTextOnNodeAndPanelBackgrounds(string relativePath)
    {
        var declared = ThemeXamlRewriter.DeclaredColors(RepoFile.ReadRepoFile(relativePath));
        AssertClearsTextFloor(relativePath, declared, "WarningColor", NodeBackgroundKey);
        AssertClearsTextFloor(relativePath, declared, "WarningColor", PanelBackgroundKey);
    }

    /// <summary>
    /// CriticalTextBrush is a literal-hex SolidColorBrush with no backing &lt;Color&gt; key (same shape as
    /// WarningTextBrush — #4632), so it is read with <see cref="LiteralBrushHex"/> instead of
    /// <c>ThemeXamlRewriter.DeclaredColors</c>, which only sees &lt;Color&gt; declarations.
    /// </summary>
    [Theory]
    [MemberData(nameof(LightDarkThemeFiles))]
    public void CriticalTextBrush_ReadsAsTextOnNodePanelAndStatusBarBackgrounds(string relativePath)
    {
        var xaml = RepoFile.ReadRepoFile(relativePath);
        var declared = ThemeXamlRewriter.DeclaredColors(xaml);
        var fgHex = LiteralBrushHex(xaml, "CriticalTextBrush");
        Assert.True(fgHex is not null, $"{relativePath}: no CriticalTextBrush.");

        AssertClearsTextFloor(relativePath, "CriticalTextBrush", fgHex!, declared, NodeBackgroundKey);
        AssertClearsTextFloor(relativePath, "CriticalTextBrush", fgHex!, declared, PanelBackgroundKey);
        AssertClearsTextFloor(relativePath, "CriticalTextBrush", fgHex!, declared, StatusBarBackgroundKey);
    }

    /// <summary>Golden pin: every theme family's <c>CriticalTextBrush</c> value, so a future edit is deliberate.</summary>
    [Theory]
    [InlineData("Lite/Themes/LightTheme.xaml", "#A83A0D")]
    [InlineData("Lite/Themes/DarkTheme.xaml", "#FF7043")]
    [InlineData("Lite/Themes/CoolBreezeTheme.xaml", "#A83A0D")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/LightTheme.xaml", "#A83A0D")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/DarkTheme.xaml", "#FF7043")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/CoolBreezeTheme.xaml", "#A83A0D")]
    public void CriticalTextBrush_MatchesPinnedHex(string relativePath, string expectedHex)
    {
        var hex = LiteralBrushHex(RepoFile.ReadRepoFile(relativePath), "CriticalTextBrush");
        Assert.True(hex is not null, $"{relativePath}: no CriticalTextBrush.");
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
        var xaml = RepoFile.ReadRepoFile(relativePath);
        Assert.True(ThemeXamlRewriter.DeclaredColors(xaml).ContainsKey("WarningColor"), $"{relativePath}: no WarningColor.");
        Assert.True(LiteralBrushHex(xaml, "CriticalTextBrush") is not null, $"{relativePath}: no CriticalTextBrush.");
    }

    private static void AssertClearsTextFloor(string relativePath, IReadOnlyDictionary<string, string> declared, string colorKey, string backgroundKey)
    {
        Assert.True(declared.TryGetValue(colorKey, out var fgHex), $"{relativePath}: no {colorKey}.");
        AssertClearsTextFloor(relativePath, colorKey, fgHex, declared, backgroundKey);
    }

    /// <summary>Overload for a foreground hex already in hand — CriticalTextBrush is a literal hex, not a
    /// &lt;Color&gt; key <c>DeclaredColors</c> can look up.</summary>
    private static void AssertClearsTextFloor(string relativePath, string fgLabel, string fgHex, IReadOnlyDictionary<string, string> declared, string backgroundKey)
    {
        Assert.True(declared.TryGetValue(backgroundKey, out var bgHex), $"{relativePath}: no {backgroundKey}.");
        Assert.True(ThemeColorOverrides.TryParseHex(fgHex, out var fg), $"{relativePath}: {fgLabel} = '{fgHex}' is not a hex color.");
        Assert.True(ThemeColorOverrides.TryParseHex(bgHex, out var bg), $"{relativePath}: {backgroundKey} = '{bgHex}' is not a hex color.");

        var ratio = WcagContrast.Ratio(fg, bg);
        Assert.True(ratio >= WcagContrast.TextMinimum,
            $"{relativePath}: {fgLabel} {fgHex} on {backgroundKey} {bgHex} is {WcagContrast.Format(ratio)}, below the {WcagContrast.TextMinimum}:1 text floor (#4629).");
    }

    /// <summary>Reads a literal-hex SolidColorBrush's Color attribute directly — for a brush like
    /// CriticalTextBrush or WarningTextBrush that has no backing &lt;Color&gt; key for DeclaredColors to find.</summary>
    private static string? LiteralBrushHex(string xaml, string key)
    {
        var m = Regex.Match(xaml, "<SolidColorBrush\\s+x:Key=\"" + Regex.Escape(key) + "\"\\s+Color=\"(?<hex>#[0-9A-Fa-f]{6,8})\"\\s*/>");
        return m.Success ? m.Groups["hex"].Value : null;
    }

}
