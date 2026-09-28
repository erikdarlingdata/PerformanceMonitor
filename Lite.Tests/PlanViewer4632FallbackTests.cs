/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4632 LIVE pin: the deprecated Dashboard's three theme files deliberately do NOT declare
/// <c>CriticalTextBrush</c> (out of scope for #4629/#4635 — see <c>Viewer4629Tests</c>'s class remarks in
/// Darling.Tests). <see cref="PlanViewerControl"/> resolves that key in CODE, not a XAML
/// <c>StaticResource</c> binding: <c>(TryFindResource("CriticalTextBrush") as SolidColorBrush) ??
/// Brushes.OrangeRed</c> in <c>PlanViewerControl.xaml.cs</c>. A <c>StaticResource</c> to a missing key
/// throws at load and would take the Dashboard's plan viewer down with it; <c>TryFindResource</c> does not.
///
/// <para><see cref="PlanViewerCapabilityPinTests"/> (both test projects) is a source scan. This is the
/// first test that constructs a LIVE <see cref="PlanViewerControl"/> and proves the fallback actually
/// resolves right — under a theme that lacks the key (Dashboard's real Light theme) and one that has it
/// (Lite's real Light theme).</para>
///
/// <para>Lives in <c>Lite.Tests</c> rather than twinned into <c>Darling.Tests</c>: this proves one
/// code-behind mechanism shared by every host, not a per-app behavior, so one copy is enough —
/// <c>PerformanceMonitor.Ui</c> grants <c>InternalsVisibleTo</c> to both test projects equally, so either
/// could have hosted it. <c>Lite.Tests</c> already carries <c>ParitySource</c> for reading repo source
/// (including the deprecated Dashboard's) as plain text — read-only, no reference to and no build of
/// <c>deprecated/Dashboard</c> — plus the <c>OnStaThread</c> shape <c>MainWindowAccessKeyTests</c>,
/// <c>ThemeColorOverrideTests</c> and <c>QueryWindowTruncationTests</c> already use for constructing WPF
/// objects off-screen.</para>
///
/// <para><c>CriticalOrangeBrush</c> stays <c>private</c>, read through reflection instead of widened — the
/// same shape <c>StoreCopyPhaseLivePostgresTests</c> uses for a private method — so this proof adds zero
/// product-surface change.</para>
///
/// <para>Deliberately NOT attempted: loading a real plan and asserting a critical-tier node's rendered cost
/// text. <see cref="PlanViewerControl.LoadPlan"/> is <c>async</c> and awaits <c>Task.Run</c>; the
/// <c>OnStaThread</c> harness below (like all three precedents above) joins a thread that never pumps a
/// <c>Dispatcher</c> message loop, so that continuation would never return to it — no test in either suite
/// pumps a <c>DispatcherFrame</c> today, and standing that up is a bigger job than this proof needs. The
/// resolved-brush assertions below already pin the exact color value that rendering path would hand a
/// critical-tier node under each scope.</para>
/// </summary>
public sealed class PlanViewer4632FallbackTests
{
    private const string DashboardLightTheme = "deprecated/Dashboard/Themes/LightTheme.xaml";
    private const string LiteLightTheme = "Lite/Themes/LightTheme.xaml";

    /// <summary>OrangeRed, #FF4500 — the pre-#4629 fixed brush <c>PlanViewerControl</c> falls back to when
    /// <c>CriticalTextBrush</c> is not in scope, matching <c>PlanViewerControl.xaml.cs</c>'s own comment
    /// at the fallback line.</summary>
    private static readonly Color OrangeRedHex = Color.FromRgb(0xFF, 0x45, 0x00);

    /// <summary>Lite's Light theme's real <c>CriticalTextBrush</c> hex (#4629) — the value construction
    /// inside that theme's scope must resolve to instead of the fallback.</summary>
    private static readonly Color LiteCriticalOrangeHex = Color.FromRgb(0xA8, 0x3A, 0x0D);

    [Fact]
    public void DashboardScope_MissingCriticalTextBrush_ConstructsCleanly_AndFallsBackToOrangeRed()
    {
        OnStaThread(() =>
        {
            var dict = LoadThemeDictionary(DashboardLightTheme);
            Assert.False(dict.Contains("CriticalTextBrush"),
                "premise check: the Dashboard theme now declares CriticalTextBrush — this pin no longer proves the fallback path.");

            var host = new Border();
            PlanViewerControl? control = null;
            SolidColorBrush? resolved = null;

            var ex = Record.Exception(() =>
            {
                host.Resources.MergedDictionaries.Add(dict);
                control = new PlanViewerControl();
                host.Child = control;
                resolved = CriticalOrangeBrushOf(control);
            });

            Assert.Null(ex);
            Assert.Equal(OrangeRedHex, resolved!.Color);

            control!.Cleanup();
        });
    }

    [Fact]
    public void LiteScope_WithCriticalTextBrush_ConstructsCleanly_AndResolvesThemeHex()
    {
        OnStaThread(() =>
        {
            var dict = LoadThemeDictionary(LiteLightTheme);
            Assert.True(dict.Contains("CriticalTextBrush"),
                "premise check: Lite's Light theme no longer declares CriticalTextBrush.");

            var host = new Border();
            PlanViewerControl? control = null;
            SolidColorBrush? resolved = null;

            var ex = Record.Exception(() =>
            {
                host.Resources.MergedDictionaries.Add(dict);
                control = new PlanViewerControl();
                host.Child = control;
                resolved = CriticalOrangeBrushOf(control);
            });

            Assert.Null(ex);
            Assert.Equal(LiteCriticalOrangeHex, resolved!.Color);

            control!.Cleanup();
        });
    }

    /// <summary>No <c>.xaml</c> under <c>PerformanceMonitor.Ui</c> may bind either alert-tier brush with
    /// <c>StaticResource</c> — that throws at load when the key is missing (the Dashboard's case for both
    /// keys). Every reference in this project already goes through <c>DynamicResource</c> in XAML or
    /// <c>TryFindResource</c> in code-behind, and that must stay true for both the critical and the warning
    /// tier, not just the one #4632 renamed.</summary>
    [Fact]
    public void NoUiXaml_BindsCriticalOrWarningBrush_WithStaticResource()
    {
        var uiDir = Path.Combine(ParitySource.RepoRoot(), "PerformanceMonitor.Ui");
        var xamlFiles = Directory.GetFiles(uiDir, "*.xaml", SearchOption.AllDirectories);
        Assert.True(xamlFiles.Length > 0, "no .xaml found under PerformanceMonitor.Ui — this scan's anchor moved.");

        foreach (var file in xamlFiles)
        {
            var xaml = File.ReadAllText(file);
            Assert.DoesNotContain("StaticResource CriticalTextBrush", xaml, StringComparison.Ordinal);
            Assert.DoesNotContain("StaticResource WarningBrush", xaml, StringComparison.Ordinal);
        }
    }

    /* ── helpers ── */

    private static ResourceDictionary LoadThemeDictionary(string repoRelativePath)
    {
        var xaml = ParitySource.ReadFile(repoRelativePath);
        return (ResourceDictionary)XamlReader.Parse(xaml);
    }

    /// <summary>Reads the private <c>CriticalOrangeBrush</c> property (#4632) via reflection rather than
    /// widening its access, so this proof adds no product-surface change.</summary>
    private static SolidColorBrush CriticalOrangeBrushOf(PlanViewerControl control)
    {
        var property = typeof(PlanViewerControl).GetProperty("CriticalOrangeBrush", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.True(property is not null, "PlanViewerControl.CriticalOrangeBrush no longer exists under that name — this pin's reflection anchor moved.");
        return (SolidColorBrush)property!.GetValue(control)!;
    }

    /// <summary>WPF objects require STA; same shape as <c>MainWindowAccessKeyTests</c>/<c>ThemeColorOverrideTests</c>/<c>QueryWindowTruncationTests</c>.</summary>
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
