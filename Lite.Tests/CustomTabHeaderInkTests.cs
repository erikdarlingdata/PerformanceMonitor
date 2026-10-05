/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4678: a selected tab whose header is custom content (a panel holding a TextBlock) drew ForegroundBrush on the
/// accent fill (Dark 1.99:1, Cool Breeze 2.71:1) instead of AccentForegroundBrush. #3577 shadowed the theme's app-level
/// implicit TextBlock style inside the TabItem TEMPLATE, which only reaches the TextBlock WPF generates for a STRING
/// header; a custom header's TextBlock is logically parented to the TabItem and never sees the template. The fix is
/// an empty implicit TextBlock style in each custom header's own Resources (not on the TabItem style, because the tab
/// BODY is also logically parented to the TabItem).
///
/// <para>#4679: the status bar's collector-health text copied a brush once, so it kept the old theme's colour after a
/// live theme switch. It is now a <c>SetResourceReference</c>.</para>
///
/// <para>The live tests load the REAL theme files and build headers through the production code
/// (<see cref="StandalonePlanViewerController"/>, <see cref="MainWindow.PaintCollectorHealth"/>). The sites that
/// need a whole window or a loaded plan (server tab, plan tabs of a server tab, the XAML Plan Viewer tab, the Darling
/// Viewer twin) are pinned in source.</para>
/// </summary>
[Trait("Reads", "Darling")]
public sealed class CustomTabHeaderInkTests
{
    private static ResourceDictionary Theme(string name) =>
        (ResourceDictionary)XamlReader.Parse(ParitySource.ReadFile($"Lite/Themes/{name}Theme.xaml"));

    private static Color Ink(ResourceDictionary dict, string brushKey) => ((SolidColorBrush)dict[brushKey]).Color;

    private static Color HeaderInk(TabItem tab) =>
        ((SolidColorBrush)((TextBlock)((StackPanel)tab.Header).Children[0]).Foreground).Color;

    private static void Layout(FrameworkElement e)
    {
        e.Measure(new Size(1200, 800));
        e.Arrange(new Rect(0, 0, 1200, 800));
        e.UpdateLayout();
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("CoolBreeze")]
    [InlineData("Light")]
    public void PlanSubTabHeader_SelectedUsesAccentInk_UnselectedUsesForegroundInk(string theme)
    {
        OnStaThread(() =>
        {
            var dict = Theme(theme);
            var tabs = new TabControl();
            var host = new Border { Child = tabs };
            host.Resources.MergedDictionaries.Add(dict);
            var controller = new StandalonePlanViewerController(tabs);
            controller.EnsureInitialized();
            var first = controller.AddNewEmptyPlanSubTab();
            var second = controller.AddNewEmptyPlanSubTab();
            tabs.SelectedItem = second;
            Layout(host);

            Assert.True(second.IsSelected);
            Assert.False(first.IsSelected);
            Assert.Equal(Ink(dict, "AccentForegroundBrush"), HeaderInk(second));
            Assert.Equal(Ink(dict, "ForegroundBrush"), HeaderInk(first));
        });
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("CoolBreeze")]
    public void AddTabHeader_TextBlockAsHeader_UsesAccentInkWhenSelected(string theme)
    {
        OnStaThread(() =>
        {
            var dict = Theme(theme);
            var tabs = new TabControl();
            var host = new Border { Child = tabs };
            host.Resources.MergedDictionaries.Add(dict);
            new StandalonePlanViewerController(tabs).EnsureInitialized();
            Layout(host);

            var plus = (TabItem)tabs.Items[0];
            Assert.True(plus.IsSelected, "premise: the sole '+' tab is auto-selected and the insert is deferred, never pumped here.");
            Assert.Equal(Ink(dict, "AccentForegroundBrush"), ((SolidColorBrush)((TextBlock)plus.Header).Foreground).Color);
        });
    }

    /// <summary>Premise: without the shadow style the same header keeps the app style's ForegroundBrush, so the
    /// tests above are not vacuous.</summary>
    [Theory]
    [InlineData("Dark")]
    [InlineData("CoolBreeze")]
    public void Premise_BarePanelHeader_KeepsForegroundInkOnAccent(string theme)
    {
        OnStaThread(() =>
        {
            var dict = Theme(theme);
            var header = new StackPanel();
            header.Children.Add(new TextBlock { Text = "Server" });
            var tab = new TabItem { Header = header, Content = new Grid() };
            var tabs = new TabControl();
            tabs.Items.Add(tab);
            var host = new Border { Child = tabs };
            host.Resources.MergedDictionaries.Add(dict);
            Layout(host);

            Assert.True(tab.IsSelected);
            Assert.Equal(Ink(dict, "ForegroundBrush"), HeaderInk(tab));
        });
    }

    [Fact]
    public void CustomHeaderSites_ShadowTheAppTextBlockStyle()
    {
        const string code = "Resources.Add(typeof(TextBlock), new Style(typeof(TextBlock)))";
        var mainCs = ParitySource.ReadFile("Lite/MainWindow.xaml.cs");
        var createHeader = mainCs[mainCs.IndexOf("StackPanel CreateTabHeader(", StringComparison.Ordinal)..];
        Assert.Contains("panel." + code, createHeader[..createHeader.IndexOf("return panel;", StringComparison.Ordinal)]);
        Assert.Contains("header." + code, ParitySource.ReadFile("Lite/Controls/ServerTab.Plans.cs"));
        Assert.Contains("header." + code, ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Plans.cs"));
        var xaml = ParitySource.ReadFile("Lite/MainWindow.xaml");
        var planTab = xaml[xaml.IndexOf("<TabItem.Header>", StringComparison.Ordinal)..xaml.IndexOf("</TabItem.Header>", StringComparison.Ordinal)];
        Assert.Contains("<StackPanel.Resources><Style TargetType=\"TextBlock\"/></StackPanel.Resources>", planTab);
    }

    private static CollectorHealthSummary Health(string kind) => kind switch
    {
        "ok" => new CollectorHealthSummary { TotalCollectors = 4 },
        "erroring" => new CollectorHealthSummary { TotalCollectors = 4, ErroringCollectors = 1, Errors = { new CollectorHealthEntry { CollectorName = "wait_stats" } } },
        "capture" => new CollectorHealthSummary { TotalCollectors = 4, XeSessionFailures = { new CollectorHealthEntry { CollectorName = "blocked_process" } } },
        _ => new CollectorHealthSummary { TotalCollectors = 4, LoggingFailures = 3 },
    };

    [Theory]
    [InlineData("ok", "ForegroundBrush")]
    [InlineData("erroring", "CriticalTextBrush")]
    [InlineData("capture", "CriticalTextBrush")]
    [InlineData("broken", "CriticalTextBrush")]
    public void CollectorHealthInk_FollowsALiveThemeSwitch_WithoutAnotherPaint(string kind, string key)
    {
        OnStaThread(() =>
        {
            var text = new TextBlock();
            var host = new Border { Child = text };
            var dark = Theme("Dark");
            host.Resources.MergedDictionaries.Add(dark);
            MainWindow.PaintCollectorHealth(text, Health(kind));
            Assert.Equal(Ink(dark, key), ((SolidColorBrush)text.Foreground).Color);

            var light = Theme("Light");
            host.Resources.MergedDictionaries.Clear();
            host.Resources.MergedDictionaries.Add(light);
            Assert.Equal(Ink(light, key), ((SolidColorBrush)text.Foreground).Color);
        });
    }

    [Fact]
    public void ViewerCollectorHealth_UsesLiveReferences_NoOneTimeCopy()
    {
        var src = ParitySource.ReadFile("Darling/PerformanceMonitor.Darling.Viewer/MainWindow.ServerManagement.cs");
        Assert.DoesNotContain("CollectorHealthText.Foreground =", src);
        Assert.DoesNotContain("Brushes.OrangeRed", src);
        Assert.Contains("CollectorHealthText.SetResourceReference(System.Windows.Controls.TextBlock.ForegroundProperty, \"CriticalTextBrush\")", src);
        Assert.Contains("CollectorHealthText.SetResourceReference(System.Windows.Controls.TextBlock.ForegroundProperty, \"ForegroundMutedBrush\")", src);
    }

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
