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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8331: at a maximized 1920x1080 window with the sidebar open, every server sub-tab is on the row and nothing is behind the "»"
/// button; at a narrower window the button says how many tabs it holds and its tooltip names them. The real ServerTab is built on
/// the shared STA thread, themed, shown at the width the main window leaves it beside the sidebar, and everything is read from the
/// rendered controls.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabStripFitTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string OverflowButtonName = "PART_TabOverflowButton";

    /* The sidebar's open width and the rail it collapses to below about 1100 DIPs (MainWindow.xaml's SidebarColumn). */
    private const int OpenWidth = 280;
    private const int RailWidth = 40;

    private static int TabWidth(int windowWidth, bool sidebarCollapsed) => windowWidth - (sidebarCollapsed || windowWidth < 1100 ? RailWidth : OpenWidth);

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheServerSubTabs_AllFitTheRow_At1920x1080_WithTheSidebarOpen(string theme)
    {
        var (hidden, buttonShown, rowWidth) = WithServerTab(theme, TabWidth(1920, sidebarCollapsed: false), 1080, (window, tab) =>
        {
            var main = (TabControl)tab.FindName("MainTabControl");
            var button = (Button)main.Template.FindName(OverflowButtonName, main);
            return (Names(TabStripOverflow.HiddenTabs(main)), button.Visibility == Visibility.Visible, main.ActualWidth);
        });

        Assert.True(hidden.Count == 0, $"{theme}: {hidden.Count} server sub-tabs are behind the » button on a {rowWidth:F0} wide strip: {string.Join(", ", hidden)}");
        Assert.False(buttonShown, $"{theme}: every sub-tab fits the {rowWidth:F0} wide strip but the » button is shown");
    }

    [Theory]
    [InlineData("MainTabControl")]
    [InlineData("QueriesSubTabControl")]
    public void At900x600_TheStripKeepsOneRow_AndTheButtonCarriesTheHiddenCountAndNames(string stripName)
    {
        var (hidden, caption, tooltip, tops, buttonShown) = WithServerTab("Dark", TabWidth(900, sidebarCollapsed: false), 600, (window, tab) =>
        {
            var main = (TabControl)tab.FindName("MainTabControl");
            main.SelectedItem = main.Items.OfType<TabItem>().First(t => Equals(t.Header, "Queries"));
            StaTestThread.Settle(window);
            var tabs = (TabControl)tab.FindName(stripName);
            var button = (Button)tabs.Template.FindName(OverflowButtonName, tabs);
            var menu = TabStripOverflow.HiddenTabs(tabs);
            var onRow = tabs.Items.OfType<TabItem>().Where(t => t.Visibility == Visibility.Visible).Except(menu).ToList();
            var distinctTops = onRow.Select(t => Math.Round(t.TransformToAncestor(tabs).TransformBounds(new Rect(t.RenderSize)).Top)).Distinct().Count();
            return (Names(menu), button.Content as string, button.ToolTip as string, distinctTops, button.Visibility == Visibility.Visible);
        });

        Assert.True(hidden.Count > 0, $"{stripName}: at 900 wide every tab fits, so there is nothing for the button to count");
        Assert.True(tops == 1, $"{stripName}: the tabs on the strip sit at {tops} different tops (one row expected)");
        Assert.True(buttonShown, $"{stripName}: {hidden.Count} tabs are hidden but the » button is not shown");
        Assert.Equal("» " + hidden.Count, caption);
        Assert.Equal("Tabs that do not fit: " + string.Join(", ", hidden), tooltip);
    }

    /// <summary>
    /// At each width: one row, no tab cut at an edge, the button shown exactly when a tab is hidden, and its count matching. The hidden
    /// count per width is appended to TAB_STRIP_FIT_REPORT when that names a folder, so a layout can be read without a failure.
    /// </summary>
    [Theory]
    [InlineData(1920, false)]
    [InlineData(1920, true)]
    [InlineData(1600, false)]
    [InlineData(1280, false)]
    [InlineData(900, false)]
    public void AtEveryWidth_TheServerStripIsOneRow_AndTheButtonMatchesWhatIsHidden(int windowWidth, bool collapseSidebar)
    {
        var width = TabWidth(windowWidth, collapseSidebar);
        var problems = WithServerTab("Dark", width, 1080, (window, tab) =>
        {
            var main = (TabControl)tab.FindName("MainTabControl");
            var found = Check(main, "server tabs");
            var hidden = TabStripOverflow.HiddenTabs(main).Count;
            Report($"lite\t{windowWidth}\tsidebar {(collapseSidebar ? "collapsed" : "open")} ({windowWidth - width})\tstrip {main.ActualWidth:F0}\thidden {hidden}");
            return found;
        });

        Assert.True(problems.Count == 0, $"{windowWidth} wide window, sidebar {(collapseSidebar ? "collapsed" : "open")}:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    // ---- helpers ----

    private static List<string> Check(TabControl tabs, string label)
    {
        var found = new List<string>();
        var button = (Button)tabs.Template.FindName(OverflowButtonName, tabs);
        var menu = TabStripOverflow.HiddenTabs(tabs);
        var onRow = tabs.Items.OfType<TabItem>().Where(t => t.Visibility == Visibility.Visible).Except(menu).ToList();
        var boxes = onRow.Select(t => t.TransformToAncestor(tabs).TransformBounds(new Rect(t.RenderSize))).ToList();
        if (boxes.Select(b => Math.Round(b.Top)).Distinct().Count() != 1)
        {
            found.Add($"[{label}] the tabs on the strip sit at more than one top");
        }

        if (boxes.Any(b => b.Right > tabs.ActualWidth + 0.5 || b.Left < -0.5))
        {
            found.Add($"[{label}] a tab on the row is cut at the strip's edge");
        }

        if ((button.Visibility == Visibility.Visible) != (menu.Count > 0))
        {
            found.Add($"[{label}] {menu.Count} tabs are hidden but the » button is {button.Visibility}");
        }

        if (menu.Count > 0 && !Equals(button.Content, "» " + menu.Count))
        {
            found.Add($"[{label}] {menu.Count} tabs are hidden but the button says '{button.Content}'");
        }

        return found;
    }

    private static List<string> Names(IEnumerable<TabItem> tabs) => tabs.Select(TabStripOverflow.HeaderText).ToList();

    private static void Report(string line)
    {
        var dir = Environment.GetEnvironmentVariable("TAB_STRIP_FIT_REPORT");
        if (string.IsNullOrEmpty(dir))
        {
            return;
        }

        Directory.CreateDirectory(dir);
        File.AppendAllText(Path.Combine(dir, "lite-sub-tab-strip.tsv"), line + Environment.NewLine);
    }

    /// <summary>Builds the real ServerTab against an empty store, shows it themed at the given size, and hands it to <paramref name="body"/>.</summary>
    private static T WithServerTab<T>(string theme, int width, int height, Func<Window, ServerTab, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabStripFit_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithAppTheme(theme, () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "strip-fit-test", DisplayName = "strip-fit-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None };
                window.Resources.MergedDictionaries.Add(LoadTheme(theme));
                StaTestThread.ShowOffScreen(window);
                try
                {
                    StaTestThread.Settle(window);
                    return body(window, tab);
                }
                finally
                {
                    window.Close();
                }
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static ResourceDictionary LoadTheme(string theme) =>
        new() { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}Theme.xaml") };

    /// <summary>Runs <paramref name="body"/> with the theme under test as the application's own, so a control resolves its brushes and the app-level text style from it as the running app does.</summary>
    private static T WithAppTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = LoadTheme(theme);
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
