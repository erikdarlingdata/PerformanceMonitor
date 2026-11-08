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
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;
using LiteMainWindow = PerformanceMonitorLite.MainWindow;

namespace Lite.Tests;

/// <summary>
/// #8331: at a maximized 1920x1080 window with the sidebar open, every server sub-tab is on the row and nothing is behind the "»"
/// button; at a narrower window the button says how many tabs it holds and its tooltip names them. The real main window is built and
/// laid out off screen at the size under test, with a server tab hosted in its tab strip the way the app places one, so the strip is
/// exactly as wide as the window leaves it beside the sidebar. Everything is read from the rendered controls.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabStripFitTests
{
    private const string OverflowButtonName = "PART_TabOverflowButton";

    /* The sidebar's open width (MainWindow.xaml's SidebarColumn); below about 1100 DIPs it shows its 40 px rail by itself. */
    private const double OpenWidth = 280;

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void TheServerSubTabs_AllFitTheRow_At1920x1080_WithTheSidebarOpen(string theme)
    {
        var (sidebar, hidden, buttonShown, rowWidth) = RunTab(theme, (w, tab) =>
        {
            Lay(w, 1920, 1080);
            var main = (TabControl)tab.FindName("MainTabControl");
            var button = (Button)main.Template.FindName(OverflowButtonName, main);
            return (Col(w).ActualWidth, Names(TabStripOverflow.HiddenTabs(main)), button.Visibility == Visibility.Visible, main.ActualWidth);
        });

        Assert.True(sidebar == OpenWidth, $"{theme}: the sidebar is {sidebar:F0} wide at 1920, expected the open {OpenWidth}");
        Assert.True(hidden.Count == 0, $"{theme}: {hidden.Count} server sub-tabs are behind the » button on a {rowWidth:F0} wide strip: {string.Join(", ", hidden)}");
        Assert.False(buttonShown, $"{theme}: every sub-tab fits the {rowWidth:F0} wide strip but the » button is shown");
    }

    [Theory]
    [InlineData("MainTabControl")]
    [InlineData("QueriesSubTabControl")]
    public void At900x600_TheStripKeepsOneRow_AndTheButtonCarriesTheHiddenCountAndNames(string stripName)
    {
        var (hidden, caption, tooltip, tops, buttonShown) = RunTab("Dark", (w, tab) =>
        {
            Lay(w, 900, 600);
            var main = (TabControl)tab.FindName("MainTabControl");
            SelectTab(main, "Queries");
            Lay(w, 900, 600);
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
        var problems = RunTab("Dark", (w, tab) =>
        {
            Lay(w, windowWidth, 1080);
            if (collapseSidebar)
            {
                Toggle(w);
                Lay(w, windowWidth, 1080);
            }

            var main = (TabControl)tab.FindName("MainTabControl");
            var found = Check(main, "server tabs");
            var hidden = TabStripOverflow.HiddenTabs(main).Count;
            Report($"lite\t{windowWidth}\tsidebar {(collapseSidebar ? "collapsed" : "open")} ({Col(w).ActualWidth:F0})\tstrip {main.ActualWidth:F0}\thidden {hidden}");
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

    private static ColumnDefinition Col(LiteMainWindow w) => (ColumnDefinition)w.FindName("SidebarColumn");

    private static void Toggle(LiteMainWindow w)
    {
        var button = (Button)w.FindName("SidebarToggleButton");
        button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
        HostOf(w).UpdateLayout();
    }

    private static void Lay(LiteMainWindow w, double width, double height)
    {
        var host = HostOf(w);
        host.Width = width;
        host.Height = height;
        StaTestThread.WaitForIdle(host.Dispatcher, StaTestThread.IdleLimit);
    }

    /* A window that was never shown has no layout (it reads 0 wide), so the main window's content is moved into a window that is shown. */
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<LiteMainWindow, Window> s_hosts = new();

    private static Window HostOf(LiteMainWindow w) => s_hosts.TryGetValue(w, out var host) ? host : throw new InvalidOperationException("not hosted");

    private static void SelectTab(TabControl tabs, string header)
    {
        tabs.SelectedItem = tabs.Items.OfType<TabItem>().First(i => Equals(i.Header, header));
        tabs.UpdateLayout();
    }

    private static T RunTab<T>(string theme, Func<LiteMainWindow, ServerTab, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabStripFit_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithTheme(theme, () =>
            {
                var w = NewWindow(dir);
                try
                {
                    return body(w, HostTab(w, duckDb));
                }
                finally
                {
                    HostOf(w).Close();
                }
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static LiteMainWindow NewWindow(string dir)
    {
        /* The window reads these three paths when it is built; point them at a folder of the test's own. */
        foreach (var (name, value) in new[] { ("DatabasePath", Path.Combine(dir, "monitor.duckdb")), ("ConfigDirectory", dir), ("SharedConfigDirectory", dir) })
        {
            typeof(PerformanceMonitorLite.App).GetProperty(name, BindingFlags.Static | BindingFlags.Public)!.SetValue(null, value);
        }

        var w = new LiteMainWindow();
        var root = (UIElement)w.Content;
        w.Content = null;
        var host = new Window { Content = root, Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
        /* The main window sets these on itself, and its content inherits them; the host stands in for the window, so it sets the same. */
        TextOptions.SetTextFormattingMode(host, TextFormattingMode.Display);
        host.UseLayoutRounding = true;
        s_hosts.Add(w, host);
        StaTestThread.ShowOffScreen(host);
        /* Windows will not size a window past the screen (plus its borders), so a 1920 wide host is cut on a smaller screen and the
           sidebar would rightly be the rail. The host lifts that limit, so a test gets the width it asks for on any screen. */
        ((System.Windows.Interop.HwndSource)PresentationSource.FromVisual(host)).AddHook(AllowAnySize);
        ClippedTextProbe.Settle(host);
        return w;
    }

    private const int WM_GETMINMAXINFO = 0x0024;

    [System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
    private struct MinMaxInfo { public int ReservedX, ReservedY, MaxSizeX, MaxSizeY, MaxPositionX, MaxPositionY, MinTrackX, MinTrackY, MaxTrackX, MaxTrackY; }

    private static IntPtr AllowAnySize(IntPtr hwnd, int msg, IntPtr wParam, IntPtr lParam, ref bool handled)
    {
        if (msg == WM_GETMINMAXINFO)
        {
            var info = System.Runtime.InteropServices.Marshal.PtrToStructure<MinMaxInfo>(lParam);
            info.MaxTrackX = Math.Max(info.MaxTrackX, 4000);
            info.MaxTrackY = Math.Max(info.MaxTrackY, 4000);
            System.Runtime.InteropServices.Marshal.StructureToPtr(info, lParam, false);
        }
        return IntPtr.Zero;
    }

    /// <summary>A server tab in the main window's tab strip, the way the app places one, against an empty store.</summary>
    private static ServerTab HostTab(LiteMainWindow w, DuckDbInitializer duckDb)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "strip-fit-test", DisplayName = "strip-fit-test", IsEnabled = true };
        var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
        /* Switching to a tab would start that tab's load against a server that is not there. */
        typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
        ((UIElement)w.FindName("EmptyStatePanel")).Visibility = Visibility.Collapsed;
        var tabs = (TabControl)w.FindName("ServerTabControl");
        tabs.Visibility = Visibility.Visible;
        var item = new TabItem { Header = "strip-fit-test", Content = tab };
        tabs.Items.Add(item);
        tabs.SelectedItem = item;
        return tab;
    }

    /// <summary>Runs <paramref name="body"/> with <paramref name="theme"/> as the application's own resources, as the running app has them, then puts the previous set back.</summary>
    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}Theme.xaml") });
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var d in saved)
            {
                app.Resources.MergedDictionaries.Add(d);
            }
        }
    }
}
