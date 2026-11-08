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
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8331: at a maximized 1920x1080 window with the sidebar open, every server sub-tab is on the row and nothing is behind the "»"
/// button; at a narrower window the button says how many tabs it holds and its tooltip names them. The real main window is built
/// and laid out off screen at the size under test, with a server tab hosted the way OpenServerTab places one. Every number is read
/// from a rendered control.
/// </summary>
public sealed class ViewerSubTabStripFitTests
{
    /* The sidebar's open width and the rail it collapses to (MainWindow.xaml's SidebarColumn). */
    private const double OpenWidth = 280;
    private const double RailWidth = 40;

    private const string OverflowButtonName = "PART_TabOverflowButton";

    public static TheoryData<string> Themes => new() { "Dark", "Light", "CoolBreeze" };

    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheSqlServerSubTabs_AllFitTheRow_At1920x1080_WithTheSidebarOpen(string theme)
    {
        var (sidebar, hidden, buttonShown, rowWidth) = RunTab(theme, engineKind: null, (w, tab) =>
        {
            Lay(w, 1920, 1080);
            var inner = (TabControl)tab.FindName("InnerTabs");
            var button = (Button)inner.Template.FindName(OverflowButtonName, inner);
            return (Col(w).ActualWidth, Names(TabStripOverflow.HiddenTabs(inner)), button.Visibility == Visibility.Visible, inner.ActualWidth);
        });

        Assert.True(sidebar == OpenWidth, $"{theme}: the sidebar is {sidebar:F0} wide at 1920, expected the open {OpenWidth}");
        Assert.True(hidden.Count == 0, $"{theme}: {hidden.Count} SQL Server sub-tabs are behind the » button on a {rowWidth:F0} wide strip: {string.Join(", ", hidden)}");
        Assert.False(buttonShown, $"{theme}: every sub-tab fits the {rowWidth:F0} wide strip but the » button is shown");
    }

    [Fact]
    public void ThePostgresSubTabs_AllFitTheRow_At1920x1080_WithTheSidebarOpen()
    {
        var (shown, hidden, rowWidth) = RunTab("Dark", engineKind: "postgres", (w, tab) =>
        {
            Lay(w, 1920, 1080);
            var inner = (TabControl)tab.FindName("InnerTabs");
            var visible = inner.Items.OfType<TabItem>().Count(t => t.Visibility == Visibility.Visible);
            return (visible, Names(TabStripOverflow.HiddenTabs(inner)), inner.ActualWidth);
        });

        Assert.True(shown > 0, "a PostgreSQL server shows no sub-tabs at all");
        Assert.True(hidden.Count == 0, $"{hidden.Count} of {shown} PostgreSQL sub-tabs are behind the » button on a {rowWidth:F0} wide strip: {string.Join(", ", hidden)}");
    }

    [Theory]
    [InlineData("InnerTabs")]
    [InlineData("QueriesSubTabControl")]
    public void At900x600_TheStripKeepsOneRow_AndTheButtonCarriesTheHiddenCountAndNames(string stripName)
    {
        var (hidden, caption, tooltip, tops, buttonShown) = RunTab("Dark", engineKind: null, (w, tab) =>
        {
            Lay(w, 900, 600);
            var inner = (TabControl)tab.FindName("InnerTabs");
            SelectTab(inner, "Queries");
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
    public void AtEveryWidth_TheSqlServerStripIsOneRow_AndTheButtonMatchesWhatIsHidden(int windowWidth, bool collapseSidebar)
    {
        var problems = RunTab("Dark", engineKind: null, (w, tab) =>
        {
            Lay(w, windowWidth, 1080);
            if (collapseSidebar)
            {
                Toggle(w);
                Lay(w, windowWidth, 1080);
            }

            var inner = (TabControl)tab.FindName("InnerTabs");
            var found = Check(inner, "InnerTabs");
            var hidden = TabStripOverflow.HiddenTabs(inner).Count;
            Report($"viewer\t{windowWidth}\tsidebar {(collapseSidebar ? "collapsed" : "open")} ({Col(w).ActualWidth:F0})\tstrip {inner.ActualWidth:F0}\thidden {hidden}");
            return found;
        });

        Assert.True(problems.Count == 0, $"{windowWidth} wide, sidebar {(collapseSidebar ? "collapsed" : "open")}:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
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
        File.AppendAllText(Path.Combine(dir, "viewer-sub-tab-strip.tsv"), line + Environment.NewLine);
    }

    private static ColumnDefinition Col(MainWindow w) => (ColumnDefinition)w.FindName("SidebarColumn");

    private static void Toggle(MainWindow w)
    {
        var button = (Button)w.FindName("SidebarToggleButton");
        button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
        HostOf(w).UpdateLayout();
    }

    private static void Lay(MainWindow w, double width, double height)
    {
        var host = HostOf(w);
        host.Width = width;
        host.Height = height;
        StaTestThread.WaitForIdle(host.Dispatcher, StaTestThread.IdleLimit);
    }

    /* A window that was never shown has no layout (it reads 0 wide), so the main window's content is moved into a window that is shown. */
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<MainWindow, Window> s_hosts = new();

    private static Window HostOf(MainWindow w) => s_hosts.TryGetValue(w, out var host) ? host : throw new InvalidOperationException("not hosted");

    private static void SelectTab(TabControl tabs, string header)
    {
        tabs.SelectedItem = tabs.Items.OfType<TabItem>().First(i => Equals(i.Header, header));
        tabs.UpdateLayout();
    }

    private static T RunTab<T>(string theme, string? engineKind, Func<MainWindow, ViewerServerTab, T> body) =>
        StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var w = NewWindow();
            try
            {
                return body(w, HostTab(w, engineKind));
            }
            finally
            {
                HostOf(w).Close();
            }
        }));

    private static MainWindow NewWindow()
    {
        var w = new MainWindow();
        var root = (UIElement)w.Content;
        w.Content = null;
        var host = new Window { Content = root, Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
        s_hosts.Add(w, host);
        StaTestThread.ShowOffScreen(host);
        /* Windows will not size a window past the screen (plus its borders), so a 1920 wide host is cut on a smaller screen. The host lifts
           that limit, so a test gets the width it asks for on any screen. */
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

    /// <summary>A server tab in the main window's tab strip, the way OpenServerTab places one, against a store that is not there.</summary>
    private static ViewerServerTab HostTab(MainWindow w, string? engineKind)
    {
        var tab = new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "strip-fit-test", "strip-fit-test", true, null, engineKind: engineKind));
        // Switching to a tab would start that tab's load against a server that is not there: the refresh loop bounces instead.
        typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
        ((UIElement)w.FindName("EmptyStatePanel")).Visibility = Visibility.Collapsed;
        var tabs = (TabControl)w.FindName("MainTabs");
        tabs.Visibility = Visibility.Visible;
        var item = new TabItem { Header = "strip-fit-test", Content = tab };
        tabs.Items.Add(item);
        tabs.SelectedItem = item;
        return tab;
    }

    /// <summary>Runs <paramref name="body"/> with <paramref name="theme"/> applied the way the app applies one, then puts the previous theme back.</summary>
    private static T WithTheme<T>(string theme, Func<T> body)
    {
        _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var previous = ThemeManager.CurrentTheme;
        ThemeManager.ThemeUriProvider = name => new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");
        try
        {
            ThemeManager.Apply(theme);
            return body();
        }
        finally
        {
            ThemeManager.Apply(previous);
            ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
        }
    }
}
