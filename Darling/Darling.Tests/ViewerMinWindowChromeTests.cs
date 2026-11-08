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
using System.Windows.Media.Imaging;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8266: the Viewer's window chrome at its 900x600 minimum, the counterpart of Lite's MinWindowChromeTests. The real main window
/// is built and laid out off screen at the size under test (it is never shown, so its load, which connects to the store, does not
/// run). Every number is read from a rendered control.
/// </summary>
public sealed class ViewerMinWindowChromeTests
{
    /* The rail the sidebar collapses to (the column's MinWidth in MainWindow.xaml) and its open width. */
    private const double RailWidth = 40;
    private const double OpenWidth = 280;

    /* The main window's content grid has a 10 DIP margin on each side, so a server tab is narrower than the window by the sidebar and 20. */
    private const double RootMargin = 20;

    public static TheoryData<string> Themes => new() { "Dark", "Light", "CoolBreeze" };

    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Theory]
    [InlineData(900, RailWidth)]
    [InlineData(1280, OpenWidth)]
    public void TheSidebar_IsTheRailAtTheMinimumAndOpenAtFullSize(int windowWidth, double expected)
    {
        /* 900: 280 would be 31% of the window, so the rail (40) shows. 1280: room enough, so 280. The split is about 1100. */
        var width = Run("Dark", w =>
        {
            Lay(w, windowWidth, 700);
            return Col(w).ActualWidth;
        });
        Assert.True(width == expected, $"sidebar column is {width:F0} wide at a {windowWidth} wide window, expected {expected}");
    }

    [Fact]
    public void TheSidebar_KeepsAManualChoiceWhenTheWindowIsResized()
    {
        var opened = Run("Dark", w =>
        {
            var found = new List<double>();
            Lay(w, 900, 700); found.Add(Col(w).ActualWidth);   // 40: the automatic rail
            Toggle(w); found.Add(Col(w).ActualWidth);          // 280: the user opened it
            Lay(w, 1280, 700); found.Add(Col(w).ActualWidth);  // 280
            Lay(w, 900, 700); found.Add(Col(w).ActualWidth);   // 280: the choice is kept
            return found;
        });
        Assert.Equal(new[] { RailWidth, OpenWidth, OpenWidth, OpenWidth }, opened);

        var collapsed = Run("Dark", w =>
        {
            var found = new List<double>();
            Lay(w, 1280, 700); found.Add(Col(w).ActualWidth);  // 280
            Toggle(w); found.Add(Col(w).ActualWidth);          // 40: the user collapsed it
            Lay(w, 900, 700); Lay(w, 1280, 700); found.Add(Col(w).ActualWidth); // 40: kept at full size
            return found;
        });
        Assert.Equal(new[] { OpenWidth, RailWidth, RailWidth }, collapsed);
    }

    [Theory]
    [InlineData(900)]
    [InlineData(1280)]
    public void TheServerToolbar_UsesAtMostTwoRows_AndNothingIsLost(int windowWidth)
    {
        /* Two rows: the toolbar's controls, those not in an overflow menu, fall into at most two bands of the header. */
        var (bands, lost) = RunTab("Dark", (w, tab) =>
        {
            Lay(w, windowWidth, 600);
            var names = new[] { "ServerNameText", "QueryTagFilterCombo", "DatabaseFilterButton", "RangePicker", "CompareToCombo", "RefreshDataButton", "AutoRefreshCheckBox", "TimeDisplayModeBox" };
            var shown = names.Select(n => (FrameworkElement)tab.FindName(n)).Where(e => e.IsVisible && !ToolBar.GetIsOverflowItem(e)).ToList();
            var boxes = shown.Select(e => e.TransformToAncestor(tab).TransformBounds(new Rect(e.RenderSize))).OrderBy(r => r.Top).ToList();
            var rows = 0;
            var bottom = double.MinValue;
            foreach (var r in boxes)
            {
                if (r.Top >= bottom - 1)
                {
                    rows++;
                    bottom = r.Bottom;
                }
                else
                {
                    bottom = Math.Max(bottom, r.Bottom);
                }
            }

            var must = new[] { "ServerNameText", "RefreshDataButton", "AutoRefreshCheckBox" };
            var missing = must.Where(n => !shown.Contains((FrameworkElement)tab.FindName(n))).ToList();
            var outside = shown.Where(e => e.TransformToAncestor(tab).TransformBounds(new Rect(e.RenderSize)).Right > tab.ActualWidth + 0.5).Select(e => e.Name).ToList();
            return (rows, missing.Concat(outside).ToList());
        });

        Assert.True(bands <= 2, $"toolbar uses {bands} rows at {windowWidth} wide");
        Assert.True(lost.Count == 0, $"never overflow or past the edge: {string.Join(", ", lost)}");
    }

    [Theory]
    [InlineData("InnerTabs")]
    [InlineData("QueriesSubTabControl")]
    public void TheTabStrips_KeepToOneRow_At900x600_WithAMenuForTheRest(string stripName)
    {
        /* One row: every tab that is arranged sits at the same top. A tab that does not fit the row is in the "»" menu, not on a second row. */
        var (tops, outsideRow, total, hidden) = RunTab("Dark", (w, tab) =>
        {
            Lay(w, 900, 600);
            var inner = (TabControl)tab.FindName("InnerTabs");
            SelectTab(inner, "Queries");
            Lay(w, 900, 600);
            var tabs = (TabControl)tab.FindName(stripName);
            var items = tabs.Items.OfType<TabItem>().Where(t => t.Visibility == Visibility.Visible).ToList();
            var menu = TabStripOverflow.HiddenTabs(tabs);
            var onRow = items.Except(menu).ToList();
            var boxes = onRow.Select(t => t.TransformToAncestor(tabs).TransformBounds(new Rect(t.RenderSize))).ToList();
            var distinctTops = boxes.Select(b => Math.Round(b.Top)).Distinct().Count();
            var past = boxes.Count(b => b.Right > tabs.ActualWidth + 0.5 || b.Left < -0.5);
            return (distinctTops, past, items.Count, menu.Count);
        });

        Assert.True(tops == 1, $"{stripName}: the tabs on the strip sit at {tops} different tops (one row expected); {hidden} of {total} are in the menu");
        Assert.True(outsideRow == 0, $"{stripName}: {outsideRow} tabs on the row are cut at the strip's edge; {hidden} of {total} are in the menu");
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TopQueriesShowsAtLeastFiveRows_At900x600(string theme)
    {
        var (rows, summary) = RunTab(theme, (w, tab) =>
        {
            Lay(w, 900, 600);
            var inner = (TabControl)tab.FindName("InnerTabs");
            SelectTab(inner, "Queries");
            var queries = (TabControl)tab.FindName("QueriesSubTabControl");
            SelectTab(queries, "Top Queries by Duration");
            Lay(w, 900, 600);
            var grid = (DataGrid)tab.FindName("QueryStatsGrid");
            grid.ItemsSource = Enumerable.Range(0, 60).Select(i => new ViewerQueryStatsRow { QueryText = "select " + i, DatabaseName = "db" }).ToList();
            Lay(w, 900, 600);
            Snap(w, theme, "top-queries-900x600");
            var visibleRows = ClippedTextProbe.Descendants(grid).OfType<DataGridRow>()
                .Count(r => r.IsVisible && r.TransformToAncestor(grid).TransformBounds(new Rect(r.RenderSize)).Bottom <= grid.ActualHeight + 0.5);
            return (visibleRows, $"grid {grid.ActualHeight:F0} high, tab {tab.ActualWidth:F0}x{tab.ActualHeight:F0}");
        });

        /* Each row is 38 high, the themes' DataGridRow height, so the rows come from the space the chrome leaves the grid. */
        Assert.True(rows >= 5, $"{theme}: Top Queries shows {rows} rows ({summary})");
    }

    [Theory]
    [InlineData("Dark", 900, 600)]
    [InlineData("Light", 900, 600)]
    [InlineData("CoolBreeze", 900, 600)]
    [InlineData("Dark", 1280, 800)]
    [InlineData("Light", 1280, 800)]
    [InlineData("CoolBreeze", 1280, 800)]
    public void CurrentWaits_ShowsBothCharts_SideBySideWhenTheTabIsUnder460(string theme, int width, int height)
    {
        /* Both charts are shown whole: each plot (the data rectangle) is 120 or more and lies inside the visible area. A tab under 460 high
           (900x600) puts the two beside each other; a taller one (1280x800) stacks them. */
        var problems = RunTab(theme, (w, tab) =>
        {
            Lay(w, width, height);
            var inner = (TabControl)tab.FindName("InnerTabs");
            SelectTab(inner, "Blocking");
            var blocking = (TabControl)tab.FindName("BlockingSubTabs");
            SelectTab(blocking, "Current Waits");
            Lay(w, width, height);
            var window24 = (ValueTuple<DateTime, DateTime>)typeof(ViewerServerTab).GetMethod("GetWindowUtc", Instance)!.Invoke(tab, null)!;
            var dataStart = window24.Item2.AddHours(-6);
            tab.ServerDataStartUtc = dataStart;
            foreach (var name in new[] { "RenderCurrentWaitsDurationChart", "RenderCurrentWaitsBlockedChart" })
            {
                var m = typeof(ViewerServerTab).GetMethod(name, Instance)!;
                m.Invoke(tab, new object?[] { ChartLayoutProbe.MakeRows(m.GetParameters()[0].ParameterType.GetGenericArguments()[0], dataStart, window24.Item2) });
            }

            Lay(w, width, height);
            Snap(w, theme, $"current-waits-{width}x{height}");
            var viewport = (ScrollViewer)tab.FindName("CurrentWaitsScroll");
            var found = new List<string>();
            var boxes = new List<Rect>();
            foreach (var name in new[] { "CurrentWaitsDurationChart", "CurrentWaitsBlockedChart" })
            {
                var chart = (ScottPlot.WPF.WpfPlot)tab.FindName(name);
                var plot = chart.Plot.RenderManager.LastRender.DataRect;
                var box = chart.TransformToAncestor(viewport).TransformBounds(new Rect(chart.RenderSize));
                boxes.Add(box);
                /* LastRender.DataRect is in the chart's own DIP units at any display scale, the same units as the box. */
                var plotBottom = box.Top + plot.Bottom;
                var inside = chart.IsVisible && box.Top >= -0.5 && plotBottom <= viewport.ViewportHeight + 0.5 && box.Left >= -0.5 && box.Right <= viewport.ViewportWidth + 0.5;
                if (plot.Height < 120 || !inside)
                {
                    found.Add($"{name}: plot {plot.Height:F0} high ending at {plotBottom:F0}, chart {box.Width:F0}x{box.Height:F0} at ({box.Left:F0},{box.Top:F0}), visible area {viewport.ViewportWidth:F0}x{viewport.ViewportHeight:F0}");
                }
            }

            var sideBySide = boxes[1].Left >= boxes[0].Right - 0.5 && Math.Abs(boxes[1].Top - boxes[0].Top) <= 0.5;
            var stacked = boxes[1].Top >= boxes[0].Bottom - 0.5;
            var wantSideBySide = viewport.ViewportHeight < 460;
            if (wantSideBySide ? !sideBySide : !stacked)
            {
                found.Add($"the tab's content is {viewport.ViewportHeight:F0} high and the charts are {(sideBySide ? "side by side" : stacked ? "stacked" : "neither side by side nor stacked")}: first at ({boxes[0].Left:F0},{boxes[0].Top:F0}) {boxes[0].Width:F0}x{boxes[0].Height:F0}, second at ({boxes[1].Left:F0},{boxes[1].Top:F0})");
            }

            return found;
        });

        Assert.True(problems.Count == 0, $"{theme} {width}x{height}: " + string.Join("; ", problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheQueryHeatmapCard_FitsItsViewWithoutScrolling_At900x600(string theme)
    {
        var (scrollable, cardBottom, viewportHeight, plotHeight) = RunTab(theme, (w, tab) =>
        {
            Lay(w, 900, 600);
            var inner = (TabControl)tab.FindName("InnerTabs");
            SelectTab(inner, "Queries");
            var queries = (TabControl)tab.FindName("QueriesSubTabControl");
            SelectTab(queries, "Query Heatmap");
            Lay(w, 900, 600);
            Snap(w, theme, "heatmap-900x600");
            var scroll = (ScrollViewer)tab.FindName("HeatmapScroll");
            var chart = (ScottPlot.WPF.WpfPlot)tab.FindName("QueryHeatmapChart");
            var card = ClippedTextProbe.Descendants(scroll).OfType<ChartPanel>().First(p => p.Title == "Query Heatmap");
            var bottom = card.TransformToAncestor(scroll).TransformBounds(new Rect(card.RenderSize)).Bottom;
            return (scroll.ScrollableHeight, bottom, scroll.ViewportHeight, chart.Plot.RenderManager.LastRender.DataRect.Height);
        });

        Assert.True(scrollable <= 0.5, $"{theme}: the heatmap view scrolls {scrollable:F0} px (card bottom {cardBottom:F0}, visible area {viewportHeight:F0} high, plot {plotHeight:F0})");
        Assert.True(cardBottom <= viewportHeight + 0.5, $"{theme}: the card ends at {cardBottom:F0}, past the {viewportHeight:F0} high visible area");
    }

    [Theory]
    [InlineData(1280)]
    [InlineData(900)]
    public void TheFilterBars_KeepEveryLabelBesideItsControl(int windowWidth)
    {
        /* The tab is the window less the sidebar and the content margin: the rail (40) at 900, the open 280 at 1280. */
        var tabWidth = windowWidth - (windowWidth < 1100 ? RailWidth : OpenWidth) - RootMargin;
        var problems = StaTestThread.Run(() => WithTheme("Dark", () =>
        {
            var found = new List<string>();
            found.AddRange(PairsShareARow(new JobHistoryTab(), tabWidth, "Server:", "Status:", "Category:", "Time Range:").Select(p => "[Job History] " + p));
            found.AddRange(PairsShareARow(new AlertsHistoryTab(), tabWidth, "Server:", "Time Range:").Select(p => "[Alert History] " + p));
            return found;
        }));
        Assert.True(problems.Count == 0, $"{windowWidth} wide window:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [InlineData(900)]
    [InlineData(1280)]
    public void LongQueries_DoesNotCoverItsHeading(int windowWidth)
    {
        var problems = RunTab("Dark", (w, tab) =>
        {
            Lay(w, windowWidth, 600);
            var inner = (TabControl)tab.FindName("InnerTabs");
            SelectTab(inner, "Long Queries");
            Lay(w, windowWidth, 600);
            return NoOverlap(tab, "Long-Running Query Completions", "LongQueriesIncludeMonitorCheckBox");
        });
        Assert.True(problems.Count == 0, $"{windowWidth} wide window:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    // ---- helpers ----

    /// <summary>The heading (found by its text) is wholly inside the tab at its full height, and none of the named controls sit on it or past the tab's edge.</summary>
    private static List<string> NoOverlap(FrameworkElement tab, string headingStart, params string[] names)
    {
        var problems = new List<string>();
        var heading = ClippedTextProbe.Descendants(tab).OfType<TextBlock>().First(t => t.IsVisible && t.Text.StartsWith(headingStart, StringComparison.Ordinal));
        Rect Bounds(FrameworkElement e) => e.TransformToAncestor(tab).TransformBounds(new Rect(e.RenderSize));
        var h = Bounds(heading);
        if (h.Right > tab.ActualWidth + 0.5 || heading.ActualHeight + 0.5 < heading.DesiredSize.Height - heading.Margin.Top - heading.Margin.Bottom)
        {
            problems.Add($"heading spans {h.Left:F0}..{h.Right:F0} of {tab.ActualWidth:F0}, {heading.ActualHeight:F0} high of {heading.DesiredSize.Height - heading.Margin.Top - heading.Margin.Bottom:F0}");
        }

        foreach (var name in names)
        {
            var element = (FrameworkElement)tab.FindName(name);
            var b = Bounds(element);
            if (b.Right > tab.ActualWidth + 0.5 || b.IntersectsWith(Rect.Inflate(h, -0.5, -0.5)))
            {
                problems.Add($"{name} spans {b.Left:F0}..{b.Right:F0}, {b.Top:F0}..{b.Bottom:F0}, over the heading {h.Left:F0}..{h.Right:F0}, {h.Top:F0}..{h.Bottom:F0} or past the tab's {tab.ActualWidth:F0}");
            }
        }

        return problems;
    }

    /// <summary>Each caption's next sibling control (its pair) must share a row with it: their vertical centers within 6 DIPs.</summary>
    private static List<string> PairsShareARow(FrameworkElement tab, double width, params string[] captions)
    {
        var window = new Window { Content = tab, Width = width, Height = 600, Left = 0, Top = 0 };
        window.Show();
        try
        {
            ClippedTextProbe.Settle(window);
            var problems = new List<string>();
            foreach (var caption in captions)
            {
                var label = ClippedTextProbe.Descendants(tab).OfType<TextBlock>().First(t => t.Text == caption);
                var next = NextControl(label);
                var l = label.TransformToAncestor(tab).TransformBounds(new Rect(label.RenderSize));
                var c = next.TransformToAncestor(tab).TransformBounds(new Rect(next.RenderSize));
                if (Math.Abs((l.Top + l.Bottom) / 2 - (c.Top + c.Bottom) / 2) > 6)
                {
                    problems.Add($"{caption} is at y {l.Top:F0}, its control at y {c.Top:F0}");
                }
            }

            return problems;
        }
        finally
        {
            window.Close();
        }
    }

    private static FrameworkElement NextControl(FrameworkElement label)
    {
        var parent = (Panel)(label.Parent ?? throw new InvalidOperationException("the label has no panel"));
        return (FrameworkElement)parent.Children[parent.Children.IndexOf(label) + 1];
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

    private static T Run<T>(string theme, Func<MainWindow, T> body) =>
        StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var w = NewWindow();
            try
            {
                return body(w);
            }
            finally
            {
                HostOf(w).Close();
            }
        }));

    private static T RunTab<T>(string theme, Func<MainWindow, ViewerServerTab, T> body) =>
        StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var w = NewWindow();
            try
            {
                return body(w, HostTab(w));
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
        host.Show();
        /* Windows will not size a window past the screen (plus its borders), so a 1280 wide host is cut on a 1024 wide screen
           and the sidebar would rightly be the rail. The host lifts that limit, so a test gets the width it asks for on any screen. */
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
    private static ViewerServerTab HostTab(MainWindow w)
    {
        var tab = new ViewerServerTab(
            new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
            new DarlingServer(1, "chrome-test", "chrome-test", true, null));
        // Switching to a tab would start that tab's load against a server that is not there: the refresh loop bounces instead.
        typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
        ((UIElement)w.FindName("EmptyStatePanel")).Visibility = Visibility.Collapsed;
        var tabs = (TabControl)w.FindName("MainTabs");
        tabs.Visibility = Visibility.Visible;
        var item = new TabItem { Header = "chrome-test", Content = tab };
        tabs.Items.Add(item);
        tabs.SelectedItem = item;
        return tab;
    }

    /// <summary>Writes a PNG of the laid-out window when MIN_WINDOW_CHROME_PNG names a folder; never in CI.</summary>
    private static void Snap(MainWindow w, string theme, string name)
    {
        var dir = Environment.GetEnvironmentVariable("MIN_WINDOW_CHROME_PNG");
        if (string.IsNullOrEmpty(dir))
        {
            return;
        }

        Directory.CreateDirectory(dir);
        var host = HostOf(w);
        var bitmap = new RenderTargetBitmap((int)host.ActualWidth, (int)host.ActualHeight, 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(host);
        var encoder = new PngBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create(bitmap));
        using var stream = File.Create(Path.Combine(dir, $"viewer-{theme}-{name}.png"));
        encoder.Save(stream);
    }

    /// <summary>Runs <paramref name="body"/> with <paramref name="theme"/> applied the way the app applies one (with the shared styles), then puts the previous theme back.</summary>
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
