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
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8219: the window chrome at Lite's 900x600 minimum. The real main window is built and laid out off screen at the size under
/// test (it is never shown, so its load, which opens the store and starts collection, does not run). Every number is read from
/// a rendered control.
/// </summary>
[Collection("credential-manager")]
public sealed class MinWindowChromeTests
{
    /* The rail the sidebar collapses to (the column's MinWidth in MainWindow.xaml) and its open width. */
    private const double RailWidth = 40;
    private const double OpenWidth = 280;

    public static TheoryData<string> Themes => new() { "Dark", "Light", "CoolBreeze" };

    [Theory]
    [InlineData(900, RailWidth)]
    [InlineData(1280, OpenWidth)]
    public void TheSidebar_IsTheRailAtTheMinimumAndOpenAtFullSize(int windowWidth, double expected)
    {
        /* 900: 280 px would be 31% of the window, so the rail (40) shows. 1280: room enough, so 280. The split is about 1100. */
        var (width, diag) = Run("Dark", w =>
        {
            Lay(w, windowWidth, 700);
            var host = HostOf(w);
            return (Col(w).ActualWidth, $"host {host.ActualWidth:F0}x{host.ActualHeight:F0} visible {host.IsVisible}, root {((FrameworkElement)host.Content).ActualWidth:F0}");
        });
        Assert.True(width == expected, $"sidebar column is {width:F0} wide at a {windowWidth} wide window, expected {expected} ({diag})");
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
            Lay(w, 900, 700); found.Add(Col(w).ActualWidth);   // 280: the choice is kept, no auto-switching
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
    [MemberData(nameof(Themes))]
    public void TheStatusBar_IsOneLineAt900_AndKeepsTheFullTextForATooltip(string theme)
    {
        var (barHeight, lineHeight, textHeight, rightEdge, tip, full) = Run(theme, w =>
        {
            var text = "Connected to production-sql-cluster-listener.corp.example.com\\INSTANCE_ONE (read-only intent) - Data loaded for the last 24 hours";
            ((TextBlock)w.FindName("StatusText")).Text = text;
            ((TextBlock)w.FindName("DatabaseSizeText")).Text = "Database: 1234.5 / 2345.6 MB";
            ((TextBlock)w.FindName("CollectionStatusText")).Text = "Collection: Starting...";
            ((TextBlock)w.FindName("CollectorHealthText")).Text = "Collectors: 31 OK, 2 failed";
            ((TextBlock)w.FindName("ServerCountText")).Text = "Servers: 12";
            Lay(w, 900, 600);
            var status = (TextBlock)w.FindName("StatusText");
            var bar = (FrameworkElement)VisualTreeHelper.GetParent(VisualTreeHelper.GetParent(status));
            var one = new FormattedText("Xg", System.Globalization.CultureInfo.CurrentCulture, FlowDirection.LeftToRight,
                new Typeface(status.FontFamily, status.FontStyle, status.FontWeight, status.FontStretch), status.FontSize, Brushes.Black, 1.0);
            var servers = (TextBlock)w.FindName("ServerCountText");
            var right = servers.TransformToAncestor((Visual)HostOf(w).Content).TransformBounds(new Rect(servers.RenderSize)).Right;
            return (bar.ActualHeight, one.Height, status.ActualHeight, right, (status.ToolTip as string) ?? (status.ToolTip as ToolTip)?.Content as string, text);
        });

        /* One text line is about 15 DIPs; the bar adds 16 of padding. A wrapped bar is 2+ lines (the old one was 3 lines, about 110). */
        Assert.True(textHeight <= lineHeight + 1, $"{theme}: status text is {textHeight:F0} high, one line is {lineHeight:F0}");
        Assert.True(barHeight <= lineHeight + 20, $"{theme}: status bar is {barHeight:F0} high at 900");
        Assert.True(rightEdge <= 900.5, $"{theme}: the server count ends at {rightEdge:F0}, past the 900 wide window");
        Assert.Equal(full, tip);
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TopQueriesShowsAtLeastFiveRows_At900x600(string theme)
    {
        var dir = Path.Combine(Path.GetTempPath(), "MinWindowChrome_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var (rows, charts, summary) = StaTestThread.Run(() => WithTheme(theme, () =>
            {
                var w = NewWindow(dir);
                var tab = HostTab(w, duckDb);
                Lay(w, 900, 600);
                var main = (TabControl)tab.FindName("MainTabControl");
                SelectTab(main, "Queries");
                var queries = (TabControl)tab.FindName("QueriesSubTabControl");
                SelectTab(queries, "Top Queries by Duration");
                Lay(w, 900, 600);
                var grid = (DataGrid)tab.FindName("QueryStatsGrid");
                grid.ItemsSource = Enumerable.Range(0, 60).Select(i => new QueryStatsRow { QueryText = "select " + i, DatabaseName = "db" }).ToList();
                Lay(w, 900, 600);
                Snap(w, theme, "top-queries-900x600");
                var visibleRows = ClippedTextProbe.Descendants(grid).OfType<DataGridRow>()
                    .Count(r => r.IsVisible && r.TransformToAncestor(grid).TransformBounds(new Rect(r.RenderSize)).Bottom <= grid.ActualHeight + 0.5);

                SelectTab(main, "Blocking");
                var blocking = (TabControl)tab.FindName("BlockingSubTabControl");
                SelectTab(blocking, "Current Waits");
                Lay(w, 900, 600);
                Snap(w, theme, "current-waits-900x600");
                var viewport = ClippedTextProbe.Descendants(blocking).OfType<ScrollViewer>().First(s => s.IsVisible && s.ViewportHeight > 0);
                var shown = new[] { "CurrentWaitsDurationChart", "CurrentWaitsBlockedChart" }
                    .Select(n => (FrameworkElement)tab.FindName(n))
                    .Count(c => c.IsVisible && c.TransformToAncestor(viewport).TransformBounds(new Rect(c.RenderSize)) is var b && b.Top >= -0.5 && b.Bottom <= viewport.ViewportHeight + 0.5 && b.Height >= 100);
                var summaryText = $"grid {grid.ActualHeight:F0} high, viewport {viewport.ViewportHeight:F0} high";
                HostOf(w).Close();
                return (visibleRows, shown, summaryText);
            }));

            /* Before #8219 the grid showed 2 rows (113 high). Each row is 38 high, set by the themes' DataGridRow style (the same look
               as the Viewer), so the rows come from the space: the slicer is compact in a short window (70 instead of 130). */
            Assert.True(rows >= 5, $"{theme}: Top Queries shows {rows} rows ({summary})");
            _ = charts;
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void CurrentWaits_KeepsEachPlotAboveItsFloor_AtTheMinimumAndAtFullSize()
    {
        /* At 900x600 the tab has about 377 high and both charts at their floor need about 470 (a plot the shared 120 floor keeps needs more
           still), so the tab scrolls and the second chart is reached with the scroll bar. What is asserted is that no plot is squeezed under
           its floor to make both fit: 120 at the minimum, and 200 (the row minimum) at 1280x800. */
        var found = RunTab("Dark", (w, tab) =>
        {
            var main = (TabControl)tab.FindName("MainTabControl");
            SelectTab(main, "Blocking");
            var blocking = (TabControl)tab.FindName("BlockingSubTabControl");
            SelectTab(blocking, "Current Waits");
            var measured = new List<(int Width, string Name, double Height)>();
            foreach (var (width, height) in new[] { (900, 600), (1280, 800) })
            {
                Lay(w, width, height);
                foreach (var name in new[] { "CurrentWaitsDurationChart", "CurrentWaitsBlockedChart" })
                {
                    measured.Add((width, name, ((FrameworkElement)tab.FindName(name)).ActualHeight));
                }
            }

            return measured;
        });

        foreach (var (width, name, height) in found)
        {
            var floor = width == 900 ? 120 : 200;
            Assert.True(height >= floor, $"{name} is {height:F0} high at {width} wide, the floor is {floor}");
        }
    }

    [Fact]
    public void TheTimeSlicer_IsCompactInAShortWindow_AndFullHeightWhenTheWindowGrows()
    {
        var (heights, problems) = RunTab("Dark", (w, tab) =>
        {
            var main = (TabControl)tab.FindName("MainTabControl");
            SelectTab(main, "Queries");
            var queries = (TabControl)tab.FindName("QueriesSubTabControl");
            SelectTab(queries, "Top Queries by Duration");
            var slicer = (TimeRangeSlicerControl)tab.FindName("QueryStatsSlicer");
            var start = new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);
            slicer.LoadData(Enumerable.Range(0, 24).Select(i => new PerformanceMonitor.Common.TimeSliceBucket { BucketTime = start.AddHours(i), Value = i + 1 }).ToList(),
                "Total CPU (ms)", start, start.AddHours(24));
            var border = (FrameworkElement)slicer.FindName("SlicerBorder");
            var canvas = (Canvas)slicer.FindName("SlicerCanvas");
            var heights = new List<double>();
            var problems = new List<string>();
            foreach (var (width, height) in new[] { (900, 600), (1280, 800), (900, 600) })
            {
                Lay(w, width, height);
                slicer.Redraw();
                canvas.UpdateLayout();
                heights.Add(border.ActualHeight);
                var texts = canvas.Children.OfType<TextBlock>().ToList();
                if (!texts.Any(t => t.Text.Contains('/')))
                {
                    problems.Add($"{width}x{height}: no time axis label drawn");
                }

                foreach (var t in texts.Where(t => Canvas.GetTop(t) + t.ActualHeight > border.ActualHeight + 0.5))
                {
                    problems.Add($"{width}x{height}: '{t.Text}' ends at {Canvas.GetTop(t) + t.ActualHeight:F0}, past the {border.ActualHeight:F0} high slicer");
                }

                if (canvas.Children.OfType<System.Windows.Shapes.Rectangle>().Count(r => Math.Abs(r.Height - border.ActualHeight) < 0.5 && r.Width <= 12) < 2)
                {
                    problems.Add($"{width}x{height}: the two selection handles are not drawn full height");
                }
            }

            return (heights, problems);
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        Assert.Equal(new[] { 70.0, 130.0, 70.0 }, heights);
    }

    [Theory]
    [InlineData(1280)]
    [InlineData(900)]
    public void TheFilterBars_KeepEveryLabelBesideItsControl(int windowWidth)
    {
        /* The tab is the window less the sidebar: the rail (40) at 900, the open 280 at 1280. */
        var tabWidth = windowWidth - (windowWidth < 1100 ? RailWidth : OpenWidth);
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
    public void DefaultTraceAndLongQueries_DoNotCoverTheirHeadings(int windowWidth)
    {
        var tabWidth = windowWidth - (windowWidth < 1100 ? RailWidth : OpenWidth);
        var dir = Path.Combine(Path.GetTempPath(), "MinWindowChrome_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var problems = StaTestThread.Run(() => WithTheme("Dark", () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "chrome-test", DisplayName = "chrome-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = tabWidth, Height = 600, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    var found = new List<string>();
                    var main = (TabControl)tab.FindName("MainTabControl");
                    SelectTab(main, "System Events");
                    var sub = ClippedTextProbe.Descendants(main).OfType<TabControl>().First(t => t.Items.OfType<TabItem>().Any(i => Equals(i.Header, "Default Trace")));
                    SelectTab(sub, "Default Trace");
                    ((TextBlock)tab.FindName("DefaultTraceCountIndicator")).Text = "1,234 events";
                    ClippedTextProbe.Settle(window);
                    found.AddRange(NoOverlap(tab, "Default Trace (", "Refresh", "DefaultTraceCountIndicator").Select(p => "[Default Trace] " + p));
                    SelectTab(main, "Long Queries");
                    ClippedTextProbe.Settle(window);
                    found.AddRange(NoOverlap(tab, "Long-Running Query Completions", "LongQueriesIncludeMonitorCheckBox").Select(p => "[Long Queries] " + p));
                    return found;
                }
                finally
                {
                    window.Close();
                }
            }));
            Assert.True(problems.Count == 0, $"{windowWidth} wide window:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Theory]
    [InlineData(900)]
    [InlineData(1280)]
    public void TheServerToolbar_UsesAtMostTwoRows_AndNothingIsLost(int windowWidth)
    {
        /* Two rows: the toolbar's controls, those not in the overflow menu, fall into at most two bands of the header. */
        var tabWidth = windowWidth - (windowWidth < 1100 ? RailWidth : OpenWidth);
        var dir = Path.Combine(Path.GetTempPath(), "MinWindowChrome_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var (bands, hidden, header) = StaTestThread.Run(() => WithTheme("Dark", () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "chrome-test", DisplayName = "chrome-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = tabWidth, Height = 600, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    ClippedTextProbe.Settle(window);
                    var names = new[] { "ServerNameText", "QueryTagFilterCombo", "DatabaseFilterButton", "RangePicker", "CompareToCombo", "RefreshDataButton", "AutoRefreshCheckBox", "TimeDisplayModeBox" };
                    var shown = names.Select(n => (FrameworkElement)tab.FindName(n)).Where(e => e.IsVisible && !ToolBar.GetIsOverflowItem(e)).ToList();
                    var tops = shown.Select(e => e.TransformToAncestor(tab).TransformBounds(new Rect(e.RenderSize))).OrderBy(r => r.Top).ToList();
                    var rows = 0;
                    var bottom = double.MinValue;
                    foreach (var r in tops)
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
                    var lost = must.Where(n => !shown.Contains((FrameworkElement)tab.FindName(n))).ToList();
                    var inside = shown.Where(e => e.TransformToAncestor(tab).TransformBounds(new Rect(e.RenderSize)).Right > tab.ActualWidth + 0.5).Select(e => e.Name).ToList();
                    return (rows, lost.Concat(inside).ToList(), ((FrameworkElement)VisualTreeHelper.GetParent(((FrameworkElement)tab.FindName("RefreshDataButton")))).ActualHeight);
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.True(bands <= 2, $"toolbar uses {bands} rows at {windowWidth} wide");
            Assert.True(hidden.Count == 0, $"never overflow or past the edge: {string.Join(", ", hidden)}");
            _ = header;
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
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
            var element = tab.FindName(name) as FrameworkElement
                ?? ClippedTextProbe.Descendants(tab).OfType<Button>().First(b => b.IsVisible && Equals(b.Content, name));
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
            var dir = Path.Combine(Path.GetTempPath(), "MinWindowChrome_" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(dir);
            try
            {
                var w = NewWindow(dir);
                try
                {
                    return body(w);
                }
                finally
                {
                    HostOf(w).Close();
                }
            }
            finally
            {
                try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
            }
        }));

    private static T RunTab<T>(string theme, Func<MainWindow, ServerTab, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "MinWindowChrome_" + Guid.NewGuid().ToString("N")[..8]);
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

    private static MainWindow NewWindow(string dir)
    {
        /* The window reads these three paths when it is built; point them at a folder of the test's own. */
        foreach (var (name, value) in new[] { ("DatabasePath", Path.Combine(dir, "monitor.duckdb")), ("ConfigDirectory", dir), ("SharedConfigDirectory", dir) })
        {
            typeof(App).GetProperty(name, BindingFlags.Static | BindingFlags.Public)!.SetValue(null, value);
        }

        var w = new MainWindow();
        var root = (UIElement)w.Content;
        w.Content = null;
        var host = new Window { Content = root, Width = 900, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
        s_hosts.Add(w, host);
        host.Show();
        ClippedTextProbe.Settle(host);
        return w;
    }

    private static ServerTab HostTab(MainWindow w, DuckDbInitializer duckDb)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "chrome-test", DisplayName = "chrome-test", IsEnabled = true };
        var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
        typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
        ((UIElement)w.FindName("EmptyStatePanel")).Visibility = Visibility.Collapsed;
        var tabs = (TabControl)w.FindName("ServerTabControl");
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
        using var stream = File.Create(Path.Combine(dir, $"{theme}-{name}.png"));
        encoder.Save(stream);
    }

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
