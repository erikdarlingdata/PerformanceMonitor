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
using Darling.Tests;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Latches &amp; Spinlocks page and the System Events Memory Broker sub-tab, rendered on the real <see cref="ServerTab"/> with a
/// theme loaded, at 900x600 and at a large window. Opening a snapshot expander makes the page taller, so what sits under it can be
/// scrolled into view, and closing it gives the height back; the Latch Class column shows every class name whole; Memory Broker keeps
/// the layout of its sibling sub-tabs: a title row with Refresh over a full-width grid whose Broker column fits its names and whose
/// every column header is named.
/// </summary>
public sealed class LatchSpinlockPageReachTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string Theme = "DarkTheme";

    private static readonly (int Width, int Height)[] Sizes = { (900, 600), (1920, 1080) };

    /* The sidebar beside a tab in MainWindow.xaml: 280 px, or its 40 px rail below about 1100 DIPs. */
    private static int PaneWidth(int windowWidth) => windowWidth - (windowWidth < 1100 ? 40 : 280);

    /* Latch classes whose long names were read as the same class when the column cut them. */
    private static readonly string[] LatchClasses =
    {
        "ACCESS_METHODS_HOB", "ACCESS_METHODS_HOB_VIRTUAL_ROOT", "ACCESS_METHODS_HOBT_COUNT", "ACCESS_METHODS_SCAN_RANGE_GENERATOR",
        "NESTING_TRANSACTION_FULL", "NESTING_TRANSACTION_READONLY",
    };

    private static readonly string[] Spinlocks = { "SOS_CACHESTORE", "LOCK_HASH", "XDESMGR", "SOS_SCHEDULER", "LOGCACHE_ACCESS", "BUF_HASH" };

    private static readonly string[] Brokers =
    {
        "MEMORYBROKER_FOR_CACHE", "MEMORYBROKER_FOR_STEAL", "MEMORYBROKER_FOR_RESERVE", "MEMORYBROKER_FOR_COMMITTED", "MEMORYBROKER_FOR_XTP",
    };

    [Fact]
    public void OpeningASnapshotExpander_MakesThePageTaller_SoWhatIsUnderItScrollsIntoView_AndClosingItGivesTheHeightBack()
    {
        var problems = WithServerTab((tab, window) =>
        {
            var found = new List<string>();
            var latchGrid = Find<DataGrid>(tab, "LatchStatsGrid");
            var spinlockGrid = Find<DataGrid>(tab, "SpinlockStatsGrid");
            FillSnapshotGrids(latchGrid, spinlockGrid);
            Reveal(latchGrid);
            Settle(window);
            var latches = ExpanderOf(latchGrid);
            var spinlocks = ExpanderOf(spinlockGrid);
            var page = PageOf(latches);

            foreach (var (width, height) in Sizes)
            {
                var at = $"{width}x{height}";
                latches.IsExpanded = false;
                spinlocks.IsExpanded = false;
                Resize(window, PaneWidth(width), height);
                var closed = page.ExtentHeight;
                var content = (FrameworkElement)page.Content;
                if (content.ActualHeight < page.ViewportHeight - 0.5)
                {
                    found.Add($"{at}, both closed: the page is {content.ActualHeight:0.#} px in a {page.ViewportHeight:0.#} px viewport, so the charts do not size from the space they have");
                }
                if (height > 1000 && page.ScrollableHeight > 0.5)
                {
                    found.Add($"{at}, both closed: a large window scrolls by {page.ScrollableHeight:0.#} px");
                }

                latches.IsExpanded = true;
                Settle(window);
                found.AddRange(Reaches(page, latchGrid, $"{at}, Latch classes open: the latch grid"));
                found.AddRange(Reaches(page, spinlocks, $"{at}, Latch classes open: the Spinlocks expander"));

                spinlocks.IsExpanded = true;
                Settle(window);
                found.AddRange(Reaches(page, spinlockGrid, $"{at}, both open: the spinlock grid"));

                latches.IsExpanded = false;
                Settle(window);
                found.AddRange(Reaches(page, spinlockGrid, $"{at}, Spinlocks open: the spinlock grid"));

                spinlocks.IsExpanded = false;
                Settle(window);
                if (page.ExtentHeight > closed + 0.5)
                {
                    found.Add($"{at}, both closed again: the page stays {page.ExtentHeight - closed:0.#} px taller than before they opened");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheLatchClassColumn_ShowsEveryClassNameWhole()
    {
        var problems = WithServerTab((tab, window) =>
        {
            var found = new List<string>();
            var latchGrid = Find<DataGrid>(tab, "LatchStatsGrid");
            var spinlockGrid = Find<DataGrid>(tab, "SpinlockStatsGrid");
            FillSnapshotGrids(latchGrid, spinlockGrid);
            Reveal(latchGrid);
            Settle(window);
            var latches = ExpanderOf(latchGrid);

            foreach (var (width, height) in Sizes)
            {
                Resize(window, PaneWidth(width), height);
                latches.IsExpanded = true;
                Settle(window);
                var shown = new HashSet<string>(StringComparer.Ordinal);
                foreach (var cell in Cells(latchGrid, "Latch Class"))
                {
                    if (!Whole(cell))
                    {
                        found.Add($"{width}x{height}: '{cell.Text}' is cut in a {cell.ActualWidth:0.#} px Latch Class cell");
                    }
                    if (!shown.Add(Visible(cell)))
                    {
                        found.Add($"{width}x{height}: two latch classes read the same ('{Visible(cell)}')");
                    }
                }
                if (shown.Count == 0)
                {
                    found.Add($"{width}x{height}: no Latch Class cell was realized");
                }
                latches.IsExpanded = false;
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void MemoryBroker_HasATitleRowWithRefresh_OverAFullWidthGrid_WhoseBrokerNamesFit_AndWhoseEveryHeaderIsNamed()
    {
        var problems = WithServerTab((tab, window) =>
        {
            var found = new List<string>();
            var brokerGrid = Find<DataGrid>(tab, "MemoryBrokerGrid");
            var siblingGrid = Find<DataGrid>(tab, "MemoryConditionsGrid");
            brokerGrid.ItemsSource = Brokers.Select((broker, i) => new
            {
                EventTimeLocal = "2026-11-08 12:00:00", EventTime = new DateTime(2026, 11, 8, 12, 0, 0, DateTimeKind.Utc), BrokerId = i + 1, Broker = broker,
                Notification = "SHRINK", PoolMetadataId = 1, DeltaTime = 1000L * (i + 1), MemoryRatio = 50L, NewTarget = 1024L, Overall = 2048L, Rate = 10L,
                CurrentlyPredicated = 512L, CurrentlyAllocated = 256L, PreviouslyAllocated = 128L,
            }).ToList();

            foreach (var (width, height) in Sizes)
            {
                var at = $"{width}x{height}";
                Resize(window, PaneWidth(width), height);
                Reveal(siblingGrid);
                Settle(window);
                var sibling = Bounds(siblingGrid, tab);
                Reveal(brokerGrid);
                Settle(window);
                var grid = Bounds(brokerGrid, tab);

                if (CardOf(brokerGrid) is not null)
                {
                    found.Add($"{at}: the Memory Broker grid sits in a card, unlike its sibling sub-tabs");
                }
                if (Math.Abs(grid.Left - sibling.Left) > 1 || Math.Abs(grid.Width - sibling.Width) > 1)
                {
                    found.Add($"{at}: the Memory Broker grid spans {grid.Left:0.#}..{grid.Right:0.#}, its sibling Memory Conditions grid {sibling.Left:0.#}..{sibling.Right:0.#}");
                }

                var root = SubTabContentOf(brokerGrid);
                var refresh = Descendants<Button>(root).Where(b => b.Content as string == "Refresh").ToList();
                if (refresh.Count != 1)
                {
                    found.Add($"{at}: {refresh.Count} Refresh buttons on the Memory Broker sub-tab");
                }
                else
                {
                    var button = Bounds(refresh[0], root);
                    var title = VisualTreeHelper.GetParent(refresh[0]) is Panel row
                        ? row.Children.OfType<TextBlock>().FirstOrDefault(t => !string.IsNullOrWhiteSpace(t.Text) && t.FontSize >= 14)
                        : null;
                    if (title is null)
                    {
                        found.Add($"{at}: no title shares a row with the Refresh button");
                    }
                    if (button.Bottom > Bounds(brokerGrid, root).Top + 0.5)
                    {
                        found.Add($"{at}: the Refresh button ends at {button.Bottom:0.#} px, below the grid's top at {Bounds(brokerGrid, root).Top:0.#} px");
                    }
                }

                var brokers = Cells(brokerGrid, "Broker").ToList();
                if (brokers.Count == 0)
                {
                    found.Add($"{at}: no Broker cell was realized");
                }
                foreach (var cell in brokers.Where(c => !Whole(c)))
                {
                    found.Add($"{at}: '{cell.Text}' is cut in a {cell.ActualWidth:0.#} px Broker cell");
                }

                var broker = ColumnOf(brokerGrid, "Broker");
                var notification = ColumnOf(brokerGrid, "Notification");
                if (notification.ActualWidth >= broker.ActualWidth)
                {
                    found.Add($"{at}: the Notification column ({notification.ActualWidth:0.#} px) is as wide as the Broker column ({broker.ActualWidth:0.#} px), though it only holds SHRINK");
                }

                foreach (var header in Descendants<DataGridColumnHeader>(brokerGrid).Where(h => h.Column is not null))
                {
                    var name = Descendants<TextBlock>(header).FirstOrDefault(t => !string.IsNullOrWhiteSpace(t.Text));
                    if (name is null)
                    {
                        found.Add($"{at}: the column bound to {Path(header.Column)} has a header with no name");
                    }
                    else if (!Whole(name) || Bounds(name, header).Right > header.ActualWidth + 0.5)
                    {
                        found.Add($"{at}: the header '{name.Text}' does not fit its {header.ActualWidth:0.#} px column");
                    }
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /* The element ends inside what the page can show (its scroll extent, or the viewport when the content is stretched to fill a taller one), and scrolling to the bottom brings its end into the viewport. */
    private static IEnumerable<string> Reaches(ScrollViewer page, FrameworkElement element, string label)
    {
        var content = (FrameworkElement)page.Content;
        var end = Bounds(element, content).Bottom;
        var reach = Math.Max(page.ExtentHeight, page.ViewportHeight);
        if (end > reach + 0.5)
        {
            yield return $"{label} ends at {end:0.#} px, past the page's {page.ExtentHeight:0.#} px scroll extent in a {page.ViewportHeight:0.#} px viewport ({Shape(content)})";
            yield break;
        }

        page.ScrollToBottom();
        page.UpdateLayout();
        var inView = Bounds(element, page);
        if (inView.Bottom > page.ViewportHeight + 0.5)
        {
            yield return $"{label} ends at {inView.Bottom:0.#} px of a {page.ViewportHeight:0.#} px viewport after scrolling to the bottom";
        }
        page.ScrollToTop();
        page.UpdateLayout();
    }

    /* The page's own height, what it asked for, and each of its rows, for a failure message. */
    private static string Shape(FrameworkElement content)
    {
        var rows = content is Grid grid
            ? string.Join(", ", grid.RowDefinitions.Select((r, i) => $"row {i} {r.Height}: {r.ActualHeight:0.#} px, min {r.MinHeight:0.#}"))
            : "not a grid";
        return $"page {content.ActualHeight:0.#} px high, asks {content.DesiredSize.Height:0.#} px, min {content.MinHeight:0.#} px; {rows}";
    }

    private static void FillSnapshotGrids(DataGrid latchGrid, DataGrid spinlockGrid)
    {
        var collected = new DateTime(2026, 11, 8, 12, 0, 0, DateTimeKind.Utc);
        latchGrid.ItemsSource = LatchClasses.Select((latchClass, i) => new
        {
            LatchClass = latchClass, DeltaWaitTimeMs = (long?)(1000L * (i + 1)), DeltaWaitingRequestsCount = (long?)(10L * (i + 1)), IntervalDisplay = "300",
            SampleIntervalSeconds = 300, WaitTimeMs = 50000L * (i + 1), MaxWaitTimeMs = 120L, WaitingRequestsCount = 200L * (i + 1), CollectionTimeLocal = "2026-11-08 12:00:00", CollectionTime = collected,
        }).ToList();
        spinlockGrid.ItemsSource = Spinlocks.Select((spinlock, i) => new
        {
            SpinlockName = spinlock, DeltaCollisions = (long?)(100L * (i + 1)), DeltaSpins = (long?)(5000L * (i + 1)), IntervalDisplay = "300", SampleIntervalSeconds = 300,
            SpinsPerCollision = 50.0, Collisions = 1000L * (i + 1), Spins = 50000L * (i + 1), SleepTime = 10L, Backoffs = 5L, CollectionTimeLocal = "2026-11-08 12:00:00", CollectionTime = collected,
        }).ToList();
    }

    /* The realized cells of the column whose header reads <paramref name="header"/>. */
    private static IEnumerable<TextBlock> Cells(DataGrid grid, string header)
    {
        var column = ColumnOf(grid, header);
        for (var i = 0; i < grid.Items.Count; i++)
        {
            if (grid.ItemContainerGenerator.ContainerFromIndex(i) is DataGridRow row && column.GetCellContent(row) is TextBlock cell)
            {
                yield return cell;
            }
        }
    }

    private static DataGridColumn ColumnOf(DataGrid grid, string header) =>
        grid.Columns.FirstOrDefault(c => HeaderText(c) == header) ?? throw new Xunit.Sdk.XunitException($"{grid.Name} has no '{header}' column");

    private static string HeaderText(DataGridColumn column) => column.Header switch
    {
        string s => s,
        TextBlock t => t.Text,
        Panel p => p.Children.OfType<TextBlock>().Select(t => t.Text).FirstOrDefault(t => !string.IsNullOrWhiteSpace(t)) ?? "",
        _ => "",
    };

    private static string Path(DataGridColumn column) =>
        (column as DataGridBoundColumn)?.Binding is System.Windows.Data.Binding b ? b.Path.Path : column.SortMemberPath ?? "?";

    /* The text fits the room its cell or header leaves it: a copy measured without a width limit is no wider than that room. */
    private static bool Whole(TextBlock text) => Natural(text) <= Room(text) + 0.5;

    /* What a reader sees of the text: the whole of it when it fits, else the prefix that fits, as an ellipsis would show it. */
    private static string Visible(TextBlock text)
    {
        if (Whole(text)) return text.Text;
        var room = Room(text);
        var fits = text.Text.Length;
        while (fits > 0 && Natural(text, text.Text[..fits]) > room)
        {
            fits--;
        }
        return text.Text[..fits];
    }

    /* The width the text takes when nothing limits it. */
    private static double Natural(TextBlock text, string? value = null)
    {
        var probe = new TextBlock
        {
            Text = value ?? text.Text, FontFamily = text.FontFamily, FontSize = text.FontSize, FontWeight = text.FontWeight, FontStyle = text.FontStyle,
            FontStretch = text.FontStretch, TextTrimming = TextTrimming.None, TextWrapping = TextWrapping.NoWrap,
        };
        TextOptions.SetTextFormattingMode(probe, TextOptions.GetTextFormattingMode(text));
        probe.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        return probe.DesiredSize.Width;
    }

    /* The width left for the text inside the cell or header that clips it: from where the text starts to the host's inner right edge. A clipped TextBlock keeps its full width, so its own ActualWidth says nothing. */
    private static double Room(TextBlock text)
    {
        Control? host = null;
        for (var up = VisualTreeHelper.GetParent(text); up is not null && host is null; up = VisualTreeHelper.GetParent(up))
        {
            if (up is DataGridCell or DataGridColumnHeader) host = (Control)up;
        }
        if (host is null) return text.ActualWidth - text.Padding.Left - text.Padding.Right;
        var left = Bounds(text, host).Left + text.Padding.Left;
        return host.ActualWidth - host.Padding.Right - host.BorderThickness.Right - left - text.Padding.Right;
    }

    /* The walks below follow the logical tree: a closed expander's content and a tab's presented content have no visual parent chain to the tab. */
    private static Expander ExpanderOf(DependencyObject element)
    {
        for (var up = element; up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is Expander expander) return expander;
        }
        throw new Xunit.Sdk.XunitException("the grid is not inside an Expander");
    }

    /* The page that scrolls as one: the first ScrollViewer above the element (a grid's own scroll viewer sits below it, not above). */
    private static ScrollViewer PageOf(DependencyObject element)
    {
        for (var up = LogicalTreeHelper.GetParent(element); up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is ScrollViewer page) return page;
        }
        throw new Xunit.Sdk.XunitException("the expander is not on a scrolling page");
    }

    private static ChartPanel? CardOf(DependencyObject element)
    {
        for (var up = element; up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is ChartPanel card) return card;
        }
        return null;
    }

    /* The content of the sub-tab the element sits on: the element just under the nearest TabItem. */
    private static FrameworkElement SubTabContentOf(FrameworkElement element)
    {
        FrameworkElement last = element;
        for (var up = (DependencyObject)element; up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is TabItem) return last;
            if (up is FrameworkElement fe) last = fe;
        }
        throw new Xunit.Sdk.XunitException($"{element.Name} is not on a sub-tab");
    }

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match) yield return match;
            foreach (var below in Descendants<T>(child)) yield return below;
        }
    }

    private static List<string> WithServerTab(Func<ServerTab, Window, List<string>> body)
    {
        var dir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "LatchReach_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(System.IO.Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "latch-reach-test", DisplayName = "latch-reach-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there, and the load would replace the rows the test shows. */
                typeof(ServerTab).GetField("_isRefreshing", Instance)!.SetValue(tab, true);
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = PaneWidth(1280), Height = 800, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
                StaTestThread.ShowOffScreen(window);
                Settle(window);
                try
                {
                    return body(tab, window);
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

    private static void Resize(Window window, int width, int height)
    {
        window.Width = width;
        window.Height = height;
        Settle(window);
    }

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
    }

    private static T Find<T>(FrameworkElement tab, string name) where T : FrameworkElement =>
        tab.FindName(name) as T ?? throw new Xunit.Sdk.XunitException($"{name} is not on the tab");

    /* Selects every tab on the way up to the element, so it is laid out and drawn. */
    private static void Reveal(FrameworkElement element)
    {
        for (DependencyObject? at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
            {
                owner.SelectedItem = item;
            }
        }
    }

    private static Rect Bounds(FrameworkElement element, FrameworkElement ancestor) =>
        element.TransformToAncestor(ancestor).TransformBounds(new Rect(element.RenderSize));

    /* Runs the body with the theme and the shared styles as the app's resources. */
    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{Theme}.xaml") });
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var dictionary in saved)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }
    }
}
