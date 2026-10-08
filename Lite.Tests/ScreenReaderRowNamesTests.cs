/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Windows;
using System.Windows.Automation;
using System.Windows.Automation.Peers;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Threading;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// What UI Automation reads for a grid ROW (it was the data item's type name, "...ViewerJobHistoryRow"), for a templated cell with
/// nothing in it, and what the per-event handlers cost. The Viewer twin is in Darling.Tests (ViewerScreenReaderRowNamesTests).
/// </summary>
[Trait("Reads", "Darling")]
public class ScreenReaderRowNamesTests
{
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        if (error is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw();
        }

        return result;
    }

    private static void Settle(Window window)
    {
        window.Show();
        var frame = new DispatcherFrame();
        window.Dispatcher.BeginInvoke(DispatcherPriority.ApplicationIdle, new Action(() => frame.Continue = false));
        Dispatcher.PushFrame(frame);
    }

    private static void Walk(AutomationPeer peer, List<AutomationPeer> into)
    {
        into.Add(peer);
        foreach (var child in peer.GetChildren() ?? new List<AutomationPeer>())
        {
            Walk(child, into);
        }
    }

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < System.Windows.Media.VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = System.Windows.Media.VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var deeper in Descendants(child))
            {
                yield return deeper;
            }
        }
    }

    /// <summary>No ToString override: a screen reader reads the type name, as it did for the job history row.</summary>
    public sealed class JobRow
    {
        public string Time { get; set; } = "";
        public string Server { get; set; } = "";
        public string Retries { get; set; } = "";
    }

    private static DataTemplate TextTemplate(string path)
    {
        var template = new DataTemplate();
        var panel = new FrameworkElementFactory(typeof(StackPanel));
        var text = new FrameworkElementFactory(typeof(TextBlock));
        text.SetBinding(TextBlock.TextProperty, new Binding(path));
        panel.AppendChild(text);
        template.VisualTree = panel;
        return template;
    }

    private static DataGrid NewGrid(IEnumerable<DataGridColumn> columns, IEnumerable<JobRow> rows, bool recycling = false)
    {
        var grid = new DataGrid { AutoGenerateColumns = false, HeadersVisibility = DataGridHeadersVisibility.Column };
        foreach (var column in columns)
        {
            grid.Columns.Add(column);
        }

        if (recycling)
        {
            VirtualizingPanel.SetVirtualizationMode(grid, VirtualizationMode.Recycling);
        }

        grid.ItemsSource = rows.ToList();
        return grid;
    }

    private static DataGridTextColumn TextColumn(string header, string path) =>
        new() { Header = header, Binding = new Binding(path) };

    [Fact]
    public void AGridRow_IsNamedFromItsFirstTwoVisibleColumns_NotFromItsDataTypeName()
    {
        var names = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var grid = NewGrid(new DataGridColumn[] { TextColumn("Time", "Time"), TextColumn("Server", "Server"), TextColumn("Retries", "Retries") },
                new[] { new JobRow { Time = "06:01:12", Server = "example-sql-01" } });
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(grid), peers);
            window.Close();
            return peers.Select(p => (Type: p.GetType().Name, Name: p.GetName())).ToList();
        });

        Assert.Contains(names, n => n.Type == "DataGridItemAutomationPeer" && n.Name == "Time 06:01:12, Server example-sql-01");
        Assert.DoesNotContain(names, n => n.Name.Contains("JobRow", StringComparison.Ordinal));
    }

    [Fact]
    public void AGridRow_SkipsHiddenColumnsAndReadsTemplatedColumns_AndFollowsTheDisplayOrder()
    {
        var name = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var grid = NewGrid(new DataGridColumn[]
                {
                    new DataGridTemplateColumn { Header = "Retries", CellTemplate = TextTemplate("Retries") },
                    new DataGridTextColumn { Header = "Hidden", Binding = new Binding("Time"), Visibility = Visibility.Collapsed },
                    new DataGridTemplateColumn { Header = "Server", CellTemplate = TextTemplate("Server") },
                    TextColumn("Time", "Time"),
                },
                new[] { new JobRow { Time = "06:01:12", Server = "example-sql-01", Retries = "" } });
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var row = Descendants(grid).OfType<DataGridRow>().First();
            var result = AutomationProperties.GetName(row);
            window.Close();
            return result;
        });

        /* Retries is empty, so the first two columns WITH text are Server and Time; the hidden column is never read. */
        Assert.Equal("Server example-sql-01, Time 06:01:12", name);
    }

    [Fact]
    public void ARowReusedForAnotherItem_IsRenamed_AndWatchersFollowTheRealizedCells_NotTheRowCount()
    {
        var (mismatches, checkedRows, atTop, peak, realized) = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var rows = Enumerable.Range(0, 3000).Select(i => new JobRow { Time = "T" + i, Server = "S" + i, Retries = i % 2 == 0 ? "" : "1" }).ToList();
            var grid = NewGrid(new DataGridColumn[] { TextColumn("Time", "Time"), TextColumn("Server", "Server"), TextColumn("Retries", "Retries") }, rows, recycling: true);
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var scroll = Descendants(grid).OfType<ScrollViewer>().First();
            var top = AccessibleNames.WatcherCount;
            var max = top;
            var bad = 0;
            var seen = 0;

            for (var offset = 0; offset <= rows.Count; offset += 37)
            {
                scroll.ScrollToVerticalOffset(offset);
                Settle(window);
                max = Math.Max(max, AccessibleNames.WatcherCount);

                foreach (var row in Descendants(grid).OfType<DataGridRow>())
                {
                    if (row.Item is not JobRow item)
                    {
                        continue;
                    }

                    seen++;

                    if (AutomationProperties.GetName(row) != "Time " + item.Time + ", Server " + item.Server)
                    {
                        bad++;
                    }
                }
            }

            var realizedCells = Descendants(grid).OfType<DataGridCell>().Count();
            window.Close();
            return (bad, seen, top, max, realizedCells);
        });

        Assert.True(checkedRows > 1000, "the scroll visited only " + checkedRows + " rows");
        Assert.Equal(0, mismatches);
        /* A few watchers per realized cell and row, never one per data row. */
        Assert.True(peak <= atTop * 2, "watchers grew from " + atTop + " to " + peak + " while scrolling 3000 rows (" + realized + " cells realized at the end)");
        Assert.True(peak < 600, "watchers peaked at " + peak);
    }

    [Fact]
    public void ATemplatedCellWithNothingVisible_IsNamedBlank_AndKeepsItsRealContent()
    {
        var (blank, filled, withBox, withText) = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var box = new DataTemplate { VisualTree = new FrameworkElementFactory(typeof(CheckBox)) };
            var grid = NewGrid(new DataGridColumn[]
                {
                    TextColumn("Time", "Time"),
                    new DataGridTemplateColumn { Header = "Retries", CellTemplate = TextTemplate("Retries") },
                    new DataGridTemplateColumn { Header = "Done", CellTemplate = box },
                },
                new[] { new JobRow { Time = "06:01:12" } });
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var cells = Descendants(grid).OfType<DataGridCell>().ToList();
            var retries = cells.First(c => c.Column.Header as string == "Retries");
            var done = cells.First(c => c.Column.Header as string == "Done");
            var time = cells.First(c => c.Column.Header as string == "Time");
            var first = AutomationProperties.GetName(retries);
            var boxName = AutomationProperties.GetName(done);
            var timeName = AutomationProperties.GetName(time);
            var text = Descendants(retries).OfType<TextBlock>().First();
            text.Text = "3";
            var second = AutomationProperties.GetName(retries);
            window.Close();
            return (first, second, boxName, timeName);
        });

        Assert.Equal("Retries: blank", blank);
        Assert.Equal("", filled);
        Assert.Equal("", withBox);
        Assert.Equal("", withText);
    }

    [Fact]
    public void AfterTheFirstLook_TheSizeChangedHandlers_DoNoAllocationAndAddNoWatchers()
    {
        var (bytes, before, after) = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var tabs = new TabControl();
            tabs.Items.Add(new TabItem { Header = new StackPanel { Children = { new TextBlock { Text = "example-sql-01" } } }, Content = new TextBlock() });
            tabs.Items.Add(new TabItem { Header = new StackPanel(), Content = new TextBlock() });
            var panelHeader = new StackPanel { Orientation = Orientation.Horizontal };
            panelHeader.Children.Add(new TextBlock { Text = "Time" });
            var grid = NewGrid(new DataGridColumn[]
                {
                    new DataGridTextColumn { Header = panelHeader, Binding = new Binding("Time") },
                    new DataGridTextColumn { Header = new StackPanel(), Binding = new Binding("Retries") },
                    TextColumn("Server", "Server"),
                },
                new[] { new JobRow { Time = "06:01:12", Server = "example-sql-01", Retries = "" } });
            var host = new StackPanel();
            host.Children.Add(tabs);
            host.Children.Add(grid);
            var window = new Window { Content = host, Width = 500, Height = 400 };
            Settle(window);

            var elements = Descendants(host).Where(d => d is TabItem or System.Windows.Controls.Primitives.DataGridColumnHeader or DataGridRow or DataGridCell).Cast<FrameworkElement>().ToList();
            Assert.True(elements.Count >= 9, "found " + elements.Count + " elements");
            var args = new RoutedEventArgs(FrameworkElement.SizeChangedEvent);
            RoutedEventHandler[] handlers =
            {
                AccessibleNames.OnCellSized, AccessibleNames.OnRowSized, AccessibleNames.OnColumnHeaderSized, AccessibleNames.OnTabItemSized,
            };

            foreach (var element in elements)
            {
                foreach (var handler in handlers)
                {
                    handler(element, args);
                }
            }

            var watchersBefore = AccessibleNames.WatcherCount;
            var allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

            for (var i = 0; i < 2000; i++)
            {
                foreach (var element in elements)
                {
                    foreach (var handler in handlers)
                    {
                        handler(element, args);
                    }
                }
            }

            var allocated = GC.GetAllocatedBytesForCurrentThread() - allocatedBefore;
            var watchersAfter = AccessibleNames.WatcherCount;
            window.Close();
            return (allocated, watchersBefore, watchersAfter);
        });

        Assert.Equal(0, bytes);
        Assert.Equal(before, after);
    }
}
