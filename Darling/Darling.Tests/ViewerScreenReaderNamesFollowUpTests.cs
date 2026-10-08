/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Windows;
using System.Windows.Automation;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Screen-reader names that go stale or read badly: a header or tab put back at the same size, columns dragged into a new
/// order, hidden elements inside a templated cell, heat-map and bar cells, and a long query text in a row name. The Lite twin is
/// in Lite.Tests (ScreenReaderNamesFollowUpTests).
/// </summary>
public sealed class ViewerScreenReaderNamesFollowUpTests
{
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        using var staGate = WpfStaGate.Enter();
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

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var deeper in Descendants(child))
            {
                yield return deeper;
            }
        }
    }

    public sealed class Row
    {
        public string Time { get; set; } = "";
        public string Server { get; set; } = "";
        public string Retries { get; set; } = "";
        public string Query { get; set; } = "";
    }

    /// <summary>A template whose visual tree is a stack panel holding the given elements.</summary>
    private static DataTemplate PanelTemplate(params FrameworkElementFactory[] children)
    {
        var template = new DataTemplate();
        var panel = new FrameworkElementFactory(typeof(StackPanel));
        foreach (var child in children)
        {
            panel.AppendChild(child);
        }

        template.VisualTree = panel;
        return template;
    }

    private static FrameworkElementFactory BoundText(string path, Visibility visibility = Visibility.Visible)
    {
        var text = new FrameworkElementFactory(typeof(TextBlock));
        text.SetBinding(TextBlock.TextProperty, new Binding(path));
        text.SetValue(UIElement.VisibilityProperty, visibility);
        return text;
    }

    private static DataGrid NewGrid(IEnumerable<DataGridColumn> columns, IEnumerable<Row> rows)
    {
        var grid = new DataGrid { AutoGenerateColumns = false, HeadersVisibility = DataGridHeadersVisibility.Column };
        foreach (var column in columns)
        {
            grid.Columns.Add(column);
        }

        grid.ItemsSource = rows.ToList();
        return grid;
    }

    private static DataGridTextColumn TextColumn(string header, string path) =>
        new() { Header = header, Binding = new Binding(path) };

    [Fact]
    public void AHeaderAndATabPutBackAtTheSameSize_ReadTheirChangedText()
    {
        var (headerBefore, headerAfter, tabBefore, tabAfter) = OnStaThread(() =>
        {
            AccessibleNames.Register();

            var headerText = new TextBlock { Text = "Time (server)" };
            var headerPanel = new StackPanel { Orientation = Orientation.Horizontal };
            headerPanel.Children.Add(headerText);
            headerPanel.Children.Add(new Button { Content = "x" });
            var grid = NewGrid(new DataGridColumn[] { new DataGridTextColumn { Header = headerPanel, Binding = new Binding("Time"), Width = 150 } },
                new[] { new Row { Time = "06:01:12" } });

            var tabText = new TextBlock { Text = "example-sql-01" };
            var tabPanel = new StackPanel { Orientation = Orientation.Horizontal };
            tabPanel.Children.Add(tabText);
            tabPanel.Children.Add(new Button { Content = "x" });
            var tabs = new TabControl { Width = 400, Height = 200 };
            tabs.Items.Add(new TabItem { Header = tabPanel, Content = grid });

            var window = new Window { Content = tabs, Width = 500, Height = 300 };
            Settle(window);

            var header = Descendants(grid).OfType<DataGridColumnHeader>().First(h => h.Column is not null);
            var tab = tabs.Items.OfType<TabItem>().First();
            var hb = AutomationProperties.GetName(header);
            var tb = AutomationProperties.GetName(tab);

            /* Out of the window and back, at the same size; the text changes while it is out (Alert History: UTC picked on another tab). */
            window.Content = null;
            Settle(window);
            headerText.Text = "Time (UTC)";
            tabText.Text = "example-sql-02";
            window.Content = tabs;
            Settle(window);

            var ha = AutomationProperties.GetName(Descendants(grid).OfType<DataGridColumnHeader>().First(h => h.Column is not null));
            var ta = AutomationProperties.GetName(tab);
            window.Close();
            return (hb, ha, tb, ta);
        });

        Assert.Equal("Time (server)", headerBefore);
        Assert.Equal("example-sql-01", tabBefore);
        Assert.Equal("Time (UTC)", headerAfter);
        Assert.Equal("example-sql-02", tabAfter);
    }

    [Fact]
    public void ARowIsRenamed_WhenTheUserDragsColumnsIntoANewOrder()
    {
        var (before, after) = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var grid = NewGrid(new DataGridColumn[] { TextColumn("Time", "Time"), TextColumn("Server", "Server"), TextColumn("Retries", "Retries") },
                new[] { new Row { Time = "06:01:12", Server = "example-sql-01", Retries = "3" } });
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var row = Descendants(grid).OfType<DataGridRow>().First();
            var b = AutomationProperties.GetName(row);

            grid.Columns[2].DisplayIndex = 0;
            Settle(window);
            var a = AutomationProperties.GetName(row);
            window.Close();
            return (b, a);
        });

        Assert.Equal("Time 06:01:12, Server example-sql-01", before);
        Assert.Equal("Retries 3, Time 06:01:12", after);
    }

    [Fact]
    public void HiddenElementsInATemplatedCell_AreNotContent()
    {
        var (hiddenText, hiddenButton, rowName) = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var hiddenButtonFactory = new FrameworkElementFactory(typeof(Button));
            hiddenButtonFactory.SetValue(ContentControl.ContentProperty, "go");
            hiddenButtonFactory.SetValue(UIElement.VisibilityProperty, Visibility.Collapsed);
            var grid = NewGrid(new DataGridColumn[]
                {
                    new DataGridTemplateColumn { Header = "Retries", CellTemplate = PanelTemplate(BoundText("Query", Visibility.Collapsed)) },
                    new DataGridTemplateColumn { Header = "Action", CellTemplate = PanelTemplate(hiddenButtonFactory) },
                    TextColumn("Time", "Time"),
                },
                new[] { new Row { Time = "06:01:12", Query = "SELECT 1" } });
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var cells = Descendants(grid).OfType<DataGridCell>().ToList();
            var row = Descendants(grid).OfType<DataGridRow>().First();
            var result = (AutomationProperties.GetName(cells[0]), AutomationProperties.GetName(cells[1]), AutomationProperties.GetName(row));
            window.Close();
            return result;
        });

        Assert.Equal("Retries: blank", hiddenText);
        Assert.Equal("Action: blank", hiddenButton);
        /* The hidden text is not read into the row's name either. */
        Assert.Equal("Time 06:01:12", rowName);
    }

    [Fact]
    public void HeatMapAndBarCells_GiveTheirValueToTheRowName_AndAnEmptyHeatCellIsBlank()
    {
        var (rowName, emptyHeat, barRowName) = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var heat = new FrameworkElementFactory(typeof(Border));
            heat.SetValue(Border.BackgroundProperty, Brushes.Transparent);
            heat.AppendChild(BoundText("Server"));
            var heatTemplate = new DataTemplate { VisualTree = heat };

            var emptyHeatFactory = new FrameworkElementFactory(typeof(Border));
            emptyHeatFactory.SetValue(Border.BackgroundProperty, Brushes.Transparent);
            emptyHeatFactory.AppendChild(BoundText("Retries"));

            var bar = new FrameworkElementFactory(typeof(BarChartCell));
            bar.SetBinding(BarChartCell.TextProperty, new Binding("Query"));
            bar.SetValue(BarChartCell.MaximumProperty, 10.0);

            var grid = NewGrid(new DataGridColumn[]
                {
                    new DataGridTemplateColumn { Header = "Row Lock Wait ms", CellTemplate = heatTemplate },
                    new DataGridTemplateColumn { Header = "Empty heat", CellTemplate = new DataTemplate { VisualTree = emptyHeatFactory } },
                    new DataGridTemplateColumn { Header = "Executions", CellTemplate = new DataTemplate { VisualTree = bar } },
                },
                new[] { new Row { Server = "1,204", Retries = "" }, new Row { Server = "", Retries = "", Query = "42" } });
            var window = new Window { Content = grid, Width = 700, Height = 300 };
            Settle(window);
            var rows = Descendants(grid).OfType<DataGridRow>().ToList();
            var cells = Descendants(rows[0]).OfType<DataGridCell>().ToList();
            var result = (AutomationProperties.GetName(rows[0]), AutomationProperties.GetName(cells[1]), AutomationProperties.GetName(rows[1]));
            window.Close();
            return result;
        });

        /* The heat cell's text is read although its brush is transparent, and the empty transparent cell is blank, not content. */
        Assert.Equal("Row Lock Wait ms 1,204", rowName);
        Assert.Equal("Empty heat: blank", emptyHeat);
        /* The bar cell's value is read through the user control. */
        Assert.Equal("Executions 42", barRowName);
    }

    [Fact]
    public void ARowNamePart_IsCapped_SoALongQueryTextIsNotReadOutInFull()
    {
        var name = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var grid = NewGrid(new DataGridColumn[] { TextColumn("Collected", "Time"), TextColumn("Query Text", "Query") },
                new[] { new Row { Time = "06:01:12", Query = new string('x', 5000) } });
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var result = AutomationProperties.GetName(Descendants(grid).OfType<DataGridRow>().First());
            window.Close();
            return result;
        });

        Assert.StartsWith("Collected 06:01:12, Query Text xxx", name, StringComparison.Ordinal);
        Assert.EndsWith("...", name, StringComparison.Ordinal);
        Assert.True(name.Length <= "Collected 06:01:12, ".Length + 80, "the name is " + name.Length + " characters");
    }
}
