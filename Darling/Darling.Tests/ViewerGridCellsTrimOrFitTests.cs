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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's cells and list entries whose text can be longer than their column either fit whole or trim with an ellipsis and
/// carry the whole text as a tooltip, rendered on the real controls with the app's theme. FinOps Database Sizes trims Database and
/// File Name; FinOps Server Inventory shows UNDER PROVISIONED whole at the maximized tab width; FinOps Locking &amp; Contention shows
/// no horizontal scroll bar while it holds no rows; Job History trims Job and Step; the Wait Stats picker trims a long wait type and
/// never scrolls sideways at a 900x600 window. Lite's twin is GridCellsTrimOrFitTests in Lite.Tests.
/// </summary>
public sealed class ViewerGridCellsTrimOrFitTests
{
    private const int MaximizedTabWidth = 1270;
    private const int MinimumTabWidth = 860;
    private const string LongName = "CardinalityExtrapolation_long_database_name_for_this_grid";

    [Fact]
    public void DatabaseSizes_TrimsLongDatabaseAndFileNames_WithAnEllipsisAndTheWholeNameAsTooltip()
    {
        var problems = StaTestThread.Run(() => WithDarkTheme(() => WithFinOpsTab(MinimumTabWidth, 600, (tab, window) =>
        {
            var grid = (DataGrid)tab.FindName("FinOpsDatabaseSizesDataGrid");
            Reveal(grid);
            grid.ItemsSource = new List<DatabaseSizeRow>
            {
                new() { DatabaseName = LongName, FileTypeDesc = "ROWS", FileName = LongName + "_data_01.mdf" },
            };
            Settle(window);
            var found = new List<string>();
            found.AddRange(TrimProblems(grid, "DatabaseName", "Database Sizes"));
            found.AddRange(TrimProblems(grid, "FileName", "Database Sizes"));
            return found;
        })));
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void ServerInventory_ShowsEveryStatusWhole_AtTheMaximizedTabWidth()
    {
        var problems = StaTestThread.Run(() => WithDarkTheme(() => WithFinOpsTab(MaximizedTabWidth, 925, (tab, window) =>
        {
            var grid = (DataGrid)tab.FindName("FinOpsServerInventoryDataGrid");
            Reveal(grid);
            grid.ItemsSource = new[] { "UNDER_PROVISIONED", "OVER_PROVISIONED", "RIGHT_SIZED" }
                .Select(status => new ServerPropertyRow { ServerName = "inventory", ProvisioningStatus = status })
                .ToList();
            Settle(window);
            var cells = Cells(grid, "ProvisioningDisplay").ToList();
            var found = new List<string>();
            if (cells.Count != 3)
            {
                found.Add($"Server Inventory realized {cells.Count} Status cells, not 3");
            }
            found.AddRange(cells.Where(c => !Whole(c)).Select(c => $"Server Inventory: '{c.Text}' is cut in a {c.ActualWidth:0.#} px Status cell"));
            return found;
        })));
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void LockingAndContention_ShowsNoHorizontalScrollBar_WhileItHoldsNoRows()
    {
        var problems = StaTestThread.Run(() => WithDarkTheme(() => WithFinOpsTab(MinimumTabWidth, 600, (tab, window) =>
        {
            var grid = (DataGrid)tab.FindName("FinOpsIndexLockingDataGrid");
            Reveal(grid);
            Settle(window);
            return ScrollBarProblems(grid, "Locking & Contention");
        })));
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void JobHistory_TrimsLongJobAndStepNames_WithAnEllipsisAndTheWholeNameAsTooltip()
    {
        var problems = StaTestThread.Run(() => WithDarkTheme(() =>
        {
            var tab = new JobHistoryTab();
            var host = new Grid { Width = 900, Height = 600, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
            host.Children.Add(tab);
            var window = new Window { Content = host, Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("JobHistoryDataGrid");
                grid.ItemsSource = new List<ViewerJobHistoryRow>
                {
                    new() { JobName = "Log sp_WhoIsActive To A Daily Table For Later Review", StepId = 1, StepName = "Log sp_WhoIsActive To A Daily Table", RunStatusDesc = "Succeeded", Message = "The step succeeded." },
                };
                Settle(window);
                var found = new List<string>();
                found.AddRange(TrimProblems(grid, "JobName", "Job History"));
                found.AddRange(TrimProblems(grid, "StepDisplay", "Job History"));
                return found;
            }
            finally
            {
                window.Close();
            }
        }));
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheWaitTypePicker_TrimsALongWaitType_AndNeverScrollsSideways_AtTheMinimumWindow()
    {
        var problems = StaTestThread.Run(() => WithDarkTheme(() =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "wait-picker", "wait-picker", true, null));
            var window = new Window { Content = ChartLayoutProbe.HostLikeMainWindow(tab), Width = 900, Height = 600, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var main = (TabControl)tab.FindName("InnerTabs");
                main.SelectedItem = main.Items.OfType<TabItem>().Single(i => Equals(i.Header, "Wait Stats"));
                Settle(window);
                var list = (ListBox)tab.FindName("WaitTypesList");
                list.ItemsSource = WaitTypes();
                Settle(window);
                return PickerProblems(list, "Wait Stats");
            }
            finally
            {
                window.Close();
            }
        }));
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A long wait type between two short ones, each checked, as the picker lists them.</summary>
    private static List<SelectableItem> WaitTypes() =>
    [
        new() { DisplayName = "CXPACKET", IsSelected = true },
        new() { DisplayName = "PREEMPTIVE_OS_AUTHENTICATIONOPS_LONGER_THAN_THE_PANE", IsSelected = true },
        new() { DisplayName = "SOS_SCHEDULER_YIELD", IsSelected = true },
    ];

    /// <summary>The picker's list never scrolls sideways, and the entry too long for it trims with an ellipsis and carries the whole name as a tooltip.</summary>
    private static List<string> PickerProblems(ListBox list, string page)
    {
        var found = new List<string>();
        var scroller = Descendants(list).OfType<ScrollViewer>().FirstOrDefault();
        if (scroller is null)
        {
            found.Add($"{page}: the picker has no ScrollViewer");
        }
        else if (scroller.ScrollableWidth > 0.5 || scroller.ComputedHorizontalScrollBarVisibility == Visibility.Visible)
        {
            found.Add($"{page}: the picker scrolls {scroller.ScrollableWidth:0.#} px sideways to show its longest wait type");
        }
        var blocks = Descendants(list).OfType<TextBlock>().Where(b => b.IsVisible && WaitTypes().Any(w => w.DisplayName == b.Text)).ToList();
        if (blocks.Count != WaitTypes().Count)
        {
            found.Add($"{page}: the picker realized {blocks.Count} of {WaitTypes().Count} entries");
        }
        var longest = blocks.FirstOrDefault(b => b.Text.StartsWith("PREEMPTIVE", StringComparison.Ordinal));
        if (longest is not null)
        {
            if (!(ClippedTextProbe.NaturalWidth(longest) > longest.ActualWidth + 1))
            {
                found.Add($"{page}: '{longest.Text}' fits its {longest.ActualWidth:0.#} px entry, so the picker is wider than this test means it to be");
            }
            found.AddRange(TrimProblems(longest, page));
        }
        return found;
    }

    /// <summary>Every realized cell of the column is cut (its text is wider than the cell), trims with an ellipsis and carries its whole text as a tooltip.</summary>
    private static List<string> TrimProblems(DataGrid grid, string path, string page)
    {
        var found = new List<string>();
        var cells = Cells(grid, path).ToList();
        if (cells.Count == 0)
        {
            found.Add($"{page}: the grid realized no {path} cell");
        }
        foreach (var cell in cells)
        {
            if (!(ClippedTextProbe.NaturalWidth(cell) > cell.ActualWidth + 1))
            {
                found.Add($"{page}: '{cell.Text}' fits its {cell.ActualWidth:0.#} px {path} cell, so the column is wider than this test means it to be");
            }
            found.AddRange(TrimProblems(cell, page));
        }
        return found;
    }

    private static IEnumerable<string> TrimProblems(TextBlock block, string page)
    {
        if (block.TextTrimming != TextTrimming.CharacterEllipsis)
        {
            yield return $"{page}: '{block.Text}' is cut at {block.ActualWidth:0.#} px with no ellipsis (TextTrimming is {block.TextTrimming})";
        }
        if (block.ToolTip is not string tip || tip != block.Text)
        {
            yield return $"{page}: '{block.Text}' carries no tooltip with the whole text";
        }
    }

    /// <summary>The grid's horizontal scroll bar is not shown while the grid holds no rows.</summary>
    private static List<string> ScrollBarProblems(DataGrid grid, string page)
    {
        var found = new List<string>();
        if (grid.Items.Count != 0)
        {
            found.Add($"{page}: the grid holds {grid.Items.Count} rows, so this is not the empty state");
        }
        var scroller = Descendants(grid).OfType<ScrollViewer>().FirstOrDefault();
        if (scroller is null)
        {
            found.Add($"{page}: the grid has no ScrollViewer");
        }
        else if (scroller.ComputedHorizontalScrollBarVisibility == Visibility.Visible)
        {
            found.Add($"{page}: the empty grid shows a horizontal scroll bar ({scroller.ExtentWidth:0} px of columns in a {scroller.ViewportWidth:0} px viewport)");
        }
        return found;
    }

    private static IEnumerable<TextBlock> Cells(DataGrid grid, string path) =>
        Descendants(grid).OfType<DataGridCell>()
            .Where(c => c.Column is DataGridBoundColumn bound && bound.Binding is Binding binding && binding.Path.Path == path)
            .SelectMany(c => Descendants(c).OfType<TextBlock>())
            .Where(b => b.IsVisible);

    private static bool Whole(TextBlock block) => ClippedTextProbe.NaturalWidth(block) <= block.ActualWidth + 1;

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root) => ClippedTextProbe.Descendants(root);

    private static void Settle(Window window) => ClippedTextProbe.Settle(window);

    /// <summary>Selects the sub-tab that holds <paramref name="element"/>: a TabItem's content is laid out only while it is selected.</summary>
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

    private static List<string> WithFinOpsTab(int width, int height, Func<FinOpsTab, Window, List<string>> body)
    {
        var tab = new FinOpsTab();
        /* The tab is laid out at exactly width x height by a sized host, whatever the screen allows the window itself. */
        var host = new Grid { Width = width, Height = height, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
        host.Children.Add(tab);
        var window = new Window { Content = host, Width = width, Height = height, Left = 0, Top = 0 };
        StaTestThread.ShowOffScreen(window);
        try
        {
            Settle(window);
            return body(tab, window);
        }
        finally
        {
            window.Close();
        }
    }

    /// <summary>Runs <paramref name="body"/> with the Dark theme in the application scope, where a control's StaticResource lookups resolve at load.</summary>
    private static T WithDarkTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
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
