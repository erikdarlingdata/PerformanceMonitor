/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Windows;
using System.Windows.Automation;
using System.Windows.Controls;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite click-through F15 (empty grids and charts say so), F17 (tabs and column headers have accessible names), F18 (the tab
/// strip never reorders its rows) and F19 (lane labels carry units).
/// </summary>
public class ServerTabEmptyStateAndNamesTests
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
            throw error;
        }

        return result;
    }

    private static string RepoFile(string relative, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", relative)));

    private static List<TextBlock> EmptyTexts(Grid grid) =>
        grid.Children.OfType<TextBlock>().Where(t => t.Name.StartsWith(EmptyState.ElementName, StringComparison.Ordinal)).ToList();

    [Fact]
    public void EmptyState_ShowsItsTextOverAnEmptyGrid_AndHidesItWhenRowsArrive()
    {
        var (shownEmpty, text, shownWithRows) = OnStaThread(() =>
        {
            var host = new Grid();
            var dataGrid = new DataGrid();
            host.Children.Add(dataGrid);
            EmptyState.SetText(dataGrid, "No deadlocks in the selected time window.");

            EmptyState.Show(dataGrid, isEmpty: true);
            var block = Assert.Single(EmptyTexts(host));
            var visibleEmpty = block.Visibility == Visibility.Visible;

            EmptyState.Show(dataGrid, isEmpty: false);

            return (visibleEmpty, block.Text, EmptyTexts(host).Single().Visibility == Visibility.Visible);
        });

        Assert.True(shownEmpty);
        Assert.Equal("No deadlocks in the selected time window.", text);
        Assert.False(shownWithRows);
    }

    [Fact]
    public void EmptyState_AGridThatNeverIsEmpty_GetsNoElement_AndAChartInABorderIsAnchoredOnTheBorder()
    {
        var (neverEmptyCount, borderRow, borderBlockRow) = OnStaThread(() =>
        {
            var host = new Grid();
            var rows = new DataGrid();
            host.Children.Add(rows);
            EmptyState.Show(rows, isEmpty: false);
            var neverEmpty = EmptyTexts(host).Count;

            var wrapped = new Border { Child = new Canvas { Name = "FakeChart" } };
            Grid.SetRow(wrapped, 3);
            host.Children.Add(wrapped);
            EmptyState.Show((FrameworkElement)wrapped.Child, isEmpty: true, "No blocking in the selected time window.");

            return (neverEmpty, Grid.GetRow(wrapped), Grid.GetRow(EmptyTexts(host).Single()));
        });

        Assert.Equal(0, neverEmptyCount);
        Assert.Equal(borderRow, borderBlockRow);
    }

    [Fact]
    public void EmptyState_LeavesASurfaceWithItsOwnNoDataMessageAlone()
    {
        var count = OnStaThread(() =>
        {
            var host = new Grid();
            var dataGrid = new DataGrid();
            host.Children.Add(dataGrid);
            host.Children.Add(new TextBlock { Name = "SevereErrorsNoDataMessage" });

            EmptyState.Show(dataGrid, isEmpty: true);
            return EmptyTexts(host).Count;
        });

        Assert.Equal(0, count);
    }

    [Fact]
    public void AccessibleNames_NameAPanelHeaderFromItsTitle_NotFromItsFilterButton()
    {
        var (tabName, headerName, handNamed) = OnStaThread(() =>
        {
            var panel = new StackPanel { Orientation = Orientation.Horizontal };
            panel.Children.Add(new Button());
            panel.Children.Add(new TextBlock { Text = "Duration (ms)" });
            var columnHeader = new System.Windows.Controls.Primitives.DataGridColumnHeader { Content = panel };
            AccessibleNames.Name(columnHeader, columnHeader.Content);

            var tabHeader = new StackPanel();
            tabHeader.Children.Add(new TextBlock { Text = "SQL2022" });
            tabHeader.Children.Add(new Button { Content = "x" });
            var tab = new TabItem { Header = tabHeader };
            AccessibleNames.Name(tab, tab.Header);

            var otherHeader = new StackPanel();
            otherHeader.Children.Add(new TextBlock { Text = "SQL2025" });
            var named = new TabItem { Header = otherHeader };
            AutomationProperties.SetName(named, "Alerts");
            AccessibleNames.Name(named, named.Header);

            return (AutomationProperties.GetName(tab), AutomationProperties.GetName(columnHeader), AutomationProperties.GetName(named));
        });

        Assert.Equal("SQL2022", tabName);
        Assert.Equal("Duration (ms)", headerName);
        Assert.Equal("Alerts", handNamed);
    }

    [Theory]
    [InlineData("Lite/Themes/CoolBreezeTheme.xaml")]
    [InlineData("Lite/Themes/DarkTheme.xaml")]
    [InlineData("Lite/Themes/LightTheme.xaml")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/CoolBreezeTheme.xaml")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/DarkTheme.xaml")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/Themes/LightTheme.xaml")]
    public void TheTabControlTemplate_UsesAWrapPanel_SoTheSelectedRowNeverMoves(string theme)
    {
        var xaml = RepoFile(theme);
        var start = xaml.IndexOf("<Style TargetType=\"TabControl\">", StringComparison.Ordinal);
        Assert.True(start >= 0, "the TabControl style moved");
        var style = xaml.Substring(start, xaml.IndexOf("</Style>", start, StringComparison.Ordinal) - start);

        Assert.Contains("<WrapPanel", style, StringComparison.Ordinal);
        Assert.Contains("IsItemsHost=\"True\"", style, StringComparison.Ordinal);
        Assert.DoesNotContain("<TabPanel", style, StringComparison.Ordinal);
        Assert.Contains("PART_SelectedContentHost", style, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Lite/Controls/CorrelatedTimelineLanesControl.xaml")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/CorrelatedTimelineLanesControl.xaml")]
    public void TheOverviewLaneLabels_CarryTheirUnits(string file)
    {
        var xaml = RepoFile(file);

        Assert.Contains("Text=\"I/O Latency ms\"", xaml, StringComparison.Ordinal);
        Assert.Contains("<Run Text=\"events\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Text=\"CPU %\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Text=\"Buffer Pool MB\"", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryFilterManagerGrid_SaysSoWhenItComesBackEmpty_AndTheSlicersAndChartsDoToo()
    {
        Assert.Contains("EmptyState.Show(_dataGrid, newData.Count == 0);", RepoFile("PerformanceMonitor.Ui/DataGridFilterManager.cs"), StringComparison.Ordinal);

        var slicers = RepoFile("Lite/Controls/ServerTab.Slicers.cs");
        foreach (var slicer in new[] { "ActiveQueriesSlicer", "QueryStatsSlicer", "QueryStoreSlicer", "ProcStatsSlicer" })
        {
            Assert.Contains("EmptyState.Show(" + slicer + ", data.Count == 0", slicers, StringComparison.Ordinal);
        }

        var charts = RepoFile("Lite/Controls/ServerTab.Charts.cs");
        foreach (var chart in new[] { "MemoryGrantSizingChart", "MemoryGrantActivityChart", "BlockingTrendChart", "DeadlockTrendChart" })
        {
            Assert.Contains("EmptyState.Show(" + chart + ", data.Count == 0", charts, StringComparison.Ordinal);
        }

        Assert.Contains("pressureRows.Count, keepsOwnEmptyText: true", charts, StringComparison.Ordinal);

        var filters = RepoFile("Lite/Controls/ServerTab.Filters.cs");
        foreach (var grid in new[] { "QuerySnapshotsGrid", "ProcedureStatsGrid", "BlockedProcessReportGrid", "DeadlockGrid" })
        {
            Assert.Contains("EmptyState.SetText(" + grid + ",", filters, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void BothAppsRegisterTheAccessibleNameHandlersAtStartup()
    {
        Assert.Contains("AccessibleNames.Register();", RepoFile("Lite/App.xaml.cs"), StringComparison.Ordinal);
        Assert.Contains("AccessibleNames.Register();", RepoFile("Darling/PerformanceMonitor.Darling.Viewer/App.xaml.cs"), StringComparison.Ordinal);
    }
}
