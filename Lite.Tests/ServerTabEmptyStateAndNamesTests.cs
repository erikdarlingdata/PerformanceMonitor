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
[Trait("Reads", "Darling")]
public class ServerTabEmptyStateAndNamesTests
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

    [Theory]
    [InlineData("NoAlertsMessage")]
    [InlineData("NoJobsMessage")]
    [InlineData("NoDbSizesMessage")]
    [InlineData("NoStorageGrowthMessage")]
    public void EmptyState_LeavesASurfaceWithAnyOtherOwnMessageAlone_SoTwoTextsAreNeverDrawnOverEachOther(string siblingName)
    {
        var (sameCell, otherCell) = OnStaThread(() =>
        {
            var host = new Grid();
            var dataGrid = new DataGrid();
            host.Children.Add(dataGrid);
            host.Children.Add(new TextBlock { Name = siblingName });
            EmptyState.Show(dataGrid, isEmpty: true);
            var sameCellCount = EmptyTexts(host).Count;

            /* A message in ANOTHER cell does not stop the text. */
            var other = new Grid();
            var grid2 = new DataGrid();
            other.Children.Add(grid2);
            var message = new TextBlock { Name = siblingName };
            Grid.SetRow(message, 1);
            other.Children.Add(message);
            EmptyState.Show(grid2, isEmpty: true);
            return (sameCellCount, EmptyTexts(other).Count);
        });

        Assert.Equal(0, sameCell);
        Assert.Equal(1, otherCell);
    }

    [Fact]
    public void EmptyState_TheGenericGridWording_MatchesTheThemesNoDataText()
    {
        Assert.Equal("No data for the selected time range.", EmptyState.DefaultGridText);
        foreach (var theme in new[] { "Lite/Themes/DarkTheme.xaml", "Lite/Themes/LightTheme.xaml", "Lite/Themes/CoolBreezeTheme.xaml",
            "Darling/PerformanceMonitor.Darling.Viewer/Themes/DarkTheme.xaml", "Darling/PerformanceMonitor.Darling.Viewer/Themes/LightTheme.xaml", "Darling/PerformanceMonitor.Darling.Viewer/Themes/CoolBreezeTheme.xaml" })
        {
            Assert.Contains("Value=\"" + EmptyState.DefaultGridText + "\"", RepoFile(theme), StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ASlicerWithNoDataShowsItsTextOnTheChartCanvas_NotOverTheHeader_AndDropsThePreviousBars()
    {
        var (before, text, cleared) = OnStaThread(() =>
        {
            var slicer = new PerformanceMonitorLite.Controls.TimeRangeSlicerControl();
            var start = new DateTime(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);
            var buckets = Enumerable.Range(0, 6)
                .Select(i => new PerformanceMonitor.Common.TimeSliceBucket { BucketTime = start.AddHours(i), Value = i + 1 })
                .ToList();
            slicer.LoadData(buckets, "Total CPU (ms)", start, start.AddHours(6));
            var held = slicer.SelectionStartUtc;
            slicer.ShowEmpty("No query statistics in the selected time window.");
            return (held, slicer.EmptyText, slicer.SelectionStartUtc);
        });

        Assert.NotNull(before);
        Assert.Equal("No query statistics in the selected time window.", text);
        Assert.Null(cleared);

        foreach (var file in new[] { "Lite/Controls/TimeRangeSlicerControl.xaml.cs", "Darling/PerformanceMonitor.Darling.Viewer/TimeRangeSlicerControl.xaml.cs" })
        {
            var source = RepoFile(file);
            Assert.Contains("if (_data.Count < 1) { DrawEmptyText(); return; }", source, StringComparison.Ordinal);
            Assert.Contains("_emptyText = null;", source, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AccessibleNames_RecomputeAnAutoName_WhenTheHeaderTextChanges_ButNeverReplaceAHandSetOne()
    {
        var (first, second, handSet) = OnStaThread(() =>
        {
            var text = new TextBlock { Text = "SQL2022" };
            var panel = new StackPanel();
            panel.Children.Add(text);
            var tab = new TabItem { Header = panel };
            AccessibleNames.Name(tab, tab.Header);
            var firstName = AutomationProperties.GetName(tab);
            text.Text = "SQL2022 (renamed)";
            var secondName = AutomationProperties.GetName(tab);

            var other = new TextBlock { Text = "A" };
            var otherPanel = new StackPanel();
            otherPanel.Children.Add(other);
            var named = new TabItem { Header = otherPanel };
            AutomationProperties.SetName(named, "Alerts");
            AccessibleNames.Name(named, named.Header);
            other.Text = "B";
            return (firstName, secondName, AutomationProperties.GetName(named));
        });

        Assert.Equal("SQL2022", first);
        Assert.Equal("SQL2022 (renamed)", second);
        Assert.Equal("Alerts", handSet);
    }

    [Fact]
    public void AccessibleNames_GiveAColumnFilterButtonItsOwnName()
    {
        var buttonName = OnStaThread(() =>
        {
            var panel = new StackPanel { Orientation = Orientation.Horizontal };
            var button = new Button();
            panel.Children.Add(button);
            panel.Children.Add(new TextBlock { Text = "Duration (ms)" });
            var header = new System.Windows.Controls.Primitives.DataGridColumnHeader { Content = panel };
            AccessibleNames.Name(header, header.Content);
            return AutomationProperties.GetName(button);
        });

        Assert.Equal("Filter Duration (ms)", buttonName);
    }

    [Fact]
    public void TheOverviewCard_HasAnAutomationPeerThatCarriesItsName()
    {
        var (peerName, controlType) = OnStaThread(() =>
        {
            var card = new PerformanceMonitorLite.Controls.OverviewCardBorder();
            AutomationProperties.SetName(card, "example-sql-01");
            var peer = System.Windows.Automation.Peers.UIElementAutomationPeer.CreatePeerForElement(card);
            return (peer.GetName(), peer.GetAutomationControlType());
        });

        Assert.Equal("example-sql-01", peerName);
        Assert.Equal(System.Windows.Automation.Peers.AutomationControlType.Group, controlType);
        Assert.Contains("<controls:OverviewCardBorder", RepoFile("Lite/MainWindow.xaml"), StringComparison.Ordinal);
    }

    [Fact]
    public void EmptyState_ClearOfBaseline_PutsTheTextNearTheTop_AndTheDefaultStaysCentered()
    {
        var (centered, clear) = OnStaThread(() =>
        {
            var host = new Grid();
            var plain = new Border();
            host.Children.Add(plain);
            EmptyState.Show(plain, isEmpty: true, "No blocking in the selected time window.");
            var plainText = EmptyTexts(host).Single();

            var host2 = new Grid();
            var chart = new Border();
            host2.Children.Add(chart);
            EmptyState.Show(chart, isEmpty: true, "No blocking in the selected time window.", clearOfBaseline: true);
            var clearText = EmptyTexts(host2).Single();
            return ((plainText.VerticalAlignment, plainText.Margin.Top), (clearText.VerticalAlignment, clearText.Margin.Top));
        });

        Assert.Equal(VerticalAlignment.Center, centered.Item1);
        Assert.Equal(0d, centered.Item2);
        Assert.Equal(VerticalAlignment.Top, clear.Item1);
        Assert.True(clear.Item2 > 0d, "the text sits on the zero line again");

        foreach (var file in new[] { "Lite/Controls/ServerTab.Charts.cs", "Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Blocking.cs" })
        {
            var source = RepoFile(file);
            Assert.Contains("\"No blocking in the selected time window.\", clearOfBaseline: true)", source, StringComparison.Ordinal);
            Assert.Contains("\"No deadlocks in the selected time window.\", clearOfBaseline: true)", source, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "Binding=\"{Binding LogicalReads, StringFormat='{}{0:N0}'}\" Width=\"136\"")]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "Binding=\"{Binding PhysicalReads, StringFormat='{}{0:N0}'}\" Width=\"141\"")]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "Binding=\"{Binding CleanupState}\" Width=\"106\"")]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "StringFormat='{}{0:yyyy-MM-dd HH:mm:ss}'}\" Width=\"156\"")]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "Binding=\"{Binding SkippedLowWaterMark, StringFormat='{}{0:N0}'}\" Width=\"171\"")]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "Binding=\"{Binding DailyWriteOpsSaved}\" Width=\"166\"")]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "Header=\"Query Preview\" Width=\"350\"")]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "Binding=\"{Binding AvgLogicalReads, StringFormat='{}{0:N0}'}\" Width=\"161\"")]
    [InlineData("Lite/Controls/FinOpsTab.xaml", "Binding=\"{Binding CpuCount, StringFormat='{}{0:N0}'}\" Width=\"131\"")]
    [InlineData("Lite/Controls/JobHistoryTab.xaml", "Binding=\"{Binding RunStatusDesc}\" Width=\"100\"")]
    public void TheClippedColumns_KeepTheirWiderWidths(string file, string column)
    {
        Assert.Contains(column, RepoFile(file), StringComparison.Ordinal);
    }

    [Fact]
    public void TheSettingsWindow_KeepsSaveAndCloseBelowTheScrollArea()
    {
        var xaml = RepoFile("Lite/Windows/SettingsWindow.xaml");
        var scrollEnd = xaml.IndexOf("</ScrollViewer>", StringComparison.Ordinal);
        Assert.True(scrollEnd > 0);
        Assert.True(xaml.IndexOf("Click=\"SaveButton_Click\"", StringComparison.Ordinal) > scrollEnd, "Save sits inside the scroll area again");
        Assert.True(xaml.IndexOf("Click=\"CloseButton_Click\"", StringComparison.Ordinal) > scrollEnd);
    }

    [Fact]
    public void TheOverviewRefresh_ReadsEachServersClockOncePerRefresh()
    {
        var source = RepoFile("Lite/MainWindow.xaml.cs");
        Assert.Contains("var refreshClocks = new Dictionary<int, ServerClock>();", source, StringComparison.Ordinal);
        Assert.Contains("refreshClocks.TryGetValue(serverId, out var cached)", source, StringComparison.Ordinal);
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
        Assert.Contains("KeyboardNavigation.TabNavigation=\"Once\"", style, StringComparison.Ordinal);
        Assert.Contains("KeyboardNavigation.DirectionalNavigation=\"Cycle\"", style, StringComparison.Ordinal);
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
        Assert.Contains("<Run Text=\"events\"/>", xaml, StringComparison.Ordinal);
        Assert.DoesNotContain("<Run Text=\"events\" FontWeight", xaml, StringComparison.Ordinal);
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
            Assert.Contains(slicer + ".ShowEmpty(", slicers, StringComparison.Ordinal);
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
