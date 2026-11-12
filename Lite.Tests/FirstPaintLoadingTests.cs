/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using Darling.Tests;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8647: a sub-tab's surfaces say "Loading..." from the moment the tab is picked until their first read returns, and the empty-state
/// sentence appears only after a read that found no rows. A picker-driven chart whose list has nothing ticked says so ("No counters
/// picked"), and says "Loading..." while the list itself has not loaded.
/// </summary>
public sealed class FirstPaintLoadingTests
{
    private static List<string> Said(ScottPlot.WPF.WpfPlot chart) =>
        chart.Plot.GetPlottables<ScottPlot.Plottables.Text>().Select(t => t.LabelText)
            .Concat(chart.Plot.GetPlottables<ScottPlot.Plottables.Annotation>().Select(a => a.LabelText))
            .Select(t => t.Replace("\n", " ")).ToList();

    private static void DrawData(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Plot.Clear();
        chart.Plot.Add.Scatter(new[] { 1.0, 2.0 }, new[] { 3.0, 4.0 });
    }

    [Fact]
    public void ALoadingChart_IsTheEmptyLookWithLoadingAndNothingElse() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var helper = new ChartRenderHelper();
            helper.RefreshLoadingChart(chart, "Perfmon", "");

            Assert.Equal(new[] { "Loading..." }, Said(chart));
            Assert.False(chart.Plot.Axes.Left.IsVisible);
            Assert.False(chart.Plot.Grid.IsVisible);
            Assert.True(helper.IsShowingLoading(chart));
        });

    [Fact]
    public void TheEmptySentence_AppearsOnlyAfterAReadFoundNoRows_AndReplacesLoading() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var helper = new ChartRenderHelper();
            helper.RefreshLoadingChart(chart, "Perfmon", "");
            Assert.DoesNotContain("No Perfmon data in this window", Said(chart));

            /* The read returned no rows: the loader draws the empty look, with no Clear in between. */
            helper.RefreshEmptyChart(chart, "Perfmon", "");

            Assert.Equal(new[] { "No Perfmon data in this window" }, Said(chart));
            Assert.False(helper.IsShowingLoading(chart));
        });

    [Fact]
    public void AChartAReadDrewOn_KeepsItsData_WhenTheTabIsResolved() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var helper = new ChartRenderHelper();
            helper.RefreshLoadingChart(chart, "Perfmon", "");
            DrawData(chart);

            Assert.False(helper.IsShowingLoading(chart));
            Assert.False(helper.ResolveLoadingChart(chart));
            Assert.Single(chart.Plot.GetPlottables<ScottPlot.Plottables.Scatter>());
        });

    [Fact]
    public void ALoadingChartNoReadDrewOn_BecomesTheEmptyLook_WhenItsPassEnds() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var helper = new ChartRenderHelper();
            helper.RefreshLoadingChart(chart, "Latch Waits", "");

            Assert.True(helper.ResolveLoadingChart(chart));
            Assert.Equal(new[] { "No Latch Waits data in this window" }, Said(chart));
        });

    [Theory]
    [InlineData(false, 0, "Loading...")]
    [InlineData(false, 40, "Loading...")]
    [InlineData(true, 40, "No counters picked")]
    [InlineData(true, 0, "No Perfmon data in this window")]
    public void ANothingTickedChart_SaysWhichOfTheThreeFactsHolds(bool listLoaded, int listCount, string expected) =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            new ChartRenderHelper().RefreshNothingToPlotChart(chart, listLoaded, listCount, "Perfmon", "counters", "");
            Assert.Equal(new[] { expected }, Said(chart));
        });

    private static (Grid Root, ScottPlot.WPF.WpfPlot Chart, DataGrid Table, PerformanceCalendar Calendar) Tab()
    {
        var root = new Grid();
        var chart = new ScottPlot.WPF.WpfPlot();
        root.Children.Add(new ChartPanel { Title = "Perfmon", Content = chart });
        var table = new DataGrid();
        var cell = new Grid();
        cell.Children.Add(table);
        root.Children.Add(cell);
        var calendar = new PerformanceCalendar();
        root.Children.Add(calendar);
        return (root, chart, table, calendar);
    }

    [Fact]
    public void PickingATab_PutsLoadingOnItsChartGridAndCalendar_UntilTheirReadsReturn() =>
        StaTestThread.Run(() =>
        {
            var (root, chart, table, calendar) = Tab();
            var helper = new ChartRenderHelper();
            var first = new FirstPaintLoading(helper, _ => { });

            first.Show(root);

            Assert.Equal(new[] { "Loading..." }, Said(chart));
            Assert.True(EmptyState.IsShowingLoading(table));
            Assert.True(calendar.IsLoading);

            /* The grid's read returned no rows: the empty sentence takes the place of "Loading...". */
            table.ItemsSource = new List<string>();
            EmptyState.Show(table, isEmpty: true, "Nothing in this window.");
            Assert.False(EmptyState.IsShowingLoading(table));
            var cell = (Grid)table.Parent;
            var sentence = cell.Children.OfType<TextBlock>().Single(t => t.Name.StartsWith(EmptyState.ElementName));
            Assert.Equal("Nothing in this window.", sentence.Text);
            Assert.Equal(Visibility.Visible, sentence.Visibility);
        });

    [Fact]
    public void AGridWhoseRowsArriveWithNoEmptyStateCall_StillLosesLoading() =>
        StaTestThread.Run(() =>
        {
            var (root, _, table, _) = Tab();
            new FirstPaintLoading(new ChartRenderHelper(), _ => { }).Show(root);
            Assert.True(EmptyState.IsShowingLoading(table));

            table.ItemsSource = new List<string> { "a row" };

            Assert.False(EmptyState.IsShowingLoading(table));
        });

    [Fact]
    public void APickedAgain_TabKeepsTheRowsItHolds_NoLoadingOverThem() =>
        StaTestThread.Run(() =>
        {
            var (root, chart, table, calendar) = Tab();
            var helper = new ChartRenderHelper();
            var first = new FirstPaintLoading(helper, _ => { });
            first.Show(root);

            DrawData(chart);
            table.ItemsSource = new List<string> { "a row" };
            calendar.IsLoading = false;

            first.Show(root);

            Assert.Single(chart.Plot.GetPlottables<ScottPlot.Plottables.Scatter>());
            Assert.DoesNotContain("Loading...", Said(chart));
            Assert.False(EmptyState.IsShowingLoading(table));
            Assert.False(calendar.IsLoading);
        });

    /// <summary>#8728: the Queries sub-tabs keep a collapsed comparison grid in the main grid's cell. Its "Loading..." must not outlive the read.</summary>
    [Fact]
    public void ACollapsedComparisonGridInTheMainGridsCell_NeverLeavesLoadingOverTheMainGridsRows() =>
        StaTestThread.Run(() =>
        {
            var root = new Grid();
            var cell = new Grid();
            var main = new DataGrid { Name = "QueryStatsGrid" };
            var comparison = new DataGrid { Name = "QueryStatsComparisonGrid", Visibility = Visibility.Collapsed };
            cell.Children.Add(main);
            cell.Children.Add(comparison);
            root.Children.Add(cell);
            var first = new FirstPaintLoading(new ChartRenderHelper(), _ => { });
            first.Show(root);

            main.ItemsSource = new List<string> { "a row" };

            var window = new Window { Content = root, Width = 400, Height = 300 };
            try
            {
                StaTestThread.ShowOffScreen(window);
                first.Resolve(root);
            }
            finally
            {
                window.Close();
            }

            Assert.False(EmptyState.IsShowingLoading(main));
            Assert.False(EmptyState.IsShowingLoading(comparison));
            Assert.DoesNotContain(cell.Children.OfType<TextBlock>(), t => t.Text == EmptyState.LoadingText && t.Visibility == Visibility.Visible);
        });

    [Fact]
    public void WhenAPassEndsWithoutFillingATab_WhatIsOnScreenStopsSayingLoading() =>
        StaTestThread.Run(() =>
        {
            var (root, chart, table, calendar) = Tab();
            var helper = new ChartRenderHelper();
            var first = new FirstPaintLoading(helper, _ => { });
            first.Show(root);

            /* Nothing is laid out yet, so nothing is on screen: the pass that ends does not touch what no pass read. */
            first.Resolve(root);
            Assert.True(helper.IsShowingLoading(chart));
            Assert.True(EmptyState.IsShowingLoading(table));
            Assert.True(calendar.IsLoading);

            var window = new Window { Content = root, Width = 400, Height = 300 };
            try
            {
                StaTestThread.ShowOffScreen(window);
                first.Resolve(root);
            }
            finally
            {
                window.Close();
            }

            Assert.False(helper.IsShowingLoading(chart));
            Assert.Equal(new[] { "No Perfmon data in this window" }, Said(chart));
            Assert.False(EmptyState.IsShowingLoading(table));
            Assert.False(calendar.IsLoading);
        });
}
