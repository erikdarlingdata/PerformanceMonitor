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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8187: four details of the shared WPF code both apps use. A grid whose column filters hide every row says so and offers to clear
/// them; the Sick Spinlocks chart draws the five types with the most backoffs and says when more exist; the Overview crosshair prints a
/// lane's value the same way whether the lane holds one series or several; and the plan node tooltip and the plan arrow tooltip print an
/// estimate the same way. Every surface is the real control, built on the shared STA thread; private builders are reached by reflection
/// as <see cref="PlanViewerRuntimeSummaryNestingTests"/> reaches its card, and nothing reads source text (#5833).
/// </summary>
public sealed class SharedUiDetailsTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string FilteredOutWording = "No rows match the column filters";

    // ---- 1. a column filter that hides every row --------------------------------------------------------------------------------------

    private sealed class LoginRow
    {
        public string? LoginName { get; set; }
    }

    /// <summary>A grid in a Grid cell, as every surface in both apps is hosted, with one text column and a manager over it.</summary>
    private static (Grid Host, DataGrid Grid, DataGridFilterManager<LoginRow> Manager) FilteredGrid(string filterText)
    {
        var grid = new DataGrid { Name = "LoginsGrid", AutoGenerateColumns = false };
        grid.Columns.Add(new DataGridTextColumn { Header = "LoginName", Binding = new Binding("LoginName") });
        var host = new Grid();
        host.Children.Add(grid);
        var manager = new DataGridFilterManager<LoginRow>(grid);
        manager.UpdateData(new List<LoginRow> { new() { LoginName = "sa" }, new() { LoginName = "app" } });
        manager.SetFilter(new ColumnFilterState { ColumnName = "LoginName", Value = filterText });
        return (host, grid, manager);
    }

    /// <summary>The visible text in the grid's cell that says the filters hide every row, or null when there is none.</summary>
    private static TextBlock? FilteredOutMessage(Grid host)
        => host.Children.OfType<TextBlock>().FirstOrDefault(t =>
            t.Visibility == Visibility.Visible
            && new TextRange(t.ContentStart, t.ContentEnd).Text.Contains(FilteredOutWording, StringComparison.Ordinal));

    [Fact]
    public void AFilterThatHidesEveryRow_SaysSo_AndItsLinkClearsTheFilters()
    {
        var (messageShown, hasLink, rowsAfterClear, messageAfterClear) = StaTestThread.Run(() =>
        {
            var (host, grid, _) = FilteredGrid("nobody");
            Assert.Empty(grid.Items.OfType<LoginRow>());

            var message = FilteredOutMessage(host);
            var link = message?.Inlines.OfType<Hyperlink>().FirstOrDefault();
            link?.RaiseEvent(new RoutedEventArgs(Hyperlink.ClickEvent));

            return (message is not null, link is not null, grid.Items.OfType<LoginRow>().Count(), FilteredOutMessage(host));
        });

        Assert.True(messageShown, "a grid whose filters hide every row shows only a bare header row, with nothing that says why");
        Assert.True(hasLink, "the filtered-out message has no link that clears the filters");
        Assert.Equal(2, rowsAfterClear);
        Assert.Null(messageAfterClear);
    }

    [Fact]
    public void AFilterThatKeepsARow_ShowsNoFilteredOutMessage()
    {
        var (rows, message) = StaTestThread.Run(() =>
        {
            var (host, grid, _) = FilteredGrid("sa");
            return (grid.Items.OfType<LoginRow>().Count(), FilteredOutMessage(host));
        });

        Assert.Equal(1, rows);
        Assert.Null(message);
    }

    // ---- 2. Sick Spinlocks' top five ---------------------------------------------------------------------------------------------------

    private static readonly DateTime SampleUtc = new(2026, 1, 15, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>Renders the chart and returns the legend text of each series in legend order, and the chart's Y label.</summary>
    private static (List<string> Legend, string YLabel) SickSpinlocks(params (string Type, long Backoffs)[] samples)
        => StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var data = samples.Select((s, i) => new SystemHealthRecord { EventTime = SampleUtc.AddMinutes(i), SickSpinlockType = s.Type, SpinlockBackoffs = s.Backoffs }).ToList();
            new SystemHealthChartRenderer(new ChartRenderHelper(), t => t)
                .RenderSickSpinlocksChart(chart, null, data, SampleUtc.AddHours(-1).ToOADate(), SampleUtc.AddHours(1).ToOADate());
            var legend = chart.Plot.GetPlottables().OfType<ScottPlot.Plottables.Scatter>().Select(s => s.LegendText).ToList();
            return (legend, chart.Plot.Axes.Left.Label.Text);
        });

    [Fact]
    public void SickSpinlocks_DrawsTheFiveBusiestTypes_BusiestFirst_AndSaysHowManyThereWere()
    {
        /* Six types in time order; the busiest (SOS_CACHESTORE) arrives last, the quietest (LOCK_HASH) first. */
        var (legend, yLabel) = SickSpinlocks(
            ("LOCK_HASH", 1), ("XDESMGR", 10), ("SECURITY_CACHE", 20), ("LOGCACHE_ACCESS", 30), ("BUF_HASH", 40), ("SOS_CACHESTORE", 1000));

        Assert.Equal(5, legend.Count);
        Assert.Equal("SOS_CACHESTORE", legend[0]);
        Assert.DoesNotContain("LOCK_HASH", legend);
        Assert.Contains("5 of 6", yLabel, StringComparison.Ordinal);
    }

    [Fact]
    public void SickSpinlocks_WithFiveTypesOrFewer_DrawsThemAll_AndSaysNothingMore()
    {
        var (legend, yLabel) = SickSpinlocks(("LOCK_HASH", 1), ("XDESMGR", 10), ("SOS_CACHESTORE", 100));

        Assert.Equal(3, legend.Count);
        Assert.Equal("Sick Spinlocks (backoffs)", yLabel);
    }

    // ---- 3. the Overview crosshair's precision -----------------------------------------------------------------------------------------

    [Fact]
    public void ACrosshairLanesValue_ReadsTheSame_WithOneSeriesAndWithTwo()
    {
        var (single, multi) = StaTestThread.Run(() =>
        {
            var hook = typeof(CorrelatedCrosshairManager).GetMethod(
                "HoverLinesAt", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, new[] { typeof(ScottPlot.WPF.WpfPlot), typeof(double) });
            Assert.True(hook is not null,
                "the crosshair has no one place that prints a lane's value: the single-series and the multi-series branch each format it inline, to one decimal and to none");

            var times = new[] { 100.0, 101.0, 102.0 };
            var sqlCpu = new[] { 45.3, 45.3, 45.3 };
            var totalCpu = new[] { 61.7, 61.7, 61.7 };

            var one = new ScottPlot.WPF.WpfPlot();
            var two = new ScottPlot.WPF.WpfPlot();
            using var manager = new CorrelatedCrosshairManager();
            manager.AddLane(one, "SQL CPU", "%");
            manager.SetLaneData(one, times, sqlCpu);
            manager.AddLane(two, "SQL CPU", "%");
            manager.SetLaneData(two, times, sqlCpu);
            manager.AddLaneSeries(two, "Total CPU", "%", times, totalCpu);

            return (
                ((IReadOnlyList<string>)hook!.Invoke(manager, new object[] { one, 101.0 })!).ToList(),
                ((IReadOnlyList<string>)hook.Invoke(manager, new object[] { two, 101.0 })!).ToList());
        });

        Assert.Equal("SQL CPU: 45.3 %", Assert.Single(single));
        Assert.Equal(2, multi.Count);
        Assert.Equal(single[0], multi[0]);
        Assert.Equal("Total CPU: 61.7 %", multi[1]);
    }

    // ---- 4. the node and arrow tooltips' estimates --------------------------------------------------------------------------------------

    [Fact]
    public void TheNodeTooltip_AndTheArrowTooltip_PrintAFractionalEstimateTheSameWay()
    {
        var (node, arrow) = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            var operatorNode = new PlanNode { NodeId = 3, PhysicalOp = "Index Seek", LogicalOp = "Index Seek", EstimateRows = 1.1 };

            var buildNode = typeof(PlanViewerControl).GetMethod("BuildNodeTooltip", Instance);
            var buildEdge = typeof(PlanViewerControl).GetMethod("BuildEdgeTooltipContent", Instance);
            Assert.True(buildNode is not null && buildEdge is not null,
                "PlanViewerControl.BuildNodeTooltip or BuildEdgeTooltipContent no longer exists under that name - this test's reflection anchor moved.");

            var tip = (ToolTip)buildNode!.Invoke(control, new object?[] { operatorNode, null })!;
            var border = (Border)buildEdge!.Invoke(control, new object[] { operatorNode })!;
            return (NodeTooltipRows(tip)["Estimated Rows"], EdgeTooltipRows(border)["Estimated Number of Rows Per Execution"]);
        });

        Assert.Equal(node, arrow);
    }

    /// <summary>The node tooltip's rows by label: a horizontal StackPanel of a "label: " TextBlock and a value TextBlock each.</summary>
    private static Dictionary<string, string> NodeTooltipRows(ToolTip tip)
    {
        var stack = Assert.IsType<StackPanel>(tip.Content);
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var row in stack.Children.OfType<StackPanel>())
        {
            var cells = row.Children.OfType<TextBlock>().Select(t => t.Text).ToList();
            if (cells.Count == 2)
            {
                rows[cells[0].TrimEnd(' ', ':')] = cells[1];
            }
        }

        return rows;
    }

    /// <summary>The arrow tooltip's rows by label: a Grid with a label TextBlock in column 0 and a value TextBlock in column 1 per row.</summary>
    private static Dictionary<string, string> EdgeTooltipRows(Border border)
    {
        var grid = Assert.IsType<Grid>(border.Child);
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var row in grid.Children.OfType<TextBlock>().GroupBy(Grid.GetRow))
        {
            var cells = row.OrderBy(Grid.GetColumn).Select(t => t.Text).ToList();
            if (cells.Count == 2)
            {
                rows[cells[0]] = cells[1];
            }
        }

        return rows;
    }
}
