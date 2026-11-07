/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Windows.Controls;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8185: an operator runs once, plus once per rebind and once per rewind, and every surface of the shared plan viewer counts it that
/// way. The Properties panel's Est. Executions and Est. Rows All Execs, the arrow tooltip's Estimated Number of Rows for All Executions
/// and the one helper behind them agree on an inner-side spool with 10 rebinds and 990 rewinds: 1,001 executions. The control is built
/// on the shared STA thread and its private builders are reached by reflection, as <see cref="PlanViewerRuntimeSummaryNestingTests"/>
/// reaches its card; nothing reads source text (#5833).
/// </summary>
public sealed class PlanViewerEstimatedExecutionsTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /* An inner-side lazy spool: 10 rows per run, rebound 10 times and rewound 990 times, so 1,001 runs and 10,010 rows in all. */
    private static PlanNode Spool() => new()
    {
        NodeId = 7,
        PhysicalOp = "Table Spool",
        LogicalOp = "Lazy Spool",
        EstimateRows = 10,
        EstimateRebinds = 10,
        EstimateRewinds = 990,
    };

    [Fact]
    public void ThePropertiesPanel_CountsRewindsAsExecutions_AndMultipliesRowsByThem()
    {
        var rows = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            ShowProperties(control, Spool());
            return PropertyRows(control);
        });

        Assert.Equal(1001.0.ToString("N0", CultureInfo.CurrentCulture), rows["Est. Executions"]);
        Assert.Equal(10010.0.ToString("N1", CultureInfo.CurrentCulture), rows["Est. Rows All Execs"]);
    }

    [Fact]
    public void ThePropertiesPanel_AndTheArrowTooltip_AgreeOnRowsForAllExecutions()
    {
        var (panel, arrow) = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            ShowProperties(control, Spool());
            var build = typeof(PlanViewerControl).GetMethod("BuildEdgeTooltipContent", Instance);
            Assert.True(build is not null, "PlanViewerControl.BuildEdgeTooltipContent no longer exists under that name - this test's reflection anchor moved.");
            var border = (Border)build!.Invoke(control, new object[] { Spool() })!;
            return (PropertyRows(control)["Est. Rows All Execs"], TooltipRows(border)["Estimated Number of Rows for All Executions"]);
        });

        Assert.Equal(Number(arrow), Number(panel));
    }

    private static void ShowProperties(PlanViewerControl control, PlanNode node)
    {
        var show = typeof(PlanViewerControl).GetMethod("ShowPropertiesPanel", Instance);
        Assert.True(show is not null, "PlanViewerControl.ShowPropertiesPanel no longer exists under that name - this test's reflection anchor moved.");
        show!.Invoke(control, new object[] { node });
    }

    private static double Number(string text) => double.Parse(text, NumberStyles.Any, CultureInfo.CurrentCulture);

    /// <summary>The Properties panel's rows by label: each is a Grid of a label TextBlock and a value TextBox inside a section's Expander.</summary>
    private static Dictionary<string, string> PropertyRows(PlanViewerControl control)
    {
        var content = control.FindName("PropertiesContent") as Panel;
        Assert.True(content is not null, "the plan viewer has no PropertiesContent panel - the Properties panel's host moved");

        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var grid in content!.Children.OfType<Expander>().Select(e => e.Content).OfType<Panel>().SelectMany(p => p.Children.OfType<Grid>()))
        {
            var label = grid.Children.OfType<TextBlock>().FirstOrDefault()?.Text;
            var value = grid.Children.OfType<TextBox>().FirstOrDefault()?.Text;
            if (label is not null && value is not null)
            {
                rows[label] = value;
            }
        }

        return rows;
    }

    /// <summary>The arrow tooltip's rows by label: a Grid with a label TextBlock in column 0 and a value TextBlock in column 1 per row.</summary>
    private static Dictionary<string, string> TooltipRows(Border border)
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
