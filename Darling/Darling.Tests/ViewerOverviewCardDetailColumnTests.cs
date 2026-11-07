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
using System.Runtime.CompilerServices;
using System.Xml.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8112: the Overview card's metric grid is [dot] [label] [value] [detail] at a fixed card width, and the first three columns are
/// Auto. Any row whose label or value is wider than the core rows' widens a column every row shares, and the star detail column
/// is what gives way. Read straight from MainWindow.xaml with XDocument, the way ViewerButtonAccessKeyTests reads its windows:
/// the optional rows (Agent alerts, Identity capacity, Protection, Database state) must reach the detail column, so their width
/// is their own, and every detail cell must trim with an ellipsis and carry its full text as a tooltip. Lite's Overview card is
/// a two-column grid (label, value) with no detail cell, so it has no twin here.
/// </summary>
public sealed class ViewerOverviewCardDetailColumnTests
{
    private static readonly XNamespace P = "http://schemas.microsoft.com/winfx/2006/xaml/presentation";
    private static readonly XNamespace X = "http://schemas.microsoft.com/winfx/2006/xaml";

    private static readonly string[] DetailBindings =
    {
        "CpuDetail", "ThreadsDetail", "MemoryDetail", "BlockingDetail", "DeadlockDetail", "CollectorDetail",
    };

    private static readonly string[] OptionalRowFlags =
    {
        "HasAgentAlertGaps", "HasIdentityCapacity", "HasDatabaseProtectionOverdue", "HasDatabaseStateDeviations",
    };

    private static string MainWindowXaml([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));

    /// <summary>The card template's metric grid: the Grid that holds the "CPU:" label inside the Overview items control.</summary>
    private static XElement MetricGrid()
    {
        var document = XDocument.Load(MainWindowXaml());
        var items = document.Descendants().Single(e => (string?)e.Attribute(X + "Name") == "OverviewItemsControl");
        var cpuLabel = items.Descendants(P + "TextBlock").Single(t => (string?)t.Attribute("Text") == "CPU:");
        var grid = cpuLabel.Parent!;
        Assert.Equal(P + "Grid", grid.Name);
        return grid;
    }

    private static List<XElement> Columns(XElement grid) =>
        grid.Element(P + "Grid.ColumnDefinitions")!.Elements(P + "ColumnDefinition").ToList();

    private static int StarColumn(XElement grid)
    {
        var columns = Columns(grid);
        var star = columns.FindIndex(c => ((string?)c.Attribute("Width") ?? "").Contains('*'));
        Assert.True(star >= 0, "the metric grid has no star column");
        return star;
    }

    private static int Column(XElement cell) => int.Parse((string?)cell.Attribute("Grid.Column") ?? "0");

    private static int ColumnSpan(XElement cell) => int.Parse((string?)cell.Attribute("Grid.ColumnSpan") ?? "1");

    [Fact]
    public void EveryDetailCell_SitsInTheStarColumn_TrimsWithAnEllipsis_AndCarriesItsFullTextAsATooltip()
    {
        var grid = MetricGrid();
        var star = StarColumn(grid);
        foreach (var binding in DetailBindings)
        {
            var text = "{Binding " + binding + "}";
            var cell = grid.Descendants(P + "TextBlock").Single(t => (string?)t.Attribute("Text") == text);
            Assert.True(Column(cell) + ColumnSpan(cell) - 1 == star, binding + ": the detail cell is in column " + Column(cell) + ", not the star column " + star);
            Assert.True((string?)cell.Attribute("TextTrimming") == "CharacterEllipsis",
                binding + ": TextTrimming is " + ((string?)cell.Attribute("TextTrimming") ?? "unset") + " and TextWrapping is " + ((string?)cell.Attribute("TextWrapping") ?? "unset") + ", so the detail breaks mid-word or is cut without a mark");
            Assert.True((string?)cell.Attribute("ToolTip") == text,
                binding + ": ToolTip is " + ((string?)cell.Attribute("ToolTip") ?? "unset") + ", so a trimmed detail cannot be read on hover");
        }
    }

    [Fact]
    public void TheOptionalRows_ReachTheStarColumn_SoTheyWidenNeitherTheLabelNorTheValueColumn()
    {
        var grid = MetricGrid();
        var star = StarColumn(grid);
        foreach (var flag in OptionalRowFlags)
        {
            var cells = grid.Elements()
                .Where(e => ((string?)e.Attribute("Visibility") ?? "").Contains("Binding " + flag + ",", StringComparison.Ordinal))
                .Where(e => e.Name != P + "Ellipse")
                .ToList();
            Assert.True(cells.Count > 0, flag + ": no text cell in the metric grid is shown by it");
            foreach (var cell in cells)
            {
                Assert.True(Column(cell) + ColumnSpan(cell) - 1 >= star,
                    flag + ": a " + cell.Name.LocalName + " in column " + Column(cell) + " spanning " + ColumnSpan(cell) + " ends before the star column " + star + ", so its width sizes an Auto column every row shares and the detail column gives way");
            }
        }
    }

    [Fact]
    public void TheDetailColumn_IsTheLastColumn_AndTheOnlyStar()
    {
        var grid = MetricGrid();
        var columns = Columns(grid);
        Assert.Equal(4, columns.Count);
        Assert.Equal(new[] { false, false, false, true }, columns.Select(c => ((string?)c.Attribute("Width") ?? "").Contains('*')).ToArray());
    }
}
