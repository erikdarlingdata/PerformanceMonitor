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
using System.Xml.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8099: the server-tab grids keep the figures a reader opens them for inside a 1,536 px viewport (a 3840 px display
/// at 250 % scaling). Two column shapes pushed them off. The Query Store Health grid gave every column a fixed width,
/// 1,715 px in all, with 190 px on a Read-Only Reason that is blank for every READ_WRITE store and 200 px on a one-word
/// State, so Stale Days, Max Plans/Query and Interval Min sat past the right edge. The query grids gave Query Text a
/// 500 or 560 px cell that wrapped to 90 px tall, so each row was a block of cut SQL and the duration, CPU and
/// execution columns were off screen. Both server tabs (Lite's <c>ServerTab.xaml</c> and the Viewer's
/// <c>ViewerServerTab.xaml</c>) are read here column by column; the two carry the same grids under the same names,
/// and the Viewer has one more, its current active queries.
/// </summary>
public sealed class QueryGridColumnLayoutTests
{
    private static readonly XNamespace X = "http://schemas.microsoft.com/winfx/2006/xaml";

    /// <summary>The widest a one-line Query Text cell is: enough to recognise a statement, with the full text in the
    /// tooltip and the row's detail view. The same width the Viewer's other one-line statement columns use.</summary>
    private const int QueryTextMaxWidth = 320;

    /// <summary>A maximized window on a 3840 px display at 250 % scaling.</summary>
    private const int Viewport = 1536;

    private const string Lite = "Lite";
    private const string Viewer = "Viewer";

    /// <summary>The query grids in each app, with the sort member their Query Text column carries (only Plan Corrections
    /// sorts by text; the others sort by their figures) and whether its header has a filter button (the comparison grids
    /// and the Viewer's current active queries have a plain header).</summary>
    public static IEnumerable<object?[]> QueryTextGrids()
    {
        foreach (var app in new[] { Lite, Viewer })
        {
            yield return new object?[] { app, "QuerySnapshotsGrid", null, true };
            yield return new object?[] { app, "QueryStatsGrid", null, true };
            yield return new object?[] { app, "QueryStatsComparisonGrid", null, false };
            yield return new object?[] { app, "ProcStatsComparisonGrid", null, false };
            yield return new object?[] { app, "QueryStoreGrid", null, true };
            yield return new object?[] { app, "QueryStoreComparisonGrid", null, false };
            yield return new object?[] { app, "PlanCorrectionGrid", "QueryText", true };
            yield return new object?[] { app, "SignificantWaitsGrid", null, true };
        }

        yield return new object?[] { Viewer, "CurrentActiveQueriesGrid", null, false };
    }

    [Theory]
    [MemberData(nameof(QueryTextGrids))]
    public void QueryText_IsOneTrimmedLine_NoWiderThanAStatementNeedsToBeRecognised(string app, string grid, string? sortMemberPath, bool hasFilter)
    {
        var column = QueryTextColumn(Grid(app, grid));

        var width = FixedWidth(column);
        Assert.True(width is not null && width <= QueryTextMaxWidth,
            $"{app} {grid}: Query Text is Width=\"{column.Attribute("Width")?.Value}\", wider than {QueryTextMaxWidth} px, so the figures to its right start past the viewport");

        var cell = Assert.Single(column.Descendants().Where(e => e.Name.LocalName == "DataTemplate").Descendants(), e => e.Name.LocalName == "TextBlock");
        Assert.Equal("CharacterEllipsis", (string?)cell.Attribute("TextTrimming"));
        Assert.Null(cell.Attribute("TextWrapping"));
        Assert.Null(cell.Attribute("MaxHeight"));
        Assert.Equal("{Binding QueryText, Converter={StaticResource OneLine}}", (string?)cell.Attribute("Text"));

        /* The full statement, line breaks and all, stays one hover away; Copy Cell and Copy Row read the bound property,
           not the cell's text, so they keep it too. */
        Assert.Equal("{Binding QueryText}", (string?)cell.Attribute("ToolTip"));

        /* The header's filter button, where the grid has one, and the column's sort member are as they were. */
        var filter = column.Descendants().Where(e => e.Name.LocalName == "Button").SingleOrDefault(b => (string?)b.Attribute("Tag") == "QueryText");
        Assert.True(hasFilter == (filter is not null), $"{app} {grid}: the Query Text header {(hasFilter ? "lost" : "gained")} a filter button");
        Assert.Equal(sortMemberPath, (string?)column.Attribute("SortMemberPath"));
    }

    [Theory]
    [InlineData(Lite)]
    [InlineData(Viewer)]
    public void QueryStoreHealth_FixedColumnsFitTheViewport_AndTheBlankByDefaultColumnsTakeOnlyWhatTheyShow(string app)
    {
        var columns = Columns(Grid(app, "QueryStoreHealthGrid"));
        Assert.True(columns.Count >= 13, $"{app}: only {columns.Count} Query Store Health columns parsed, so the scan is broken");

        /* Read-Only Reason is blank for every READ_WRITE store, and State is one word unless the store is in error or
           not in the state it was asked for: each sizes to what it shows rather than reserving a fixed strip. */
        var reason = Column(columns, "ReadonlyReasonDisplay");
        Assert.True(IsAuto(reason), $"{app}: Read-Only Reason is Width=\"{reason.Attribute("Width")?.Value}\", a fixed strip that is blank on most rows");
        var state = Column(columns, "StateDisplay");
        Assert.True(IsAuto(state) || FixedWidth(state) <= 120, $"{app}: State is Width=\"{state.Attribute("Width")?.Value}\" for a one-word value");

        var fixedWidths = columns.Sum(c => FixedWidth(c) ?? 0);
        Assert.True(fixedWidths <= Viewport, $"{app}: the fixed-width Query Store Health columns total {fixedWidths} px against a {Viewport} px viewport");

        /* The settings a reader opens the grid for come before the two capture modes and Collected, which trail as the
           store columns and the MCP payload do. */
        var order = columns.Select(BindingPath).ToList();
        Assert.True(order.IndexOf("StaleQueryThresholdDays") < order.IndexOf("CaptureModeDisplay"), $"{app}: Stale Days sits after Capture Mode");
        Assert.True(order.IndexOf("MaxPlansPerQuery") < order.IndexOf("CaptureModeDisplay"), $"{app}: Max Plans/Query sits after Capture Mode");
        Assert.True(order.IndexOf("IntervalLengthMinutes") < order.IndexOf("CaptureModeDisplay"), $"{app}: Interval Min sits after Capture Mode");
    }

    private static XElement Grid(string app, string name)
    {
        var xaml = app == Lite
            ? RepoFile.ReadRepoFile("Lite", "Controls", "ServerTab.xaml")
            : RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");
        var grid = XDocument.Parse(xaml).Descendants()
            .SingleOrDefault(e => e.Name.LocalName == "DataGrid" && (string?)e.Attribute(X + "Name") == name);
        Assert.True(grid is not null, $"{app} has no DataGrid named {name}");
        return grid!;
    }

    /// <summary>The grid's columns in display order, comments skipped.</summary>
    private static List<XElement> Columns(XElement grid) =>
        grid.Elements().Single(e => e.Name.LocalName == "DataGrid.Columns").Elements().ToList();

    private static XElement Column(List<XElement> columns, string bindingPath)
    {
        var column = columns.SingleOrDefault(c => BindingPath(c) == bindingPath);
        Assert.True(column is not null, $"no column bound to {bindingPath}");
        return column!;
    }

    /// <summary>The Query Text column: the template column whose cell binds <c>QueryText</c>.</summary>
    private static XElement QueryTextColumn(XElement grid)
    {
        var column = Columns(grid).SingleOrDefault(c =>
            c.Name.LocalName == "DataGridTemplateColumn"
            && c.Descendants().Any(e => e.Name.LocalName == "TextBlock" && ((string?)e.Attribute("Text") ?? "").StartsWith("{Binding QueryText", StringComparison.Ordinal)));
        Assert.True(column is not null, $"{(string?)grid.Attribute(X + "Name")} has no Query Text column");
        return column!;
    }

    /// <summary>The property a text column binds, read from its <c>Binding</c> attribute: <c>{Binding Path, ...}</c>.</summary>
    private static string? BindingPath(XElement column)
    {
        var binding = (string?)column.Attribute("Binding");
        if (binding is null || !binding.StartsWith("{Binding ", StringComparison.Ordinal))
        {
            return null;
        }

        return binding["{Binding ".Length..].TrimEnd('}').Split(',')[0].Trim();
    }

    /// <summary>The column's fixed pixel width, or null when it is Auto, star or header sized.</summary>
    private static int? FixedWidth(XElement column) =>
        int.TryParse((string?)column.Attribute("Width"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var px) ? px : null;

    private static bool IsAuto(XElement column) => string.Equals((string?)column.Attribute("Width"), "Auto", StringComparison.Ordinal);
}
