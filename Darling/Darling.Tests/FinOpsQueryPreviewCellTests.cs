/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Xml.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8110: the FinOps Query Preview cells in Lite and the Viewer show the statement on one line. The bound
/// preview is the head of the stored text with the statement's own line breaks and indentation, so a cell that
/// wrapped it under a 90 px cap showed a few scattered words and cut its last line. Each cell binds through
/// <c>OneLineTextConverter</c>, the #7382 cell form the Long Queries grids use: every run of whitespace is one
/// space, the line trims with an ellipsis at the column's edge, and the tooltip carries the whole statement.
/// The binding path stays on the row's preview property, so Copy Cell, Copy Row and the exports, which read
/// that property off the row, are unchanged.
/// </summary>
public sealed class FinOpsQueryPreviewCellTests
{
    private const string LiteTab = "Lite/Controls/FinOpsTab.xaml";
    private const string ViewerTab = "Darling/PerformanceMonitor.Darling.Viewer/FinOpsTab.xaml";

    private static readonly XNamespace Presentation = "http://schemas.microsoft.com/winfx/2006/xaml/presentation";
    private static readonly XNamespace Xaml = "http://schemas.microsoft.com/winfx/2006/xaml";
    private const string UiNamespace = "clr-namespace:PerformanceMonitor.Ui;assembly=PerformanceMonitor.Ui";

    [Theory]
    [InlineData(LiteTab, "ExpensiveQueriesDataGrid", "QueryPreview")]
    [InlineData(LiteTab, "HighImpactDataGrid", "SampleQueryText")]
    [InlineData(ViewerTab, "FinOpsExpensiveQueriesDataGrid", "QueryPreview")]
    [InlineData(ViewerTab, "FinOpsHighImpactDataGrid", "SampleQueryText")]
    public void TheQueryPreviewCell_IsOneTrimmedLineThroughTheConverter_WithTheWholeStatementInTheTooltip(
        string file, string grid, string property)
    {
        var cell = PreviewCell(Load(file), grid, property);

        Assert.Equal($"{{Binding {property}, Converter={{StaticResource OneLine}}}}", (string?)cell.Attribute("Text"));
        Assert.Equal("CharacterEllipsis", (string?)cell.Attribute("TextTrimming"));
        Assert.Null(cell.Attribute("TextWrapping"));
        Assert.Null(cell.Attribute("MaxHeight"));
        Assert.Equal("{Binding FullQueryText}", (string?)cell.Attribute("ToolTip"));
    }

    /* StaticResource resolves lexically at load (XamlStaticResourceHygieneTests), so the key the cells name is
       defined in the tab's own resources, as the two Server tabs define theirs. */
    [Theory]
    [InlineData(LiteTab)]
    [InlineData(ViewerTab)]
    public void TheOneLineConverter_IsDefinedInTheTabThatBindsThroughIt(string file)
    {
        var definition = Assert.Single(Load(file).Descendants(),
            e => e.Name.LocalName == "OneLineTextConverter" && (string?)e.Attribute(Xaml + "Key") == "OneLine");

        Assert.Equal(UiNamespace, definition.Name.NamespaceName);
    }

    private static XDocument Load(string file) => XDocument.Parse(RepoFile.ReadRepoFile(file.Split('/')));

    private static XElement PreviewCell(XDocument doc, string grid, string property)
    {
        var dataGrid = Assert.Single(doc.Descendants(Presentation + "DataGrid"),
            g => (string?)g.Attribute(Xaml + "Name") == grid);

        return Assert.Single(dataGrid.Descendants(Presentation + "TextBlock"),
            t => ((string?)t.Attribute("Text") ?? "").StartsWith($"{{Binding {property}", StringComparison.Ordinal));
    }
}
