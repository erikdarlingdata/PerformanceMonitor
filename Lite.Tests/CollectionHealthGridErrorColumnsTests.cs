/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8101. The Collection Health grid's columns are fixed width, and on a wide display the two that explain a row's Errors count,
/// Last Error At and Last Error, sat past the right edge after the counts, the duration and the last-run instants. A row that
/// read HEALTHY beside Errors 1 said nothing visible about the error. Both apps carry a hand-duplicated copy of this grid, so this
/// pins both: the two error columns follow Status directly, ahead of every count, and the message is trimmed to its cell with the
/// whole text in a tooltip.
/// </summary>
public sealed class CollectionHealthGridErrorColumnsTests
{
    private const string LiteGrid = "Lite/Controls/ServerTab.xaml";
    private const string ViewerGrid = "Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml";

    [Theory]
    [InlineData(LiteGrid)]
    [InlineData(ViewerGrid)]
    public void TheNewestErrorAndItsTime_AreTheTwoColumnsAfterStatus(string gridPath)
    {
        var tags = ColumnTags(CollectionHealthGrid(gridPath));

        var status = tags.IndexOf("DisplayStatus");
        Assert.True(status >= 0, "the grid has a Status column");
        Assert.Equal(status + 1, tags.IndexOf("LastErrorFormatted"));
        Assert.Equal(status + 2, tags.IndexOf("LastError"));
        Assert.True(tags.IndexOf("TotalRuns") > tags.IndexOf("LastError"), "the counts follow the error columns");
    }

    [Theory]
    [InlineData(LiteGrid)]
    [InlineData(ViewerGrid)]
    public void TheLastErrorCell_TrimsToItsWidth_WithTheWholeMessageInItsTooltip(string gridPath)
    {
        var grid = CollectionHealthGrid(gridPath);

        Assert.Matches(
            new Regex(@"<TextBlock Text=""\{Binding LastError\}"" TextTrimming=""CharacterEllipsis"" ToolTip=""\{Binding LastError\}""", RegexOptions.Singleline),
            grid);
    }

    /// <summary>The Health Summary DataGrid's markup: from its name to its closing tag.</summary>
    private static string CollectionHealthGrid(string gridPath)
    {
        var xaml = ParitySource.ReadFile(gridPath);
        var start = xaml.IndexOf("x:Name=\"CollectionHealthGrid\"", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{gridPath} has a CollectionHealthGrid");
        var end = xaml.IndexOf("</DataGrid>", start, StringComparison.Ordinal);
        Assert.True(end > start, $"{gridPath}'s CollectionHealthGrid closes");
        return xaml.Substring(start, end - start);
    }

    /// <summary>The filter-button Tag of each column, in column order: the property the column shows.</summary>
    private static List<string> ColumnTags(string grid)
    {
        var tags = new List<string>();
        foreach (Match match in Regex.Matches(grid, @"Tag=""([A-Za-z]+)"" Click=""FilterButton_Click"""))
        {
            tags.Add(match.Groups[1].Value);
        }

        Assert.NotEmpty(tags);
        return tags;
    }
}
