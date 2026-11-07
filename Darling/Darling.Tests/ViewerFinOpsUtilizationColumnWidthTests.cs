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
using System.IO;
using System.Linq;
using System.Xml.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8113: the FinOps Utilization sub-tab's two "Top Databases" grids show execution counts and CPU totals that reach ten or more digits
/// on a busy server. A numeric column with a fixed width narrower than its widest value is clipped at the cell edge with no ellipsis, so
/// "115,538,069" reads as "115,538,06", a plausible number a tenth of the real one. Every unbounded numeric column in those grids either
/// sizes to its content (Width="Auto") or declares a fixed width that holds twelve digits with group separators. Percentages are bounded
/// at 100.0 and are exempt. Lite's twin is FinOpsUtilizationColumnWidthTests in Lite.Tests.
/// </summary>
public sealed class ViewerFinOpsUtilizationColumnWidthTests
{
    /// <summary>The pixels a twelve-digit value with three group separators needs at the grid's font, plus the cell padding.</summary>
    private const double TwelveDigitWidth = 110;

    private static string FinOpsTabXaml() => RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");

    [Theory]
    [InlineData("FinOpsTopTotalGrid")]
    [InlineData("FinOpsTopAvgGrid")]
    public void UtilizationGrid_GivesEveryUnboundedNumericColumnRoomForTwelveDigits(string gridName)
    {
        var doc = XDocument.Load(FinOpsTabXaml());
        XNamespace x = "http://schemas.microsoft.com/winfx/2006/xaml";
        var grid = doc.Descendants().Single(e => e.Name.LocalName == "DataGrid" && (string?)e.Attribute(x + "Name") == gridName);
        var columns = grid.Descendants().Where(e => e.Name.LocalName == "DataGridTextColumn").ToList();
        Assert.NotEmpty(columns);

        var tooNarrow = new List<string>();
        var numeric = 0;
        foreach (var column in columns)
        {
            var binding = (string?)column.Attribute("Binding") ?? "";
            var header = (string?)column.Attribute("Header") ?? "";
            if (!binding.Contains("StringFormat=N", StringComparison.Ordinal) || header.Contains('%'))
            {
                continue;
            }

            numeric++;
            var width = (string?)column.Attribute("Width") ?? "Auto";
            if (double.TryParse(width, NumberStyles.Float, CultureInfo.InvariantCulture, out var pixels) && pixels < TwelveDigitWidth)
            {
                tooNarrow.Add($"{header}: Width=\"{width}\"");
            }
        }

        Assert.True(numeric > 0, $"{gridName} has no numeric column; the grid changed shape and this test needs a new target.");
        Assert.True(tooNarrow.Count == 0, $"{gridName} clips a long number in: {string.Join(", ", tooNarrow)}. Size the column to its content (Width=\"Auto\" with a MinWidth) or give it at least {TwelveDigitWidth} px.");
    }
}
