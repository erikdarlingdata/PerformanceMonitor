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
using System.Runtime.CompilerServices;
using System.Xml.Linq;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8113: the FinOps grids show execution counts, CPU totals, reads and sizes that reach ten or more digits on a busy server. A numeric
/// column with a fixed width narrower than its widest value is clipped at the cell edge with no ellipsis, so "115,538,069" reads as
/// "115,538,06", a plausible number a tenth of the real one. Every unbounded numeric column in every FinOps grid either sizes to its
/// content (Width="Auto") or declares a fixed width that holds twelve digits with group separators. Percentages are bounded at 100.0 and
/// a column whose element style trims with an ellipsis cannot be misread, so both are exempt. The Darling Viewer's twin is
/// ViewerFinOpsUtilizationColumnWidthTests in Darling.Tests.
/// </summary>
public sealed class FinOpsUtilizationColumnWidthTests
{
    /// <summary>The pixels a twelve-digit value with three group separators needs at the grid's font, plus the cell padding.</summary>
    private const double TwelveDigitWidth = 110;

    private static readonly XNamespace X = "http://schemas.microsoft.com/winfx/2006/xaml";

    private static string FinOpsTabXaml([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls", "FinOpsTab.xaml"));

    [Fact]
    public void EveryFinOpsGrid_GivesEveryUnboundedNumericColumnRoomForTwelveDigits()
    {
        var doc = XDocument.Load(FinOpsTabXaml());
        var grids = doc.Descendants().Where(e => e.Name.LocalName == "DataGrid" && e.Attribute(X + "Name") is not null).ToList();
        Assert.NotEmpty(grids);

        var tooNarrow = new List<string>();
        var gridsWithNumbers = 0;
        foreach (var grid in grids)
        {
            var gridName = (string)grid.Attribute(X + "Name")!;
            var numeric = 0;
            foreach (var column in grid.Descendants().Where(e => e.Name.LocalName == "DataGridTextColumn"))
            {
                var binding = (string?)column.Attribute("Binding") ?? "";
                var header = HeaderText(column);
                if (NumericFormat(binding) is null || IsPercentage(header, binding) || TrimsWithEllipsis(column))
                {
                    continue;
                }

                numeric++;
                var width = (string?)column.Attribute("Width") ?? "Auto";
                if (double.TryParse(width, NumberStyles.Float, CultureInfo.InvariantCulture, out var pixels) && pixels < TwelveDigitWidth)
                {
                    tooNarrow.Add($"{gridName} / {header}: Width=\"{width}\"");
                }
            }

            if (numeric > 0)
            {
                gridsWithNumbers++;
            }
        }

        Assert.True(gridsWithNumbers >= 10, $"Only {gridsWithNumbers} FinOps grids have a numeric column; the tab changed shape and this test needs a new reading.");
        Assert.True(tooNarrow.Count == 0, $"These FinOps columns clip a long number:\n  {string.Join("\n  ", tooNarrow)}\nSize the column to its content (Width=\"Auto\" with a MinWidth) or give it at least {TwelveDigitWidth} px.");
    }

    /// <summary>The column's header text: the Header attribute, or the Text of the first TextBlock inside a Header element.</summary>
    private static string HeaderText(XElement column) =>
        (string?)column.Attribute("Header")
        ?? column.Elements().FirstOrDefault(e => e.Name.LocalName == "DataGridTextColumn.Header")?
            .Descendants().FirstOrDefault(e => e.Name.LocalName == "TextBlock")?.Attribute("Text")?.Value
        ?? "";

    /// <summary>The N0/N1/N2 or #,##0 format the binding applies, or null when the column is not formatted as a number.</summary>
    private static string? NumericFormat(string binding)
    {
        var i = binding.IndexOf("StringFormat=", StringComparison.Ordinal);
        if (i < 0)
        {
            return null;
        }

        i += "StringFormat=".Length;
        if (i < binding.Length && binding[i] == '\'')
        {
            i++;
        }

        if (binding.AsSpan(i).StartsWith("{}", StringComparison.Ordinal))
        {
            i += 2;
        }

        if (i < binding.Length && binding[i] == '$')
        {
            i++;
        }

        if (binding.AsSpan(i).StartsWith("{0:", StringComparison.Ordinal))
        {
            i += 3;
        }

        var end = binding.IndexOfAny(['}', ',', '\''], i);
        var format = end < 0 ? binding[i..] : binding[i..end];
        return format.Length > 0 && (format[0] == 'N' || format[0] == '#') ? format : null;
    }

    /// <summary>A percentage is bounded at 100.0, so it always fits: the header says % or Percent, or the format appends %.</summary>
    private static bool IsPercentage(string header, string binding) =>
        header.Contains('%') || header.Contains("Percent", StringComparison.Ordinal) || binding.Contains("}%'", StringComparison.Ordinal);

    /// <summary>An element style that sets TextTrimming shows an ellipsis when the value does not fit, so the cut is visible.</summary>
    private static bool TrimsWithEllipsis(XElement column) =>
        column.Elements().FirstOrDefault(e => e.Name.LocalName == "DataGridTextColumn.ElementStyle")?
            .Descendants().Any(e => e.Name.LocalName == "Setter" && (string?)e.Attribute("Property") == "TextTrimming") == true;
}
