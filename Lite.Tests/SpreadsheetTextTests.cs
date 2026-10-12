/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Helpers;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// A spreadsheet that opens an exported CSV, or receives copied cells, evaluates a text cell that starts with
/// <c>=</c>, <c>+</c>, <c>-</c> or <c>@</c> (or a tab or carriage return). Every grid copy that lands in a
/// spreadsheet (Copy Row, Copy All Rows, Ctrl+C on selected rows), grid CSV export and chart CSV export writes
/// such text with a leading apostrophe, and writes numbers, negative ones too, as numbers. The single-cell Copy
/// Cell is the exception: it copies the text exactly as it is, because a cell is often query text on its way
/// into SSMS, where a leading apostrophe breaks a line that starts with <c>--</c>. These tests pin the rule and
/// each path that applies it.
/// </summary>
public sealed class SpreadsheetTextTests
{
    private sealed class Row
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    private static T RunSta<T>(Func<T> body) => StaTestThread.Run(body);

    private static DataGrid BuildGrid()
    {
        var grid = new DataGrid();
        grid.Columns.Add(new DataGridTextColumn { Header = "Name", Binding = new Binding(nameof(Row.Name)) });
        grid.Columns.Add(new DataGridTextColumn { Header = "Value", Binding = new Binding(nameof(Row.Value)) });
        grid.ItemsSource = new[] { new Row { Name = "=1+1", Value = -5 } };
        return grid;
    }

    private static DataGrid BuildGridWithTexts(params string[] names)
    {
        var grid = new DataGrid();
        grid.Columns.Add(new DataGridTextColumn { Header = "Name", Binding = new Binding(nameof(Row.Name)) });
        grid.Columns.Add(new DataGridTextColumn { Header = "Value", Binding = new Binding(nameof(Row.Value)) });
        grid.ItemsSource = names.Select((n, i) => new Row { Name = n, Value = -(i + 1) }).ToArray();
        return grid;
    }

    [Theory]
    [InlineData("=1+1", "'=1+1")]
    [InlineData("+1+1", "'+1+1")]
    [InlineData("-1+1", "'-1+1")]
    [InlineData("@SUM(1)", "'@SUM(1)")]
    [InlineData("\t=1", "'\t=1")]
    [InlineData("\r=1", "'\r=1")]
    [InlineData("-- a comment", "'-- a comment")]
    [InlineData("-", "'-")]
    [InlineData("-1e", "'-1e")]
    public void TextThatStartsLikeAFormula_GetsALeadingApostrophe(string text, string expected)
    {
        Assert.Equal(expected, SpreadsheetText.Neutralize(text));
    }

    [Theory]
    [InlineData("-5")]
    [InlineData("+5")]
    [InlineData("-0.5")]
    [InlineData("-1,234.5")]
    [InlineData("-1.5E-05")]
    [InlineData("5")]
    public void PlainNumbers_StayNumbers(string text)
    {
        Assert.Equal(text, SpreadsheetText.Neutralize(text));
    }

    [Theory]
    [InlineData("SELECT 1")]
    [InlineData("a=b")]
    [InlineData(" =1+1")]
    [InlineData("'=1")]
    [InlineData("")]
    public void OtherText_IsUnchanged(string text)
    {
        Assert.Equal(text, SpreadsheetText.Neutralize(text));
    }

    [Fact]
    public void Neutralize_IsIdempotent_AndTreatsNullAsEmpty()
    {
        var once = SpreadsheetText.Neutralize("=1+1");

        Assert.Equal(once, SpreadsheetText.Neutralize(once));
        Assert.Equal("", SpreadsheetText.Neutralize(null));
    }

    [Fact]
    public void NeutralizeCell_LeavesNumbersAlone_AndNeutralizesStrings()
    {
        Assert.Equal(-5, SpreadsheetText.NeutralizeCell(-5));
        Assert.Equal(-1.5, SpreadsheetText.NeutralizeCell(-1.5));
        Assert.Null(SpreadsheetText.NeutralizeCell(null));
        Assert.Equal("'=1+1", SpreadsheetText.NeutralizeCell("=1+1"));
    }

    [Fact]
    public void FormatForExport_WritesTextNeutralized_AndNumbersAsNumbers()
    {
        Assert.Equal("'=1+1", DataGridExport.FormatForExport("=1+1"));
        Assert.Equal("-5", DataGridExport.FormatForExport(-5));
        Assert.Equal("-1.5", DataGridExport.FormatForExport(-1.5));
    }

    [Fact]
    public void GridCsv_WritesTheTextCellNeutralized_AndTheNegativeNumberAsANumber()
    {
        var csv = RunSta(() => DataGridExport.BuildCsv(BuildGrid(), ",").Replace("\r\n", "\n"));

        Assert.Equal("'=1+1,-5", csv.TrimEnd('\n').Split('\n')[1]);
    }

    [Fact]
    public void GridCopy_WritesTheTextCellNeutralized_AndTheNegativeNumberAsANumber()
    {
        var text = RunSta(() => DataGridExport.BuildClipboardText(BuildGrid()).Replace("\r\n", "\n"));

        Assert.Equal("'=1+1\t-5", text.TrimEnd('\n').Split('\n')[1]);
    }

    [Fact]
    public void LiteContextMenuCells_ReadThroughTheSameRule()
    {
        var (name, value) = RunSta(() =>
        {
            var grid = BuildGrid();
            var item = grid.Items.Cast<object>().First();
            return (ContextMenuHelper.GetCellValue(grid.Columns[0], item),
                    ContextMenuHelper.GetCellValue(grid.Columns[1], item));
        });

        Assert.Equal("'=1+1", name);
        Assert.Equal("-5", value);
    }

    [Fact]
    public void KeyboardCopy_NeutralizesTextCells_AndLeavesNumbersAndHeadersAlone()
    {
        var (headerText, dataName, dataValue) = RunSta(() =>
        {
            var grid = BuildGrid();
            var item = grid.Items.Cast<object>().First();
            var nameColumn = grid.Columns[0];
            var valueColumn = grid.Columns[1];

            var header = new DataGridRowClipboardEventArgs(item, 0, 1, isColumnHeadersRow: true);
            header.ClipboardRowContent.Add(new DataGridClipboardCellContent(item, nameColumn, "=Header"));
            DataGridClipboardBehavior.FixHeaderCopy(grid, header);

            var data = new DataGridRowClipboardEventArgs(item, 0, 1, isColumnHeadersRow: false);
            data.ClipboardRowContent.Add(new DataGridClipboardCellContent(item, nameColumn, "=1+1"));
            data.ClipboardRowContent.Add(new DataGridClipboardCellContent(item, valueColumn, -5));
            DataGridClipboardBehavior.FixHeaderCopy(grid, data);

            return (header.ClipboardRowContent[0].Content, data.ClipboardRowContent[0].Content,
                    data.ClipboardRowContent[1].Content);
        });

        Assert.Equal("=Header", headerText);
        Assert.Equal("'=1+1", dataName);
        Assert.Equal(-5, dataValue);
    }

    [Theory]
    [InlineData("--x")]
    [InlineData("=1+1")]
    [InlineData("@SUM(A1)")]
    [InlineData("-5 rows")]
    public void CopyCell_CopiesTheTextExactlyAsItIs(string text)
    {
        var (name, value) = RunSta(() =>
        {
            var grid = BuildGridWithTexts(text);
            var item = grid.Items.Cast<object>().First();
            return (DataGridExport.GetRawCellValue(grid.Columns[0], item),
                    DataGridExport.GetRawCellValue(grid.Columns[1], item));
        });

        Assert.Equal(text, name);
        Assert.Equal("-1", value);
    }

    [Fact]
    public void CopyCell_ReadsTheCellRaw_InBothImplementations()
    {
        // Copy Cell has no pure builder (it reads the grid's current cell and writes the clipboard), so the wiring
        // is pinned: both CopyCell bodies read through GetRawCellValue and never through the neutralizing reader.
        foreach (var (file, next) in new[]
        {
            ("PerformanceMonitor.Ui/DataGridExport.cs", "public static void CopyRow("),
            ("Lite/Helpers/ContextMenuHelper.cs", "public static void CopyRow("),
        })
        {
            var source = ParitySource.ReadFile(file);
            var start = source.IndexOf("public static void CopyCell(", StringComparison.Ordinal);
            var end = source.IndexOf(next, StringComparison.Ordinal);
            Assert.True(start >= 0 && end > start, $"the read of CopyCell in {file} is broken.");

            var body = source.Substring(start, end - start);
            Assert.Contains("GetRawCellValue(", body);
            Assert.DoesNotContain("GetCellValue(", body);
        }
    }

    [Fact]
    public void MultiRowCopy_StillNeutralizesTextThatStartsWithDashes()
    {
        var (allRows, rowValues, csv) = RunSta(() =>
        {
            var grid = BuildGridWithTexts("--x", "=1+1");
            var first = grid.Items.Cast<object>().First();
            return (DataGridExport.BuildClipboardText(grid).Replace("\r\n", "\n"),
                    DataGridExport.GetRowValues(grid, first),
                    DataGridExport.BuildCsv(grid, ",").Replace("\r\n", "\n"));
        });

        // Copy All Rows: header, then one line per row.
        Assert.Equal(new[] { "Name\tValue", "'--x\t-1", "'=1+1\t-2" }, allRows.TrimEnd('\n').Split('\n'));
        // Copy Row is the tab-joined GetRowValues of one row.
        Assert.Equal(new[] { "'--x", "-1" }, rowValues);
        Assert.Equal(new[] { "Name,Value", "'--x,-1", "'=1+1,-2" }, csv.TrimEnd('\n').Split('\n'));
    }

    [Fact]
    public void LiteChartCsv_NeutralizesTheSeriesName()
    {
        var line = ContextMenuHelper.ChartCsvLine(46000.5, "=1+1", 3, ",", TimeZoneInfo.Utc);

        Assert.Contains(",'=1+1,", line);
    }

    [Theory]
    [InlineData("a\t=1", "a =1")]
    [InlineData("a\n=1", "a =1")]
    [InlineData("a\r\n+1", "a +1")]
    [InlineData("a\r\n\r\n-1", "a -1")]
    [InlineData("a\t\t@SUM(1)", "a @SUM(1)")]
    [InlineData("\t=1", " =1")]
    [InlineData("\r\n=1", " =1")]
    [InlineData("=1\n=2", "'=1 =2")]
    [InlineData("plain", "plain")]
    public void ClipboardCell_CollapsesTabsAndLineBreaksToOneSpace_BeforeTheFormulaRule(string text, string expected)
    {
        Assert.Equal(expected, SpreadsheetText.NeutralizeClipboardCell(text));
    }

    [Theory]
    [InlineData("SELECT 1 /*\n=HYPERLINK(\"https://x/\",\"open\")\n*/")]
    [InlineData("x\r\n=1+1")]
    [InlineData("x\r\n+1")]
    [InlineData("x\r\n-1")]
    [InlineData("x\r\n@SUM(1)")]
    [InlineData("x\t=1+1")]
    [InlineData("x\t+1")]
    [InlineData("x\t-1")]
    [InlineData("x\t@SUM(1)")]
    [InlineData("x\r=1+1")]
    [InlineData("\t=1+1")]
    [InlineData("\r\n@SUM(1)")]
    public void RowCopies_KeepOneCellOneField_AndNoFieldStartsLikeAFormula(string text)
    {
        var (copyAll, rowValues) = RunSta(() =>
        {
            var grid = BuildGridWithTexts(text);
            var first = grid.Items.Cast<object>().First();
            return (DataGridExport.BuildClipboardText(grid), DataGridExport.GetClipboardRowValues(grid, first));
        });

        // Copy All Rows: header line plus one line, two fields each. A tab or line break inside the cell would add fields or lines.
        var lines = copyAll.TrimEnd('\r', '\n').Split("\r\n");
        Assert.Equal(2, lines.Length);
        Assert.All(lines, l => Assert.Equal(2, l.Split('\t').Length));
        AssertNoFormulaField(copyAll.Split('\t', '\r', '\n'));

        // Copy Row: the same two fields.
        Assert.Equal(2, rowValues.Count);
        AssertNoFormulaField(rowValues);
        Assert.Equal(rowValues[0], lines[1].Split('\t')[0]);
    }

    private static void AssertNoFormulaField(System.Collections.Generic.IEnumerable<string> fields)
    {
        foreach (var field in fields)
        {
            if (field.Length == 0 || int.TryParse(field, out _)) continue;
            Assert.DoesNotContain(field[0], "=+-@\t\r");
        }
    }

    [Fact]
    public void ClipboardCell_LeavesNumbersAndTheCsvExportAlone()
    {
        Assert.Equal("-5", DataGridExport.FormatForClipboard(-5));
        Assert.Equal("-1,234.5", SpreadsheetText.NeutralizeClipboardCell("-1,234.5"));

        // The CSV export keeps the line break inside a quoted field and still neutralizes the start.
        var csv = RunSta(() => DataGridExport.BuildCsv(BuildGridWithTexts("a\n=1", "=1\r\nb"), ",").Replace("\r\n", "\n"));
        Assert.Equal("Name,Value\n\"a\n=1\",-1\n\"'=1\nb\",-2\n", csv);
    }

    [Fact]
    public void KeyboardCopy_CollapsesTabsAndLineBreaksInsideATextCell()
    {
        var content = RunSta(() =>
        {
            var grid = BuildGrid();
            var item = grid.Items.Cast<object>().First();
            var data = new DataGridRowClipboardEventArgs(item, 0, 1, isColumnHeadersRow: false);
            data.ClipboardRowContent.Add(new DataGridClipboardCellContent(item, grid.Columns[0], "x\n=1+1"));
            data.ClipboardRowContent.Add(new DataGridClipboardCellContent(item, grid.Columns[0], "x\t+1"));
            data.ClipboardRowContent.Add(new DataGridClipboardCellContent(item, grid.Columns[0], "\r\n@SUM(1)"));
            data.ClipboardRowContent.Add(new DataGridClipboardCellContent(item, grid.Columns[1], -5));
            DataGridClipboardBehavior.FixHeaderCopy(grid, data);
            return data.ClipboardRowContent.Select(c => c.Content).ToArray();
        });

        Assert.Equal(new object?[] { "x =1+1", "x +1", " @SUM(1)", -5 }, content);
    }

    [Fact]
    public void RowCopies_ReadCellsThroughTheClipboardReader_InBothImplementations()
    {
        var ui = ParitySource.ReadFile("PerformanceMonitor.Ui/DataGridExport.cs");
        var copyRow = ui.Substring(ui.IndexOf("public static void CopyRow(", StringComparison.Ordinal));
        copyRow = copyRow.Substring(0, copyRow.IndexOf("public static void CopyAllRows(", StringComparison.Ordinal));
        Assert.Contains("GetClipboardRowValues(", copyRow);
        Assert.DoesNotContain("GetRowValues(", copyRow);

        var build = ui.Substring(ui.IndexOf("public static string BuildClipboardText(", StringComparison.Ordinal));
        build = build.Substring(0, build.IndexOf("public static string BuildCsv(", StringComparison.Ordinal));
        Assert.Contains("GetClipboardRowValues(", build);
        Assert.DoesNotContain("GetRowValues(", build);

        var lite = ParitySource.ReadFile("Lite/Helpers/ContextMenuHelper.cs");
        var rows = lite.Substring(lite.IndexOf("public static void CopyRow(", StringComparison.Ordinal));
        rows = rows.Substring(0, rows.IndexOf("public static void ExportToCsv(", StringComparison.Ordinal));
        Assert.Equal(2, rows.Split("GetClipboardCellValue(").Length - 1);
        Assert.DoesNotContain("GetCellValue(", rows);
    }

    [Theory]
    [InlineData("a,b", ";", "\"a,b\"")]
    [InlineData("a;b", ",", "\"a;b\"")]
    [InlineData("a\tb", ",", "\"a\tb\"")]
    [InlineData("a|b", "|", "\"a|b\"")]
    [InlineData("a\"b", ";", "\"a\"\"b\"")]
    [InlineData("a\nb", ";", "\"a\nb\"")]
    [InlineData("a\rb", "\t", "\"a\rb\"")]
    [InlineData("plain", ";", "plain")]
    [InlineData("", ";", "")]
    public void CsvEscape_QuotesTheSeparatorQuoteBreakAndTheCommonDelimiters_WhateverTheSeparatorIs(string value, string separator, string expected)
    {
        Assert.Equal(expected, DataGridExport.CsvEscape(value, separator));
    }
}
