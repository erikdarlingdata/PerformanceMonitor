using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Ui;
using Xunit;

namespace Dashboard.Tests;

/// <summary>
/// Tests for the shared DataGridExport (PerformanceMonitor.Ui) used by every copy/export
/// context menu in both apps. Because all ~29 sites delegate here, these cover their behavior.
/// The pure formatter/escaper tests run anywhere; the grid tests run on an STA thread because
/// WPF objects require it.
/// </summary>
public class DataGridExportTests
{
    private sealed class Row
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
        public string Note { get; set; } = "";
    }

    private static T RunSta<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        if (error != null) throw error;
        return result;
    }

    private static DataGrid BuildGrid()
    {
        var grid = new DataGrid();
        grid.Columns.Add(new DataGridTextColumn { Header = "Name", Binding = new Binding(nameof(Row.Name)) });
        grid.Columns.Add(new DataGridTextColumn { Header = "Value", Binding = new Binding(nameof(Row.Value)) });

        // A template column with a TextBlock bound to Note -- the case the old Dashboard
        // handlers mishandled (header filtered out, value kept).
        var template = new System.Windows.DataTemplate();
        var tb = new System.Windows.FrameworkElementFactory(typeof(TextBlock));
        tb.SetBinding(TextBlock.TextProperty, new Binding(nameof(Row.Note)));
        template.VisualTree = tb;
        template.Seal();
        grid.Columns.Add(new DataGridTemplateColumn { Header = "Note", CellTemplate = template });

        grid.ItemsSource = new[]
        {
            new Row { Name = "a,b", Value = 42, Note = "hi" },
            new Row { Name = "plain", Value = 7, Note = "there" }
        };
        return grid;
    }

    [Theory]
    [InlineData(null, "")]
    [InlineData("text", "text")]
    public void FormatForExport_HandlesStringsAndNull(string? input, string expected)
    {
        Assert.Equal(expected, DataGridExport.FormatForExport(input));
    }

    [Fact]
    public void FormatForExport_UsesInvariantCultureForNumbers()
    {
        var prior = CultureInfo.CurrentCulture;
        try
        {
            // A culture whose decimal separator is a comma -- proves we don't emit "1,5".
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            Assert.Equal("1.5", DataGridExport.FormatForExport(1.5));
            Assert.Equal("42", DataGridExport.FormatForExport(42));
        }
        finally
        {
            CultureInfo.CurrentCulture = prior;
        }
    }

    [Theory]
    [InlineData("plain", "plain")]
    [InlineData("", "")]
    [InlineData("a,b", "\"a,b\"")]
    [InlineData("a\"b", "\"a\"\"b\"")]
    [InlineData("a\nb", "\"a\nb\"")]
    public void CsvEscape_QuotesWhenNeeded(string input, string expected)
    {
        Assert.Equal(expected, DataGridExport.CsvEscape(input, ","));
    }

    [Fact]
    public void CsvEscape_QuotesWhenValueContainsSeparator()
    {
        // A comma, semicolon or tab is quoted whatever the separator, so a spreadsheet whose list separator
        // differs cannot split the value into a new cell.
        Assert.Equal("\"a,b\"", DataGridExport.CsvEscape("a,b", ";"));
        Assert.Equal("\"a;b\"", DataGridExport.CsvEscape("a;b", ";"));
    }

    [Fact]
    public void GetRowValues_ReturnsOneValuePerColumn_IncludingTemplate()
    {
        var values = RunSta(() =>
        {
            var grid = BuildGrid();
            var item = grid.Items.Cast<object>().First();
            return DataGridExport.GetRowValues(grid, item);
        });

        // Bound, bound, template -> three values, in column order.
        Assert.Equal(new[] { "a,b", "42", "hi" }, values);
    }

    [Fact]
    public void BuildClipboardText_KeepsHeadersAndRowsAligned()
    {
        var (headerFields, rowFieldCounts, columnCount) = RunSta(() =>
        {
            var grid = BuildGrid();
            var text = DataGridExport.BuildClipboardText(grid);
            var lines = text.Replace("\r\n", "\n").TrimEnd('\n').Split('\n');
            var headers = lines[0].Split('\t');
            var counts = lines.Skip(1).Select(l => l.Split('\t').Length).ToArray();
            return (headers, counts, grid.Columns.Count);
        });

        Assert.Equal(new[] { "Name", "Value", "Note" }, headerFields);
        Assert.Equal(3, columnCount);
        // The regression guard: every row has exactly as many fields as there are columns/headers.
        Assert.All(rowFieldCounts, count => Assert.Equal(columnCount, count));
    }

    [Fact]
    public void BuildCsv_EscapesAndIncludesAllColumns()
    {
        var csv = RunSta(() =>
        {
            var grid = BuildGrid();
            return DataGridExport.BuildCsv(grid, ",").Replace("\r\n", "\n");
        });

        var lines = csv.TrimEnd('\n').Split('\n');
        Assert.Equal("Name,Value,Note", lines[0]);
        Assert.Equal("\"a,b\",42,hi", lines[1]);   // comma value quoted, number invariant, template value present
        Assert.Equal("plain,7,there", lines[2]);
    }
}
