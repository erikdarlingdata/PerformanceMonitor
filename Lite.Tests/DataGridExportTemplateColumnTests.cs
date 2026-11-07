/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8153: Copy Row, Copy All Rows and Export to CSV carry every column a grid shows, not only the columns whose
/// template root is a bare <see cref="TextBlock"/> and whose binding path is one property deep. The Query Stats grids
/// in both apps draw their four measures through <see cref="BarChartCell"/> template columns, the FinOps lock-wait
/// cells through a <see cref="Border"/> root, and some grids bind a nested path. Each shape is built here as the
/// real column with the theme loaded, on the shared STA thread, and the builders behind the three menu items are
/// asserted to carry the value.
/// </summary>
public sealed class DataGridExportTemplateColumnTests
{
    private sealed class Detail
    {
        public string ServerName { get; set; } = "";
    }

    private sealed class Row
    {
        public string QueryText { get; set; } = "";
        public long TotalExecutions { get; set; }
        public double TotalCpuMs { get; set; }
        public long RowLockWaitInMs { get; set; }
        public Detail Nested { get; set; } = new();
    }

    private static readonly Row s_row = new()
    {
        QueryText = "SELECT 1",
        TotalExecutions = 12345,
        TotalCpuMs = 678.5,
        RowLockWaitInMs = 910,
        Nested = new Detail { ServerName = "a8153-server" },
    };

    private static T RunSta<T>(Func<T> body) => StaTestThread.Run(() => WithTheme(body));

    /// <summary>The Query Stats shape: a template column whose root is a <see cref="BarChartCell"/>, sorted by the bound measure.</summary>
    private static DataGridTemplateColumn BarColumn(string header, string path, string format)
    {
        var cell = new FrameworkElementFactory(typeof(BarChartCell));
        cell.SetBinding(BarChartCell.ValueProperty, new Binding(path));
        cell.SetValue(BarChartCell.MaximumProperty, 100000.0);
        cell.SetBinding(BarChartCell.TextProperty, new Binding(path) { StringFormat = format });
        var template = new DataTemplate { VisualTree = cell };
        template.Seal();
        return new DataGridTemplateColumn { Header = header, SortMemberPath = path, CellTemplate = template };
    }

    /// <summary>The FinOps heat-cell shape: a <see cref="Border"/> root around the value's <see cref="TextBlock"/>.</summary>
    private static DataGridTemplateColumn HeatColumn(string header, string path)
    {
        var text = new FrameworkElementFactory(typeof(TextBlock));
        text.SetBinding(TextBlock.TextProperty, new Binding(path) { StringFormat = "{0:N0}" });
        var border = new FrameworkElementFactory(typeof(Border));
        border.AppendChild(text);
        var template = new DataTemplate { VisualTree = border };
        template.Seal();
        return new DataGridTemplateColumn { Header = header, SortMemberPath = path, CellTemplate = template };
    }

    private static DataGrid BuildQueryStatsGrid()
    {
        var grid = new DataGrid();
        grid.Columns.Add(new DataGridTextColumn { Header = "Query Text", Binding = new Binding(nameof(Row.QueryText)) });
        grid.Columns.Add(BarColumn("Executions", nameof(Row.TotalExecutions), "N0"));
        grid.Columns.Add(BarColumn("Total CPU (ms)", nameof(Row.TotalCpuMs), "N1"));
        grid.Columns.Add(HeatColumn("Row Lock Wait ms", nameof(Row.RowLockWaitInMs)));
        grid.Columns.Add(new DataGridTextColumn { Header = "Server", Binding = new Binding("Nested.ServerName") });
        grid.ItemsSource = new[] { s_row };
        return grid;
    }

    [Fact]
    public void ExportToCsv_CarriesTheBarChartCellColumns_TheBorderRootedColumn_AndTheNestedPath()
    {
        var csv = RunSta(() => DataGridExport.BuildCsv(BuildQueryStatsGrid(), ",").Replace("\r\n", "\n"));

        var lines = csv.TrimEnd('\n').Split('\n');
        Assert.Equal("Query Text,Executions,Total CPU (ms),Row Lock Wait ms,Server", lines[0]);
        Assert.Equal("SELECT 1,12345,678.5,910,a8153-server", lines[1]);
    }

    [Fact]
    public void CopyAllRows_CarriesTheBarChartCellColumns_TheBorderRootedColumn_AndTheNestedPath()
    {
        var text = RunSta(() => DataGridExport.BuildClipboardText(BuildQueryStatsGrid()).Replace("\r\n", "\n"));

        var lines = text.TrimEnd('\n').Split('\n');
        Assert.Equal("Query Text\tExecutions\tTotal CPU (ms)\tRow Lock Wait ms\tServer", lines[0]);
        Assert.Equal("SELECT 1\t12345\t678.5\t910\ta8153-server", lines[1]);
    }

    [Fact]
    public void CopyRow_CarriesEveryColumn_InColumnOrder()
    {
        var values = RunSta(() =>
        {
            var grid = BuildQueryStatsGrid();
            var item = grid.Items.Cast<object>().First();
            return DataGridExport.GetClipboardRowValues(grid, item);
        });

        Assert.Equal(new[] { "SELECT 1", "12345", "678.5", "910", "a8153-server" }, values);
    }

    [Fact]
    public void CopyCell_ReadsABarChartCellColumn_AndANestedPath_Raw()
    {
        var (executions, server) = RunSta(() =>
        {
            var grid = BuildQueryStatsGrid();
            var item = grid.Items.Cast<object>().First();
            return (DataGridExport.GetRawCellValue(grid.Columns[1], item), DataGridExport.GetRawCellValue(grid.Columns[4], item));
        });

        Assert.Equal("12345", executions);
        Assert.Equal("a8153-server", server);
    }

    [Fact]
    public void ATemplateColumnWithNoSortMemberPath_StillReadsTheTextBlockInsideItsRoot()
    {
        var value = RunSta(() =>
        {
            var column = HeatColumn("Row Lock Wait ms", nameof(Row.RowLockWaitInMs));
            column.SortMemberPath = null;
            return DataGridExport.GetCellValue(column, s_row);
        });

        Assert.Equal("910", value);
    }

    [Fact]
    public void ABoundColumnWhosePathLeadsThroughNull_ExportsAnEmptyCell()
    {
        var value = RunSta(() =>
        {
            var column = new DataGridTextColumn { Header = "Server", Binding = new Binding("Nested.ServerName") };
            return DataGridExport.GetCellValue(column, new Row { Nested = null! });
        });

        Assert.Equal("", value);
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
