/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A grid cell too narrow for its text ends it with an ellipsis instead of cutting it at the cell's edge, in every theme: both the text a
/// text column makes with no element style of its own and the text styled with <c>DataGridCellText</c>, the style the other cell styles
/// build on.
/// </summary>
public sealed class DataGridCellTrimmingTests
{
    private sealed record Row(string Text);

    [Theory]
    [InlineData("DarkTheme")]
    [InlineData("LightTheme")]
    [InlineData("CoolBreezeTheme")]
    public void ANarrowTextCell_EndsItsTextWithAnEllipsis_WithAndWithoutTheCellTextStyle(string theme)
    {
        var (plain, styled) = StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var grid = new DataGrid { AutoGenerateColumns = false, HeadersVisibility = DataGridHeadersVisibility.Column, Width = 300, Height = 120 };
            grid.Columns.Add(new DataGridTextColumn { Header = "Plain", Binding = new Binding(nameof(Row.Text)), Width = 120 });
            grid.Columns.Add(new DataGridTextColumn { Header = "Styled", Binding = new Binding(nameof(Row.Text)), Width = 120, ElementStyle = (Style)Application.Current.FindResource("DataGridCellText") });
            grid.ItemsSource = new[] { new Row("SELECT v FROM dbo.walk_rows WHERE id = @id AND v > 1000 ORDER BY v") };
            var window = new Window { Content = grid, Width = 400, Height = 200, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                window.UpdateLayout();
                var cells = ClippedTextProbe.Descendants(grid).OfType<DataGridCell>().ToList();
                Assert.Equal(2, cells.Count);
                return (Read(cells[0]), Read(cells[1]));
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(plain.Trimming == TextTrimming.CharacterEllipsis, $"{theme}: the plain cell's text is cut with {plain.Trimming}, {plain.TextWidth:F0} px of text in a {plain.CellWidth:F0} px cell");
        Assert.True(styled.Trimming == TextTrimming.CharacterEllipsis, $"{theme}: the DataGridCellText cell's text is cut with {styled.Trimming}, {styled.TextWidth:F0} px of text in a {styled.CellWidth:F0} px cell");
        Assert.True(plain.TextWidth <= plain.CellWidth + 1 && styled.TextWidth <= styled.CellWidth + 1, $"{theme}: the text runs past the cell ({plain.TextWidth:F0} and {styled.TextWidth:F0} px in {plain.CellWidth:F0} px cells)");
    }

    private static (TextTrimming Trimming, double CellWidth, double TextWidth) Read(DataGridCell cell)
    {
        var block = ClippedTextProbe.Descendants(cell).OfType<TextBlock>().Single(b => !string.IsNullOrWhiteSpace(b.Text));
        return (block.TextTrimming, cell.ActualWidth, block.ActualWidth);
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") };
        app.Resources.MergedDictionaries.Add(added);
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Remove(added);
        }
    }
}
