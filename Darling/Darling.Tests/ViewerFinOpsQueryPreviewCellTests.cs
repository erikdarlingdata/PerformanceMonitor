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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8110: the Viewer's FinOps Query Preview cells show the statement on one line. The bound preview is the head of the stored text
/// with the statement's own line breaks and indentation, and a cell that wrapped it under a 90 px cap showed a few scattered words
/// and cut its last line. The real Expensive Queries and High Impact grids are taken off the real tab, themed as the app themes
/// them, and shown with one formatted row; the realized preview cell is one trimmed line whose tooltip carries the whole statement.
/// </summary>
public sealed class ViewerFinOpsQueryPreviewCellTests
{
    /* The head of a formatted statement as the reader stores it: CRLF line breaks, a tab and eight-space indentation. */
    private const string Preview = "SELECT\r\n\to.OrderId,\r\n        o.CustomerId\r\nFROM dbo.Orders AS o\r\nWHERE o.OrderDate >= @since";
    private const string Full = Preview + "\r\nORDER BY o.OrderDate DESC;";
    private const string OneLine = "SELECT o.OrderId, o.CustomerId FROM dbo.Orders AS o WHERE o.OrderDate >= @since";

    [Fact]
    public void TheExpensiveQueriesPreviewCell_IsOneTrimmedLine_WithTheWholeStatementInItsTooltip()
    {
        var cell = StaTestThread.Run(() => WithTheme(() =>
            Rendered(new FinOpsTab().FinOpsExpensiveQueriesDataGrid,
                new ExpensiveQueryRow { DatabaseName = "Sales", QueryPreview = Preview, FullQueryText = Full })));

        AssertOneTrimmedLine(cell);
    }

    [Fact]
    public void TheHighImpactPreviewCell_IsOneTrimmedLine_WithTheWholeStatementInItsTooltip()
    {
        var cell = StaTestThread.Run(() => WithTheme(() =>
            Rendered(new FinOpsTab().FinOpsHighImpactDataGrid,
                new HighImpactQueryRow { DatabaseName = "Sales", SampleQueryText = Preview, FullQueryText = Full })));

        AssertOneTrimmedLine(cell);
    }

    private static void AssertOneTrimmedLine(PreviewCell cell)
    {
        Assert.Equal(OneLine, cell.Text);
        Assert.Equal(TextWrapping.NoWrap, cell.Wrapping);
        Assert.Equal(TextTrimming.CharacterEllipsis, cell.Trimming);
        Assert.Equal(Full, cell.ToolTip);
        Assert.True(cell.Height < cell.FontSize * 2,
            $"the preview cell is {cell.Height:F0} px tall at a {cell.FontSize:F0} px font, so it shows more than one line");
    }

    private sealed record PreviewCell(string Text, TextWrapping Wrapping, TextTrimming Trimming, object? ToolTip, double Height, double FontSize);

    /// <summary>
    /// Shows <paramref name="grid"/> on its own in a window with <paramref name="row"/> as its one item and reads the realized
    /// Query Preview cell, found by the tooltip that carries the whole statement. The grid comes off the tab first: its templates
    /// and row style were resolved when the tab loaded, and on its own it realizes its rows without the tab's sub-tabs or expanders.
    /// </summary>
    private static PreviewCell Rendered(DataGrid grid, object row)
    {
        Detach(grid);
        grid.ItemsSource = new[] { row };
        var window = new Window { Content = grid, Width = 1280, Height = 400, Left = 0, Top = 0 };
        StaTestThread.ShowOffScreen(window);
        try
        {
            ClippedTextProbe.Settle(window);
            var text = ClippedTextProbe.Descendants(grid).OfType<TextBlock>().SingleOrDefault(t => Equals(t.ToolTip, Full))
                ?? throw new Xunit.Sdk.XunitException("the Query Preview cell was not realized");
            return new PreviewCell(text.Text, text.TextWrapping, text.TextTrimming, text.ToolTip, text.ActualHeight, text.FontSize);
        }
        finally
        {
            window.Close();
        }
    }

    private static void Detach(FrameworkElement element)
    {
        switch (element.Parent)
        {
            case Panel panel: panel.Children.Remove(element); break;
            case Decorator decorator: decorator.Child = null; break;
            case ContentControl content: content.Content = null; break;
            default: throw new Xunit.Sdk.XunitException($"{element.Name} sits in a {element.Parent?.GetType().Name ?? "null"}, which this test does not know how to take it out of");
        }
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
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
