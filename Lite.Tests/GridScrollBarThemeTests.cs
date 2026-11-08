/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8223: Lite's grids showed the system scroll bars (a pale thumb, white arrows, a black track), both scroll bars at once when there was nothing to
/// scroll (with a black corner square), and on an empty grid the last column cut at the edge with no way to scroll to it. These lay real grids out
/// on the shared STA thread in a window themed with each of Lite's themes and read back what was rendered: which scroll bars show, the colour a
/// thumb is painted, whether arrow buttons exist, where the last column ends, and how many layout passes the grid needed to settle.
/// </summary>
public class GridScrollBarThemeTests
{
    /// <summary>A grid that counts its own measure passes, styled by the theme's DataGrid style.</summary>
    private sealed class CountingGrid : DataGrid
    {
        internal int Measures;

        protected override Size MeasureOverride(Size availableSize)
        {
            Measures++;
            return base.MeasureOverride(availableSize);
        }
    }

    private sealed record Laid(
        CountingGrid Grid,
        ScrollBar Vertical,
        ScrollBar Horizontal,
        ScrollViewer Scroll,
        Color ThumbCenter,
        Color ThemeBorder,
        int ArrowButtons,
        double ColumnsTotal,
        int MeasuresAfterSettle,
        int MeasuresAfterMore,
        double[] FillWidths,
        bool ContentScrolls);

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}.xaml");

    private static IEnumerable<T> Find<T>(DependencyObject root) where T : DependencyObject =>
        ClippedTextProbe.Descendants(root).OfType<T>();

    private static bool Shows(ScrollBar bar) => bar.Visibility == Visibility.Visible && bar.ActualWidth > 0 && bar.ActualHeight > 0;

    /// <summary>
    /// A grid of <paramref name="fixedColumns"/> fixed columns (130 px) and, when <paramref name="fillColumn"/>, a fill column, with <paramref name="rows"/> rows,
    /// in a canvas <paramref name="width"/> by <paramref name="height"/>, in a window themed with <paramref name="theme"/>.
    /// </summary>
    private static Laid Lay(string theme, int rows, int fixedColumns, bool fillColumn, double width, double height,
        double columnWidth = 130, string header = "Column ", int? rowsAfter = null) =>
        StaTestThread.Run(() =>
        {
            var grid = new CountingGrid { AutoGenerateColumns = false, IsReadOnly = true };
            for (var i = 0; i < fixedColumns; i++)
            {
                grid.Columns.Add(new DataGridTextColumn { Header = header + i, Binding = new System.Windows.Data.Binding("."), Width = columnWidth });
            }

            if (fillColumn)
            {
                grid.Columns.Add(new DataGridTextColumn { Header = "Detail", Binding = new System.Windows.Data.Binding("."), Width = new DataGridLength(1, DataGridLengthUnitType.Star) });
            }

            grid.ItemsSource = Enumerable.Range(0, rows).Select(i => "row " + i).ToList();
            grid.Width = width;
            grid.Height = height;
            var window = new Window { Content = new Canvas { Children = { grid } }, Width = 600, Height = 400 };
            window.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri(theme) });
            grid.Style = (Style)window.FindResource(typeof(DataGrid));
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                if (rowsAfter is { } more)
                {
                    /* The rows arrive after the grid was shown, as a refresh does. */
                    grid.ItemsSource = Enumerable.Range(0, more).Select(i => "row " + i).ToList();
                    ClippedTextProbe.Settle(window);
                }

                var afterSettle = grid.Measures;
                var fillBefore = grid.Columns.Select(c => c.ActualWidth).ToArray();
                for (var i = 0; i < 2; i++)
                {
                    StaTestThread.WaitForIdle(window.Dispatcher, TimeSpan.FromSeconds(5));
                    window.UpdateLayout();
                }

                var scroll = (ScrollViewer)grid.Template.FindName("DG_ScrollViewer", grid);
                var vertical = (ScrollBar)scroll.Template.FindName("PART_VerticalScrollBar", scroll);
                var horizontal = (ScrollBar)scroll.Template.FindName("PART_HorizontalScrollBar", scroll);

                /* The colour painted at the middle of the vertical thumb, read from a render of the grid at 96 dpi (one pixel per device-independent unit). */
                var thumbColor = Colors.Transparent;
                var thumb = Find<Thumb>(vertical).FirstOrDefault(t => t.ActualHeight > 0 && t.ActualWidth > 0);
                if (thumb is not null && Shows(vertical))
                {
                    var bounds = thumb.TransformToAncestor(grid).TransformBounds(new Rect(0, 0, thumb.ActualWidth, thumb.ActualHeight));
                    var bitmap = new RenderTargetBitmap((int)Math.Ceiling(grid.ActualWidth), (int)Math.Ceiling(grid.ActualHeight), 96, 96, PixelFormats.Pbgra32);
                    bitmap.Render(grid);
                    var pixel = new byte[4];
                    bitmap.CopyPixels(new Int32Rect((int)(bounds.X + bounds.Width / 2), (int)(bounds.Y + bounds.Height / 2), 1, 1), pixel, 4, 0);
                    thumbColor = Color.FromArgb(pixel[3], pixel[2], pixel[1], pixel[0]);
                }

                /* A line button is a RepeatButton wired to a scroll bar's line command; the track's page buttons are not arrows. */
                var arrows = Find<RepeatButton>(vertical).Concat(Find<RepeatButton>(horizontal))
                    .Count(b => b.IsVisible && b.ActualWidth > 0 && (b.Command == ScrollBar.LineUpCommand || b.Command == ScrollBar.LineDownCommand
                        || b.Command == ScrollBar.LineLeftCommand || b.Command == ScrollBar.LineRightCommand));

                var lastRight = grid.Columns.Sum(c => c.ActualWidth);
                var border = (SolidColorBrush)grid.FindResource("BorderBrush");
                return new Laid(grid, vertical, horizontal, scroll, thumbColor, border.Color, arrows, lastRight, afterSettle, grid.Measures,
                    fillBefore.Zip(grid.Columns.Select(c => c.ActualWidth), (a, b) => b - a).ToArray(), scroll.CanContentScroll);
            }
            finally
            {
                window.Close();
            }
        });

    public static IEnumerable<object[]> Themes() =>
        new[] { "DarkTheme", "LightTheme", "CoolBreezeTheme" }.Select(t => new object[] { t });

    [Theory]
    [MemberData(nameof(Themes))]
    public void AGridWithTwoRowsThatFit_ShowsNoScrollBar(string theme)
    {
        var laid = Lay(theme, 2, 3, true, 1280, 700);

        Assert.False(Shows(laid.Vertical), "nothing to scroll down: no vertical scroll bar");
        Assert.False(Shows(laid.Horizontal), "nothing to scroll across: no horizontal scroll bar");
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AnEmptyGridThatFits_ShowsNoScrollBar_AndNoCornerSquare(string theme)
    {
        var laid = Lay(theme, 0, 3, true, 900, 500);

        Assert.False(Shows(laid.Vertical));
        Assert.False(Shows(laid.Horizontal));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AGridWithTwoHundredRows_ShowsAThemedVerticalScrollBar_WithoutArrowButtons(string theme)
    {
        var laid = Lay(theme, 200, 3, true, 1280, 400);

        Assert.True(Shows(laid.Vertical), "200 rows do not fit in 400 px: the vertical scroll bar shows");
        Assert.Equal(0, laid.ArrowButtons);
        Assert.True(laid.Vertical.ActualWidth <= 14, $"a thin scroll bar, got {laid.Vertical.ActualWidth}");
        Assert.Equal(255, laid.ThumbCenter.A);
        Assert.Equal((laid.ThemeBorder.R, laid.ThemeBorder.G, laid.ThemeBorder.B), (laid.ThumbCenter.R, laid.ThumbCenter.G, laid.ThumbCenter.B));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AGridWiderThanItsHost_ShowsAHorizontalScrollBar_WithoutArrowButtons(string theme)
    {
        var laid = Lay(theme, 5, 12, true, 700, 400);

        Assert.True(Shows(laid.Horizontal), "twelve 130 px columns do not fit in 700 px");
        Assert.True(laid.Scroll.ScrollableWidth > 500);
        Assert.Equal(0, laid.ArrowButtons);
        Assert.True(laid.Horizontal.ActualHeight <= 14, $"a thin scroll bar, got {laid.Horizontal.ActualHeight}");
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AnEmptyGridWithFixedColumnsWiderThanItsHost_DoesNotCutItsLastColumn(string theme)
    {
        /* The health grids (Memory Conditions, Node OOM, Significant Waits, CPU Tasks) are all fixed columns, empty most of the time. */
        var laid = Lay(theme, 0, 12, false, 700, 300);

        var lastRightInContent = laid.ColumnsTotal;
        var fits = lastRightInContent <= laid.Scroll.ViewportWidth + 0.5;
        var scrolls = Shows(laid.Horizontal) && laid.Scroll.ScrollableWidth >= lastRightInContent - laid.Scroll.ViewportWidth - 0.5;
        Assert.True(fits || scrolls,
            $"the last column ends at {lastRightInContent} in a viewport of {laid.Scroll.ViewportWidth}, scrollable {laid.Scroll.ScrollableWidth}, bar shown {Shows(laid.Horizontal)}");
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AGridWithAFillColumnAndManyRows_SettlesItsLayout_WhenTheVerticalScrollBarAppears(string theme)
    {
        /* A vertical scroll bar appearing narrows the viewport, which refits the fill column: that must not start another round. */
        var laid = Lay(theme, 200, 5, true, 900, 400);

        Assert.True(laid.MeasuresAfterSettle < 60, $"the grid was measured {laid.MeasuresAfterSettle} times to settle");
        Assert.Equal(laid.MeasuresAfterSettle, laid.MeasuresAfterMore);
        Assert.All(laid.FillWidths, w => Assert.Equal(0, w, 0.5));
        Assert.False(Shows(laid.Horizontal), "the fill column takes the room the vertical scroll bar leaves: nothing to scroll across");
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AGridWhoseColumnsJustFit_SettlesItsLayout(string theme)
    {
        /* Columns that fit only without the vertical scroll bar: the bar's width pushes a horizontal one in, whose height could push the vertical one out. */
        var laid = Lay(theme, 40, 6, false, 6 * 130 + 4, 400);

        Assert.True(laid.MeasuresAfterSettle < 60, $"the grid was measured {laid.MeasuresAfterSettle} times to settle");
        Assert.Equal(laid.MeasuresAfterSettle, laid.MeasuresAfterMore);
        Assert.Equal(laid.ColumnsTotal > laid.Scroll.ViewportWidth + 0.5, Shows(laid.Horizontal));
    }

    /// <summary>Everything to the right of the viewport is reached by scrolling: the columns fit, or the scroll bar shows and goes far enough.</summary>
    private static void AssertLastColumnReachable(Laid laid)
    {
        var overflow = laid.ColumnsTotal - laid.Scroll.ViewportWidth;
        Assert.True(overflow <= 0.5 || (Shows(laid.Horizontal) && laid.Scroll.ScrollableWidth >= overflow - 1),
            $"columns are {laid.ColumnsTotal} wide in a viewport of {laid.Scroll.ViewportWidth}; scrollable {laid.Scroll.ScrollableWidth}, bar shown {Shows(laid.Horizontal)}");
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AGridWhoseHeadersWidenItsColumns_CanScrollToItsLastColumn(string theme)
    {
        /* A 40 px column with a long header grows to the header's width once the grid loads; the scroll extent has to follow. */
        var laid = Lay(theme, 5, 10, false, 700, 300, columnWidth: 40, header: "Memory Used Total MB ");

        Assert.True(laid.ColumnsTotal > 1000, $"the headers should have widened the columns, got {laid.ColumnsTotal}");
        AssertLastColumnReachable(laid);
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AnEmptyGridWhoseHeadersWidenItsColumns_CanScrollToItsLastColumn(string theme)
    {
        var laid = Lay(theme, 0, 10, false, 700, 300, columnWidth: 40, header: "Memory Used Total MB ");

        Assert.True(laid.ColumnsTotal > 1000, $"the headers should have widened the columns, got {laid.ColumnsTotal}");
        AssertLastColumnReachable(laid);
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AnEmptyGridThatIsRefreshedWithRows_ScrollsByRowsAgain_AndStillReachesItsLastColumn(string theme)
    {
        var laid = Lay(theme, 0, 12, false, 700, 300, rowsAfter: 200);

        AssertLastColumnReachable(laid);
        Assert.True(Shows(laid.Vertical), "200 rows do not fit in 300 px");
        Assert.True(laid.ContentScrolls, "rows scroll by row again once the grid has some");
        Assert.True(laid.Scroll.ScrollableHeight < 200 * 5, $"scrolling by row, the grid scrolls about 200 rows, not 200 rows of pixels: {laid.Scroll.ScrollableHeight}");
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AGridWithRows_ReachesItsLastColumn_At900x600AndAt1280x800(string theme)
    {
        AssertLastColumnReachable(Lay(theme, 60, 14, true, 900 - 20, 600 - 150));
        AssertLastColumnReachable(Lay(theme, 60, 14, true, 1280 - 20, 800 - 150));
    }
}
