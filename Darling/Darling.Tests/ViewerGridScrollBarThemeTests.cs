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

namespace Darling.Tests;

/// <summary>
/// #8223: the Viewer's themes had the same stock scroll bars and the same grids that showed both scroll bars with nothing to scroll as Lite's, and its
/// empty grids cut their last column the same way. Lite's twin is GridScrollBarThemeTests in Lite.Tests; this lays a grid out in a window themed
/// with each of the Viewer's themes and reads back what was rendered.
/// </summary>
public sealed class ViewerGridScrollBarThemeTests
{
    private sealed record Laid(bool VerticalShown, bool HorizontalShown, int ArrowButtons, double ThumbWidth, Color ThumbCenter, Color ThemeBorder,
        double ColumnsTotal, double Viewport, double Scrollable);

    private static bool Shows(ScrollBar bar) => bar.Visibility == Visibility.Visible && bar.ActualWidth > 0 && bar.ActualHeight > 0;

    private static Laid Lay(string theme, int rows, int columns, double width, double height) =>
        StaTestThread.Run(() =>
        {
            var grid = new DataGrid { AutoGenerateColumns = false, IsReadOnly = true };
            for (var i = 0; i < columns; i++)
            {
                grid.Columns.Add(new DataGridTextColumn { Header = "Column " + i, Binding = new System.Windows.Data.Binding("."), Width = 130 });
            }

            grid.ItemsSource = Enumerable.Range(0, rows).Select(i => "row " + i).ToList();
            grid.Width = width;
            grid.Height = height;
            var window = new Window { Content = new Canvas { Children = { grid } }, Width = 600, Height = 400 };
            window.Resources.MergedDictionaries.Add(new ResourceDictionary
            {
                Source = new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}.xaml"),
            });
            grid.Style = (Style)window.FindResource(typeof(DataGrid));
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var scroll = (ScrollViewer)grid.Template.FindName("DG_ScrollViewer", grid);
                var vertical = (ScrollBar)scroll.Template.FindName("PART_VerticalScrollBar", scroll);
                var horizontal = (ScrollBar)scroll.Template.FindName("PART_HorizontalScrollBar", scroll);

                var thumbColor = Colors.Transparent;
                var thumb = ClippedTextProbe.Descendants(vertical).OfType<Thumb>().FirstOrDefault(t => t.ActualHeight > 0 && t.ActualWidth > 0);
                if (thumb is not null && Shows(vertical))
                {
                    var bounds = thumb.TransformToAncestor(grid).TransformBounds(new Rect(0, 0, thumb.ActualWidth, thumb.ActualHeight));
                    var bitmap = new RenderTargetBitmap((int)Math.Ceiling(grid.ActualWidth), (int)Math.Ceiling(grid.ActualHeight), 96, 96, PixelFormats.Pbgra32);
                    bitmap.Render(grid);
                    var pixel = new byte[4];
                    bitmap.CopyPixels(new Int32Rect((int)(bounds.X + bounds.Width / 2), (int)(bounds.Y + bounds.Height / 2), 1, 1), pixel, 4, 0);
                    thumbColor = Color.FromArgb(pixel[3], pixel[2], pixel[1], pixel[0]);
                }

                var arrows = ClippedTextProbe.Descendants(vertical).Concat(ClippedTextProbe.Descendants(horizontal)).OfType<RepeatButton>()
                    .Count(b => b.IsVisible && b.ActualWidth > 0 && (b.Command == ScrollBar.LineUpCommand || b.Command == ScrollBar.LineDownCommand
                        || b.Command == ScrollBar.LineLeftCommand || b.Command == ScrollBar.LineRightCommand));

                return new Laid(Shows(vertical), Shows(horizontal), arrows, vertical.ActualWidth, thumbColor,
                    ((SolidColorBrush)grid.FindResource("BorderBrush")).Color, grid.Columns.Sum(c => c.ActualWidth), scroll.ViewportWidth, scroll.ScrollableWidth);
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
    public void AViewerGridWithNothingToScroll_ShowsNoScrollBar(string theme)
    {
        var two = Lay(theme, 2, 3, 900, 500);
        var empty = Lay(theme, 0, 3, 900, 500);

        Assert.False(two.VerticalShown || two.HorizontalShown, "two rows that fit: no scroll bar");
        Assert.False(empty.VerticalShown || empty.HorizontalShown, "an empty grid whose columns fit: no scroll bar, no corner square");
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AViewerGridWithTwoHundredRows_ShowsAThemedThinScrollBar_WithoutArrowButtons(string theme)
    {
        var laid = Lay(theme, 200, 3, 900, 400);

        Assert.True(laid.VerticalShown);
        Assert.Equal(0, laid.ArrowButtons);
        Assert.True(laid.ThumbWidth <= 14, $"a thin scroll bar, got {laid.ThumbWidth}");
        Assert.Equal((laid.ThemeBorder.R, laid.ThemeBorder.G, laid.ThemeBorder.B), (laid.ThumbCenter.R, laid.ThumbCenter.G, laid.ThumbCenter.B));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AnEmptyViewerGridWiderThanItsHost_ScrollsToItsLastColumn(string theme)
    {
        var laid = Lay(theme, 0, 12, 700, 300);

        Assert.True(laid.HorizontalShown, "twelve 130 px columns do not fit in 700 px");
        Assert.True(laid.Scrollable >= laid.ColumnsTotal - laid.Viewport - 1,
            $"columns are {laid.ColumnsTotal} wide in a viewport of {laid.Viewport}; the grid scrolls {laid.Scrollable}");
    }
}
