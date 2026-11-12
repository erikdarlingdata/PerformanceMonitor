/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7383: a grid with a fill (<c>Width="*"</c>) column took width back from its fixed columns once they did not fit, down to 20 px, so the
/// columns read as "Bacl" and "Us" and there was no horizontal scrollbar. Lite's grids take their style from the theme dictionaries, so this lays
/// a plain grid out in a window themed with each of them. The viewer's twin, which also covers the viewer's own grids, is ViewerFillColumnTests
/// in Darling.Tests.
/// </summary>
public class FillColumnTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    private static ResourceDictionary Theme(string name) =>
        (ResourceDictionary)XamlReader.Parse(File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Themes", name + ".xaml")));

    /// <summary>A window narrower than any screen the tests run on; the grid under test is sized by its host, so the window only has to exist.</summary>
    private const double ScreenProofWindowWidth = 600;

    /// <summary>Twelve 100 px columns and a fill column in a window themed as Lite themes it: what each column was given, and how far the grid scrolls sideways.</summary>
    private static (double[] Fixed, double Fill, double Scrollable, double Viewport) Lay(string theme, double width) =>
        StaTestThread.Run(() =>
        {
            var grid = new DataGrid { AutoGenerateColumns = false, ItemsSource = new[] { new object() } };
            for (var i = 0; i < 12; i++)
            {
                grid.Columns.Add(new DataGridTextColumn { Header = "Col" + i, Width = 100 });
            }

            grid.Columns.Add(new DataGridTextColumn { Header = "Fill", Width = new DataGridLength(1, DataGridLengthUnitType.Star) });
            /* The grid is given its width by the canvas, not by the window: Windows clamps a window to the screen, and a small CI screen made a "wide" 1500 px window about 1000 px. */
            grid.Width = width;
            grid.Height = 300;
            var window = new Window { Content = new Canvas { Children = { grid } }, Width = ScreenProofWindowWidth, Height = 400 };
            window.Resources.MergedDictionaries.Add(Theme(theme));
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var scroll = (ScrollViewer)grid.Template.FindName("DG_ScrollViewer", grid);
                return (grid.Columns.Take(12).Select(c => c.ActualWidth).ToArray(), grid.Columns[12].ActualWidth, scroll.ScrollableWidth, scroll.ViewportWidth);
            }
            finally
            {
                window.Close();
            }
        });

    [Theory]
    [InlineData("DarkTheme")]
    [InlineData("LightTheme")]
    [InlineData("CoolBreezeTheme")]
    public void AGridWithAFillColumn_KeepsItsFixedColumns_AndScrollsSideways_WhenItsColumnsDoNotFit(string theme)
    {
        var (fixedWidths, fill, scrollable, viewport) = Lay(theme, 700);

        Assert.All(fixedWidths, w => Assert.Equal(100, w, 0.5));
        Assert.True(fill >= 150, $"the fill column keeps its floor, got {fill}");
        Assert.True(scrollable > 500, $"the grid should scroll sideways; it scrolls {scrollable} in a viewport of {viewport}");
    }

    [Theory]
    [InlineData("DarkTheme")]
    [InlineData("LightTheme")]
    [InlineData("CoolBreezeTheme")]
    public void AGridWithAFillColumn_GivesTheFillColumnTheRoomLeft_WhenTheWindowIsWide(string theme)
    {
        var (fixedWidths, fill, scrollable, viewport) = Lay(theme, 1500);

        Assert.All(fixedWidths, w => Assert.Equal(100, w, 0.5));
        Assert.Equal(viewport - 1200, fill, 1.5);
        Assert.True(scrollable < 1, $"nothing to scroll when the columns fit; it scrolls {scrollable}");
    }
}
