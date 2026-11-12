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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8223 (part 4): a grid whose columns do not fill it draws one more header, with no content, after the last column. It took the header's cell
/// border, so it read as a blank header cell ("a blank header cell after Working Set Δ MB"). Lite's twin is the Scheduler Issues test in
/// GridPolishRound2Tests; this lays a grid out in a window themed with each of the Viewer's themes and reads the filler header's border.
/// </summary>
public sealed class ViewerGridPolishPart4Tests
{
    public static IEnumerable<object[]> Themes() =>
        new[] { "DarkTheme", "LightTheme", "CoolBreezeTheme" }.Select(t => new object[] { t });

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheHeaderAfterTheLastColumn_HasNoCellBorder(string theme)
    {
        var found = StaTestThread.Run(() =>
        {
            var grid = new DataGrid { AutoGenerateColumns = false, IsReadOnly = true };
            for (var i = 0; i < 3; i++)
            {
                grid.Columns.Add(new DataGridTextColumn { Header = "Column " + i, Binding = new System.Windows.Data.Binding("."), Width = 130 });
            }

            grid.ItemsSource = new List<string> { "row 1", "row 2" };
            grid.Width = 900;
            grid.Height = 300;
            var window = new Window { Content = new Canvas { Children = { grid } }, Width = 1000, Height = 400 };
            window.Resources.MergedDictionaries.Add(new ResourceDictionary
            {
                Source = new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}.xaml"),
            });
            grid.Style = (Style)window.FindResource(typeof(DataGrid));
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var filler = ClippedTextProbe.Descendants(grid).OfType<System.Windows.Controls.Primitives.DataGridColumnHeader>().First(h => h.Column is null);
                var border = ClippedTextProbe.Descendants(filler).OfType<Border>().First();
                return (filler.ActualWidth, border.BorderThickness.Right);
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(found.ActualWidth > 100, $"{theme}: the columns leave a blank header area ({found.ActualWidth:0} px)");
        Assert.True(found.Right == 0, $"{theme}: the blank header area after the last column has a {found.Right:0} px right border, so it reads as a header cell");
    }
}
