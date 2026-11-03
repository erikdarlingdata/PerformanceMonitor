/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7218: the Availability Groups cards set BorderBrush to the theme key CardBorderBrush, which no Lite theme defined, so the
/// card outline never drew. The two FinOps grids set AlternatingRowBackground to DataGridAlternatingRowBrush, which exists in no
/// theme at all; that local value sat on top of the theme's own DataGrid stripe. The tests load each Lite theme dictionary and
/// resolve the key, then build the real FinOps tab against each theme and read where each grid's AlternatingRowBackground comes from.
/// </summary>
public sealed class LiteThemeKeysResolveTests
{
    private static readonly string[] Themes = ["DarkTheme", "LightTheme", "CoolBreezeTheme"];

    private static ResourceDictionary Load(string theme) => new()
    {
        Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml"),
    };

    [Theory]
    [InlineData("DarkTheme")]
    [InlineData("LightTheme")]
    [InlineData("CoolBreezeTheme")]
    public void CardBorderBrush_ResolvesInEveryLiteTheme_AndIsVisibleAgainstTheCardBackground(string theme)
    {
        StaTestThread.Run(() =>
        {
            /* The pack:// scheme is registered by creating the Application. */
            _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var dict = Load(theme);

            var border = Assert.IsType<SolidColorBrush>(dict["CardBorderBrush"]);
            var card = Assert.IsType<SolidColorBrush>(dict["BackgroundLightBrush"]);
            Assert.NotEqual(card.Color, border.Color);
        });
    }

    [Theory]
    [InlineData("DarkTheme")]
    [InlineData("LightTheme")]
    [InlineData("CoolBreezeTheme")]
    public void FinOpsGrids_TakeTheirStripeFromTheTheme_NotFromAnUnresolvedLocalKey(string theme)
    {
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var dict = Load(theme);
            app.Resources.MergedDictionaries.Add(dict);
            try
            {
                var tab = new FinOpsTab();
                var grids = new List<DataGrid>();
                Collect(tab, grids);

                Assert.NotEmpty(grids);
                foreach (var grid in grids)
                {
                    var source = DependencyPropertyHelper.GetValueSource(grid, DataGrid.AlternatingRowBackgroundProperty);
                    Assert.True(source.BaseValueSource != BaseValueSource.Local,
                        $"DataGrid '{grid.Name}' sets AlternatingRowBackground locally, which hides the theme's stripe.");
                }
            }
            finally
            {
                app.Resources.MergedDictionaries.Remove(dict);
            }
        });
    }

    private static void Collect(DependencyObject root, List<DataGrid> found)
    {
        if (root is DataGrid grid)
        {
            found.Add(grid);
        }

        foreach (var child in LogicalTreeHelper.GetChildren(root))
        {
            if (child is DependencyObject d)
            {
                Collect(d, found);
            }
        }
    }
}
