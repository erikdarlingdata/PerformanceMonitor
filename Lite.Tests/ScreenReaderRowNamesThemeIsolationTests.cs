/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The recycled-row scroll of ScreenReaderRowNamesTests, run with a Lite theme merged at application scope, as a class that
/// applies a theme earlier in the same test process leaves it. The theme's implicit DataGridCell style pads every cell by 8,6,
/// which makes each row taller, so fewer rows fit the 300 px window on every scroll step. The test's grid must not take any
/// style the process left at application scope: the row count and the names it reads are the same with the theme as without.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public class ScreenReaderRowNamesThemeIsolationTests
{
    private static readonly Uri DarkTheme = new("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml");

    [Fact]
    public void WithALiteThemeMergedAtApplicationScope_TheScrollStillVisitsOverAThousandRows_AndEveryRowIsNamedForItsItem()
    {
        var (mismatches, checkedRows, _, _, _) = StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var theme = new ResourceDictionary { Source = DarkTheme };
            app.Resources.MergedDictionaries.Add(theme);
            try
            {
                return ScreenReaderRowNamesTests.ScrollARecycledGrid();
            }
            finally
            {
                app.Resources.MergedDictionaries.Remove(theme);
            }
        });

        Assert.True(checkedRows > 1000, "the scroll visited only " + checkedRows + " rows with the Dark theme merged at application scope");
        Assert.Equal(0, mismatches);
    }
}
