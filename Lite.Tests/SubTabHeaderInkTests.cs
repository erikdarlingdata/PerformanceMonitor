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
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8769: the tab styles dim an unselected tab through its Foreground, but a string header's text is a TextBlock, and the
/// theme's app-level implicit TextBlock style sets that TextBlock's Foreground directly, which beats the inherited value
/// (#3577). Each template shadows that style inside the header, so the header ink is the TabItem's Foreground. This merges
/// the real theme into Application resources (the only scope where the app-level style reaches the header), puts a
/// TabControl with each TabItem style on screen, and checks every header's ink, selected and unselected, plain and with
/// an access key.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class SubTabHeaderInkTests
{
    public static IEnumerable<object[]> ThemesAndStyles()
    {
        foreach (var theme in new[] { "Dark", "Light", "CoolBreeze" })
        {
            yield return new object[] { theme, null! };
            yield return new object[] { theme, "SubTabItemStyle" };
            yield return new object[] { theme, "SubSubTabItemStyle" };
        }
    }

    [Theory]
    [MemberData(nameof(ThemesAndStyles))]
    public void StringTabHeader_InkIsTheTabForeground_SelectedAndUnselected(string theme, string? styleKey)
    {
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var dictionary = new ResourceDictionary
            {
                Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}Theme.xaml")
            };
            app.Resources.MergedDictionaries.Add(dictionary);
            try
            {
                var tabs = new TabControl();
                if (styleKey is not null)
                {
                    tabs.ItemContainerStyle = (Style)dictionary[styleKey];
                }
                foreach (var header in new[] { "Overview", "Wait Stats", "_Queries" })
                {
                    tabs.Items.Add(new TabItem { Header = header, Content = new Border() });
                }
                var window = new Window { Content = tabs, Width = 600, Height = 300, ShowInTaskbar = false };
                try
                {
                    StaTestThread.Settle(window);

                    Assert.True(((TabItem)tabs.Items[0]).IsSelected);
                    foreach (TabItem item in tabs.Items)
                    {
                        // The access-key header draws through AccessText's inner TextBlock, so each tab has one TextBlock.
                        var ink = Assert.Single(Descendants<TextBlock>(item));
                        var expected = Assert.IsType<SolidColorBrush>(item.Foreground).Color;
                        Assert.Equal(expected, Assert.IsType<SolidColorBrush>(ink.Foreground).Color);
                    }
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                app.Resources.MergedDictionaries.Remove(dictionary);
            }
        });
    }

    private static IEnumerable<T> Descendants<T>(DependencyObject parent) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);
            if (child is T match)
            {
                yield return match;
            }
            foreach (var nested in Descendants<T>(child))
            {
                yield return nested;
            }
        }
    }
}
