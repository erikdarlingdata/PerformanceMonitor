/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8766: the Investigations window tab headers ("_Requests", "Report Feed_back", ...) rendered their underscores
/// literally, because each theme's custom TabItem template presented the header without
/// <c>RecognizesAccessKey</c>. This puts a real TabControl with each theme's TabItem style on screen and checks that
/// a string header is drawn as an access-key text (the key recognised, the underscore consumed), not as plain text.
///
/// <para>#8769: a string header's text is a TextBlock, and the theme's app-level implicit TextBlock style sets its
/// Foreground directly, which beats the value inherited from the TabItem (#3577). Each template shadows that style
/// inside the header, so the header ink is the TabItem's Foreground: dim on an unselected sub-tab, full when selected.
/// The theme is merged into Application resources, the only scope where the app-level style reaches the header.</para>
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerTabHeaderAccessKeyTests
{
    public static IEnumerable<object[]> ThemesAndStyles()
    {
        foreach (var theme in new[] { "DarkTheme", "LightTheme", "CoolBreezeTheme" })
        {
            yield return new object[] { theme, null! };
            yield return new object[] { theme, "SubTabItemStyle" };
        }
    }

    [Theory]
    [MemberData(nameof(ThemesAndStyles))]
    public void StringTabHeader_ShowsItsAccessKey_NotALiteralUnderscore(string theme, string? styleKey)
    {
        WithThemedTabs(theme, styleKey, new[] { "_Requests", "Report Feed_back" }, tabs =>
        {
            var keys = new List<string>();
            foreach (TabItem item in tabs.Items)
            {
                var accessText = Assert.Single(Descendants<AccessText>(item));
                keys.Add(accessText.AccessKey.ToString());

                // The text AccessText draws is its inner TextBlock.
                AssertInkIsTheTabForeground(item, Assert.Single(Descendants<TextBlock>(accessText)));
            }

            Assert.Equal(new[] { "R", "B" }, keys, StringComparer.OrdinalIgnoreCase);
        });
    }

    [Theory]
    [MemberData(nameof(ThemesAndStyles))]
    public void PlainTabHeader_InkIsTheTabForeground_SelectedAndUnselected(string theme, string? styleKey)
    {
        WithThemedTabs(theme, styleKey, new[] { "Overview", "Wait Stats" }, tabs =>
        {
            Assert.True(((TabItem)tabs.Items[0]).IsSelected);
            Assert.False(((TabItem)tabs.Items[1]).IsSelected);
            foreach (TabItem item in tabs.Items)
            {
                AssertInkIsTheTabForeground(item, Assert.Single(Descendants<TextBlock>(item)));
            }
        });
    }

    private static void AssertInkIsTheTabForeground(TabItem item, TextBlock ink)
    {
        var expected = Assert.IsType<SolidColorBrush>(item.Foreground).Color;
        Assert.Equal(expected, Assert.IsType<SolidColorBrush>(ink.Foreground).Color);
    }

    private static void WithThemedTabs(string theme, string? styleKey, string[] headers, Action<TabControl> check)
    {
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var dictionary = new ResourceDictionary
            {
                Source = new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}.xaml")
            };
            app.Resources.MergedDictionaries.Add(dictionary);
            try
            {
                var tabs = new TabControl();
                if (styleKey is not null)
                {
                    tabs.ItemContainerStyle = (Style)dictionary[styleKey];
                }
                foreach (var header in headers)
                {
                    tabs.Items.Add(new TabItem { Header = header, Content = new Border() });
                }
                var window = new Window { Content = tabs, Width = 600, Height = 300, ShowInTaskbar = false };
                try
                {
                    StaTestThread.Settle(window);
                    check(tabs);
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
