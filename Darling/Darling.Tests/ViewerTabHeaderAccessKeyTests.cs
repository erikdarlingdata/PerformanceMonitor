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
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerTabHeaderAccessKeyTests
{
    public static IEnumerable<object[]> Themes()
    {
        yield return new object[] { "DarkTheme" };
        yield return new object[] { "LightTheme" };
        yield return new object[] { "CoolBreezeTheme" };
    }

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
                tabs.Items.Add(new TabItem { Header = "_Requests", Content = new Border() });
                tabs.Items.Add(new TabItem { Header = "Report Feed_back", Content = new Border() });
                var window = new Window { Content = tabs, Width = 600, Height = 300, ShowInTaskbar = false };
                try
                {
                    StaTestThread.Settle(window);

                    var keys = new List<string>();
                    foreach (TabItem item in tabs.Items)
                    {
                        var accessText = Assert.Single(Descendants<AccessText>(item));
                        keys.Add(accessText.AccessKey.ToString());

                        /* #3577: on the main TabItem style the header ink must still follow the TabItem's Foreground, which
                           that template's empty TextBlock style arranges. (SubTabItemStyle has never had that shadow style; its
                           ink is not part of this change.) The text AccessText draws is its inner TextBlock. */
                        if (styleKey is null)
                        {
                            var ink = Assert.Single(Descendants<TextBlock>(accessText));
                            var expected = Assert.IsType<SolidColorBrush>(item.Foreground).Color;
                            Assert.Equal(expected, Assert.IsType<SolidColorBrush>(ink.Foreground).Color);
                        }
                    }

                    Assert.Equal(new[] { "R", "B" }, keys, StringComparer.OrdinalIgnoreCase);
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
