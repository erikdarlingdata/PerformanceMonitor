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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8540: a row of the FinOps locking detail view follows a theme changed while the view is open. The real tab is built on the STA thread
/// under Dark, a detail row is added to a panel in a shown window, the application's theme dictionary is replaced, and the row's label and
/// value are read from their text blocks against the page.
/// </summary>
[Collection("credential-manager")]
public sealed class FinOpsLockingRowThemeTests
{
    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}Theme.xaml");

    private static Color ColorOf(Brush? brush) => brush is SolidColorBrush solid ? solid.Color : Colors.Magenta;

    private static double Luminance(Color color)
    {
        static double Channel(byte value)
        {
            var c = value / 255.0;
            return c <= 0.03928 ? c / 12.92 : Math.Pow((c + 0.055) / 1.055, 2.4);
        }

        return (0.2126 * Channel(color.R)) + (0.7152 * Channel(color.G)) + (0.0722 * Channel(color.B));
    }

    private static double Contrast(Color a, Color b)
    {
        var la = Luminance(a);
        var lb = Luminance(b);
        return (Math.Max(la, lb) + 0.05) / (Math.Min(la, lb) + 0.05);
    }

    [Theory]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void ALockingDetailRow_IsReadable_AfterTheThemeChanges(string theme)
    {
        var (count, weak) = StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            app.Resources.MergedDictionaries.Clear();
            app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri("Dark") });
            try
            {
                var tab = new FinOpsTab();
                var rows = new StackPanel();
                var window = new Window { Content = rows, Width = 600, Height = 300 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var add = typeof(FinOpsTab).GetMethod("AddLockingDetailRow", Instance);
                    Assert.True(add is not null, "FinOpsTab.AddLockingDetailRow no longer exists under that name - this test's reflection anchor moved.");
                    add!.Invoke(tab, new object[] { rows, "Database", "tempdb" });

                    // The theme changes the way Settings changes it: the application's theme dictionary is replaced.
                    app.Resources.MergedDictionaries.Clear();
                    app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri(theme) });
                    ClippedTextProbe.Settle(window);

                    var page = ColorOf((Brush)app.FindResource("BackgroundBrush"));
                    var texts = rows.Children.OfType<Panel>().SelectMany(row => row.Children.OfType<TextBlock>()).ToList();
                    var found = new List<string>();
                    foreach (var text in texts)
                    {
                        var colour = ColorOf(text.Foreground);
                        var ratio = Contrast(colour, page);
                        if (ratio < 4.5)
                        {
                            found.Add($"{theme}: \"{text.Text}\" is {colour} against the page {page}, {ratio:0.00}:1, under 4.5:1");
                        }
                    }

                    return (texts.Count, found);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in before)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }
            }
        });

        Assert.Equal(2, count);
        Assert.True(weak.Count == 0, string.Join(Environment.NewLine, weak));
    }
}
