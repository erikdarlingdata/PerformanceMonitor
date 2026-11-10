/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8540: the status bar's "Access: read-only" stays readable when the theme changes at run time. The real main window is built on the
/// shared STA thread under the Dark theme and hosted at 1000x700, inside a 1024x768 screen; the access field is given the read-only
/// state, the application's theme dictionary is replaced, and the text's colour is read from the element against the brush the main window paints its background with.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerSeatTextThemeTests
{
    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    private static Color ColorOf(Brush? brush) => brush is SolidColorBrush solid ? solid.Color : Colors.Magenta;

    private static MainWindow Shell()
    {
        var mode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            return new MainWindow();
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
        }
    }

    /// <summary>A window that was never shown has no layout, so the main window's content is moved into a shown host of the given size.</summary>
    private static Window Host(MainWindow w, double width, double height)
    {
        var root = (UIElement)w.Content;
        w.Content = null;
        var host = new Window { Content = root, Width = width, Height = height, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
        StaTestThread.ShowOffScreen(host);
        ClippedTextProbe.Settle(host);
        return host;
    }

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
    [InlineData("Dark")]
    public void ReadOnlyAccess_IsReadableOnTheStatusBar_AfterTheThemeChanges(string theme)
    {
        var (shown, text, bar) = StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            app.Resources.MergedDictionaries.Clear();
            app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri("Dark") });
            try
            {
                var w = Shell();
                var host = Host(w, 1000, 700);
                try
                {
                    typeof(MainWindow).GetMethod("ApplySeatState", Instance)!.Invoke(w, new object[] { ViewerSeatState.ReadOnly });

                    // The theme changes the way Settings changes it: the application's theme dictionary is replaced.
                    app.Resources.MergedDictionaries.Clear();
                    app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = ThemeUri(theme) });
                    ClippedTextProbe.Settle(host);

                    var field = (TextBlock)w.FindName("SeatText");
                    return (field.Text, ColorOf(field.Foreground), ColorOf((Brush)app.FindResource("BackgroundLightBrush")));
                }
                finally
                {
                    host.Close();
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

        Assert.Equal("Access: read-only", shown);
        var ratio = Contrast(text, bar);
        Assert.True(ratio >= 4.5, $"{theme}: \"{shown}\" is {text} on the status bar's {bar}, {ratio:0.00}:1, under 4.5:1");
    }
}
