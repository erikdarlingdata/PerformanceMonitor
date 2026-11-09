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
using System.Windows.Media;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's About window, rendered with its Dark theme, describes the product the way the rest of the Viewer does: its
/// heading is the title bar's product name, and its description names both engines the service monitors, SQL Server and
/// PostgreSQL, since the store it opens can hold either.
/// </summary>
public sealed class ViewerAboutWindowTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    [Fact]
    public void TheHeading_NamesTheProductAsTheTitleBarDoes()
    {
        var (title, heading) = StaTestThread.Run(() => WithTheme(() =>
        {
            var window = new AboutWindow();
            Show(window);
            var headingText = VisibleTexts(window).Single(t => t.FontSize == 18 && t.FontWeight == FontWeights.Bold).Text;
            var result = (window.Title, headingText);
            window.Close();
            return result;
        }));

        Assert.Equal("Performance Monitor Darling", heading);
        Assert.Equal("About " + heading, title);
    }

    [Fact]
    public void TheDescription_NamesBothEnginesTheServiceMonitors()
    {
        var description = StaTestThread.Run(() => WithTheme(() =>
        {
            var window = new AboutWindow();
            Show(window);
            var text = VisibleTexts(window).Single(t => t.TextWrapping == TextWrapping.Wrap && t.Text.StartsWith("The desktop viewer", StringComparison.Ordinal)).Text;
            window.Close();
            return text;
        }));

        Assert.Contains("SQL Server", description, StringComparison.Ordinal);
        Assert.Contains("PostgreSQL", description, StringComparison.Ordinal);
    }

    [Fact]
    public void TheContent_FitsTheWindowAtItsOwnSize()
    {
        var (needed, given) = StaTestThread.Run(() => WithTheme(() =>
        {
            var window = new AboutWindow();
            Show(window);
            var content = (FrameworkElement)window.Content;
            var client = (FrameworkElement)VisualTreeHelper.GetChild(window, 0);
            var result = (content.DesiredSize.Height, client.ActualHeight);
            window.Close();
            return result;
        }));

        Assert.True(needed <= given, $"the About window's content needs {needed:F0} px of height and the window gives it {given:F0} px");
    }

    private static void Show(Window window)
    {
        StaTestThread.ShowOffScreen(window);
        ClippedTextProbe.Settle(window);
    }

    private static IEnumerable<TextBlock> VisibleTexts(DependencyObject root)
        => ClippedTextProbe.Descendants(root).OfType<TextBlock>().Where(t => t.IsVisible);

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("BackgroundLightBrush"))
        {
            added = new ResourceDictionary { Source = new Uri(ThemeUri) };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
