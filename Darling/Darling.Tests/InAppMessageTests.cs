/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The in-app message the plan code shows in place of the native "No Plan Available" box: it is drawn from the theme's brushes, it
/// carries the sentence, and it has one OK button. Built with the Viewer's dark theme merged, the way the apps run it.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class InAppMessageTests
{
    [Fact]
    public void TheMessage_TakesTheThemesBrushes_AndCarriesTheSentenceAndOneButton()
    {
        var (background, expected, text, buttons) = StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("BackgroundBrush"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }

            try
            {
                var window = InAppMessage.Build(null, "No Plan Available", "No query plan is available for this row.");
                var panel = (StackPanel)window.Content;
                return (
                    (window.Background as SolidColorBrush)?.Color,
                    ((SolidColorBrush)app.Resources["BackgroundBrush"]).Color,
                    panel.Children.OfType<TextBlock>().Single().Text,
                    panel.Children.OfType<Button>().Count());
            }
            finally
            {
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
            }
        });

        Assert.Equal(expected, background);
        Assert.Equal("No query plan is available for this row.", text);
        Assert.Equal(1, buttons);
    }
}
