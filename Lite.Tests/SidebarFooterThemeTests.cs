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
using PerformanceMonitorLite.Controls;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8169: the sidebar footer's buttons take the theme's Button style. A footer button given a style of its own, with no
/// BasedOn, drops the implicit theme style and renders as the stock light-gray Windows button, whose face the dark
/// themes' light text cannot be read on. Each theme is loaded as the app's theme, as the app loads it.
/// </summary>
[Collection("credential-manager")]
public sealed class SidebarFooterThemeTests
{
    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void EveryFooterButton_DrawsTheThemeButtonFace(string theme)
    {
        var faces = StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var saved = app.Resources.MergedDictionaries.ToList();
            app.Resources.MergedDictionaries.Clear();
            var dict = new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}Theme.xaml") };
            app.Resources.MergedDictionaries.Add(dict);
            try
            {
                var footer = new SidebarFooter { VerticalAlignment = VerticalAlignment.Top };
                var window = new Window { Content = footer, Width = 280, Height = 400, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    StaTestThread.Settle(window);
                    var themed = ((SolidColorBrush)dict["BackgroundLightBrush"]).Color;
                    return ClippedTextProbe.Descendants(footer).OfType<Button>()
                        .Select(b => (b.Name, Face: (b.Background as SolidColorBrush)?.Color, Themed: themed))
                        .ToList();
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                app.Resources.MergedDictionaries.Clear();
                foreach (var d in saved)
                {
                    app.Resources.MergedDictionaries.Add(d);
                }
            }
        });

        Assert.Equal(5, faces.Count);
        foreach (var (name, face, themed) in faces)
        {
            Assert.True(face == themed, $"{theme}: {name} draws {face?.ToString() ?? "a non-solid brush"}, not the theme's button face {themed}");
        }
    }
}
