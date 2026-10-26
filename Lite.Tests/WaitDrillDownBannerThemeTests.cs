/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Media;
using PerformanceMonitorLite.Windows;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6773: the banner at the top of the Wait Drill-Down window painted its text from literals picked for the Dark theme (a light
/// blue for the "too brief to appear in query snapshots" and head-blocker notes, amber for the not-captured note), so on Light
/// and Cool Breeze the text was pale on a pale page. The tests build the real window against each theme's own dictionary and read
/// the brush the banner text and border actually resolve to, then swap the theme under the open window and read them again.
/// </summary>
public sealed class WaitDrillDownBannerThemeTests
{
    private static readonly string[] Themes = ["DarkTheme", "LightTheme", "CoolBreezeTheme"];

    private static ResourceDictionary Load(string theme) => new()
    {
        Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml"),
    };

    private static Color ColorOf(object? brush) => Assert.IsType<SolidColorBrush>(brush).Color;

    private static void OnWindow(string theme, Action<WaitDrillDownWindow, ResourceDictionary, ResourceDictionary> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var dict = Load(theme);
            var other = Load(theme == "DarkTheme" ? "LightTheme" : "DarkTheme");
            app.Resources.MergedDictionaries.Add(dict);
            try
            {
                /* The banner is set from the wait type alone; no data service call happens until the window loads. */
                var window = new WaitDrillDownWindow(null!, 1, "srv", "PAGEIOLATCH_SH", 1, TimeZoneInfo.Utc);
                try
                {
                    body(window, dict, other);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                app.Resources.MergedDictionaries.Remove(dict);
                app.Resources.MergedDictionaries.Remove(other);
            }
        });

    [Theory]
    [InlineData("DarkTheme", "PAGEIOLATCH_SH")]
    [InlineData("LightTheme", "PAGEIOLATCH_SH")]
    [InlineData("CoolBreezeTheme", "PAGEIOLATCH_SH")]
    [InlineData("CoolBreezeTheme", "LCK_M_X")]
    public void InfoBanner_UsesTheThemesInfoTextBrush_AndFollowsAThemeSwap(string theme, string waitType)
    {
        OnWindow(theme, (window, dict, other) =>
        {
            window.SetWarningBanner(WaitDrillDownHelper.Classify(waitType));

            Assert.Equal(Visibility.Visible, window.WarningBanner.Visibility);
            Assert.Equal(ColorOf(dict["InfoTextBrush"]), ColorOf(window.WarningText.Foreground));
            Assert.Equal(ColorOf(dict["InfoBrush"]), ColorOf(window.WarningBanner.BorderBrush));

            /* A theme switch while the window is open: the banner reads the new theme's brushes, not a copy of the first. */
            var app = Application.Current!;
            app.Resources.MergedDictionaries.Remove(dict);
            app.Resources.MergedDictionaries.Add(other);
            Assert.Equal(ColorOf(other["InfoTextBrush"]), ColorOf(window.WarningText.Foreground));
            Assert.Equal(ColorOf(other["InfoBrush"]), ColorOf(window.WarningBanner.BorderBrush));
        });
    }

    [Theory]
    [InlineData("DarkTheme")]
    [InlineData("LightTheme")]
    [InlineData("CoolBreezeTheme")]
    public void UncapturableBanner_UsesTheThemesWarningBrushes_AndFollowsAThemeSwap(string theme)
    {
        OnWindow(theme, (window, dict, other) =>
        {
            window.SetWarningBanner(WaitDrillDownHelper.Classify("THREADPOOL"));

            Assert.Equal(Visibility.Visible, window.WarningBanner.Visibility);
            Assert.Equal(ColorOf(dict["WarningTextBrush"]), ColorOf(window.WarningText.Foreground));
            Assert.Equal(ColorOf(dict["WarningBrush"]), ColorOf(window.WarningBanner.BorderBrush));

            var app = Application.Current!;
            app.Resources.MergedDictionaries.Remove(dict);
            app.Resources.MergedDictionaries.Add(other);
            Assert.Equal(ColorOf(other["WarningTextBrush"]), ColorOf(window.WarningText.Foreground));
            Assert.Equal(ColorOf(other["WarningBrush"]), ColorOf(window.WarningBanner.BorderBrush));
        });
    }

    [Theory]
    [InlineData("DarkTheme")]
    [InlineData("LightTheme")]
    [InlineData("CoolBreezeTheme")]
    public void BannerTextBrushes_AreReadableOnTheThemesPageSurfaces(string theme)
    {
        StaTestThread.Run(() =>
        {
            _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var dict = Load(theme);
            foreach (var textKey in new[] { "InfoTextBrush", "WarningTextBrush" })
            {
                foreach (var surfaceKey in new[] { "BackgroundBrush", "BackgroundLightBrush", "BackgroundDarkBrush" })
                {
                    var ratio = WcagContrast.Ratio(ColorOf(dict[textKey]), ColorOf(dict[surfaceKey]));
                    Assert.True(ratio >= WcagContrast.TextMinimum,
                        $"{theme}: {textKey} on {surfaceKey} is {WcagContrast.Format(ratio)}, under {WcagContrast.TextMinimum}:1");
                }
            }
        });
    }
}
