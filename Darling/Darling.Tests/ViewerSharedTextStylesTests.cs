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
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8144: the shared text styles (page title, panel title, panel subtitle, KPI value, empty hint) resolve in the Viewer after the shared
/// ThemeManager applies each of its three themes, and carry their brushes from that theme. The Viewer's twin of the Lite test, which
/// runs the same check against Lite's theme files.
/// </summary>
public sealed class ViewerSharedTextStylesTests
{
    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void EveryTextStyle_ResolvesAfterThemeManagerAppliesTheTheme_AndKeepsItsBrushInTheTheme(string theme)
    {
        var failures = StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = name => new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");
            try
            {
                /* A switch from another theme first: the shared styles must survive it, not just the first apply. */
                ThemeManager.Apply(theme == "Dark" ? "Light" : "Dark");
                ThemeManager.Apply(theme);
                return Unresolved(app);
            }
            finally
            {
                ThemeManager.Apply(previous);
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in before)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }

                ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
            }
        });

        Assert.True(failures.Count == 0, $"{theme}:{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
    }

    private static List<string> Unresolved(Application app)
    {
        var failures = new List<string>();
        foreach (var (key, size, brush) in new[]
        {
            ("PageTitleText", 18.0, "ForegroundBrush"),
            ("PanelTitleText", 14.0, "ForegroundBrush"),
            ("PanelSubtitleText", 11.0, "ForegroundMutedBrush"),
            ("KpiValueText", 22.0, "ForegroundBrush"),
            ("EmptyHint", 0.0, "ForegroundMutedBrush"),
        })
        {
            if (app.TryFindResource(key) is not Style style)
            {
                failures.Add($"{key} does not resolve");
                continue;
            }

            var text = new TextBlock { Style = style };
            if (size > 0 && text.FontSize != size)
            {
                failures.Add($"{key} is {text.FontSize} px, expected {size}");
            }

            if (text.Foreground is not SolidColorBrush actual || app.TryFindResource(brush) is not SolidColorBrush expected || actual.Color != expected.Color)
            {
                failures.Add($"{key} does not carry {brush} from the theme");
            }
        }

        return failures;
    }
}
