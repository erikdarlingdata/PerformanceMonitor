/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8225: the Viewer's count text reads the way the refreshed views do. A count label on a tab's toolbar is the muted subtitle
/// under a card title, not blue bold italic, and a count reads as a natural plural ("3 events", "1 recommendation"). The rendered
/// facts build the real Job History and Alert History tabs on the shared STA thread, themed as the app themes them, at 1280x800
/// and at 900x600, and read the realized font style and brush of each count label.
/// </summary>
public sealed class ViewerEmptyAndCountTextTests
{
    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    [Theory]
    [InlineData(1, "1 recommendation")]
    [InlineData(3, "3 recommendations")]
    public void RecommendationsCount_ReadsAsANaturalPlural(int count, string expected)
    {
        Assert.Equal(expected, FinOpsRecommendationsCountText.For(count));
    }

    public static IEnumerable<object[]> ThemesAndSizes()
    {
        foreach (var theme in new[] { "DarkTheme", "LightTheme", "CoolBreezeTheme" })
        {
            yield return new object[] { theme, 1280, 800 };
            yield return new object[] { theme, 900, 600 };
        }
    }

    [Theory]
    [MemberData(nameof(ThemesAndSizes))]
    public void JobHistoryAndAlertHistoryCountLabels_AreMutedAndUpright_InsideTheTab(string theme, int width, int height)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, resources =>
        {
            var muted = ((SolidColorBrush)resources["ForegroundMutedBrush"]).Color;
            var found = new List<string>();
            var jobs = new JobHistoryTab();
            jobs.JobCountIndicator.Text = "12 runs";
            found.AddRange(Check("Job History", theme, jobs, jobs.JobCountIndicator, muted, width, height));
            var alerts = new AlertsHistoryTab();
            alerts.AlertCountIndicator.Text = "7 alerts";
            found.AddRange(Check("Alert History", theme, alerts, alerts.AlertCountIndicator, muted, width, height));
            return found;
        }));

        Assert.True(problems.Count == 0, $"{theme} at {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
    }

    private static List<string> Check(string tabName, string theme, UserControl tab, TextBlock label, Color muted, int width, int height)
    {
        var problems = new List<string>();
        var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None };
        window.Show();
        try
        {
            Settle(window);
            Shoot(window, $"{tabName.Replace(" ", "", StringComparison.Ordinal)}-{theme}-{width}x{height}.png");

            if (!label.IsVisible)
            {
                problems.Add($"{tabName}: the count label is not visible");
                return problems;
            }

            if (label.FontStyle != FontStyles.Normal)
            {
                problems.Add($"{tabName}: the count label is {label.FontStyle}");
            }

            if (label.FontWeight != FontWeights.Normal)
            {
                problems.Add($"{tabName}: the count label is {label.FontWeight}");
            }

            if (label.Foreground is not SolidColorBrush brush || brush.Color != muted)
            {
                problems.Add($"{tabName}: the count label is {Describe(label.Foreground)}, not the muted foreground {muted}");
            }

            var bounds = label.TransformToAncestor(window).TransformBounds(new Rect(label.RenderSize));
            if (bounds.Right > window.ActualWidth + 0.5 || bounds.Bottom > window.ActualHeight + 0.5)
            {
                problems.Add($"{tabName}: the count label ends at {bounds.Right:0}x{bounds.Bottom:0} in a {window.ActualWidth:0}x{window.ActualHeight:0} window");
            }
        }
        finally
        {
            window.Close();
        }

        return problems;
    }

    private static string Describe(Brush? brush) => brush is SolidColorBrush solid ? solid.Color.ToString() : brush?.ToString() ?? "null";

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        Dispatcher.CurrentDispatcher.Invoke(DispatcherPriority.Loaded, () => { });
        window.UpdateLayout();
    }

    /// <summary>Saves the window as a PNG under REFRESH_SHOTS_DIR when that directory is given, so a run can be looked at; otherwise nothing.</summary>
    private static void Shoot(Window window, string fileName)
    {
        var dir = Environment.GetEnvironmentVariable("REFRESH_SHOTS_DIR");
        if (string.IsNullOrEmpty(dir) || window.Content is not FrameworkElement content || content.ActualWidth <= 0)
        {
            return;
        }

        var bitmap = new RenderTargetBitmap((int)content.ActualWidth, (int)content.ActualHeight, 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(content);
        var encoder = new PngBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create(bitmap));
        Directory.CreateDirectory(dir);
        using var stream = File.Create(Path.Combine(dir, fileName));
        encoder.Save(stream);
    }

    private static T WithTheme<T>(string theme, Func<ResourceDictionary, T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>
        {
            new() { Source = new Uri(ViewerPack + theme + ".xaml") },
            new() { Source = ThemeManager.SharedStylesUri },
        };
        foreach (var dictionary in added)
        {
            app.Resources.MergedDictionaries.Add(dictionary);
        }

        try
        {
            return body(app.Resources);
        }
        finally
        {
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Remove(dictionary);
            }
        }
    }
}
