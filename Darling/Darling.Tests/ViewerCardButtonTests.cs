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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8226: a button on a recommendation card reads as one, and the server toolbar draws no separator between the auto-refresh group
/// and the Times group. Each fact builds the real control on the shared STA thread with the theme and the shared styles merged, shows
/// it at the width a tab has beside the sidebar of a 1280x800 and a 900x600 window, and reads the realized result. With
/// REFRESH_SHOTS_DIR set, each screen is saved as a PNG.
/// </summary>
public sealed class ViewerCardButtonTests
{
    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly (int Width, int Height)[] Sizes = { (1280, 800), (900, 600) };

    /* The sidebar beside a tab in MainWindow.xaml: 280 px. */
    private static int PaneWidth(int windowWidth) => windowWidth - 280;

    public static IEnumerable<object[]> Themes()
    {
        yield return new object[] { "DarkTheme" };
        yield return new object[] { "LightTheme" };
        yield return new object[] { "CoolBreezeTheme" };
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheAskAiButton_StandsOutFromItsCard(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var found = new List<string>();
            var template = CardTemplate();
            var card = RecommendationsViewModel.FromFindings(new List<ViewerFindingRow> { ViewerPanelCardsTests.Row(1.8, "Plan cache bloat", "inc-A") }, "SQL2022").Sections[0].Cards[0];
            var presenter = new ContentPresenter { Content = card, ContentTemplate = template, Margin = new Thickness(10) };
            foreach (var (width, height) in Sizes)
            {
                var window = Open(presenter, PaneWidth(width), height);
                try
                {
                    var at = $"{theme} {width}x{height}";
                    var border = Visual<Border>(presenter).First();
                    var button = Visual<Button>(presenter).FirstOrDefault(b => b.Content as string == "Ask AI");
                    if (button is null || !button.IsVisible)
                    {
                        found.Add($"{at}: the card shows no Ask AI button");
                    }
                    else if (Describe(button.Background) == Describe(border.Background))
                    {
                        found.Add($"{at}: Ask AI is drawn in the card's own background ({Describe(border.Background)}), so it does not read as a button");
                    }

                    Shoot(window, $"{theme}-{width}x{height}-recommendation-card.png");
                }
                finally
                {
                    window.Close();
                }
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheServerToolbar_HasNoSeparatorAfterTheAutoRefreshGroup(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var found = new List<string>();
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "toolbar-test", "toolbar-test", true, null));
            typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
            foreach (var (width, height) in Sizes)
            {
                var window = Open(tab, PaneWidth(width), height);
                try
                {
                    var autoRefresh = (FrameworkElement)tab.FindName("AutoRefreshCheckBox")!;
                    var times = (FrameworkElement)tab.FindName("TimeDisplayModeBox")!;
                    var toolbar = PanelOf(autoRefresh);
                    var items = toolbar.Children.OfType<DependencyObject>().ToList();
                    var afterAutoRefresh = items.IndexOf(ItemOf(autoRefresh, toolbar)) + 1;
                    if (afterAutoRefresh <= 0 || afterAutoRefresh >= items.Count || !items[afterAutoRefresh].Equals(ItemOf(times, toolbar)))
                    {
                        var between = afterAutoRefresh > 0 && afterAutoRefresh < items.Count ? items[afterAutoRefresh].GetType().Name : "nothing";
                        found.Add($"{theme} {width}x{height}: the Times group does not follow the auto-refresh group directly ({between} sits between them)");
                    }

                    Shoot(window, $"{theme}-{width}x{height}-toolbar.png");
                }
                finally
                {
                    window.Close();
                }
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// The recommendation card template lives in the Recommendations tab's resources in MainWindow.xaml, so the shell is built (never
    /// shown) to read it. Its constructor seeds the process-wide time display mode from the app settings; that is put back afterwards.
    /// </summary>
    private static DataTemplate CardTemplate()
    {
        var mode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            var shell = new MainWindow();
            return (DataTemplate)((TabItem)shell.FindName("RecommendationsTab")).Resources["RecoCardTemplate"];
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
        }
    }

    /// <summary>The toolbar <paramref name="element"/> sits on: the nearest wrapping panel up the logical tree.</summary>
    private static Panel PanelOf(DependencyObject element)
    {
        for (DependencyObject? up = LogicalTreeHelper.GetParent(element); up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is WrapPanel panel)
            {
                return panel;
            }
        }

        throw new Xunit.Sdk.XunitException("the element is not on the toolbar");
    }

    /// <summary>The toolbar item that holds <paramref name="element"/>: the element itself, or the group around it.</summary>
    private static DependencyObject ItemOf(DependencyObject element, Panel toolbar)
    {
        for (DependencyObject? up = element; up is not null; up = LogicalTreeHelper.GetParent(up))
        {
            if (up is UIElement item && toolbar.Children.Contains(item))
            {
                return up;
            }
        }

        throw new Xunit.Sdk.XunitException("the element is not on the toolbar");
    }

    private static IEnumerable<T> Visual<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var below in Visual<T>(child))
            {
                yield return below;
            }
        }
    }

    private static Window Open(FrameworkElement content, int width, int height)
    {
        var window = new Window { Content = content, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
        window.Show();
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
        return window;
    }

    private static string Describe(Brush? brush) => brush is SolidColorBrush solid ? solid.Color.ToString() : brush?.ToString() ?? "null";

    /// <summary>Saves the window's content as a PNG under REFRESH_SHOTS_DIR when that directory is given; otherwise nothing.</summary>
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

    private static T WithTheme<T>(string theme, Func<T> body)
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
            return body();
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
