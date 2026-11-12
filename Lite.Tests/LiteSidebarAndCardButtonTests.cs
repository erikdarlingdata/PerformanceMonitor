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
using Darling.Tests;
using PerformanceMonitorLite.Analysis.Recommendations;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8226: the sidebar names a server once when its display name is its server name, a recommendation card's "Ask AI" reads as a
/// button against the card it sits on, and the server toolbar has no separator after the auto-refresh group. Each fact renders
/// the real template or tab on the shared STA thread with the theme and the shared styles merged, at both window sizes, and
/// reads the realized result. With REFRESH_SHOTS_DIR set, each screen is saved as a PNG.
/// </summary>
[Collection("credential-manager")]
public sealed class LiteSidebarAndCardButtonTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly (int Width, int Height)[] Sizes = { (1280, 800), (900, 600) };

    /* The sidebar's width in MainWindow.xaml, and the width a tab has beside it (or beside its 40 px rail below about 1100 DIPs, #8219). */
    private const int SidebarWidth = 280;
    public static IEnumerable<object[]> Themes()
    {
        yield return new object[] { "DarkTheme" };
        yield return new object[] { "LightTheme" };
        yield return new object[] { "CoolBreezeTheme" };
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheSidebar_ShowsTheServerName_OnlyWhenItDiffersFromTheDisplayName(string theme)
    {
        var dir = Path.Combine(Path.GetTempPath(), "SidebarLine_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            var problems = StaTestThread.Run(() => WithTheme(theme, () =>
            {
                /* The window reads these three paths when it is built; point them at a folder of the test's own, and put them back before the folder is deleted. */
                using var paths = new AppPathsScope(dir);

                var main = new MainWindow();
                var found = new List<string>();
                try
                {
                    /* The server row's template is the keyed one the list's selector hands out for a server row. */
                    var template = (DataTemplate)main.Resources["FleetServerRowTemplate"];
                    var same = Item("same-name", "same-name");
                    var differs = Item("server-a.example", "Display Name");
                    var list = new StackPanel();
                    list.Children.Add(new ContentPresenter { Content = same, ContentTemplate = template });
                    list.Children.Add(new ContentPresenter { Content = differs, ContentTemplate = template });
                    foreach (var (width, height) in Sizes)
                    {
                        var window = Open(list, SidebarWidth, height);
                        try
                        {
                            var at = $"{theme} {width}x{height}";
                            var sameLines = VisibleLines((FrameworkElement)list.Children[0], "same-name");
                            if (sameLines != 1)
                            {
                                found.Add($"{at}: a server whose display name is its server name shows that name {sameLines} times, not once");
                            }

                            var first = VisibleLines((FrameworkElement)list.Children[1], "Display Name");
                            var second = VisibleLines((FrameworkElement)list.Children[1], "server-a.example");
                            if (first != 1 || second != 1)
                            {
                                found.Add($"{at}: a server with its own display name shows the display name {first} times and the server name {second} times, not once each");
                            }

                            Shoot(window, $"{theme}-{width}x{height}-sidebar.png");
                        }
                        finally
                        {
                            window.Close();
                        }
                    }
                }
                finally
                {
                    main.Close();
                }

                return found;
            }));

            Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>A sidebar item the way the list builds one: the server row over a server whose connection carries the names.</summary>
    private static object Item(string serverName, string displayName) =>
        new PerformanceMonitorLite.FleetServerRow(
            new PerformanceMonitorLite.FleetServer(1, displayName, false, new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = serverName, DisplayName = displayName, IsEnabled = true }),
            depth: 0);

    /// <summary>How many visible text lines in <paramref name="item"/> read exactly <paramref name="text"/>.</summary>
    private static int VisibleLines(FrameworkElement item, string text) =>
        Visual<TextBlock>(item).Count(t => t.IsVisible && t.ActualWidth > 0 && t.Text == text);

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
        StaTestThread.ShowOffScreen(window);
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
        return window;
    }

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

    /// <summary>Runs <paramref name="body"/> with <paramref name="theme"/> and the shared styles as the app's resources.</summary>
    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var dictionary in saved)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }
    }
}
