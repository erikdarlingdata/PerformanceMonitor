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
using System.Windows.Controls.Primitives;
using System.Windows.Markup;
using System.Windows.Media;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8169: the server tab strip is one row that scrolls, with a "»" button that lists the tabs that do not fit and selects the one
/// clicked; tabs are flat, with no solid accent fill. The real ServerTab is built on the shared STA thread, themed, shown at the
/// given width, and everything is read from the rendered controls.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabStripTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string OverflowButtonName = "PART_TabOverflowButton";
    private const string ScrollName = "PART_TabStripScroll";

    [Theory]
    [InlineData("Dark", 900, 600)]
    [InlineData("Light", 900, 600)]
    [InlineData("CoolBreeze", 900, 600)]
    [InlineData("Dark", 1280, 800)]
    public void TheServerTabAndSubTabStrips_AreOneRow_AndTheOverflowButtonListsExactlyTheTabsThatDoNotFit(string theme, int width, int height)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabStrip_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var problems = StaTestThread.Run(() => WithAppTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "tab-strip-test", DisplayName = "tab-strip-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
                window.Resources.MergedDictionaries.Add(LoadTheme(theme));
                StaTestThread.ShowOffScreen(window);
                try
                {
                    StaTestThread.Settle(window);
                    var found = new List<string>();
                    var main = (TabControl)tab.FindName("MainTabControl");
                    if (!ReferenceEquals(main.ItemContainerStyle, window.FindResource("SubTabItemStyle")))
                    {
                        found.Add("[server tabs] the server tab strip does not use SubTabItemStyle");
                    }

                    found.AddRange(CheckStrip(window, main, "server tabs"));

                    var queries = main.Items.OfType<TabItem>().First(t => Equals(t.Header, "Queries"));
                    main.SelectedItem = queries;
                    StaTestThread.Settle(window);
                    found.AddRange(CheckStrip(window, (TabControl)tab.FindName("QueriesSubTabControl"), "Queries sub-tabs"));
                    return found;
                }
                finally
                {
                    window.Close();
                }
            }));

            Assert.True(problems.Count == 0, $"{theme} at {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, problems));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>One row; the button is shown only when something does not fit; its menu lists those tabs and no others; a click selects.</summary>
    private static List<string> CheckStrip(Window window, TabControl tabs, string label)
    {
        var found = new List<string>();
        var scroll = tabs.Template.FindName(ScrollName, tabs) as ScrollViewer;
        var button = tabs.Template.FindName(OverflowButtonName, tabs) as Button;
        if (scroll is null || button is null)
        {
            return new List<string> { $"[{label}] the strip has no scrolling row ({ScrollName}) or no overflow button ({OverflowButtonName})" };
        }

        var visible = tabs.Items.OfType<TabItem>().Where(t => t.Visibility == Visibility.Visible).ToList();
        var tops = visible.Select(t => t.TransformToAncestor(scroll).Transform(new Point(0, 0)).Y).ToList();
        if (tops.Max() - tops.Min() > 1)
        {
            found.Add($"[{label}] the tabs take more than one row (tops {tops.Min():F0}..{tops.Max():F0})");
        }

        var expected = visible.Where(t => !Fits(t, scroll)).Select(t => (string)t.Header).ToList();
        if (expected.Count == 0)
        {
            if (button.Visibility == Visibility.Visible)
            {
                found.Add($"[{label}] every tab fits but the overflow button is shown");
            }

            return found;
        }

        if (button.Visibility != Visibility.Visible)
        {
            found.Add($"[{label}] {expected.Count} tabs do not fit but the overflow button is hidden");
            return found;
        }

        button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
        var menu = button.ContextMenu;
        var listed = menu is null ? new List<string>() : menu.Items.OfType<MenuItem>().Select(i => (string)i.Header).ToList();
        if (!listed.SequenceEqual(expected))
        {
            found.Add($"[{label}] the overflow menu lists [{string.Join(", ", listed)}] but the tabs that do not fit are [{string.Join(", ", expected)}]");
        }

        if (menu is not null && menu.Items.Count > 0)
        {
            var last = (MenuItem)menu.Items[^1];
            last.RaiseEvent(new RoutedEventArgs(MenuItem.ClickEvent));
            menu.IsOpen = false;
            StaTestThread.Settle(window);
            var picked = (TabItem)tabs.SelectedItem;
            if (!Equals(picked.Header, expected[^1]))
            {
                found.Add($"[{label}] clicking '{expected[^1]}' selected '{picked.Header}'");
            }
            else if (!Fits(picked, scroll))
            {
                found.Add($"[{label}] the clicked tab '{picked.Header}' was selected but left outside the strip");
            }
        }

        return found;
    }

    private static bool Fits(TabItem tab, ScrollViewer scroll)
    {
        var bounds = tab.TransformToAncestor(scroll).TransformBounds(new Rect(tab.RenderSize));
        return bounds.Left >= -0.5 && bounds.Right <= scroll.ViewportWidth + 0.5;
    }

    private static ResourceDictionary LoadTheme(string theme) =>
        (ResourceDictionary)XamlReader.Parse(ParitySource.ReadFile($"Lite/Themes/{theme}Theme.xaml"));

    private static T WithAppTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
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
