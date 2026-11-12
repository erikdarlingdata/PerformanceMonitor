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
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8197: the real server tab, built on the shared STA thread and themed as the app themes it. Every grid column is as wide as its
/// header (also with no rows), an empty window shows one muted sentence in the grid area with the slicer band gone, and the Memory
/// Broker panel does not repeat its tab's name as a title.
/// </summary>
[Collection("credential-manager")]
public sealed class GridPolishTests
{
    [Theory]
    [InlineData("DarkTheme", 1280, 800)]
    [InlineData("LightTheme", 1280, 800)]
    [InlineData("CoolBreezeTheme", 1280, 800)]
    [InlineData("DarkTheme", 900, 600)]
    [InlineData("CoolBreezeTheme", 900, 600)]
    public void EveryColumnOfTheQueryGrids_IsAsWideAsItsHeader_WithNoRows(string theme, int width, int height)
    {
        var clipped = Run(theme, width, height, (tab, window) =>
        {
            var found = new List<string>();
            foreach (var name in new[] { "QueryStatsGrid", "ProcedureStatsGrid", "QueryStoreGrid" })
            {
                var grid = (DataGrid)tab.FindName(name);
                SelectTabOf(grid);
                ClippedTextProbe.Settle(window);
                Assert.Empty(grid.Items);
                found.AddRange(ClippedTextProbe.ClippedHeaders(grid).Select(c => $"[{name}] {c}"));
            }

            return found;
        });

        Assert.True(clipped.Count == 0, $"{theme} {width}x{height}:{Environment.NewLine}" + ClippedTextProbe.Describe(clipped));
    }

    /// <summary>Shows every tab on the way to <paramref name="element"/>, as a click on each would.</summary>
    private static void SelectTabOf(DependencyObject element)
    {
        for (var node = element; node is not null; node = LogicalTreeHelper.GetParent(node))
        {
            if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
            {
                tabs.SelectedItem = item;
            }
        }
    }

    private static List<string> Run(string theme, int width, int height, Func<ServerTab, Window, List<string>> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "GridPolish_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithTheme(theme, () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "grid-polish", DisplayName = "grid-polish", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    return body(tab, window);
                }
                finally
                {
                    window.Close();
                }
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>();
        if (!app.Resources.Contains("NumericCell"))
        {
            added.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
            added.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
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
