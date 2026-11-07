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
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8098: every control on a tab's toolbar is reachable at every window size the app allows. The real tabs are built on the shared STA
/// thread, themed as the app themes them, and shown at 1280x800 and at Lite's 900x600 minimum. Each control a reader needs must lie
/// inside the tab's bounds at its full width: a toolbar that does not wrap arranges its overflow past the right edge, where the header
/// clips it and nothing can reach it.
/// </summary>
public sealed class ServerTabToolbarWrapTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /* The sidebar's width in MainWindow.xaml; a tab is narrower than the window by that much. */
    private const int SidebarWidth = 280;

    [Theory]
    [InlineData(1280, 800)]
    [InlineData(900, 600)]
    public void TheServerTabToolbar_KeepsRefreshAutoRefreshAndTimesInsideTheTab(int width, int height)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabToolbarWrap_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var outside = StaTestThread.Run(() => WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "toolbar-wrap-test", DisplayName = "toolbar-wrap-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                return Outside(tab, width, height, "ServerNameText", "RefreshDataButton", "AutoRefreshCheckBox", "AutoRefreshIntervalCombo", "TimeDisplayModeBox");
            }));

            Assert.True(outside.Count == 0, $"server tab at {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, outside));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Theory]
    [InlineData(1280, 800)]
    [InlineData(900, 600)]
    public void TheJobAndAlertHistoryToolbars_KeepRefreshAndDismissAllInsideTheTab_BesideTheSidebar(int width, int height)
    {
        var outside = StaTestThread.Run(() => WithTheme(() =>
        {
            var found = new List<string>();
            found.AddRange(Outside(new JobHistoryTab(), width - SidebarWidth, height, "Refresh").Select(o => "[Job History] " + o));
            found.AddRange(Outside(new AlertsHistoryTab(), width - SidebarWidth, height, "Refresh", "DismissAllButton").Select(o => "[Alert History] " + o));
            return found;
        }));

        Assert.True(outside.Count == 0, $"beside the sidebar at {width}x{height}:{Environment.NewLine}" + string.Join(Environment.NewLine, outside));
    }

    /// <summary>
    /// Shows <paramref name="tab"/> as the content of a window of the given size and reports every named control (by x:Name, or a
    /// button by its caption) that is not wholly inside the tab's bounds, or is given less than its natural width.
    /// </summary>
    private static List<string> Outside(FrameworkElement tab, int width, int height, params string[] names)
    {
        var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
        window.Show();
        try
        {
            ClippedTextProbe.Settle(window);
            var found = new List<string>();
            foreach (var name in names)
            {
                var element = tab.FindName(name) as FrameworkElement
                    ?? ClippedTextProbe.Descendants(tab).OfType<Button>().FirstOrDefault(b => Equals(b.Content, name))
                    ?? throw new Xunit.Sdk.XunitException($"{name} is not on the tab");
                var bounds = element.TransformToAncestor(tab).TransformBounds(new Rect(element.RenderSize));
                var natural = element.DesiredSize.Width - element.Margin.Left - element.Margin.Right;
                if (bounds.Left < -0.5 || bounds.Right > tab.ActualWidth + 0.5 || element.RenderSize.Width + 0.5 < natural)
                {
                    found.Add($"{name} spans {bounds.Left:F0}..{bounds.Right:F0} of a {tab.ActualWidth:F0} wide tab (natural width {natural:F0}, given {element.RenderSize.Width:F0})");
                }
            }

            return found;
        }
        finally
        {
            window.Close();
        }
    }

    private static T WithTheme<T>(Func<T> body)
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
