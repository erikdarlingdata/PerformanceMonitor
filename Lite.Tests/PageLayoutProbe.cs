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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The rendered-page helpers the #8307 layout tests share: the real server tab on the STA thread under a theme, shown at a stated window
/// size, and the realized facts a reader sees (bounds, the scroll viewer a grid or a page scrolls with, a template part).
/// </summary>
internal static class PageLayoutProbe
{
    internal const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>The width a tab has beside the sidebar of a window: 280 px, or the 40 px rail below about 1100 DIPs (#8219).</summary>
    internal static int PaneWidth(int windowWidth) => windowWidth - (windowWidth < 1100 ? 40 : 280);

    /// <summary>
    /// Builds the real server tab under <paramref name="theme"/> with <paramref name="credentials"/> (the caller's, so the class that reaches
    /// the Credential Manager is the one in its collection), shows it at <paramref name="width"/> x <paramref name="height"/>, and runs <paramref name="body"/>.
    /// </summary>
    internal static T WithServerTab<T>(string theme, int width, int height, CredentialResolver credentials, Func<ServerTab, Window, DuckDbInitializer, T> body,
        int utcOffsetMinutes = 0, Func<bool>? isLongQueryTraceEnabled = null)
    {
        var dir = Path.Combine(Path.GetTempPath(), "PageLayout_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            duckDb.InitializeAsync().GetAwaiter().GetResult();
            return StaTestThread.Run(() => WithTheme(theme, () =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "page-layout-test", DisplayName = "page-layout-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, credentials, utcOffsetMinutes: utcOffsetMinutes, isLongQueryTraceEnabled: isLongQueryTraceEnabled);
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = Open(tab, width, height);
                try
                {
                    return body(tab, window, duckDb);
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

    internal static T WithTheme<T>(string theme, Func<T> body)
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

    internal static Window Open(FrameworkElement content, int width, int height)
    {
        var window = new Window { Content = content, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
        StaTestThread.ShowOffScreen(window);
        Settle(window);
        return window;
    }

    internal static void Resize(Window window, int width, int height)
    {
        window.Width = width;
        window.Height = height;
        Settle(window);
    }

    internal static void Settle(Window window)
    {
        window.UpdateLayout();
        Dispatcher.CurrentDispatcher.Invoke(DispatcherPriority.Loaded, () => { });
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
    }

    internal static T Find<T>(FrameworkElement root, string name) where T : FrameworkElement =>
        root.FindName(name) as T ?? throw new Xunit.Sdk.XunitException($"{name} is not on the tab");

    /// <summary>Selects every tab on the way up to <paramref name="element"/>, so it is laid out and drawn.</summary>
    internal static void Reveal(FrameworkElement element)
    {
        for (DependencyObject? at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
            {
                owner.SelectedItem = item;
            }
        }
    }

    internal static Rect Bounds(FrameworkElement element, FrameworkElement ancestor) =>
        element.TransformToAncestor(ancestor).TransformBounds(new Rect(element.RenderSize));

    /// <summary>The scroll viewer a page scrolls with: the nearest one above <paramref name="element"/> in the visual tree.</summary>
    internal static ScrollViewer PageOf(DependencyObject element)
    {
        for (var up = VisualTreeHelper.GetParent(element); up is not null; up = VisualTreeHelper.GetParent(up))
        {
            if (up is ScrollViewer page)
            {
                return page;
            }
        }

        throw new Xunit.Sdk.XunitException("the element is not on a page that scrolls");
    }

    /// <summary>The scroll viewer inside a grid's own template: the one that scrolls its rows.</summary>
    internal static ScrollViewer InnerScroll(FrameworkElement grid) =>
        ClippedTextProbe.Descendants(grid).OfType<ScrollViewer>().FirstOrDefault() ?? throw new Xunit.Sdk.XunitException("the grid has no scroll viewer of its own");

    internal static IEnumerable<T> Visual<T>(DependencyObject root) => ClippedTextProbe.Descendants(root).OfType<T>();

    internal static object? Invoke(object target, string method, params object?[] args) =>
        (target.GetType().GetMethod(method, Instance) ?? throw new Xunit.Sdk.XunitException($"{target.GetType().Name} has no {method}")).Invoke(target, args);

    /// <summary>Pumps the dispatcher until <paramref name="task"/> completes, so a refresh that hops to a worker and back finishes on the STA thread.</summary>
    internal static void Pump(Task task)
    {
        var frame = new DispatcherFrame();
        task.ContinueWith(_ => frame.Continue = false, TaskScheduler.Default);
        Dispatcher.PushFrame(frame);
        task.GetAwaiter().GetResult();
    }
}
