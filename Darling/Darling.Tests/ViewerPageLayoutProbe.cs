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
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;

namespace Darling.Tests;

/// <summary>
/// The rendered-page helpers the #8307 layout tests share: the real Viewer server tab on the STA thread under a theme, shown at a stated
/// window size, and the realized facts a reader sees (bounds, the scroll viewer a grid or a page scrolls with, a template part).
/// </summary>
internal static class ViewerPageLayoutProbe
{
    internal const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    /// <summary>The width a tab has beside the sidebar of a window in MainWindow.xaml: 280 px.</summary>
    internal static int PaneWidth(int windowWidth) => windowWidth - 280;

    /// <summary>Builds the real server tab under <paramref name="theme"/>, shows it at <paramref name="width"/> x <paramref name="height"/>, and runs <paramref name="body"/>.</summary>
    internal static T WithServerTab<T>(string theme, int width, int height, Func<ViewerServerTab, Window, T> body)
    {
        return StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "page-layout-test", "page-layout-test", true, null));
            /* Switching to a tab would start that tab's load against a store that is not there. */
            typeof(ViewerServerTab).GetField("_refreshInFlight", Instance)!.SetValue(tab, true);
            typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", Instance)?.SetValue(tab, true);
            var window = Open(tab, width, height);
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

    internal static T WithTheme<T>(string theme, Func<T> body)
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

    /// <summary>Pumps the dispatcher until <paramref name="task"/> completes, so work that hops to a worker and back finishes on the STA thread.</summary>
    internal static void Pump(Task task)
    {
        var frame = new DispatcherFrame();
        task.ContinueWith(_ => frame.Continue = false, TaskScheduler.Default);
        Dispatcher.PushFrame(frame);
        task.GetAwaiter().GetResult();
    }
}
