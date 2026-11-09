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

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Shared ground for the rendered dialog tests: Lite's Dark theme merged into the application the way the app merges it, a
/// scratch config folder per test, and the visual-tree reads the asserts need. Every body runs inside <c>StaTestThread.Run</c>.
/// </summary>
internal static class DialogWalkTestHost
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml";

    internal static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri(ThemeUri) };
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

    /// <summary>A fresh config folder for a <c>ServerManager</c>; the caller deletes it.</summary>
    internal static string ScratchDir(string stem)
    {
        var dir = Path.Combine(Path.GetTempPath(), stem + "_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    internal static void Delete(string dir)
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>Shows the window off screen and lets its layout settle.</summary>
    internal static void Show(Window window)
    {
        StaTestThread.ShowOffScreen(window);
        ClippedTextProbe.Settle(window);
    }

    internal static IEnumerable<TextBlock> TextBlocks(DependencyObject root) =>
        ClippedTextProbe.Descendants(root).OfType<TextBlock>();

    /// <summary>
    /// Lays out a window's content at the window's own size without showing the window, so a dialog whose Loaded handler would
    /// reach for a server can still be measured: its Loaded never fires. The content is the window's root Grid.
    /// </summary>
    internal static FrameworkElement LayOutUnshown(Window window)
    {
        var root = (FrameworkElement)window.Content;
        var size = new Size(window.Width, window.Height);
        root.Measure(size);
        root.Arrange(new Rect(size));
        root.UpdateLayout();
        return root;
    }

    /// <summary>Every element in the logical tree under <paramref name="root"/>, which exists before any layout.</summary>
    internal static IEnumerable<DependencyObject> LogicalDescendants(DependencyObject root)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            yield return child;
            foreach (var below in LogicalDescendants(child))
            {
                yield return below;
            }
        }
    }

    /// <summary>The TextBlocks under <paramref name="root"/> that are visible and whose text starts with <paramref name="start"/>.</summary>
    internal static List<TextBlock> VisibleTextStarting(DependencyObject root, string start) =>
        TextBlocks(root)
            .Where(t => t.IsVisible && t.Text.StartsWith(start, StringComparison.Ordinal))
            .ToList();

    /// <summary>The element's top edge in the window's coordinates.</summary>
    internal static double TopIn(FrameworkElement element, Visual ancestor) =>
        element.TransformToAncestor(ancestor).Transform(new Point(0, 0)).Y;

    internal static double LeftIn(FrameworkElement element, Visual ancestor) =>
        element.TransformToAncestor(ancestor).Transform(new Point(0, 0)).X;

    internal static double RightIn(FrameworkElement element, Visual ancestor) =>
        element.TransformToAncestor(ancestor).Transform(new Point(element.ActualWidth, 0)).X;

    /// <summary>The nearest Border above the element: the card a Settings heading sits in.</summary>
    internal static Border CardOf(DependencyObject element)
    {
        var at = VisualTreeHelper.GetParent(element);
        while (at is not null && at is not Border)
        {
            at = VisualTreeHelper.GetParent(at);
        }

        return (Border)at!;
    }
}
