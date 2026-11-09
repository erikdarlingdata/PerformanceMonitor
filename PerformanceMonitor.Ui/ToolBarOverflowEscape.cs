/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Closes a toolbar's "..." overflow on Escape however it was opened. A mouse click on the overflow toggle opens the popup without
/// moving keyboard focus into it, so the key goes to whatever had focus before the click and the toolbar never hears it. While the
/// overflow is open this watches the toolbar's window for Escape, closes the overflow and marks the key handled, so the key does
/// not also act on the window. Set by the shared toolbar style, so both apps' server toolbars get it.
/// </summary>
public static class ToolBarOverflowEscape
{
    public static readonly DependencyProperty EnabledProperty =
        DependencyProperty.RegisterAttached(
            "Enabled",
            typeof(bool),
            typeof(ToolBarOverflowEscape),
            new PropertyMetadata(false, OnEnabledChanged));

    public static bool GetEnabled(DependencyObject obj) => (bool)obj.GetValue(EnabledProperty);
    public static void SetEnabled(DependencyObject obj, bool value) => obj.SetValue(EnabledProperty, value);

    /// <summary>The window watched while the overflow is open, and the handler watching it, so the close detaches exactly what the open attached.</summary>
    private static readonly DependencyProperty WatchProperty =
        DependencyProperty.RegisterAttached("Watch", typeof(Watch), typeof(ToolBarOverflowEscape));

    private sealed record Watch(Window Window, KeyEventHandler Handler);

    /// <summary>The name of the overflow popup in the shared toolbar template.</summary>
    internal const string OverflowPopupPartName = "OverflowPopup";

    private static void OnEnabledChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is not ToolBar toolBar)
        {
            return;
        }

        /* Idempotent: detach first so re-applying the style does not double-subscribe. */
        toolBar.Loaded -= OnLoaded;
        if ((bool)e.NewValue)
        {
            toolBar.Loaded += OnLoaded;
            if (toolBar.IsLoaded)
            {
                HookPopup(toolBar);
            }
        }
    }

    private static void OnLoaded(object sender, RoutedEventArgs e) => HookPopup((ToolBar)sender);

    private static void HookPopup(ToolBar toolBar)
    {
        toolBar.ApplyTemplate();
        if (toolBar.Template?.FindName(OverflowPopupPartName, toolBar) is not Popup popup)
        {
            return;
        }

        popup.Opened -= OnOverflowOpened;
        popup.Opened += OnOverflowOpened;
        popup.Closed -= OnOverflowClosed;
        popup.Closed += OnOverflowClosed;
    }

    private static void OnOverflowOpened(object? sender, EventArgs e)
    {
        if (sender is not Popup { TemplatedParent: ToolBar toolBar } || Window.GetWindow(toolBar) is not Window window)
        {
            return;
        }

        OnOverflowClosed(sender, e);
        var overflowPopup = (Popup)sender;
        KeyEventHandler handler = (_, key) =>
        {
            if (key.Key == Key.Escape && toolBar.IsOverflowOpen && !InsideANestedPopup(key.OriginalSource as DependencyObject, overflowPopup))
            {
                toolBar.IsOverflowOpen = false;
                key.Handled = true;
            }
        };
        /* The window sees the key first wherever focus sits: on the toggle, in the popup, or on the control that had it before the click. */
        window.PreviewKeyDown += handler;
        toolBar.SetValue(WatchProperty, new Watch(window, handler));
    }

    /// <summary>
    /// True when the key came from inside something open within the overflow: a combo whose list is down, or any other popup. That
    /// control takes the Escape for itself, and the overflow stays until the next one. The walk goes from the key's source up to the
    /// overflow's own popup, through the visual tree and, where a popup's content has no visual parent, its logical parent.
    /// </summary>
    private static bool InsideANestedPopup(DependencyObject? source, Popup overflowPopup)
    {
        for (var node = source; node is not null; node = Parent(node))
        {
            if (ReferenceEquals(node, overflowPopup) || node is ToolBar)
            {
                return false;
            }

            if (node is ComboBox { IsDropDownOpen: true } || node is Popup { IsOpen: true })
            {
                return true;
            }
        }

        return false;
    }

    private static DependencyObject? Parent(DependencyObject node) =>
        (node is System.Windows.Media.Visual or System.Windows.Media.Media3D.Visual3D ? System.Windows.Media.VisualTreeHelper.GetParent(node) : null)
        ?? LogicalTreeHelper.GetParent(node);

    private static void OnOverflowClosed(object? sender, EventArgs e)
    {
        if (sender is not Popup { TemplatedParent: ToolBar toolBar } || toolBar.GetValue(WatchProperty) is not Watch watch)
        {
            return;
        }

        watch.Window.PreviewKeyDown -= watch.Handler;
        toolBar.ClearValue(WatchProperty);
    }
}
