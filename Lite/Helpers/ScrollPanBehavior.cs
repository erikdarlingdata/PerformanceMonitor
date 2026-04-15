/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using System.Windows.Media;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Enables middle-mouse drag panning for scrollable controls such as DataGrid and ListView.
/// </summary>
public static class ScrollPanBehavior
{
    public static readonly DependencyProperty EnableMiddleClickPanningProperty =
        DependencyProperty.RegisterAttached(
            "EnableMiddleClickPanning",
            typeof(bool),
            typeof(ScrollPanBehavior),
            new PropertyMetadata(false, OnEnableMiddleClickPanningChanged));

    private static readonly DependencyProperty PanStateProperty =
        DependencyProperty.RegisterAttached(
            "PanState",
            typeof(PanState),
            typeof(ScrollPanBehavior),
            new PropertyMetadata(null));

    public static bool GetEnableMiddleClickPanning(DependencyObject obj) => (bool)obj.GetValue(EnableMiddleClickPanningProperty);
    public static void SetEnableMiddleClickPanning(DependencyObject obj, bool value) => obj.SetValue(EnableMiddleClickPanningProperty, value);

    private static PanState GetOrCreatePanState(FrameworkElement element)
    {
        if (element.GetValue(PanStateProperty) is not PanState state)
        {
            state = new PanState();
            element.SetValue(PanStateProperty, state);
        }

        return state;
    }

    private static void OnEnableMiddleClickPanningChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is not FrameworkElement element)
        {
            return;
        }

        var isEnabled = (bool)e.NewValue;

        if (isEnabled)
        {
            element.Loaded += OnLoaded;
            element.Unloaded += OnUnloaded;
            element.PreviewMouseDown += OnPreviewMouseDown;
            element.PreviewMouseMove += OnPreviewMouseMove;
            element.PreviewMouseUp += OnPreviewMouseUp;
            element.LostMouseCapture += OnLostMouseCapture;
        }
        else
        {
            element.Loaded -= OnLoaded;
            element.Unloaded -= OnUnloaded;
            element.PreviewMouseDown -= OnPreviewMouseDown;
            element.PreviewMouseMove -= OnPreviewMouseMove;
            element.PreviewMouseUp -= OnPreviewMouseUp;
            element.LostMouseCapture -= OnLostMouseCapture;
            StopPanning(element, restoreCursor: true);
        }
    }

    private static void OnLoaded(object sender, RoutedEventArgs e)
    {
        if (sender is FrameworkElement element)
        {
            GetOrCreatePanState(element).ScrollViewer = FindVisualChild<ScrollViewer>(element);
        }
    }

    private static void OnUnloaded(object sender, RoutedEventArgs e)
    {
        if (sender is FrameworkElement element)
        {
            StopPanning(element, restoreCursor: false);
            GetOrCreatePanState(element).ScrollViewer = null;
        }
    }

    private static void OnPreviewMouseDown(object sender, MouseButtonEventArgs e)
    {
        if (e.ChangedButton != MouseButton.Middle
            || sender is not FrameworkElement element
            || !CanStartPanning(e.OriginalSource as DependencyObject))
        {
            return;
        }

        var state = GetOrCreatePanState(element);
        state.ScrollViewer ??= FindVisualChild<ScrollViewer>(element);

        if (state.ScrollViewer is null)
        {
            return;
        }

        if (state.ScrollViewer.ScrollableWidth <= 0 && state.ScrollViewer.ScrollableHeight <= 0)
        {
            return;
        }

        state.IsPanning = true;
        state.StartPoint = e.GetPosition(state.ScrollViewer);
        state.StartHorizontalOffset = state.ScrollViewer.HorizontalOffset;
        state.StartVerticalOffset = state.ScrollViewer.VerticalOffset;
        state.OriginalCursor = element.Cursor;

        element.Cursor = Cursors.ScrollAll;
        element.CaptureMouse();
        e.Handled = true;
    }

    private static void OnPreviewMouseMove(object sender, MouseEventArgs e)
    {
        if (sender is not FrameworkElement element)
        {
            return;
        }

        var state = GetOrCreatePanState(element);
        if (!state.IsPanning || state.ScrollViewer is null)
        {
            return;
        }

        var currentPoint = e.GetPosition(state.ScrollViewer);
        var deltaX = currentPoint.X - state.StartPoint.X;
        var deltaY = currentPoint.Y - state.StartPoint.Y;

        state.ScrollViewer.ScrollToHorizontalOffset(ClampOffset(state.StartHorizontalOffset - deltaX, state.ScrollViewer.ScrollableWidth));
        state.ScrollViewer.ScrollToVerticalOffset(ClampOffset(state.StartVerticalOffset - deltaY, state.ScrollViewer.ScrollableHeight));
        e.Handled = true;
    }

    private static void OnPreviewMouseUp(object sender, MouseButtonEventArgs e)
    {
        if (e.ChangedButton != MouseButton.Middle || sender is not FrameworkElement element)
        {
            return;
        }

        if (!GetOrCreatePanState(element).IsPanning)
        {
            return;
        }

        StopPanning(element, restoreCursor: true);
        e.Handled = true;
    }

    private static void OnLostMouseCapture(object sender, MouseEventArgs e)
    {
        if (sender is FrameworkElement element)
        {
            StopPanning(element, restoreCursor: true);
        }
    }

    private static void StopPanning(FrameworkElement element, bool restoreCursor)
    {
        var state = GetOrCreatePanState(element);
        if (!state.IsPanning)
        {
            if (restoreCursor)
            {
                element.ClearValue(FrameworkElement.CursorProperty);
            }

            return;
        }

        state.IsPanning = false;

        if (restoreCursor)
        {
            if (state.OriginalCursor is null)
            {
                element.ClearValue(FrameworkElement.CursorProperty);
            }
            else
            {
                element.Cursor = state.OriginalCursor;
            }
        }

        state.OriginalCursor = null;

        if (Mouse.Captured == element)
        {
            element.ReleaseMouseCapture();
        }
    }

    private static bool CanStartPanning(DependencyObject? source)
    {
        while (source is not null)
        {
            if (source is ScrollBar
                || source is Thumb
                || source is DataGridColumnHeader
                || source is GridViewColumnHeader
                || source is TextBoxBase
                || source is PasswordBox
                || source is ComboBox
                || source is ComboBoxItem
                || source is ButtonBase)
            {
                return false;
            }

            source = VisualTreeHelper.GetParent(source);
        }

        return true;
    }

    private static double ClampOffset(double value, double maxValue) => Math.Max(0, Math.Min(maxValue, value));

    private static T? FindVisualChild<T>(DependencyObject? parent) where T : DependencyObject
    {
        if (parent is null)
        {
            return null;
        }

        for (int i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);
            if (child is T target)
            {
                return target;
            }

            var nested = FindVisualChild<T>(child);
            if (nested is not null)
            {
                return nested;
            }
        }

        return null;
    }

    private sealed class PanState
    {
        public bool IsPanning { get; set; }
        public Point StartPoint { get; set; }
        public double StartHorizontalOffset { get; set; }
        public double StartVerticalOffset { get; set; }
        public Cursor? OriginalCursor { get; set; }
        public ScrollViewer? ScrollViewer { get; set; }
    }
}