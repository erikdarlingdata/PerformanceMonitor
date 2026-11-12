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
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #7385: gives its child a share of the visible height, with a floor, on a tab that scrolls. A star row cannot: a
/// <see cref="ScrollViewer"/> measures its content with infinite height, so a star row takes whatever its content asks for, and
/// a grid would ask for every row it has. The box reads the enclosing viewer's viewport instead, and measures again when it changes.
/// </summary>
/// <remarks>
/// The floor is the element's own <see cref="FrameworkElement.MinHeight"/>. Where the box is not inside a viewer, or the viewer
/// has not laid out yet, the floor is the height. Used for the PostgreSQL Waits and I/O tabs, whose grids showed fewer than two
/// rows once the trend charts below them took their share.
/// </remarks>
public sealed class ViewportBox : Decorator
{
    public static readonly DependencyProperty FractionProperty = DependencyProperty.Register(
        nameof(Fraction), typeof(double), typeof(ViewportBox),
        new FrameworkPropertyMetadata(0.3, FrameworkPropertyMetadataOptions.AffectsMeasure));

    /// <summary>The share of the visible height the child gets, 0 to 1.</summary>
    public double Fraction { get => (double)GetValue(FractionProperty); set => SetValue(FractionProperty, value); }

    private ScrollViewer? _viewer;

    public ViewportBox() => Unloaded += (_, _) => Subscribe(null);

    /// <summary>The height for a box: its share of the visible height, never under the floor.</summary>
    internal static double HeightFor(double visibleHeight, double fraction, double floor) =>
        double.IsFinite(visibleHeight) && visibleHeight > 0 ? Math.Max(floor, Math.Floor(visibleHeight * Math.Clamp(fraction, 0, 1))) : floor;

    protected override Size MeasureOverride(Size constraint)
    {
        if (_viewer is null) Subscribe(FindViewer(this));
        var visible = double.IsFinite(constraint.Height)
            ? constraint.Height
            : _viewer is null ? double.NaN : _viewer.ViewportHeight > 0 ? _viewer.ViewportHeight : _viewer.ActualHeight;
        var height = HeightFor(visible, Fraction, MinHeight);
        var width = double.IsFinite(constraint.Width) ? constraint.Width : (_viewer is { ViewportWidth: > 0 } ? _viewer.ViewportWidth : 0);
        Child?.Measure(new Size(width, height));
        return new Size(width, height);
    }

    protected override Size ArrangeOverride(Size arrangeSize)
    {
        Child?.Arrange(new Rect(0, 0, arrangeSize.Width, arrangeSize.Height));
        return arrangeSize;
    }

    internal static ScrollViewer? FindViewer(DependencyObject start)
    {
        for (var node = VisualTreeHelper.GetParent(start); node is not null; node = VisualTreeHelper.GetParent(node))
        {
            if (node is ScrollViewer sv && sv.VerticalScrollBarVisibility != ScrollBarVisibility.Disabled) return sv;
        }
        return null;
    }

    private void Subscribe(ScrollViewer? viewer)
    {
        if (ReferenceEquals(viewer, _viewer)) return;
        if (_viewer is not null) _viewer.ScrollChanged -= OnScrollChanged;
        _viewer = viewer;
        if (_viewer is not null) _viewer.ScrollChanged += OnScrollChanged;
    }

    private void OnScrollChanged(object sender, ScrollChangedEventArgs e)
    {
        if (e.ViewportHeightChange != 0 || e.ViewportWidthChange != 0) InvalidateMeasure();
    }
}
