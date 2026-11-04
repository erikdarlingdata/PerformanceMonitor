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
/// #7385: lays a block of charts out from the space it has, not from a size someone tuned on one screen.
/// </summary>
/// <remarks>
/// <para>
/// The column count follows the width: as many as fit at <see cref="MinChartWidth"/>, at most <see cref="MaxColumns"/>.
/// The height follows the visible height: the rows share <see cref="HeightFill"/> of what the window shows, never less
/// than <see cref="MinChartHeight"/> and never taller than <see cref="MaxHeightToWidth"/> of the chart's width, so a 4K
/// window gives bigger charts and not absurdly tall ones. Below the floor the block is taller than the window and the
/// tab scrolls.
/// </para>
/// <para>
/// Inside a <see cref="ScrollViewer"/> the panel is measured with infinite height (and with infinite width when the viewer
/// scrolls sideways), so star rows cannot do this. The panel reads the enclosing viewer's viewport instead and measures
/// again when it changes. Its charts are one <see cref="ChartGroupAligner"/> group: they share one plot rectangle.
/// </para>
/// </remarks>
public sealed class ChartGridPanel : Panel
{
    public static readonly DependencyProperty MinChartWidthProperty = DependencyProperty.Register(
        nameof(MinChartWidth), typeof(double), typeof(ChartGridPanel),
        new FrameworkPropertyMetadata(380.0, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty MaxColumnsProperty = DependencyProperty.Register(
        nameof(MaxColumns), typeof(int), typeof(ChartGridPanel),
        new FrameworkPropertyMetadata(4, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty MinChartHeightProperty = DependencyProperty.Register(
        nameof(MinChartHeight), typeof(double), typeof(ChartGridPanel),
        new FrameworkPropertyMetadata(300.0, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty HeightFillProperty = DependencyProperty.Register(
        nameof(HeightFill), typeof(double), typeof(ChartGridPanel),
        new FrameworkPropertyMetadata(1.0, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty MaxHeightToWidthProperty = DependencyProperty.Register(
        nameof(MaxHeightToWidth), typeof(double), typeof(ChartGridPanel),
        new FrameworkPropertyMetadata(0.9, FrameworkPropertyMetadataOptions.AffectsMeasure));

    internal static readonly DependencyProperty ContentMinHeightProperty = DependencyProperty.Register(
        nameof(ContentMinHeight), typeof(double), typeof(ChartGridPanel),
        new FrameworkPropertyMetadata(0.0, FrameworkPropertyMetadataOptions.AffectsMeasure));

    public static readonly DependencyProperty GapProperty = DependencyProperty.Register(
        nameof(Gap), typeof(double), typeof(ChartGridPanel),
        new FrameworkPropertyMetadata(8.0, FrameworkPropertyMetadataOptions.AffectsMeasure));

    /// <summary>The narrowest a chart is allowed to be; the column count drops before a chart gets narrower.</summary>
    public double MinChartWidth { get => (double)GetValue(MinChartWidthProperty); set => SetValue(MinChartWidthProperty, value); }

    /// <summary>The most columns the block uses, however wide the window is.</summary>
    public int MaxColumns { get => (int)GetValue(MaxColumnsProperty); set => SetValue(MaxColumnsProperty, value); }

    /// <summary>The shortest a chart is allowed to be, enough for its caption, tick labels, a legend line and a plot; below it the tab scrolls.</summary>
    public double MinChartHeight { get => (double)GetValue(MinChartHeightProperty); set => SetValue(MinChartHeightProperty, value); }

    /// <summary>The share of the visible height the block's rows fill, 0 to 1.</summary>
    public double HeightFill { get => (double)GetValue(HeightFillProperty); set => SetValue(HeightFillProperty, value); }

    /// <summary>The tallest a chart gets, as a multiple of its width.</summary>
    public double MaxHeightToWidth { get => (double)GetValue(MaxHeightToWidthProperty); set => SetValue(MaxHeightToWidthProperty, value); }

    /// <summary>Space between charts, both ways.</summary>
    public double Gap { get => (double)GetValue(GapProperty); set => SetValue(GapProperty, value); }

    /// <summary>
    /// What the charts' own chrome (caption, tick labels, a legend that wrapped) asks for, so the plot keeps its minimum height
    /// whatever the legend did (<see cref="ChartLayout.RowMinHeightFor"/>). Set from <see cref="ChartGroupAligner"/>'s check.
    /// </summary>
    internal double ContentMinHeight { get => (double)GetValue(ContentMinHeightProperty); set => SetValue(ContentMinHeightProperty, value); }

    private ScrollViewer? _viewer;

    public ChartGridPanel()
    {
        Unloaded += (_, _) => Subscribe(null);
    }

    /// <summary>The column count and chart size for a block of <paramref name="count"/> charts in the given space; a negative or non-finite size means "not known".</summary>
    internal static (int Columns, int Rows, double Width, double Height) Plan(
        int count, double width, double visibleHeight, double minWidth, int maxColumns, double minHeight, double fill, double maxHeightToWidth, double gap)
    {
        if (count < 1) return (1, 0, 0, 0);
        if (!double.IsFinite(width) || width < 1) width = minWidth;

        var columns = (int)Math.Floor((width + gap) / (Math.Max(1, minWidth) + gap));
        columns = Math.Clamp(columns, 1, Math.Max(1, Math.Min(maxColumns, count)));
        var rows = (count + columns - 1) / columns;
        var chartWidth = Math.Max(1, (width - gap * (columns - 1)) / columns);

        var height = minHeight;
        if (double.IsFinite(visibleHeight) && visibleHeight > 0)
        {
            var fromWindow = (visibleHeight * Math.Clamp(fill, 0, 1) - gap * (rows - 1)) / rows;
            height = Math.Max(minHeight, Math.Min(fromWindow, chartWidth * maxHeightToWidth));
        }
        return (columns, rows, chartWidth, Math.Floor(height));
    }

    /// <summary>The column count and chart size this panel chose in its last layout.</summary>
    internal (int Columns, int Rows, double Width, double Height) Chosen { get; private set; }

    protected override Size MeasureOverride(Size availableSize)
    {
        // Found here and not on Loaded: a panel measured before its window shows (and a headless test) never sees Loaded.
        // The first measure may precede the viewer's own layout; its first ScrollChanged measures this panel again.
        if (_viewer is null) Subscribe(FindViewer());
        var viewer = _viewer;
        var width = availableSize.Width;
        if (!double.IsFinite(width)) width = viewer is { ViewportWidth: > 0 } ? viewer.ViewportWidth : double.NaN;
        var visible = availableSize.Height;
        if (!double.IsFinite(visible)) visible = ViewportHeightOf(viewer);

        var visibleChildren = 0;
        foreach (UIElement child in InternalChildren) if (child.Visibility != Visibility.Collapsed) visibleChildren++;

        var plan = Chosen = Plan(visibleChildren, width, visible, MinChartWidth, MaxColumns, Math.Max(MinChartHeight, ContentMinHeight), HeightFill, MaxHeightToWidth, Gap);
        foreach (UIElement child in InternalChildren)
            child.Measure(new Size(plan.Width, plan.Height));

        if (plan.Rows == 0) return new Size(0, 0);
        return new Size(
            plan.Columns * plan.Width + Gap * (plan.Columns - 1),
            plan.Rows * plan.Height + Gap * (plan.Rows - 1));
    }

    protected override Size ArrangeOverride(Size finalSize)
    {
        var plan = Chosen;
        var index = 0;
        foreach (UIElement child in InternalChildren)
        {
            if (child.Visibility == Visibility.Collapsed) continue;
            var row = index / plan.Columns;
            var column = index % plan.Columns;
            child.Arrange(new Rect(column * (plan.Width + Gap), row * (plan.Height + Gap), plan.Width, plan.Height));
            index++;
        }
        return finalSize;
    }

    private static double ViewportHeightOf(ScrollViewer? viewer)
    {
        if (viewer is null) return double.NaN;
        // ViewportHeight is 0 until the viewer has laid out once; its own height is the next best, and the first
        // ScrollChanged that reports the real viewport measures again.
        return viewer.ViewportHeight > 0 ? viewer.ViewportHeight : viewer.ActualHeight;
    }

    private ScrollViewer? FindViewer()
    {
        for (DependencyObject? node = this; node is not null; node = VisualTreeHelper.GetParent(node))
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
        // Only the viewport matters here. The extent changes because of this panel's own answer, and reacting to it would loop.
        if (e.ViewportHeightChange != 0 || e.ViewportWidthChange != 0) InvalidateMeasure();
    }
}
