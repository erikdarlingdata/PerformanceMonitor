/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Media;
using System.Windows.Threading;
using ScottPlot;
using ScottPlot.WPF;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #7385: the WPF half of <see cref="ChartGroupAligner"/>. A group is a <see cref="ChartGridPanel"/>, or any element marked
/// <see cref="ChartGroup.IsAlignedProperty"/> in XAML; its charts are the WpfPlot controls beneath it.
/// </summary>
/// <remarks>
/// <para>
/// Nothing here renders on its own schedule. Each chart reports a finished render (<see cref="Attach"/>, wired in
/// <see cref="ChartStyle.ApplyThemeToChart"/>), a debounce collects the burst a data load or a resize causes, and one
/// check then reads what the charts already rendered. Only when their frames differ, or the group's size changed (floors from a
/// wider layout would otherwise stay: a caption that wrapped at one width keeps its second line), does it measure
/// again, one headless render per chart, and refresh the charts whose frame changed. A tab switch re-renders no
/// one and so measures nothing (#7256).
/// </para>
/// </remarks>
internal static partial class ChartGroupAligner
{
    /// <summary>How long after the last finished render a group waits before it checks its frames.</summary>
    internal static readonly TimeSpan Debounce = TimeSpan.FromMilliseconds(150);

    /// <summary>Alignment passes allowed in a row without the group settling, so a layout that will not converge cannot spin.</summary>
    private const int MaxPasses = 3;

    private static readonly TimeSpan PassWindow = TimeSpan.FromSeconds(5);

    private sealed class GroupState
    {
        public DispatcherTimer? Timer;
        public string Signature = string.Empty;
        public int Passes;
        public DateTime LastPassUtc;
    }

    private static readonly ConditionalWeakTable<Plot, object> s_attached = new();
    private static readonly ConditionalWeakTable<Plot, object> s_empty = new();

    /// <summary>
    /// #8086: marks a plot that shows only its empty-state sentence. It has no axes and so no plot rectangle to share; it is left
    /// out of the group, which would otherwise floor edges that are not drawn and never see its frame match. A chart whose data
    /// comes back is reset by <c>ClearChart</c>, which makes a new plot and so drops the mark.
    /// </summary>
    internal static void MarkEmpty(Plot plot) => s_empty.AddOrUpdate(plot, new object());

    internal static bool IsEmptyState(Plot plot) => s_empty.TryGetValue(plot, out _);
    private static readonly ConditionalWeakTable<FrameworkElement, GroupState> s_groups = new();
    private static bool s_aligning;

    /// <summary>Reports the chart's finished renders to its group. Safe to call more than once per plot.</summary>
    public static void Attach(WpfPlot chart)
    {
        var plot = chart.Plot;
        if (s_attached.TryGetValue(plot, out _)) return;
        s_attached.Add(plot, new object());
        plot.RenderManager.RenderFinished += (_, _) =>
        {
            if (s_aligning) return;
            var root = FindGroup(chart);
            if (root is not null) Schedule(root);
        };
    }

    /// <summary>The element that groups this chart with its neighbors, or null for a chart that stands alone.</summary>
    internal static FrameworkElement? FindGroup(DependencyObject chart)
    {
        for (var node = VisualTreeHelper.GetParent(chart); node is not null; node = VisualTreeHelper.GetParent(node))
        {
            if (node is FrameworkElement fe && (fe is ChartGridPanel || ChartGroup.GetIsAligned(fe))) return fe;
        }
        return null;
    }

    private static void Schedule(FrameworkElement root)
    {
        var state = s_groups.GetOrCreateValue(root);
        if (state.Timer is null)
        {
            state.Timer = new DispatcherTimer(DispatcherPriority.Background, root.Dispatcher) { Interval = Debounce };
            state.Timer.Tick += (_, _) =>
            {
                state.Timer!.Stop();
                Check(root);
            };
        }
        state.Timer.Stop();
        state.Timer.Start();
    }

    private static void CollectCharts(DependencyObject node, List<WpfPlot> into)
    {
        var count = VisualTreeHelper.GetChildrenCount(node);
        for (var i = 0; i < count; i++)
        {
            var child = VisualTreeHelper.GetChild(node, i);
            if (child is WpfPlot chart)
            {
                if (IsShown(chart) && !IsEmptyState(chart.Plot) && chart.ActualWidth >= 1 && chart.ActualHeight >= 1 && chart.Plot.RenderManager.RenderCount > 0)
                    into.Add(chart);
            }
            else
            {
                CollectCharts(child, into);
            }
        }
    }

    /// <summary>
    /// Reads the group's charts and aligns them if their frames differ or their size changed. Called by the debounce;
    /// internal so a test can run it without waiting. Returns true when it measured.
    /// </summary>
    internal static bool Check(FrameworkElement root)
    {
        var members = new List<WpfPlot>();
        CollectCharts(root, members);
        if (root is ChartGridPanel grid) FloorPanelHeight(grid, members);
        if (members.Count < 2) return false;

        var state = s_groups.GetOrCreateValue(root);
        var signature = new System.Text.StringBuilder();
        var frames = new Frame[members.Count];
        var widest = default(Frame);
        for (var i = 0; i < members.Count; i++)
        {
            signature.Append(PixelWidth(members[i])).Append('x').Append(PixelHeight(members[i])).Append(';');
            frames[i] = LastFrame(members[i].Plot);
            widest = i == 0 ? frames[i] : widest.Max(frames[i]);
        }

        var aligned = true;
        foreach (var f in frames) aligned &= f.Matches(widest, 1f);
        var resized = state.Signature != signature.ToString();
        if (aligned && !resized)
        {
            state.Passes = 0;
            return false;
        }

        var now = DateTime.UtcNow;
        if (now - state.LastPassUtc > PassWindow) state.Passes = 0;
        if (state.Passes >= MaxPasses) return false;
        state.Passes++;
        state.LastPassUtc = now;
        state.Signature = signature.ToString();

        var inputs = new List<(Plot, int, int)>(members.Count);
        foreach (var m in members) inputs.Add((m.Plot, PixelWidth(m), PixelHeight(m)));

        Frame? target;
        s_aligning = true;
        try { target = Align(inputs); }
        finally { s_aligning = false; }
        if (target is not { } shared) return true;

        for (var i = 0; i < members.Count; i++)
        {
            if (!frames[i].Matches(shared, 0.5f)) members[i].Refresh();
        }
        return true;
    }

    /// <summary>A block never gets shorter than its tallest chart's chrome plus the minimum plot (the stacked tabs do this per row).</summary>
    private static void FloorPanelHeight(ChartGridPanel panel, List<WpfPlot> members)
    {
        var floor = 0.0;
        foreach (var m in members)
        {
            if (ChartLayout.RowMinHeightFor(m.Plot) is { } min) floor = Math.Max(floor, Math.Ceiling(min));
        }
        if (Math.Abs(panel.ContentMinHeight - floor) > 0.5) panel.ContentMinHeight = floor;
    }

    // IsVisible is false for a control that is laid out but not in a window (a headless test), where Visibility still says.
    private static bool IsShown(WpfPlot chart) =>
        chart.IsVisible || (PresentationSource.FromVisual(chart) is null && chart.Visibility == Visibility.Visible);

    // The control renders at its size in device pixels (the same factor ChartDisplayScale sets).
    private static int PixelWidth(WpfPlot chart) => (int)(chart.ActualWidth * chart.DisplayScale);

    private static int PixelHeight(WpfPlot chart) => (int)(chart.ActualHeight * chart.DisplayScale);
}
