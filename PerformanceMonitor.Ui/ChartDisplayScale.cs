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
using ScottPlot.WPF;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #6678: at 250% Windows scaling the chart text was about 40% of its size. ScottPlot.WPF paints into a device-pixel
    /// canvas and takes every font size as device pixels, and nothing set <see cref="ScottPlot.Plot.ScaleFactor"/> from the
    /// window's DPI. (The control's own <c>DisplayScale</c> is read once, in its constructor, before it has a window, and
    /// ScottPlot never feeds it into the render.) <see cref="Apply(WpfPlot, double)"/> is the rule: the plot's scale is the
    /// control's DPI scale, so a 13 point tick label is the same size next to the window's own text at every scaling.
    /// <see cref="Register"/> applies it to every chart in the process when the chart is first laid out, and again when its
    /// window moves to a monitor with another DPI.
    /// </summary>
    /// <remarks>
    /// Hit-testing keeps working because every caller already hands <c>Plot.GetCoordinates</c> a DEVICE pixel (the mouse
    /// position times <c>GetDpi</c>), and <c>GetCoordinates</c> divides it by <c>ScaleFactor</c> itself. A caller that
    /// compares a pixel with the plot's LAYOUT rectangles (the legend panel) must divide by the scale first: layout is in
    /// scaled units, see <see cref="ChartHoverHelper"/>.
    /// </remarks>
    public static class ChartDisplayScale
    {
        private static bool s_registered;

        /// <summary>The scale to draw at for a DPI scale: itself, or 1 when the DPI is unknown (zero, negative, NaN).</summary>
        public static float ScaleFor(double dpiScale) =>
            dpiScale > 0 && double.IsFinite(dpiScale) ? (float)dpiScale : 1f;

        /// <summary>
        /// Makes the chart's plot draw at <paramref name="dpiScale"/>. Returns true when the plot's scale changed, so the
        /// caller knows to refresh. Safe to call again after <c>WpfPlot.Reset()</c>, which swaps in a plot at scale 1.
        /// </summary>
        public static bool Apply(WpfPlot chart, double dpiScale)
        {
            var scale = ScaleFor(dpiScale);
            chart.DisplayScale = scale;
            if (Math.Abs(chart.Plot.ScaleFactor - scale) < 0.0001) return false;
            chart.Plot.ScaleFactor = scale;
            return true;
        }

        /// <summary>Applies the DPI scale of the chart's own window; for a chart that is already in the visual tree.</summary>
        public static bool Apply(WpfPlot chart) => Apply(chart, VisualTreeHelper.GetDpi(chart).DpiScaleX);

        /// <summary>
        /// Registers the handlers once for the process. Call it from the application's startup. SizeChanged, not Loaded, for
        /// the reason <see cref="AccessibleNames.Register"/> gives: WPF raises Loaded straight to an instance's own handlers.
        /// </summary>
        public static void Register()
        {
            if (s_registered) return;
            s_registered = true;
            EventManager.RegisterClassHandler(typeof(WpfPlot), FrameworkElement.SizeChangedEvent, new RoutedEventHandler(OnSized));
        }

        /// <summary>The charts of one window, held weakly, so a DPI change reaches the ones on tabs that are not showing.</summary>
        private sealed class WindowCharts
        {
            public readonly List<WeakReference<WpfPlot>> Charts = [];
        }

        private static readonly ConditionalWeakTable<Window, WindowCharts> s_windows = new();

        private static void OnSized(object sender, RoutedEventArgs e)
        {
            if (sender is not WpfPlot chart) return;
            if (Apply(chart)) chart.Refresh();
            Track(chart);
        }

        private static void Track(WpfPlot chart)
        {
            var window = Window.GetWindow(chart);
            if (window is null) return;

            if (!s_windows.TryGetValue(window, out var known))
            {
                known = new WindowCharts();
                s_windows.Add(window, known);
                window.DpiChanged += (_, args) => OnDpiChanged(known, args.NewDpi.DpiScaleX);
            }

            known.Charts.RemoveAll(w => !w.TryGetTarget(out _));
            foreach (var w in known.Charts)
            {
                if (w.TryGetTarget(out var seen) && ReferenceEquals(seen, chart)) return;
            }

            known.Charts.Add(new WeakReference<WpfPlot>(chart));
        }

        private static void OnDpiChanged(WindowCharts known, double dpiScale)
        {
            foreach (var w in known.Charts.ToArray())
            {
                if (w.TryGetTarget(out var chart) && Apply(chart, dpiScale)) chart.Refresh();
            }
        }
    }
}
