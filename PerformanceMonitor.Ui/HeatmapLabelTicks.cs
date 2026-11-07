/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #8220: the labels of a heatmap axis whose cells are numbered 0..count-1 and named by <c>labelOf</c> (a time bucket, a duration range).
    /// It labels every cell when the labels fit and every n-th cell when they do not, with n taken from the axis length at the moment of
    /// each render, so a resize or a longer range thins the labels instead of packing them against each other. A fixed count of
    /// labels per axis ("about 12") touched at the app's minimum window.
    /// </summary>
    public sealed class HeatmapLabelTicks : ScottPlot.ITickGenerator
    {
        /// <summary>Pixels of clear space kept between two neighbouring labels of the X axis (the Y axis keeps <see cref="StackedGapPixels"/>).</summary>
        public const double GapPixels = 12;

        /// <summary>Pixels of clear space kept between two stacked labels of the Y axis.</summary>
        public const double StackedGapPixels = 4;

        private readonly int _count;
        private readonly Func<int, string> _labelOf;
        private readonly bool _horizontal;

        /// <param name="count">How many cells the axis holds.</param>
        /// <param name="labelOf">The label of a cell.</param>
        /// <param name="horizontal">True for the X axis (labels side by side), false for the Y axis (labels stacked).</param>
        public HeatmapLabelTicks(int count, Func<int, string> labelOf, bool horizontal)
        {
            _count = count;
            _labelOf = labelOf;
            _horizontal = horizontal;
        }

        public ScottPlot.Tick[] Ticks { get; private set; } = Array.Empty<ScottPlot.Tick>();

        public int MaxTickCount { get; set; } = int.MaxValue;

        public void Regenerate(
            ScottPlot.CoordinateRange range, ScottPlot.Edge edge, ScottPlot.PixelLength size,
            ScottPlot.Paint paint, ScottPlot.LabelStyle labelStyle)
        {
            if (_count <= 0)
            {
                Ticks = Array.Empty<ScottPlot.Tick>();
                return;
            }

            var span = range.Span;
            var pixelsPerCell = span > 0 && size.Length > 0 ? size.Length / span : double.PositiveInfinity;
            var stride = StrideFor(WidestLabel(labelStyle, paint), pixelsPerCell, _horizontal ? GapPixels : StackedGapPixels);
            var ticks = new System.Collections.Generic.List<ScottPlot.Tick>();
            for (var i = 0; i < _count; i += stride)
            {
                ticks.Add(ScottPlot.Tick.Major(i, _labelOf(i)));
            }

            Ticks = ticks.ToArray();
        }

        /// <summary>Every n-th cell: the smallest n for which labels <paramref name="labelPixels"/> long keep <paramref name="gapPixels"/> clear
        /// when each cell is <paramref name="pixelsPerCell"/> long. 1 when they fit as they are.</summary>
        public static int StrideFor(double labelPixels, double pixelsPerCell, double gapPixels)
        {
            if (!double.IsFinite(pixelsPerCell) || pixelsPerCell <= 0)
            {
                return 1;
            }

            return Math.Max(1, (int)Math.Ceiling((labelPixels + gapPixels) / pixelsPerCell));
        }

        /// <summary>The longest label's length along the axis, read from the first, middle and last cells and every cell when there are few.</summary>
        private double WidestLabel(ScottPlot.LabelStyle style, ScottPlot.Paint paint)
        {
            var widest = 0.0;
            var step = Math.Max(1, _count / 16);
            for (var i = 0; i < _count; i += step)
            {
                widest = Math.Max(widest, Measure(style, paint, _labelOf(i)));
            }

            return Math.Max(widest, Measure(style, paint, _labelOf(_count - 1)));
        }

        private double Measure(ScottPlot.LabelStyle style, ScottPlot.Paint paint, string label)
        {
            var size = style.Measure(label, paint);
            return _horizontal ? size.Width : size.Height;
        }
    }
}
