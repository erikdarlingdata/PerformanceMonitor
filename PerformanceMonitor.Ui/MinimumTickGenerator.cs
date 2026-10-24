/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #6652: the automatic numeric ticks, but never fewer than two labelled ones. On a short plot, or one whose
    /// data is all zero (axis 0..1), the stock generator can place only "0", which leaves the reader no magnitude.
    /// </summary>
    public sealed class MinimumTickGenerator : ScottPlot.ITickGenerator
    {
        private readonly ScottPlot.TickGenerators.NumericAutomatic _inner = new();

        public ScottPlot.Tick[] Ticks { get; private set; } = Array.Empty<ScottPlot.Tick>();

        public int MaxTickCount
        {
            get => _inner.MaxTickCount;
            set => _inner.MaxTickCount = value;
        }

        public void Regenerate(
            ScottPlot.CoordinateRange range, ScottPlot.Edge edge, ScottPlot.PixelLength size,
            ScottPlot.Paint paint, ScottPlot.LabelStyle labelStyle)
        {
            _inner.Regenerate(range, edge, size, paint, labelStyle);
            Ticks = EnsureMinimum(_inner.Ticks, range.Min, range.Max);
        }

        /// <summary>
        /// <paramref name="ticks"/> with the axis' low and high ends added when fewer than two major ticks fall
        /// inside <paramref name="min"/>..<paramref name="max"/>. A zero-width range gets <c>min</c> and <c>min + 1</c>.
        /// </summary>
        public static ScottPlot.Tick[] EnsureMinimum(ScottPlot.Tick[] ticks, double min, double max)
        {
            if (double.IsNaN(min) || double.IsNaN(max) || double.IsInfinity(min) || double.IsInfinity(max))
                return ticks;

            var lo = Math.Min(min, max);
            var hi = Math.Max(min, max);
            var inside = ticks.Where(t => t.IsMajor && t.Position >= lo && t.Position <= hi).ToList();
            if (inside.Count >= 2) return ticks;

            if (hi <= lo) hi = lo + 1;
            var result = ticks.ToList();
            foreach (var position in new[] { lo, hi })
            {
                if (inside.Any(t => Math.Abs(t.Position - position) < (hi - lo) * 1e-9)) continue;
                result.Add(ScottPlot.Tick.Major(position, position.ToString("0.##", CultureInfo.InvariantCulture)));
            }

            return result.OrderBy(t => t.Position).ToArray();
        }
    }
}
