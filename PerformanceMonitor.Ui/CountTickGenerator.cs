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
    /// #7606: the left-axis ticks of a chart that counts things (blocked sessions, deadlocks, tasks, events). The stock automatic
    /// ticks step by 0.5 when the counts are small, so Blocked Sessions labelled "0.5" and "1.5" sessions. A count has only whole
    /// ticks, and at least one above zero whenever the axis reaches 1 (the two-tick floor of <see cref="MinimumTickGenerator"/>,
    /// kept here with whole numbers). Install it with <see cref="ChartStyle.UseCountAxis"/>.
    /// </summary>
    public sealed class CountTickGenerator : ScottPlot.ITickGenerator
    {
        private readonly ScottPlot.TickGenerators.NumericAutomatic _inner = new() { IntegerTicksOnly = true };

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
            Ticks = WholeOnly(_inner.Ticks, range.Min, range.Max);
        }

        /// <summary>
        /// <paramref name="ticks"/> with every tick at a fractional position removed, then whole numbers added at the low and high
        /// ends of <paramref name="min"/>..<paramref name="max"/> when fewer than two major ticks remain inside it (a zero-width range
        /// gets <c>min</c> and <c>min + 1</c>). An axis shorter than 1 has no whole number above zero to label, so it keeps what it has.
        /// </summary>
        public static ScottPlot.Tick[] WholeOnly(ScottPlot.Tick[] ticks, double min, double max)
        {
            var kept = ticks.Where(t => IsWhole(t.Position)).ToList();
            if (double.IsNaN(min) || double.IsNaN(max) || double.IsInfinity(min) || double.IsInfinity(max))
                return kept.ToArray();

            var lo = Math.Min(min, max);
            var hi = Math.Max(min, max);
            var inside = kept.Where(t => t.IsMajor && t.Position >= lo && t.Position <= hi).ToList();
            if (inside.Count < 2)
            {
                var first = Math.Ceiling(lo - 1e-9) + 0.0; // + 0.0: Ceiling(-1e-9) is -0, which prints "-0"
                var last = hi <= lo ? first + 1 : Math.Floor(hi + 1e-9);
                foreach (var position in new[] { first, last })
                {
                    if (position < lo - 1e-9 || position > Math.Max(hi, lo + 1) + 1e-9) continue;
                    if (kept.Any(t => t.IsMajor && Math.Abs(t.Position - position) < 1e-9)) continue;
                    kept.Add(ScottPlot.Tick.Major(position, position.ToString("0", CultureInfo.InvariantCulture)));
                }
            }

            return kept.OrderBy(t => t.Position).ToArray();
        }

        private static bool IsWhole(double position) => Math.Abs(position - Math.Round(position)) < 1e-9;
    }
}
