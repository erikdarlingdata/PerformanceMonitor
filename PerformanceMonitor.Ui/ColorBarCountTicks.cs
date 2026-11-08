/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #8224: the ticks of a colour bar that labels counts on a log1p ramp: the round values present in the data and the data's maximum.
    /// The maximum is always labelled. A round value is dropped when, at the bar's length at the moment of each render, its label would
    /// sit within one label height of the maximum's or of the last label kept below it, so "200" is not drawn under "252" and "1" and
    /// "2" do not merge on a short bar. A fixed list of ticks drew those pairs over each other at the app's windows.
    /// </summary>
    public sealed class ColorBarCountTicks : ScottPlot.ITickGenerator
    {
        /// <summary>Pixels of clear space kept between two neighbouring labels along the bar.</summary>
        public const double GapPixels = 2;

        private readonly IReadOnlyList<(double Position, string Label)> _roundValues;
        private readonly (double Position, string Label) _maximum;

        /// <param name="roundValues">The round values in the ramp's units with their labels, ascending.</param>
        /// <param name="maximum">The data's maximum in the ramp's units with its label.</param>
        public ColorBarCountTicks(IReadOnlyList<(double Position, string Label)> roundValues, (double Position, string Label) maximum)
        {
            _roundValues = roundValues;
            _maximum = maximum;
        }

        public ScottPlot.Tick[] Ticks { get; private set; } = Array.Empty<ScottPlot.Tick>();

        public int MaxTickCount { get; set; } = int.MaxValue;

        public void Regenerate(
            ScottPlot.CoordinateRange range, ScottPlot.Edge edge, ScottPlot.PixelLength size,
            ScottPlot.Paint paint, ScottPlot.LabelStyle labelStyle)
        {
            var pixelsPerUnit = range.Span > 0 && size.Length > 0 ? size.Length / range.Span : double.PositiveInfinity;
            var room = labelStyle.Measure(_maximum.Label, paint).Height + GapPixels;
            Ticks = Keep(_roundValues, _maximum, pixelsPerUnit, room)
                .Select(t => ScottPlot.Tick.Major(t.Position, t.Label))
                .ToArray();
        }

        /// <summary>The ticks drawn: every round value at least <paramref name="roomPixels"/> from the maximum and from the last round value
        /// kept below it, then the maximum. With no pixel scale yet, every round value is kept.</summary>
        public static List<(double Position, string Label)> Keep(
            IReadOnlyList<(double Position, string Label)> roundValues, (double Position, string Label) maximum,
            double pixelsPerUnit, double roomPixels)
        {
            var kept = new List<(double Position, string Label)>();
            foreach (var value in roundValues.OrderBy(v => v.Position))
            {
                if (double.IsFinite(pixelsPerUnit))
                {
                    if (Math.Abs(maximum.Position - value.Position) * pixelsPerUnit < roomPixels) continue;
                    if (kept.Count > 0 && (value.Position - kept[^1].Position) * pixelsPerUnit < roomPixels) continue;
                }

                kept.Add(value);
            }

            kept.Add(maximum);
            return kept;
        }
    }
}
