/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using ScottPlot;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #7386: where the X axis of a time chart starts. Charts pin the axis to the picked range, so a server with three hours of
    /// history under "Past day" drew a day with its lines in the right eighth. The axis starts at the LATER of the picked start and
    /// the server's data start, the first time the server was collected at all. The data start is a server-level fact, never the
    /// first point of a series: a wait that first appeared an hour ago on a server collected all day still shows the quiet day
    /// before it, and charts stacked on one page keep one ruler because they all get the same start.
    /// </summary>
    public static class ChartAxisStart
    {
        /// <summary>The shortest axis the data start may produce: two points of the slowest (15 minute) collector cadence. A data start
        /// nearer the end than this keeps the picked start, so data that began a minute ago does not give a chart one minute wide.</summary>
        public static readonly TimeSpan MinimumSpan = TimeSpan.FromMinutes(30);

        /// <summary>A small run-in before the first collection (one fiftieth of the span shown), so the first point is not drawn on the
        /// edge of the plot with half its marker cut off.</summary>
        private const int RunInDivisor = 50;

        /// <summary>
        /// The X axis start for a chart over [<paramref name="pickedStart"/>, <paramref name="pickedEnd"/>]. The picked start when
        /// <paramref name="dataStart"/> is null (no claim), at or before the picked start, at or after the picked end (a window the
        /// server was not collected in at all stays as picked), or when the axis it would give is shorter than
        /// <see cref="MinimumSpan"/>. Otherwise the data start less its run-in, never earlier than the picked start.
        /// </summary>
        public static DateTime XStart(DateTime pickedStart, DateTime pickedEnd, DateTime? dataStart)
        {
            if (dataStart is not DateTime first || first <= pickedStart || first >= pickedEnd)
            {
                return pickedStart;
            }

            var runIn = TimeSpan.FromTicks((pickedEnd - first).Ticks / RunInDivisor);
            var start = first - runIn;
            if (start < pickedStart)
            {
                start = pickedStart;
            }

            return pickedEnd - start < MinimumSpan ? pickedStart : start;
        }

        /// <summary><see cref="XStart(DateTime, DateTime, DateTime?)"/> for a chart whose X axis is held as OLE automation dates (the
        /// charts' own unit). Returns the axis start in the same unit, the picked start untouched when the data start does not move it.</summary>
        public static double XStart(double pickedStartOa, double pickedEndOa, DateTime? dataStart)
        {
            if (dataStart is null)
            {
                return pickedStartOa;
            }

            var picked = DateTime.FromOADate(pickedStartOa);
            var start = XStart(picked, DateTime.FromOADate(pickedEndOa), dataStart);
            return start == picked ? pickedStartOa : start.ToOADate();
        }

        /// <summary>
        /// The earliest X a chart actually drew, as an instant, or null when it drew nothing with a usable time. Read from the
        /// plot's own plottables, so a series from a relation that reaches back further than the server's data start (an hourly tier,
        /// a collector with its own longer retention) is seen no matter which reader fed it.
        /// </summary>
        public static DateTime? EarliestPlotted(Plot plot)
        {
            double earliest = double.NaN;
            foreach (var plottable in plot.PlottableList)
            {
                var left = plottable.GetAxisLimits().Left;
                if (double.IsFinite(left) && (double.IsNaN(earliest) || left < earliest))
                {
                    earliest = left;
                }
            }

            /* Outside the OLE date range it is not a time (a placeholder, a categorical axis), so it claims nothing. */
            return double.IsFinite(earliest) && earliest > MinOaDate && earliest < MaxOaDate ? DateTime.FromOADate(earliest) : null;
        }

        private const double MinOaDate = -657434;
        private const double MaxOaDate = 2958465;

        /// <summary>The data start a chart may use: the server's data start, or the earliest point the charts drew when that is
        /// older (#7386). A chart never hides a point it plotted, whichever relation it read. Null stays null: with no claim from the
        /// server the axis stays as picked.</summary>
        public static DateTime? CoverPlotted(DateTime? dataStart, params Plot[] plots)
        {
            if (dataStart is not DateTime first)
            {
                return null;
            }

            foreach (var plot in plots)
            {
                if (EarliestPlotted(plot) is DateTime plotted && plotted < first)
                {
                    first = plotted;
                }
            }

            return first;
        }

        /// <summary><see cref="XStart(double, double, DateTime?)"/> for a chart already drawn: the data start is moved back to the
        /// earliest point on <paramref name="plots"/> when that is older, so the axis never starts after a plotted point. The picked
        /// start stays the floor: points before the picked window were never shown.</summary>
        public static double XStart(double pickedStartOa, double pickedEndOa, DateTime? dataStart, params Plot[] plots) =>
            XStart(pickedStartOa, pickedEndOa, CoverPlotted(dataStart, plots));

        /// <summary><see cref="XStart(double, double, DateTime?, Plot[])"/> as instants.</summary>
        public static DateTime XStart(DateTime pickedStart, DateTime pickedEnd, DateTime? dataStart, params Plot[] plots) =>
            XStart(pickedStart, pickedEnd, CoverPlotted(dataStart, plots));

        /// <summary>For a chart a shared renderer drew with an axis start chosen before it plotted: moves the left edge back to the
        /// start <see cref="XStart(double, double, DateTime?, Plot[])"/> gives now that the points are on the plot. Returns true when it
        /// moved the axis, and the caller refreshes the chart.</summary>
        public static bool WidenToPlotted(Plot plot, double pickedStartOa, double pickedEndOa, DateTime? dataStart)
        {
            var want = XStart(pickedStartOa, pickedEndOa, dataStart, plot);
            var limits = plot.Axes.GetLimits();
            if (want >= limits.Left)
            {
                return false;
            }

            plot.Axes.SetLimitsX(want, limits.Right);
            return true;
        }
    }
}
