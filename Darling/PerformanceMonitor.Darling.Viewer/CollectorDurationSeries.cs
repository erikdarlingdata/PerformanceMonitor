/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One line of the Collection Health tab's Duration Trends chart (#4966): a collector's bucket MAXIMA over the toolbar's range,
/// each at its bucket's start. <see cref="Times"/> is the X the chart plots (the store's naive-UTC instant as an OLE date, drawn in
/// the display zone) and <see cref="MaxMs"/> the Y: the longest successful run in each bucket, so a slow run still shows however wide
/// the bucket is. <see cref="AvgMs"/> and <see cref="Runs"/> carry each bucket's average and run count, index for index; the line
/// does not draw them, and the hover prints them as the popup's fourth line (<see cref="DetailsByX"/>).
/// A pure step of the chart (no WPF), so a test drives it with the rows the read returned.
/// </summary>
internal sealed record CollectorDurationSeries(string Collector, double[] Times, double[] MaxMs, double[] AvgMs, long[] Runs)
{
    /// <summary>
    /// The chart's lines from the buckets <see cref="ViewerDataService.GetCollectorDurationTrendAsync"/> returned: one per collector,
    /// in collector order (which sets the palette), each in time order. A collector needs two buckets to draw a line, as it needed two
    /// runs before, so one with a single bucket in the range draws nothing.
    /// </summary>
    /// <param name="buckets">The collectors' buckets, in any order.</param>
    internal static List<CollectorDurationSeries> Build(IEnumerable<CollectorDurationBucket> buckets)
    {
        var series = new List<CollectorDurationSeries>();
        foreach (var group in buckets.GroupBy(b => b.CollectorName).OrderBy(g => g.Key))
        {
            var points = group.OrderBy(b => b.BucketStart).ToList();
            if (points.Count < 2) continue;

            series.Add(new CollectorDurationSeries(
                group.Key,
                points.Select(p => p.BucketStart.ToOADate()).ToArray(),
                points.Select(p => (double)p.MaxDurationMs).ToArray(),
                points.Select(p => p.AvgDurationMs).ToArray(),
                points.Select(p => p.RunCount).ToArray()));
        }

        return series;
    }

    /// <summary>
    /// The hover's fourth line for each point, the line of how many runs a point is the slowest of and what the bucket averaged (<see cref="CollectorDurationHoverText"/>, the
    /// words Lite's chart uses), keyed by the X the point is drawn at: the very <see cref="Times"/> the scatter is built from, so the
    /// hover's lookup by the nearest point's X is exact. Keyed by X and not by position because the line breaks at every collection
    /// gap by inserting a point of its own, which shifts every position after the first break; that point has no key and so no line.
    /// </summary>
    internal IReadOnlyDictionary<double, string> DetailsByX()
    {
        var byX = new Dictionary<double, string>(Times.Length);
        for (var i = 0; i < Times.Length && i < AvgMs.Length && i < Runs.Length; i++)
        {
            byX[Times[i]] = CollectorDurationHoverText.Detail(Runs[i], AvgMs[i]);
        }

        return byX;
    }
}
