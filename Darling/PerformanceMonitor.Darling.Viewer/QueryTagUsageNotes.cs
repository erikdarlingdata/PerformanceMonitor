/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #6831: the sentences the Query Tag Usage view puts under a section, in the viewer's own clock and words. The MCP tool says the same edges as
/// a note with raw 100 ns UTC stamps and the relation names it read (<c>collect.query_stats_io_hourly (hourly rollup)</c>), for a caller that
/// parses them. A person reading this window gets the edges as fields (<see cref="HourlyWindowEdges.Describe"/>) worded like the web page's
/// <c>edgesText</c>, every instant through <see cref="ViewerTimeHelper"/> (the display mode and time zone the person chose) and no table names.
/// </summary>
public static class QueryTagUsageNotes
{
    /// <summary>The viewer's text format for a note's instant (a grid cell's own format, without the seconds a window edge never has).</summary>
    public const string StampFormat = "yyyy-MM-dd HH:mm";

    /// <summary>
    /// Renders an instant for a read over <paramref name="serverCount"/> servers. One server keeps its own clock, as every grid on that server's
    /// tab does. Several servers have no one clock, so the instant is read on the fleet-wide rule (local time or UTC). Either way the zone's label
    /// follows the time, so the note never leaves the reader to guess which clock it used.
    /// </summary>
    public static Func<DateTime, string> Stamper(int serverCount) => serverCount == 1
        ? utc =>
        {
            var naive = DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);
            return ViewerTimeHelper.FormatForDisplay(naive, StampFormat) + " "
                + ViewerTimeHelper.GetTimezoneLabel(ViewerTimeHelper.CurrentDisplayMode, ViewerTimeHelper.ActiveServerClock, naive);
        }
        : utc => ViewerTimeHelper.FormatFleetWide(DateTime.SpecifyKind(utc, DateTimeKind.Unspecified), StampFormat);

    /// <summary>A section read from an hourly rollup (plan cache, procedures): where it read from, and each edge the hour buckets moved. The span
    /// ends where the read actually stopped (<see cref="HourlyWindowEdges.Edges.ServedEnd"/>), which is not the window's end when the last hour
    /// was counted whole or the rollup is built only part of the way.</summary>
    public static string Hourly(HourlyWindowEdges.Edges edges, Func<DateTime, string> at)
    {
        var sentences = EdgeSentences(edges.StartDroppedFrom, edges.ServedStart, edges.MaterializedTo, edges.EndCountedWhole, edges.ServedEnd, at);
        if (edges.ServedEnd is null)
        {
            sentences.Add("How far the hourly rollup is built is not known, so its end is not checked.");
        }

        return sentences.Count == 0
            ? "Read from the hourly rollup up to " + at(edges.ServedEnd!.Value) + "."
            : "Read from the hourly rollup, from " + at(edges.ServedStart) + (edges.ServedEnd is DateTime end ? " to " + at(end) : "") + ". " + string.Join(" ", sentences);
    }

    /// <summary>
    /// The Query Store section read from the hash tables with the raw intervals for the edges. <paramref name="servedStart"/> is the first instant
    /// read (the window start rounded up to the hour) and <paramref name="startDroppedFrom"/> the window start that rounding passed over.
    /// </summary>
    public static string QueryStore(bool daily, DateTime servedStart, DateTime? startDroppedFrom, DateTime windowEnd, Func<DateTime, string> at)
    {
        var lead = $"Read from the {(daily ? "daily and hourly Query Store tables" : "hourly Query Store table")}, with the raw intervals for the edges, from "
            + at(servedStart) + " to " + at(windowEnd) + ".";
        var sentences = EdgeSentences(startDroppedFrom, servedStart, null, false, windowEnd, at);
        return sentences.Count == 0 ? lead : lead + " " + string.Join(" ", sentences);
    }

    /// <summary>A raw read that starts after the window does: what the raw collections still hold.</summary>
    public static string RawFloor(string sectionName, DateTime effectiveStart, Func<DateTime, string> at) =>
        $"The raw {sectionName} collections no longer hold the whole window. The figures start at {at(effectiveStart)}.";

    /// <summary>"This store has no query stats data yet.": the section's source is missing, said in the words Lite uses (#6850), never the table's name.</summary>
    public static string Missing(string sectionName) => QueryTagSourceWords.Missing(sectionName);

    private static List<string> EdgeSentences(
        DateTime? startDroppedFrom, DateTime servedStart, DateTime? materializedTo, bool endCountedWhole, DateTime? servedEnd, Func<DateTime, string> at)
    {
        var sentences = new List<string>();
        if (startDroppedFrom is DateTime dropped && servedStart > dropped)
        {
            var minutes = (int)Math.Round((servedStart - dropped).TotalMinutes);
            if (minutes >= 60)
            {
                // The first bucket came hours after the window start (nothing was stored for the hours between): "the N minutes before the first
                // full hour" would call the whole gap a partial hour, so name the span the read did not cover.
                sentences.Add("The data from " + at(dropped) + " to " + at(servedStart) + " is not included.");
            }
            else
            {
                sentences.Add(minutes < 1
                    ? "Less than a minute before the first full hour is not included."
                    : $"The {minutes} {(minutes == 1 ? "minute before the first full hour is" : "minutes before the first full hour are")} not included.");
            }
        }

        if (materializedTo is DateTime built)
        {
            sentences.Add("The hourly rollup is built only up to " + at(built) + ", so nothing after that is included.");
        }
        else if (endCountedWhole && servedEnd is DateTime end)
        {
            sentences.Add("The last hour is counted whole, so the data runs to " + at(end) + ", past the end of the window.");
        }

        return sentences;
    }
}
