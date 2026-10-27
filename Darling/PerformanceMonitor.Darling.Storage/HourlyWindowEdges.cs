/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hour-bucket edges of a read served from an hourly rollup. A bucket is keyed by its start hour and the
/// read takes <c>bucket &gt;= start AND bucket &lt; end</c>, so an unaligned window start drops the partial
/// hour it falls inside, and an unaligned window end counts the bucket holding it whole, up to the top of the
/// next hour. A window end exactly on the hour stops there: the hour that begins at the end is not read.
/// </summary>
public static class HourlyWindowEdges
{
    /// <summary>The span an hourly read actually served: from the first bucket it held (the next hour edge when
    /// the window start is unaligned and no bucket was found) to the top of the end hour, cut at the rollup's
    /// materialization ceiling. The end is null when the rollup holds no materialized bucket at all.</summary>
    public static (DateTime Start, DateTime? End) ServedSpan(
        DateTime requestedStart, DateTime? firstBucket, DateTime requestedEnd, DateTime? ceiling)
    {
        var startHour = FloorHour(requestedStart);
        var start = firstBucket ?? (startHour != requestedStart ? startHour.AddHours(1) : requestedStart);
        var endHour = FloorHour(requestedEnd);
        var wholeEnd = endHour != requestedEnd ? endHour.AddHours(1) : endHour;
        DateTime? end = ceiling is null ? null : (ceiling.Value < wholeEnd ? ceiling.Value : wholeEnd);
        return (start, end);
    }

    /// <summary>One sentence per edge that moved, joined. <paramref name="ceiling"/> is the materialization ceiling
    /// of the relation serving the window end (null = nothing materialized). When the ceiling is at or before
    /// the window end, the end sentence says nothing after it was read, and, if the start also moved, names the
    /// whole served span. Every instant it names is UTC with the Z (<c>McpHelpers.FormatEffectiveStart</c>), as
    /// <c>effective_start</c> prints (#4966): the window's own edges arrive UTC and the first bucket and the ceiling come off
    /// the store naive, and a plain "o" put the zone marker on some of them and not others.</summary>
    public static string? Note(DateTime requestedStart, DateTime? firstBucket, DateTime requestedEnd, DateTime? ceiling)
    {
        var parts = new System.Collections.Generic.List<string>(3);
        var startHour = FloorHour(requestedStart);
        var (served, servedEnd) = ServedSpan(requestedStart, firstBucket, requestedEnd, ceiling);
        var startMoved = served != requestedStart;
        if (startHour != requestedStart)
        {
            parts.Add(StartSentence(requestedStart, served));
        }

        var endHour = FloorHour(requestedEnd);
        if (ceiling is null)
        {
            parts.Add("materialization ceiling unknown; the end edge is not verified");
        }
        else if (ceiling.Value <= requestedEnd)
        {
            var cut = $"the hourly rollup is materialized only to {Utc(ceiling.Value)}; nothing after it was read";
            parts.Add(startMoved ? $"served from {Utc(served)} to {Utc(ceiling.Value)}; {cut}" : cut);
        }
        else if (endHour != requestedEnd)
        {
            parts.Add($"the bucket at {Utc(endHour)} is included whole, so up to {Utc(servedEnd!.Value)} is counted past as_of");
        }

        return string.Join("; ", parts);
    }

    /// <summary>#6831: the edges <see cref="Note"/> words, as fields a page can say in its own clock and words. <c>StartDroppedFrom</c> is the
    /// requested start when the first partial hour was not read (null when the start was on the hour). <c>MaterializedTo</c> is the rollup's
    /// build limit, set only when it ended the read at or before the window's end. <c>EndCountedWhole</c> is true when an unaligned end counted
    /// its whole last bucket, so the data runs past the window's end. <c>ServedEnd</c> is null when the rollup's ceiling is not known.</summary>
    public readonly record struct Edges(DateTime ServedStart, DateTime? ServedEnd, DateTime? StartDroppedFrom, DateTime? MaterializedTo, bool EndCountedWhole);

    /// <summary>The structured twin of <see cref="Note"/>: the same cases, as values.</summary>
    public static Edges Describe(DateTime requestedStart, DateTime? firstBucket, DateTime requestedEnd, DateTime? ceiling)
    {
        var (served, servedEnd) = ServedSpan(requestedStart, firstBucket, requestedEnd, ceiling);
        DateTime? dropped = FloorHour(requestedStart) != requestedStart ? requestedStart : null;
        DateTime? cut = ceiling is DateTime c && c <= requestedEnd ? c : null;
        var whole = ceiling is DateTime c2 && c2 > requestedEnd && FloorHour(requestedEnd) != requestedEnd;
        return new Edges(served, servedEnd, dropped, cut, whole);
    }

    /// <summary>The start edge's sentence alone, for a read whose END is exact (the Query Store route reads the wide table up to the window end,
    /// #6800), so <see cref="Note"/>'s end clauses, which describe an hourly rollup's end bucket, do not apply. Null when the start is on the hour.
    /// The words are <see cref="Note"/>'s own.</summary>
    public static string? StartNote(DateTime requestedStart, DateTime firstBucket) =>
        FloorHour(requestedStart) == requestedStart ? null : StartSentence(requestedStart, firstBucket);

    private static string StartSentence(DateTime requestedStart, DateTime served) =>
        $"hourly buckets start on the hour: no bucket before {Utc(served)}: the data from {Utc(requestedStart)} to {Utc(served)} is not included";

    /// <summary>
    /// An instant as <c>McpHelpers.FormatEffectiveStart</c> prints it: UTC, with the Z. Only the kind is set; the instant is
    /// never shifted. That formatter is internal to Common, which this project cannot see, so its one-line body is repeated here
    /// and <c>HourlyWindowEdgesTests</c> holds the two to the same text.
    /// </summary>
    private static string Utc(DateTime instant) =>
        DateTime.SpecifyKind(instant, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

    private static DateTime FloorHour(DateTime value) =>
        new(value.Year, value.Month, value.Day, value.Hour, 0, 0, value.Kind);
}
