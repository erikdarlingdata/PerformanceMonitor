/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// The counts a ring-buffer collector leaves on its run when the Extended Events ring buffer it read was truncated
/// (#6441), and the reader that turns them back into a health band and a sentence.
///
/// <para>SQL Server caps the XML of a ring_buffer target at about 4 MB (4,194,304 characters) and marks the cut with
/// <c>truncated="1"</c> on <c>RingBufferTarget</c>. The cut keeps the OLDEST events and drops the newest. A collector
/// that reads the XML with a time watermark then sees only events older than its watermark, stores 0 rows and records
/// SUCCESS, so nothing but these counts says the read is blind. The collector writes them through
/// <c>CollectorContext.Measure</c> (counts, never sentences), so they reach collection_log as the run's note:
/// <c>ring_buffer_truncated=1 ring_buffer_events_hidden=773 newest_visible_event_age_s=19380</c>.</para>
///
/// <para>The labels live here, in Common, because the writer (the Collectors assembly) references Common and the
/// readers (<see cref="CollectorHealthClassifier"/>, the health-parser reads in Lite and Darling) cannot reference the
/// Collectors assembly. One set of constants, so a renamed label moves the writer and every reader together.</para>
/// </summary>
public static class RingBufferTruncationNote
{
    /// <summary>1 when the ring buffer's XML carried <c>truncated="1"</c>. Written only when it did.</summary>
    public const string TruncatedLabel = "ring_buffer_truncated";

    /// <summary>The target's own <c>eventCount</c> attribute less the events the truncated XML holds.</summary>
    public const string EventsHiddenLabel = "ring_buffer_events_hidden";

    /// <summary>Seconds between the newest event the truncated XML holds and the server's clock at the read.</summary>
    public const string NewestVisibleAgeLabel = "newest_visible_event_age_s";

    /// <summary>
    /// Whether <paramref name="note"/> carries <c>ring_buffer_truncated=</c> with a value above zero, and the other two
    /// counts when it does (0 when a count is absent). Reads the label=value pairs anywhere in the note, so a host note
    /// before the counts (<c>"...; ring_buffer_truncated=1 ..."</c>) still parses.
    /// </summary>
    public static bool TryParse(string? note, out long eventsHidden, out long newestVisibleAgeSeconds)
    {
        eventsHidden = 0;
        newestVisibleAgeSeconds = 0;
        if (string.IsNullOrWhiteSpace(note))
        {
            return false;
        }

        long truncated = 0;
        foreach (var token in note.Split(new[] { ' ', ';' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = token.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0
                || !long.TryParse(token.AsSpan(eq + 1), NumberStyles.None, CultureInfo.InvariantCulture, out var value))
            {
                continue;
            }

            var label = token.AsSpan(0, eq);
            if (label.SequenceEqual(TruncatedLabel))
            {
                truncated = value;
            }
            else if (label.SequenceEqual(EventsHiddenLabel))
            {
                eventsHidden = value;
            }
            else if (label.SequenceEqual(NewestVisibleAgeLabel))
            {
                newestVisibleAgeSeconds = value;
            }
        }

        if (truncated > 0)
        {
            return true;
        }

        eventsHidden = 0;
        newestVisibleAgeSeconds = 0;
        return false;
    }

    /// <summary>
    /// The plain sentence for a truncated read, or null when the note does not say the ring buffer was truncated.
    /// Used by the collection-health note and the health-parser reads, so both say the same thing.
    /// </summary>
    public static string? Describe(string? note)
    {
        if (!TryParse(note, out var hidden, out var ageSeconds))
        {
            return null;
        }

        return string.Format(
            CultureInfo.InvariantCulture,
            "the Extended Events ring buffer this collector reads is truncated (SQL Server cuts its XML at about 4 MB and drops the NEWEST events): {0} event(s) are hidden from the read and the newest visible event is {1} old, so newer events are not being collected",
            hidden,
            FormatAge(ageSeconds));
    }

    /// <summary>
    /// The health-parser reads' answer (Lite and Darling, one string) when a category came back empty and the newest
    /// system_health collection run says the ring buffer was truncated. Without it those reads called the window
    /// "genuinely quiet" or "the healthy answer" while the newest events sat on the server unread (#6441).
    /// <para>#6650: when the read did count events of the type (<paramref name="eventsRead"/> above 0) the first clause
    /// says so, with the filter that left none (<paramref name="noneQualifiedBecause"/>), instead of "No ... events were
    /// found" over a count of hundreds. At 0 it keeps that wording.</para>
    /// <para>#6827: <paramref name="stretch"/> is the truncated runs that read the read's window (the runs inside it and the
    /// first one after its end). The reads pass it, so the
    /// sentence names how many runs were truncated and when, rather than the collector's newest run today. Null keeps
    /// the "newest run" wording.</para>
    /// </summary>
    public static string HealthParserMessage(
        string eventType, string serverName, int hoursBack, string truncation,
        int eventsRead = 0, string? noneQualifiedBecause = null, TruncatedStretch? stretch = null)
    {
        var first = eventsRead > 0
            ? string.Format(
                CultureInfo.InvariantCulture,
                "{0} {1} event(s) were read for {2} in the last {3} hour(s), and {4}",
                eventsRead,
                eventType,
                serverName,
                hoursBack,
                noneQualifiedBecause ?? "none qualified")
            : string.Format(
                CultureInfo.InvariantCulture,
                "No {0} events were found for {1} in the last {2} hour(s)",
                eventType,
                serverName,
                hoursBack);

        /* #6827: a read over a window answers for the runs that read it, not for the collector's newest run today. */
        var who = stretch is { } s
            ? s.Count == 1
                ? string.Format(CultureInfo.InvariantCulture, "the system_health collection run at {0}, which read events of this window", FormatStamp(s.First))
                : string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} system_health collection runs that read events of this window (the first at {1}, the last at {2})",
                    s.Count, FormatStamp(s.First), FormatStamp(s.Last))
            : "the newest system_health collection run";

        return first + string.Format(
            CultureInfo.InvariantCulture,
            ", and this is NOT an all-clear: {0} reported that {1}. Events of this type may exist on the server and not be stored here. Restarting the system_health session empties its ring buffer, and collection resumes until the buffer fills again.",
            who,
            truncation);
    }

    /// <summary>
    /// #6827: the truncated system_health runs that read a read's window (inside it, or the first after its end): how many, when the first and the last
    /// ran (naive UTC, the frame collection_time is stored in), and the note of the newest of them (the hidden-event
    /// count and age <see cref="Describe"/> turns into a sentence).
    /// </summary>
    public readonly record struct TruncatedStretch(int Count, DateTime First, DateTime Last, string NewestNote);

    private static string FormatStamp(DateTime utc) =>
        utc.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC";

    /// <summary>An age in seconds as <c>5 h 23 min</c>, <c>20 min</c> or <c>45 s</c>.</summary>
    public static string FormatAge(long seconds)
    {
        if (seconds < 0)
        {
            seconds = 0;
        }

        if (seconds < 60)
        {
            return seconds.ToString(CultureInfo.InvariantCulture) + " s";
        }

        var minutes = seconds / 60;
        if (minutes < 60)
        {
            return minutes.ToString(CultureInfo.InvariantCulture) + " min";
        }

        var hours = minutes / 60;
        if (hours < 48)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0} h {1} min", hours, minutes % 60);
        }

        return string.Format(CultureInfo.InvariantCulture, "{0} d {1} h", hours / 24, hours % 24);
    }
}
