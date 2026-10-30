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
/// Which source the system_health collector's newest run read, as that run left it in its collection_log note (#6453).
/// The collector writes <c>event_file_events=N</c> when it read the live session through its event_file target (N is
/// the rows it stored, 0 included) when that read reached the end of what the file holds, and <c>event_file_fallback=1</c> instead when it fell back to the ring buffer
/// (<c>PerformanceMonitor.Collectors.SystemHealthEventsCollector</c>, #6441); a pass writes one or the other. Only the
/// file read is proof that a running session was read to the end: the ring buffer path also records SUCCESS for a
/// session that is dead or whose buffer was cut, so a SUCCESS run with no rows is not a clear negative on its own.
///
/// <para>The label is spelled here because the readers (the health-parser reads in Lite and Darling) cannot reference
/// the Collectors assembly, the same split <see cref="RingBufferTruncationNote"/> makes. A Lite test holds it to the note
/// the collector really renders.</para>
/// </summary>
public static class SystemHealthSourceNote
{
    /// <summary>The rows stored by a pass that read the event_file target to the end. Written only then: a pass the per-pass cap stopped writes nothing here, so it is never taken for a clear negative.</summary>
    public const string EventFileEventsLabel = "event_file_events";

    /// <summary>
    /// True when <paramref name="note"/> (the newest SUCCESS run's note, null otherwise) carries
    /// <c>event_file_events=</c>: that run read the session's event_file target, which only runs against a session that
    /// is started. False for no note, a ring buffer read, a fallback, and any note without the label.
    /// </summary>
    public static bool ReadThroughEventFile(string? note)
    {
        if (string.IsNullOrWhiteSpace(note))
        {
            return false;
        }

        foreach (var token in note.Split(new[] { ' ', ';' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = token.IndexOf('=', StringComparison.Ordinal);
            if (eq > 0
                && token.AsSpan(0, eq).SequenceEqual(EventFileEventsLabel)
                && long.TryParse(token.AsSpan(eq + 1), NumberStyles.None, CultureInfo.InvariantCulture, out _))
            {
                return true;
            }
        }

        return false;
    }
}
