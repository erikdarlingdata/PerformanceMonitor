/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #9365: the one place a failed refresh is reported. A tab used to write the driver's own message into the status bar
/// ("refresh failed: Exception while reading from stream") and nothing into the Viewer log, so a failure could not be told
/// from a slow store, a cancelled read or a lost connection afterwards. <see cref="Report"/> writes ONE warning line that
/// names the tab, the read that failed, how long it ran and the exception, and answers the short plain-words text the status
/// bar shows in its place.
/// </summary>
internal static class ViewerRefreshFailure
{
    /// <summary>The log source of the line.</summary>
    internal const string LogSource = "Refresh";

    /// <summary>The note a failed chart read leaves beside a grid that did load.</summary>
    internal const string GridIsCurrent = "The grid is current.";

    /// <summary>Writes the failure's one warning line and answers the status text for it.</summary>
    /// <param name="tab">The tab or surface that was refreshing, as the slow-load line names it.</param>
    /// <param name="read">The named read that failed (a <see cref="ViewerLoadTimer"/> phase), or null when the failure is not one read's.</param>
    /// <param name="elapsedMs">How long the read ran.</param>
    /// <param name="failure">The exception.</param>
    /// <param name="restNote">What is still current, said after the plain words (null says nothing).</param>
    /// <param name="retried">The read was tried a second time and failed again.</param>
    internal static string Report(string tab, string? read, long elapsedMs, Exception failure, string? restNote = null, bool retried = false)
    {
        ViewerLogger.Warn(LogSource, LogLine(tab, read, elapsedMs, failure, retried));
        return PlainText(tab, read, restNote);
    }

    /// <summary><see cref="Report"/> for a load on a <see cref="ViewerLoadTimer"/>: the read that failed and its own time come from the clock's tracked reads, and a failure no tracked read carries names no read and gives the clock's total.</summary>
    internal static string Report(string tab, ViewerLoadTimer timer, Exception failure, string? restNote = null)
    {
        var failed = timer.FailedRead(failure);
        return Report(tab, failed?.Phase, failed?.Ms ?? timer.ElapsedMs, failure, restNote);
    }

    /// <summary>The log line: tab, read, time, then each exception in the chain by type and message.</summary>
    internal static string LogLine(string tab, string? read, long elapsedMs, Exception failure, bool retried = false)
    {
        var line = new StringBuilder();
        line.Append('[').Append(tab).Append("] refresh failed: ").Append(read ?? "no single read").Append(" ran ")
            .Append(elapsedMs).Append(" ms").Append(retried ? " (after one retry)" : "").Append(". ");
        var depth = 0;
        for (var e = failure; e is not null && depth < 4; e = e.InnerException, depth++)
        {
            line.Append(depth == 0 ? "" : " <- ").Append(e.GetType().Name).Append(": ").Append(e.Message);
        }

        return line.ToString();
    }

    /// <summary>
    /// What did not load, in words a reader can act on, never the exception's own message. A named chart read says "The time-range
    /// chart did not load."; any other named read says "The &lt;read&gt; did not load."; with no read named the tab itself did not load.
    /// <paramref name="restNote"/> follows when the rest of the tab is current. The Viewer log holds the cause.
    /// </summary>
    internal static string PlainText(string tab, string? read, string? restNote = null)
    {
        if (string.IsNullOrWhiteSpace(read))
        {
            return $"{tab} did not load. The Viewer log has the cause.";
        }

        var what = read switch
        {
            "slicer read" => "The time-range chart",
            _ => "The " + (read.EndsWith(" read", StringComparison.Ordinal) ? read[..^" read".Length] : read),
        };
        return string.IsNullOrEmpty(restNote) ? $"{what} did not load." : $"{what} did not load. {restNote}";
    }
}
