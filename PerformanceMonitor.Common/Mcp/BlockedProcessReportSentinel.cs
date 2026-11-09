/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The blocked-process report's "never" stamp. SQL Server writes <c>lastbatchstarted="1900-01-01T00:00:00"</c> (and the same
/// for <c>lastbatchcompleted</c> and <c>lasttranstarted</c>) for a session that has no such instant: a real, parseable value, not
/// NULL. The collectors store it as written, in the monitored server's local clock, so a reader that shifts every stamp to UTC
/// turns it into a date near 1900 that looks measured. A stamp at or below the floor is unset and is answered as null; the
/// floor sits a day above the sentinel, so a stamp judged after a time-zone shift is still inside it.
/// </summary>
public static class BlockedProcessReportSentinel
{
    /// <summary>The first instant that counts as a real stamp. The sentinel is 1900-01-01 in the server's clock, so a whole day
    /// of headroom covers every time-zone shift.</summary>
    public static readonly DateTime Floor = new(1900, 1, 2);

    /// <summary>True for a stamp the report wrote as its sentinel (at or before <see cref="Floor"/>).</summary>
    public static bool IsUnset(DateTime value) => value <= Floor;

    /// <summary>The stamp itself, or null when it is unset or absent.</summary>
    public static DateTime? OrNull(DateTime? value) => value is { } v && !IsUnset(v) ? v : null;
}
