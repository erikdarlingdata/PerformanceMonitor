/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The ceiling of <c>sys.dm_exec_requests.total_elapsed_time</c>: a 32-bit int that stops at 2147483647 ms (24 days 20:31:23.647)
/// for a request that has run longer. The collector stores the value and its formatted text as the engine gave them, so a
/// reader that passes them through asserts an exact duration that is not a measurement. A row at the ceiling answers both
/// fields null with <see cref="Note"/> instead.
/// </summary>
public static class ElapsedTimeCeiling
{
    /// <summary>The engine's largest <c>total_elapsed_time</c>: <see cref="int.MaxValue"/> milliseconds.</summary>
    public const long Milliseconds = int.MaxValue;

    /// <summary>True when the stored elapsed time is the counter's ceiling, not a duration.</summary>
    public static bool IsAtCeiling(long elapsedMs) => elapsedMs >= Milliseconds;

    /// <summary>The note a row at the ceiling carries in place of its two elapsed-time fields.</summary>
    public const string Note =
        "elapsed_time_ms and elapsed_time_formatted are null because the request has run longer than the engine's counter can "
        + "hold: total_elapsed_time is a 32-bit int that stops at 2147483647 ms (24 days 20:31:23.647), so the stored value is the "
        + "ceiling, not a measurement. The request is at least that old; its start time on the monitored server gives the real age.";
}
