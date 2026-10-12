/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Ui;

/// <summary>
/// What the Long Queries grid says over its empty rows (Lite and the Darling Viewer). The collector is opt-in and off by
/// default, so an empty grid on a server whose trace is off is not "no long queries ran" (the Viewer listed 21 runs of a
/// 50 second statement on a server Lite was not tracing). The words name the switch instead of the generic "No data for
/// the selected time range." that read as a bug.
/// </summary>
public static class LongQueriesEmptyText
{
    /// <summary>The grid's empty text: a plain "none in this window" when the trace is on, the way to turn it on when it is off.</summary>
    /// <param name="traceEnabled">Whether the long_query_completions collector is enabled for this server.</param>
    /// <param name="whereToTurnItOn">The app's own settings path, for example "Settings → Collector Schedules → Edit".</param>
    public static string Text(bool traceEnabled, string whereToTurnItOn) => traceEnabled
        ? "No long-running query completions in the selected time window."
        : "Nothing is listed because the long-query completion trace is OFF for this server, so no completions are collected. "
          + $"Turn it on in {whereToTurnItOn}, then tick the 'long_query_completions' Enabled box.";
}
