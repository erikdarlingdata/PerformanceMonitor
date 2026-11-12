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
/// 50 second statement on a server Lite was not tracing). The sentence says why the rows are empty, not the generic "No data
/// for the selected time range." that read as a bug. The banner above the grid carries the switch and the box to tick, so the
/// grid does not repeat them: one short sentence that sits on one line in the empty-hint style.
/// </summary>
public static class LongQueriesEmptyText
{
    /// <summary>The grid's empty text: a plain "none in this window" when the trace is on, that the trace is off when it is off.</summary>
    /// <param name="traceEnabled">Whether the long_query_completions collector is enabled for this server.</param>
    public static string Text(bool traceEnabled) => traceEnabled
        ? "No long-running query completions in the selected time window."
        : "The long-query completion trace is off for this server, so no completions are collected.";
}
