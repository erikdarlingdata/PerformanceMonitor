/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// Which write Dismiss All on the Alert History tab sends (#7160). The window write dismisses every undismissed alert
/// in the time window and server, with no column predicate and no row cap. That is exactly what the grid shows only
/// while no column filter narrows it and the read stayed under its row cap. Otherwise the confirmation counts rows the
/// write would go beyond, and there is no undismiss, so Dismiss All dismisses the exact rows on screen instead.
/// Pure and in the shared project so the Lite tab and the Viewer tab ask the same question.
/// </summary>
public static class AlertDismissScope
{
    /// <summary>True when Dismiss All must dismiss the rows on screen by key rather than the whole window.
    /// <paramref name="columnFilterActive"/> is whether any column filter is set. <paramref name="readRowCount"/> is
    /// how many rows the window read returned, and <paramref name="rowCap"/> the read's cap: a read that reached the
    /// cap left older alerts in the window that the grid never showed.</summary>
    public static bool UseShownRows(bool columnFilterActive, int readRowCount, int rowCap) =>
        columnFilterActive || readRowCount >= rowCap;
}
