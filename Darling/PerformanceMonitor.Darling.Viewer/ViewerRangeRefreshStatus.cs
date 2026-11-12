/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8549: what the status bar says between a pick in the toolbar's time range picker and the data of the picked range landing.
/// The picker names the new range the moment it is picked, but a slow tab (Query Store by Duration over 30 days read for half a
/// minute) keeps showing the old range's grid and slicer until its read returns. Worse, a pick made while an earlier refresh was
/// still reading is only queued (<c>_refreshRequested</c>): the in-flight load finished the OLD window, painted it and stamped
/// "refreshed", and the picked window's own read began after that. So the header read "Past 30 days" over a 24 hour grid with a
/// fresh "refreshed" stamp. The status now says the picked range is loading from the pick on, and a load that has another
/// refresh queued behind it does not stamp itself as refreshed.
/// </summary>
public static class ViewerRangeRefreshStatus
{
    /// <summary>The status line from the pick until the picked range's load stamps "refreshed" (or reports its failure).</summary>
    public static string Loading(string serverName, TimeRangeSpec spec) => $"{serverName}: loading {spec.Name}…";

    /// <summary>
    /// Whether a load that has just finished says "refreshed". It does not when another refresh is queued behind it: the queued one
    /// reads the window the picker names now, and the status stays on "loading" until that one lands.
    /// </summary>
    public static bool StampsRefreshed(bool refreshQueued) => !refreshQueued;
}
