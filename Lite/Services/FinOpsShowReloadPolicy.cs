/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// When the FinOps tab re-reads everything for the selected server as it is shown. The tab loads once at start-up, before the first
/// collection has run, so a store that was closed for days (or a server enrolled a minute ago) shows "No Data" and idle-database
/// claims read from that empty window until the server is reselected. Reselecting runs the whole per-server load again; showing the
/// tab does the same, unless the last whole load is recent.
/// </summary>
internal static class FinOpsShowReloadPolicy
{
    /// <summary>How recent the last whole per-server load may be for a show to skip the reload: flipping between tabs does not re-run fourteen reads each time.</summary>
    internal static readonly TimeSpan MinAge = TimeSpan.FromSeconds(30);

    /// <summary>True when showing the tab should run the whole per-server load: none has run yet, or the last one began at least <see cref="MinAge"/> ago.</summary>
    internal static bool ShouldReloadOnShow(DateTime? lastLoadStartedUtc, DateTime nowUtc) =>
        lastLoadStartedUtc is not DateTime last || nowUtc - last >= MinAge;
}
