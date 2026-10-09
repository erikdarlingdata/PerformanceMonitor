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
/// tab does the same. The FIRST show after start-up always reloads, whatever the age of the start-up load (a user who opens Lite and
/// goes straight to FinOps would otherwise see the same empty pre-collection data). Later shows skip the reload when the last whole
/// load is recent, so flipping between tabs is not a reload storm. A tab that stays visible while a collection for its selected
/// server completes reloads too, at most once a minute.
/// </summary>
internal static class FinOpsShowReloadPolicy
{
    /// <summary>How recent the last whole per-server load may be for a show to skip the reload: flipping between tabs does not re-run fourteen reads each time.</summary>
    internal static readonly TimeSpan MinAge = TimeSpan.FromSeconds(30);

    /// <summary>How recent the last whole load may be for a completed collection to skip the reload: a collector cycle runs every minute, and 14 reads per cycle is the most a visible tab should add.</summary>
    internal static readonly TimeSpan CollectionReloadMinAge = TimeSpan.FromSeconds(60);

    /// <summary>
    /// True when showing the tab should run the whole per-server load: this is the first show since start-up
    /// (<paramref name="firstShowSinceStart"/>, whatever the age of the start-up load), none has run yet, or the last one began at
    /// least <see cref="MinAge"/> ago.
    /// </summary>
    internal static bool ShouldReloadOnShow(DateTime? lastLoadStartedUtc, DateTime nowUtc, bool firstShowSinceStart = false) =>
        firstShowSinceStart || lastLoadStartedUtc is not DateTime last || nowUtc - last >= MinAge;

    /// <summary>
    /// True when a collection for the selected server that finished at <paramref name="lastCollectionUtc"/> should reload the
    /// visible tab: it finished after the last whole load began (that load may have read the window before it), and that load began
    /// at least <see cref="CollectionReloadMinAge"/> ago. No load yet reloads.
    /// </summary>
    internal static bool ShouldReloadAfterCollection(DateTime? lastLoadStartedUtc, DateTime lastCollectionUtc, DateTime nowUtc) =>
        lastLoadStartedUtc is not DateTime last || (lastCollectionUtc > last && nowUtc - last >= CollectionReloadMinAge);
}
