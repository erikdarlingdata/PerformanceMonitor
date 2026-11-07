/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8133: the fleet Overview's card area while its first read is in flight. On a store with many servers over a slow link
/// that read takes tens of seconds, and an empty card area is the same picture as an empty fleet, so the area says it is
/// loading until the read returns or fails.
/// </summary>
public static class FleetOverviewLoadingText
{
    public const string Loading = "Loading the fleet…";

    /// <summary>Shown while a read is in flight and no card is drawn yet; once cards exist a refresh repaints them in place.</summary>
    public static bool Shows(bool readInFlight, int cardCount) => readInFlight && cardCount == 0;
}
