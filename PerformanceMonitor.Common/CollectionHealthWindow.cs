/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The trailing window the Collection Health read covers in Lite and the Viewer, for every consumer of it. The read is anchored
/// on now and does not follow a tab's range picker. Its consumers are Lite's Collection Health grid and Lite's MCP
/// <c>get_collection_health</c> tool, which are one read; the Viewer's grid; the reads that stamp those rows (read source, observed
/// quiet gaps); Lite's denied-since-last-success badge; the Viewer's Sweep Pressure and fleet reads; and the connection-failure
/// sentence (<see cref="CollectionConnectionFailures"/>), which words it. They take the window from here so that none of them can
/// cover different days from the rows beside it. The Darling service reads the same seven days through constants of its own
/// (<c>ReadWindowBand.CollectionHealthHours</c>, <c>TimescaleSupport.CollectionHealthConsumerWindow</c>), each held by its tests.
/// </summary>
public static class CollectionHealthWindow
{
    /// <summary>The window in days.</summary>
    public const int Days = 7;
}
