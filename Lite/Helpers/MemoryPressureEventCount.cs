/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// #7691: a server with several memory nodes stores one ring buffer record per node for the same event, so the rows list an event
/// once per node. A count of events is one per (second, notification): two nodes that report the same notification in the same
/// second are one event, and different notifications at one time are different events. The grid still lists every row.
/// </summary>
internal static class MemoryPressureEventCount
{
    /// <summary>The first row of each (sample time to the second, notification), in the order given.</summary>
    internal static List<MemoryPressureEventRow> OnePerEvent(IEnumerable<MemoryPressureEventRow> rows) =>
        rows.GroupBy(r => (r.SampleTime.Ticks / TimeSpan.TicksPerSecond, r.MemoryNotification))
            .Select(g => g.First())
            .ToList();
}
