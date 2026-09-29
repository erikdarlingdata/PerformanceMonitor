/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The row conversions behind the analysis and alert reads of a SQL Server's LOCAL wall-clock columns (#4821).
/// </summary>
internal static class ServerLocalTimes
{
    public static ServerClock ClockFrom(string? timeZoneId, int? utcOffsetMinutes) => ServerClock.Utc;

    public static bool CreatedByWindowStart(ServerClock clock, DateTime? creationTimeLocal, DateTime windowStartUtc) => false;

    public static List<ConfigChangeAttribution.TraceLine> TraceLinesInWindow(
        IEnumerable<(DateTime? EventTimeLocal, string? TextData)> rows,
        ServerClock clock,
        DateTime windowStartExclusiveUtc,
        DateTime windowEndInclusiveUtc) => new();

    public static int? JobStartOffsetMinutes(ServerClock clock, DateTime startTime, int? snapshotOffsetMinutes) => snapshotOffsetMinutes;
}
