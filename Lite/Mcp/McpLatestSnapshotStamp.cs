/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The one arithmetic every stamped latest-snapshot read shares (#3541 A10) — Lite's twin of Darling's
/// <c>LatestSnapshotStamp</c>; the two must stay in step so <c>age_seconds</c> means the same thing on both SKUs.
/// </summary>
internal static class McpLatestSnapshotStamp
{
    /// <summary>
    /// Whole seconds from a snapshot's stamp to the window's end — the anchor the caller asked for, never the
    /// service clock (<c>AsOfWindowAnchorTests</c>: an anchored tool's only "now" is its <c>as_of</c>, and a tool
    /// body that names <c>DateTime.UtcNow</c> fails the census). The store stamps naive UTC and the anchor is
    /// <c>Kind=Utc</c>; the subtraction ignores Kind, which is correct here because both are UTC instants.
    /// Non-negative by construction for a windowed read, whose rows are bounded by
    /// <c>collection_time &lt;= window end</c>; clamped at zero anyway so a sub-second precision difference
    /// between a microsecond store stamp and a 100 ns anchor can never publish "-0".
    /// </summary>
    public static long AgeSeconds(DateTime capturedAt, DateTime windowEnd) =>
        Math.Max(0L, (long)Math.Round((windowEnd - capturedAt).TotalSeconds));
}
