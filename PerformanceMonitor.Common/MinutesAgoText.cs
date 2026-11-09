/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// The "N ago" wording of an elapsed minutes count, written once for both apps' Overview cards (#8465): "just now" under a
/// minute, then "5m ago", "3h ago", "2d ago" and "1w ago", each unit whole and rounded down. An offline card's CPU row says
/// when the server was last collected in these words, and the Darling card's blocking and deadlock details say when the last
/// event was.
/// </summary>
public static class MinutesAgoText
{
    /// <summary>The wording for <paramref name="minutes"/> elapsed. A negative count reads as "just now".</summary>
    public static string Format(int minutes)
    {
        if (minutes < 1) return "just now";
        if (minutes < 60) return $"{minutes}m ago";
        if (minutes < 1440) return $"{minutes / 60}h ago";   // 24 hours
        if (minutes < 10080) return $"{minutes / 1440}d ago"; // 7 days
        return $"{minutes / 10080}w ago";
    }
}
