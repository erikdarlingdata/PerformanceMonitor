/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// How a panel note, a web panel or an MCP tool says how often a collector runs. The cadence is the collector's effective one (the shipped default, or the
/// fleet or per-server schedule override), so a note never states the default as fact for a server whose schedule was changed.
/// </summary>
public static class CollectorCadenceText
{
    /// <summary>"hourly", "daily", "every minute", "every 30 minutes" or "every 2 hours". Zero or less (no recurring cadence) reads "on its schedule".</summary>
    public static string Every(int minutes)
    {
        if (minutes <= 0)
        {
            return "on its schedule";
        }

        return minutes switch
        {
            1 => "every minute",
            60 => "hourly",
            1440 => "daily",
            _ => "every " + Span(minutes),
        };
    }

    /// <summary>"minute", "hour", "day", "30 minutes", "2 hours" or "3 days": the span of one collection interval, for "the last ...". Zero or less reads "collection interval".</summary>
    public static string Span(int minutes)
    {
        if (minutes <= 0)
        {
            return "collection interval";
        }

        var (count, unit) = minutes % 1440 == 0 ? (minutes / 1440, "day")
            : minutes % 60 == 0 ? (minutes / 60, "hour")
            : (minutes, "minute");
        return count == 1
            ? unit
            : count.ToString(CultureInfo.InvariantCulture) + " " + unit + "s";
    }
}
