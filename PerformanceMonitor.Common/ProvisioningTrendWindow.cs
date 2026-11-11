/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one rule for the 7-day provisioning trend, shared by Lite, the Viewer, the dashboard and the MCP utilization view: seven
/// calendar days, the six whole days before today and today so far. The window starts at the midnight that begins the first of
/// them, in the zone the caller counts days in, so no row before today is a part day. Today's row is marked partial, since it
/// is judged on the hours collected so far.
/// </summary>
public static class ProvisioningTrendWindow
{
    /// <summary>The calendar days the trend lists, today included; the MCP payload's <c>trend_days</c> and the most rows a read returns.</summary>
    public const int Days = 7;

    /// <summary>What follows the date in the Day cell of a row whose day has not ended.</summary>
    public const string PartialDaySuffix = ", so far";

    /// <summary>The first day the trend lists for a <paramref name="today"/> in the caller's zone: six days before it, at midnight.</summary>
    public static DateTime FirstDay(DateTime today) => today.Date.AddDays(-(Days - 1));

    /// <summary>The Day cell: the weekday and date, and <see cref="PartialDaySuffix"/> on a day that has not ended.</summary>
    public static string DayLabel(DateTime day, bool isPartial) =>
        day.ToString("ddd MM/dd") + (isPartial ? PartialDaySuffix : "");
}
