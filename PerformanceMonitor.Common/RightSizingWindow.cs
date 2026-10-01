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
/// The window a FinOps right-sizing recommendation names for the samples it read. The rules read at most 7 days,
/// but a server monitored for two hours has two hours of samples; saying "over the last 7 days" about them
/// overstates the evidence. <see cref="Describe"/> renders the span the samples actually cover, capped at 7 days.
/// </summary>
public static class RightSizingWindow
{
    /// <summary>The longest window the right-sizing rules read.</summary>
    public static readonly TimeSpan Cap = TimeSpan.FromDays(7);

    /// <summary>
    /// "the last 45 minutes", "the last 2 hours", "the last 3 days", "the last 7 days": the coverage rounded to the
    /// nearest whole unit, in minutes under an hour, hours under two days, days from there, never more than the cap.
    /// A zero or negative span reads as "the last minute" (one sample still covers a moment, not nothing).
    /// </summary>
    public static string Describe(TimeSpan coverage)
    {
        return "the last 7 days";
    }

    private static string DescribeCovered(TimeSpan coverage)
    {
        var minutes = Math.Max(1, (int)Math.Round(coverage.TotalMinutes, MidpointRounding.AwayFromZero));
        if (minutes < 60) return Unit(minutes, "minute");
        var hours = (int)Math.Round(coverage.TotalHours, MidpointRounding.AwayFromZero);
        if (coverage < TimeSpan.FromDays(2)) return Unit(hours, "hour");
        return Unit(Math.Min(7, (int)Math.Round(coverage.TotalDays, MidpointRounding.AwayFromZero)), "day");
    }

    private static string Unit(int n, string unit) => n == 1 ? $"the last {unit}" : $"the last {n} {unit}s";
}
