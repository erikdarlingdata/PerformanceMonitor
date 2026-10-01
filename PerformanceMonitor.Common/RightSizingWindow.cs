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
    /// "12 samples over 45 minutes", "1,200 samples over 1 hour", "24 samples over 4 days": the sample count and the
    /// span between the oldest and newest sample, never "the last X" (samples days apart do not cover the time between
    /// them). The span is rounded DOWN to the whole unit (never claiming more than was observed), in minutes under an
    /// hour, hours under two days, days from there, never more than the cap. A span under a minute reads
    /// "N samples within a minute"; one sample reads "1 sample" and none "no samples".
    /// </summary>
    public static string Describe(long sampleCount, TimeSpan span)
    {
        if (sampleCount <= 0) return "no samples";
        if (sampleCount == 1) return "1 sample";
        var count = sampleCount.ToString("N0", System.Globalization.CultureInfo.InvariantCulture);
        if (span >= Cap) return $"{count} samples over 7 days";
        var minutes = (int)Math.Floor(span.TotalMinutes);
        if (minutes < 1) return $"{count} samples within a minute";
        if (minutes < 60) return $"{count} samples over {Unit(minutes, "minute")}";
        if (span < TimeSpan.FromDays(2)) return $"{count} samples over {Unit((int)Math.Floor(span.TotalHours), "hour")}";
        return $"{count} samples over {Unit(Math.Min(7, (int)Math.Floor(span.TotalDays)), "day")}";
    }

    private static string Unit(int n, string unit) => n == 1 ? $"1 {unit}" : $"{n} {unit}s";
}
