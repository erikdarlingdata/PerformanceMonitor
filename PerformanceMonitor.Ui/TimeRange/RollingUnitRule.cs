/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Which ranges a "rolling only" picker admits (#5562 R5). A read that takes "N hours (or days) back from now" can
/// honor only a length counted back from now, in whole units, of at least one unit. A calendar period, a fixed range
/// or a "since" range would read through now, and 90 minutes would read as 2 hours, so the picker must never offer
/// them. Pure, so a test pins the rule without a window.
/// </summary>
public static class RollingUnitRule
{
    /// <summary>The one hour unit of the FinOps lists.</summary>
    public static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    /// <summary>The one day unit of the FinOps object heatmap.</summary>
    public static readonly TimeSpan Day = TimeSpan.FromDays(1);

    /// <summary>Why <paramref name="spec"/> is not a range a read in whole <paramref name="unit"/>s can honor, or <c>null</c> when it is.</summary>
    public static string? Refusal(TimeRangeSpec spec, TimeSpan unit)
    {
        ArgumentNullException.ThrowIfNull(spec);
        var name = unit == Day ? "day" : "hour";
        if (spec.Kind != TimeRangeKind.Relative)
        {
            return "This read takes a length back from now, such as " + (unit == Day ? "7d or 30 days" : "4h or 3 days") + ".";
        }

        if (spec.Span < unit)
        {
            return "The shortest range for this read is 1 " + name + ".";
        }

        if (spec.Span.Ticks % unit.Ticks != 0)
        {
            var whole = TimeSpan.FromTicks((spec.Span.Ticks / unit.Ticks + 1) * unit.Ticks);
            return "This read takes whole " + name + "s; try " + TimeRangeSpec.FormatLength(whole) + ".";
        }

        return null;
    }

    /// <summary>True when a read in whole <paramref name="unit"/>s can honor <paramref name="spec"/>.</summary>
    public static bool Admits(TimeRangeSpec spec, TimeSpan unit) => Refusal(spec, unit) is null;
}
