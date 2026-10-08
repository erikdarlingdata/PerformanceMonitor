/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// The Lite side of the shared time range picker (#5562): how a picker value becomes the (hoursBack, fromUtc, toUtc)
/// triple the ~60 tab consumers already take, how it is stored in settings.json, and where its notes get their data.
/// Pure, so the tests drive it without a window.
/// </summary>
internal static class LiteTimeRange
{
    /// <summary>The range a fresh install opens on: the old default of four hours.</summary>
    internal static TimeRangeSpec Default => TimeRangeSpec.Relative(TimeSpan.FromHours(4));

    /// <summary>
    /// The window a refresh reads. A rolling range of a whole number of hours stays (hours, null, null) exactly as the
    /// old presets were: charts fall back to now - hoursBack. Everything else (a sub-hour span, 'Today', a typed range,
    /// 'since') carries its two instants, and <c>hoursBack</c> is the whole hours from the start to now rounded UP (at
    /// least 1), so a reader that only takes hours back (a history window opened from a row) still covers the range.
    /// </summary>
    internal static (int hoursBack, DateTime? fromUtc, DateTime? toUtc) WindowFor(ResolvedTimeRange range)
    {
        if (range.Spec.WholeHours is { } hours && hours > 0)
        {
            return (hours, null, null);
        }

        return (HoursBackFor(range), range.StartUtc, range.EndUtc);
    }

    /// <summary>Whole hours from the range's start to the moment it was resolved, rounded up, at least 1.</summary>
    internal static int HoursBackFor(ResolvedTimeRange range)
    {
        var hours = Math.Ceiling((range.NowUtc - range.StartUtc).TotalHours);
        return hours < 1 ? 1 : hours > int.MaxValue ? int.MaxValue : (int)hours;
    }

    /// <summary>
    /// Hours back for a reader that only takes hours (the Alert and Job History reads, the FinOps lists): the picker's
    /// range through <see cref="WindowFor"/>. A calendar period or typed range is covered from its start to now, so a
    /// read can return rows newer than a range that ended earlier. <paramref name="fallbackHours"/> when the held range
    /// cannot be used at this moment ('Today' in the first minutes after midnight).
    /// </summary>
    internal static int HoursBackOf(TimeRangePicker picker, int fallbackHours) =>
        picker.Resolve() is { } range ? WindowFor(range).hoursBack : fallbackHours;

    /// <summary>True when <paramref name="range"/> carries its own instants rather than 'the last N hours'.</summary>
    internal static bool HasExplicitInstants(ResolvedTimeRange range) => range.Spec.WholeHours is not > 0;

    /// <summary>
    /// The range settings.json holds: the new <c>default_time_range</c> text when it names a preset or a calendar period,
    /// else the legacy <c>default_time_range_hours</c> mapped with <see cref="TimeRangePresets.FromLegacyHours"/>, else four
    /// hours. A typed range (fixed or 'since') is never taken from a file: it is not persisted.
    /// </summary>
    internal static TimeRangeSpec FromSettings(string? rangeId, int legacyHours)
    {
        if (TimeRangeSpec.TryFromId(rangeId, out var spec) && spec is { } s && IsPersistable(s))
        {
            return s;
        }

        return TimeRangePresets.FromLegacyHours(legacyHours) is { } legacy ? legacy : Default;
    }

    /// <summary>A drill window under the model's 5-minute floor, centred and widened to it; anything longer is returned as it was.</summary>
    internal static (DateTime fromUtc, DateTime toUtc) AtLeastMinimumSpan(DateTime fromUtc, DateTime toUtc)
    {
        if (toUtc - fromUtc >= TimeRangeSpec.MinimumSpan)
        {
            return (fromUtc, toUtc);
        }

        var mid = fromUtc + (toUtc - fromUtc) / 2;
        return (mid - TimeRangeSpec.MinimumSpan / 2, mid + TimeRangeSpec.MinimumSpan / 2);
    }

    /// <summary>A preset or calendar period persists; a typed fixed or 'since' range does not (a window that ended two days ago is worse than none).</summary>
    internal static bool IsPersistable(TimeRangeSpec spec) =>
        spec.Kind is TimeRangeKind.Relative or TimeRangeKind.Calendar && (spec.Kind != TimeRangeKind.Relative || spec.Span >= TimeRangeSpec.MinimumSpan);

    /// <summary>
    /// What to write for <paramref name="spec"/>: a whole-hour rolling range goes to the legacy hours key (older builds read
    /// it) and clears the new key; any other persistable range goes to the new key. <c>(null, null)</c> for a range that is not persisted.
    /// </summary>
    internal static (string? rangeId, int? hours) SettingsFor(TimeRangeSpec spec)
    {
        if (!IsPersistable(spec))
        {
            return (null, null);
        }

        return spec.WholeHours is { } h && h > 0 ? (null, h) : (spec.Id, null);
    }

    /// <summary>
    /// Where the picker's 'Data starts' note takes its start from: the coverage probe's floor the tab already awaited, else
    /// the oldest instant the archive keeps (<see cref="RetentionService.OldestRetainedInstant"/>). No new query.
    /// </summary>
    internal static DateTime DataStartFor(DateTime? probedFloorUtc, DateTime utcNow) =>
        probedFloorUtc ?? RetentionService.OldestRetainedInstant(utcNow);

    /// <summary>A collector's actual cadence as the picker's sample interval; null for a collector that does not run on a schedule (0 = on load).</summary>
    internal static TimeSpan? SampleIntervalFor(int? frequencyMinutes) =>
        frequencyMinutes is > 0 ? TimeSpan.FromMinutes(frequencyMinutes.Value) : null;
}
