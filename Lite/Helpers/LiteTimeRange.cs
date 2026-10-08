/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// The Lite side of the shared time range picker (#5562): how a picker value becomes the (hoursBack, fromUtc, toUtc)
/// triple the ~60 tab consumers already take, how it is stored in settings.json, and where its notes get their data.
/// Pure, so the tests drive it without a window.
/// </summary>
internal static class LiteTimeRange
{
    /// <summary>
    /// A FinOps picker (#5562 R5): compact and rolling only, in whole <paramref name="unit"/>s (<see cref="RollingUnitRule.Hour"/> for
    /// a list, <see cref="RollingUnitRule.Day"/> for the heatmap), through the shared control's <c>RollingUnit</c>: the read behind it
    /// is "N units back from now" and could not honor a finished range. The Darling Viewer's FinOps tab sets the same two units.
    /// </summary>
    internal static void ConfigureFinOpsPicker(TimeRangePicker picker, TimeSpan unit)
    {
        picker.Compact = true;
        picker.RollingUnit = unit;
        picker.ZoneProvider = () => ServerTimeHelper.CurrentDisplayZone; /* #5562 M1: the tooltip words the range in the zone the grid beside it uses */
    }

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
    internal static int HoursBackOf(TimeRangePicker picker, int fallbackHours)
    {
        var range = picker.Resolve();
        return range != null ? WindowFor(range).hoursBack : fallbackHours;
    }

    /// <summary>
    /// The window a history read takes: the picker's start and, for a range that has finished, its exclusive end (#5562).
    /// A rolling or live range ('Past 24 hours', 'Today', 'since X') has no end bound (<c>null</c>), so rows collected after
    /// <paramref name="nowUtc"/> are never cut. <paramref name="fallbackHours"/> when the held range cannot be used right now.
    /// </summary>
    internal static (DateTime startUtc, DateTime? endUtc) BoundsOf(TimeRangePicker picker, int fallbackHours, DateTime nowUtc)
    {
        var range = picker.Resolve();
        if (range == null)
        {
            return (nowUtc.AddHours(-fallbackHours), null);
        }

        return BoundsOf(range, nowUtc);
    }

    /// <summary><see cref="BoundsOf(TimeRangePicker, int, DateTime)"/> for a range already resolved.</summary>
    internal static (DateTime startUtc, DateTime? endUtc) BoundsOf(ResolvedTimeRange range, DateTime nowUtc)
    {
        if (range.Spec.WholeHours is { } hours && hours > 0)
        {
            return (nowUtc.AddHours(-hours), null);
        }

        return (range.StartUtc, range.IsLive ? null : range.EndUtc);
    }

    /// <summary>
    /// The longest span Alert History offers (#5562 R8; the old "All" item is gone): 365 days. It covers everything Lite keeps:
    /// the alert log is archived like every signal table and its archive files are deleted by whole month,
    /// <see cref="RetentionService.ArchiveRetentionMonths"/> months back, so rows older than 3 months survive until their month's
    /// file goes, and a 3-month span would hide them. The parser has no year unit, so "1y" cannot be typed; this choice is the way to it.
    /// </summary>
    internal static TimeSpan AlertHistoryLongest { get; } = TimeSpan.FromDays(365);

    /// <summary>The longest choice as the shared control's extra rolling choice (<c>SetLongestChoice</c>), the Viewer's "All" at Lite's reach.</summary>
    internal static TimeRangeSpec AlertHistoryLongestChoice { get; } = TimeRangeSpec.Relative(AlertHistoryLongest);

    /// <summary>
    /// The longest span Job History offers (#5562 R8): 365 days, the old "Last Year", on both desktops. The parser has no year
    /// unit, so this choice is the only way to a year.
    /// </summary>
    internal static TimeRangeSpec JobHistoryLongestChoice { get; } = TimeRangeSpec.Relative(TimeSpan.FromDays(365));

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

    /// <summary>
    /// The collector behind the data a server-tab page shows (#5562 R3): the page's MAIN collector, by the header of the top
    /// tab and of the sub-tab on screen. <c>null</c> for a page with no single sampled source (Overview, Plan Viewer, the
    /// on-load configuration pages, Daily Summary, Collection Health), where the picker shows no sample note.
    /// </summary>
    internal static string? MainCollectorFor(string? tab, string? subTab) => tab switch
    {
        "Wait Stats" => "wait_stats",
        "Queries" => subTab switch
        {
            "Performance Trends" => "query_stats",
            "Active Queries" => "query_snapshots",
            "Top Queries by Duration" => "query_stats",
            "Top Procedures by Duration" => "procedure_stats",
            "Query Store by Duration" => "query_store",
            "Plan Corrections" => "plan_correction",
            "Query Heatmap" => "query_stats",
            _ => null
        },
        "CPU" => "cpu_utilization",
        "Memory" => subTab switch
        {
            "Overview" => "memory_stats",
            "Memory Clerks" => "memory_clerks",
            "Memory Grants" => "memory_grant_stats",
            "Memory Pressure Events" => "memory_pressure_events",
            _ => null
        },
        "File I/O" => "file_io_stats",
        "tempdb" => "tempdb_stats",
        "Blocking" => subTab switch
        {
            "Trends" => "blocked_process_report",
            "Current Waits" => "waiting_tasks",
            "Blocked Process Reports" => "blocked_process_report",
            "Deadlocks" => "deadlocks",
            "Blocking Stats" => "deadlocks",
            _ => null
        },
        "Perfmon" => "perfmon_stats",
        "Running Jobs" => "running_jobs",
        "Latches & Spinlocks" => "latch_stats",
        "CPU Scheduler" => "cpu_scheduler_stats",
        "Plan Cache" => "plan_cache_stats",
        "Session Stats" => "session_stats",
        "System Events" => subTab == "Default Trace" ? "default_trace_events" : "system_health_events",
        _ => null
    };

    /// <summary>The collector a floor probe's relation measures (#5562 R7), the key a banner site feeds the picker's data start under; null for a relation with no scheduled collector.</summary>
    internal static string? CollectorOfRelation(QueryWindowRelation relation) => relation switch
    {
        QueryWindowRelation.QueryStats => "query_stats",
        QueryWindowRelation.ProcedureStats => "procedure_stats",
        QueryWindowRelation.QueryStoreStats => "query_store",
        QueryWindowRelation.PlanCorrection => "plan_correction",
        QueryWindowRelation.BlockedProcessReports => "blocked_process_report",
        QueryWindowRelation.Deadlocks => "deadlocks",
        QueryWindowRelation.DmvBlockingSnapshots => "dmv_blocking_snapshot",
        QueryWindowRelation.QuerySnapshots => "query_snapshots",
        QueryWindowRelation.SystemHealthEvents => "system_health_events",
        QueryWindowRelation.DefaultTraceEvents => "default_trace_events",
        QueryWindowRelation.LongQueryCompletions => "long_query_completions",
        QueryWindowRelation.WaitingTasks => "waiting_tasks",
        QueryWindowRelation.MemoryPressureEvents => "memory_pressure_events",
        QueryWindowRelation.JobHistory => "job_history",
        QueryWindowRelation.WaitStats => "wait_stats",
        _ => null
    };

    /// <summary>
    /// A collector's actual cadence for the sample note: its schedule on this server when it has one (a disabled collector
    /// samples nothing, so no note), else the shipped default (<see cref="CollectorScheduleDefaults"/>). <c>null</c> for no
    /// collector, an on-load one, or one that ships off and was never scheduled.
    /// </summary>
    internal static TimeSpan? SampleIntervalForCollector(string? collector, CollectorSchedule? schedule)
    {
        if (collector is null)
        {
            return null;
        }

        if (schedule is not null)
        {
            return schedule.Enabled ? SampleIntervalFor(schedule.FrequencyMinutes) : null;
        }

        return CollectorScheduleDefaults.All.TryGetValue(collector, out var entry) && entry.DefaultEnabled
            ? SampleIntervalFor(entry.FrequencyMinutes)
            : null;
    }

    /// <summary>A collector's actual cadence as the picker's sample interval; null for a collector that does not run on a schedule (0 = on load).</summary>
    internal static TimeSpan? SampleIntervalFor(int? frequencyMinutes) =>
        frequencyMinutes is > 0 ? TimeSpan.FromMinutes(frequencyMinutes.Value) : null;
}
