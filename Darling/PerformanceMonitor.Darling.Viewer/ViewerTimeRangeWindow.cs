/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The pure half of the Viewer's time range (#5562): how a <see cref="TimeRangeSpec"/> the shared picker holds becomes
/// the naive-UTC window every inner tab reads, the whole hours the few integer readers still take, the legacy
/// preset index old preference files hold, and which collector a tab's chart is fed by (for
/// the picker's "collected every N minutes" note). Nothing here reads the clock, the zone or a control, so a test pins
/// each rule without a window (a <c>ViewerServerTab</c> needs the app's resources and cannot be built in a test).
/// </summary>
internal static class ViewerTimeRangeWindow
{
    /// <summary>The window a range that cannot be resolved right now (a 'since' start that has not happened yet; a calendar
    /// period is exempt from the 5-minute floor) falls back to: the Viewer's historical 24 hours, so no surface is ever window-less.</summary>
    internal static readonly TimeSpan FallbackSpan = TimeSpan.FromHours(24);

    /// <summary>The window to read now: the range's start and end as naive UTC, and whether the end slides with the
    /// clock. A live range (past N, since X, today) ends at <paramref name="nowUtc"/>; a fixed or finished calendar
    /// period keeps its instants. No upper bound is applied: a long period reads what the data holds (#5562 R1).</summary>
    internal static (DateTime StartUtc, DateTime EndUtc, bool IsLive) Window(TimeRangeSpec spec, DateTime nowUtc, TimeZoneInfo zone)
    {
        ArgumentNullException.ThrowIfNull(spec);
        ArgumentNullException.ThrowIfNull(zone);
        if (spec.TryResolve(nowUtc, zone, out var range, out _) && range is not null)
        {
            return (range.StartUtc, range.EndUtc, range.IsLive);
        }

        return (nowUtc - FallbackSpan, nowUtc, true);
    }

    /// <summary>The whole hours a reader that still takes "hours back" is given for a window: rounded up, at least one,
    /// so a 45-minute window reads as 1 hour rather than 0. A reader that can take the window's two instants should
    /// (the Overview lanes do); this is for the ones that cannot.</summary>
    internal static int HoursBack(DateTime startUtc, DateTime endUtc)
    {
        var hours = (int)Math.Ceiling((endUtc - startUtc).TotalHours);
        return hours < 1 ? 1 : hours;
    }

    /// <summary>Legacy preset index (the old combo and the old preference file: 0=1h, 1=4h, 2=12h, 3=24h, 4=7d) to
    /// hours back. Index 5 (Custom) and any stray value fall to the 24-hour default. Pure and static so the mapping is
    /// unit-testable.</summary>
    internal static int LegacyIndexToHours(int index) => index switch
    {
        0 => 1,
        1 => 4,
        2 => 12,
        3 => 24,
        4 => 168,
        _ => 24,
    };

    /// <summary>The range a legacy preset index stands for: 1h, 4h, 12h, 1d, 1w (24 hours is "1d" and 168 is "1w").</summary>
    internal static TimeRangeSpec FromLegacyIndex(int index)
        => TimeRangePresets.FromLegacyHours(LegacyIndexToHours(index)) ?? TimeRangeSpec.Relative(FallbackSpan);

    /// <summary>The legacy index nearest a range, so a preference file written now still opens on an older build:
    /// 1h=0, 4h=1, 12h=2, 1d=3, 1w=4, and the historical default (3) for anything the old combo could not hold.</summary>
    internal static int ToLegacyIndex(TimeRangeSpec spec)
        => spec.Id switch
        {
            "1h" => 0,
            "4h" => 1,
            "12h" => 2,
            "1d" => 3,
            "1w" => 4,
            _ => ViewerPreferences.DefaultTimeRangeIndexValue,
        };

    /// <summary>The collector that feeds the first chart of a top-level inner tab, by the tab's header, or null for a tab
    /// that has no single main collector (it then shows no sample-interval note). Names are
    /// <see cref="CollectorScheduleDefaults"/> keys.</summary>
    internal static string? MainCollectorFor(string? innerTabHeader) => innerTabHeader switch
    {
        "Overview" => "cpu_utilization",
        "Wait Stats" => "wait_stats",
        "Queries" => "query_stats",
        "CPU" => "cpu_utilization",
        "Memory" => "memory_stats",
        "File I/O" => "file_io_stats",
        "tempdb" => "tempdb_stats",
        "Blocking" => "blocked_process_report",
        _ => null,
    };

    /// <summary>The collector behind the page on screen (#5562 L3): the top-level tab's header and, for a tab that holds sub-tabs, the
    /// selected sub-tab's, as Lite's <c>LiteTimeRange.MainCollectorFor</c> does, so Queries &gt; Query Store names the 5-minute
    /// collector and Blocking &gt; Current Waits the waiting-tasks one. A sub-tab with no entry here, or none, falls back to the
    /// top-level tab's collector (<see cref="MainCollectorFor(string?)"/>).</summary>
    internal static string? MainCollectorFor(string? innerTabHeader, string? subTabHeader)
    {
        var bySubTab = innerTabHeader switch
        {
            "Queries" => subTabHeader switch
            {
                "Performance Trends" => "query_stats",
                "Active Queries" => "query_snapshots",
                "Top Queries by Duration" => "query_stats",
                "Top Procedures by Duration" => "procedure_stats",
                "Query Store by Duration" => "query_store",
                "Plan Corrections" => "plan_correction",
                "Query Heatmap" => "query_stats",
                _ => null,
            },
            "CPU" => subTabHeader == "CPU Scheduler" ? "cpu_scheduler_stats" : null,
            "Memory" => subTabHeader switch
            {
                "Memory Clerks" => "memory_clerks",
                "Memory Grants" => "memory_grant_stats",
                "Plan Cache" => "plan_cache_stats",
                "Memory Pressure Events" => "memory_pressure_events",
                _ => null,
            },
            "Blocking" => subTabHeader switch
            {
                "Current Waits" => "waiting_tasks",
                "Deadlocks" => "deadlocks",
                "Blocking Stats" => "deadlocks",
                _ => null,
            },
            _ => null,
        };

        return bySubTab ?? MainCollectorFor(innerTabHeader);
    }

    /// <summary>How often <paramref name="collector"/> actually runs on this server: its fleet or per-server schedule
    /// override if there is one, else the shipped default (<see cref="CollectorScheduleDefaults"/>). Null for a collector
    /// that is not scheduled on a cadence (on load) or is not known, so the picker shows no note rather than a wrong one.</summary>
    internal static TimeSpan? SampleIntervalFor(string? collector, int serverId, IEnumerable<CollectorScheduleRow>? overrides)
    {
        if (string.IsNullOrEmpty(collector))
        {
            return null;
        }

        var minutes = CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(collector, serverId, overrides);
        return minutes is > 0 ? TimeSpan.FromMinutes(minutes.Value) : null;
    }

    /// <summary>The longest choice Alert History offers (#5562 R8), the "All" the old list had: a span equal to what the alert
    /// table keeps, <see cref="DarlingRetentionHorizons.AlertHistoryRetentionDays"/> (90 days; <c>config_alert_log</c> is purged
    /// past it), and never longer than a year when that retention is raised. A longer range would only read the same rows.</summary>
    internal static TimeRangeSpec AlertHistoryLongestChoice { get; } =
        TimeRangeSpec.Relative(TimeSpan.FromDays(Math.Min(DarlingRetentionHorizons.AlertHistoryRetentionDays, 365)));

    /// <summary>The longest choice Job History offers (#5562 R8): 365 days, the old "Last Year", the same on Lite. The parser has no
    /// year unit, so this choice is the way to a year.</summary>
    internal static TimeRangeSpec JobHistoryLongestChoice { get; } = TimeRangeSpec.Relative(TimeSpan.FromDays(365));

    /// <summary>The range a newly-opened tab starts on, from the persisted preference (a string id wins; the legacy
    /// index stands in for a file written before the string existed).</summary>
    internal static TimeRangeSpec DefaultFor(ViewerPreferences preferences)
    {
        ArgumentNullException.ThrowIfNull(preferences);
        return TimeRangeSpec.TryFromId(preferences.DefaultTimeRange, out var spec) && spec is not null && IsPersistable(spec)
            ? spec
            : FromLegacyIndex(preferences.DefaultTimeRangeIndex);
    }

    /// <summary>True for a range that makes sense as a standing default: a rolling length or a calendar period. A fixed
    /// or since range names concrete instants and stays unpersisted (as Custom always has).</summary>
    internal static bool IsPersistable(TimeRangeSpec spec)
        => spec.Kind is TimeRangeKind.Relative or TimeRangeKind.Calendar;
}
