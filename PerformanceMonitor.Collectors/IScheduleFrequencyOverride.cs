/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #4999: the three columns of a <c>config.config_collector_schedules</c> override row that decide the interval a
/// collector runs at. The service's <c>ScheduleOverride</c> and the viewer's <c>CollectorScheduleRow</c> both carry a
/// row as a record of their own, and both implement this, so one rule
/// (<see cref="CollectorScheduleDefaults.SelectScheduleOverrides{T}"/> and
/// <see cref="CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes{T}"/>) picks the row and resolves the interval for
/// the worker that schedules a collector and for every surface that judges it against that interval.
/// </summary>
public interface IScheduleFrequencyOverride
{
    /// <summary>The server the row is for, or null for the fleet-wide row.</summary>
    int? ServerId { get; }

    /// <summary>The collector the row is for.</summary>
    string CollectorName { get; }

    /// <summary>The row's interval in minutes, or null when the row leaves the interval to the next level.</summary>
    int? FrequencyMinutes { get; }
}
