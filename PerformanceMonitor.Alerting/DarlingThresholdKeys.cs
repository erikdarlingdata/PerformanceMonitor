/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The tunable thresholds Darling can hold an override for (#5916): one row per wire name, which is the name
/// <c>get_alert_settings</c> and <c>update_alert_settings</c> already use for the global value (for example
/// <c>cpu.threshold_percent</c>). Every registry key of #5910 (<see cref="AlertThresholdKey"/>) is here, plus the two
/// PostgreSQL count thresholds, which are Darling's own and are not on the engine's settings contract. PostgreSQL
/// predictor constants are not settings and have no entry. The bounds are the ones <c>update_alert_settings</c> and
/// the Viewer's Settings window enforce on the global value, so an override can never hold a value the global setting
/// would refuse.
/// </summary>
public static class DarlingThresholdKeys
{
    /// <summary>One tunable threshold: its wire name, the engine key (null for a PostgreSQL count threshold), the scope it
    /// can take, its bounds, its <c>config_alert_settings</c> column and its off end. The off end is the one value inside the
    /// bounds at which the alert can no longer fire: the top of the range where a higher value means fewer alerts (the counts,
    /// minutes and percent bars), zero for the four Low Disk bars where a lower value means fewer alerts, the shortest window
    /// for the Failed Job look-back. It is null where no value turns the alert off (the File Growth rate window only averages).
    /// An agent is never allowed to set a threshold to its off end (<c>threshold_disables_alert</c>); that is the whole rule,
    /// so a very high count that is still reachable is allowed.</summary>
    public sealed record Entry(string Wire, AlertThresholdKey? Engine, AlertThresholdScope Scope, int Min, int Max, string Column, int? OffEnd);

    public const string PgBlockingCount = "blocking.pg_count_threshold";
    public const string PgDeadlockCount = "deadlocks.pg_count_threshold";

    /// <summary>The Identity Capacity alert's two bars, percent used (#6875). Darling's own, so no engine key. A per-server or per-database override
    /// (the database is the column's database) replaces the global bar for that scope.</summary>
    public const string IdentityCapacityWarning = "identity_capacity.warning_percent";
    public const string IdentityCapacityCritical = "identity_capacity.critical_percent";

    private static readonly Entry[] All =
    [
        new("cpu.threshold_percent", AlertThresholdKey.CpuThresholdPercent, AlertThresholdScope.Server, 1, 100, "cpu_threshold_percent", 100),
        new("blocking.count_threshold", AlertThresholdKey.BlockingCountThreshold, AlertThresholdScope.Server, PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue, "blocking_count_threshold", int.MaxValue),
        new("blocking.wait_threshold_seconds", AlertThresholdKey.BlockingWaitSecondsThreshold, AlertThresholdScope.Server, 0, int.MaxValue, "blocking_wait_seconds_threshold", int.MaxValue),
        new("deadlocks.count_threshold", AlertThresholdKey.DeadlockCountThreshold, AlertThresholdScope.Server, PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue, "deadlock_count_threshold", int.MaxValue),
        new("runnable_queue.warning_per_scheduler", AlertThresholdKey.RunnableQueueWarningPerScheduler, AlertThresholdScope.Server, 1, int.MaxValue, "runnable_queue_warning_per_scheduler", int.MaxValue),
        new("runnable_queue.critical_per_scheduler", AlertThresholdKey.RunnableQueueCriticalPerScheduler, AlertThresholdScope.Server, 1, int.MaxValue, "runnable_queue_critical_per_scheduler", int.MaxValue),
        new("client_timeouts.warning_per_minute", AlertThresholdKey.ClientTimeoutsWarningPerMinute, AlertThresholdScope.Server, 1, int.MaxValue, "client_timeouts_warning_per_minute", int.MaxValue),
        new("client_timeouts.critical_per_minute", AlertThresholdKey.ClientTimeoutsCriticalPerMinute, AlertThresholdScope.Server, 1, int.MaxValue, "client_timeouts_critical_per_minute", int.MaxValue),
        new("long_running_query.threshold_minutes", AlertThresholdKey.LongRunningQueryThresholdMinutes, AlertThresholdScope.Server, 1, int.MaxValue, "long_running_query_threshold_minutes", int.MaxValue),
        new("tempdb_space.threshold_percent", AlertThresholdKey.TempDbSpaceThresholdPercent, AlertThresholdScope.Server, 1, 100, "tempdb_space_threshold_percent", 100),
        new("low_disk.threshold_percent", AlertThresholdKey.LowDiskThresholdPercent, AlertThresholdScope.Server, 0, 100, "low_disk_threshold_percent", 0),
        new("low_disk.threshold_gb", AlertThresholdKey.LowDiskThresholdGb, AlertThresholdScope.Server, 0, int.MaxValue, "low_disk_threshold_gb", 0),
        new("low_disk.critical_free_percent", AlertThresholdKey.DiskCriticalFreePercent, AlertThresholdScope.Server, 0, 100, "disk_critical_free_percent", 0),
        new("low_disk.critical_free_gb", AlertThresholdKey.DiskCriticalFreeGb, AlertThresholdScope.Server, 0, int.MaxValue, "disk_critical_free_gb", 0),
        new("pvs.threshold_percent", AlertThresholdKey.PvsThresholdPercent, AlertThresholdScope.ServerAndDatabase, 0, 100, "pvs_threshold_percent", 100),
        new("pvs.floor_gb", AlertThresholdKey.PvsFloorGb, AlertThresholdScope.ServerAndDatabase, 0, int.MaxValue, "pvs_floor_gb", int.MaxValue),
        new("file_growth.rise_mb", AlertThresholdKey.FileGrowthRiseMb, AlertThresholdScope.ServerAndDatabase, 0, int.MaxValue, "file_growth_rise_mb", int.MaxValue),
        new("file_growth.volume_percent", AlertThresholdKey.FileGrowthVolumePercent, AlertThresholdScope.ServerAndDatabase, 0, 100, "file_growth_volume_percent", 100),
        new("file_growth.lookback_minutes", AlertThresholdKey.FileGrowthLookbackMinutes, AlertThresholdScope.Server, 5, 1440, "file_growth_lookback_minutes", null),
        new("long_running_job.multiplier", AlertThresholdKey.LongRunningJobMultiplier, AlertThresholdScope.Server, 2, 20, "long_running_job_multiplier", 20),
        new("failed_job.lookback_minutes", AlertThresholdKey.FailedJobLookbackMinutes, AlertThresholdScope.Server, 1, 1440, "failed_job_lookback_minutes", 1),
        new(PgBlockingCount, null, AlertThresholdScope.Server, PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue, "pg_blocking_count_threshold", int.MaxValue),
        new(PgDeadlockCount, null, AlertThresholdScope.Server, PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue, "pg_deadlock_count_threshold", int.MaxValue),
        new(IdentityCapacityWarning, null, AlertThresholdScope.ServerAndDatabase, 1, 100, "identity_capacity_warning_percent", 100),
        new(IdentityCapacityCritical, null, AlertThresholdScope.ServerAndDatabase, 1, 100, "identity_capacity_critical_percent", 100),
    ];

    private static readonly Dictionary<string, Entry> ByWire = BuildByWire();
    private static readonly Dictionary<AlertThresholdKey, Entry> ByEngine = BuildByEngine();

    private static Dictionary<string, Entry> BuildByWire()
    {
        var map = new Dictionary<string, Entry>(StringComparer.Ordinal);
        foreach (var e in All)
            map[e.Wire] = e;
        return map;
    }

    private static Dictionary<AlertThresholdKey, Entry> BuildByEngine()
    {
        var map = new Dictionary<AlertThresholdKey, Entry>();
        foreach (var e in All)
        {
            if (e.Engine is { } k)
                map[k] = e;
        }

        return map;
    }

    /// <summary>Every tunable threshold, in display order.</summary>
    public static IReadOnlyList<Entry> Entries => All;

    public static bool TryGet(string? wire, out Entry entry)
    {
        entry = null!;
        return wire is not null && ByWire.TryGetValue(wire.Trim(), out entry!);
    }

    public static Entry For(AlertThresholdKey key) => ByEngine[key];

    /// <summary>
    /// Why a key cannot be overridden, or null when it is a tunable threshold. The reasons name the class of key so an
    /// operator or agent sees why: an override moves a threshold, never a switch, a cadence or a destination.
    /// </summary>
    public static (string Reason, string Message)? RefusalFor(string? key)
    {
        var k = (key ?? string.Empty).Trim();
        if (k.Length == 0)
            return ("key_required", "key is required: a threshold such as cpu.threshold_percent (see get_alert_settings with server_name).");
        if (TryGet(k, out _))
            return null;
        if (k.StartsWith("self_alerts.", StringComparison.OrdinalIgnoreCase))
            return ("not_tunable", $"'{k}' is a self-monitoring setting (self_alerts.*) about the monitor itself, not a monitored server. It has no per-server override.");
        if (k.EndsWith(".enabled", StringComparison.OrdinalIgnoreCase) || string.Equals(k, "enabled", StringComparison.OrdinalIgnoreCase))
            return ("not_tunable", $"'{k}' is an enable flag. An override changes a threshold, never turns an alert on or off; mute the alert to silence it.");
        if (k.Contains("cooldown", StringComparison.OrdinalIgnoreCase))
            return ("not_tunable", $"'{k}' is a cooldown. It spaces notifications and has no per-server override.");
        if (k.Contains("route", StringComparison.OrdinalIgnoreCase) || k.StartsWith("delivery.", StringComparison.OrdinalIgnoreCase))
            return ("not_tunable", $"'{k}' is a route or delivery setting. It has no per-server override.");
        return ("not_in_registry", $"'{k}' is not a tunable threshold. See get_alert_settings with server_name for the keys that can be overridden.");
    }

    /// <summary>Why <paramref name="value"/> or <paramref name="database"/> is refused for <paramref name="entry"/>, or null.</summary>
    public static (string Reason, string Message)? ValidateValue(Entry entry, int value, string? database)
    {
        if (value < entry.Min || value > entry.Max)
        {
            var max = entry.Max == int.MaxValue ? "2147483647" : entry.Max.ToString(CultureInfo.InvariantCulture);
            return ("out_of_bounds", $"'{entry.Wire}' must be an integer between {entry.Min.ToString(CultureInfo.InvariantCulture)} and {max}, the same bounds the global setting has.");
        }

        if (!string.IsNullOrWhiteSpace(database) && entry.Scope != AlertThresholdScope.ServerAndDatabase)
            return ("database_scope", $"'{entry.Wire}' is compared against a server-level figure, so it takes a server override only, not a database_name.");
        return null;
    }
}
