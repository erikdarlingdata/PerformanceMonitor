/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Alerting;

/// <summary>Where an <see cref="AlertThresholdKey"/> can be overridden (#5910).</summary>
public enum AlertThresholdScope
{
    /// <summary>Per server only: the check compares one server-level value against the threshold.</summary>
    Server,

    /// <summary>
    /// Per server, and per database on that server: the check compares each database's own row against the
    /// threshold, so a database can carry its own value.
    /// </summary>
    ServerAndDatabase
}

/// <summary>
/// The registry of tunable alert thresholds (#5910): one key per tunable integer member of
/// <see cref="IAlertEngineSettings"/>, named exactly as that member. <see cref="AlertEngine"/> reads every one of
/// them through <see cref="IAlertThresholdOverrides.Resolve"/>; every other member of the settings is on
/// <see cref="AlertThresholdKeys.NotTunable"/> with its reason. The two lists divide the settings by hand: nothing
/// enforces that a new member lands on one of them, and each key's resolution is pinned by a behaviour test, which
/// moves one server's outcome with an override and leaves another's alone.
/// </summary>
public enum AlertThresholdKey
{
    CpuThresholdPercent,
    BlockingCountThreshold,
    BlockingWaitSecondsThreshold,
    DeadlockCountThreshold,
    RunnableQueueWarningPerScheduler,
    RunnableQueueCriticalPerScheduler,
    ClientTimeoutsWarningPerMinute,
    ClientTimeoutsCriticalPerMinute,
    LongRunningQueryThresholdMinutes,
    TempDbSpaceThresholdPercent,
    LowDiskThresholdPercent,
    LowDiskThresholdGb,
    DiskCriticalFreePercent,
    DiskCriticalFreeGb,
    PvsThresholdPercent,
    PvsFloorGb,
    FileGrowthRiseMb,
    FileGrowthVolumePercent,
    FileGrowthLookbackMinutes,
    LongRunningJobMultiplier,
    FailedJobLookbackMinutes
}

/// <summary>The scope of each <see cref="AlertThresholdKey"/>, the settings members that are not tunable, and the
/// one place the engine reads a key's global value (#5910).</summary>
public static class AlertThresholdKeys
{
    /// <summary>
    /// The scope each key can take. Database scope is given only where the engine compares the threshold against
    /// one database's row: the version store (PVS) alert grades each ADR database's version store, and the file
    /// growth alert grades each database file. Every other check compares a server-level figure: a count across
    /// all databases (blocking, deadlocks), a server or volume reading (CPU, runnable queue, client timeouts,
    /// tempdb, volume free space), or a value the read itself filters on before any database is seen (the
    /// long-running query and job thresholds, and both lookback windows).
    /// </summary>
    public static IReadOnlyDictionary<AlertThresholdKey, AlertThresholdScope> Scopes { get; } =
        new Dictionary<AlertThresholdKey, AlertThresholdScope>
        {
            [AlertThresholdKey.CpuThresholdPercent] = AlertThresholdScope.Server,
            [AlertThresholdKey.BlockingCountThreshold] = AlertThresholdScope.Server,
            [AlertThresholdKey.BlockingWaitSecondsThreshold] = AlertThresholdScope.Server,
            [AlertThresholdKey.DeadlockCountThreshold] = AlertThresholdScope.Server,
            [AlertThresholdKey.RunnableQueueWarningPerScheduler] = AlertThresholdScope.Server,
            [AlertThresholdKey.RunnableQueueCriticalPerScheduler] = AlertThresholdScope.Server,
            [AlertThresholdKey.ClientTimeoutsWarningPerMinute] = AlertThresholdScope.Server,
            [AlertThresholdKey.ClientTimeoutsCriticalPerMinute] = AlertThresholdScope.Server,
            [AlertThresholdKey.LongRunningQueryThresholdMinutes] = AlertThresholdScope.Server,
            [AlertThresholdKey.TempDbSpaceThresholdPercent] = AlertThresholdScope.Server,
            [AlertThresholdKey.LowDiskThresholdPercent] = AlertThresholdScope.Server,
            [AlertThresholdKey.LowDiskThresholdGb] = AlertThresholdScope.Server,
            [AlertThresholdKey.DiskCriticalFreePercent] = AlertThresholdScope.Server,
            [AlertThresholdKey.DiskCriticalFreeGb] = AlertThresholdScope.Server,
            [AlertThresholdKey.PvsThresholdPercent] = AlertThresholdScope.ServerAndDatabase,
            [AlertThresholdKey.PvsFloorGb] = AlertThresholdScope.ServerAndDatabase,
            [AlertThresholdKey.FileGrowthRiseMb] = AlertThresholdScope.ServerAndDatabase,
            [AlertThresholdKey.FileGrowthVolumePercent] = AlertThresholdScope.ServerAndDatabase,
            [AlertThresholdKey.FileGrowthLookbackMinutes] = AlertThresholdScope.Server,
            [AlertThresholdKey.LongRunningJobMultiplier] = AlertThresholdScope.Server,
            [AlertThresholdKey.FailedJobLookbackMinutes] = AlertThresholdScope.Server,
        };

    /// <summary>
    /// Every member of <see cref="IAlertEngineSettings"/> that is not a key, with the reason it is not tunable per
    /// server. Route keys are not here because they are not on this contract: alert routing lives in each host's
    /// own settings, outside the engine.
    /// </summary>
    public static IReadOnlyDictionary<string, string> NotTunable { get; } = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        /* Integer members. */
        [nameof(IAlertEngineSettings.CooldownMinutes)] = "A cooldown: it spaces repeat notifications and decides no fire.",
        [nameof(IAlertEngineSettings.PoisonWaitThresholdMs)] = "Retired as a threshold by #3539: the engine no longer reads it.",
        [nameof(IAlertEngineSettings.LongRunningQueryMaxResults)] = "A read cap: it bounds how many sessions the card lists and decides no fire.",
        [nameof(IAlertEngineSettings.SelfDiskFreeWarnPercent)] = "Self-monitoring (self_alerts.*): the monitor's own store volume, not a monitored server.",
        [nameof(IAlertEngineSettings.SelfDiskFreeWarnGb)] = "Self-monitoring (self_alerts.*): the monitor's own store volume, not a monitored server.",
        [nameof(IAlertEngineSettings.CollectionStaleMinutes)] = "Self-monitoring (self_alerts.*): the monitor's own collection health.",
        [nameof(IAlertEngineSettings.CollectionFailureThreshold)] = "Self-monitoring (self_alerts.*): the monitor's own collection health.",

        /* Enable flags. */
        [nameof(IAlertEngineSettings.AlertsEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.CpuEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.BlockingEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.DeadlockEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.PoisonWaitEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.RunnableQueueEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.ClientTimeoutsEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.LongRunningQueryEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.TempDbSpaceEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.LowDiskEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.LongRunningJobEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.FailedJobEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.PvsEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.FileGrowthEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.DatabaseStateEnabled)] = "An enable flag, not a threshold.",
        [nameof(IAlertEngineSettings.ForcePlanFailureEnabled)] = "An enable flag, not a threshold.",

        /* Non-integer members. */
        [nameof(IAlertEngineSettings.LongRunningQueryExcludeSpServerDiagnostics)] = "Not an integer: a long-running query read filter.",
        [nameof(IAlertEngineSettings.LongRunningQueryExcludeWaitFor)] = "Not an integer: a long-running query read filter.",
        [nameof(IAlertEngineSettings.LongRunningQueryExcludeBackups)] = "Not an integer: a long-running query read filter.",
        [nameof(IAlertEngineSettings.LongRunningQueryExcludeMiscWaits)] = "Not an integer: a long-running query read filter.",
        [nameof(IAlertEngineSettings.LongRunningQueryExcludeCdc)] = "Not an integer: a long-running query read filter.",
        [nameof(IAlertEngineSettings.LongRunningQueryExcludedProgramNamePrefixes)] = "Not an integer: a long-running query exclusion list.",
        [nameof(IAlertEngineSettings.LongRunningQueryExcludedLogins)] = "Not an integer: a long-running query exclusion list.",
        [nameof(IAlertEngineSettings.ExcludedDatabases)] = "Not an integer: the excluded-database list.",
        [nameof(IAlertEngineSettings.DeadlockRateThresholds)] = "Not an integer: the deadlock rate tiers grade a fire and decide none.",
        [nameof(IAlertEngineSettings.CpuAlertMode)] = "Not an integer: which CPU figure is compared.",
    };

    /// <summary>The scope <paramref name="key"/> can take.</summary>
    public static AlertThresholdScope ScopeOf(AlertThresholdKey key) =>
        Scopes.TryGetValue(key, out var scope)
            ? scope
            : throw new ArgumentOutOfRangeException(nameof(key), key, "Not in the threshold registry.");

    /// <summary>
    /// The global value of <paramref name="key"/>: the member of <paramref name="settings"/> the key names. The
    /// engine reads a tunable member only through this switch, so a census test can hold that it never reads one
    /// around <see cref="IAlertThresholdOverrides.Resolve"/>.
    /// </summary>
    public static int GlobalValue(this IAlertEngineSettings settings, AlertThresholdKey key) => key switch
    {
        AlertThresholdKey.CpuThresholdPercent => settings.CpuThresholdPercent,
        AlertThresholdKey.BlockingCountThreshold => settings.BlockingCountThreshold,
        AlertThresholdKey.BlockingWaitSecondsThreshold => settings.BlockingWaitSecondsThreshold,
        AlertThresholdKey.DeadlockCountThreshold => settings.DeadlockCountThreshold,
        AlertThresholdKey.RunnableQueueWarningPerScheduler => settings.RunnableQueueWarningPerScheduler,
        AlertThresholdKey.RunnableQueueCriticalPerScheduler => settings.RunnableQueueCriticalPerScheduler,
        AlertThresholdKey.ClientTimeoutsWarningPerMinute => settings.ClientTimeoutsWarningPerMinute,
        AlertThresholdKey.ClientTimeoutsCriticalPerMinute => settings.ClientTimeoutsCriticalPerMinute,
        AlertThresholdKey.LongRunningQueryThresholdMinutes => settings.LongRunningQueryThresholdMinutes,
        AlertThresholdKey.TempDbSpaceThresholdPercent => settings.TempDbSpaceThresholdPercent,
        AlertThresholdKey.LowDiskThresholdPercent => settings.LowDiskThresholdPercent,
        AlertThresholdKey.LowDiskThresholdGb => settings.LowDiskThresholdGb,
        AlertThresholdKey.DiskCriticalFreePercent => settings.DiskCriticalFreePercent,
        AlertThresholdKey.DiskCriticalFreeGb => settings.DiskCriticalFreeGb,
        AlertThresholdKey.PvsThresholdPercent => settings.PvsThresholdPercent,
        AlertThresholdKey.PvsFloorGb => settings.PvsFloorGb,
        AlertThresholdKey.FileGrowthRiseMb => settings.FileGrowthRiseMb,
        AlertThresholdKey.FileGrowthVolumePercent => settings.FileGrowthVolumePercent,
        AlertThresholdKey.FileGrowthLookbackMinutes => settings.FileGrowthLookbackMinutes,
        AlertThresholdKey.LongRunningJobMultiplier => settings.LongRunningJobMultiplier,
        AlertThresholdKey.FailedJobLookbackMinutes => settings.FailedJobLookbackMinutes,
        _ => throw new ArgumentOutOfRangeException(nameof(key), key, "Not in the threshold registry."),
    };
}
