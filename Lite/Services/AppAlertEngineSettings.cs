/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
/* Alias to keep the app's own persisted CpuAlertMode enum (Total/SqlOnly) visually distinct from
   the engine's (TotalServer/SqlProcess) — the two are mapped, never mixed. */
using EngineCpuAlertMode = PerformanceMonitor.Alerting.CpuAlertMode;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Adapts Lite's App.* alert statics to the shared engine's <see cref="IAlertEngineSettings"/>
/// (Phase-5 forwarding) — the engine-threshold sibling of <see cref="AppAlertSettings"/>
/// (the delivery-only surface). Pure, live pass-through: every member reads the current App value
/// on each access with no caching, so a runtime settings reload is reflected on the next sweep,
/// exactly as the pre-forwarding loop's direct App.* reads behaved. Stateless; safe to construct
/// once and share.
/// <para>
/// The only member that isn't a bare forward is <see cref="CpuAlertMode"/>: Lite's persisted
/// <c>CpuAlertMode.Total</c>/<c>SqlOnly</c> maps to the engine's
/// <c>TotalServer</c>/<c>SqlProcess</c> (the engine enum is deliberately separate so app settings
/// stores never serialize engine names).
/// </para>
/// </summary>
public sealed class AppAlertEngineSettings : IAlertEngineSettings
{
    public bool AlertsEnabled => App.AlertsEnabled;

    public bool CpuEnabled => App.AlertCpuEnabled;
    public bool BlockingEnabled => App.AlertBlockingEnabled;
    public bool DeadlockEnabled => App.AlertDeadlockEnabled;
    public bool PoisonWaitEnabled => App.AlertPoisonWaitEnabled;
    public bool LongRunningQueryEnabled => App.AlertLongRunningQueryEnabled;
    public bool TempDbSpaceEnabled => App.AlertTempDbSpaceEnabled;
    public bool LowDiskEnabled => App.AlertLowDiskEnabled;
    public bool PvsEnabled => App.AlertPvsEnabled;
    public bool LongRunningJobEnabled => App.AlertLongRunningJobEnabled;
    public bool FailedJobEnabled => App.AlertFailedJobEnabled;
    public bool DatabaseStateEnabled => App.AlertDatabaseStateEnabled;

    /* #2157: ON with no user setting yet. The alert fires only on a failure counter that ROSE since the
       previous collection, so a fleet with no failing forces is silent by construction — there is nothing
       for a toggle to protect anyone from. Kept a hardcoded true rather than a darling.json/settings.json
       flag on purpose: Darling's store reload REPLACES the whole Alerts object, so a json-only alert flag
       would be silently reset on the first config_version bump. If this needs to be configurable it goes
       in the store, with the migration ladder that implies. */
    public bool ForcePlanFailureEnabled => true;

    public int CpuThresholdPercent => App.AlertCpuThreshold;
    public int BlockingCountThreshold => App.AlertBlockingThreshold;

    public int BlockingWaitSecondsThreshold => App.AlertBlockingWaitSecondsThreshold;
    public int DeadlockCountThreshold => App.AlertDeadlockThreshold;
    public int PoisonWaitThresholdMs => App.AlertPoisonWaitThresholdMs;
    public int LongRunningQueryThresholdMinutes => App.AlertLongRunningQueryThresholdMinutes;

    /* The six long-running-query read knobs — Lite exposes all six as real settings. */
    public int LongRunningQueryMaxResults => App.AlertLongRunningQueryMaxResults;
    public bool LongRunningQueryExcludeSpServerDiagnostics => App.AlertLongRunningQueryExcludeSpServerDiagnostics;
    public bool LongRunningQueryExcludeWaitFor => App.AlertLongRunningQueryExcludeWaitFor;
    public bool LongRunningQueryExcludeBackups => App.AlertLongRunningQueryExcludeBackups;
    public bool LongRunningQueryExcludeMiscWaits => App.AlertLongRunningQueryExcludeMiscWaits;
    public bool LongRunningQueryExcludeCdc => App.AlertLongRunningQueryExcludeCdc;

    public int TempDbSpaceThresholdPercent => App.AlertTempDbSpaceThresholdPercent;
    public int LowDiskThresholdPercent => App.AlertLowDiskThresholdPercent;
    public int LowDiskThresholdGb => App.AlertLowDiskThresholdGb;

    /* #2107: the low-disk CRITICAL tier floors — settings.json-backed like their WARNING-tier
       siblings above. The three Darling self-monitoring knobs below them return the shipped
       defaults: Lite has no headless store volume or fleet collection loop to self-monitor, and
       the members exist so the two apps' settings objects stay one shape (the PVS precedent). */
    public int DiskCriticalFreePercent => App.AlertDiskCriticalFreePercent;
    public int DiskCriticalFreeGb => App.AlertDiskCriticalFreeGb;
    public int SelfDiskFreeWarnPercent => 10;
    public int CollectionStaleMinutes => ServerHealthThresholds.CollectionStoppedMinutesDefault;
    public int CollectionFailureThreshold => 10;
    public int PvsThresholdPercent => App.AlertPvsThresholdPercent;
    public int PvsFloorGb => App.AlertPvsFloorGb;

    /* #2349: the file-growth gates, same clamps as Darling's adapter so the two SKUs cannot disagree about
       what a threshold means. Zero disables one gate rather than being nonsense, so rise-only or level-only
       needs no second switch. */
    public bool FileGrowthEnabled => App.AlertFileGrowthEnabled;
    public int FileGrowthRiseMb => Math.Max(0, App.AlertFileGrowthRiseMb);
    public int FileGrowthVolumePercent => Math.Clamp(App.AlertFileGrowthVolumePercent, 0, 100);
    public int FileGrowthLookbackMinutes => Math.Clamp(App.AlertFileGrowthLookbackMinutes, 5, 1440);
    public int LongRunningJobMultiplier => App.AlertLongRunningJobMultiplier;
    public int FailedJobLookbackMinutes => App.AlertFailedJobLookbackMinutes;
    public int CooldownMinutes => App.AlertCooldownMinutes;

    public IReadOnlyList<string> ExcludedDatabases => App.AlertExcludedDatabases;

    public EngineCpuAlertMode CpuAlertMode =>
        App.AlertCpuMode == PerformanceMonitorLite.CpuAlertMode.Total
            ? EngineCpuAlertMode.TotalServer
            : EngineCpuAlertMode.SqlProcess;
}
