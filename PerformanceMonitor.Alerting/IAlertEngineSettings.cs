/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Which CPU metric the CPU alert evaluates against. The ENGINE — not the caller — selects which
/// collected CPU value to compare based on this mode: <see cref="SqlProcess"/> compares the SQL
/// Server scheduler ProcessUtilization %, <see cref="TotalServer"/> compares total non-idle CPU
/// (SQL process + other processes, matching OS user+system — "is the box in trouble"). Callers
/// only provide the mode; they never pre-select the value, so the selection rule cannot drift
/// between hosts.
/// <para>
/// Deliberately a NEW enum with engine-descriptive value names rather than reusing either app's
/// <c>CpuAlertMode</c> (<c>Total</c>/<c>SqlOnly</c>): the app enums are persisted in each app's
/// settings store and are mapped to this one at the adapter seam.
/// </para>
/// </summary>
public enum CpuAlertMode
{
    /// <summary>SQL Server scheduler ProcessUtilization only.</summary>
    SqlProcess,

    /// <summary>SQL process + other-process CPU — matches OS user+system.</summary>
    TotalServer
}

/// <summary>
/// The ENGINE threshold surface for the Phase-5 shared alert engine: every enabled flag and
/// threshold the sweep evaluates alert conditions against. This is one of the three engine seams
/// (with <see cref="IAlertStateStore"/> and <see cref="IAlertDeliverer"/>) consumed by the headless
/// Darling alert engine first; Lite forwards its <c>App.*</c> alert statics through an adapter in a
/// later slice, and Dashboard convergence is a separately-decided migration (its live engine reads
/// <c>UserPreferences</c> directly today).
/// <para>
/// Deliberately SEPARATE from <c>PerformanceMonitor.Notifications.IAlertSettings</c>, which is the
/// delivery-only surface (SMTP/webhook/analysis-notify). Delivery settings answer "how do I send an
/// alert"; this interface answers "when does an alert exist". Keeping them apart lets a host wire
/// delivery without the engine (Dashboard today) or the engine with a different deliverer (Darling).
/// </para>
/// <para>
/// Blocking and deadlock thresholds here are COUNT-based — Lite's collected-store semantics (the
/// number of blocked-process reports / deadlocks in the rolling window). The Dashboard's live
/// engine instead thresholds blocking on longest-blocked SECONDS and deadlocks on a since-last-check
/// DELTA; those semantics intentionally stay app-side and are NOT modeled here — a Dashboard
/// migration would be a semantic change decided separately, not smuggled through this seam.
/// </para>
/// <para>
/// All members are pass-through reads of live values — the engine reads them every sweep, so a
/// settings reload is reflected immediately (no caching), matching the apps' direct settings reads.
/// </para>
/// </summary>
public interface IAlertEngineSettings
{
    /// <summary>Master switch — when false the engine runs no alert sweep at all.</summary>
    bool AlertsEnabled { get; }

    /* Per-alert enabled flags. */
    bool CpuEnabled { get; }
    bool BlockingEnabled { get; }
    bool DeadlockEnabled { get; }
    bool PoisonWaitEnabled { get; }
    bool LongRunningQueryEnabled { get; }
    bool TempDbSpaceEnabled { get; }
    bool LowDiskEnabled { get; }
    bool LongRunningJobEnabled { get; }
    bool FailedJobEnabled { get; }
    bool PvsEnabled { get; }

    /// <summary>
    /// The database file-growth alert (#2349) — OFF by default. It sits between <c>tempdb Space</c>, whose
    /// denominator grows with autogrowth so its percentage FALLS as tempdb balloons, and <c>Volume Free
    /// Space</c>, which fires on the consequence and cannot attribute it to a file.
    /// </summary>
    bool FileGrowthEnabled { get; }
    bool DatabaseStateEnabled { get; }

    /// <summary>
    /// The forced-plan-failure alert (#2157). Default ON in both apps: it fires only on a counter that ROSE
    /// since the previous collection, so a quiet fleet is silent by construction rather than by threshold —
    /// which is what makes it safe to enable without asking. Deliberately NOT a store column yet; if
    /// operators need per-deployment control it becomes one, with the full migration ladder that implies.
    /// </summary>
    bool ForcePlanFailureEnabled { get; }

    /* Thresholds. */

    /// <summary>Fire when the selected CPU metric (see <see cref="CpuAlertMode"/>) is at/above this %.</summary>
    int CpuThresholdPercent { get; }

    /// <summary>Fire when the rolling-window blocked-process-report count reaches this value (count-based; see class remarks).</summary>
    int BlockingCountThreshold { get; }

    /// <summary>
    /// Fire when the TOTAL blocked wait time in the latest blocking snapshot reaches this many seconds
    /// (#1839). 0 = OFF, and off is the shipped default — this is a second, independent gate alongside
    /// <see cref="BlockingCountThreshold"/>: a count gate cannot tell one session blocked for an hour
    /// from one blocked for a second. Level-triggered (fires while above, re-fires on cooldown, resolves
    /// when it drops below), unlike the count gate's rolling-window edge trigger, and it reports under
    /// its own "Blocking Wait Time" metric so mutes, history and cooldowns never tangle with the count
    /// gate's. Both gates still respect <see cref="BlockingEnabled"/>.
    /// </summary>
    int BlockingWaitSecondsThreshold { get; }

    /// <summary>Fire when the rolling-window deadlock count reaches this value (count-based; see class remarks).</summary>
    int DeadlockCountThreshold { get; }

    /// <summary>Fire when a poison wait type's average ms-per-wait is at/above this value.</summary>
    int PoisonWaitThresholdMs { get; }

    /// <summary>Fire when a query's elapsed time is at/above this many minutes.</summary>
    int LongRunningQueryThresholdMinutes { get; }

    /* The long-running-query read shape the engine forwards to
       IAlertReadAdapter.GetLongRunningQueriesAsync — Lite exposes all six as real settings
       (App.AlertLongRunningQueryMaxResults + the five opt-out noise filters), so they belong on
       the engine surface for Lite to forward later. Lite/Darling defaults: 5 / all true. */

    /// <summary>Row cap for the long-running-query read (Lite clamps 1–1000; default 5).</summary>
    int LongRunningQueryMaxResults { get; }

    /// <summary>Exclude sessions waiting on SP_SERVER_DIAGNOSTICS (default true).</summary>
    bool LongRunningQueryExcludeSpServerDiagnostics { get; }

    /// <summary>Exclude WAITFOR / BROKER_RECEIVE_WAITFOR sessions (default true).</summary>
    bool LongRunningQueryExcludeWaitFor { get; }

    /// <summary>Exclude BACKUPTHREAD / BACKUPIO sessions (default true).</summary>
    bool LongRunningQueryExcludeBackups { get; }

    /// <summary>Exclude XE_LIVE_TARGET_TVF sessions (default true).</summary>
    bool LongRunningQueryExcludeMiscWaits { get; }

    /// <summary>Exclude CDC capture sessions (default true).</summary>
    bool LongRunningQueryExcludeCdc { get; }

    /// <summary>Fire when tempdb reserved space is at/above this % of total.</summary>
    int TempDbSpaceThresholdPercent { get; }

    /// <summary>Fire when a volume's free space is below this % (0 disables the percent dimension).</summary>
    int LowDiskThresholdPercent { get; }

    /// <summary>Fire when a volume's free space is below this many GB (0 disables the GB dimension).</summary>
    int LowDiskThresholdGb { get; }

    /// <summary>
    /// The low-disk CRITICAL severity tier's percent floor (#1136/#2107): free space at/below this
    /// % grades the Volume Free Space alert CRITICAL instead of WARNING. Was a compile-time 3.0 in
    /// <c>LowDiskAlertGate</c>; both apps now pass their configured value.
    /// </summary>
    int DiskCriticalFreePercent { get; }

    /// <summary>The critical tier's GB floor — at/below this many GB free is CRITICAL on any
    /// volume, OR-ed with the percent floor exactly as before (#1136/#2107).</summary>
    int DiskCriticalFreeGb { get; }

    /// <summary>
    /// The store/self-monitoring warning percent (#2107, Darling's self-alerts): the monitor's own
    /// store volume warns below this % free. Lite has no headless store volume to self-monitor and
    /// returns the shipped default — on the engine surface anyway so the two apps' settings
    /// objects stay one shape (the PVS-knob precedent).
    /// </summary>
    int SelfDiskFreeWarnPercent { get; }

    /// <summary>How long collection may go quiet before Collection Stopped / Agent Not Running
    /// fire (#2107; was a compile-time 30 minutes). Lite returns the default.</summary>
    int CollectionStaleMinutes { get; }

    /// <summary>The Collection Stopped fast path — this many consecutive failures with zero
    /// successes fires without waiting out the staleness window (#2107). Lite returns the default.</summary>
    int CollectionFailureThreshold { get; }

    /// <summary>
    /// Fire when an ADR database's persistent version store reaches this % of the database's data
    /// files (#1984). Percent rather than absolute size because a shipped absolute guess is
    /// workload-specific and would page half a fleet (the ag_redo_queue precedent) — this ratio is
    /// the one MS's troubleshooting guide reads first ("close to 50% of the database size" =
    /// large). 0 disables the check outright: percent is the alert's ONLY trigger, so unlike the
    /// low-disk pair there is no second dimension to fall back on.
    /// </summary>
    int PvsThresholdPercent { get; }

    /// <summary>
    /// A breach additionally requires the PVS to be at least this many GB (#1984) — an AND
    /// qualifier, NOT the low-disk pair's either-breach-fires OR: a 10 MB database at 60% is six
    /// megabytes, and no one should be paged for six megabytes. 0 removes the floor (percent
    /// alone decides).
    /// </summary>
    int PvsFloorGb { get; }

    /// <summary>
    /// The RISE gate: a file that grew at least this many MB inside the lookback window (#2349). Primary
    /// rather than the level, for #2157's reason — a level alone re-pages every cooldown about a size that has
    /// been true for a week, which trains people to mute it, while a rise is an event.
    /// </summary>
    int FileGrowthRiseMb { get; }

    /// <summary>
    /// The LEVEL gate: a file occupying at least this share of its volume (#2349). Self-scaling, which is what
    /// makes ONE global setting usable across a fleet whose servers have very different normal sizes — an
    /// absolute MB threshold cannot be set low enough for the small instances without deafening the large ones.
    /// </summary>
    int FileGrowthVolumePercent { get; }

    /// <summary>How far back the rise is measured (#2349). The window is MEASURED from the samples rather than
    /// assumed, so a gap in collection cannot make a slow rise look fast.</summary>
    int FileGrowthLookbackMinutes { get; }

    /// <summary>Fire when a running job exceeds this multiple of its historical average duration.</summary>
    int LongRunningJobMultiplier { get; }

    /// <summary>How many minutes back to look for failed Agent job runs.</summary>
    int FailedJobLookbackMinutes { get; }

    /// <summary>Minimum minutes between repeated notifications for the same alert condition.</summary>
    int CooldownMinutes { get; }

    /// <summary>
    /// Databases excluded from database-scoped alert evaluation (blocking/deadlock/long-running-query
    /// rows in these databases don't count toward thresholds). Compared case-insensitively.
    /// </summary>
    IReadOnlyList<string> ExcludedDatabases { get; }

    /// <summary>Which CPU value the engine compares against <see cref="CpuThresholdPercent"/>.</summary>
    CpuAlertMode CpuAlertMode { get; }
}
