/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's control-plane writes to <c>config.config_alert_settings</c> — the single-row (id=1) desired
/// state for the shared alert engine's toggles/thresholds plus the scheduled-analysis cadence knobs the
/// Stage-1 <c>StoreConfigProvider</c> reads and hot-swaps into the running service's
/// <c>DarlingAlertSettings</c>/<c>AnalysisConfig</c> on every reload beacon. This is the Stage-3b rewire that
/// replaces the severed <c>viewer-settings.json</c> alert block: saving the Settings window's Notifications +
/// Automated-Analysis sections now drives the running service.
///
/// <para>Mirrors <see cref="MonitoredServerUpsertSql"/>/<see cref="GetMuteRulesAsync"/> discipline exactly:
/// public-const SQL (so Darling.Tests pin the dialect + column parity with the service's
/// <c>StoreConfigProvider.ReadAlertSettingsAsync</c> without a live Postgres), every value bound as a
/// <c>$N</c> parameter, the write routed through <see cref="ExecuteWriteAsync"/> so a read-only <c>viewer</c>
/// seat degrades to <see cref="ViewerReadOnlyException"/> (42501). The single global row is pinned to
/// <c>id = 1</c>; <c>modified_at</c> is server-side naive-UTC. The bare table name resolves through the
/// <c>collect,config,public</c> search_path to <c>config.config_alert_settings</c>.</para>
///
/// <para><b>cpu_mode.</b> The store carries the SERVICE's vocabulary — <c>"sql"</c> (SQL-process CPU) vs
/// <c>"total"</c> (all non-idle CPU) — which the service compares case-insensitively against <c>"sql"</c>.
/// The viewer's <see cref="AlertSettingsRow.CpuMode"/> holds that same store value; the Settings window maps
/// its "Total"/"SqlOnly" combo to/from it (see <see cref="MapCpuModeToStore"/>).</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /* The 47 AlertsConfig + AnalysisConfig columns in the SAME order the service reads them
       (StoreConfigProvider.ReadAlertSettingsAsync), so the parity test pins one list against both ends.
       delivery_mode/per_event_max (V18, #1141) then the six long-running-query read knobs + the connection-change
       notify toggle (V20) are appended so the existing ordinals stay pinned. */
    private const string AlertSettingsColumns =
        "enabled, cpu_enabled, cpu_threshold_percent, cpu_mode, blocking_enabled, blocking_count_threshold, " +
        "deadlock_enabled, deadlock_count_threshold, poison_wait_enabled, poison_wait_threshold_ms, " +
        "long_running_query_enabled, long_running_query_threshold_minutes, tempdb_space_enabled, " +
        "tempdb_space_threshold_percent, low_disk_enabled, low_disk_threshold_percent, low_disk_threshold_gb, " +
        "long_running_job_enabled, long_running_job_multiplier, failed_job_enabled, failed_job_lookback_minutes, " +
        "cooldown_minutes, excluded_databases, analysis_enabled, analysis_interval_minutes, " +
        "analysis_notifications_enabled, analysis_notify_severity, delivery_mode, per_event_max, " +
        "long_running_query_max_results, long_running_query_exclude_sp_server_diagnostics, " +
        "long_running_query_exclude_wait_for, long_running_query_exclude_backups, " +
        "long_running_query_exclude_misc_waits, long_running_query_exclude_cdc, notify_connection_changes, " +
        "notify_connection_down_at_startup, connection_refire_minutes, " +
        "notify_ag_health, ag_lag_alert_seconds, ag_redo_queue_alert_kb, " +
        "ag_disconnect_refire_minutes, blocking_wait_seconds_threshold, " +
        "pvs_enabled, pvs_threshold_percent, pvs_floor_gb, database_state_enabled, " +
        "self_disk_free_warn_percent, collection_stale_minutes, collection_failure_threshold, " +
        "disk_critical_free_percent, disk_critical_free_gb, analysis_notify_cooldown_minutes, " +
        "store_job_cadence_warn_percent, " +
        /* #2391: V79 (#2349). APPENDED — this list drives the SELECT ordinals AND the upsert
           parameter positions, so inserting anywhere but the end re-maps both at once. */
        "file_growth_enabled, file_growth_rise_mb, file_growth_volume_percent, file_growth_lookback_minutes";

    /// <summary>The single global alert-settings row (id=1), for the Settings window prefill + the migrate-in
    /// defaults check. Column order matches <see cref="AlertSettingsColumns"/>.</summary>
    public const string AlertSettingsSelectSql =
        "SELECT " + AlertSettingsColumns + " FROM config_alert_settings WHERE id = 1";

    /// <summary>Upserts the single global alert-settings row (Settings window Save). ON CONFLICT rewrites every
    /// column and bumps <c>modified_at</c> (and, via the V17 statement trigger, <c>config_version</c> — the
    /// service reloads on its next sweep). $1..$47 bind the columns in <see cref="AlertSettingsColumns"/> order.</summary>
    public const string AlertSettingsUpsertSql = @"
INSERT INTO config_alert_settings (id, " + AlertSettingsColumns + @", modified_at)
VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
        $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43,
        $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58,
        (now() AT TIME ZONE 'UTC'))
ON CONFLICT (id) DO UPDATE SET
    enabled = EXCLUDED.enabled,
    cpu_enabled = EXCLUDED.cpu_enabled,
    cpu_threshold_percent = EXCLUDED.cpu_threshold_percent,
    cpu_mode = EXCLUDED.cpu_mode,
    blocking_enabled = EXCLUDED.blocking_enabled,
    blocking_count_threshold = EXCLUDED.blocking_count_threshold,
    deadlock_enabled = EXCLUDED.deadlock_enabled,
    deadlock_count_threshold = EXCLUDED.deadlock_count_threshold,
    poison_wait_enabled = EXCLUDED.poison_wait_enabled,
    poison_wait_threshold_ms = EXCLUDED.poison_wait_threshold_ms,
    long_running_query_enabled = EXCLUDED.long_running_query_enabled,
    long_running_query_threshold_minutes = EXCLUDED.long_running_query_threshold_minutes,
    tempdb_space_enabled = EXCLUDED.tempdb_space_enabled,
    tempdb_space_threshold_percent = EXCLUDED.tempdb_space_threshold_percent,
    low_disk_enabled = EXCLUDED.low_disk_enabled,
    low_disk_threshold_percent = EXCLUDED.low_disk_threshold_percent,
    low_disk_threshold_gb = EXCLUDED.low_disk_threshold_gb,
    long_running_job_enabled = EXCLUDED.long_running_job_enabled,
    long_running_job_multiplier = EXCLUDED.long_running_job_multiplier,
    failed_job_enabled = EXCLUDED.failed_job_enabled,
    failed_job_lookback_minutes = EXCLUDED.failed_job_lookback_minutes,
    cooldown_minutes = EXCLUDED.cooldown_minutes,
    excluded_databases = EXCLUDED.excluded_databases,
    analysis_enabled = EXCLUDED.analysis_enabled,
    analysis_interval_minutes = EXCLUDED.analysis_interval_minutes,
    analysis_notifications_enabled = EXCLUDED.analysis_notifications_enabled,
    analysis_notify_severity = EXCLUDED.analysis_notify_severity,
    delivery_mode = EXCLUDED.delivery_mode,
    per_event_max = EXCLUDED.per_event_max,
    long_running_query_max_results = EXCLUDED.long_running_query_max_results,
    long_running_query_exclude_sp_server_diagnostics = EXCLUDED.long_running_query_exclude_sp_server_diagnostics,
    long_running_query_exclude_wait_for = EXCLUDED.long_running_query_exclude_wait_for,
    long_running_query_exclude_backups = EXCLUDED.long_running_query_exclude_backups,
    long_running_query_exclude_misc_waits = EXCLUDED.long_running_query_exclude_misc_waits,
    long_running_query_exclude_cdc = EXCLUDED.long_running_query_exclude_cdc,
    notify_connection_changes = EXCLUDED.notify_connection_changes,
    notify_connection_down_at_startup = EXCLUDED.notify_connection_down_at_startup,
    connection_refire_minutes = EXCLUDED.connection_refire_minutes,
    notify_ag_health = EXCLUDED.notify_ag_health,
    ag_lag_alert_seconds = EXCLUDED.ag_lag_alert_seconds,
    ag_redo_queue_alert_kb = EXCLUDED.ag_redo_queue_alert_kb,
    ag_disconnect_refire_minutes = EXCLUDED.ag_disconnect_refire_minutes,
    blocking_wait_seconds_threshold = EXCLUDED.blocking_wait_seconds_threshold,
    pvs_enabled = EXCLUDED.pvs_enabled,
    pvs_threshold_percent = EXCLUDED.pvs_threshold_percent,
    pvs_floor_gb = EXCLUDED.pvs_floor_gb,
    database_state_enabled = EXCLUDED.database_state_enabled,
    self_disk_free_warn_percent = EXCLUDED.self_disk_free_warn_percent,
    collection_stale_minutes = EXCLUDED.collection_stale_minutes,
    collection_failure_threshold = EXCLUDED.collection_failure_threshold,
    disk_critical_free_percent = EXCLUDED.disk_critical_free_percent,
    disk_critical_free_gb = EXCLUDED.disk_critical_free_gb,
    analysis_notify_cooldown_minutes = EXCLUDED.analysis_notify_cooldown_minutes,
    store_job_cadence_warn_percent = EXCLUDED.store_job_cadence_warn_percent,
    file_growth_enabled = EXCLUDED.file_growth_enabled,
    file_growth_rise_mb = EXCLUDED.file_growth_rise_mb,
    file_growth_volume_percent = EXCLUDED.file_growth_volume_percent,
    file_growth_lookback_minutes = EXCLUDED.file_growth_lookback_minutes,
    modified_at = (now() AT TIME ZONE 'UTC')";

    /// <summary>The two <c>cpu_mode</c> values the service honors (it compares case-insensitively against
    /// <see cref="CpuModeSql"/>; anything else is total).</summary>
    public const string CpuModeSql = "sql";
    public const string CpuModeTotal = "total";

    /// <summary>Reads the single global alert-settings row, or null when the store has not seeded it yet
    /// (a pre-Stage-1 store, or the service has not started) — the caller then shows viewer defaults.</summary>
    public async Task<AlertSettingsRow?> GetAlertSettingsAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(AlertSettingsSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadAlertSettingsRow(reader) : null;
    }

    /// <summary>Upserts the single global alert-settings row (Settings window Save). Read-only seats throw
    /// <see cref="ViewerReadOnlyException"/> via <see cref="ExecuteWriteAsync"/>.</summary>
    public async Task UpsertAlertSettingsAsync(AlertSettingsRow row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);

        await using var command = _dataSource.CreateCommand(AlertSettingsUpsertSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        BindAlertSettings(command, row);
        await ExecuteWriteAsync(command, cancellationToken);
    }

    private static void BindAlertSettings(NpgsqlCommand command, AlertSettingsRow r)
    {
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.Enabled });                          // $1
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.CpuEnabled });                       // $2
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.CpuThresholdPercent });               // $3
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = r.CpuMode });                        // $4
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.BlockingEnabled });                  // $5
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.BlockingCountThreshold });            // $6
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.DeadlockEnabled });                  // $7
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.DeadlockCountThreshold });            // $8
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.PoisonWaitEnabled });                // $9
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.PoisonWaitThresholdMs });             // $10
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.LongRunningQueryEnabled });          // $11
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.LongRunningQueryThresholdMinutes });  // $12
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.TempDbSpaceEnabled });               // $13
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.TempDbSpaceThresholdPercent });       // $14
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.LowDiskEnabled });                   // $15
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.LowDiskThresholdPercent });           // $16
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.LowDiskThresholdGb });                // $17
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.LongRunningJobEnabled });            // $18
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.LongRunningJobMultiplier });          // $19
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.FailedJobEnabled });                 // $20
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.FailedJobLookbackMinutes });          // $21
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.CooldownMinutes });                   // $22
        AddTextArray(command, r.ExcludedDatabases);                                                             // $23
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.AnalysisEnabled });                  // $24
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.AnalysisIntervalMinutes });           // $25
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.AnalysisNotificationsEnabled });     // $26
        command.Parameters.Add(new NpgsqlParameter<double> { TypedValue = r.AnalysisNotifySeverity });         // $27
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = r.DeliveryMode });                   // $28
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.PerEventMax });                       // $29
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.LongRunningQueryMaxResults });        // $30
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.LongRunningQueryExcludeSpServerDiagnostics }); // $31
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.LongRunningQueryExcludeWaitFor });   // $32
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.LongRunningQueryExcludeBackups });   // $33
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.LongRunningQueryExcludeMiscWaits });  // $34
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.LongRunningQueryExcludeCdc });       // $35
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.NotifyConnectionChanges });          // $36
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.NotifyConnectionDownAtStartup });   // $37 (#1659, V33)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.ConnectionRefireMinutes });          // $38 (#1659, V33)
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.NotifyAgHealth });                  // $39 (#991, V35)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.AgLagAlertSeconds });                // $40 (#991, V35)
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = r.AgRedoQueueAlertKb });              // $41 (#991, V35)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.AgDisconnectRefireMinutes });        // $42 (#1696, V37)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.BlockingWaitSecondsThreshold });    // $43 (#1839, V40)
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.PvsEnabled });                     // $44 (#1984, V48)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.PvsThresholdPercent });             // $45 (#1984, V48)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.PvsFloorGb });                      // $46 (#1984, V48)
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.DatabaseStateEnabled });            // $47 (database-state alert, V49)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.SelfDiskFreeWarnPercent });          // $48 (#2107, V55)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.CollectionStaleMinutes });           // $49 (#2107, V55)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.CollectionFailureThreshold });       // $50 (#2107, V55)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.DiskCriticalFreePercent });          // $51 (#2107, V55)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.DiskCriticalFreeGb });               // $52 (#2107, V55)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.AnalysisNotifyCooldownMinutes });    // $53 (#2107, V55)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.StoreJobCadenceWarnPercent });      // $54 (#2136, V57)
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = r.FileGrowthEnabled });             // $55 (#2349/#2391, V79)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.FileGrowthRiseMb });               // $56 (#2349/#2391, V79)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.FileGrowthVolumePercent });        // $57 (#2349/#2391, V79)
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = r.FileGrowthLookbackMinutes });      // $58 (#2349/#2391, V79)
    }

    private static AlertSettingsRow ReadAlertSettingsRow(NpgsqlDataReader reader) => new()
    {
        Enabled = reader.GetBoolean(0),
        CpuEnabled = reader.GetBoolean(1),
        CpuThresholdPercent = reader.GetInt32(2),
        CpuMode = reader.GetString(3),
        BlockingEnabled = reader.GetBoolean(4),
        BlockingCountThreshold = reader.GetInt32(5),
        DeadlockEnabled = reader.GetBoolean(6),
        DeadlockCountThreshold = reader.GetInt32(7),
        PoisonWaitEnabled = reader.GetBoolean(8),
        PoisonWaitThresholdMs = reader.GetInt32(9),
        LongRunningQueryEnabled = reader.GetBoolean(10),
        LongRunningQueryThresholdMinutes = reader.GetInt32(11),
        TempDbSpaceEnabled = reader.GetBoolean(12),
        TempDbSpaceThresholdPercent = reader.GetInt32(13),
        LowDiskEnabled = reader.GetBoolean(14),
        LowDiskThresholdPercent = reader.GetInt32(15),
        LowDiskThresholdGb = reader.GetInt32(16),
        LongRunningJobEnabled = reader.GetBoolean(17),
        LongRunningJobMultiplier = reader.GetInt32(18),
        FailedJobEnabled = reader.GetBoolean(19),
        FailedJobLookbackMinutes = reader.GetInt32(20),
        CooldownMinutes = reader.GetInt32(21),
        ExcludedDatabases = reader.IsDBNull(22) ? new List<string>() : reader.GetFieldValue<string[]>(22).ToList(),
        AnalysisEnabled = reader.GetBoolean(23),
        AnalysisIntervalMinutes = reader.GetInt32(24),
        AnalysisNotificationsEnabled = reader.GetBoolean(25),
        AnalysisNotifySeverity = reader.GetDouble(26),
        DeliveryMode = reader.GetString(27),
        PerEventMax = reader.GetInt32(28),
        LongRunningQueryMaxResults = reader.GetInt32(29),
        LongRunningQueryExcludeSpServerDiagnostics = reader.GetBoolean(30),
        LongRunningQueryExcludeWaitFor = reader.GetBoolean(31),
        LongRunningQueryExcludeBackups = reader.GetBoolean(32),
        LongRunningQueryExcludeMiscWaits = reader.GetBoolean(33),
        LongRunningQueryExcludeCdc = reader.GetBoolean(34),
        NotifyConnectionChanges = reader.GetBoolean(35),
        /* #1659 opt-ins appended (V33) at ordinals 36-37. */
        NotifyConnectionDownAtStartup = reader.GetBoolean(36),
        ConnectionRefireMinutes = reader.GetInt32(37),
        /* #991 AG knobs appended (V35) at ordinals 38-40. */
        NotifyAgHealth = reader.GetBoolean(38),
        AgLagAlertSeconds = reader.GetInt32(39),
        AgRedoQueueAlertKb = reader.GetInt64(40),
        /* #1696 AG disconnect re-fire appended (V37) at ordinal 41. */
        AgDisconnectRefireMinutes = reader.GetInt32(41),
        /* #1839 total-blocked-wait gate appended (V40) at ordinal 42. */
        BlockingWaitSecondsThreshold = reader.GetInt32(42),
        /* #1984 PVS-pressure knobs appended (V48) at ordinals 43-45. */
        PvsEnabled = reader.GetBoolean(43),
        PvsThresholdPercent = reader.GetInt32(44),
        PvsFloorGb = reader.GetInt32(45),
        /* database-state alert master switch appended (V49) at ordinal 46. */
        DatabaseStateEnabled = reader.GetBoolean(46),
        /* #2107 threshold knobs appended (V55) at ordinals 47–52. */
        SelfDiskFreeWarnPercent = reader.GetInt32(47),
        CollectionStaleMinutes = reader.GetInt32(48),
        CollectionFailureThreshold = reader.GetInt32(49),
        DiskCriticalFreePercent = reader.GetInt32(50),
        DiskCriticalFreeGb = reader.GetInt32(51),
        AnalysisNotifyCooldownMinutes = reader.GetInt32(52),
        StoreJobCadenceWarnPercent = reader.GetInt32(53),
        FileGrowthEnabled = reader.GetBoolean(54),
        FileGrowthRiseMb = reader.GetInt32(55),
        FileGrowthVolumePercent = reader.GetInt32(56),
        FileGrowthLookbackMinutes = reader.GetInt32(57),
    };

    /// <summary>Maps the Settings window's CPU-mode combo tag ("Total"/"SqlOnly") to the store value.</summary>
    public static string MapCpuModeToStore(string viewerMode) =>
        string.Equals(viewerMode, "SqlOnly", StringComparison.OrdinalIgnoreCase) ? CpuModeSql : CpuModeTotal;

    /// <summary>Maps the store value back to the Settings window's combo tag.</summary>
    public static string MapCpuModeFromStore(string? storeMode) =>
        string.Equals(storeMode, CpuModeSql, StringComparison.OrdinalIgnoreCase) ? "SqlOnly" : "Total";
}

/// <summary>
/// A <c>config.config_alert_settings</c> row (the single id=1 global row) as the viewer authors + reads it —
/// the desired-state twin of the service's <c>AlertsConfig</c> + <c>AnalysisConfig</c> the store hot-swaps in.
/// Carries ONLY the columns the store (and hence the service) has, now INCLUDING the Summary/Per-event delivery
/// mode + per-event cap (#1141/#1236, V18 — the service honors them at delivery time), the long-running-query
/// read shape (max-results + the five noise filters, V20 — the service forwards them to the LRQ read), and the
/// connection-change notify toggle (V20 — the service gates the Server-Unreachable/Restored edge on it). The
/// remaining viewer-only alert fields the Settings window also edits (tray minimize, mute-rule default expiration,
/// dismissal logging, analysis re-notify cooldown) are NOT service-honored config and stay viewer-local in
/// <see cref="ViewerAppSettings"/>. Defaults mirror the V17/V18/V20 DDL (and Lite's <c>App.*</c>) member-for-member
/// so <see cref="Defaults"/> equals a freshly-seeded row.
/// </summary>
public sealed class AlertSettingsRow
{
    public bool Enabled { get; set; } = true;

    /// <summary>Whether the service delivers the Server-Unreachable/Restored connect-edge alerts (V20 DDL default true).</summary>
    public bool NotifyConnectionChanges { get; set; } = true;

    /// <summary>#1659 opt-in (V33): announce a server already down on the service's first-ever connect attempt.</summary>
    public bool NotifyConnectionDownAtStartup { get; set; }

    /// <summary>#1659 opt-in (V33): re-announce a standing outage every N minutes (0 = off).</summary>
    public int ConnectionRefireMinutes { get; set; }

    /// <summary>#991 (V35): the master switch for the Availability Group alert family (DDL default true).</summary>
    public bool NotifyAgHealth { get; set; } = true;

    /// <summary>#991 (V35): "AG Sync Fell Behind" lag trigger in seconds (0 = off).</summary>
    public int AgLagAlertSeconds { get; set; } = 300;

    /// <summary>#991 (V35): "AG Sync Fell Behind" redo-queue trigger in KB (0 = off).</summary>
    public long AgRedoQueueAlertKb { get; set; }

    /// <summary>#1696 (V37): re-announce a still-disconnected AG replica every N minutes (0 = off).</summary>
    public int AgDisconnectRefireMinutes { get; set; }

    /// <summary>Master switch for the baseline-deviation database-state alert (V40 DDL default true).</summary>
    public bool DatabaseStateEnabled { get; set; } = true;

    /* #2107 (V55): the previously-hardcoded thresholds; defaults are the constants they replaced. */
    public int SelfDiskFreeWarnPercent { get; set; } = 10;
    public int CollectionStaleMinutes { get; set; } = 30;
    public int CollectionFailureThreshold { get; set; } = 10;
    public int DiskCriticalFreePercent { get; set; } = 3;
    public int DiskCriticalFreeGb { get; set; } = 2;
    public int AnalysisNotifyCooldownMinutes { get; set; } = 360;
    public int StoreJobCadenceWarnPercent { get; set; } = 25;

    /* #2391: defaults mirror the V79 column defaults, so a viewer prefilling against a store that has
       not seeded the row shows what the store would have given it. Ships OFF, per #2349. */
    public bool FileGrowthEnabled { get; set; }
    public int FileGrowthRiseMb { get; set; } = 10240;
    public int FileGrowthVolumePercent { get; set; } = 60;
    public int FileGrowthLookbackMinutes { get; set; } = 60;

    public bool CpuEnabled { get; set; } = true;
    public int CpuThresholdPercent { get; set; } = 80;

    /// <summary><see cref="ViewerDataService.CpuModeSql"/> or <see cref="ViewerDataService.CpuModeTotal"/>.</summary>
    public string CpuMode { get; set; } = ViewerDataService.CpuModeTotal;

    public bool BlockingEnabled { get; set; } = true;
    public int BlockingCountThreshold { get; set; } = 1;

    /// <summary>#1839 (V40): fire when the latest blocking snapshot's TOTAL blocked wait reaches this many
    /// seconds. 0 = off, matching the V40 DDL default — the shipped behavior is unchanged on upgrade.</summary>
    public int BlockingWaitSecondsThreshold { get; set; }

    public bool DeadlockEnabled { get; set; } = true;
    public int DeadlockCountThreshold { get; set; } = 1;
    public bool PoisonWaitEnabled { get; set; } = true;
    public int PoisonWaitThresholdMs { get; set; } = 500;
    public bool LongRunningQueryEnabled { get; set; } = true;
    public int LongRunningQueryThresholdMinutes { get; set; } = 30;
    public bool TempDbSpaceEnabled { get; set; } = true;
    public int TempDbSpaceThresholdPercent { get; set; } = 80;
    public bool LowDiskEnabled { get; set; } = true;
    public int LowDiskThresholdPercent { get; set; } = 10;
    public int LowDiskThresholdGb { get; set; } = 5;

    /// <summary>#1984 (V48): the PVS-pressure alert — enabled at 40% of database with a 1 GB floor
    /// (AND semantics; percent 0 disables the check, floor 0 removes the qualifier).</summary>
    public bool PvsEnabled { get; set; } = true;
    public int PvsThresholdPercent { get; set; } = 40;
    public int PvsFloorGb { get; set; } = 1;

    public bool LongRunningJobEnabled { get; set; } = true;
    public int LongRunningJobMultiplier { get; set; } = 3;
    public bool FailedJobEnabled { get; set; } = true;
    public int FailedJobLookbackMinutes { get; set; } = 60;
    public int CooldownMinutes { get; set; } = 5;
    public List<string> ExcludedDatabases { get; set; } = new();
    public bool AnalysisEnabled { get; set; } = true;
    public int AnalysisIntervalMinutes { get; set; } = 30;

    /// <summary>Darling's shipped default is TRUE (Lite's App default is false) — matches the V17 DDL.</summary>
    public bool AnalysisNotificationsEnabled { get; set; } = true;
    public double AnalysisNotifySeverity { get; set; } = 1.5;

    /// <summary>Deadlock/blocking delivery mode: "Summary" (one card per cycle) or "PerEvent" (#1141) — the store's
    /// <c>delivery_mode</c> text, matching the service's <c>AlertNotificationMode</c> names. Default "Summary" (V18 DDL).</summary>
    public string DeliveryMode { get; set; } = "Summary";

    /// <summary>Per-event mode's per-cycle incident cap before the "+N more" batch. Default 5 (V18 DDL); clamped 1–100.</summary>
    public int PerEventMax { get; set; } = 5;

    /* The long-running-query read shape (V20): the service forwards these to the LRQ read. Defaults match Lite's
       App.* (5 rows / every filter on) so a freshly-seeded row suppresses the same noise it always did. */

    /// <summary>Row cap for the long-running-query read (V20 DDL default 5; the read adapter clamps 1–1000).</summary>
    public int LongRunningQueryMaxResults { get; set; } = 5;

    /// <summary>Exclude sessions waiting on SP_SERVER_DIAGNOSTICS (V20 DDL default true).</summary>
    public bool LongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;

    /// <summary>Exclude WAITFOR / BROKER_RECEIVE_WAITFOR sessions (V20 DDL default true).</summary>
    public bool LongRunningQueryExcludeWaitFor { get; set; } = true;

    /// <summary>Exclude BACKUPTHREAD / BACKUPIO sessions (V20 DDL default true).</summary>
    public bool LongRunningQueryExcludeBackups { get; set; } = true;

    /// <summary>Exclude XE_LIVE_TARGET_TVF sessions (V20 DDL default true).</summary>
    public bool LongRunningQueryExcludeMiscWaits { get; set; } = true;

    /// <summary>Exclude CDC capture sessions (V20 DDL default true).</summary>
    public bool LongRunningQueryExcludeCdc { get; set; } = true;

    /// <summary>A row equal to the V17/V18/V20 seed defaults — the migrate-in "is the service section still at defaults?" baseline.</summary>
    public static AlertSettingsRow Defaults() => new();

    /// <summary>Value-equality against another row (sequence-comparing the excluded-databases list) — the
    /// migrate-in uses it to detect an untouched (default) store section and a genuine viewer customization.</summary>
    public bool ValueEquals(AlertSettingsRow other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Enabled == other.Enabled
            && NotifyConnectionChanges == other.NotifyConnectionChanges
            && NotifyConnectionDownAtStartup == other.NotifyConnectionDownAtStartup
            && ConnectionRefireMinutes == other.ConnectionRefireMinutes
            && NotifyAgHealth == other.NotifyAgHealth
            && AgLagAlertSeconds == other.AgLagAlertSeconds
            && AgRedoQueueAlertKb == other.AgRedoQueueAlertKb
            && AgDisconnectRefireMinutes == other.AgDisconnectRefireMinutes
            && DatabaseStateEnabled == other.DatabaseStateEnabled
            && SelfDiskFreeWarnPercent == other.SelfDiskFreeWarnPercent
            && CollectionStaleMinutes == other.CollectionStaleMinutes
            && CollectionFailureThreshold == other.CollectionFailureThreshold
            && DiskCriticalFreePercent == other.DiskCriticalFreePercent
            && DiskCriticalFreeGb == other.DiskCriticalFreeGb
            && AnalysisNotifyCooldownMinutes == other.AnalysisNotifyCooldownMinutes
            && CpuEnabled == other.CpuEnabled
            && CpuThresholdPercent == other.CpuThresholdPercent
            && string.Equals(CpuMode, other.CpuMode, StringComparison.OrdinalIgnoreCase)
            && BlockingEnabled == other.BlockingEnabled
            && BlockingCountThreshold == other.BlockingCountThreshold
            && BlockingWaitSecondsThreshold == other.BlockingWaitSecondsThreshold
            && DeadlockEnabled == other.DeadlockEnabled
            && DeadlockCountThreshold == other.DeadlockCountThreshold
            && PoisonWaitEnabled == other.PoisonWaitEnabled
            && PoisonWaitThresholdMs == other.PoisonWaitThresholdMs
            && LongRunningQueryEnabled == other.LongRunningQueryEnabled
            && LongRunningQueryThresholdMinutes == other.LongRunningQueryThresholdMinutes
            && TempDbSpaceEnabled == other.TempDbSpaceEnabled
            && TempDbSpaceThresholdPercent == other.TempDbSpaceThresholdPercent
            && LowDiskEnabled == other.LowDiskEnabled
            && LowDiskThresholdPercent == other.LowDiskThresholdPercent
            && LowDiskThresholdGb == other.LowDiskThresholdGb
            && PvsEnabled == other.PvsEnabled
            && PvsThresholdPercent == other.PvsThresholdPercent
            && PvsFloorGb == other.PvsFloorGb
            && LongRunningJobEnabled == other.LongRunningJobEnabled
            && LongRunningJobMultiplier == other.LongRunningJobMultiplier
            && FailedJobEnabled == other.FailedJobEnabled
            && FailedJobLookbackMinutes == other.FailedJobLookbackMinutes
            && CooldownMinutes == other.CooldownMinutes
            && (ExcludedDatabases ?? new List<string>()).SequenceEqual(other.ExcludedDatabases ?? new List<string>())
            && AnalysisEnabled == other.AnalysisEnabled
            && AnalysisIntervalMinutes == other.AnalysisIntervalMinutes
            && AnalysisNotificationsEnabled == other.AnalysisNotificationsEnabled
            && Math.Abs(AnalysisNotifySeverity - other.AnalysisNotifySeverity) < 0.0001
            && string.Equals(DeliveryMode, other.DeliveryMode, StringComparison.OrdinalIgnoreCase)
            && PerEventMax == other.PerEventMax
            && LongRunningQueryMaxResults == other.LongRunningQueryMaxResults
            && LongRunningQueryExcludeSpServerDiagnostics == other.LongRunningQueryExcludeSpServerDiagnostics
            && LongRunningQueryExcludeWaitFor == other.LongRunningQueryExcludeWaitFor
            && LongRunningQueryExcludeBackups == other.LongRunningQueryExcludeBackups
            && LongRunningQueryExcludeMiscWaits == other.LongRunningQueryExcludeMiscWaits
            && LongRunningQueryExcludeCdc == other.LongRunningQueryExcludeCdc;
    }
}
