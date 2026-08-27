/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the alerts MCP tools (<see cref="DarlingMcpAlertTools"/>) — the alert-history log
/// (<c>config_alert_log</c>) and the single global alert-settings row (<c>config_alert_settings</c>), both
/// STORED reads (no live monitored-server hit). Each SQL is reproduced from the viewer's proven read
/// (<c>ViewerDataService.AlertHistory.cs</c> / <c>.AlertSettings.cs</c>) rather than referenced — the MCP
/// host is in the Service assembly and cannot reference the WPF Viewer, the same reason
/// <see cref="DarlingConfigHistoryReader"/> reproduces the viewer's config SQL. The reads live in public
/// constants so Darling.Tests can pin the dialect + columns without a live Postgres.
///
/// <para>The alert-history read has the viewer's two shapes: one scoped to a server, one across ALL servers
/// (the fleet default) — both exclude dismissed rows, newest first, windowed on the naive-UTC
/// <c>alert_time</c>. The settings read is the single id=1 desired-state row the service hot-swaps into its
/// running <c>DarlingAlertSettings</c>, so it reports the alert engine + analysis config the service is
/// actually using (matching the viewer's Settings-window prefill), or null when the store has not seeded it
/// yet.</para>
/// </summary>
internal static class DarlingAlertReader
{
    /* ─────────────────────────── alert history ─────────────────────────── */

    public sealed record AlertHistoryReadRow(
        DateTime AlertTime, int ServerId, string ServerName, string MetricName,
        double CurrentValue, double ThresholdValue, bool AlertSent, string NotificationType,
        string? SendError, bool Muted, string? DetailText);

    private const string AlertHistorySelectColumns = @"
    alert_time,
    server_id,
    server_name,
    metric_name,
    current_value,
    threshold_value,
    alert_sent,
    notification_type,
    send_error,
    muted,
    detail_text";

    /// <summary>Per-server alert history — the viewer's <c>AlertHistorySql</c>. $1 window start, $2 window
    /// end, $3 server_id, $4 limit (naive UTC / naive UTC / int / int).
    ///
    /// <para>The upper edge is bounded rather than open (#2495): the row cap is applied by the database, so
    /// trimming after the read would spend the whole LIMIT on rows newer than the anchor and hand back an
    /// empty window that looks like a quiet one.</para></summary>
    public const string AlertHistorySql = @"
SELECT" + AlertHistorySelectColumns + @"
FROM config_alert_log
WHERE alert_time >= $1
AND   alert_time <= $2
AND   server_id = $3
AND   dismissed = FALSE
ORDER BY alert_time DESC
LIMIT $4";

    /// <summary>All-servers alert history (the fleet default) — the viewer's <c>AlertHistoryAllServersSql</c>.
    /// $1 window start, $2 window end, $3 limit (naive UTC / naive UTC / int).</summary>
    public const string AlertHistoryAllServersSql = @"
SELECT" + AlertHistorySelectColumns + @"
FROM config_alert_log
WHERE alert_time >= $1
AND   alert_time <= $2
AND   dismissed = FALSE
ORDER BY alert_time DESC
LIMIT $3";

    /// <summary>
    /// Recent alerts newest first, excluding dismissed rows — the Alert History read. With no
    /// <paramref name="serverId"/> it aggregates ALL servers (the fleet default); with one it scopes to that
    /// server. Mirrors the viewer's optional-serverId <c>GetAlertHistoryAsync</c>.
    /// </summary>
    public static async Task<List<AlertHistoryReadRow>> GetAlertHistoryAsync(
        NpgsqlDataSource postgres, DateTime sinceUtc, DateTime untilUtc, int? serverId, int limit, CancellationToken cancellationToken = default)
    {
        var rows = new List<AlertHistoryReadRow>();

        await using var command = postgres.CreateCommand(serverId.HasValue ? AlertHistorySql : AlertHistoryAllServersSql);
        DarlingMcpReadParameters.AddTimestamp(command, sinceUtc);
        DarlingMcpReadParameters.AddTimestamp(command, untilUtc);
        if (serverId.HasValue)
        {
            DarlingMcpReadParameters.AddInt(command, serverId.Value);
        }
        DarlingMcpReadParameters.AddInt(command, limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new AlertHistoryReadRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                !reader.IsDBNull(6) && reader.GetBoolean(6),
                reader.IsDBNull(7) ? "" : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                !reader.IsDBNull(9) && reader.GetBoolean(9),
                reader.IsDBNull(10) ? null : reader.GetString(10)));
        }

        return rows;
    }

    /* ─────────────────────────── alert settings ─────────────────────────── */

    /// <summary>The single global alert-settings row the service hot-swaps into <c>DarlingAlertSettings</c> —
    /// the alert engine toggles/thresholds, delivery mode, and scheduled-analysis cadence. Mirror of the
    /// viewer's <c>AlertSettingsRow</c> (only the store-honored fields).</summary>
    public sealed record AlertSettingsReadRow(
        bool Enabled, bool CpuEnabled, int CpuThresholdPercent, string CpuMode,
        bool BlockingEnabled, int BlockingCountThreshold, bool DeadlockEnabled, int DeadlockCountThreshold,
        bool PoisonWaitEnabled, int PoisonWaitThresholdMs, bool LongRunningQueryEnabled, int LongRunningQueryThresholdMinutes,
        bool TempDbSpaceEnabled, int TempDbSpaceThresholdPercent, bool LowDiskEnabled, int LowDiskThresholdPercent, int LowDiskThresholdGb,
        bool LongRunningJobEnabled, int LongRunningJobMultiplier, bool FailedJobEnabled, int FailedJobLookbackMinutes,
        int CooldownMinutes, IReadOnlyList<string> ExcludedDatabases, bool AnalysisEnabled, int AnalysisIntervalMinutes,
        bool AnalysisNotificationsEnabled, double AnalysisNotifySeverity, string DeliveryMode, int PerEventMax,
        int LongRunningQueryMaxResults, bool LongRunningQueryExcludeSpServerDiagnostics, bool LongRunningQueryExcludeWaitFor,
        bool LongRunningQueryExcludeBackups, bool LongRunningQueryExcludeMiscWaits, bool LongRunningQueryExcludeCdc,
        bool NotifyConnectionChanges,
        bool NotifyConnectionDownAtStartup,
        int ConnectionRefireMinutes,
        bool NotifyAgHealth,
        int AgLagAlertSeconds,
        long AgRedoQueueAlertKb,
        int AgDisconnectRefireMinutes,
        int BlockingWaitSecondsThreshold,
        bool PvsEnabled,
        int PvsThresholdPercent,
        int PvsFloorGb,
        bool DatabaseStateEnabled,
        int SelfDiskFreeWarnPercent,
        int CollectionStaleMinutes,
        int CollectionFailureThreshold,
        int DiskCriticalFreePercent,
        int DiskCriticalFreeGb,
        int AnalysisNotifyCooldownMinutes,
        int StoreJobCadenceWarnPercent,
        /* #2391 (V79, #2349's knobs): APPENDED, never inserted — every field above is positional and read
           by ordinal, so placing these anywhere but the end would silently re-map all of them. */
        bool FileGrowthEnabled,
        int FileGrowthRiseMb,
        int FileGrowthVolumePercent,
        int FileGrowthLookbackMinutes);

    /// <summary>The single global alert-settings row (id=1) — the viewer's <c>AlertSettingsSelectSql</c>. The
    /// 58 columns are read in the SAME order the service reads them (<c>StoreConfigProvider</c>). This had
    /// stopped at 36, so <c>get_alert_settings</c> reported a store whose newest five knobs did not exist:
    /// an MCP client could not see the V33 connection opt-ins or the V35 Availability Group family at all.</summary>
    public const string AlertSettingsSelectSql = @"
SELECT enabled, cpu_enabled, cpu_threshold_percent, cpu_mode, blocking_enabled, blocking_count_threshold,
       deadlock_enabled, deadlock_count_threshold, poison_wait_enabled, poison_wait_threshold_ms,
       long_running_query_enabled, long_running_query_threshold_minutes, tempdb_space_enabled,
       tempdb_space_threshold_percent, low_disk_enabled, low_disk_threshold_percent, low_disk_threshold_gb,
       long_running_job_enabled, long_running_job_multiplier, failed_job_enabled, failed_job_lookback_minutes,
       cooldown_minutes, excluded_databases, analysis_enabled, analysis_interval_minutes,
       analysis_notifications_enabled, analysis_notify_severity, delivery_mode, per_event_max,
       long_running_query_max_results, long_running_query_exclude_sp_server_diagnostics,
       long_running_query_exclude_wait_for, long_running_query_exclude_backups,
       long_running_query_exclude_misc_waits, long_running_query_exclude_cdc, notify_connection_changes,
       notify_connection_down_at_startup, connection_refire_minutes,
       notify_ag_health, ag_lag_alert_seconds, ag_redo_queue_alert_kb,
       ag_disconnect_refire_minutes, blocking_wait_seconds_threshold, pvs_enabled, pvs_threshold_percent,
       pvs_floor_gb, database_state_enabled,
       self_disk_free_warn_percent, collection_stale_minutes, collection_failure_threshold,
       disk_critical_free_percent, disk_critical_free_gb, analysis_notify_cooldown_minutes,
       store_job_cadence_warn_percent,
       file_growth_enabled, file_growth_rise_mb, file_growth_volume_percent, file_growth_lookback_minutes
FROM config_alert_settings
WHERE id = 1";

    /// <summary>Reads the single global alert-settings row, or null when the store has not seeded it yet
    /// (a pre-control-plane store, or the service has not started).</summary>
    public static async Task<AlertSettingsReadRow?> GetAlertSettingsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(AlertSettingsSelectSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new AlertSettingsReadRow(
            reader.GetBoolean(0), reader.GetBoolean(1), reader.GetInt32(2), reader.GetString(3),
            reader.GetBoolean(4), reader.GetInt32(5), reader.GetBoolean(6), reader.GetInt32(7),
            reader.GetBoolean(8), reader.GetInt32(9), reader.GetBoolean(10), reader.GetInt32(11),
            reader.GetBoolean(12), reader.GetInt32(13), reader.GetBoolean(14), reader.GetInt32(15), reader.GetInt32(16),
            reader.GetBoolean(17), reader.GetInt32(18), reader.GetBoolean(19), reader.GetInt32(20),
            reader.GetInt32(21), reader.IsDBNull(22) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(22),
            reader.GetBoolean(23), reader.GetInt32(24), reader.GetBoolean(25), reader.GetDouble(26),
            reader.GetString(27), reader.GetInt32(28), reader.GetInt32(29), reader.GetBoolean(30),
            reader.GetBoolean(31), reader.GetBoolean(32), reader.GetBoolean(33), reader.GetBoolean(34),
            reader.GetBoolean(35),
            /* V33 (#1659) at 36-37, V35 (#991) at 38-40, V37 (#1696) at 41, V40 (#1839) at 42,
               V48 (#1984) at 43-45, database-state alert master switch (V49) at 46. */
            reader.GetBoolean(36), reader.GetInt32(37),
            reader.GetBoolean(38), reader.GetInt32(39), reader.GetInt64(40),
            reader.GetInt32(41),
            reader.GetInt32(42),
            reader.GetBoolean(43), reader.GetInt32(44), reader.GetInt32(45),
            reader.GetBoolean(46),
            /* #2107 threshold knobs (V55) at 47–52; #2136 cadence-warn knob (V57) at 53. */
            reader.GetInt32(47), reader.GetInt32(48), reader.GetInt32(49),
            reader.GetInt32(50), reader.GetInt32(51), reader.GetInt32(52),
            reader.GetInt32(53),
            /* #2391: V79 file-growth knobs at 54–57. */
            reader.GetBoolean(54), reader.GetInt32(55), reader.GetInt32(56), reader.GetInt32(57));
    }
}
