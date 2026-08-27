/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets alert history from the config_alert_log table (excludes dismissed alerts).
    /// </summary>
    public async Task<List<AlertHistoryRow>> GetAlertHistoryAsync(int hoursBack = 24, int limit = 500, int? serverId = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* Both edges, not just the lower one: the row cap is applied by the database, so trimming
           after the read would spend the whole LIMIT on rows newer than an as_of anchor and hand back an
           empty window that looks exactly like a quiet one. */
        var (cutoff, until) = GetTimeRange(hoursBack, null, null, asOfUtc);

        if (serverId.HasValue)
        {
            command.CommandText = @"
SELECT
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
    detail_text,
    source,
    context_json
FROM v_config_alert_log
WHERE alert_time >= $1
AND   alert_time <= $2
AND   server_id = $3
AND   dismissed = FALSE
ORDER BY alert_time DESC
LIMIT $4";
            command.Parameters.Add(new DuckDBParameter { Value = cutoff });
            command.Parameters.Add(new DuckDBParameter { Value = until });
            command.Parameters.Add(new DuckDBParameter { Value = serverId.Value });
            command.Parameters.Add(new DuckDBParameter { Value = limit });
        }
        else
        {
            command.CommandText = @"
SELECT
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
    detail_text,
    source,
    context_json
FROM v_config_alert_log
WHERE alert_time >= $1
AND   alert_time <= $2
AND   dismissed = FALSE
ORDER BY alert_time DESC
LIMIT $3";
            command.Parameters.Add(new DuckDBParameter { Value = cutoff });
            command.Parameters.Add(new DuckDBParameter { Value = until });
            command.Parameters.Add(new DuckDBParameter { Value = limit });
        }

        var items = new List<AlertHistoryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new AlertHistoryRow
            {
                AlertTime = reader.GetDateTime(0),
                ServerId = (int)ToInt64(reader.GetValue(1)),
                ServerName = reader.GetString(2),
                MetricName = reader.GetString(3),
                CurrentValue = Convert.ToDouble(reader.GetValue(4)),
                ThresholdValue = Convert.ToDouble(reader.GetValue(5)),
                AlertSent = reader.GetBoolean(6),
                NotificationType = reader.GetString(7),
                SendError = reader.IsDBNull(8) ? null : reader.GetString(8),
                Muted = !reader.IsDBNull(9) && reader.GetBoolean(9),
                DetailText = reader.IsDBNull(10) ? null : reader.GetString(10),
                Source = reader.IsDBNull(11) ? "live" : reader.GetString(11),
                ContextJson = reader.IsDBNull(12) ? null : reader.GetString(12)
            });
        }

        return items;
    }

    /// <summary>
    /// Dismisses specific alerts by marking them as dismissed in DuckDB.
    /// Uses a single batched UPDATE with an exclusive write lock and transaction
    /// to prevent race conditions with archival and ensure all-or-nothing semantics.
    /// If alerts only exist in archived parquet, inserts into dismissed_archive_alerts instead.
    /// Logs structured telemetry and verifies dismissal success.
    /// </summary>
    public async Task<int> DismissAlertsAsync(List<AlertHistoryRow> alerts)
    {
        if (alerts.Count == 0) return 0;

        var sw = System.Diagnostics.Stopwatch.StartNew();

        if (App.LogAlertDismissals)
            AppLogger.Info("AlertDismiss", $"Action=DismissSelected, Requested={alerts.Count}");

        using var connection = await OpenWriteConnectionAsync();
        int archivedDismissed = 0;

        using var beginCmd = connection.CreateCommand();
        beginCmd.CommandText = "BEGIN TRANSACTION";
        await beginCmd.ExecuteNonQueryAsync();

        try
        {
            // Build a single batched UPDATE using VALUES list
            var valuesClauses = new System.Text.StringBuilder();
            var parameters = new List<DuckDBParameter>();
            for (int i = 0; i < alerts.Count; i++)
            {
                if (i > 0) valuesClauses.Append(", ");
                var p1 = $"${i * 3 + 1}";
                var p2 = $"${i * 3 + 2}";
                var p3 = $"${i * 3 + 3}";
                valuesClauses.Append($"({p1}, {p2}, {p3})");
                parameters.Add(new DuckDBParameter { Value = alerts[i].AlertTime });
                parameters.Add(new DuckDBParameter { Value = alerts[i].ServerId });
                parameters.Add(new DuckDBParameter { Value = alerts[i].MetricName });
            }

            using var command = connection.CreateCommand();
            command.CommandText = $@"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  dismissed = FALSE
AND    (alert_time, server_id, metric_name) IN (VALUES {valuesClauses})";
            foreach (var p in parameters)
                command.Parameters.Add(p);

            var totalAffected = await command.ExecuteNonQueryAsync();

            // Sidecar fallback for alerts that weren't in the live table (archived to parquet)
            if (totalAffected < alerts.Count)
            {
                foreach (var alert in alerts)
                {
                    using var sidecarCmd = connection.CreateCommand();
                    sidecarCmd.CommandText = @"
INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name)
SELECT $1, $2, $3
WHERE NOT EXISTS (
    SELECT 1 FROM config_alert_log
    WHERE  alert_time = $1 AND server_id = $2 AND metric_name = $3
)
AND NOT EXISTS (
    SELECT 1 FROM dismissed_archive_alerts
    WHERE  alert_time = $1 AND server_id = $2 AND metric_name = $3
)";
                    sidecarCmd.Parameters.Add(new DuckDBParameter { Value = alert.AlertTime });
                    sidecarCmd.Parameters.Add(new DuckDBParameter { Value = alert.ServerId });
                    sidecarCmd.Parameters.Add(new DuckDBParameter { Value = alert.MetricName });
                    archivedDismissed += await sidecarCmd.ExecuteNonQueryAsync();
                }
            }

            using var commitCmd = connection.CreateCommand();
            commitCmd.CommandText = "COMMIT";
            await commitCmd.ExecuteNonQueryAsync();

            sw.Stop();

            if (App.LogAlertDismissals)
                AppLogger.Info("AlertDismiss", $"Action=DismissSelected, Result=Complete, Requested={alerts.Count}, LiveUpdated={totalAffected}, ArchivedDismissed={archivedDismissed}, Duration={sw.ElapsedMilliseconds}ms");

            // Post-dismiss verification: confirm the dismissed rows are no longer visible
            if (totalAffected > 0)
            {
                await VerifyDismissAsync(connection, alerts, totalAffected);
            }

            return totalAffected + archivedDismissed;
        }
        catch
        {
            try
            {
                using var rollbackCmd = connection.CreateCommand();
                rollbackCmd.CommandText = "ROLLBACK";
                await rollbackCmd.ExecuteNonQueryAsync();
            }
            catch (Exception rbEx)
            {
                AppLogger.Error("AlertDismiss", $"Rollback failed: {rbEx.Message}");
            }
            throw;
        }
    }

    /// <summary>
    /// Dismisses all visible (non-dismissed) alerts matching the current filter criteria.
    /// Uses an exclusive write lock to prevent race conditions with archival.
    /// Updates the live table, then inserts any remaining archived alerts into the sidecar table.
    /// Logs structured telemetry and verifies dismissal success.
    /// </summary>
    public async Task<int> DismissAllVisibleAlertsAsync(int hoursBack, int? serverId = null)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        if (App.LogAlertDismissals)
            AppLogger.Info("AlertDismiss", $"Action=DismissAll, HoursBack={hoursBack}, ServerId={serverId?.ToString() ?? "all"}");

        using var connection = await OpenWriteConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        if (serverId.HasValue)
        {
            command.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  alert_time >= $1
AND    server_id = $2
AND    dismissed = FALSE";
            command.Parameters.Add(new DuckDBParameter { Value = cutoff });
            command.Parameters.Add(new DuckDBParameter { Value = serverId.Value });
        }
        else
        {
            command.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  alert_time >= $1
AND    dismissed = FALSE";
            command.Parameters.Add(new DuckDBParameter { Value = cutoff });
        }

        var liveAffected = await command.ExecuteNonQueryAsync();

        // Dismiss any remaining archived alerts that matched the filter
        using var sidecarCmd = connection.CreateCommand();
        if (serverId.HasValue)
        {
            sidecarCmd.CommandText = @"
INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name)
SELECT v.alert_time, v.server_id, v.metric_name
FROM   v_config_alert_log v
WHERE  v.alert_time >= $1
AND    v.server_id = $2
AND    v.dismissed = FALSE
AND    NOT EXISTS (
    SELECT 1 FROM config_alert_log l
    WHERE  l.alert_time = v.alert_time
    AND    l.server_id  = v.server_id
    AND    l.metric_name = v.metric_name
)
AND    NOT EXISTS (
    SELECT 1 FROM dismissed_archive_alerts d
    WHERE  d.alert_time = v.alert_time
    AND    d.server_id  = v.server_id
    AND    d.metric_name = v.metric_name
)";
            sidecarCmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
            sidecarCmd.Parameters.Add(new DuckDBParameter { Value = serverId.Value });
        }
        else
        {
            sidecarCmd.CommandText = @"
INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name)
SELECT v.alert_time, v.server_id, v.metric_name
FROM   v_config_alert_log v
WHERE  v.alert_time >= $1
AND    v.dismissed = FALSE
AND    NOT EXISTS (
    SELECT 1 FROM config_alert_log l
    WHERE  l.alert_time = v.alert_time
    AND    l.server_id  = v.server_id
    AND    l.metric_name = v.metric_name
)
AND    NOT EXISTS (
    SELECT 1 FROM dismissed_archive_alerts d
    WHERE  d.alert_time = v.alert_time
    AND    d.server_id  = v.server_id
    AND    d.metric_name = v.metric_name
)";
            sidecarCmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
        }

        var archivedAffected = await sidecarCmd.ExecuteNonQueryAsync();
        sw.Stop();

        if (App.LogAlertDismissals)
            AppLogger.Info("AlertDismiss", $"Action=DismissAll, Result=Complete, LiveUpdated={liveAffected}, ArchivedDismissed={archivedAffected}, Cutoff={cutoff:O}, Duration={sw.ElapsedMilliseconds}ms");

        // Post-dismiss verification: confirm no undismissed live rows remain
        if (liveAffected > 0)
        {
            await VerifyDismissAllAsync(connection, cutoff, serverId, liveAffected);
        }

        return liveAffected + archivedAffected;
    }

    /// <summary>
    /// Verifies that specific dismissed alerts are no longer in undismissed state.
    /// </summary>
    private static async System.Threading.Tasks.Task VerifyDismissAsync(LockedConnection connection, List<AlertHistoryRow> alerts, int expectedDismissed)
    {
        try
        {
            // Check how many of the targeted alerts are still undismissed
            int stillUndismissed = 0;
            foreach (var alert in alerts)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = @"
SELECT COUNT(1) FROM config_alert_log
WHERE  alert_time = $1
AND    server_id = $2
AND    metric_name = $3
AND    dismissed = FALSE";
                cmd.Parameters.Add(new DuckDBParameter { Value = alert.AlertTime });
                cmd.Parameters.Add(new DuckDBParameter { Value = alert.ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = alert.MetricName });
                var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
                if (count > 0) stillUndismissed++;
            }

            if (stillUndismissed > 0)
                AppLogger.Warn("AlertDismiss", $"Action=DismissVerify, Result=Mismatch, StillUndismissed={stillUndismissed}, ExpectedDismissed={expectedDismissed}");
            else if (App.LogAlertDismissals)
                AppLogger.Info("AlertDismiss", $"Action=DismissVerify, Result=Verified, Confirmed={expectedDismissed}");
        }
        catch (Exception ex)
        {
            AppLogger.Warn("AlertDismiss", $"Action=DismissVerify, Result=Error, Message={ex.Message}");
        }
    }

    /// <summary>
    /// How long a <c>dismissed_archive_alerts</c> row is kept, keyed on the <c>alert_time</c> of the
    /// alert it suppresses. The horizon MUST outlive the parquet archives, because suppressing an
    /// archived alert is the row's entire job: purge it while its alert is still readable through
    /// <c>v_config_alert_log</c> and an alert the user already dismissed reappears. Parquet is pruned
    /// at 3 months (<see cref="RetentionService.CleanupOldArchives"/>) on the FILE's date prefix, and
    /// a file's prefix is the archive date — always newer than the rows inside it, by 7 days in
    /// steady state and by however long the app was closed otherwise — so real parquet lifetime runs
    /// past 3 months from alert_time. 180 days clears that with room to spare while still bounding a
    /// table that previously had no purge path at all (#1651). Growth is user-paced (one row per
    /// dismissed archived alert), so a generous horizon costs nothing.
    /// </summary>
    internal const int DismissedArchiveAlertRetentionDays = 180;

    /// <summary>
    /// Purges <c>dismissed_archive_alerts</c> rows whose alert is older than
    /// <paramref name="retentionDays"/> — rows whose parquet has long since been pruned, so they can
    /// never match anything again. This is the table's ONLY delete path: it is deliberately in
    /// <see cref="ArchiveService"/>'s PreservedConfigTables (it must survive the 512 MB emergency
    /// reset, or every dismissal a user ever made comes back at once), which is exactly why it needs
    /// an age horizon of its own — nothing else ever shrinks it. Returns the rows removed.
    /// </summary>
    public async Task<int> PurgeOldDismissedArchiveAlertsAsync(int retentionDays = DismissedArchiveAlertRetentionDays)
    {
        using var connection = await OpenWriteConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM dismissed_archive_alerts WHERE alert_time < $1";
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-retentionDays) });
        return await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Verifies that no undismissed alerts remain in the dismissed time range.
    /// </summary>
    private static async System.Threading.Tasks.Task VerifyDismissAllAsync(LockedConnection connection, DateTime cutoff, int? serverId, int expectedDismissed)
    {
        try
        {
            using var cmd = connection.CreateCommand();
            if (serverId.HasValue)
            {
                cmd.CommandText = @"
SELECT COUNT(1) FROM config_alert_log
WHERE  alert_time >= $1
AND    server_id = $2
AND    dismissed = FALSE";
                cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
                cmd.Parameters.Add(new DuckDBParameter { Value = serverId.Value });
            }
            else
            {
                cmd.CommandText = @"
SELECT COUNT(1) FROM config_alert_log
WHERE  alert_time >= $1
AND    dismissed = FALSE";
                cmd.Parameters.Add(new DuckDBParameter { Value = cutoff });
            }

            var remaining = Convert.ToInt64(await cmd.ExecuteScalarAsync());
            if (remaining > 0)
                AppLogger.Warn("AlertDismiss", $"Action=DismissAllVerify, Result=Mismatch, StillUndismissed={remaining}, ExpectedDismissed={expectedDismissed}");
            else if (App.LogAlertDismissals)
                AppLogger.Info("AlertDismiss", $"Action=DismissAllVerify, Result=Verified, Confirmed={expectedDismissed}");
        }
        catch (Exception ex)
        {
            AppLogger.Warn("AlertDismiss", $"Action=DismissAllVerify, Result=Error, Message={ex.Message}");
        }
    }
}

public class AlertHistoryRow
{
    public DateTime AlertTime { get; set; }
    public int ServerId { get; set; }
    public string ServerName { get; set; } = "";
    public string MetricName { get; set; } = "";
    public double CurrentValue { get; set; }
    public double ThresholdValue { get; set; }
    public bool AlertSent { get; set; }
    public string NotificationType { get; set; } = "";
    public string? SendError { get; set; }
    public bool Muted { get; set; }
    public string? DetailText { get; set; }
    public string Source { get; set; } = "live";
    public string? ContextJson { get; set; }

    public bool IsArchived => string.Equals(Source, "archive", StringComparison.OrdinalIgnoreCase);

    public string TimeLocal => AlertTime.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
    public string CurrentValueDisplay => AlertMetricClassifier.FormatHistoryValue(MetricName, CurrentValue);
    public string ThresholdValueDisplay => AlertMetricClassifier.FormatHistoryValue(MetricName, ThresholdValue);

    public string StatusDisplay
    {
        get
        {
            if (NotificationType == "email")
                return AlertSent ? "Sent" : (!string.IsNullOrEmpty(SendError) ? "Failed" : "Not sent");
            return AlertSent ? "Delivered" : "Shown";
        }
    }

    public bool IsResolved => AlertMetricClassifier.IsResolution(MetricName);
    public bool IsCritical => AlertMetricClassifier.IsCritical(MetricName);
    public bool IsWarning => AlertMetricClassifier.IsWarning(MetricName);

}
