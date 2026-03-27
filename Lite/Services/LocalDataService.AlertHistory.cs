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
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets alert history from the config_alert_log table (excludes dismissed alerts).
    /// </summary>
    public async Task<List<AlertHistoryRow>> GetAlertHistoryAsync(int hoursBack = 24, int limit = 500, int? serverId = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

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
    source
FROM v_config_alert_log
WHERE alert_time >= $1
AND   server_id = $2
AND   dismissed = FALSE
ORDER BY alert_time DESC
LIMIT $3";
            command.Parameters.Add(new DuckDBParameter { Value = cutoff });
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
    source
FROM v_config_alert_log
WHERE alert_time >= $1
AND   dismissed = FALSE
ORDER BY alert_time DESC
LIMIT $2";
            command.Parameters.Add(new DuckDBParameter { Value = cutoff });
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
                Source = reader.IsDBNull(11) ? "live" : reader.GetString(11)
            });
        }

        return items;
    }

    /// <summary>
    /// Dismisses specific alerts by marking them as dismissed in DuckDB.
    /// Identifies rows by (alert_time, server_id, metric_name) composite key.
    /// If an alert only exists in archived parquet, inserts into dismissed_archive_alerts instead.
    /// Logs structured telemetry and verifies dismissal success.
    /// </summary>
    public async Task<int> DismissAlertsAsync(List<AlertHistoryRow> alerts)
    {
        if (alerts.Count == 0) return 0;

        var sw = System.Diagnostics.Stopwatch.StartNew();

        if (App.LogAlertDismissals)
            AppLogger.Info("AlertDismiss", $"Action=DismissSelected, Requested={alerts.Count}");

        using var connection = await OpenConnectionAsync();
        int totalAffected = 0;
        int archivedDismissed = 0;

        foreach (var alert in alerts)
        {
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  alert_time = $1
AND    server_id = $2
AND    metric_name = $3
AND    dismissed = FALSE";
            command.Parameters.Add(new DuckDBParameter { Value = alert.AlertTime });
            command.Parameters.Add(new DuckDBParameter { Value = alert.ServerId });
            command.Parameters.Add(new DuckDBParameter { Value = alert.MetricName });
            var affected = await command.ExecuteNonQueryAsync();
            totalAffected += affected;

            if (affected == 0)
            {
                using var sidecarCmd = connection.CreateCommand();
                sidecarCmd.CommandText = @"
INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name)
SELECT $1, $2, $3
WHERE NOT EXISTS (
    SELECT 1 FROM dismissed_archive_alerts
    WHERE  alert_time = $1
    AND    server_id  = $2
    AND    metric_name = $3
)";
                sidecarCmd.Parameters.Add(new DuckDBParameter { Value = alert.AlertTime });
                sidecarCmd.Parameters.Add(new DuckDBParameter { Value = alert.ServerId });
                sidecarCmd.Parameters.Add(new DuckDBParameter { Value = alert.MetricName });
                var sidecarAffected = await sidecarCmd.ExecuteNonQueryAsync();
                archivedDismissed += sidecarAffected;

                if (App.LogAlertDismissals)
                    AppLogger.Info("AlertDismiss", $"Action=DismissSelected, Result=SidecarInsert, AlertTime={alert.AlertTime:O}, ServerId={alert.ServerId}, Metric={alert.MetricName}");
            }
        }

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

    /// <summary>
    /// Dismisses all visible (non-dismissed) alerts matching the current filter criteria.
    /// Updates the live table, then inserts any remaining archived alerts into the sidecar table.
    /// Logs structured telemetry and verifies dismissal success.
    /// </summary>
    public async Task<int> DismissAllVisibleAlertsAsync(int hoursBack, int? serverId = null)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        if (App.LogAlertDismissals)
            AppLogger.Info("AlertDismiss", $"Action=DismissAll, HoursBack={hoursBack}, ServerId={serverId?.ToString() ?? "all"}");

        using var connection = await OpenConnectionAsync();
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

    public bool IsArchived => string.Equals(Source, "archive", StringComparison.OrdinalIgnoreCase);

    public string TimeLocal => AlertTime.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
    public string CurrentValueDisplay => FormatValue(MetricName, CurrentValue);
    public string ThresholdValueDisplay => FormatValue(MetricName, ThresholdValue);

    public string StatusDisplay
    {
        get
        {
            if (NotificationType == "email")
                return AlertSent ? "Sent" : (!string.IsNullOrEmpty(SendError) ? "Failed" : "Not sent");
            return AlertSent ? "Delivered" : "Shown";
        }
    }

    public bool IsResolved => MetricName.Contains("Cleared") || MetricName.Contains("Resolved");
    public bool IsCritical => MetricName.Contains("Deadlock") || MetricName.Contains("Poison");
    public bool IsWarning => !IsResolved && !IsCritical;

    private static string FormatValue(string metricName, double value)
    {
        if (metricName.Contains("CPU")) return $"{value:F0}%";
        if (metricName.Contains("TempDB")) return $"{value:F0}%";
        return $"{value:G}";
    }
}
