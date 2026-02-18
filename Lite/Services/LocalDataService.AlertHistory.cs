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
    send_error
FROM config_alert_log
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
    send_error
FROM config_alert_log
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
                SendError = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }

        return items;
    }

    /// <summary>
    /// Dismisses specific alerts by marking them as dismissed in DuckDB.
    /// Identifies rows by (alert_time, server_id, metric_name) composite key.
    /// </summary>
    public async Task DismissAlertsAsync(List<AlertHistoryRow> alerts)
    {
        if (alerts.Count == 0) return;

        using var connection = await OpenConnectionAsync();

        foreach (var alert in alerts)
        {
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  alert_time = $1
AND    server_id = $2
AND    metric_name = $3";
            command.Parameters.Add(new DuckDBParameter { Value = alert.AlertTime });
            command.Parameters.Add(new DuckDBParameter { Value = alert.ServerId });
            command.Parameters.Add(new DuckDBParameter { Value = alert.MetricName });
            await command.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Dismisses all visible (non-dismissed) alerts matching the current filter criteria.
    /// </summary>
    public async Task DismissAllVisibleAlertsAsync(int hoursBack, int? serverId = null)
    {
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

        await command.ExecuteNonQueryAsync();
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
