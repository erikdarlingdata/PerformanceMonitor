using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Tests.Helpers;

/// <summary>
/// Helper for inserting test alert data into DuckDB live tables and archived parquet files.
/// </summary>
public class TestAlertDataHelper
{
    private readonly string _dbPath;
    private readonly string _archivePath;

    public TestAlertDataHelper(string dbPath)
    {
        _dbPath = dbPath;
        _archivePath = Path.Combine(Path.GetDirectoryName(dbPath) ?? ".", "archive");
        Directory.CreateDirectory(_archivePath);
    }

    public string ArchivePath => _archivePath;

    /// <summary>
    /// Inserts a single alert row into the live config_alert_log table.
    /// </summary>
    public async Task InsertLiveAlertAsync(
        DuckDBConnection connection,
        DateTime alertTime,
        int serverId,
        string serverName,
        string metricName,
        double currentValue = 95.0,
        double thresholdValue = 80.0,
        bool alertSent = true,
        string notificationType = "tray",
        string? sendError = null,
        bool dismissed = false,
        bool muted = false,
        string? detailText = null)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value,
     threshold_value, alert_sent, notification_type, send_error,
     dismissed, muted, detail_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = alertTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = metricName });
        cmd.Parameters.Add(new DuckDBParameter { Value = currentValue });
        cmd.Parameters.Add(new DuckDBParameter { Value = thresholdValue });
        cmd.Parameters.Add(new DuckDBParameter { Value = alertSent });
        cmd.Parameters.Add(new DuckDBParameter { Value = notificationType });
        cmd.Parameters.Add(new DuckDBParameter { Value = sendError ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = dismissed });
        cmd.Parameters.Add(new DuckDBParameter { Value = muted });
        cmd.Parameters.Add(new DuckDBParameter { Value = detailText ?? (object)DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Creates a parquet file in the archive directory containing the specified alert rows.
    /// Uses a staging table to avoid touching the live config_alert_log table.
    /// The parquet file is named to match the ArchiveService naming convention.
    /// </summary>
    public async Task CreateArchivedAlertsParquetAsync(
        DuckDBConnection connection,
        List<TestAlertRecord> alerts,
        string? parquetFileName = null)
    {
        parquetFileName ??= $"20260101_0000_config_alert_log.parquet";
        var parquetPath = Path.Combine(_archivePath, parquetFileName).Replace("\\", "/");

        using var createCmd = connection.CreateCommand();
        createCmd.CommandText = @"
CREATE TEMP TABLE _staging_alerts (
    alert_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    metric_name VARCHAR NOT NULL,
    current_value DOUBLE NOT NULL,
    threshold_value DOUBLE NOT NULL,
    alert_sent BOOLEAN NOT NULL DEFAULT false,
    notification_type VARCHAR NOT NULL DEFAULT 'tray',
    send_error VARCHAR,
    dismissed BOOLEAN NOT NULL DEFAULT false,
    muted BOOLEAN NOT NULL DEFAULT false,
    detail_text VARCHAR
)";
        await createCmd.ExecuteNonQueryAsync();

        foreach (var alert in alerts)
        {
            using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = @"
INSERT INTO _staging_alerts
    (alert_time, server_id, server_name, metric_name, current_value,
     threshold_value, alert_sent, notification_type, send_error,
     dismissed, muted, detail_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.AlertTime });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.ServerId });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.ServerName });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.MetricName });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.CurrentValue });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.ThresholdValue });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.AlertSent });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.NotificationType });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.SendError ?? (object)DBNull.Value });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.Dismissed });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.Muted });
            insertCmd.Parameters.Add(new DuckDBParameter { Value = alert.DetailText ?? (object)DBNull.Value });
            await insertCmd.ExecuteNonQueryAsync();
        }

        using var copyCmd = connection.CreateCommand();
        copyCmd.CommandText = $"COPY _staging_alerts TO '{parquetPath}' (FORMAT PARQUET)";
        await copyCmd.ExecuteNonQueryAsync();

        using var dropCmd = connection.CreateCommand();
        dropCmd.CommandText = "DROP TABLE _staging_alerts";
        await dropCmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Recreates archive views so they pick up newly created parquet files.
    /// </summary>
    public async Task RefreshArchiveViewsAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.CreateArchiveViewsAsync();
    }

    /// <summary>
    /// Creates a test alert record with sensible defaults. Adjust properties as needed.
    /// </summary>
    public static TestAlertRecord CreateAlert(
        DateTime? alertTime = null,
        int serverId = 1,
        string serverName = "TestServer",
        string metricName = "High CPU",
        double currentValue = 95.0,
        double thresholdValue = 80.0,
        bool dismissed = false,
        bool muted = false,
        string? detailText = null)
    {
        return new TestAlertRecord
        {
            AlertTime = alertTime ?? DateTime.UtcNow.AddDays(-10),
            ServerId = serverId,
            ServerName = serverName,
            MetricName = metricName,
            CurrentValue = currentValue,
            ThresholdValue = thresholdValue,
            AlertSent = true,
            NotificationType = "tray",
            SendError = null,
            Dismissed = dismissed,
            Muted = muted,
            DetailText = detailText
        };
    }
}

/// <summary>
/// Plain record for constructing test alert data.
/// </summary>
public class TestAlertRecord
{
    public DateTime AlertTime { get; set; }
    public int ServerId { get; set; }
    public string ServerName { get; set; } = "TestServer";
    public string MetricName { get; set; } = "High CPU";
    public double CurrentValue { get; set; } = 95.0;
    public double ThresholdValue { get; set; } = 80.0;
    public bool AlertSent { get; set; } = true;
    public string NotificationType { get; set; } = "tray";
    public string? SendError { get; set; }
    public bool Dismissed { get; set; }
    public bool Muted { get; set; }
    public string? DetailText { get; set; }
}
