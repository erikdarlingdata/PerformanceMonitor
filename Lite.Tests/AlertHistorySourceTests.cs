using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests that the archive view source column correctly distinguishes live vs archived alerts,
/// and that dismiss operations only target live alerts.
/// </summary>
public class AlertHistorySourceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly TestAlertDataHelper _helper;

    public AlertHistorySourceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _helper = new TestAlertDataHelper(_dbPath);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private async Task<DuckDBConnection> InitializeDatabaseAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    [Fact]
    public async Task LiveAlerts_HaveSourceLive()
    {
        using var connection = await InitializeDatabaseAsync();

        var recentTime = DateTime.UtcNow.AddHours(-1);
        await _helper.InsertLiveAlertAsync(connection, recentTime, 1, "Server1", "High CPU");

        await _helper.RefreshArchiveViewsAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT source FROM v_config_alert_log WHERE metric_name = 'High CPU'";
        var source = (string)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;

        Assert.Equal("live", source);
    }

    [Fact]
    public async Task ArchivedAlerts_HaveSourceArchive()
    {
        using var connection = await InitializeDatabaseAsync();

        var oldAlerts = new List<TestAlertRecord>
        {
            TestAlertDataHelper.CreateAlert(
                alertTime: DateTime.UtcNow.AddDays(-14),
                metricName: "Blocking",
                serverName: "Server1")
        };

        await _helper.CreateArchivedAlertsParquetAsync(connection, oldAlerts);
        await _helper.RefreshArchiveViewsAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT source FROM v_config_alert_log WHERE metric_name = 'Blocking'";
        var source = (string)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;

        Assert.Equal("archive", source);
    }

    [Fact]
    public async Task MixedAlerts_CorrectSourcePerRow()
    {
        using var connection = await InitializeDatabaseAsync();

        // Insert a live alert (recent)
        await _helper.InsertLiveAlertAsync(
            connection,
            DateTime.UtcNow.AddHours(-2),
            1, "Server1", "High CPU");

        // Create an archived alert (old)
        var archivedAlerts = new List<TestAlertRecord>
        {
            TestAlertDataHelper.CreateAlert(
                alertTime: DateTime.UtcNow.AddDays(-14),
                metricName: "Deadlock Detected",
                serverName: "Server1")
        };
        await _helper.CreateArchivedAlertsParquetAsync(connection, archivedAlerts);
        await _helper.RefreshArchiveViewsAsync();

        // Query both and verify sources
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT metric_name, source
FROM v_config_alert_log
ORDER BY alert_time DESC";

        var results = new List<(string MetricName, string Source)>();
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            results.Add((reader.GetString(0), reader.GetString(1)));
        }

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.MetricName == "High CPU" && r.Source == "live");
        Assert.Contains(results, r => r.MetricName == "Deadlock Detected" && r.Source == "archive");
    }

    [Fact]
    public async Task DismissUpdate_OnlyAffectsLiveTable()
    {
        using var connection = await InitializeDatabaseAsync();

        // Insert a live alert
        var liveTime = DateTime.UtcNow.AddHours(-1);
        await _helper.InsertLiveAlertAsync(
            connection, liveTime, 1, "Server1", "High CPU");

        // Create an archived alert
        var archiveTime = DateTime.UtcNow.AddDays(-14);
        var archivedAlerts = new List<TestAlertRecord>
        {
            TestAlertDataHelper.CreateAlert(
                alertTime: archiveTime,
                metricName: "Blocking",
                serverName: "Server1")
        };
        await _helper.CreateArchivedAlertsParquetAsync(connection, archivedAlerts);
        await _helper.RefreshArchiveViewsAsync();

        // Dismiss the live alert
        using var dismissCmd = connection.CreateCommand();
        dismissCmd.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  metric_name = 'High CPU'
AND    dismissed = FALSE";
        var affected = await dismissCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, affected);

        // Attempt to dismiss the archived alert — should affect 0 rows
        using var dismissArchiveCmd = connection.CreateCommand();
        dismissArchiveCmd.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  metric_name = 'Blocking'
AND    dismissed = FALSE";
        var archivedAffected = await dismissArchiveCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, archivedAffected);

        // The archived alert should still be visible in the view (undismissed)
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = @"
SELECT COUNT(1)
FROM v_config_alert_log
WHERE metric_name = 'Blocking'
AND   dismissed = FALSE";
        var stillVisible = Convert.ToInt64(await checkCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, stillVisible);
    }

    [Fact]
    public async Task ViewWithNoParquet_AllRowsAreLive()
    {
        using var connection = await InitializeDatabaseAsync();

        // No parquet files — just live data
        await _helper.InsertLiveAlertAsync(
            connection, DateTime.UtcNow.AddHours(-1), 1, "Server1", "High CPU");
        await _helper.InsertLiveAlertAsync(
            connection, DateTime.UtcNow.AddHours(-2), 2, "Server2", "TempDB Space");

        await _helper.RefreshArchiveViewsAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(1) FROM v_config_alert_log WHERE source = 'live'";
        var liveCount = Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, liveCount);

        using var archiveCmd = connection.CreateCommand();
        archiveCmd.CommandText = "SELECT COUNT(1) FROM v_config_alert_log WHERE source = 'archive'";
        var archiveCount = Convert.ToInt64(await archiveCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, archiveCount);
    }

    [Fact]
    public async Task AlertHistoryRow_IsArchived_ReflectsSource()
    {
        var liveRow = new AlertHistoryRow { Source = "live" };
        var archiveRow = new AlertHistoryRow { Source = "archive" };
        var defaultRow = new AlertHistoryRow();

        Assert.False(liveRow.IsArchived);
        Assert.True(archiveRow.IsArchived);
        Assert.False(defaultRow.IsArchived);

        await Task.CompletedTask;
    }

    [Fact]
    public async Task MultipleParquetFiles_AllMarkedAsArchive()
    {
        using var connection = await InitializeDatabaseAsync();

        // Create two separate parquet files (simulating multiple archive cycles)
        var batch1 = new List<TestAlertRecord>
        {
            TestAlertDataHelper.CreateAlert(
                alertTime: DateTime.UtcNow.AddDays(-14),
                metricName: "High CPU",
                serverName: "Server1"),
            TestAlertDataHelper.CreateAlert(
                alertTime: DateTime.UtcNow.AddDays(-13),
                metricName: "Blocking",
                serverName: "Server1")
        };

        var batch2 = new List<TestAlertRecord>
        {
            TestAlertDataHelper.CreateAlert(
                alertTime: DateTime.UtcNow.AddDays(-21),
                metricName: "Deadlock Detected",
                serverName: "Server2")
        };

        await _helper.CreateArchivedAlertsParquetAsync(
            connection, batch1, "20260301_0000_config_alert_log.parquet");
        await _helper.CreateArchivedAlertsParquetAsync(
            connection, batch2, "20260215_0000_config_alert_log.parquet");

        await _helper.RefreshArchiveViewsAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT COUNT(1) FROM v_config_alert_log WHERE source = 'archive'";
        var archiveCount = Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(3, archiveCount);
    }
}
