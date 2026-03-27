using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests that the dismissed_archive_alerts sidecar table allows archived alerts
/// to be dismissed, and that the archive view filters them out correctly.
/// </summary>
public class DismissedArchiveSidecarTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly TestAlertDataHelper _helper;

    public DismissedArchiveSidecarTests()
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
    public async Task SidecarTable_ExistsAfterInit()
    {
        using var connection = await InitializeDatabaseAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(1) FROM information_schema.tables WHERE table_name = 'dismissed_archive_alerts'";
        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task SidecarTable_HasCorrectColumns()
    {
        using var connection = await InitializeDatabaseAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'dismissed_archive_alerts'
ORDER BY ordinal_position";

        var columns = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            columns.Add(reader.GetString(0));

        Assert.Equal(new[] { "alert_time", "server_id", "metric_name", "dismissed_at" }, columns);
    }

    [Fact]
    public async Task SidecarInsert_HidesArchivedAlertFromView()
    {
        using var connection = await InitializeDatabaseAsync();

        var alertTime = DateTime.UtcNow.AddDays(-14);
        var archivedAlerts = new List<TestAlertRecord>
        {
            TestAlertDataHelper.CreateAlert(
                alertTime: alertTime,
                serverId: 1,
                metricName: "High CPU",
                serverName: "Server1")
        };
        await _helper.CreateArchivedAlertsParquetAsync(connection, archivedAlerts);
        await _helper.RefreshArchiveViewsAsync();

        // Verify alert is visible before sidecar insert
        using var beforeCmd = connection.CreateCommand();
        beforeCmd.CommandText = "SELECT COUNT(1) FROM v_config_alert_log WHERE metric_name = 'High CPU'";
        var beforeCount = Convert.ToInt64(await beforeCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, beforeCount);

        // Insert into sidecar
        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = @"
INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name)
VALUES ($1, $2, $3)";
        insertCmd.Parameters.Add(new DuckDBParameter { Value = alertTime });
        insertCmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        insertCmd.Parameters.Add(new DuckDBParameter { Value = "High CPU" });
        await insertCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Verify alert is now hidden from view
        using var afterCmd = connection.CreateCommand();
        afterCmd.CommandText = "SELECT COUNT(1) FROM v_config_alert_log WHERE metric_name = 'High CPU'";
        var afterCount = Convert.ToInt64(await afterCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, afterCount);
    }

    [Fact]
    public async Task SidecarInsert_DoesNotAffectLiveAlerts()
    {
        using var connection = await InitializeDatabaseAsync();

        // Insert a live alert
        var liveTime = DateTime.UtcNow.AddHours(-1);
        await _helper.InsertLiveAlertAsync(connection, liveTime, 1, "Server1", "Blocking");

        // Insert an archived alert
        var archiveTime = DateTime.UtcNow.AddDays(-14);
        var archivedAlerts = new List<TestAlertRecord>
        {
            TestAlertDataHelper.CreateAlert(
                alertTime: archiveTime,
                serverId: 1,
                metricName: "High CPU",
                serverName: "Server1")
        };
        await _helper.CreateArchivedAlertsParquetAsync(connection, archivedAlerts);
        await _helper.RefreshArchiveViewsAsync();

        // Dismiss the archived alert via sidecar
        using var insertCmd = connection.CreateCommand();
        insertCmd.CommandText = @"
INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name)
VALUES ($1, $2, $3)";
        insertCmd.Parameters.Add(new DuckDBParameter { Value = archiveTime });
        insertCmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        insertCmd.Parameters.Add(new DuckDBParameter { Value = "High CPU" });
        await insertCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Live alert should still be visible
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(1) FROM v_config_alert_log WHERE metric_name = 'Blocking'";
        var liveCount = Convert.ToInt64(await checkCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, liveCount);

        // Archived alert should be hidden
        using var archiveCmd = connection.CreateCommand();
        archiveCmd.CommandText = "SELECT COUNT(1) FROM v_config_alert_log WHERE metric_name = 'High CPU'";
        var archiveCount = Convert.ToInt64(await archiveCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, archiveCount);
    }

    [Fact]
    public async Task SidecarInsert_PreventsDuplicates()
    {
        using var connection = await InitializeDatabaseAsync();

        var alertTime = DateTime.UtcNow.AddDays(-14);

        // Insert the same sidecar entry twice using the NOT EXISTS pattern
        for (int i = 0; i < 2; i++)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name)
SELECT $1, $2, $3
WHERE NOT EXISTS (
    SELECT 1 FROM dismissed_archive_alerts
    WHERE  alert_time = $1
    AND    server_id  = $2
    AND    metric_name = $3
)";
            cmd.Parameters.Add(new DuckDBParameter { Value = alertTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
            cmd.Parameters.Add(new DuckDBParameter { Value = "High CPU" });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        // Verify only one row exists
        using var countCmd = connection.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(1) FROM dismissed_archive_alerts";
        var count = Convert.ToInt64(await countCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task DismissAll_HandlesLiveAndArchivedAlerts()
    {
        using var connection = await InitializeDatabaseAsync();

        // Insert live alerts
        var liveTime1 = DateTime.UtcNow.AddHours(-1);
        var liveTime2 = DateTime.UtcNow.AddHours(-2);
        await _helper.InsertLiveAlertAsync(connection, liveTime1, 1, "Server1", "High CPU");
        await _helper.InsertLiveAlertAsync(connection, liveTime2, 1, "Server1", "Blocking");

        // Create archived alerts
        var archiveTime = DateTime.UtcNow.AddDays(-3);
        var archivedAlerts = new List<TestAlertRecord>
        {
            TestAlertDataHelper.CreateAlert(
                alertTime: archiveTime,
                serverId: 1,
                metricName: "Deadlock Detected",
                serverName: "Server1")
        };
        await _helper.CreateArchivedAlertsParquetAsync(connection, archivedAlerts);
        await _helper.RefreshArchiveViewsAsync();

        // Verify 3 alerts visible
        using var beforeCmd = connection.CreateCommand();
        beforeCmd.CommandText = "SELECT COUNT(1) FROM v_config_alert_log WHERE dismissed = FALSE";
        var beforeCount = Convert.ToInt64(await beforeCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(3, beforeCount);

        // Dismiss all live alerts
        using var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  dismissed = FALSE";
        var liveAffected = await updateCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, liveAffected);

        // Insert archived alert into sidecar (simulates what DismissAllVisibleAlertsAsync does)
        using var sidecarCmd = connection.CreateCommand();
        sidecarCmd.CommandText = @"
INSERT INTO dismissed_archive_alerts (alert_time, server_id, metric_name)
SELECT v.alert_time, v.server_id, v.metric_name
FROM   v_config_alert_log v
WHERE  v.dismissed = FALSE
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
        var archivedAffected = await sidecarCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, archivedAffected);

        // All alerts should now be hidden from view
        using var afterCmd = connection.CreateCommand();
        afterCmd.CommandText = "SELECT COUNT(1) FROM v_config_alert_log WHERE dismissed = FALSE";
        var afterCount = Convert.ToInt64(await afterCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, afterCount);
    }

    [Fact]
    public async Task SchemaVersion_IsUpdatedTo23()
    {
        using var connection = await InitializeDatabaseAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT MAX(version) FROM schema_version";
        var version = Convert.ToInt32(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        Assert.Equal(23, version);
    }
}
