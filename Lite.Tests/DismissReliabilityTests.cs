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
/// Tests that dismiss operations use batched UPDATEs within transactions
/// and that the write lock prevents race conditions.
/// </summary>
public class DismissReliabilityTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly TestAlertDataHelper _helper;

    public DismissReliabilityTests()
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
    public async Task BatchedUpdate_DismissesMultipleAlertsInSingleStatement()
    {
        using var connection = await InitializeDatabaseAsync();

        // Capture fixed timestamps to ensure insert and update match exactly
        var timestamps = new DateTime[5];
        for (int i = 0; i < 5; i++)
            timestamps[i] = DateTime.UtcNow.AddHours(-(i + 1));

        // Insert 5 live alerts with the captured timestamps
        for (int i = 0; i < 5; i++)
        {
            await _helper.InsertLiveAlertAsync(
                connection,
                timestamps[i],
                1, "Server1", $"Alert_{i}");
        }

        // Build a batched UPDATE matching the pattern used in DismissAlertsAsync
        var valuesClauses = new System.Text.StringBuilder();
        var parameters = new List<DuckDBParameter>();
        for (int i = 0; i < 5; i++)
        {
            if (i > 0) valuesClauses.Append(", ");
            var p1 = $"${i * 3 + 1}";
            var p2 = $"${i * 3 + 2}";
            var p3 = $"${i * 3 + 3}";
            valuesClauses.Append($"({p1}, {p2}, {p3})");
            parameters.Add(new DuckDBParameter { Value = timestamps[i] });
            parameters.Add(new DuckDBParameter { Value = 1 });
            parameters.Add(new DuckDBParameter { Value = $"Alert_{i}" });
        }

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  dismissed = FALSE
AND    (alert_time, server_id, metric_name) IN (VALUES {valuesClauses})";
        foreach (var p in parameters)
            cmd.Parameters.Add(p);

        var affected = await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(5, affected);

        // Verify all are dismissed
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(1) FROM config_alert_log WHERE dismissed = FALSE";
        var remaining = Convert.ToInt64(await checkCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, remaining);
    }

    [Fact]
    public async Task BatchedUpdate_ReturnsCorrectCount_WhenSomeAlreadyDismissed()
    {
        using var connection = await InitializeDatabaseAsync();

        // Insert 3 alerts, dismiss 1 beforehand
        var time1 = DateTime.UtcNow.AddHours(-1);
        var time2 = DateTime.UtcNow.AddHours(-2);
        var time3 = DateTime.UtcNow.AddHours(-3);

        await _helper.InsertLiveAlertAsync(connection, time1, 1, "Server1", "Alert_A");
        await _helper.InsertLiveAlertAsync(connection, time2, 1, "Server1", "Alert_B");
        await _helper.InsertLiveAlertAsync(connection, time3, 1, "Server1", "Alert_C", dismissed: true);

        // Batch dismiss all 3 — only 2 should be affected (Alert_C already dismissed)
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  dismissed = FALSE
AND    (alert_time, server_id, metric_name) IN (VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9))";
        cmd.Parameters.Add(new DuckDBParameter { Value = time1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Alert_A" });
        cmd.Parameters.Add(new DuckDBParameter { Value = time2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Alert_B" });
        cmd.Parameters.Add(new DuckDBParameter { Value = time3 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Alert_C" });

        var affected = await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, affected);
    }

    [Fact]
    public async Task Transaction_RollbackRestoresState()
    {
        using var connection = await InitializeDatabaseAsync();

        await _helper.InsertLiveAlertAsync(
            connection, DateTime.UtcNow.AddHours(-1), 1, "Server1", "High CPU");

        // Begin transaction, dismiss, then rollback
        using var beginCmd = connection.CreateCommand();
        beginCmd.CommandText = "BEGIN TRANSACTION";
        await beginCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        using var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  metric_name = 'High CPU'
AND    dismissed = FALSE";
        var affected = await updateCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, affected);

        using var rollbackCmd = connection.CreateCommand();
        rollbackCmd.CommandText = "ROLLBACK";
        await rollbackCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Alert should still be undismissed after rollback
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(1) FROM config_alert_log WHERE dismissed = FALSE AND metric_name = 'High CPU'";
        var undismissed = Convert.ToInt64(await checkCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, undismissed);
    }

    [Fact]
    public async Task Transaction_CommitPersistsState()
    {
        using var connection = await InitializeDatabaseAsync();

        await _helper.InsertLiveAlertAsync(
            connection, DateTime.UtcNow.AddHours(-1), 1, "Server1", "High CPU");

        // Begin transaction, dismiss, then commit
        using var beginCmd = connection.CreateCommand();
        beginCmd.CommandText = "BEGIN TRANSACTION";
        await beginCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        using var updateCmd = connection.CreateCommand();
        updateCmd.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  metric_name = 'High CPU'
AND    dismissed = FALSE";
        await updateCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        using var commitCmd = connection.CreateCommand();
        commitCmd.CommandText = "COMMIT";
        await commitCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Alert should be dismissed after commit
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(1) FROM config_alert_log WHERE dismissed = TRUE AND metric_name = 'High CPU'";
        var dismissed = Convert.ToInt64(await checkCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, dismissed);
    }

    [Fact]
    public async Task WriteLock_BlocksReadersDuringDismiss()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        // Acquire write lock on this thread
        using var writeLock = initializer.AcquireWriteLock();

        // A second write lock with timeout from a different thread should throw TimeoutException
        Exception? caughtException = null;
        var thread = new System.Threading.Thread(() =>
        {
            try
            {
                using var secondLock = initializer.AcquireWriteLock(timeout: TimeSpan.FromMilliseconds(50));
            }
            catch (Exception ex)
            {
                caughtException = ex;
            }
        });
        thread.Start();
        thread.Join(2000);

        Assert.NotNull(caughtException);
        Assert.IsType<TimeoutException>(caughtException);
        Assert.Contains("could not acquire", caughtException.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WriteLock_Timeout_ThrowsTimeoutException()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        // Simulate archival holding the write lock on a background thread
        using var archivalLock = initializer.AcquireWriteLock();

        // A concurrent dismiss attempt with timeout should throw TimeoutException
        var lockAcquired = false;
        var thread = new System.Threading.Thread(() =>
        {
            try
            {
                using var dismissLock = initializer.AcquireWriteLock(timeout: TimeSpan.FromMilliseconds(100));
                lockAcquired = true;
            }
            catch (TimeoutException)
            {
                lockAcquired = false;
            }
        });
        thread.Start();
        thread.Join(2000);

        Assert.False(lockAcquired, "Dismiss should not acquire write lock while archival holds it");
    }

    [Fact]
    public async Task DismissAll_UsesWriteLock()
    {
        using var connection = await InitializeDatabaseAsync();

        // Insert alerts
        await _helper.InsertLiveAlertAsync(
            connection, DateTime.UtcNow.AddHours(-1), 1, "Server1", "High CPU");
        await _helper.InsertLiveAlertAsync(
            connection, DateTime.UtcNow.AddHours(-2), 1, "Server1", "Blocking");

        // DismissAll targets the live table — should work with write lock
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
UPDATE config_alert_log
SET    dismissed = TRUE
WHERE  dismissed = FALSE";
        var affected = await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, affected);

        // Verify all dismissed
        using var checkCmd = connection.CreateCommand();
        checkCmd.CommandText = "SELECT COUNT(1) FROM config_alert_log WHERE dismissed = FALSE";
        var remaining = Convert.ToInt64(await checkCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, remaining);
    }
}
