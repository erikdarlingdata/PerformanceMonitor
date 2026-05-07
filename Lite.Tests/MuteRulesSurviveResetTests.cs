using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Issue #938 — mute rules (especially expires_at_utc = NULL "permanent" rules)
/// were silently lost when ArchiveAllAndResetAsync fired due to the 512 MB size threshold.
/// The reset deletes monitor.duckdb outright, and config_mute_rules was not preserved.
/// </summary>
public class MuteRulesSurviveResetTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public MuteRulesSurviveResetTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
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

    [Fact]
    public async Task PermanentMuteRule_SurvivesArchiveAllAndReset()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var ruleId = Guid.NewGuid().ToString();
        var createdAt = new DateTime(2026, 5, 1, 12, 0, 0, DateTimeKind.Utc);

        await InsertMuteRuleAsync(ruleId, createdAt, expiresAtUtc: null,
            serverName: "ProdSql01", metricName: "Blocking Detected");

        var archiveService = new ArchiveService(initializer, _archiveDir);
        await archiveService.ArchiveAllAndResetAsync();

        var (count, expiresIsNull, serverName) = await ReadMuteRuleAsync(ruleId);

        Assert.Equal(1, count);
        Assert.True(expiresIsNull);
        Assert.Equal("ProdSql01", serverName);
    }

    [Fact]
    public async Task ExpiringMuteRule_SurvivesArchiveAllAndReset()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var ruleId = Guid.NewGuid().ToString();
        var createdAt = DateTime.UtcNow;
        var expiresAt = createdAt.AddDays(7);

        await InsertMuteRuleAsync(ruleId, createdAt, expiresAt,
            serverName: "ProdSql02", metricName: "Long-Running Job");

        var archiveService = new ArchiveService(initializer, _archiveDir);
        await archiveService.ArchiveAllAndResetAsync();

        var (count, expiresIsNull, serverName) = await ReadMuteRuleAsync(ruleId);

        Assert.Equal(1, count);
        Assert.False(expiresIsNull);
        Assert.Equal("ProdSql02", serverName);
    }

    [Fact]
    public async Task EmptyMuteRulesTable_DoesNotBreakReset()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var archiveService = new ArchiveService(initializer, _archiveDir);
        await archiveService.ArchiveAllAndResetAsync();

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM config_mute_rules";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        Assert.Equal(0, count);
    }

    private async Task InsertMuteRuleAsync(
        string id,
        DateTime createdAt,
        DateTime? expiresAtUtc,
        string serverName,
        string metricName)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO config_mute_rules
    (id, enabled, created_at_utc, expires_at_utc, reason,
     server_name, metric_name, database_pattern,
     query_text_pattern, wait_type_pattern, job_name_pattern)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = id });
        cmd.Parameters.Add(new DuckDBParameter { Value = true });
        cmd.Parameters.Add(new DuckDBParameter { Value = createdAt });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)expiresAtUtc ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = "test rule" });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = metricName });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<(int Count, bool ExpiresIsNull, string ServerName)> ReadMuteRuleAsync(string id)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT expires_at_utc, server_name FROM config_mute_rules WHERE id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = id });

        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        if (!await reader.ReadAsync(TestContext.Current.CancellationToken))
            return (0, false, "");

        var expiresIsNull = reader.IsDBNull(0);
        var serverName = reader.GetString(1);
        return (1, expiresIsNull, serverName);
    }
}
