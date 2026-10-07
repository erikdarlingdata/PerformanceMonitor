using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Alert History shows one spelling per server. An analysis alert is stored under the storage name
/// (<c>host:database</c>) and an engine alert under the display name; the read shows the display name for both,
/// keeps the stored spelling in <see cref="AlertHistoryRow.StoredServerName"/>, and changes nothing in the log.
/// </summary>
public sealed class AlertHistoryDisplayNameTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly TestAlertDataHelper _helper;

    public AlertHistoryDisplayNameTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _helper = new TestAlertDataHelper(_dbPath);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    private static ServerConnection Server() => new()
    {
        ServerName = "sqlhost01",
        DatabaseName = "Sales",
        DisplayName = "Sales Primary"
    };

    private async Task<List<AlertHistoryRow>> ReadAsync(bool withNames)
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var server = Server();
        var id = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        var storageName = RemoteCollectorService.GetServerNameForStorage(server);

        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await _helper.InsertLiveAlertAsync(connection, DateTime.UtcNow.AddHours(-1), id, storageName, "Analysis: Lock Waits");
            await _helper.InsertLiveAlertAsync(connection, DateTime.UtcNow.AddHours(-2), id, "Sales Primary", "High CPU");
        }

        var service = new LocalDataService(initializer);
        if (withNames)
            service.DisplayNames = () => LocalDataService.BuildDisplayNameMap([server]);
        return await service.GetAlertHistoryAsync(24, 50);
    }

    [Fact]
    public async Task AnalysisRow_ShowsTheDisplayName_AndKeepsTheStoredSpelling()
    {
        var rows = await ReadAsync(withNames: true);

        var analysis = rows.Single(r => r.MetricName == "Analysis: Lock Waits");
        Assert.Equal("Sales Primary", analysis.ServerName);
        Assert.Contains(":", analysis.StoredServerName);
        Assert.StartsWith("sqlhost01", analysis.StoredServerName);
    }

    [Fact]
    public async Task ADisplayNameFilter_FindsTheAnalysisRow_AndTheEngineRowIsUnchanged()
    {
        var rows = await ReadAsync(withNames: true);

        Assert.Equal(2, rows.Count(r => r.ServerName == "Sales Primary"));
        var engine = rows.Single(r => r.MetricName == "High CPU");
        Assert.Equal("Sales Primary", engine.ServerName);
        Assert.Equal("Sales Primary", engine.StoredServerName);
    }

    [Fact]
    public async Task WithoutANameMap_TheStoredNameIsShown()
    {
        var rows = await ReadAsync(withNames: false);

        var analysis = rows.Single(r => r.MetricName == "Analysis: Lock Waits");
        Assert.Equal(analysis.StoredServerName, analysis.ServerName);
    }
}
