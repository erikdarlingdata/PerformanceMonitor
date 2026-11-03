/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7196, the Lite half: a cpu_utilization collector that stopped leaves its last reading as the newest row, and the
/// Overview card and get_server_summary showed it as current. Both now show no CPU figure once the newest reading's
/// collection_time is older than the larger of 15 minutes and twice the cpu_utilization interval scheduled on that
/// server. The alert gate's input is NOT bounded: the item keeps the raw reading in the properties
/// MainWindow.AlertEngine reads (CpuPercent, TotalCpuPercent, CpuSampleTimeUtc, CpuSampleTime).
/// </summary>
public sealed class StaleSqlServerCpuOverviewTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public StaleSqlServerCpuOverviewTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "StaleCpuOverview_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);

        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AStaleReading_ShowsNoCpuOnTheCardAndInGetServerSummary_ButTheAlertInputKeepsTheValue()
    {
        var cpuAt = Whole(DateTime.UtcNow.AddMinutes(-40));
        await SeedCpuAsync(cpuAt, sqlCpu: 77, otherCpu: 5);
        await SeedMemoryAsync(Whole(DateTime.UtcNow.AddDays(-2)), 40000);

        var card = await _dataService.GetServerSummaryAsync(_serverId, ServerName, registeredAtUtc: null);
        Assert.NotNull(card);
        Assert.True(card!.CpuIsStale);
        Assert.Equal("--", card.CpuDisplay);
        Assert.Null(card.ShownCpuPercent);
        Assert.Null(card.ShownCpuPercentForAlert);

        /* The alert engine reads these properties for its snapshot (MainWindow.AlertEngine: SqlCpuPercent,
           TotalCpuPercent, CpuSampleTimeUtc ?? CpuSampleTime): a stale reading must still reach the gate. */
        Assert.Equal(77d, card.CpuPercent);
        Assert.Equal(82d, card.TotalCpuPercent);
        Assert.Equal(cpuAt, card.CpuSampleTime);

        var root = Parse(await McpHealthTools.GetServerSummary(_dataService, _serverManager, ServerName));
        Assert.Equal(JsonValueKind.Null, root.GetProperty("cpu_percent").ValueKind);
        Assert.Equal(JsonValueKind.Null, root.GetProperty("cpu_captured_at").ValueKind);
        /* Memory keeps its newest row at any age. */
        Assert.Equal(40000d, root.GetProperty("memory_mb").GetDouble());
    }

    [Fact]
    public async Task AFreshReading_ShowsOnTheCardAndInGetServerSummary()
    {
        var cpuAt = Whole(DateTime.UtcNow.AddMinutes(-5));
        await SeedCpuAsync(cpuAt, sqlCpu: 42, otherCpu: 3);

        var card = await _dataService.GetServerSummaryAsync(_serverId, ServerName, registeredAtUtc: null);
        Assert.False(card!.CpuIsStale);
        Assert.Equal("45% (SQL 42%)", card.CpuDisplay);

        var root = Parse(await McpHealthTools.GetServerSummary(_dataService, _serverManager, ServerName));
        Assert.Equal(42d, root.GetProperty("cpu_percent").GetDouble());
        Assert.Equal(cpuAt.ToString("o"), root.GetProperty("cpu_captured_at").GetString());
    }

    [Fact]
    public async Task ATwentyMinuteOldReading_ShowsOnA30MinuteSchedule_ButNotOnTheShippedOneMinuteSchedule()
    {
        await SeedCpuAsync(Whole(DateTime.UtcNow.AddMinutes(-20)), sqlCpu: 61, otherCpu: 0);

        /* The shipped cpu_utilization interval is one minute: the bound is the 15 minute floor. */
        var shipped = await _dataService.GetServerSummaryAsync(_serverId, ServerName, registeredAtUtc: null);
        Assert.True(shipped!.CpuIsStale);

        /* An operator's every-30-minutes schedule bounds at 60 minutes, so 20 minutes between samples is normal. */
        var slow = new LocalDataService(_duckDb)
        {
            CollectorFrequencyMinutes = (id, collector) => id == _serverId && collector == "cpu_utilization" ? 30 : null
        };
        var card = await slow.GetServerSummaryAsync(_serverId, ServerName, registeredAtUtc: null);
        Assert.False(card!.CpuIsStale);
        Assert.Equal("61% (SQL 61%)", card.CpuDisplay);

        var root = Parse(await McpHealthTools.GetServerSummary(slow, _serverManager, ServerName));
        Assert.Equal(61d, root.GetProperty("cpu_percent").GetDouble());
    }

    [Fact]
    public async Task AReadingPastTwiceTheScheduledInterval_IsStaleEvenOnASlowSchedule()
    {
        await SeedCpuAsync(Whole(DateTime.UtcNow.AddMinutes(-70)), sqlCpu: 61, otherCpu: 0);
        var slow = new LocalDataService(_duckDb) { CollectorFrequencyMinutes = (_, _) => 30 };
        var card = await slow.GetServerSummaryAsync(_serverId, ServerName, registeredAtUtc: null);
        Assert.True(card!.CpuIsStale);
        Assert.Equal("--", card.CpuDisplay);
    }

    [Fact]
    public async Task AnOnLoadCpuSchedule_BoundsTheReadingByTheOnLoadInterval_AsDarlingDoes()
    {
        /* A schedule of 0 is on-load only, re-run daily: the bound is two days, the same as Darling's SQL. */
        var onLoad = new LocalDataService(_duckDb) { CollectorFrequencyMinutes = (_, _) => 0 };

        await SeedCpuAsync(Whole(DateTime.UtcNow.AddHours(-3)), sqlCpu: 61, otherCpu: 0);
        var threeHours = await onLoad.GetServerSummaryAsync(_serverId, ServerName, registeredAtUtc: null);
        Assert.False(threeHours!.CpuIsStale);

        await ExecAsync("DELETE FROM cpu_utilization_stats WHERE server_id = $1", _serverId);
        await SeedCpuAsync(Whole(DateTime.UtcNow.AddDays(-3)), sqlCpu: 61, otherCpu: 0);
        var threeDays = await onLoad.GetServerSummaryAsync(_serverId, ServerName, registeredAtUtc: null);
        Assert.True(threeDays!.CpuIsStale);
    }

    [Fact]
    public void AStaleOrUnmeasuredCpuRow_IsTheUnknownGrey_NotTheHealthyGreen_AndSinksBelowAMeasuredServerInTheSort()
    {
        var healthy = new ServerSummaryItem { DisplayName = "calm", ServerId = 1, IsOnline = true, CpuPercent = 1 };
        var stale = new ServerSummaryItem { DisplayName = "stale", ServerId = 2, IsOnline = true, CpuPercent = 90, CpuIsStale = true };
        var neverMeasured = new ServerSummaryItem { DisplayName = "never", ServerId = 3, IsOnline = true };
        var unknownGrey = new ServerSummaryItem { IsOnline = null }.StatusBrush.Color;

        /* #3635: a CPU row with no shown figure is Unknown, as Darling bands it, never the Healthy colour. */
        Assert.NotEqual(healthy.CpuBrush.Color, stale.CpuBrush.Color);
        Assert.Equal(unknownGrey, stale.CpuBrush.Color);
        Assert.Equal(unknownGrey, neverMeasured.CpuBrush.Color);
        /* The stale 90% must not light the critical colour or the card border either. */
        Assert.NotEqual(new ServerSummaryItem { IsOnline = true, CpuPercent = 90 }.CpuBrush.Color, stale.CpuBrush.Color);
        Assert.Equal(healthy.CardBorderBrush.Color, stale.CardBorderBrush.Color);

        var ordered = PerformanceMonitor.Ui.ServerOverviewSort.Order(
            new[] { stale, healthy }, PerformanceMonitor.Ui.ServerOverviewSortMode.Cpu,
            s => s.ShownCpuPercentForAlert, s => s.DisplayName, s => s.ServerId);
        Assert.Equal(new[] { healthy, stale }, ordered);
    }

    /// <summary>The store holds microseconds and a tick is finer, so a seeded instant is cut to whole seconds.</summary>
    private static DateTime Whole(DateTime utc) =>
        DateTime.SpecifyKind(new DateTime(utc.Ticks - (utc.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private Task SeedCpuAsync(DateTime at, double sqlCpu, double otherCpu) => ExecAsync(@"
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $2, $5, $6)", _nextId--, at, _serverId, ServerName, sqlCpu, otherCpu);

    private Task SeedMemoryAsync(DateTime at, double totalServerMb) => ExecAsync(@"
INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, total_server_memory_mb)
VALUES ($1, $2, $3, $4, 65536, 8192, $5)", _nextId--, at, _serverId, ServerName, totalServerMb);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
