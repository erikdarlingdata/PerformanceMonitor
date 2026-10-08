/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The Optimization tab's Idle Databases grid follows the recommendation row's rule: a database is idle for 7 days only when query
/// stats hold a sample on each of the last 7 UTC days. Without that coverage the grid is empty AND says why, instead of "No idle
/// databases detected". (The Server Inventory "Idle DBs" count has its own read test in FinOpsFleetReadParityTests.)
/// </summary>
public sealed class FinOpsIdleDatabasesCoverageTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 5011;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public FinOpsIdleDatabasesCoverageTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task ExecAsync(string sql, params object[] vals)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open) await _seedConn.OpenAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in vals) cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDatabaseAsync(string name) =>
        await ExecAsync(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
     file_name, physical_name, total_size_mb, used_size_mb)
VALUES ($1, $2, $3, 'IdleSrv', $4, 7, 1, 'ROWS', $5, $5, 100, NULL)", _nextId++, DateTime.UtcNow, ServerId, name, name + ".mdf");

    private async Task SeedQueryStatsDaysAsync(int days)
    {
        for (var d = 0; d < days; d++)
            await ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
VALUES ($1, $2, $3, 'IdleSrv', 'DbElsewhere', '0xE', 1, 10, 20, 1)", _nextId++, DateTime.UtcNow.AddDays(-d), ServerId);
    }

    [Fact]
    public async Task TwoDaysOfQueryStats_HasNoCoverage_SoTheGridSaysWhyItIsEmpty()
    {
        await SeedDatabaseAsync("DbOne");
        await SeedQueryStatsDaysAsync(2);

        var svc = new LocalDataService(_duckDb);

        Assert.False(await svc.HasQueryStatsCoverageAsync(ServerId));
        Assert.NotEqual("No idle databases detected", LocalDataService.IdleDatabasesEmptyText(false));
        Assert.Contains("7 days", LocalDataService.IdleDatabasesEmptyText(false), StringComparison.Ordinal);
    }

    [Fact]
    public async Task SevenDaysOfQueryStats_HasCoverage_AndAnUnusedDatabaseIsIdle()
    {
        await SeedDatabaseAsync("DbOne");
        await SeedQueryStatsDaysAsync(7);

        var svc = new LocalDataService(_duckDb);

        Assert.True(await svc.HasQueryStatsCoverageAsync(ServerId));
        Assert.Equal("DbOne", Assert.Single(await svc.GetIdleDatabasesAsync(ServerId)).DatabaseName);
        Assert.Equal("No idle databases detected", LocalDataService.IdleDatabasesEmptyText(true));
    }

    [Fact]
    public void TheTab_ChecksCoverageBeforeReadingTheGrid_AndSetsTheEmptyText()
    {
        var tab = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml.cs");
        var load = tab.IndexOf("Task LoadIdleDatabasesAsync(", StringComparison.Ordinal);
        Assert.True(load >= 0);
        var body = tab.Substring(load, 1600);

        var coverage = body.IndexOf("HasQueryStatsCoverageAsync(serverId)", StringComparison.Ordinal);
        var read = body.IndexOf("GetIdleDatabasesAsync(serverId)", StringComparison.Ordinal);
        Assert.True(coverage >= 0 && read > coverage);
        Assert.Contains("IdleDatabasesNoDataMessage.Text = LocalDataService.IdleDatabasesEmptyText(covered);", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServerInventoryIdleCountAndHealthCells_ShowADashWhenThereIsNoValue()
    {
        var xaml = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml");

        Assert.Contains("Binding=\"{Binding IdleDbCount, TargetNullValue='-'}\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Binding=\"{Binding HealthScore, TargetNullValue='-'}\"", xaml, StringComparison.Ordinal);
    }
}

/// <summary>
/// The Server Inventory grid's health score is built from the server's 24-hour CPU average plus two defaults (memory 80, storage 50%
/// free). A server with no CPU sample used to score from the defaults alone (about 90); it now has no score and the grid shows a dash,
/// the same rule as the Utilization badge.
/// </summary>
public sealed class FinOpsInventoryHealthScoreTests
{
    [Fact]
    public void NoCpuSample_HasNoScore_AndTheRowShowsTheNoScoreColorAndNote()
    {
        Assert.Null(FinOpsHealthCalculator.InventoryScore(null));

        var row = new ServerPropertyRow { HealthScore = FinOpsHealthCalculator.InventoryScore(null) };

        Assert.Null(row.HealthScore);
        Assert.Equal(FinOpsHealthCalculator.NoScoreColor, row.HealthScoreColor);
        Assert.Equal(FinOpsHealthCalculator.NoScoreNote, row.HealthScoreNote);
    }

    [Fact]
    public void ACpuSample_ScoresFromCpuPlusTheDefaults_InTheScoresOwnColor()
    {
        // 40% CPU -> 100 - 40*50/70 = 71; memory default 80; storage default (50% free) 100: 71*0.4 + 80*0.3 + 100*0.3 = 82.
        var score = FinOpsHealthCalculator.InventoryScore(40m);
        var row = new ServerPropertyRow { HealthScore = score };

        Assert.Equal(82, score);
        Assert.Equal(FinOpsHealthCalculator.ScoreColor(82), row.HealthScoreColor);
        Assert.Null(row.HealthScoreNote);
    }

    [Fact]
    public void TheTab_ScoresTheInventoryThroughTheSharedRule()
    {
        var tab = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml.cs");

        Assert.Contains("item.HealthScore = FinOpsHealthCalculator.InventoryScore(item.AvgCpuPct);", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("var memScore = 80;", tab, StringComparison.Ordinal);
    }
}
