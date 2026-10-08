/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The Optimization tab's Idle Databases grid follows the recommendation row's rule: a database is idle for 7 days only when query
/// stats hold a sample on each of the 7 complete UTC days before today AND the oldest sample is at least 7 days old. Without that coverage the grid is empty AND says why, instead of "No idle
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

    private async Task SeedQueryStatsDaysAsync(int days) => await SeedQueryStatsAtDaysBackAsync(Enumerable.Range(0, days).Select(d => (double)d).ToArray());

    private async Task SeedQueryStatsAtAsync(DateTime timeUtc) =>
        await ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
VALUES ($1, $2, $3, 'IdleSrv', 'DbElsewhere', '0xE', 1, 10, 20, 1)", _nextId++, timeUtc, ServerId);

    /// <summary>One sample per entry, that many days before now (a fraction is a part of a day).</summary>
    private async Task SeedQueryStatsAtDaysBackAsync(params double[] daysBack)
    {
        foreach (var d in daysBack)
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
    public async Task EightDaysBackThroughYesterday_HasCoverage_AndAnUnusedDatabaseIsIdle()
    {
        // The oldest sample is 8 days old and each of the 7 complete days before today holds one; nothing has arrived yet today.
        await SeedDatabaseAsync("DbOne");
        await SeedQueryStatsAtDaysBackAsync(1, 2, 3, 4, 5, 6, 7, 8);

        var svc = new LocalDataService(_duckDb);

        Assert.True(await svc.HasQueryStatsCoverageAsync(ServerId));
        Assert.Equal("DbOne", Assert.Single(await svc.GetIdleDatabasesAsync(ServerId)).DatabaseName);
        Assert.Equal("No idle databases detected", LocalDataService.IdleDatabasesEmptyText(true));
    }

    [Fact]
    public async Task SixAndAHalfDaysSampledEveryDay_HasNoCoverage_BecauseTheOldestSampleIsNotSevenDaysOld()
    {
        await SeedDatabaseAsync("DbOne");
        await SeedQueryStatsAtDaysBackAsync(0, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5);

        Assert.False(await new LocalDataService(_duckDb).HasQueryStatsCoverageAsync(ServerId));
    }

    [Fact]
    public async Task SevenAndAHalfDaysMissingOneCompleteDay_HasNoCoverage()
    {
        // Samples at noon 8, 7, 6, 5, 4, 2 and 1 days back: the oldest sample is older than 7 days, but 3 days back was never watched.
        await SeedDatabaseAsync("DbOne");
        foreach (var d in new[] { 8, 7, 6, 5, 4, 2, 1 })
            await SeedQueryStatsAtAsync(DateTime.UtcNow.Date.AddDays(-d).AddHours(12));

        Assert.False(await new LocalDataService(_duckDb).HasQueryStatsCoverageAsync(ServerId));
    }

    [Fact]
    public async Task FullCoverageWithNoSampleYetToday_IsStillCovered()
    {
        // The state just after 00:00 UTC: every complete day from 7 days back to yesterday holds a sample, today none yet.
        await SeedDatabaseAsync("DbOne");
        foreach (var d in new[] { 8, 7, 6, 5, 4, 3, 2, 1 })
            await SeedQueryStatsAtAsync(DateTime.UtcNow.Date.AddDays(-d).AddHours(12));

        Assert.True(await new LocalDataService(_duckDb).HasQueryStatsCoverageAsync(ServerId));
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
/// The Server Inventory grid's health score is the Utilization tab's score for the same server: p95 CPU, buffer pool share and
/// free space, through <see cref="FinOpsHealthCalculator.Score"/>. A server with no CPU sample has no score and the grid shows a dash.
/// </summary>
public sealed class FinOpsInventoryHealthScoreTests
{
    [Fact]
    public void NoScore_ShowsTheNoScoreColorAndNote()
    {
        var row = new ServerPropertyRow { HealthScore = null };

        Assert.Null(row.HealthScore);
        Assert.Equal(FinOpsHealthCalculator.NoScoreColor, row.HealthScoreColor);
        Assert.Equal(FinOpsHealthCalculator.NoScoreNote, row.HealthScoreNote);
    }

    [Fact]
    public void AScore_IsInTheScoresOwnColor()
    {
        // p95 40% CPU -> 71; buffer pool 50% of RAM -> 100; 50% free -> 100: 71*0.4 + 100*0.3 + 100*0.3 = 88.
        var score = FinOpsHealthCalculator.Score(true, 40m, 1000, 500, 50m);
        var row = new ServerPropertyRow { HealthScore = score };

        Assert.Equal(88, score);
        Assert.Equal(FinOpsHealthCalculator.ScoreColor(88), row.HealthScoreColor);
        Assert.Null(row.HealthScoreNote);
    }

    [Fact]
    public void TheTab_NoLongerScoresTheInventoryFromDefaults()
    {
        var tab = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml.cs");

        Assert.DoesNotContain("InventoryScore(", tab, StringComparison.Ordinal);
        Assert.DoesNotContain("var memScore = 80;", tab, StringComparison.Ordinal);
    }
}
