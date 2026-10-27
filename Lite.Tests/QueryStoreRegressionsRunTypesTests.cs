/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
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
/// Lite's get_query_store_regressions shows Regular, Aborted and Exception runs side by side, as Darling's does (#6268, #5506):
/// the cost gate compares Regular runs per execution (weighted by execution count), and a rise in timeouts (Aborted) or errors
/// (Exception) lists the query on its own, with the reason named. Logical reads are not reported. Every figure below is worked
/// by hand from the seeded intervals.
/// </summary>
public sealed class QueryStoreRegressionsRunTypesTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "RunTypesSrv";
    private const string Db = "AppDb";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStoreRegressionsRunTypesTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-runtypes-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /// <summary>Query 5: its Regular CPU FELL, but its timeouts rose from 4 in the 16 hours of baseline the store holds (its data starts 40
    /// hours back) to 30 in the last 24 hours. It is listed for timeouts alone, which a Regular-only comparison (or one that blended the runs into one average) never shows.</summary>
    [Fact]
    public async Task ARiseInTimeouts_ListsTheQuery_EvenThoughItsRegularCostFell()
    {
        var now = Truncate(DateTime.UtcNow);
        /* Regular baseline: 100 executions at 2 ms CPU and 300 at 1 ms: weighted 1.25 ms, not the 1.5 ms average of the two. */
        await SeedAsync("Regular", now.AddHours(-40), 100, 2000, 2000, intervalId: 1, queryId: 5);
        await SeedAsync("Regular", now.AddHours(-30), 300, 1000, 1000, intervalId: 2, queryId: 5);
        await SeedAsync("Regular", now.AddHours(-1), 200, 1000, 1000, intervalId: 3, queryId: 5);
        /* Four timeouts in the baseline, thirty in the window, each 30 s of duration and 5 to 6 ms of CPU. */
        await SeedAsync("Aborted", now.AddHours(-35), 4, 30_000_000, 5000, intervalId: 4, queryId: 5);
        await SeedAsync("Aborted", now.AddHours(-2), 30, 30_000_000, 6000, intervalId: 5, queryId: 5);

        var rows = await ReadRowsAsync();

        var row = Assert.Single(rows);
        Assert.Equal(5, row.GetProperty("query_id").GetInt64());
        Assert.Equal("timeouts", row.GetProperty("regression_reason").GetString());
        /* 30 / 24 h = 1.25 per hour against 4 / 16 h = 0.25 per hour (#6750: the hours the store holds, from its first row 40 hours
           back to the window's start, not the 168 asked for): five times, so HIGH. */
        Assert.Equal("HIGH", row.GetProperty("severity").GetString());
        Assert.Equal(1.25, row.GetProperty("baseline_cpu_ms").GetDouble(), 6);
        Assert.Equal(1.0, row.GetProperty("recent_cpu_ms").GetDouble(), 6);
        Assert.Equal(-20.0, row.GetProperty("cpu_regression_percent").GetDouble(), 6);

        Assert.Equal(4, row.GetProperty("baseline_aborted_count").GetInt64());
        Assert.Equal(30, row.GetProperty("recent_aborted_count").GetInt64());
        Assert.Equal(5.0, row.GetProperty("baseline_aborted_cpu_ms").GetDouble(), 6);
        Assert.Equal(6.0, row.GetProperty("recent_aborted_cpu_ms").GetDouble(), 6);
        Assert.Equal(30_000.0, row.GetProperty("baseline_aborted_duration_ms").GetDouble(), 6);
        Assert.Equal(30_000.0, row.GetProperty("recent_aborted_duration_ms").GetDouble(), 6);
        Assert.Equal(0.25, row.GetProperty("baseline_aborted_per_hour").GetDouble(), 6);
        Assert.Equal(1.25, row.GetProperty("recent_aborted_per_hour").GetDouble(), 6);
        Assert.Equal(0, row.GetProperty("baseline_exception_count").GetInt64());
        Assert.Equal(0, row.GetProperty("recent_exception_count").GetInt64());
        Assert.False(row.TryGetProperty("recent_exception_per_hour", out _));
    }

    /// <summary>Query 6 had no Exception run in the baseline and has three in the window, and no Regular run at all: listed for
    /// errors, with the missing cost comparison said once. Query 8 has only two errors, below the three-run floor, and is not listed.</summary>
    [Fact]
    public async Task ErrorsWhereThereWereNone_ListTheQuery_AndTooFewErrorsDoNot()
    {
        var now = Truncate(DateTime.UtcNow);
        await SeedAsync("Exception", now.AddHours(-3), 3, 1000, 500, intervalId: 1, queryId: 6);
        await SeedAsync("Exception", now.AddHours(-3), 2, 1000, 500, intervalId: 2, queryId: 8);
        /* A baseline Regular run for both, so that the baseline window holds a capture. */
        await SeedAsync("Regular", now.AddHours(-40), 100, 1000, 1000, intervalId: 3, queryId: 6);
        await SeedAsync("Regular", now.AddHours(-40), 100, 1000, 1000, intervalId: 4, queryId: 8);

        var rows = await ReadRowsAsync();

        var row = Assert.Single(rows);
        Assert.Equal(6, row.GetProperty("query_id").GetInt64());
        Assert.Equal("errors", row.GetProperty("regression_reason").GetString());
        Assert.Equal("HIGH", row.GetProperty("severity").GetString());
        Assert.Equal(0, row.GetProperty("baseline_exception_count").GetInt64());
        Assert.Equal(3, row.GetProperty("recent_exception_count").GetInt64());
        Assert.Equal(1.0, row.GetProperty("recent_exception_duration_ms").GetDouble(), 6);
        Assert.Equal(0.5, row.GetProperty("recent_exception_cpu_ms").GetDouble(), 6);
        Assert.Equal(0.125, row.GetProperty("recent_exception_per_hour").GetDouble(), 1);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("cpu_regression_percent").ValueKind);
        Assert.Contains("no Regular runs in the recent window", row.GetProperty("undefined_percents")[0].GetString(), StringComparison.Ordinal);
    }

    /// <summary>The cost comparison is Regular runs only and WEIGHTED by execution count, with no logical reads anywhere in the row. Query 7's baseline is
    /// 100 executions at 1 ms CPU and 300 at 3 ms: 2.5 ms weighted (the average of the two intervals' averages would be 2 ms and show
    /// +100%), and a huge Aborted run beside it must not leak into the Regular figure. 4 ms against 2.5 ms is +60%, reason cost, HIGH.
    /// #6781: 1,000 recent runs, so the 1.5 ms rise adds 1.5 s of CPU, over the 1 s floor.</summary>
    [Fact]
    public async Task TheCostFigureIsRegularOnlyAndWeighted_AndNoReadsAreReported()
    {
        var now = Truncate(DateTime.UtcNow);
        await SeedAsync("Regular", now.AddHours(-40), 100, 1000, 1000, intervalId: 1, queryId: 7);
        await SeedAsync("Regular", now.AddHours(-30), 300, 3000, 3000, intervalId: 2, queryId: 7);
        await SeedAsync("Aborted", now.AddHours(-35), 1, 900_000, 900_000, intervalId: 3, queryId: 7);
        await SeedAsync("Regular", now.AddHours(-1), 1000, 4000, 4000, intervalId: 4, queryId: 7);

        var rows = await ReadRowsAsync();

        var row = Assert.Single(rows);
        Assert.Equal(7, row.GetProperty("query_id").GetInt64());
        Assert.Equal("cost", row.GetProperty("regression_reason").GetString());
        Assert.Equal("HIGH", row.GetProperty("severity").GetString());
        Assert.Equal(2.5, row.GetProperty("baseline_cpu_ms").GetDouble(), 6);
        Assert.Equal(4.0, row.GetProperty("recent_cpu_ms").GetDouble(), 6);
        Assert.Equal(60.0, row.GetProperty("cpu_regression_percent").GetDouble(), 6);
        Assert.Equal(60.0, row.GetProperty("duration_regression_percent").GetDouble(), 6);
        Assert.Equal(400, row.GetProperty("baseline_exec_count").GetInt64());
        Assert.Equal(1000, row.GetProperty("recent_exec_count").GetInt64());
        /* 1.5 ms more per execution across the 1,000 recent executions. */
        Assert.Equal(1500.0, row.GetProperty("additional_duration_ms").GetDouble(), 6);
        /* The Aborted group is counted beside it, and a row listed for cost alone carries counts, not rates. */
        Assert.Equal(1, row.GetProperty("baseline_aborted_count").GetInt64());
        Assert.False(row.TryGetProperty("baseline_aborted_per_hour", out _));

        Assert.False(row.TryGetProperty("baseline_reads", out _));
        Assert.False(row.TryGetProperty("recent_reads", out _));
        Assert.False(row.TryGetProperty("io_regression_percent", out _));
    }

    /// <summary>Cost rows come first, ranked by added duration; a row listed only for timeouts follows them.</summary>
    [Fact]
    public async Task CostRowsComeFirst_ThenTheRowsListedOnlyForFailures()
    {
        var now = Truncate(DateTime.UtcNow);
        /* Query 5: timeouts only. Query 7: cost only. */
        await SeedAsync("Regular", now.AddHours(-30), 100, 1000, 1000, intervalId: 1, queryId: 5);
        await SeedAsync("Regular", now.AddHours(-1), 100, 1000, 1000, intervalId: 2, queryId: 5);
        await SeedAsync("Aborted", now.AddHours(-2), 5, 1000, 1000, intervalId: 3, queryId: 5);
        await SeedAsync("Regular", now.AddHours(-30), 100, 1000, 1000, intervalId: 4, queryId: 7);
        /* #6781: enough recent runs that the rise adds at least 1 s of CPU (QueryStoreRegressionRules.CostMinAddedCpuMs). */
        await SeedAsync("Regular", now.AddHours(-1), 1000, 4000, 4000, intervalId: 5, queryId: 7);

        var json = Root(await McpQueryTools.GetQueryStoreRegressions(new LocalDataService(_duckDb), _serverManager, ServerName, 24));
        var ids = json.GetProperty("regressions").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).ToArray();

        Assert.Equal(new long[] { 7, 5 }, ids);
        Assert.Equal(2, json.GetProperty("regression_count").GetInt32());
        Assert.Contains("timeouts", json.GetProperty("gate").GetString(), StringComparison.Ordinal);
    }

    /// <summary>#6781, Lite's twin of Darling's QueryStoreRegressionsAddedCpuFloorLiveTests: the cost test also needs at least
    /// QueryStoreRegressionRules.CostMinAddedCpuMs of CPU added over the window (recent runs times the rise in CPU per run). Query 61,
    /// two cheap runs at +50% CPU (10 ms added), is not listed. Query 62, one run of a job whose CPU went from 5 to 50 minutes, is.
    /// Query 63, 5,000 runs at 1 to 3 ms, is. Query 64, CPU per run up 100 to 400 ms while its duration fell 500 to 300 ms (a plan that
    /// went parallel), is listed for cost though it added no duration. Query 65, cheap like 61 but with timeouts 0 to 4, is listed for
    /// timeouts only, and regression_count counts the four rows that pass.</summary>
    [Fact]
    public async Task ACheapLowRunRise_IsNotACostRegression_AndARareExpensiveOneAndAParallelPlanAre()
    {
        var now = Truncate(DateTime.UtcNow);
        long interval = 100;
        async Task S(string type, int hoursBack, long q, long n, long durUs, long cpuUs) =>
            await SeedAsync(type, now.AddHours(-hoursBack), n, durUs, cpuUs, intervalId: ++interval, queryId: q);
        await S("Regular", 40, 61, 2, 20_000, 10_000);
        await S("Regular", 1, 61, 2, 30_000, 15_000);
        await S("Regular", 40, 62, 1, 310_000_000, 300_000_000);
        await S("Regular", 1, 62, 1, 3_010_000_000, 3_000_000_000);
        await S("Regular", 40, 63, 5_000, 2_000, 1_000);
        await S("Regular", 1, 63, 5_000, 6_000, 3_000);
        await S("Regular", 40, 64, 100, 500_000, 100_000);
        await S("Regular", 1, 64, 100, 300_000, 400_000);
        await S("Regular", 40, 65, 2, 20_000, 10_000);
        await S("Regular", 1, 65, 2, 30_000, 15_000);
        await S("Aborted", 2, 65, 4, 30_000_000, 10_000);

        var json = Root(await McpQueryTools.GetQueryStoreRegressions(new LocalDataService(_duckDb), _serverManager, ServerName, 24));
        var rows = json.GetProperty("regressions").EnumerateArray()
            .Select(r => (r.GetProperty("query_id").GetInt64(), r.GetProperty("regression_reason").GetString())).ToArray();

        Assert.Equal(new (long, string?)[] { (62L, "cost"), (63L, "cost"), (64L, "cost"), (65L, "timeouts") }, rows);
        Assert.Equal(4, json.GetProperty("regression_count").GetInt32());
    }

    private async Task<JsonElement[]> ReadRowsAsync()
    {
        var json = Root(await McpQueryTools.GetQueryStoreRegressions(new LocalDataService(_duckDb), _serverManager, ServerName, 24));
        return json.GetProperty("regressions").EnumerateArray().ToArray();
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task SeedAsync(
        string executionType, DateTime collectionTime, long executions, long avgDurationUs, long avgCpuUs, long intervalId, long queryId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= await OpenSeedAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 9L });
        cmd.Parameters.Add(new DuckDBParameter { Value = executionType });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgCpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100L });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.Widgets" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> OpenSeedAsync()
    {
        var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        return connection;
    }
}
