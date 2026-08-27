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
/// Lite's get_query_store_regressions (#2484), the twin of Darling's.
///
/// <para>Two things are pinned here that no reader-level test would see. The four empty branches - only one
/// of which is good news - and the DEDUP, which is correctness rather than performance on this read: the
/// baseline arm is unbounded while the recent arm is a short window, so the two sides being compared have
/// systematically different re-collection density per interval, and losing the dedup moves the averages and
/// the 25% gate for reasons that have nothing to do with the query.</para>
/// </summary>
public sealed class QueryStoreRegressionsToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "RegressionsSrv";
    private const string Db = "AppDb";

    /* Lite DERIVES its server id from the storage name; a hardcoded one would seed rows the tool looks
       straight past and pass the never-collected assertion for the wrong reason. */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStoreRegressionsToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-regressions-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task FourKindsOfNothing_AreFourDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);

        /* 1. nothing ever collected. */
        var never = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        Assert.Equal("unavailable", never.GetProperty("status").GetString());
        var neverText = never.GetProperty("message").GetString()!;
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);
        Assert.Contains("Query Store may be OFF", neverText, StringComparison.Ordinal);

        /*
            2. a baseline exists but nothing landed in the window. Seeded FIRST, and the ordering is
            load-bearing: once a row exists 30 minutes ago no window a caller can legally ask for excludes
            it, so this branch is unreachable after the recent row is planted. The four states are walked
            by moving hours_back over one fixed set of rows rather than by deleting rows between
            assertions.
        */
        await SeedAsync(baseNow.AddHours(-40), executions: 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 2);

        var noRecent = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 2));
        Assert.Equal("empty", noRecent.GetProperty("status").GetString());
        var noRecentText = noRecent.GetProperty("message").GetString()!;
        Assert.Contains("Widen hours_back", noRecentText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", noRecentText, StringComparison.Ordinal);

        /* 3. both sides collected, nothing regressed: the ONE good-news answer. */
        await SeedAsync(baseNow.AddMinutes(-30), executions: 100, avgDurationUs: 1000, avgCpuUs: 1000, intervalId: 1);

        var clear = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        Assert.Equal("empty", clear.GetProperty("status").GetString());
        var clearText = clear.GetProperty("message").GetString()!;
        Assert.Contains("all-clear", clearText, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", clearText, StringComparison.Ordinal);
        Assert.DoesNotContain("Widen", clearText, StringComparison.Ordinal);

        /*
            4. recent rows but NO baseline - the branch this read exists to get right, reached from the
            SAME rows by widening the window until every one of them falls inside it. There is then nothing
            to compare against, and answering "no regressions" would be a confident wrong answer rather
            than a missing one.
        */
        var noBaseline = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 48));
        Assert.Equal("unavailable", noBaseline.GetProperty("status").GetString());
        var noBaselineText = noBaseline.GetProperty("message").GetString()!;
        Assert.Contains("no baseline", noBaselineText, StringComparison.Ordinal);
        Assert.Contains("NOT a clean bill of health", noBaselineText, StringComparison.Ordinal);

        /* Widening makes the window bigger and the baseline SHORTER - the wrong direction. */
        Assert.Contains("Shorten hours_back", noBaselineText, StringComparison.Ordinal);
        Assert.DoesNotContain("Widen", noBaselineText, StringComparison.Ordinal);

        /* Four sentences, not one sentence four times. */
        var messages = new[] { neverText, noBaselineText, noRecentText, clearText };
        Assert.Equal(4, messages.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ARealRegression_IsRanked_AndSurvivesTheDedup()
    {
        var service = new LocalDataService(_duckDb);
        var baseNow = Truncate(DateTime.UtcNow);

        /*
            Query 7 doubles its CPU and quadruples its duration between the baseline and the window. The
            baseline interval is ALSO re-collected with a higher cumulative count, which is what the dedup
            has to survive: un-deduped, the baseline exec_count would be 140 (50 + 90) rather than 90 and
            the baseline averages would be an avg-of-avgs over two snapshots of one interval.
        */
        await SeedAsync(baseNow.AddHours(-40), 50, 1000, 1000, intervalId: 3, queryId: 7);
        await SeedAsync(baseNow.AddHours(-39), 90, 1000, 1000, intervalId: 3, queryId: 7);
        await SeedAsync(baseNow.AddMinutes(-30), 200, 4000, 4000, intervalId: 4, queryId: 7);

        var hit = Root(await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24));
        Assert.Equal(ServerName, hit.GetProperty("server").GetString());
        Assert.Equal(1, hit.GetProperty("regression_count").GetInt32());
        Assert.False(hit.GetProperty("truncated").GetBoolean());

        var row = hit.GetProperty("regressions")[0];
        Assert.Equal(7, row.GetProperty("query_id").GetInt64());
        Assert.Equal(Db, row.GetProperty("database_name").GetString());

        /* 1 ms -> 4 ms is +300%, the CRITICAL band (> 100%). */
        Assert.Equal("CRITICAL", row.GetProperty("severity").GetString());
        Assert.Equal(1.0, row.GetProperty("baseline_duration_ms").GetDouble(), 6);
        Assert.Equal(4.0, row.GetProperty("recent_duration_ms").GetDouble(), 6);
        Assert.Equal(300.0, row.GetProperty("duration_regression_percent").GetDouble(), 6);

        /* The ranking key: 3 ms per execution across the 200 recent executions. */
        Assert.Equal(600.0, row.GetProperty("additional_duration_ms").GetDouble(), 6);

        /* 90, not 140. This is the dedup, and it is the assertion the whole read leans on. */
        Assert.Equal(90, row.GetProperty("baseline_exec_count").GetInt64());
        Assert.Equal(200, row.GetProperty("recent_exec_count").GetInt64());
    }

    [Fact]
    public async Task AnOutOfRangeCap_IsRefused_NotSilentlyClamped()
    {
        var service = new LocalDataService(_duckDb);
        await SeedAsync(Truncate(DateTime.UtcNow).AddMinutes(-30), 100, 1000, 1000, intervalId: 1);

        var tooBig = await McpQueryTools.GetQueryStoreRegressions(service, _serverManager, ServerName, 24, null, 5000);
        Assert.Contains("exceeds maximum of", tooBig, StringComparison.Ordinal);
        Assert.Contains("1000", tooBig, StringComparison.Ordinal);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedAsync(
        DateTime collectionTime, long executions, long avgDurationUs, long avgCpuUs, long intervalId, long queryId = 1)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
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
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgCpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100L });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM dbo.Widgets" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        await cmd.ExecuteNonQueryAsync();
    }
}
