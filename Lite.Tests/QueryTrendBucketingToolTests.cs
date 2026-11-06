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
using System.Reflection;
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
/// #7907: Lite's <c>get_query_trend</c> is bucketed and capped like the other trends. Lite collects query_stats every
/// minute, so one point per collection is about 1,440 points over the default day and about 10,000 over a week; the
/// points are gathered into buckets sized to the shared point budget, each summing the collections it holds, with
/// <c>bucket_minutes</c> naming a width. Run through the real tool over a real DuckDB.
/// </summary>
public sealed class QueryTrendBucketingToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "QueryTrendBucketSrv";
    private const string Database = "AppDb";
    private const string Hash = "0xBUCKETHASH";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 910000;

    public QueryTrendBucketingToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-querytrend-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);

        /* Derived, not stored -- seeding under a hardcoded id would write rows the tool looks past. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task ADayOfMinuteCollections_IsBucketedWithinTheBudget_AndEachPointSumsItsCollections()
    {
        var service = new LocalDataService(_duckDb);
        var asOf = new DateTime(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);
        /* One collection a minute for the whole 24-hour window, every one 10 executions and 10 ms of CPU. */
        var first = asOf.AddHours(-24).AddSeconds(30);
        for (var i = 0; i < 1440; i++)
        {
            await SeedQueryAsync(first.AddMinutes(i), executions: 10, cpuUs: 10_000);
        }

        var root = JsonDocument.Parse(await McpQueryTools.GetQueryTrend(
            service, _serverManager, Hash, Database, ServerName, 24, asOf.ToString("o"))).RootElement;

        Assert.False(root.TryGetProperty("status", out _), root.GetRawText());
        Assert.True(root.TryGetProperty("bucket_minutes", out var width) && width.ValueKind == JsonValueKind.Number,
            "the answer names no bucket width: " + root.GetProperty("bucket").GetRawText());
        Assert.True(width.GetInt32() > 1, "a day of one-minute collections cannot be served one point per collection");
        var points = root.GetProperty("trend").EnumerateArray().ToList();
        Assert.True(points.Count <= 200, $"{points.Count} points is over the 200-point target");
        Assert.Equal(points.Count, root.GetProperty("data_points").GetInt32());

        /* The deltas are summed per bucket and the averages weighted by executions, so every point reads 1 ms a run. */
        Assert.Equal(14_400, points.Sum(p => p.GetProperty("execution_count").GetInt64()));
        Assert.Equal(1440, points.Sum(p => p.GetProperty("collection_count").GetInt64()));
        Assert.All(points, p =>
        {
            Assert.Equal(p.GetProperty("collection_count").GetInt64() * 10, p.GetProperty("execution_count").GetInt64());
            Assert.Equal(p.GetProperty("collection_count").GetInt64() * 10.0, p.GetProperty("cpu_ms").GetDouble(), 3);
            Assert.Equal(1.0, p.GetProperty("avg_cpu_ms").GetDouble(), 3);
            Assert.Equal("0xPLAN", p.GetProperty("query_plan_hash").GetString());
            Assert.Equal(1, p.GetProperty("plan_count").GetInt32());
        });

        /* The first point sits at the window's start, and effective_start is the first collection the store held. */
        Assert.Equal(DateTime.SpecifyKind(asOf.AddHours(-24), DateTimeKind.Unspecified).ToString("o"), points[0].GetProperty("collection_time").GetString());
        Assert.Equal(DateTime.SpecifyKind(first, DateTimeKind.Unspecified).ToString("o"), root.GetProperty("effective_start").GetString());
        Assert.Contains("bucket", root.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnHourOfMinuteCollections_KeepsOnePointPerCollection()
    {
        var service = new LocalDataService(_duckDb);
        var asOf = new DateTime(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);
        var first = asOf.AddHours(-1).AddSeconds(30);
        for (var i = 0; i < 60; i++)
        {
            await SeedQueryAsync(first.AddMinutes(i), executions: 2, cpuUs: 4_000);
        }

        var root = JsonDocument.Parse(await McpQueryTools.GetQueryTrend(
            service, _serverManager, Hash, Database, ServerName, 1, asOf.ToString("o"))).RootElement;

        Assert.False(root.TryGetProperty("status", out _), root.GetRawText());
        Assert.True(root.TryGetProperty("bucket_minutes", out var width) && width.ValueKind == JsonValueKind.Number,
            "the answer names no bucket width: " + root.GetProperty("bucket").GetRawText());
        Assert.Equal(1, width.GetInt32());
        Assert.Equal("1 minute", root.GetProperty("bucket").GetString());
        var points = root.GetProperty("trend").EnumerateArray().ToList();
        Assert.Equal(60, points.Count);
        Assert.All(points, p =>
        {
            Assert.Equal(1, p.GetProperty("collection_count").GetInt64());
            Assert.Equal(2, p.GetProperty("execution_count").GetInt64());
            Assert.Equal(2.0, p.GetProperty("avg_cpu_ms").GetDouble(), 3);
        });
    }

    /// <summary>
    /// <c>bucket_minutes</c> names a width the way the sibling trends take it, and a width that would put the answer over
    /// the read's cap is refused with the narrowest width that fits. Reached by reflection so this test builds against a
    /// tool that does not have the parameter yet, and fails there by assertion.
    /// </summary>
    [Fact]
    public async Task ANamedWidth_IsServed_AndOneOverTheCapIsRefused()
    {
        var service = new LocalDataService(_duckDb);
        var asOf = new DateTime(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);
        var first = asOf.AddHours(-24).AddSeconds(30);
        for (var i = 0; i < 1440; i += 5)
        {
            await SeedQueryAsync(first.AddMinutes(i), executions: 5, cpuUs: 5_000);
        }

        var method = typeof(McpQueryTools).GetMethod(nameof(McpQueryTools.GetQueryTrend), BindingFlags.Public | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.True(parameters.Any(p => p.Name == "bucket_minutes"),
            "get_query_trend takes no bucket_minutes; it has: " + string.Join(", ", parameters.Select(p => p.Name)));

        var hourly = JsonDocument.Parse(await Invoke(method, service, 24, asOf, bucketMinutes: 60)).RootElement;
        Assert.False(hourly.TryGetProperty("status", out _), hourly.GetRawText());
        Assert.Equal(60, hourly.GetProperty("bucket_minutes").GetInt32());
        Assert.Equal("1 hour", hourly.GetProperty("bucket").GetString());
        var points = hourly.GetProperty("trend").EnumerateArray().ToList();
        Assert.InRange(points.Count, 24, 25);
        Assert.Equal(288, points.Sum(p => p.GetProperty("collection_count").GetInt64()));
        Assert.Equal(1440, points.Sum(p => p.GetProperty("execution_count").GetInt64()));

        /* One-minute points over a week are about 10,000: over the cap, so the width is refused, never widened. */
        var refused = JsonDocument.Parse(await Invoke(method, service, 168, asOf, bucketMinutes: 1)).RootElement;
        Assert.Equal("invalid", refused.GetProperty("status").GetString());
        Assert.Contains("bucket_minutes", refused.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    private async Task<string> Invoke(MethodInfo method, LocalDataService service, int hoursBack, DateTime asOf, int bucketMinutes)
    {
        var args = method.GetParameters().Select(p => p.Name switch
        {
            "dataService" => (object?)service,
            "serverManager" => _serverManager,
            "query_hash" => Hash,
            "database_name" => Database,
            "server_name" => ServerName,
            "hours_back" => hoursBack,
            "as_of" => asOf.ToString("o"),
            "bucket_minutes" => bucketMinutes,
            _ => p.HasDefaultValue ? p.DefaultValue : null,
        }).ToArray();
        return await (Task<string>)method.Invoke(null, args)!;
    }

    private async Task SeedQueryAsync(DateTime collectionTimeUtc, long executions, long cpuUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, query_plan_hash, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, min_dop, max_dop, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 1, 1, $13)";
        var naive = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified);
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId++);
        P(naive);
        P(_serverId);
        P(ServerName);
        P(Database);
        P(Hash);
        P("0xSQLH");
        P("0xPLAN");
        P(naive);
        P(executions);
        P(cpuUs);
        P(cpuUs * 2);
        P("SELECT * FROM Orders");
        await cmd.ExecuteNonQueryAsync();
    }
}
