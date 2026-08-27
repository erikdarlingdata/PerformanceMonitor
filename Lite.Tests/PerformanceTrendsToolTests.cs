/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// Lite's Performance-Trends siblings (#2484) - get_procedure_duration_trend and
/// get_query_store_duration_trend - and the two-branch empty answer all three siblings now share.
///
/// <para>Written at the TOOL level: the empty branches and the payload field names live in the tool, and a
/// reader-level test would see neither. The SKUs promise each other the same sentences for the same state,
/// which is a promise only a test can hold.</para>
/// </summary>
public sealed class PerformanceTrendsToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "PerfTrendsSrv";
    private const string Db = "AppDb";

    /* Lite DERIVES its server id from the storage name; seeding under a hardcoded one would write rows the
       tool looks straight past and pass the never-sampled assertion for the wrong reason. */
    private readonly int _serverId;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public PerformanceTrendsToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-perftrends-" + Guid.NewGuid().ToString("N"));
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
    public async Task AQuietWindow_AndNeverSampled_AreDifferentAnswers()
    {
        var service = new LocalDataService(_duckDb);

        /* Registered, never sampled: not a quiet server, and it must not be described as one. */
        var never = await McpQueryTools.GetProcedureDurationTrend(service, _serverManager, ServerName, 24);
        var neverRoot = JsonDocument.Parse(never).RootElement;
        Assert.Equal("unavailable", neverRoot.GetProperty("status").GetString());
        var neverText = neverRoot.GetProperty("message").GetString()!;
        Assert.Contains("EVER", neverText, StringComparison.Ordinal);
        Assert.Contains("NOT a quiet server", neverText, StringComparison.Ordinal);
        Assert.DoesNotContain("widen", neverText, StringComparison.OrdinalIgnoreCase);

        /* Sampled, but outside the asked-for window: widening IS the move. */
        await SeedProcedureAsync(Truncate(DateTime.UtcNow.AddHours(-48)), executions: 5, elapsedUs: 1_000_000);

        var quiet = await McpQueryTools.GetProcedureDurationTrend(service, _serverManager, ServerName, 1);
        var quietRoot = JsonDocument.Parse(quiet).RootElement;
        Assert.Equal("empty", quietRoot.GetProperty("status").GetString());
        var quietText = quietRoot.GetProperty("message").GetString()!;
        Assert.Contains("widen", quietText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task QueryStoreEmpty_NamesTheCauseThePlanCacheTrendsDoNotHave()
    {
        var service = new LocalDataService(_duckDb);

        var never = await McpQueryTools.GetQueryStoreDurationTrend(service, _serverManager, ServerName, 24);
        var root = JsonDocument.Parse(never).RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());

        /*
            Query Store can simply be OFF on every database, which no amount of collector health fixes. A
            message that led with the collector would send the reader to the wrong place.
        */
        Assert.Contains("Query Store may be OFF", root.GetProperty("message").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheExecutionRate_SurvivesBeingBelowOnePerSecond()
    {
        var service = new LocalDataService(_duckDb);

        /* Two snapshots five minutes apart, two executions between them: 0.0067/sec. */
        var baseNow = Truncate(DateTime.UtcNow);
        await SeedProcedureAsync(baseNow.AddMinutes(-20), executions: 0, elapsedUs: 0);
        await SeedProcedureAsync(baseNow.AddMinutes(-15), executions: 2, elapsedUs: 600_000);

        var hit = await McpQueryTools.GetProcedureDurationTrend(service, _serverManager, ServerName, 4);
        var trend = JsonDocument.Parse(hit).RootElement.GetProperty("trend");
        Assert.Equal(2, trend.GetArrayLength());

        var second = trend[1];
        Assert.True(second.GetProperty("value").GetDouble() > 0, "elapsed ms/sec must be a real rate");

        /* The shipped integer field rounds this to an idle server. The double is why it is here. */
        Assert.Equal(0, second.GetProperty("execution_count").GetInt64());
        Assert.True(
            second.GetProperty("executions_per_second").GetDouble() > 0,
            "executions_per_second must survive a rate below 1/sec that execution_count truncates to zero");
    }

    [Fact]
    public async Task EachQueryStoreInterval_IsCountedOnce_AtTheHourTheWorkRan()
    {
        var service = new LocalDataService(_duckDb);

        /*
            Two runtime intervals, each fetched twice while open, the second fetch carrying the higher
            cumulative count. Charging every fetch to its collection time would give four points and double
            the work; dedup + placement gives two, at the interval starts.
        */
        var baseNow = Truncate(DateTime.UtcNow);
        var intervalA = baseNow.AddHours(-3);
        var intervalB = baseNow.AddHours(-2);
        await SeedQueryStoreAsync(baseNow.AddMinutes(-150), 41, intervalA, executions: 10, avgDurationUs: 2000);
        await SeedQueryStoreAsync(baseNow.AddMinutes(-145), 41, intervalA, executions: 40, avgDurationUs: 2000);
        await SeedQueryStoreAsync(baseNow.AddMinutes(-90), 42, intervalB, executions: 5, avgDurationUs: 3000);
        await SeedQueryStoreAsync(baseNow.AddMinutes(-85), 42, intervalB, executions: 25, avgDurationUs: 3000);

        var hit = await McpQueryTools.GetQueryStoreDurationTrend(service, _serverManager, ServerName, 6);
        var trend = JsonDocument.Parse(hit).RootElement.GetProperty("trend");

        Assert.Equal(2, trend.GetArrayLength());

        /*
            The surviving snapshot is the FINAL one (25 executions over the 3600 seconds between the two
            interval starts), not the first (5) and not their sum (30). A dedup keeping the wrong row would
            still return two points and would still look right.
        */
        Assert.Equal(25d / 3600d, trend[1].GetProperty("executions_per_second").GetDouble(), 6);
    }

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

    private async Task SeedProcedureAsync(DateTime collectionTimeUtc, long executions, long elapsedUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, schema_name, object_name,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = "usp_Trend" });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs / 2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryStoreAsync(
        DateTime collectionTime, long intervalId, DateTime intervalStart, long executions, long avgDurationUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, execution_count, avg_duration_us,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = 7L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 9L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = executions });
        cmd.Parameters.Add(new DuckDBParameter { Value = avgDurationUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalId });
        cmd.Parameters.Add(new DuckDBParameter { Value = intervalStart });
        await cmd.ExecuteNonQueryAsync();
    }
}
