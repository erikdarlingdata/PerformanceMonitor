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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's get_current_waits_trend (#2484), twin of Darling's. Ported in the same change rather than parked
/// on the divergence ratchet.
///
/// <para>The empty case is the one that matters, and it is sharper here than for most reads: the WRONG
/// answer is the reassuring one. "Nothing was waiting" reads as an all-clear, and a caller who believes it
/// stops looking — so a server the collector has never sampled must not be described that way.</para>
/// </summary>
public sealed class CurrentWaitsTrendToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "CurrentWaitsSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 700000;

    public CurrentWaitsTrendToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-waits-" + Guid.NewGuid().ToString("N"));
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
    public async Task NeverSampled_IsNotReportedAsAnAllClear()
    {
        var service = new LocalDataService(_duckDb);

        var never = await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 4, null);
        var root = JsonDocument.Parse(never).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("NOT an all-clear", text, StringComparison.Ordinal);
        Assert.Contains("EVER", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SampledButQuiet_IsAGenuineAllClear_AndSaysSoDifferently()
    {
        var service = new LocalDataService(_duckDb);
        await SeedWaitAsync(DateTime.UtcNow.AddHours(-48), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");

        var clear = await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 1, null);
        var root = JsonDocument.Parse(clear).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("genuine all-clear", text, StringComparison.Ordinal);

        /* Same zero rows as the never-sampled case, and it must not reach for the same word. */
        Assert.DoesNotContain("EVER", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #6453: a collector whose newest run completed clean and stored no rows is HEALTHY in get_collection_health, so the read
    /// says <c>empty</c>, not <c>unavailable</c>; a faulting newest run keeps <c>unavailable</c>.
    /// </summary>
    [Fact]
    public async Task NeverSampled_ButTheCollectorRanClean_IsEmpty_AndAFaultingRunStaysUnavailable()
    {
        var service = new LocalDataService(_duckDb);
        await SeedCollectionLogAsync(DateTime.UtcNow.AddMinutes(-5), "SUCCESS", null);

        var healthy = JsonDocument.Parse(await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 4, null)).RootElement;
        Assert.Equal("empty", healthy.GetProperty("status").GetString());
        var text = healthy.GetProperty("message").GetString()!;
        Assert.Contains("true negative", text, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT an all-clear", text, StringComparison.Ordinal);

        await SeedCollectionLogAsync(DateTime.UtcNow.AddMinutes(-1), "ERROR", "login failed");
        var faulted = JsonDocument.Parse(await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 4, null)).RootElement;
        Assert.Equal("unavailable", faulted.GetProperty("status").GetString());
    }

    [Fact]
    public async Task BothSeriesComeBackTogether_AndOnlyBlockedRowsCountAsBlocked()
    {
        var service = new LocalDataService(_duckDb);
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-10), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-9), "PAGEIOLATCH_SH", 250, blockingSessionId: 0, database: "AppDb");

        var hit = await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 4, null);
        var root = JsonDocument.Parse(hit).RootElement;

        Assert.Equal(2, root.GetProperty("waiting_tasks").GetArrayLength());

        /*
            The PAGEIOLATCH row waits on IO and blocks on nothing, so it is in the wait series and NOT the
            blocked series. That difference is exactly why the two are returned together: the same wait
            spike means a resource problem without blocked sessions and contention with them.
        */
        var blocked = root.GetProperty("blocked_sessions");
        Assert.Equal(1, blocked.GetArrayLength());
        Assert.Equal("AppDb", blocked[0].GetProperty("database_name").GetString());
    }

    /// <summary>
    /// #7877, #7904: two hours of one-minute collections read with a 30-hour look-back, as Darling's twin test does. The read summed a
    /// bucket and said nothing about it; now each point is still the figure of ONE collection (averaged over the bucket) and the payload
    /// names the width and each point's collection count.
    /// </summary>
    [Fact]
    public async Task ALongWindow_IsBucketed_EachPointIsPerCollection_AndNamesItsWidthAndCount()
    {
        var service = new LocalDataService(_duckDb);
        const int collections = 120;
        for (var minute = 0; minute < collections; minute++)
        {
            await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-(5 + minute)), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");
        }

        var root = JsonDocument.Parse(await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 30, null)).RootElement;

        var bucketMinutes = root.GetProperty("bucket_minutes").GetInt32();
        Assert.True(bucketMinutes > 1, "a 30-hour window with two series cannot be served one collection per point");
        Assert.Contains("ONE collection", root.GetProperty("note").GetString(), StringComparison.Ordinal);
        var waits = root.GetProperty("waiting_tasks").EnumerateArray().ToArray();
        var blocked = root.GetProperty("blocked_sessions").EnumerateArray().ToArray();
        Assert.NotEmpty(waits);
        Assert.NotEmpty(blocked);
        Assert.True(waits.Length + blocked.Length <= TrendBuckets.McpPointBudget, "the two series together stay within the point budget");

        /* Every point is the figure of one collection, whatever the width: 500 ms, never 500 times the collections it merged. */
        Assert.All(waits, w =>
        {
            Assert.Equal(500, w.GetProperty("total_wait_ms").GetInt64());
            Assert.Equal(500, w.GetProperty("peak_wait_ms").GetInt64());
        });
        Assert.All(blocked, b =>
        {
            Assert.Equal(1, b.GetProperty("blocked_count").GetDouble());
            Assert.Equal(1, b.GetProperty("peak_blocked_count").GetInt64());
        });
        Assert.True(waits.Max(w => w.GetProperty("collection_count").GetInt64()) > 1, "a bucket says how many collections it covers");
        Assert.Equal(collections, waits.Sum(w => w.GetProperty("collection_count").GetInt64()));

        /* A width the caller names is refused, not widened, when it would put more points on the wire than the read's cap. */
        var narrow = await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 30, null, null, 1);
        Assert.Contains("bucket_minutes", narrow, StringComparison.Ordinal);
        Assert.DoesNotContain("waiting_tasks", narrow, StringComparison.Ordinal);

        /* A window short enough to need no bucketing serves one point per collection. */
        var shortWindow = JsonDocument.Parse(await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 1, null)).RootElement;
        Assert.Equal(1, shortWindow.GetProperty("bucket_minutes").GetInt32());
        Assert.Equal(1, shortWindow.GetProperty("waiting_tasks")[0].GetProperty("collection_count").GetInt64());
    }

    /// <summary>#5244 L3: a blank database_name is "no filter" and is echoed as null, the way Darling's twin echoes it; a name is echoed as sent.</summary>
    [Theory]
    [InlineData("   ", null)]
    [InlineData("", null)]
    [InlineData("AppDb", "AppDb")]
    public async Task DatabaseName_IsEchoedNormalized_BlankIsNull(string sent, string? echoed)
    {
        var service = new LocalDataService(_duckDb);
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-10), "LCK_M_X", 500, blockingSessionId: 99, database: "AppDb");

        var root = JsonDocument.Parse(await McpHealthTools.GetCurrentWaitsTrend(service, _serverManager, ServerName, 4, sent)).RootElement;

        var echo = root.GetProperty("database_name");
        Assert.Equal(echoed, echo.ValueKind == JsonValueKind.Null ? null : echo.GetString());
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedCollectionLogAsync(DateTime collectionTimeUtc, string status, string? errorMessage)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, error_message, rows_collected)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "waiting_tasks" });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)errorMessage ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0 });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedWaitAsync(
        DateTime collectionTimeUtc, string waitType, long waitMs, int blockingSessionId, string database)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO waiting_tasks
    (collection_id, collection_time, server_id, server_name, session_id, wait_type,
     wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = 55 });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockingSessionId });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        await cmd.ExecuteNonQueryAsync();
    }
}
