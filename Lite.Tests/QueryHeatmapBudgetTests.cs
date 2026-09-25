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
using System.Text;
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
/// #4198 byte-budget pin for Lite's get_query_heatmap, the twin of
/// <c>Darling.Tests.DarlingQueryHeatmapBudgetLiveTests</c>. Needs no rig: Lite reads its own DuckDB file, so
/// this seeds a fresh one directly, same as <c>QueryHeatmapToolTests</c> does for the shape pins.
///
/// <para>A heatmap payload is CELLS (queries times time buckets) plus TEXT. #4198 measured a real busy store
/// at default arguments — 144,757 bytes for 500 cells of a 120-character preview each — and the seed below
/// reproduces that shape: enough distinct (time bin, magnitude bucket) cells to fill the default cap several
/// times over, each carrying a 227-character statement (past both the old 120-character preview and the new
/// 80-character one), so both the CAP and the TEXT WIDTH are exercised, not just one of them.</para>
/// </summary>
public sealed class QueryHeatmapBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "HeatmapBudgetSrv";
    private const string Db = "AppDb";

    /* Realistic, not minimal: a two-table join with a WHERE and an ORDER BY, 227 characters - past both the
       old 120-char preview and the new 80-char default, so every seeded cell's top query is truncated at
       default and NOT truncated under full_text. Matches Darling's twin seed exactly. */
    private const string QueryText =
        "SELECT o.OrderId, o.CustomerId, o.OrderDate, o.TotalAmount, c.CustomerName FROM dbo.Orders AS o " +
        "JOIN dbo.Customers AS c ON o.CustomerId = c.CustomerId WHERE o.OrderDate >= @start AND o.Status = @status " +
        "ORDER BY o.OrderDate DESC";

    private const int SeedBins = 60;

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryHeatmapBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-heatmap-budget-" + Guid.NewGuid().ToString("N"));
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
    public async Task DefaultCall_OnABusyStore_StaysUnderTheSharedBudget()
    {
        Assert.True(QueryText.Length > 120, "seed text must exceed both the old and new preview widths");

        var service = new LocalDataService(_duckDb);
        var t0 = FloorToHour(Truncate(DateTime.UtcNow)).AddHours(-1);
        long[] elapsedMicrosByBucket = { 500, 5_000, 50_000, 500_000, 5_000_000, 50_000_000, 500_000_000 };

        /* SeedBins (60) x 7 magnitude buckets = 420 cells, well past both the new default cap (100) and the
           old one (500) - a server busy enough that the cap, not the window, bounds the default call either
           way. Execution count pinned at 1, so metric_value is delta_elapsed/1000 ms directly. */
        for (var bin = 0; bin < SeedBins; bin++)
        {
            var t = t0.AddMinutes(-5 * bin);
            for (var bucket = 0; bucket < 7; bucket++)
            {
                /* 18 characters - CONVERT(varchar(64), query_hash, 1)'s real width for an 8-byte hash, not a
                   shortened test stand-in. */
                await SeedAsync(t, $"0x{bin:X8}{bucket:X8}", elapsedMicrosByBucket[bucket]);
            }
        }

        /* ── default call: the fix under test ── */
        var defaultJson = await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName);
        var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
        var root = Root(defaultJson);

        Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
            $"get_query_heatmap default call is {defaultBytes:N0} bytes, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        /*
            Every seeded bin is fully populated (all seven buckets), so the cap lands mid-bin and the "no
            partial column" rule drops the one bin the cap only partly reached — cell_count is the cap
            rounded DOWN to a whole number of bins, not the cap itself. Darling twin asserts the same shape.
        */
        var cellCount = root.GetProperty("cell_count").GetInt32();
        Assert.True(cellCount % 7 == 0 && cellCount <= 100 && cellCount > 100 - 7,
            $"cell_count {cellCount} should be the default cap (100) rounded down to whole bins");
        Assert.True(root.GetProperty("truncated").GetBoolean(), "420 populated cells at the default cap must report truncated");
        Assert.False(root.GetProperty("full_text").GetBoolean());

        var cells = root.GetProperty("cells").EnumerateArray().ToArray();
        Assert.Equal(cellCount, cells.Length);
        Assert.All(cells, c =>
        {
            Assert.True(c.GetProperty("top_query_text_truncated").GetBoolean());
            Assert.Equal(80, c.GetProperty("top_query_text").GetString()!.Length);
            Assert.Equal(QueryText[..80], c.GetProperty("top_query_text").GetString());
        });

        /* ── full_text opts back into the whole statement, honestly marked as not truncated ── */
        var fullJson = await McpQueryTools.GetQueryHeatmap(
            service, _serverManager, ServerName, 24, null, null, LocalDataService.ViewerHeatmapBucketMinutes,
            100, null, true);
        var fullRoot = Root(fullJson);
        Assert.True(fullRoot.GetProperty("full_text").GetBoolean());
        var fullCells = fullRoot.GetProperty("cells").EnumerateArray().ToArray();
        Assert.All(fullCells, c =>
        {
            Assert.False(c.GetProperty("top_query_text_truncated").GetBoolean());
            Assert.Equal(QueryText, c.GetProperty("top_query_text").GetString());
        });
        /* An explicit ask still gets what it asks for (#4198's ruling) even past the budget. */
        var fullBytes = Encoding.UTF8.GetByteCount(fullJson);
        Assert.True(fullBytes > defaultBytes, "full_text=true must not be smaller than the truncated default");
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime Truncate(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static DateTime FloorToHour(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerHour), value.Kind);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedAsync(DateTime collectionTime, string queryHash, long deltaElapsed)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        var naive = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified);
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xSQLH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaElapsed });
        cmd.Parameters.Add(new DuckDBParameter { Value = QueryText });
        await cmd.ExecuteNonQueryAsync();
    }
}

