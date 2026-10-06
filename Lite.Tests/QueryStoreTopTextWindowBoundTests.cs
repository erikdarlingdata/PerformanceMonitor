/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5420: the Query Store top read's text lookup (a LATERAL over <c>v_query_store_stats</c>) is bounded to the read's own
/// window, like the Top Queries and procedure comparison picks were in #5381. Before, it ran over the whole archive, which
/// DuckDB executes as a window over every archived row. The one visible change: a query whose only captured text is older
/// than the window now reads blank, as a query with no text anywhere already did. A query with text inside the window keeps it.
/// </summary>
public sealed class QueryStoreTopTextWindowBoundTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "query-store-text-window-5420";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private readonly string _tempDir;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public QueryStoreTopTextWindowBoundTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryStoreTextWindow_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _dataService = new LocalDataService(_duckDb);
        var serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task TheTextLookup_StopsAtTheWindow_AndKeepsTextCapturedInsideIt()
    {
        var to = DateTime.SpecifyKind(new DateTime(2026, 3, 10, 12, 0, 0), DateTimeKind.Unspecified);
        var from = to.AddHours(-2);
        var inside = to.AddMinutes(-30);
        var older = to.AddDays(-3);

        /* 1: text inside the window (an earlier in-window row has it, the newest in-window row has none). */
        await SeedAsync(1, inside.AddMinutes(-20), "SELECT 1 /* inside */");
        await SeedAsync(1, inside, null);
        /* 2: text only BEFORE the window; the in-window row has none. */
        await SeedAsync(2, older, "SELECT 2 /* only before the window */");
        await SeedAsync(2, inside, null);
        /* 3: no text anywhere. */
        await SeedAsync(3, inside, null);

        var rows = await _dataService.GetQueryStoreTopQueriesAsync(_serverId, top: 50, fromDate: from, toDate: to);

        string TextOf(long queryId) => rows.Single(r => r.QueryId == queryId).QueryText;
        Assert.Equal("SELECT 1 /* inside */", TextOf(1));
        Assert.Equal("", TextOf(2));
        Assert.Equal("", TextOf(3));
    }

    private async Task SeedAsync(long queryId, DateTime collectionTime, string? queryText)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open) await _seedConn.OpenAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO query_store_stats
                (collection_id, collection_time, server_id, server_name, database_name,
                 query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
                 module_name, query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
                 avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount,
                 query_plan_hash, runtime_stats_interval_id)
            VALUES
                ($1, $2, $3, $4, 'db5420',
                 $5, $5, 'Regular', $2, $2,
                 NULL, $6, $7, 100, 10, 20,
                 1, 1, 1, 1,
                 $8, $9)
            """;
        object[] values = { _nextId--, collectionTime, _serverId, ServerName, queryId, (object?)queryText ?? DBNull.Value, "0xQ" + queryId, "0xP" + queryId, queryId * 1000 + collectionTime.DayOfYear };
        foreach (var v in values) cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync();
    }
}
