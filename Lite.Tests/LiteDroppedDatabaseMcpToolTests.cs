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
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7725: Lite's get_top_queries_by_cpu and get_query_store_regressions rank the databases the server still lists first, then
/// fill what the limit leaves from the databases it dropped (ranked the same way), and mark each of those rows
/// <c>database_dropped</c>; get_top_procedures_by_cpu does the same and its envelope names only the databases with no row.
/// Own class, own database: GoneDb was in an earlier database_states snapshot and is absent from the newest.
/// </summary>
public sealed class LiteDroppedDatabaseMcpToolTests : IDisposable
{
    private const string ServerName = "DroppedToolServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly DateTime _now = DateTime.UtcNow;
    private long _nextId = 1;

    public LiteDroppedDatabaseMcpToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "DroppedMcpTool_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "tool.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(_tempDir);
        var connection = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(connection);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(connection));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private async Task ExecAsync(string sql, params object[] values)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
        {
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private DateTime Ago(int minutes) => _now.AddMinutes(-minutes);

    /// <summary>An earlier snapshot names GoneDb and LiveDb; the newest names LiveDb only, so GoneDb was dropped in between.</summary>
    private async Task SeedDropAsync()
    {
        foreach (var (age, database) in new[] { (180, "GoneDb"), (180, "LiveDb"), (10, "LiveDb") })
        {
            await ExecAsync(
                "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1, $2, $3, 'x', $4, 5, 'ONLINE', false)",
                _nextId++, Ago(age), _serverId, database);
        }
    }

    private Task ProcedureAsync(string database, string objectName, long cpuUs) => ExecAsync(
        "INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time) VALUES ($1, $2, $3, 'x', $4, 'dbo', $5, 'SQL_STORED_PROCEDURE', $2, 10, $6, $6)",
        _nextId++, Ago(60), _serverId, database, objectName, cpuUs);

    private Task QueryAsync(string database, string hash, long cpuUs) => ExecAsync(
        "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes) VALUES ($1, $2, $3, 'x', $4, $5, $2, 1, $6, $6, 10, 1)",
        _nextId++, Ago(60), _serverId, database, hash, cpuUs);

    /// <summary>One baseline row (3 days back) and one recent row (1 hour back) at 1,000 runs, so the 2 ms rise clears the cost gate.</summary>
    private async Task RegressionAsync(string database, long queryId)
    {
        foreach (var (at, avgUs, interval) in new[] { (_now.AddDays(-3), 1000L, 1L), (_now.AddHours(-1), 3000L, 2L) })
        {
            await ExecAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, 'x', $4, $5, 9, 'Regular', 1000, $6, $6, 100, $7, 'SELECT * FROM dbo.Widgets', $2)",
                _nextId++, at, _serverId, database, queryId, avgUs, interval);
        }
    }

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static string[] Databases(JsonElement root, string rows) =>
        root.GetProperty(rows).EnumerateArray().Select(r => r.GetProperty("database_name").GetString()!).ToArray();

    private static bool[] Marks(JsonElement root, string rows) =>
        root.GetProperty(rows).EnumerateArray().Select(r => r.TryGetProperty("database_dropped", out var flag) && flag.GetBoolean()).ToArray();

    private static string[] LeftOut(JsonElement root) =>
        root.TryGetProperty("databases_left_out", out var named) && named.ValueKind == JsonValueKind.Array
            ? named.EnumerateArray().Select(n => n.GetString()!).ToArray()
            : Array.Empty<string>();

    [Fact]
    public async Task TopProcedures_FillFromTheDroppedDatabase_AfterTheListedOnes_AndMarkIt()
    {
        await SeedDropAsync();
        await ProcedureAsync("GoneDb", "usp_Gone", 900_000);
        await ProcedureAsync("LiveDb", "usp_Live", 100_000);

        var all = Parse(await McpQueryTools.GetTopProceduresByCpu(_service, _serverManager, ServerName, 24, 10));
        Assert.Equal(new[] { "LiveDb", "GoneDb" }, Databases(all, "procedures"));
        Assert.Equal(new[] { false, true }, Marks(all, "procedures"));
        Assert.Empty(LeftOut(all));

        var one = Parse(await McpQueryTools.GetTopProceduresByCpu(_service, _serverManager, ServerName, 24, 1));
        Assert.Equal(new[] { "LiveDb" }, Databases(one, "procedures"));
        Assert.Equal(new[] { "GoneDb" }, LeftOut(one));
    }

    [Fact]
    public async Task TopQueries_RankTheListedDatabaseFirst_ThenTheDroppedOne_Marked()
    {
        await SeedDropAsync();
        await QueryAsync("GoneDb", "0xGONE", 900_000);
        await QueryAsync("LiveDb", "0xLIVE", 100_000);

        /* GoneDb is the heavier database and used to rank first, unmarked. */
        var all = Parse(await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, 24, 10));
        Assert.Equal(new[] { "LiveDb", "GoneDb" }, Databases(all, "queries"));
        Assert.Equal(new[] { false, true }, Marks(all, "queries"));

        var one = Parse(await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, 24, 1));
        Assert.Equal(new[] { "LiveDb" }, Databases(one, "queries"));
        Assert.Equal(new[] { false }, Marks(one, "queries"));

        /* A named database is read as asked and is not marked. */
        var named = Parse(await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, 24, 10, "GoneDb"));
        Assert.Equal(new[] { "GoneDb" }, Databases(named, "queries"));
        Assert.Equal(new[] { false }, Marks(named, "queries"));
    }

    [Fact]
    public async Task QueryStoreRegressions_ListTheListedDatabaseFirst_ThenTheDroppedOne_Marked()
    {
        await SeedDropAsync();
        await RegressionAsync("GoneDb", queryId: 1);
        await RegressionAsync("LiveDb", queryId: 2);

        var all = Parse(await McpQueryTools.GetQueryStoreRegressions(_service, _serverManager, ServerName, hours_back: 24, limit: 10));
        Assert.Equal(new[] { "LiveDb", "GoneDb" }, Databases(all, "regressions"));
        Assert.Equal(new[] { false, true }, Marks(all, "regressions"));
        Assert.Empty(LeftOut(all));
        Assert.Equal(2, all.GetProperty("regression_count").GetInt32());

        var one = Parse(await McpQueryTools.GetQueryStoreRegressions(_service, _serverManager, ServerName, hours_back: 24, limit: 1));
        Assert.Equal(new[] { "LiveDb" }, Databases(one, "regressions"));
        Assert.Equal(new[] { "GoneDb" }, LeftOut(one));
    }
}
