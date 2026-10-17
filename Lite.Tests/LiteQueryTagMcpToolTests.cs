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
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Database;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Behaviour pins through the MCP tool layer for the query tag work (#5992): the tag on the query reads, the filter's echo in each
/// payload, the tag-only empty message, the roll-up's fleet total under a tag and a known tag that matches nothing. Own class, own database.
/// </summary>
public sealed class LiteQueryTagMcpToolTests : IDisposable
{
    private const string ServerA = "TagToolServerA";
    private const string ServerB = "TagToolServerB";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly ServerManager _serverManager;
    private readonly int _idA;
    private readonly int _idB;
    private readonly DateTime _now = DateTime.UtcNow;
    private long _nextId = 1;

    public LiteQueryTagMcpToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryTagMcpTool_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "tool.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(_tempDir);
        var a = new ServerConnection { ServerName = ServerA, DisplayName = ServerA };
        var b = new ServerConnection { ServerName = ServerB, DisplayName = ServerB };
        _serverManager.AddServer(a);
        _serverManager.AddServer(b);
        _idA = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(a));
        _idB = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(b));
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

    private Task MapAsync(int server, string kind, string key, int tagId) => ExecAsync(
        "INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ($1, $2, $3, $4, 'hand_hash')", server, kind, key, tagId);

    private Task PlanAsync(int server, string hash, long cpuUs) => ExecAsync(
        "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes) VALUES ($1, $2, $3, 'x', 'db1', $4, $2, 1, $5, $5, 10, 1)",
        _nextId++, Ago(30), server, hash, cpuUs);

    private Task SnapshotAsync(int server, int session, string hash, string? wait) => ExecAsync(
        "INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, wait_type, wait_time_ms, query_hash) VALUES ($1, $2, $3, 'x', $4, 'db1', 'select 1', 'running', $5, 10, $6)",
        _nextId++, Ago(20), server, session, wait!, hash);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private async Task<string> CpuTagAsync(string name, int server, string hash)
    {
        var id = await _service.CreateQueryTagAsync(name, null);
        await MapAsync(server, "hash", hash, id);
        return name;
    }

    [Fact]
    public async Task ResourceUse_UnderATag_FleetTotalIsTheSumOfTheServersTaggedTotals()
    {
        var a = await _service.CreateQueryTagAsync("A", null);
        var other = await _service.CreateQueryTagAsync("Other", null);
        foreach (var server in new[] { _idA, _idB })
        {
            await PlanAsync(server, "0x01", 4_000_000);
            await PlanAsync(server, "0x02", 2_000_000);
            await PlanAsync(server, "0x03", 1_000_000);
            await MapAsync(server, "hash", "0x01", a);
            await MapAsync(server, "hash", "0x02", other);
        }

        var root = Parse(await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, null, 24, "A"));
        var plan = root.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == "plan_cache");

        var perServer = plan.GetProperty("servers").EnumerateArray().Select(s => s.GetProperty("total").GetProperty("cpu_ms").GetDouble()).ToArray();
        Assert.Equal([4000.0, 4000.0], perServer);
        Assert.Equal(8000.0, plan.GetProperty("fleet_total").GetProperty("cpu_ms").GetDouble());
    }

    [Fact]
    public async Task ResourceUse_AKnownTagThatMatchesNothing_IsEmptyWithItsOwnMessage()
    {
        await _service.CreateQueryTagAsync("Idle", null);
        await PlanAsync(_idA, "0x01", 1_000_000);

        var root = Parse(await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, ServerA, 24, "Idle"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("query tag 'Idle'", message, StringComparison.Ordinal);
        Assert.DoesNotContain("No query tag is named", message, StringComparison.Ordinal);
        var plan = root.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == "plan_cache");
        Assert.False(plan.TryGetProperty("untagged", out _), "a tag filter has no untagged row, even on an empty section");
    }

    [Fact]
    public async Task ResourceUse_AnUnknownTagName_IsStillARefusalNotTheEmptyAnswer()
    {
        var root = Parse(await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, ServerA, 24, "Nope"));

        Assert.Contains("No query tag is named", root.GetRawText(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ActiveQueries_TagAndWaitType_KeepsTheTaggedRowAndNamesTheTagInFiltersApplied()
    {
        await CpuTagAsync("Hot", _idA, "0x00000000000000AA");
        await SnapshotAsync(_idA, 51, "0x00000000000000AA", "PAGEIOLATCH_SH");
        await SnapshotAsync(_idA, 52, "0x00000000000000AA", "LCK_M_S");
        await SnapshotAsync(_idA, 53, "0x00000000000000BB", "PAGEIOLATCH_SH");

        var root = Parse(await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerA, 1, wait_type: "PAGEIOLATCH_SH", tag: "hot"));

        var rows = root.GetProperty("queries").EnumerateArray().ToList();
        Assert.Equal([51], rows.Select(r => r.GetProperty("session_id").GetInt32()).ToArray());
        Assert.Equal("Hot", rows[0].GetProperty("tag").GetString());
        Assert.Equal("hot", root.GetProperty("filters_applied").GetProperty("tag").GetString());
    }

    [Fact]
    public async Task ActiveQueries_TagOnlyThatMatchesNothing_SaysWhichTagInTheEmptyMessage()
    {
        await CpuTagAsync("Hot", _idA, "0x00000000000000AA");
        await SnapshotAsync(_idA, 53, "0x00000000000000BB", null);

        var root = Parse(await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerA, 1, tag: "Hot"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("(query tag 'Hot')", message, StringComparison.Ordinal);
        Assert.DoesNotContain("matched .", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TopProcedures_WithATag_CarriesTheTagColumnAndTheTagFilterOfAnObjectKey()
    {
        var tag = await _service.CreateQueryTagAsync("Procs", null);
        await MapAsync(_idA, "object", "db1.dbo.usp_Tagged", tag);
        foreach (var name in new[] { "usp_Tagged", "usp_Plain" })
        {
            await ExecAsync(
                "INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads) VALUES ($1, $2, $3, 'x', 'db1', 'dbo', $4, $2, 1, 1000, 1000, 5)",
                _nextId++, Ago(30), _idA, name);
        }

        var json = await McpQueryTools.GetTopProceduresByCpu(_service, _serverManager, ServerA, 24, 10, tag: "procs");
        var root = Parse(json);

        Assert.True(root.TryGetProperty("tag_filter", out _), json.Length > 300 ? json[..300] : json);
        Assert.Equal("procs", root.GetProperty("tag_filter").GetString());
        var rows = root.GetProperty("procedures").EnumerateArray().ToList();
        var only = Assert.Single(rows);
        Assert.EndsWith("usp_Tagged", only.GetProperty("full_name").GetString(), StringComparison.Ordinal);
        Assert.Equal("Procs", only.GetProperty("tag").GetString());
    }

    [Fact]
    public async Task TopProcedures_WithoutATag_HasNoTagFilterMember()
    {
        await ExecAsync(
            "INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads) VALUES ($1, $2, $3, 'x', 'db1', 'dbo', 'usp_Plain', $2, 1, 1000, 1000, 5)",
            _nextId++, Ago(30), _idA);

        var root = Parse(await McpQueryTools.GetTopProceduresByCpu(_service, _serverManager, ServerA, 24, 10));

        Assert.False(root.TryGetProperty("tag_filter", out _));
    }

    [Fact]
    public async Task LongQueryCompletions_WithATag_MatchesAHashStoredAsADecimal()
    {
        await CpuTagAsync("Dec", _idA, "0x00000000000000AA");
        foreach (var hash in new[] { "170", "187" })
        {
            await ExecAsync(
                "INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, query_hash, statement_text, duration_microseconds) VALUES ($1, $2, $3, 'x', $2, 'rpc_completed', 'db1', $4, 'select 1', 5000000)",
                _nextId++, Ago(30), _idA, hash);
        }

        var root = Parse(await McpLongQueryTools.GetLongQueryCompletions(_service, _serverManager, ServerA, 24, 10, tag: "Dec"));

        Assert.Equal("Dec", root.GetProperty("tag_filter").GetString());
        var row = Assert.Single(root.GetProperty("completions").EnumerateArray().ToList());
        Assert.Equal("Dec", row.GetProperty("tag").GetString());
    }

    [Fact]
    public async Task LongQueryCompletions_WithATagAndNoRows_StillSaysTheCollectorIsOptIn_AndNamesTheTag()
    {
        await CpuTagAsync("Dec", _idA, "0x00000000000000AA");

        var root = Parse(await McpLongQueryTools.GetLongQueryCompletions(_service, _serverManager, ServerA, 24, 10, tag: "Dec"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("for query tag 'Dec'", message, StringComparison.Ordinal);
        Assert.Contains("collector is opt-in (default OFF)", message, StringComparison.Ordinal);
        Assert.DoesNotContain("carry query tag", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ResourceUse_OnAnAzureSqlDatabaseServer_AnsweringNoRows_SaysNotCollectedForTheSectionsItsCollectorsSkip()
    {
        await ExecAsync(
            "INSERT INTO server_properties (server_id, collection_time, engine_edition) VALUES ($1, $2, 5)", _idA, Ago(5));

        var root = Parse(await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, ServerA, 24));

        var sections = root.GetProperty("sections").EnumerateArray().ToDictionary(s => s.GetProperty("section").GetString()!);
        Assert.Equal("not_collected", sections["blocking_snapshots"].GetProperty("status").GetString());
        Assert.Equal("empty", sections["plan_cache"].GetProperty("status").GetString());
    }

    [Fact]
    public async Task ResourceUse_DeadlockSection_UnderATag_FleetTotalCountsOnlyTheGraphsWithAProcessUnderTheTag()
    {
        var a = await _service.CreateQueryTagAsync("A", null);
        foreach (var (server, apps) in new[] { (_idA, new[] { "appA", "other" }), (_idB, new[] { "other", "other2" }) })
        {
            await ExecAsync(
                "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1, $2, $3, 'x', $2, $4, 'db1')",
                _nextId++, Ago(15), server,
                "<deadlock><process-list>" + string.Concat(apps.Select((x, i) => $"<process id=\"p{i}\" clientapp=\"{x}\" loginname=\"lg\" hostname=\"h\" currentdbname=\"db1\" />")) + "</process-list></deadlock>");
        }

        await MapAsync(_idA, "caller", QueryTagRollup.CallerKey("appA", "lg", "h", "db1"), a);
        await MapAsync(_idB, "caller", QueryTagRollup.CallerKey("appA", "lg", "h", "db1"), a);

        var root = Parse(await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, null, 24, "A"));
        var deadlocks = root.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == "deadlocks");

        Assert.Equal(1.0, deadlocks.GetProperty("fleet_total").GetProperty("deadlocks").GetDouble());
        Assert.Equal(1.0, deadlocks.GetProperty("fleet_total").GetProperty("processes").GetDouble());
    }

    [Fact]
    public async Task QueryStoreTop_WithATag_KeepsOnlyTheTaggedQueryAndNamesTheFilter()
    {
        await CpuTagAsync("Qs", _idA, "0x00000000000000AA");
        var interval = 1L;
        foreach (var (hash, queryId) in new[] { ("0x00000000000000AA", 7L), ("0x00000000000000BB", 8L) })
        {
            await ExecAsync(
                "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, execution_type_desc, execution_count, avg_duration_us, last_execution_time, runtime_stats_interval_id, interval_start_time_utc) VALUES ($1, $2, $3, 'x', 'db1', $4, $4, $5, 'Regular', 3, 1000, $2, $6, $2)",
                _nextId++, Ago(30), _idA, queryId, hash, interval++);
        }

        var root = Parse(await McpQueryTools.GetQueryStoreTop(_service, _serverManager, ServerA, 24, 10, tag: "Qs"));

        Assert.Equal("Qs", root.GetProperty("tag_filter").GetString());
        var row = Assert.Single(root.GetProperty("queries").EnumerateArray().ToList());
        Assert.Equal(7, row.GetProperty("query_id").GetInt64());
        Assert.Equal("Qs", row.GetProperty("tag").GetString());
    }

    [Fact]
    public async Task TopQueriesByCpu_WithATag_NamesItInFilterApplied()
    {
        await CpuTagAsync("Cpu", _idA, "0x00000000000000AA");
        await PlanAsync(_idA, "0x00000000000000AA", 2_000_000);

        var root = Parse(await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerA, 24, 10, tag: "Cpu"));

        Assert.Contains("query tag 'Cpu'", root.GetProperty("filter_applied").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ApplyingATagToAStatementWithNoToken_Throws_AndWithoutATagLeavesTheStatementAlone()
    {
        await CpuTagAsync("Tok", _idA, "0x00000000000000AA");
        var tagged = await _service.GetQueryTagReadScopeAsync(_idA, "Tok");
        var plain = await _service.GetQueryTagReadScopeAsync(_idA, null);

        var ex = Assert.Throws<InvalidOperationException>(() => tagged.Apply("SELECT 1 FROM v_query_stats", 7));

        Assert.Contains("SELECT 1 FROM v_query_stats", ex.Message, StringComparison.Ordinal);
        Assert.Equal("SELECT 1 FROM v_query_stats", plain.Apply("SELECT 1 FROM v_query_stats", 7));
    }
}
