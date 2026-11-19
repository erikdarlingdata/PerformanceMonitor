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

namespace Lite.Tests;

/// <summary>
/// #9055: a page latch wait in Lite's get_active_queries says what the page is, from the stored data alone, and Lite now returns
/// <c>wait_resource</c> the way Darling does. One capture holds page latch rows (each with the plan it ran under), and two daily index
/// snapshots rank the same indexes differently: the newer snapshot must win, the match kinds must follow the plan, and a row that is not a
/// page latch wait carries no <c>page_latch</c> object. Own class, own database.
/// </summary>
public sealed class LiteActiveQueriesPageLatchTests : IDisposable
{
    private const string ServerName = "PageLatchServer";

    private const string RelOpAttributes = "EstimateRows=\"1\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.003\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\"";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly DateTime _now = DateTime.UtcNow;
    private long _nextId = 1;

    public LiteActiveQueriesPageLatchTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "PageLatchTool_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "tool.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(_tempDir);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static string Plan(string relOps, string statementText = "x") =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.5\" Build=\"16.0.1000.6\"><BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + statementText + "\" StatementId=\"1\" StatementType=\"SELECT\"><QueryPlan CachedPlanSize=\"16\">" + relOps + "</QueryPlan></StmtSimple>"
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static string Seek(string database, string table, string index, string kind) =>
        $"<RelOp NodeId=\"0\" PhysicalOp=\"Index Seek\" LogicalOp=\"Index Seek\" {RelOpAttributes}><OutputList/>"
        + "<IndexScan Ordered=\"1\" ScanDirection=\"FORWARD\" ForcedIndex=\"0\" ForceSeek=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">"
        + $"<Object Database=\"[{database}]\" Schema=\"[dbo]\" Table=\"[{table}]\" Index=\"[{index}]\" IndexKind=\"{kind}\" Storage=\"RowStore\"/></IndexScan></RelOp>";

    private static readonly string SeekPlan = Plan(Seek("Shop", "Orders", "IX_Orders_Date", "NonClustered"));

    private static readonly string OtherDatabasePlan = Plan(Seek("Other", "OtherTable", "PK_OtherTable", "Clustered"));

    /* A clustered index insert: the first Object is the clustered index, the second the nonclustered index it maintains. */
    private static readonly string InsertPlan = Plan(
        $"<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Insert\" LogicalOp=\"Insert\" {RelOpAttributes}><OutputList/>"
        + "<Update DMLRequestSort=\"0\">"
        + "<Object Database=\"[Shop]\" Schema=\"[dbo]\" Table=\"[Orders]\" Index=\"[PK_Orders]\" IndexKind=\"Clustered\" Storage=\"RowStore\"/>"
        + "<Object Database=\"[Shop]\" Schema=\"[dbo]\" Table=\"[Orders]\" Index=\"[IX_Orders_Date]\" IndexKind=\"NonClustered\" Storage=\"RowStore\"/>"
        + "</Update></RelOp>");

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
        {
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = v ?? DBNull.Value });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private Task RowAsync(DateTime at, int session, string? waitType, string? waitResource, string? plan, int? requestId) => ExecAsync(
        "INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, wait_type, wait_time_ms, wait_resource, query_plan, request_id) VALUES ($1, $2, $3, 'x', $4, 'Shop', 'SELECT 1', 'suspended', $5, $6, $7, $8, $9)",
        _nextId++, at, _serverId, session, waitType, waitType == null ? 0L : 250L, waitResource, plan, requestId);

    private Task IndexAsync(DateTime at, string index, string table, long pageLatchMs, long pageLatchCount, long ioMs, long ioCount, string database = "Shop") => ExecAsync(
        "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, page_latch_wait_count, page_latch_wait_in_ms, page_io_latch_wait_count, page_io_latch_wait_in_ms) VALUES ($1, $2, $3, 'x', $4, $13, 5, 'dbo', $5, $6, $7, $8, 'CLUSTERED', 10, 9, 1000, $9, $10, $11, $12)",
        _nextId++, at, _serverId, at.AddDays(-30), 100 + table.Length, table, index.Length, index, pageLatchCount, pageLatchMs, ioCount, ioMs, database);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public async Task PageLatchRows_NameThePageKindAndTheIndexesFromTheNewestSnapshot()
    {
        var captured = _now.AddMinutes(-5);
        var older = _now.AddHours(-30);
        var newer = _now.AddHours(-6);

        /* The older snapshot ranks Customers first, the newer one ranks the Orders index first, so a reader on the wrong snapshot is caught. */
        await IndexAsync(older, "PK_Customers", "Customers", 9000, 900, 0, 0);
        await IndexAsync(older, "IX_Orders_Date", "Orders", 10, 1, 0, 0);
        await IndexAsync(newer, "PK_Customers", "Customers", 50, 5, 500, 50);
        await IndexAsync(newer, "IX_Orders_Date", "Orders", 400, 40, 70, 7);
        await IndexAsync(newer, "PK_Orders", "Orders", 100, 10, 900, 90);
        await IndexAsync(newer, "PK_Items", "Items", 20, 2, 0, 0);
        await IndexAsync(newer, "PK_Quiet", "Quiet", 0, 0, 0, 0);

        await RowAsync(captured, 51, "PAGELATCH_SH", "5:1:4096", SeekPlan, 0);
        await RowAsync(captured, 52, "PAGELATCH_EX", "5:1:4096", InsertPlan, 0);
        await RowAsync(captured, 53, "PAGEIOLATCH_SH", "5:1:4097", null, null);
        await RowAsync(captured, 54, "PAGELATCH_UP", "5:1:2", null, 0);
        await RowAsync(captured, 55, "PAGELATCH_EX", "2:1:4096", SeekPlan, 0);
        await RowAsync(captured, 56, "LCK_M_S", "KEY: 5:72057594043432960 (a1b2c3d4e5f6)", SeekPlan, 0);
        await RowAsync(captured, 57, "PAGELATCH_SH", "5:1:4096", "<not a plan", 0);
        await RowAsync(captured, 58, "PAGELATCH_SH", "5:1:4096", OtherDatabasePlan, 0);

        var root = Parse(await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerName, 1, limit: 50));
        var rows = root.GetProperty("queries").EnumerateArray().ToDictionary(r => r.GetProperty("session_id").GetInt32());

        Assert.Equal("5:1:4096", rows[51].GetProperty("wait_resource").GetString());
        Assert.Equal("KEY: 5:72057594043432960 (a1b2c3d4e5f6)", rows[56].GetProperty("wait_resource").GetString());

        /* One index in the plan: exact, with the NEWER snapshot's numbers. */
        var exact = rows[51].GetProperty("page_latch");
        Assert.Equal("data or index page", exact.GetProperty("page_kind").GetString());
        Assert.Equal("exact", exact.GetProperty("match").GetString());
        Assert.True(Math.Abs((exact.GetProperty("index_snapshot_time").GetDateTime().ToUniversalTime() - newer).TotalSeconds) < 1);
        var exactIndex = Assert.Single(exact.GetProperty("indexes").EnumerateArray());
        Assert.Equal("IX_Orders_Date", exactIndex.GetProperty("index_name").GetString());
        Assert.Equal(400, exactIndex.GetProperty("latch_wait_ms").GetInt64());
        Assert.Equal(40, exactIndex.GetProperty("latch_wait_count").GetInt64());

        /* A clustered insert that maintains an index: plan, both indexes, highest page latch wait first. */
        var plan = rows[52].GetProperty("page_latch");
        Assert.Equal("plan", plan.GetProperty("match").GetString());
        Assert.Equal(["IX_Orders_Date", "PK_Orders"], plan.GetProperty("indexes").EnumerateArray().Select(i => i.GetProperty("index_name").GetString()).ToArray());

        /* No plan and a NULL request_id: inferred, the three top indexes by PAGE I/O latch waits (a PAGEIOLATCH row), newest snapshot. */
        var inferred = rows[53].GetProperty("page_latch");
        Assert.Equal("inferred", inferred.GetProperty("match").GetString());
        Assert.Equal("page_io_latch", inferred.GetProperty("latch_kind").GetString());
        Assert.Equal(["PK_Orders", "PK_Customers", "IX_Orders_Date"], inferred.GetProperty("indexes").EnumerateArray().Select(i => i.GetProperty("index_name").GetString()).ToArray());
        Assert.Equal([900L, 500L, 70L], inferred.GetProperty("indexes").EnumerateArray().Select(i => i.GetProperty("latch_wait_ms").GetInt64()).ToArray());

        /* An allocation page: the kind, no index list. */
        var gam = rows[54].GetProperty("page_latch");
        Assert.Equal("GAM", gam.GetProperty("page_kind").GetString());
        Assert.False(gam.TryGetProperty("indexes", out _));

        /* tempdb data page: a note, no index list. */
        var tempdb = rows[55].GetProperty("page_latch");
        Assert.False(tempdb.TryGetProperty("indexes", out _));
        Assert.Contains("tempdb", tempdb.GetProperty("note").GetString(), StringComparison.Ordinal);

        /* A lock wait is not a page latch wait: its wait_resource is not read as a page. */
        Assert.False(rows[56].TryGetProperty("page_latch", out _));

        /* An unreadable plan and a plan with only another database's objects both fall back to the newest snapshot's ranking. */
        foreach (var session in new[] { 57, 58 })
        {
            var fallback = rows[session].GetProperty("page_latch");
            Assert.Equal("inferred", fallback.GetProperty("match").GetString());
            Assert.Equal(["IX_Orders_Date", "PK_Orders", "PK_Customers"], fallback.GetProperty("indexes").EnumerateArray().Select(i => i.GetProperty("index_name").GetString()).ToArray());
        }
    }

    [Fact]
    public async Task WithNoPageLatchRow_NoPageLatchObjectIsAdded_AndWaitResourceIsStillReturned()
    {
        var captured = _now.AddMinutes(-5);
        await RowAsync(captured, 61, "LCK_M_S", "KEY: 5:72057594043432960 (a1b2c3d4e5f6)", null, 0);
        await RowAsync(captured, 62, null, null, null, 0);

        var json = await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerName, 1, limit: 50);

        Assert.DoesNotContain("page_latch", json, StringComparison.Ordinal);
        var rows = Parse(json).GetProperty("queries").EnumerateArray().ToDictionary(r => r.GetProperty("session_id").GetInt32());
        Assert.Equal("KEY: 5:72057594043432960 (a1b2c3d4e5f6)", rows[61].GetProperty("wait_resource").GetString());
        Assert.True(!rows[62].TryGetProperty("wait_resource", out var none) || none.ValueKind == JsonValueKind.Null);
    }

    [Fact]
    public async Task PageLatchRow_WithNoIndexSnapshotAtAll_StillNamesThePageKind()
    {
        await RowAsync(_now.AddMinutes(-5), 71, "PAGELATCH_EX", "5:1:4096", SeekPlan, 0);

        var root = Parse(await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerName, 1, limit: 50));

        var latch = root.GetProperty("queries")[0].GetProperty("page_latch");
        Assert.Equal("data or index page", latch.GetProperty("page_kind").GetString());
        Assert.False(latch.TryGetProperty("indexes", out _));
    }

    [Fact]
    public async Task PageLatchRow_PlanIsJudgedBeforeItIsParsed_SoAValueTheFilterWithholdsNamesNoIndex()
    {
        /* The only way the judge can change which index a plan names is a value it withholds: the plan's database attribute here is a
           statement the filter names, and so is the snapshot's database name, so an UNJUDGED plan would match it and be exact. The judged
           plan names a placeholder instead, matches nothing, and the row gets the snapshot's ranking like a row with no plan. */
        var named = "CREATE LOGIN leaker WITH PASSWORD = 'Hunter2Secret9055';";
        Assert.True(PerformanceMonitor.Common.SensitiveStatements.Names(named));
        var captured = _now.AddMinutes(-5);
        var snapshot = _now.AddHours(-6);
        await IndexAsync(snapshot, "PK_Customers", "Customers", 900, 90, 0, 0, named);
        await IndexAsync(snapshot, "IX_Orders_Date", "Orders", 400, 40, 0, 0, named);
        var plan = Plan(Seek(System.Security.SecurityElement.Escape(named), "Orders", "IX_Orders_Date", "NonClustered"));
        await RowAsync(captured, 81, "PAGELATCH_SH", "5:1:4096", plan, 0);

        var root = Parse(await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerName, 1, limit: 50));

        var latch = root.GetProperty("queries")[0].GetProperty("page_latch");
        Assert.Equal("inferred", latch.GetProperty("match").GetString());
        Assert.Equal(["PK_Customers", "IX_Orders_Date"], latch.GetProperty("indexes").EnumerateArray().Select(i => i.GetProperty("index_name").GetString()).ToArray());
    }
}
