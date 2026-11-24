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
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's <c>get_object_usage</c> (#8784) against the shared DuckDB fixture: a table read before a restart and again after it counts both
/// sides of the reset; a table with no reads in the window is a candidate; the answering-node column is written and read; a re-collected open
/// Query Store interval is counted once; ad hoc statements are not modules; and a database with nothing stored answers <c>empty</c>.
/// </summary>
public sealed class ObjectUsageToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "ObjectUsageSrv";
    private const string Database = "Sales";

    private static readonly DateTime Now = new(2026, 11, 14, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime StartA = new(2026, 7, 1, 3, 0, 0);
    private static readonly DateTime StartB = new(2026, 10, 1, 3, 0, 0);

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = -1;

    public ObjectUsageToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-objectusage-" + Guid.NewGuid().ToString("N"));
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

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task InsertIndexAsync(DateTime capture, DateTime start, string node, int objectId, string table, long seeks, long updates)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO index_object_stats
            (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
             schema_name, object_id, table_name, index_id, index_name, index_type_desc, user_seeks, user_scans, user_lookups, user_updates,
             is_indexed_view, answering_server_name, is_memory_optimized)
            VALUES ($1,$2,$3,$4,$5,$6,7,'dbo',$7,$8,1,NULL,'CLUSTERED',$9,0,0,$10,false,$11,false)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--); P(capture); P(_serverId); P(ServerName); P(start); P(Database); P(objectId); P(table); P(seeks); P(updates); P(node);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertQueryStoreAsync(DateTime collected, string module, long queryId, long intervalId, long executions, DateTime intervalStart)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_store_stats
            (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, module_name,
             execution_count, last_execution_time, runtime_stats_interval_id, interval_start_time_utc, first_execution_time)
            VALUES ($1,$2,$3,$4,$5,$6,1,'Regular',$7,$8,$9,$10,$11,$11)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--); P(collected); P(_serverId); P(ServerName); P(Database); P(queryId); P(module); P(executions); P(collected); P(intervalId); P(intervalStart);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task InsertStatementAsync(
        DateTime collected, string database, long queryId, long intervalId, long executions, DateTime intervalStart, string? text)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_store_stats
            (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, module_name,
             execution_count, last_execution_time, runtime_stats_interval_id, interval_start_time_utc, first_execution_time, query_text)
            VALUES ($1,$2,$3,$4,$5,$6,1,'Regular','Adhoc',$7,$8,$9,$10,$10,$11)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        P(_nextId--); P(collected); P(_serverId); P(ServerName); P(database); P(queryId); P(executions); P(collected); P(intervalId); P(intervalStart); P(text);
        await cmd.ExecuteNonQueryAsync();
    }

    private static DateTime Day(int daysAgo) => DateTime.SpecifyKind(Now.AddDays(-daysAgo), DateTimeKind.Unspecified);

    private async Task<JsonElement> CallAsync(string? database = Database, int daysBack = 60) =>
        JsonDocument.Parse(await McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, database, daysBack, 100, 0, Now)).RootElement;

    private static JsonElement Obj(JsonElement root, string name) =>
        root.GetProperty("objects").EnumerateArray().Single(o => o.GetProperty("object_name").GetString() == name);

    [Fact]
    public async Task ReadsAcrossARestart_CountBothSides()
    {
        /* Orders: 1,000 at the baseline, 1,100 the next day (100 reads), then the instance restarts and it reads 30. A latest-snapshot read says
           30; a newest-minus-oldest read says -970. Both sides of the reset are 130. */
        await InsertIndexAsync(Day(62), StartA, "NODE1", 100, "Orders", 1_000, 10);
        await InsertIndexAsync(Day(62), StartA, "NODE1", 200, "Archive2019", 0, 0);
        await InsertIndexAsync(Day(30), StartA, "NODE1", 100, "Orders", 1_100, 12);
        await InsertIndexAsync(Day(30), StartA, "NODE1", 200, "Archive2019", 0, 0);
        await InsertIndexAsync(Day(1), StartB, "NODE1", 100, "Orders", 30, 1);
        await InsertIndexAsync(Day(1), StartB, "NODE1", 200, "Archive2019", 0, 0);

        var root = await CallAsync(daysBack: 60);

        var orders = Obj(root, "Orders");
        Assert.Equal(130, orders.GetProperty("reads").GetInt64());
        Assert.Equal(3, orders.GetProperty("writes").GetInt64());
        Assert.Equal("Read", orders.GetProperty("classification").GetString());
        Assert.Equal("Not used in window", Obj(root, "Archive2019").GetProperty("classification").GetString());

        var tables = root.GetProperty("coverage")[0].GetProperty("tables");
        Assert.Equal(1, tables.GetProperty("resets_crossed").GetInt32());
        Assert.Equal("NODE1", tables.GetProperty("answering_servers")[0].GetString());

        /* The baseline is the newest snapshot at or before the window start (62 days back, inside the reach), so the snapshots span 61 days.
           The restart was found 29 days after the snapshot before it, and use in that time went with the counters: those days are reported
           as blind and left out of the covered days, which leaves this window short. */
        Assert.Equal(29 * 24, tables.GetProperty("blind_hours").GetDouble());
        Assert.Equal(32, tables.GetProperty("covered_days").GetDouble());
        Assert.True(tables.GetProperty("short_of_window").GetBoolean());
        Assert.Equal(3, tables.GetProperty("snapshots").GetInt32());
    }

    [Fact]
    public async Task ABaselineOutOfReach_LeavesTheWindowShort_AndSaysSo()
    {
        /* 70 days back is more than the reach before a 60-day window, so the first snapshot inside it (30 days back) is the baseline. */
        await InsertIndexAsync(Day(70), StartA, "NODE1", 100, "Orders", 1_000, 0);
        await InsertIndexAsync(Day(30), StartA, "NODE1", 100, "Orders", 1_100, 0);
        await InsertIndexAsync(Day(1), StartA, "NODE1", 100, "Orders", 1_150, 0);

        var root = await CallAsync(daysBack: 60);

        Assert.Equal(50, Obj(root, "Orders").GetProperty("reads").GetInt64());
        Assert.True(root.GetProperty("coverage")[0].GetProperty("tables").GetProperty("short_of_window").GetBoolean());
    }

    [Fact]
    public async Task ANewAnsweringNode_UpSinceBeforeTheWindow_IsMeasuredFromItsOwnFirstSnapshot()
    {
        /* NODE2 has been up for months, so its 600 reads can predate the window: they are not counted, and it is measured from there. A
           return to NODE1 is compared with NODE1's own last snapshot. */
        await InsertIndexAsync(Day(20), StartA, "NODE1", 100, "Orders", 500, 0);
        await InsertIndexAsync(Day(10), StartA, "NODE2", 100, "Orders", 600, 0);
        await InsertIndexAsync(Day(5), StartA, "NODE1", 100, "Orders", 520, 0);
        await InsertIndexAsync(Day(1), StartA, "NODE2", 100, "Orders", 650, 0);

        var root = await CallAsync();

        Assert.Equal(70, Obj(root, "Orders").GetProperty("reads").GetInt64());
        var tables = root.GetProperty("coverage")[0].GetProperty("tables");
        Assert.Equal(1, tables.GetProperty("resets_crossed").GetInt32());
        Assert.Equal("NodeChanged, NodeBaseline", tables.GetProperty("resets")[0].GetProperty("reasons").GetString());
        Assert.Equal(2, tables.GetProperty("answering_servers").GetArrayLength());
    }

    [Fact]
    public async Task ANewAnsweringNode_StartedInsideTheWindow_CountsItsWholeValue()
    {
        await InsertIndexAsync(Day(20), StartA, "NODE1", 100, "Orders", 500, 0);
        await InsertIndexAsync(Day(10), DateTime.SpecifyKind(Now.AddDays(-12), DateTimeKind.Unspecified), "NODE2", 100, "Orders", 600, 0);

        var root = await CallAsync();

        Assert.Equal(600, Obj(root, "Orders").GetProperty("reads").GetInt64());
        Assert.Equal("NodeChanged", root.GetProperty("coverage")[0].GetProperty("tables").GetProperty("resets")[0].GetProperty("reasons").GetString());
    }

    [Fact]
    public async Task AMemoryOptimizedTable_IsNotTracked()
    {
        await InsertIndexAsync(Day(20), StartA, "NODE1", 100, "Orders", 500, 0);
        await InsertIndexAsync(Day(1), StartA, "NODE1", 100, "Orders", 500, 0);
        using (var readLock = _duckDb.AcquireReadLock())
        {
            var conn = await SeedConnectionAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE index_object_stats SET is_memory_optimized = true WHERE table_name = 'Orders'";
            await cmd.ExecuteNonQueryAsync();
        }

        var root = await CallAsync();

        Assert.Equal("Not tracked", Obj(root, "Orders").GetProperty("classification").GetString());
        Assert.Contains("memory-optimized", Obj(root, "Orders").GetProperty("caveat").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ATableStoredBeforeTheMemoryFlagWasCollected_IsNotCalledUnused()
    {
        /* Rows from before the upgrade hold NULL in is_memory_optimized. A memory-optimized table reads zero whatever its use, so an
           unmarked table with no use is not a candidate until a collection marks it. */
        await InsertIndexAsync(Day(20), StartA, "NODE1", 100, "Orders", 500, 0);
        await InsertIndexAsync(Day(1), StartA, "NODE1", 100, "Orders", 500, 0);
        using (var readLock = _duckDb.AcquireReadLock())
        {
            var conn = await SeedConnectionAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "UPDATE index_object_stats SET is_memory_optimized = NULL WHERE table_name = 'Orders'";
            await cmd.ExecuteNonQueryAsync();
        }

        var root = await CallAsync();

        Assert.Equal("Not tracked", Obj(root, "Orders").GetProperty("classification").GetString());
        Assert.Contains("before memory-optimized tables were marked", Obj(root, "Orders").GetProperty("caveat").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PagingWithTheCache_ReadsTheStoreOnce_AndKeepsOneWindow()
    {
        await InsertIndexAsync(Day(20), StartA, "NODE1", 100, "Orders", 500, 0);
        await InsertIndexAsync(Day(20), StartA, "NODE1", 200, "Customers", 10, 0);
        await InsertIndexAsync(Day(1), StartA, "NODE1", 100, "Orders", 520, 0);
        await InsertIndexAsync(Day(1), StartA, "NODE1", 200, "Customers", 10, 0);

        var cache = new ObjectUsageEvidenceCache();
        var first = JsonDocument.Parse(await McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, Database, 60, 1, 0, Now, cache)).RootElement;
        Assert.Equal(1, first.GetProperty("next_offset").GetInt32());

        /* A collection lands between the two pages. The second page is the same reply's next rows, not a different window's. */
        await InsertIndexAsync(DateTime.SpecifyKind(Now.AddHours(-1), DateTimeKind.Unspecified), StartA, "NODE1", 300, "Arrived", 0, 0);
        var second = JsonDocument.Parse(await McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, Database, 60, 1, 1, Now.AddMinutes(2), cache)).RootElement;

        Assert.Equal(first.GetProperty("window_start_utc").GetString(), second.GetProperty("window_start_utc").GetString());
        Assert.Equal(2, second.GetProperty("summary").GetProperty("objects").GetInt32());
        Assert.NotEqual(first.GetProperty("objects")[0].GetProperty("object_name").GetString(), second.GetProperty("objects")[0].GetProperty("object_name").GetString());

        /* Without the cache the store is read again. */
        var uncached = JsonDocument.Parse(await McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, Database, 60, 100, 0, Now)).RootElement;
        Assert.Equal(3, uncached.GetProperty("summary").GetProperty("objects").GetInt32());
    }

    [Fact]
    public async Task ExecutionCoverage_CountsTheDaysWithQueryStoreData()
    {
        await InsertIndexAsync(Day(5), StartA, "NODE1", 100, "Orders", 0, 0);
        await InsertQueryStoreAsync(Day(30).AddMinutes(10), "dbo.usp_Daily", 1, 501, 4, Day(30));
        await InsertQueryStoreAsync(Day(2).AddMinutes(10), "Adhoc", 2, 900, 4, Day(2));

        var root = await CallAsync();

        Assert.Equal(2, root.GetProperty("coverage")[0].GetProperty("executions").GetProperty("days_with_data").GetInt32());
    }

    [Fact]
    public async Task ModuleExecutions_CountEachIntervalOnce_AndLeaveOutAdhoc()
    {
        await InsertIndexAsync(Day(5), StartA, "NODE1", 100, "Orders", 0, 0);
        var interval = Day(3);

        /* The open interval re-collected three times as it grew: only its newest snapshot (12) counts. */
        await InsertQueryStoreAsync(interval.AddMinutes(10), "dbo.usp_Daily", 1, 501, 4, interval);
        await InsertQueryStoreAsync(interval.AddMinutes(20), "dbo.usp_Daily", 1, 501, 9, interval);
        await InsertQueryStoreAsync(interval.AddMinutes(70), "dbo.usp_Daily", 1, 501, 12, interval);
        await InsertQueryStoreAsync(interval.AddHours(25), "dbo.usp_Daily", 1, 525, 5, interval.AddHours(24));
        await InsertQueryStoreAsync(interval.AddMinutes(30), "Adhoc", 2, 501, 1_000, interval);

        var root = await CallAsync();

        var daily = Obj(root, "usp_Daily");
        Assert.Equal(17, daily.GetProperty("executions").GetInt64());
        Assert.Equal("Executed", daily.GetProperty("classification").GetString());
        Assert.DoesNotContain(root.GetProperty("objects").EnumerateArray(), o => o.GetProperty("object_name").GetString() == "Adhoc");
        Assert.Contains("NOT listed", root.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Views_AreReadFromTheStoredStatementText_IncludingFromAnotherDatabase()
    {
        /* #8784 part 2. */
        await InsertIndexAsync(Day(5), StartA, "NODE1", 100, "Orders", 3, 0);
        var interval = Day(3);

        /* vReport is named by one statement whose open interval was re-collected (only its newest snapshot, 6, counts) and by a second one. */
        await InsertStatementAsync(interval.AddMinutes(10), Database, 1, 601, 2, interval, "(@p1 int)SELECT * FROM dbo.vReport JOIN dbo.Orders ON 1 = 1 WHERE a = @p1");
        await InsertStatementAsync(interval.AddMinutes(70), Database, 1, 601, 6, interval, "(@p1 int)SELECT * FROM dbo.vReport JOIN dbo.Orders ON 1 = 1 WHERE a = @p1");
        await InsertStatementAsync(interval.AddMinutes(20), Database, 2, 601, 4, interval, "WITH vCte AS (SELECT 1 AS a) SELECT * FROM vCte, dbo.vReport");

        /* Another database reads vCross by a three-part name, and its own vElse by a plain one. */
        await InsertStatementAsync(interval.AddMinutes(30), "Reports", 3, 601, 5, interval, "SELECT * FROM Sales.dbo.vCross");
        await InsertStatementAsync(interval.AddMinutes(40), "Reports", 4, 601, 5, interval, "SELECT * FROM dbo.vElse /* Sales */");

        /* A statement that does not parse is counted, and with no object list it is not searched. */
        await InsertStatementAsync(interval.AddMinutes(50), Database, 5, 601, 1, interval, "SELEC * FROM dbo.vBroken");

        var root = await CallAsync();

        var report = Obj(root, "vReport");
        Assert.Equal("VIEW", report.GetProperty("object_type").GetString());
        Assert.Equal(ObjectUsageReport.Referenced, report.GetProperty("classification").GetString());
        Assert.Equal(10, report.GetProperty("referencing_executions").GetInt64());
        Assert.Equal(ObjectUsageReport.Referenced, Obj(root, "vCross").GetProperty("classification").GetString());
        Assert.Equal("USER_TABLE", Obj(root, "Orders").GetProperty("object_type").GetString());

        var names = root.GetProperty("objects").EnumerateArray().Select(o => o.GetProperty("object_name").GetString()).ToArray();
        Assert.DoesNotContain("vCte", names);
        Assert.DoesNotContain("vElse", names);
        Assert.DoesNotContain("vBroken", names);

        var views = root.GetProperty("coverage")[0].GetProperty("views");
        Assert.Equal(3, views.GetProperty("statements").GetInt64());
        Assert.Equal(1, views.GetProperty("statements_unparsed").GetInt64());
    }

    [Fact]
    public async Task NothingStored_IsEmpty_NotAnAllClear()
    {
        await InsertIndexAsync(Day(5), StartA, "NODE1", 100, "Orders", 0, 0);

        var root = await CallAsync(database: "NoSuchDatabase");

        Assert.Equal("empty", root.GetProperty("status").GetString());
    }

    [Fact]
    public async Task DatabaseName_IsRequired_AndTheWindowIsCappedAtLitesRetention()
    {
        var noDatabase = await CallAsync(database: null);
        Assert.Contains("database_name", noDatabase.ToString(), StringComparison.Ordinal);

        var tooLong = await CallAsync(daysBack: 365);
        Assert.Contains("days_back", tooLong.ToString(), StringComparison.Ordinal);
    }

    private async Task InsertHealthAsync(DateTime capture, string database, string? mode)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, query_capture_mode)
            VALUES ($1,$2,$3,$4,$5,$6)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--); P(capture); P(_serverId); P(ServerName); P(database); P(mode);
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task TheExecutionRead_CarriesTheCaptureMode_ANonAllModeInTheWindowWins_AndOneBeforeTheWindowDoesNot()
    {
        /* Review of #8855, the same rule as Darling's read: AUTO inside the window wins over a later ALL, and a mode before the window is
           not looked at. */
        await InsertHealthAsync(Day(40), Database, "AUTO");
        await InsertHealthAsync(Day(2), Database, "ALL");
        await InsertHealthAsync(Day(1), Database, null);
        await InsertHealthAsync(Day(200), "Steady", "CUSTOM");
        await InsertHealthAsync(Day(10), "Steady", "ALL");

        var windowStart = Day(60);
        Assert.Equal("AUTO", (await _dataService.GetObjectExecutionsAsync(_serverId, Database, windowStart)).QueryCaptureMode);
        Assert.Equal("ALL", (await _dataService.GetObjectExecutionsAsync(_serverId, "Steady", windowStart)).QueryCaptureMode);
        Assert.Null((await _dataService.GetObjectExecutionsAsync(_serverId, "NoHealth", windowStart)).QueryCaptureMode);
    }

    [Fact]
    public async Task TheReplysExecutionCoverage_NamesTheCaptureMode()
    {
        await InsertIndexAsync(Day(62), StartA, "NODE1", 100, "Orders", 1_000, 0);
        await InsertIndexAsync(Day(1), StartA, "NODE1", 100, "Orders", 1_100, 0);
        await InsertQueryStoreAsync(Day(1), "dbo.usp_Daily", 1, 1, 5, Day(1));
        await InsertHealthAsync(Day(3), Database, "AUTO");

        var root = await CallAsync();

        Assert.Equal("AUTO", root.GetProperty("coverage")[0].GetProperty("executions").GetProperty("query_capture_mode").GetString());
        Assert.Equal("Executed", Obj(root, "usp_Daily").GetProperty("classification").GetString());
    }
}
