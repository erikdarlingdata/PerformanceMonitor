/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5633 items 6, 15 and 16, Lite side (Darling's <c>McpToolAudit5633Tests</c> is the twin): get_database_sizes publishes a
/// percent-growth file's step, get_table_index_sizes sorts an unsized table last, publishes it as null and leaves tempdb
/// temp tables out, and get_index_usage never calls a heap or a clustered index Unused.
/// </summary>
public sealed class McpToolAudit5633Tests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 5633;
    private const string Db = "Audit5633";

    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public McpToolAudit5633Tests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    public void Dispose() => _seedConn?.Dispose();

    private static readonly DateTime Collected = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute), DateTimeKind.Unspecified);

    private async Task ExecAsync(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private Task PlantAsync(string table, int indexId, string type, string reservedMbLiteral, long reads, long updates, string? indexName = "IX_x") =>
        ExecAsync($@"INSERT INTO index_object_stats
            (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
             schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
             user_seeks, user_scans, user_lookups, user_updates)
            VALUES ({_nextId++}, TIMESTAMP '{Collected.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}', {ServerId}, 'Audit', TIMESTAMP '2026-01-01 00:00:00', '{Db}', 7,
                    'dbo', {1000 + table.Length * 37 + table[0]}, '{table}', {indexId}, {(indexName is null ? "NULL" : "'" + indexName + "'")}, '{type}',
                    {reservedMbLiteral}, {reservedMbLiteral}, 1000, {reads}, 0, 0, {updates})");

    [Fact]
    public async Task DatabaseSizes_PercentGrowthFile_ComesBackWithItsPercentStep_InTheReadAndThePayload()
    {
        await ExecAsync($@"INSERT INTO database_size_stats
            (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name,
             physical_name, total_size_mb, used_size_mb, auto_growth_mb, is_percent_growth, growth_pct)
            VALUES ({_nextId++}, TIMESTAMP '{Collected.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}', {ServerId}, 'Audit', '{Db}', 7, 1, 'ROWS', 'pct', 'D:\a.mdf', 1000, 500, 100, TRUE, 10),
                   ({_nextId++}, TIMESTAMP '{Collected.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)}', {ServerId}, 'Audit', '{Db}', 7, 2, 'LOG', 'mb', 'D:\a.ldf', 1000, 500, 64, FALSE, NULL)");

        var rows = await _dataService.GetLatestDatabaseSizeStatsAsync(ServerId);
        var pct = rows.Single(r => r.FileName == "pct");
        var mb = rows.Single(r => r.FileName == "mb");
        Assert.True(pct.IsPercentGrowth);
        Assert.Equal(10, pct.GrowthPct);
        Assert.False(mb.IsPercentGrowth);
        Assert.Null(mb.GrowthPct);

        using var doc = JsonDocument.Parse(McpServerInfoTools.DatabaseSizesPayload("srv", rows));
        var files = doc.RootElement.GetProperty("databases")[0].GetProperty("files").EnumerateArray().ToList();
        var filePct = files.Single(f => f.GetProperty("file_name").GetString() == "pct");
        Assert.True(filePct.GetProperty("is_percent_growth").GetBoolean());
        Assert.Equal(10, filePct.GetProperty("growth_pct").GetDouble());
        var fileMb = files.Single(f => f.GetProperty("file_name").GetString() == "mb");
        Assert.False(fileMb.GetProperty("is_percent_growth").GetBoolean());
        Assert.Equal(JsonValueKind.Null, fileMb.GetProperty("growth_pct").ValueKind);
    }

    [Fact]
    public async Task TableSizes_UnsizedTableSortsLast_ReadsNull_AndTempTablesAreLeftOut()
    {
        await PlantAsync("Big", 1, "CLUSTERED", "500", 0, 0);
        await PlantAsync("Small", 1, "CLUSTERED", "5", 0, 0);
        await PlantAsync("Unsized", 1, "CLUSTERED", "NULL", 0, 0);
        await PlantAsync("#usage_______0000001", 1, "CLUSTERED", "900", 0, 0);

        var rows = await _dataService.GetObjectSizeGrowthAsync(ServerId, 50, [Db]);

        Assert.Equal(["Big", "Small", "Unsized"], rows.Select(r => r.TableName).ToArray());
        Assert.Equal(500m, rows[0].CurrentReservedMb);
        Assert.Null(rows[2].CurrentReservedMb);
    }

    [Fact]
    public async Task IndexUsage_HeapsAndClusteredIndexes_AreNeverUnused()
    {
        await PlantAsync("TNcUnused", 2, "NONCLUSTERED", "10", 0, 0, "IX_unused");
        await PlantAsync("TNcWrite", 2, "NONCLUSTERED", "10", 0, 5, "IX_write");
        await PlantAsync("TNcActive", 2, "NONCLUSTERED", "10", 9, 5, "IX_active");
        await PlantAsync("TCx", 1, "CLUSTERED", "900", 0, 0, "PK_cx");
        await PlantAsync("TCxWrites", 1, "CLUSTERED", "800", 0, 7, "PK_cxw");
        await PlantAsync("TCxRead", 1, "CLUSTERED", "700", 3, 7, "PK_cxr");
        await PlantAsync("THeap", 0, "HEAP", "600", 0, 0, null);

        var rows = await _dataService.GetIndexUsageAsync(ServerId, 50, Db);
        string Class(string table) => rows.Single(r => r.TableName == table).Classification;

        Assert.Equal("Unused", Class("TNcUnused"));
        Assert.Equal("Write-only", Class("TNcWrite"));
        Assert.Equal("Active", Class("TNcActive"));
        Assert.Equal("Table not read", Class("TCx"));
        Assert.Equal("Table not read", Class("TCxWrites"));
        Assert.Equal("Active", Class("TCxRead"));
        Assert.Equal("Table not read", Class("THeap"));
        Assert.Equal(["TNcUnused", "TNcWrite"], rows.Take(2).Select(r => r.TableName).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void TheIndexUsageTail_NamesTheTableNotReadClass_AndEveryCounterReset()
    {
        var tail = McpToolGuideTests.Served("get_index_usage").Tail!;
        Assert.Contains("Table not read", tail, StringComparison.Ordinal);
        Assert.Contains("clears on an instance restart, a database restore, a detach and attach, the database going offline, and (for that index) a rebuild", tail, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSharedIndexObjectCollector_LeavesOutTempDbTemporaryTables()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = DateTime.UtcNow,
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { SqlMajorVersion = 17 },
            CurrentDatabaseName = "tempdb",
        };
        Assert.Contains("o.name NOT LIKE N'#%'", IndexObjectStatsCollector.Instance.BuildQuery(context).Text, StringComparison.Ordinal);
    }
}
