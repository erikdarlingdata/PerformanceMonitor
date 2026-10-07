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
/// #4198 (per-tool lane), Lite's half of the twin fix Darling's own <c>ObjectLockingBudgetLiveTests</c> pins:
/// <c>get_object_locking</c> measured 71,332 bytes at default arguments on a busy production store (200 rows,
/// the old hardcoded cap, no override) -- more than double <see cref="McpResponseBudget.DefaultBytes"/>. Needs
/// no live server and no skip-when-absent: <see cref="SharedDuckDbFixture"/> gives a real local store, the
/// same convention <c>IndexUsageTruncationTests</c> uses for Lite's <c>get_index_usage</c> twin.
/// </summary>
public sealed class ObjectLockingBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "ObjectLockingBudgetSrv";
    private const int SeededRowCount = 200;
    private const int DatabaseCount = 10;

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = -1;

    public ObjectLockingBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-objectlocking-budget-" + Guid.NewGuid().ToString("N"));
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

    /// <summary>
    /// 200 rows across 10 databases, sized like <c>ObjectLockingBudgetLiveTests</c>' Postgres fixture (same
    /// name/width pattern), every wait metric positive so the locking read's contention filter keeps all of
    /// them: default args stay under budget and are marked truncated, and an explicit limit covering every
    /// seeded row gets everything back.
    /// </summary>
    [Fact]
    public async Task DefaultCallStaysUnderBudget_AndAnExplicitLimitStillGetsEveryRow()
    {
        await SeedAsync();

        var defaultJson = await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName);
        Assert.False(McpHelpers.IsErrorEnvelope(defaultJson), $"tool returned an error: {defaultJson}");
        var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);

        Assert.True(defaultBytes <= McpResponseBudget.DefaultBytes,
            $"get_object_locking default call was {defaultBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        using (var defaultDoc = JsonDocument.Parse(defaultJson))
        {
            var root = defaultDoc.RootElement;
            var returned = root.GetProperty("objects_returned").GetInt32();
            Assert.True(root.GetProperty("truncated").GetBoolean(),
                $"expected the default page to be truncated against {SeededRowCount} seeded contended rows, but only {returned} were returned and truncated was false.");
            Assert.True(returned < SeededRowCount,
                $"expected the default page to be narrower than the seeded {SeededRowCount} rows; got {returned}.");
            Assert.Equal(returned, root.GetProperty("objects").GetArrayLength());
        }

        var fullJson = await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName, limit: SeededRowCount);
        Assert.False(McpHelpers.IsErrorEnvelope(fullJson), $"tool returned an error: {fullJson}");
        using (var fullDoc = JsonDocument.Parse(fullJson))
        {
            var root = fullDoc.RootElement;
            Assert.Equal(SeededRowCount, root.GetProperty("objects_returned").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean(),
                "an explicit limit covering every seeded row should not come back truncated.");
            Assert.Equal(SeededRowCount, root.GetProperty("objects").GetArrayLength());
        }
    }

    /// <summary>
    /// #5231: <c>database_name</c> (appended last) reaches the reader's one-database filter. Over the seeded 200
    /// rows across 10 databases, one name returns exactly that database's 20 rows, omitting it or passing a blank
    /// returns the whole server, and a name nothing was collected for gets the database named in its empty answer.
    /// </summary>
    [Fact]
    public async Task DatabaseName_LimitsTheAnswerToThatDatabase()
    {
        await SeedAsync();

        var json = await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName,
            limit: SeededRowCount, database_name: "TenantDb03");
        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using (var doc = JsonDocument.Parse(json))
        {
            var objects = doc.RootElement.GetProperty("objects");
            Assert.Equal(SeededRowCount / DatabaseCount, objects.GetArrayLength());
            foreach (var o in objects.EnumerateArray())
                Assert.Equal("TenantDb03", o.GetProperty("database_name").GetString());
            Assert.False(doc.RootElement.GetProperty("truncated").GetBoolean());
        }

        foreach (var unfiltered in new string?[] { null, "", "   " })
        {
            var all = await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName,
                limit: SeededRowCount, database_name: unfiltered);
            using var allDoc = JsonDocument.Parse(all);
            Assert.Equal(SeededRowCount, allDoc.RootElement.GetProperty("objects").GetArrayLength());
        }

        var none = await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName,
            database_name: "NoSuchDb");
        Assert.False(McpHelpers.IsErrorEnvelope(none), $"tool returned an error: {none}");
        Assert.Contains("NoSuchDb", none);
        /* #5372 M2: the server HAS contention elsewhere, so this is `empty` (looked, found nothing in that database), not
           `unavailable`, exactly as Darling's twin answers. */
        using var noneDoc = JsonDocument.Parse(none);
        Assert.Equal("empty", noneDoc.RootElement.GetProperty("status").GetString());
    }

    /// <summary>
    /// #5372 M2: a server with no contention at all is still <c>unavailable</c> (or <c>not_collected</c>) for a filtered call,
    /// so the status word keeps telling "nothing on this server" from "nothing in this database".
    /// </summary>
    [Fact]
    public async Task DatabaseName_OnAServerWithNoContentionAtAll_IsNotEmpty()
    {
        var json = await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName, database_name: "TenantDb03");
        using var doc = JsonDocument.Parse(json);
        Assert.NotEqual("empty", doc.RootElement.GetProperty("status").GetString());
    }

    /// <summary>
    /// #5372 (r2 Low): a latest snapshot that holds an index with no contention anywhere is a true negative, so the word is
    /// <c>empty</c> (the collector looked and found nothing), with or without a database chosen, in the sentence shape
    /// Darling's twin uses. A server with no snapshot at all stays <c>unavailable</c> (the test above).
    /// </summary>
    [Fact]
    public async Task ASnapshotWithNoContendedIndex_IsEmpty_NotUnavailable()
    {
        await SeedQuietAsync();

        using var all = JsonDocument.Parse(await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName));
        Assert.Equal("empty", all.RootElement.GetProperty("status").GetString());
        Assert.Equal($"No lock/latch contention on {ServerName} at the latest snapshot.", all.RootElement.GetProperty("message").GetString());

        using var one = JsonDocument.Parse(await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName, database_name: "QuietDb"));
        Assert.Equal("empty", one.RootElement.GetProperty("status").GetString());
        Assert.Equal($"No lock/latch contention in database 'QuietDb' or in any other database on {ServerName} at the latest snapshot.",
            one.RootElement.GetProperty("message").GetString());
    }

    /// <summary>#5372 M2: the TRUNCATED and Complete sentences name the chosen database, as Darling's twin does.</summary>
    [Fact]
    public async Task DatabaseName_IsNamedInTheTruncatedAndCompleteSentences()
    {
        await SeedAsync();

        using var capped = JsonDocument.Parse(await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName,
            limit: 5, database_name: "TenantDb03"));
        Assert.True(capped.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Contains("lock/latch contention in database 'TenantDb03' at the latest", capped.RootElement.GetProperty("note").GetString());

        using var whole = JsonDocument.Parse(await McpObjectStatsTools.GetObjectLocking(_dataService, _serverManager, ServerName,
            limit: SeededRowCount, database_name: "TenantDb03"));
        Assert.Equal("Complete: every index with lock/latch contention in database 'TenantDb03' at the latest snapshot is included.",
            whole.RootElement.GetProperty("note").GetString());
    }

    /// <summary>
    /// #5372 L1: rows that tie on the four summed waits (every row listed only for a lock promotion sums to 0) come back
    /// in a fixed order, promotions first and then the four-part name, so two runs at the cap return the same rows.
    /// Seeded in REVERSE name order so an unordered tie would hand back the wrong end.
    /// </summary>
    [Fact]
    public async Task TiedRows_AtTheCap_ComeBackInAFixedOrder()
    {
        await SeedTiedAsync();

        var first = await _dataService.GetIndexLockingAsync(_serverId, 3);
        var second = await _dataService.GetIndexLockingAsync(_serverId, 3);
        Assert.Equal(["Tied00", "Tied01", "Tied02"], first.Select(r => r.TableName));
        Assert.Equal(first.Select(r => r.TableName), second.Select(r => r.TableName));
    }

    private async Task SeedQuietAsync()
    {
        var capture = DateTime.UtcNow;
        var seedConn = await SeedConnectionAsync();
        using var batch = new SeedBatch(_duckDb, seedConn);
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = seedConn.CreateCommand();
        cmd.CommandText = @"INSERT INTO index_object_stats
            (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
             schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
             row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms,
             index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
            VALUES ($1,$2,$3,$4,$5,'QuietDb',7,'dbo',6000,'Quiet',1,'PK_Quiet','CLUSTERED',1,1,100,0,0,0,0,0,0,0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--); P(capture); P(_serverId); P(ServerName); P(capture.AddDays(-10));
        await cmd.ExecuteNonQueryAsync();
        batch.Commit();
    }

    private async Task SeedTiedAsync()
    {
        var capture = DateTime.UtcNow;
        var seedConn = await SeedConnectionAsync();
        using var batch = new SeedBatch(_duckDb, seedConn);
        for (var i = 9; i >= 0; i--)
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var cmd = seedConn.CreateCommand();
            cmd.CommandText = @"INSERT INTO index_object_stats
                (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
                 schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
                 row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms,
                 index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
                VALUES ($1,$2,$3,$4,$5,'TieDb',7,'dbo',$6,$7,1,$8,'CLUSTERED',1,1,100,0,0,0,0,$9,0,0)";
            void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
            P(_nextId--); P(capture); P(_serverId); P(ServerName); P(capture.AddDays(-10));
            P(5000 + i); P($"Tied{i:D2}"); P($"PK_Tied{i:D2}"); P(1L);
            await cmd.ExecuteNonQueryAsync();
        }

        batch.Commit();
    }

    private async Task SeedAsync()
    {
        var capture = DateTime.UtcNow;
        var startTime = capture.AddDays(-10);

        // One transaction for the 200 rows (#5208), committed when this method returns and before the tool reads.
        var seedConn = await SeedConnectionAsync();
        using var batch = new SeedBatch(_duckDb, seedConn);

        for (var i = 0; i < SeededRowCount; i++)
        {
            var dbIndex = i % DatabaseCount;
            var databaseName = $"TenantDb{dbIndex:D2}";
            var tableName = $"OrderLineItems{i % 30:D2}";
            var indexName = $"IX_OrderLineItems{i % 30:D2}_TenantId_OrderId";
            var indexType = i % 5 == 0 ? "CLUSTERED" : "NONCLUSTERED";
            var reservedMb = 100m + i * 7.31m;
            var totalRows = 10_000L + i * 997L;
            var rowLockWaitCount = 50L + i * 3L;
            var rowLockWaitMs = 500L + i * 17L;
            var pageLockWaitCount = 10L + i % 40L;
            var pageLockWaitMs = 100L + i % 400L;
            var indexLockPromotionCount = (long)(i % 7);
            var pageLatchWaitMs = 200L + i % 500L;
            var pageIoLatchWaitMs = 300L + i % 600L;

            using var readLock = _duckDb.AcquireReadLock();
            var conn = await SeedConnectionAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO index_object_stats
                (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
                 schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
                 row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms,
                 index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
                VALUES ($1,$2,$3,$4,$5,$6,7,'dbo',$7,$8,$9,$10,$11,$12,$12,$13,$14,$15,$16,$17,$18,$19,$20)";
            void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
            P(_nextId--); P(capture); P(_serverId); P(ServerName); P(startTime); P(databaseName);
            P(1000 + i); P(tableName); P(1 + i % 3); P(indexName); P(indexType); P(reservedMb);
            P(totalRows); P(rowLockWaitCount); P(rowLockWaitMs); P(pageLockWaitCount); P(pageLockWaitMs);
            P(indexLockPromotionCount); P(pageLatchWaitMs); P(pageIoLatchWaitMs);
            await cmd.ExecuteNonQueryAsync();
        }

        batch.Commit();
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
}
