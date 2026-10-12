/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5311 (part of #5231, PR2 lane H2): the web Locking page's index detail pane. The web dispatch row of
/// <c>get_object_locking</c> takes an optional selector (<c>detail_database</c>, <c>detail_schema</c>, <c>detail_table</c>,
/// <c>detail_index</c>), each name bound as a parameter, and answers that one index's four counters (row lock count, page lock
/// count, page latch wait count, page I/O latch wait count). Without the selector the answer is what it was before; an unknown
/// index answers a plain sentence; the MCP tool has no selector parameter.
/// </summary>
[Collection("live-postgres")]
public sealed class ObjectLockingDetailLiveTests
{
    private const string A = "DetailDbA";
    private const string B = "DetailDbB";

    private static Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime capture,
        string db, string? index, long rowLockCount, long pageLockCount, long latchCount, long ioLatchCount, long waitMs) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
                  row_lock_count, row_lock_wait_count, row_lock_wait_in_ms, page_lock_count, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_count, page_latch_wait_in_ms, page_io_latch_wait_count, page_io_latch_wait_in_ms,
                  user_seeks, user_scans, user_lookups, user_updates)
              VALUES ($1,$2,$3,$4,$5,'dbo',1001,'Orders',$6,$7,'NONCLUSTERED',100,100,1000,
                  $8,1,$12,$9,1,$12,0,$10,$12,$11,$12, 0,0,0,0)",
            CollectionIdGenerator.Next(), capture, serverId, serverName, db, index is null ? 0 : 1, (object?)index ?? DBNull.Value,
            rowLockCount, pageLockCount, latchCount, ioLatchCount, waitMs);

    /// <summary>Two databases each holding an index with the SAME name, schema and table but different counters, and a heap.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime capture)
    {
        await PlantAsync(connection, ct, serverId, serverName, capture, A, "IX_same", 11, 22, 33, 44, 500);
        await PlantAsync(connection, ct, serverId, serverName, capture, B, "IX_same", 111, 222, 333, 444, 900);
        await PlantAsync(connection, ct, serverId, serverName, capture, A, null, 7, 8, 9, 10, 100);
    }

    private const string Cleanup = "DELETE FROM index_object_stats WHERE server_id = {0}";

    private static JsonElement Detail(string json) => JsonDocument.Parse(json).RootElement.GetProperty("detail").Clone();

    [Fact]
    public Task Selector_AnswersOnlyTheIndexAskedFor_WithItsFourCounters_WhenTwoDatabasesShareTheName() =>
        WithStoreAsync("detail-locking-two-dbs", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);

            var a = Detail(await DarlingMcpObjectStatsTools.GetObjectLockingDetailAsync(postgres, serverName, A, "dbo", "Orders", "IX_same", ct));
            Assert.Equal(A, a.GetProperty("database_name").GetString());
            Assert.Equal(11, a.GetProperty("row_lock_count").GetInt64());
            Assert.Equal(22, a.GetProperty("page_lock_count").GetInt64());
            Assert.Equal(33, a.GetProperty("page_latch_wait_count").GetInt64());
            Assert.Equal(44, a.GetProperty("page_io_latch_wait_count").GetInt64());

            var b = Detail(await DarlingMcpObjectStatsTools.GetObjectLockingDetailAsync(postgres, serverName, B, "dbo", "Orders", "IX_same", ct));
            Assert.Equal(B, b.GetProperty("database_name").GetString());
            Assert.Equal(111, b.GetProperty("row_lock_count").GetInt64());
            Assert.Equal(444, b.GetProperty("page_io_latch_wait_count").GetInt64());

            /* A heap has no index name: the absent name selects it, and an index-named read never reaches it. */
            var heap = Detail(await DarlingMcpObjectStatsTools.GetObjectLockingDetailAsync(postgres, serverName, A, "dbo", "Orders", null, ct));
            Assert.Equal(JsonValueKind.Null, heap.GetProperty("index_name").ValueKind);
            Assert.Equal(7, heap.GetProperty("row_lock_count").GetInt64());
        }, Cleanup);

    [Fact]
    public Task Selector_AnUnknownIndex_AnswersAPlainSentence_NotAnError() =>
        WithStoreAsync("detail-locking-unknown", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);

            foreach (var json in new[]
            {
                await DarlingMcpObjectStatsTools.GetObjectLockingDetailAsync(postgres, serverName, A, "dbo", "Orders", "IX_missing", ct),
                await DarlingMcpObjectStatsTools.GetObjectLockingDetailAsync(postgres, serverName, "NoSuchDb", "dbo", "Orders", "IX_same", ct),
                /* the names match exactly: a different case is a different name */
                await DarlingMcpObjectStatsTools.GetObjectLockingDetailAsync(postgres, serverName, A, "dbo", "Orders", "ix_same", ct),
                /* a value that looks like SQL is only ever a name that matches nothing */
                await DarlingMcpObjectStatsTools.GetObjectLockingDetailAsync(postgres, serverName, A, "dbo", "Orders", "' OR 1=1 --", ct),
            })
            {
                using var doc = JsonDocument.Parse(json);
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
                Assert.False(doc.RootElement.TryGetProperty("detail", out _));
                Assert.Contains("No such index", doc.RootElement.GetProperty("message").GetString());
            }
        }, Cleanup);

    [Fact]
    public Task WebDispatch_WithoutTheSelector_IsTheListAnswerItWas_AndWithItIsTheDetail() =>
        WithStoreAsync("detail-locking-dispatch", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);

            var expected = await DarlingMcpObjectStatsTools.GetObjectLockingWithHeatAsync(postgres, serverName, 200, null, DatabaseFilter.All, ct);
            var (status, body) = await FinOpsWebReadParityLiveTests.GetAsync(postgres, $"/api/read/get_object_locking?server={serverName}&limit=200", ct);
            Assert.Equal(200, status);
            Assert.Equal(expected, body);

            var (detailStatus, detailBody) = await FinOpsWebReadParityLiveTests.GetAsync(postgres,
                $"/api/read/get_object_locking?server={serverName}&detail_database={A}&detail_schema=dbo&detail_table=Orders&detail_index=IX_same", ct);
            Assert.Equal(200, detailStatus);
            Assert.Equal(22, Detail(detailBody).GetProperty("page_lock_count").GetInt64());

            /* a selector missing a required name is refused before the store is read */
            var (partialStatus, partialBody) = await FinOpsWebReadParityLiveTests.GetAsync(postgres,
                $"/api/read/get_object_locking?server={serverName}&detail_schema=dbo&detail_table=Orders", ct);
            Assert.Equal(400, partialStatus);
            Assert.Contains("detail_database", partialBody);
        }, Cleanup);

    [Fact]
    public void TheMcpTool_HasNoSelectorParameter_ItsSchemaIsTheListReads()
    {
        var parameters = typeof(DarlingMcpObjectStatsTools).GetMethod(nameof(DarlingMcpObjectStatsTools.GetObjectLocking))!
            .GetParameters().Select(p => p.Name!).ToArray();
        Assert.DoesNotContain(parameters, name => name.Contains("detail", StringComparison.OrdinalIgnoreCase)
            || name.Contains("schema_name", StringComparison.OrdinalIgnoreCase)
            || name.Contains("table", StringComparison.OrdinalIgnoreCase)
            || name.Contains("index", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("server_name", parameters);
        Assert.Contains("limit", parameters);
    }

    private static async Task WithStoreAsync(
        string serverName, Func<NpgsqlConnection, NpgsqlDataSource, int, string, DateTime, CancellationToken, Task> body, string cleanupSql)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live Locking detail tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var cleanup = string.Format(System.Globalization.CultureInfo.InvariantCulture, cleanupSql, serverId)
                      + string.Format(System.Globalization.CultureInfo.InvariantCulture, "; DELETE FROM servers WHERE server_id = {0}", serverId);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
            await body(connection, postgres, serverId, serverName, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-1), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }
}
