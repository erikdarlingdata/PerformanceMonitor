/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4198 (per-tool lane): <c>get_object_locking</c> measured 71,332 bytes at default arguments on a busy
/// production store (200 rows, <c>ObjectLockingTop</c>'s old value, no override available) -- more than double
/// <see cref="McpResponseBudget.DefaultBytes"/>. #4224's fixture excludes this tool from its own budget pin
/// (<c>McpReadToolBudgetLiveTests</c>'s remarks name it a "known gap": that fixture does not seed
/// <c>index_object_stats</c>), so this file seeds it directly and measures the same call.
///
/// <para>200 rows spread over 10 databases, with index/table names sized to reproduce the field's per-row
/// width (~355-430 bytes/row measured on this fixture, in the same range as the 71,332 B / 200 rows =~356.7
/// B/row the field measurement implies). Every row carries a positive wait metric so
/// <c>DarlingObjectStatsReader.IndexLockingSql</c>'s contention filter keeps all 200 -- the read's own ORDER BY
/// then ranks them by total wait time, so which 200 stay in matters for the truncated-page assertion below.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ObjectLockingBudgetLiveTests
{
    private const string ServerName = "darling-object-locking-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const int SeededRowCount = 200;
    private const int DatabaseCount = 10;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The regression pin: a default call (no <c>limit</c> passed) stays under
    /// <see cref="McpResponseBudget.DefaultBytes"/>, is marked <c>truncated</c>, and an explicit
    /// <c>limit</c> covering the whole seeded set still gets every row -- the ruling's "an explicit argument
    /// still gets what it asks for" half. Reverting the tool's default-arguments fix reproduces the original
    /// report: this fails on the old 200-row default the same way #4198's field measurement did.
    /// </summary>
    [Fact]
    public async Task DefaultCallStaysUnderBudget_AndAnExplicitLimitStillGetsEveryRow()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live get_object_locking budget test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var capture = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-1);
            await SeedAsync(connection, ct, capture);

            var defaultJson = await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, ServerName);
            Assert.False(McpHelpers.IsErrorEnvelope(defaultJson), $"tool returned an error: {defaultJson}");
            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);

            TestContext.Current.TestOutputHelper?.WriteLine(
                $"get_object_locking, default args, {SeededRowCount} contended rows seeded: {defaultBytes:N0} bytes, budget={McpResponseBudget.DefaultBytes:N0} bytes.");

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

            /* The ruling's other half: raising limit past the seeded count returns every row, unbounded by
               whatever default the fix landed on. */
            var fullJson = await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, ServerName, limit: SeededRowCount);
            Assert.False(McpHelpers.IsErrorEnvelope(fullJson), $"tool returned an error: {fullJson}");
            using (var fullDoc = JsonDocument.Parse(fullJson))
            {
                var root = fullDoc.RootElement;
                Assert.Equal(SeededRowCount, root.GetProperty("objects_returned").GetInt32());
                Assert.False(root.GetProperty("truncated").GetBoolean(),
                    "an explicit limit covering every seeded row should not come back truncated.");
                Assert.Equal(SeededRowCount, root.GetProperty("objects").GetArrayLength());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// 200 rows across 10 databases, table/index names sized like a real composite-index schema, every wait
    /// metric positive so <see cref="DarlingObjectStatsReader.IndexLockingSql"/>'s contention filter keeps
    /// all of them. Not shared with any other #4198 lane's fixture -- several run tonight.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, DateTime capture)
    {
        var collectionId = CollectionIdGenerator.Next();

        for (var i = 0; i < SeededRowCount; i++)
        {
            var dbIndex = i % DatabaseCount;
            var databaseName = $"TenantDb{dbIndex:D2}";
            var tableName = $"OrderLineItems{(i % 30):D2}";
            var indexName = $"IX_OrderLineItems{(i % 30):D2}_TenantId_OrderId";
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

            await DarlingMcpTestData.ExecAsync(
                connection,
                ct,
                @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)",
                collectionId, capture, ServerId, ServerName, databaseName, "dbo", 1000 + i, tableName,
                1 + i % 3, indexName, indexType, reservedMb, reservedMb, totalRows,
                rowLockWaitCount, rowLockWaitMs, pageLockWaitCount, pageLockWaitMs,
                indexLockPromotionCount, pageLatchWaitMs, pageIoLatchWaitMs);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM index_object_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
