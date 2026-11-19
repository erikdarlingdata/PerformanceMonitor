/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9055: a page latch wait in get_active_queries says what the page is, from the store alone. Seeds one capture with page latch rows
/// (each with the plan it ran under) and two daily index snapshots that rank the same indexes differently, then reads the real tool.
/// The newer snapshot must win, the match kinds must follow the plan, and a row that is not a page latch wait must carry no
/// <c>page_latch</c> object. A row with a NULL request_id and no plan is in the capture too, so the plan read is NULL-safe.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingActiveQueriesPageLatchLiveTests
{
    private const string ServerName = "darling-active-queries-page-latch-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task GetActiveQueries_PageLatchRows_NameThePageKindAndTheIndexesFromTheNewestSnapshot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live page latch test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var captured = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            var older = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-30);
            var newer = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddHours(-6);

            /* The older snapshot ranks Customers first, the newer one ranks the Orders index first, so a reader on the wrong snapshot is caught. */
            await SeedIndexAsync(connection, ct, older, "PK_Customers", "Customers", pageLatchMs: 9000, pageLatchCount: 900, ioMs: 0, ioCount: 0);
            await SeedIndexAsync(connection, ct, older, "IX_Orders_Date", "Orders", 10, 1, 0, 0);
            await SeedIndexAsync(connection, ct, newer, "PK_Customers", "Customers", 50, 5, 500, 50);
            await SeedIndexAsync(connection, ct, newer, "IX_Orders_Date", "Orders", 400, 40, 70, 7);
            await SeedIndexAsync(connection, ct, newer, "PK_Orders", "Orders", 100, 10, 900, 90);
            await SeedIndexAsync(connection, ct, newer, "PK_Items", "Items", 20, 2, 0, 0);
            await SeedIndexAsync(connection, ct, newer, "PK_Quiet", "Quiet", 0, 0, 0, 0);

            await SeedRowAsync(connection, ct, captured, 51, "PAGELATCH_SH", "5:1:4096", PageLatchResourceTests.SeekPlan, requestId: 0);
            await SeedRowAsync(connection, ct, captured, 52, "PAGELATCH_EX", "5:1:4096", PageLatchResourceTests.InsertPlan, requestId: 0);
            await SeedRowAsync(connection, ct, captured, 53, "PAGEIOLATCH_SH", "5:1:4097", plan: null, requestId: null);
            await SeedRowAsync(connection, ct, captured, 54, "PAGELATCH_UP", "5:1:2", plan: null, requestId: 0);
            await SeedRowAsync(connection, ct, captured, 55, "PAGELATCH_EX", "2:1:4096", PageLatchResourceTests.SeekPlan, requestId: 0);
            await SeedRowAsync(connection, ct, captured, 56, "LCK_M_S", "KEY: 5:72057594043432960 (a1b2c3d4e5f6)", PageLatchResourceTests.SeekPlan, requestId: 0);
            await SeedRowAsync(connection, ct, captured, 57, "PAGELATCH_SH", "5:1:4096", "<not a plan", requestId: 0);
            await SeedRowAsync(connection, ct, captured, 58, "PAGELATCH_SH", "5:1:4096", PageLatchResourceTests.OtherDatabasePlan, requestId: 0);

            var json = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, limit: 50);
            DarlingMcpTestData.AssertEnvelope(json, ServerName, "queries");
            using var parsed = JsonDocument.Parse(json);
            var rows = parsed.RootElement.GetProperty("queries").EnumerateArray().ToDictionary(r => r.GetProperty("session_id").GetInt32());

            Assert.Equal("5:1:4096", rows[51].GetProperty("wait_resource").GetString());

            /* One index in the plan: exact, with the NEWER snapshot's numbers. */
            var exact = rows[51].GetProperty("page_latch");
            Assert.Equal("data or index page", exact.GetProperty("page_kind").GetString());
            Assert.Equal("exact", exact.GetProperty("match").GetString());
            Assert.Equal(newer, exact.GetProperty("index_snapshot_time").GetDateTime().ToUniversalTime());
            var exactIndex = Assert.Single(exact.GetProperty("indexes").EnumerateArray());
            Assert.Equal("IX_Orders_Date", exactIndex.GetProperty("index_name").GetString());
            Assert.Equal(400, exactIndex.GetProperty("latch_wait_ms").GetInt64());
            Assert.Equal(40, exactIndex.GetProperty("latch_wait_count").GetInt64());

            /* A clustered insert that maintains an index: plan, both indexes, highest page latch wait first. */
            var plan = rows[52].GetProperty("page_latch");
            Assert.Equal("plan", plan.GetProperty("match").GetString());
            Assert.Equal(["IX_Orders_Date", "PK_Orders"], plan.GetProperty("indexes").EnumerateArray().Select(i => i.GetProperty("index_name").GetString()).ToArray());

            /* No plan and a NULL request_id: inferred, the three top indexes by PAGE I/O latch waits (this is a PAGEIOLATCH row), newest snapshot. */
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
            Assert.Equal("inferred", rows[57].GetProperty("page_latch").GetProperty("match").GetString());
            Assert.Equal("inferred", rows[58].GetProperty("page_latch").GetProperty("match").GetString());
            Assert.Equal(["IX_Orders_Date", "PK_Orders", "PK_Customers"], rows[58].GetProperty("page_latch").GetProperty("indexes").EnumerateArray().Select(i => i.GetProperty("index_name").GetString()).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task GetActiveQueries_WithNoPageLatchRow_AddsNoPageLatchObject_AndReadsNoIndexSnapshot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live page latch test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var captured = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            await SeedRowAsync(connection, ct, captured, 61, "LCK_M_S", "KEY: 5:72057594043432960 (a1b2c3d4e5f6)", plan: null, requestId: 0);
            await SeedRowAsync(connection, ct, captured, 62, null, null, plan: null, requestId: 0);

            var json = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, limit: 50);
            DarlingMcpTestData.AssertEnvelope(json, ServerName, "queries");
            Assert.DoesNotContain("page_latch", json, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task SeedRowAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at, int session, string? waitType, string? waitResource, string? plan, int? requestId) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, wait_type, wait_time_ms, wait_resource, query_plan, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, session, "Shop", "SELECT 1", "suspended", waitType, waitType == null ? 0L : 250L, waitResource, plan, requestId);

    private static Task SeedIndexAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at, string index, string table, long pageLatchMs, long pageLatchCount, long ioMs, long ioCount) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_id, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, page_latch_wait_count, page_latch_wait_in_ms, page_io_latch_wait_count, page_io_latch_wait_in_ms)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, 5, "Shop", "dbo", 100 + table.Length, table, index.Length, index, "CLUSTERED", 10m, 9m, 1000L,
            pageLatchCount, pageLatchMs, ioCount, ioMs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; DELETE FROM index_object_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
