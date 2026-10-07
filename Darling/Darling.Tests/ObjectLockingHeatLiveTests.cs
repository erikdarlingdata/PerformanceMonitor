/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
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
/// #5311 (part of #5231, PR2 lane H1): the web Locking page's heat bands. The service bands the four wait columns
/// (row lock, page lock, page latch, page I/O latch), each on its OWN log scale over the rows the response returns (the
/// filtered, capped page), with the shared helper <see cref="FinOpsHeatmapBuilder.ColumnLogIntensities"/> the desktop
/// uses; a zero value has no band. The bands are WEB-ONLY: the MCP <c>get_object_locking</c> payload has no
/// <c>heat</c> field.
/// </summary>
[Collection("live-postgres")]
public sealed class ObjectLockingHeatLiveTests
{
    private const string A = "HeatDbA";
    private const string B = "HeatDbB";
    private const string C = "HeatDbC";

    private static readonly string[] WaitProperties =
        ["row_lock_wait_ms", "page_lock_wait_ms", "page_latch_wait_ms", "page_io_latch_wait_ms"];

    private static Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime capture,
        string db, string index, long rowLockMs, long pageLockMs, long latchMs, long ioLatchMs) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
                  row_lock_wait_count, row_lock_wait_in_ms, page_lock_wait_count, page_lock_wait_in_ms, index_lock_promotion_count, page_latch_wait_in_ms, page_io_latch_wait_in_ms, user_seeks, user_scans, user_lookups, user_updates)
              VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,1,$8,'NONCLUSTERED',100,100,1000, 1,$9, 1,$10, 0,$11,$12, 0,0,0,0)",
            CollectionIdGenerator.Next(), capture, serverId, serverName, db, 1000 + index.Length, "T_" + index, index,
            rowLockMs, pageLockMs, latchMs, ioLatchMs);

    /// <summary>
    /// Four rows over three databases whose waits span orders of magnitude in every column, with a zero in three of
    /// the columns. C holds the largest row-lock and page-lock waits, so a filter that drops C changes those two scales.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime capture)
    {
        await PlantAsync(connection, ct, serverId, serverName, capture, A, "IX_a1", rowLockMs: 10, pageLockMs: 0, latchMs: 100_000, ioLatchMs: 5);
        await PlantAsync(connection, ct, serverId, serverName, capture, A, "IX_a2", rowLockMs: 1_000, pageLockMs: 10, latchMs: 0, ioLatchMs: 0);
        await PlantAsync(connection, ct, serverId, serverName, capture, B, "IX_b1", rowLockMs: 100, pageLockMs: 1, latchMs: 1_000, ioLatchMs: 5_000);
        await PlantAsync(connection, ct, serverId, serverName, capture, C, "IX_c1", rowLockMs: 1_000_000, pageLockMs: 100_000, latchMs: 10, ioLatchMs: 50);
    }

    private const string Cleanup = "DELETE FROM index_object_stats WHERE server_id = {0}";

    /// <summary>The bands the desktop's scale gives these rows: each column's intensity over the rows, cut into eight.</summary>
    private static int?[][] ExpectedBands(JsonElement objects)
    {
        var rows = objects.EnumerateArray().ToList();
        var perColumn = WaitProperties
            .Select(p => FinOpsHeatmapBuilder.ColumnLogIntensities(rows.Select(r => r.GetProperty(p).GetInt64()).ToList()))
            .ToList();
        return rows.Select((_, i) => perColumn
            .Select(col => col[i] > 0 ? (int?)Math.Min((int)Math.Floor(col[i] * 8), 7) : null)
            .ToArray()).ToArray();
    }

    private static int?[][] ResponseBands(JsonElement objects) =>
        objects.EnumerateArray()
            .Select(r => r.GetProperty("heat").EnumerateArray()
                .Select(e => e.ValueKind == JsonValueKind.Null ? (int?)null : e.GetInt32()).ToArray())
            .ToArray();

    private static string IndexOf(JsonElement row) => row.GetProperty("index_name").GetString()!;

    [Fact]
    public Task WebRead_BandsEachColumnOnItsOwnLogScale_LikeColumnLogIntensitiesOnTheSameRows() =>
        WithStoreAsync("heat-locking-scale", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);

            var root = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingWithHeatAsync(
                postgres, serverName, 200, null, DatabaseFilter.All, ct)).RootElement;
            var objects = root.GetProperty("objects");
            Assert.Equal(4, objects.GetArrayLength());

            var actual = ResponseBands(objects);
            Assert.Equal(ExpectedBands(objects), actual);

            /* Hand-checked against the seed: each column's largest value is band 7, a zero has no band, and the
               smallest positive of a column spanning 1 to 100,000 is not shaded like its largest. */
            var byIndex = objects.EnumerateArray().Select((r, i) => (Name: IndexOf(r), Bands: actual[i])).ToDictionary(x => x.Name, x => x.Bands);
            Assert.Equal(7, byIndex["IX_c1"][0]);
            Assert.Equal(7, byIndex["IX_c1"][1]);
            Assert.Equal(7, byIndex["IX_a1"][2]);
            Assert.Equal(7, byIndex["IX_b1"][3]);
            Assert.Null(byIndex["IX_a1"][1]);
            Assert.Null(byIndex["IX_a2"][2]);
            Assert.Null(byIndex["IX_a2"][3]);
            Assert.True(byIndex["IX_a1"][0] < byIndex["IX_c1"][0]);
        }, Cleanup);

    [Fact]
    public Task WebRead_ADatabaseFilterThatDropsTheLargestRow_RescalesTheBandsOverTheFilteredRows() =>
        WithStoreAsync("heat-locking-filter", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);

            var all = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingWithHeatAsync(
                postgres, serverName, 200, null, DatabaseFilter.All, ct)).RootElement.GetProperty("objects");
            var filtered = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingWithHeatAsync(
                postgres, serverName, 200, null, DatabaseFilter.Of([A, B]), ct)).RootElement.GetProperty("objects");

            Assert.Equal(3, filtered.GetArrayLength());
            Assert.DoesNotContain(filtered.EnumerateArray(), r => r.GetProperty("database_name").GetString() == C);
            var bands = ResponseBands(filtered);
            Assert.Equal(ExpectedBands(filtered), bands);

            /* With C gone, A's 1,000 ms row-lock wait is the column's largest: band 7. Over every row it was far below C's. */
            var a2Filtered = filtered.EnumerateArray().Select((r, i) => (Name: IndexOf(r), Bands: bands[i])).Single(x => x.Name == "IX_a2").Bands;
            var allBands = ResponseBands(all);
            var a2All = all.EnumerateArray().Select((r, i) => (Name: IndexOf(r), Bands: allBands[i])).Single(x => x.Name == "IX_a2").Bands;
            Assert.Equal(7, a2Filtered[0]);
            Assert.True(a2All[0] < 7);
        }, Cleanup);

    [Fact]
    public Task WebRead_TheLimitCutsThePageFirst_AndTheBandsAreOverThePageOnly() =>
        WithStoreAsync("heat-locking-cap", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);

            var root = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLockingWithHeatAsync(
                postgres, serverName, 2, null, DatabaseFilter.All, ct)).RootElement;
            var objects = root.GetProperty("objects");
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(2, objects.GetArrayLength());
            Assert.Equal(ExpectedBands(objects), ResponseBands(objects));
        }, Cleanup);

    [Fact]
    public Task McpGetObjectLocking_HasNoHeatField_BecauseTheBandsAreWebOnly() =>
        WithStoreAsync("heat-locking-mcp", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            await SeedAsync(connection, ct, serverId, serverName, now);

            var root = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName)).RootElement;
            var objects = root.GetProperty("objects");
            Assert.Equal(4, objects.GetArrayLength());
            foreach (var row in objects.EnumerateArray())
            {
                Assert.False(row.TryGetProperty("heat", out _), "the MCP payload must not carry the web's heat bands");
                Assert.Equal(14, row.EnumerateObject().Count());
            }
        }, Cleanup);

    [Fact]
    public Task WebRead_At200Rows_GrowsByUnder30BytesARow_WithTheSameRowsAsTheMcpRead() =>
        WithStoreAsync("heat-locking-size", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            for (var i = 1; i <= 200; i++)
            {
                /* Waits across six orders of magnitude, a zero in each column for some rows (those cells carry null). */
                await PlantAsync(connection, ct, serverId, serverName, now, "SizeDb" + i % 10, "IX_size_" + i,
                    rowLockMs: i % 7 == 0 ? 0 : (long)Math.Pow(10, i % 6) + i, pageLockMs: i % 5 == 0 ? 0 : 3L * i + 1,
                    latchMs: i % 3 == 0 ? 0 : 50L * i + 1, ioLatchMs: i % 11 == 0 ? 0 : 7L * i + 1);
            }

            var mcp = await DarlingMcpObjectStatsTools.GetObjectLocking(postgres, serverName, limit: 200);
            var web = await DarlingMcpObjectStatsTools.GetObjectLockingWithHeatAsync(postgres, serverName, 200, null, DatabaseFilter.All, ct);
            var webRows = JsonDocument.Parse(web).RootElement.GetProperty("objects");
            Assert.Equal(200, webRows.GetArrayLength());
            Assert.Equal(200, JsonDocument.Parse(mcp).RootElement.GetProperty("objects").GetArrayLength());

            var growth = System.Text.Encoding.UTF8.GetByteCount(web) - System.Text.Encoding.UTF8.GetByteCount(mcp);
            Assert.InRange(growth, 1, 30 * 200);
        }, Cleanup);

    private static async Task WithStoreAsync(
        string serverName, Func<NpgsqlConnection, NpgsqlDataSource, int, string, DateTime, CancellationToken, Task> body, string cleanupSql)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live Locking heat tests.");
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
