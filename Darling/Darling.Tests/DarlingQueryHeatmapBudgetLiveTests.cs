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
/// Gated (DARLING_TEST_PG) #4198 byte-budget pin for get_query_heatmap, on its own seeded store rather than
/// the shared <c>DarlingQueryHeatmapLiveTests</c> fixture — a dozen #4198 lanes seed in parallel tonight, and
/// a shared seeding helper is exactly the kind of file every one of them would collide on.
///
/// <para>A heatmap payload is CELLS (queries times time buckets) plus TEXT. #4198 measured a real busy store
/// at default arguments — 144,757 bytes for 500 cells of a 120-character preview each — and the seed below
/// reproduces that shape: enough distinct (time bin, magnitude bucket) cells to fill the default cap several
/// times over, each carrying a 227-character statement (comfortably past both the old 120-character preview
/// and the new 80-character one), so both the CAP and the TEXT WIDTH are exercised, not just one of them.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingQueryHeatmapBudgetLiveTests
{
    private const int ServerId = -419841;
    private const string ServerName = "query-heatmap-budget";
    private const string Db = "AppDb";

    /* Realistic, not minimal: a two-table join with a WHERE and an ORDER BY, the shape of statement an actual
       OLTP server runs constantly. 227 characters — past both the old 120-char preview and the new 80-char
       default, so every seeded cell's top query is truncated at default and NOT truncated under full_text. */
    private const string QueryText =
        "SELECT o.OrderId, o.CustomerId, o.OrderDate, o.TotalAmount, c.CustomerName FROM dbo.Orders AS o " +
        "JOIN dbo.Customers AS c ON o.CustomerId = c.CustomerId WHERE o.OrderDate >= @start AND o.Status = @status " +
        "ORDER BY o.OrderDate DESC";

    private const int SeedBins = 60;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DefaultCall_OnABusyStore_StaysUnderTheSharedBudget()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live heatmap budget test.");

        Assert.True(QueryText.Length > 120, "seed text must exceed both the old and new preview widths");

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

            /*
                SeedBins (60) x 7 magnitude buckets = 420 cells, well past the new default cap (100) and the
                old one (500) both — a server busy enough that the cap, not the window, is what bounds the
                default call either way. One 5-minute-aligned bin per iteration, all seven buckets touched by
                choosing delta_elapsed_time (execution count pinned at 1, so metric_value is elapsed/1000 ms
                directly) so every one of the seven bands has at least one populated cell per bin.
            */
            var t0 = FloorToHour(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow)).AddHours(-1);
            long[] elapsedMicrosByBucket = { 500, 5_000, 50_000, 500_000, 5_000_000, 50_000_000, 500_000_000 };

            for (var bin = 0; bin < SeedBins; bin++)
            {
                var t = t0.AddMinutes(-5 * bin);
                for (var bucket = 0; bucket < 7; bucket++)
                {
                    /* 18 characters - CONVERT(varchar(64), query_hash, 1)'s real width for an 8-byte hash
                       (0x + 16 hex), not a shortened test stand-in, so the measured byte count below is not
                       flattered by an unrealistically small hash field. */
                    await SeedAsync(connection, ct, t, $"0x{bin:X8}{bucket:X8}", elapsedMicrosByBucket[bucket]);
                }
            }

            /* ── default call: the fix under test ── */
            var defaultJson = await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName);
            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);
            var root = JsonDocument.Parse(defaultJson).RootElement;

            Assert.True(defaultBytes < McpResponseBudget.DefaultBytes,
                $"get_query_heatmap default call is {defaultBytes:N0} bytes, at or over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            /*
                Every seeded bin is fully populated (all seven buckets), so the cap lands mid-bin and the
                "no partial column" rule (DarlingMcpQueryHeatmapTools.GetQueryHeatmap) drops the one bin the
                cap only partly reached — cell_count is therefore the cap rounded DOWN to a whole number of
                bins, not the cap itself.
            */
            var cellCount = root.GetProperty("cell_count").GetInt32();
            Assert.True(cellCount % 7 == 0 && cellCount <= DarlingMcpQueryHeatmapTools.DefaultCellLimit
                && cellCount > DarlingMcpQueryHeatmapTools.DefaultCellLimit - 7,
                $"cell_count {cellCount} should be the default cap ({DarlingMcpQueryHeatmapTools.DefaultCellLimit}) rounded down to whole bins");
            Assert.True(root.GetProperty("truncated").GetBoolean(), "420 populated cells at the default cap must report truncated");
            Assert.False(root.GetProperty("full_text").GetBoolean());

            var cells = root.GetProperty("cells").EnumerateArray().ToArray();
            Assert.Equal(cellCount, cells.Length);
            Assert.All(cells, c =>
            {
                Assert.True(c.GetProperty("top_query_text_truncated").GetBoolean());
                Assert.Equal(DarlingMcpQueryHeatmapTools.DefaultPreviewLength, c.GetProperty("top_query_text").GetString()!.Length);
                Assert.Equal(QueryText[..DarlingMcpQueryHeatmapTools.DefaultPreviewLength], c.GetProperty("top_query_text").GetString());
            });

            /* ── full_text opts back into the whole statement, honestly marked as not truncated ── */
            var fullJson = await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(
                postgres, ServerName, 24, null, null, DarlingQueryHeatmapReader.ViewerBucketMinutes,
                DarlingMcpQueryHeatmapTools.DefaultCellLimit, null, true);
            var fullRoot = JsonDocument.Parse(fullJson).RootElement;
            Assert.True(fullRoot.GetProperty("full_text").GetBoolean());
            var fullCells = fullRoot.GetProperty("cells").EnumerateArray().ToArray();
            Assert.All(fullCells, c =>
            {
                Assert.False(c.GetProperty("top_query_text_truncated").GetBoolean());
                Assert.Equal(QueryText, c.GetProperty("top_query_text").GetString());
            });
            /* An explicit ask still gets what it asks for (#4198's ruling) even past the budget. */
            var fullBytes = Encoding.UTF8.GetByteCount(fullJson);
            Assert.True(fullBytes > defaultBytes, "full_text=true must not be smaller than the truncated default");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime FloorToHour(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerHour), value.Kind);

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, string queryHash, long deltaElapsedMicros) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     sample_interval_seconds, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            Db, queryHash, 60, 1L, 0L, deltaElapsedMicros, 0L, 0L, QueryText);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
