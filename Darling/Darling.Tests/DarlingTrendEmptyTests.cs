/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The three windowed trends whose empty answer used to be one sentence covering two opposite states
/// (#2485): get_memory_trend, get_file_io_trend and get_query_duration_trend all returned
/// <c>Status("unavailable", "No … trend data available.")</c> whatever the reason. "Unavailable" sounds
/// specific and says nothing: a server that collected fine and was quiet in this window needs the window
/// widened, and a server the collector has never touched needs somebody to look at collection — and
/// widening will never fill that one.
///
/// <para>These are the three where Lite returned a BARE empty array while Darling returned an envelope, so
/// the same tool name gave two different answers depending on which SKU the client was pointed at. Both
/// SKUs now return the same two sentences word for word.</para>
///
/// <para>Gated on DARLING_TEST_PG like every other live class.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingTrendEmptyTests
{
    private const int ServerId = -949555;
    private const string ServerName = "trend-empty";
    private const string Db = "AppDb";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AllThreeTrends_SeparateAQuietWindowFromANeverCollectedServer_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live trend-empty test.");

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

            /* ── never collected: not an empty window, a server nothing has been stored for ── */
            AssertNeverCollected(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName, 4));
            AssertNeverCollected(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 4));
            AssertNeverCollected(await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, 4));

            /* ── collected, but everything outside the window: a genuinely quiet window ── */
            var old = HoursAgo(48);
            await SeedMemoryAsync(connection, ct, old);
            await SeedFileIoAsync(connection, ct, old);
            await SeedQueryAsync(connection, ct, old);

            AssertQuietWindow(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName, 1));
            AssertQuietWindow(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 1));
            AssertQuietWindow(await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, 1));

            /* ── inside the window: the trend payload, envelope gone ── */
            var recent = MinutesAgo(10);
            await SeedMemoryAsync(connection, ct, recent);
            await SeedFileIoAsync(connection, ct, recent);
            await SeedQueryAsync(connection, ct, recent);

            foreach (var payload in new[]
                     {
                         await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName, 4),
                         await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 4),
                         await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, 4),
                     })
            {
                var root = JsonDocument.Parse(payload).RootElement;
                Assert.False(root.TryGetProperty("status", out _));
                Assert.True(root.GetProperty("trend").GetArrayLength() > 0);
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
    /// The probe has to walk the SAME relation its trend walks, and this is derived from the SQL rather
    /// than restated: the relation is pulled out of the probe and looked for in the trend. It is not a
    /// formality — get_query_duration_trend reads the BASE <c>query_stats</c> table because a V38+ store's
    /// <c>v_query_stats</c> is the payload-RESOLVING view, so a probe that reached for the view out of
    /// habit would be asking about a different relation from the one the read returns nothing from.
    /// </summary>
    [Fact]
    public void EachProbe_WalksTheSameRelationAsItsTrend()
    {
        AssertSameRelation(DarlingTrendReader.HasAnyMemoryStatSql, DarlingTrendReader.MemoryTrendSql);
        AssertSameRelation(DarlingTrendReader.HasAnyFileIoStatSql, DarlingTrendReader.FileIoLatencyTrendSql);
        AssertSameRelation(DarlingTrendReader.HasAnyQueryStatSql, DarlingTrendReader.QueryDurationTrendSql);

        /* And each stops at the first row: it runs on a path that already found nothing, and its only job
           is to pick which of the two sentences is true. */
        Assert.Contains("LIMIT 1", DarlingTrendReader.HasAnyMemoryStatSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", DarlingTrendReader.HasAnyFileIoStatSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", DarlingTrendReader.HasAnyQueryStatSql, StringComparison.Ordinal);
    }

    private static void AssertSameRelation(string probeSql, string trendSql)
    {
        var relation = Regex.Match(probeSql, @"FROM\s+(\S+)").Groups[1].Value;
        Assert.False(string.IsNullOrEmpty(relation), "the probe must name a relation");
        Assert.Contains("FROM " + relation, trendSql, StringComparison.Ordinal);
    }

    /// <summary>Nothing has ever been stored for this server: NOT an empty window, and widening it would
    /// never help.</summary>
    private static void AssertNeverCollected(string payload)
    {
        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("EVER", text, StringComparison.Ordinal);
        Assert.Contains("not an empty window", text, StringComparison.Ordinal);
        Assert.DoesNotContain("widen hours_back", text, StringComparison.Ordinal);
    }

    /// <summary>The server collected and this window is simply quiet — the opposite next move, and it must
    /// not share wording with the case above.</summary>
    private static void AssertQuietWindow(string payload)
    {
        var root = JsonDocument.Parse(payload).RootElement;
        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("widen hours_back", text, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", text, StringComparison.Ordinal);
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task SeedMemoryAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName,
            40000m, 49152m, 35000m, 5000m);

    /* delta_reads above zero on purpose: the trend's top_files CTE requires read or write activity, so a
       row with zero deltas would leave the window empty for a reason that has nothing to do with #2485. */
    private static async Task SeedFileIoAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type,
     physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
     delta_stall_read_ms, delta_stall_write_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, Db,
            "app.mdf", "ROWS", "D:\\app.mdf", 100000m, 500L, 200L, 4096000L, 1024000L, 2500L, 400L);

    private static async Task SeedQueryAsync(NpgsqlConnection connection, CancellationToken ct, DateTime t) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
     min_dop, max_dop)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t), ServerId, ServerName, Db,
            "0xEMPTYTRENDHASH", "0xPLANHASH", "0xSQLH", "0xPLANH", "SELECT * FROM Orders",
            10L, 10000L, 20000L, 5000L, 0L, 50L, 1000L, 0L, 1, 4);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM memory_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM file_io_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
