/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_collection_log (#2484): the raw per-run log the viewer's Collection Log and Duration Trends tabs
/// read, which had no MCP tool and therefore no /api/read endpoint either -- so neither the web dashboard
/// nor an agent could see it, and the WPF viewer on a Windows desktop was the only way to it.
///
/// <para>The assertions that matter are the two KINDS of empty. "No runs in the last N hours" is true both
/// of a server that collected fine and was quiet, and of a server that has never collected at all, and
/// those want opposite responses -- widen the window, versus go find out why collection is not running.
/// A single sentence covering both is the exact failure #2485 catalogues elsewhere, so this read is not
/// allowed to add another instance of it.</para>
///
/// <para>Gated on DARLING_TEST_PG like every other live class.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCollectionLogReadTests
{
    private const int ServerId = -949552;
    private const string ServerName = "collection-log-read";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task EmptyWindow_AndNeverCollected_AreDifferentAnswers_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-log read test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);

        /*
            The cleanup runs on its OWN connection, not this body's. A teardown on the body's
            connection throws out of the finally and REPLACES the exception already in flight, so a
            failing test reports its cleanup error instead of its own -- and it is the body's failure
            that closed the connection in the first place. LiveStoreCleanup is the enforced route.
        */
        var bodySucceeded = false;

        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* ── 1. registered, never collected: a FAULT, and it has to say so ── */
            var never = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 200);
            var neverDoc = JsonDocument.Parse(never);
            Assert.Equal("unavailable", neverDoc.RootElement.GetProperty("status").GetString());
            var neverText = neverDoc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);

            /* The trap this test exists for: a caller told to widen the window will never fill it. */
            Assert.DoesNotContain("widen", neverText, StringComparison.OrdinalIgnoreCase);

            /* ── 2. collected, but not inside the asked-for window: a TRUE NEGATIVE ── */
            await SeedAsync(connection, ct, "query_store", HoursAgo(48));

            var quiet = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 1, 200);
            var quietDoc = JsonDocument.Parse(quiet);
            Assert.Equal("empty", quietDoc.RootElement.GetProperty("status").GetString());
            var quietText = quietDoc.RootElement.GetProperty("message").GetString()!;

            /* Same row count as case 1 -- zero -- and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", quietText, StringComparison.Ordinal);
            Assert.Contains("widen", quietText, StringComparison.OrdinalIgnoreCase);

            /* ── 3. rows in the window: the data path, and the split that makes the log worth reading ── */
            await SeedAsync(connection, ct, "query_store", MinutesAgo(10));
            await SeedAsync(connection, ct, "deadlocks", MinutesAgo(5));

            var hit = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 200);
            var root = JsonDocument.Parse(hit).RootElement;
            Assert.Equal(ServerName, root.GetProperty("server").GetString());
            Assert.Equal(2, root.GetProperty("run_count").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());

            var runs = root.GetProperty("runs");
            Assert.Equal(2, runs.GetArrayLength());

            /* Newest first, so the 5-minute-old deadlocks run leads the 10-minute-old query_store one. */
            Assert.Equal("deadlocks", runs[0].GetProperty("collector").GetString());

            /* The whole point of the raw log over the rollup: WHERE the time went. A collector slow
               because the monitored server is slow needs a different fix from one slow because the
               store is, and the total alone cannot separate them. */
            Assert.Equal(80, runs[0].GetProperty("sql_duration_ms").GetDouble());
            Assert.Equal(20, runs[0].GetProperty("store_duration_ms").GetDouble());

            /* ── 4. the cap announces itself rather than leaving it to be inferred ── */
            var capped = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 1);
            var cappedRoot = JsonDocument.Parse(capped).RootElement;
            Assert.Equal(1, cappedRoot.GetProperty("run_count").GetInt32());
            Assert.True(cappedRoot.GetProperty("truncated").GetBoolean());

            /*
                And it does NOT announce itself when the window simply holds exactly the cap. Comparing
                the row count to the limit cannot separate those two, so the read over-fetches by one and
                reports what it observed. Two rows, cap of two: full, but nothing beyond it.
            */
            var exact = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 2);
            var exactRoot = JsonDocument.Parse(exact).RootElement;
            Assert.Equal(2, exactRoot.GetProperty("run_count").GetInt32());
            Assert.False(exactRoot.GetProperty("truncated").GetBoolean());

            /* ── 5. an out-of-range cap is refused, not silently clamped ── */
            var tooBig = await DarlingMcpDataTools.GetCollectionLog(dataSource, ServerName, 24, 5000);
            Assert.Contains("exceeds maximum of", tooBig, StringComparison.Ordinal);
            Assert.Contains("1000", tooBig, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static DateTime HoursAgo(int hours) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, string collector, DateTime collectionTimeUtc) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector,
            DarlingMcpTestData.Naive(collectionTimeUtc), 100, "SUCCESS", null, 10, 80, 20);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
