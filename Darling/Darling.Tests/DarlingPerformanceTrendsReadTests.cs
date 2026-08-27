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
/// The Performance-Trends siblings (#2484): get_procedure_duration_trend and
/// get_query_store_duration_trend, plus the two-branch empty answer all three siblings now share.
///
/// <para>Three things here are worth more than the round-trip. The procedure trend must read
/// <c>procedure_stats</c> and not <c>query_stats</c>, or it is a second name for its sibling. The Query
/// Store trend must count each runtime interval ONCE, at the hour the work ran, or a busy interval is
/// charged again to every cycle that re-fetched it. And the execution rate must survive being below one
/// per second, which the shipped integer field does not.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingPerformanceTrendsReadTests
{
    private const int ServerId = -949555;
    private const string ServerName = "performance-trends-read";
    private const string Db = "AppDb";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheSiblings_ReadTheirOwnSources_AndSayWhichNothingTheyFound_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live performance-trends test.");

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

            /* ── never sampled: all three siblings refuse to look like a quiet server ── */
            var neverProcs = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, ServerName)).RootElement;
            Assert.Equal("unavailable", neverProcs.GetProperty("status").GetString());
            var neverProcsText = neverProcs.GetProperty("message").GetString()!;
            Assert.Contains("EVER", neverProcsText, StringComparison.Ordinal);
            Assert.Contains("NOT a quiet server", neverProcsText, StringComparison.Ordinal);
            Assert.DoesNotContain("widen", neverProcsText, StringComparison.OrdinalIgnoreCase);

            var neverStore = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, ServerName)).RootElement;
            Assert.Equal("unavailable", neverStore.GetProperty("status").GetString());

            /*
                Query Store has a cause of emptiness the other two do not: it can simply be OFF on every
                database. Naming the collector first would send someone to the wrong place.
            */
            Assert.Contains("Query Store may be OFF", neverStore.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* get_query_duration_trend used to answer "unavailable" for BOTH kinds of empty. */
            var neverQueries = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName)).RootElement;
            Assert.Equal("unavailable", neverQueries.GetProperty("status").GetString());
            Assert.Contains("EVER", neverQueries.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* ── sampled, but outside the window: a quiet window, and widening IS the move ── */
            await SeedProcedureAsync(connection, ct, HoursAgo(48), executions: 10, elapsedUs: 5_000_000);
            await SeedQueryAsync(connection, ct, HoursAgo(48), executions: 10, elapsedUs: 5_000_000);

            var quietProcs = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, ServerName, 1)).RootElement;
            Assert.Equal("empty", quietProcs.GetProperty("status").GetString());
            var quietProcsText = quietProcs.GetProperty("message").GetString()!;
            Assert.Contains("widen", quietProcsText, StringComparison.Ordinal);

            /* Same zero points as the branch above, and it must NOT reach for the same word. */
            Assert.DoesNotContain("EVER", quietProcsText, StringComparison.Ordinal);
            Assert.NotEqual(neverProcsText, quietProcsText);

            var quietQueries = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, 1)).RootElement;
            Assert.Equal("empty", quietQueries.GetProperty("status").GetString());
            Assert.Contains("widen", quietQueries.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /*
                ── the procedure series, at a rate BELOW one execution per second ──
                Two snapshots five minutes apart, two executions between them: 0.0067/sec. The shipped
                integer field truncates that to 0, which reads as an idle server; the double does not.
                This is the whole reason executions_per_second exists.
            */
            await SeedProcedureAsync(connection, ct, MinutesAgo(20), executions: 0, elapsedUs: 0);
            await SeedProcedureAsync(connection, ct, MinutesAgo(15), executions: 2, elapsedUs: 600_000);

            var procs = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, ServerName, 4)).RootElement;
            var procTrend = procs.GetProperty("trend");
            Assert.Equal(2, procTrend.GetArrayLength());

            var second = procTrend[1];
            Assert.True(second.GetProperty("value").GetDouble() > 0, "elapsed ms/sec must be a real rate");
            Assert.Equal(0, second.GetProperty("execution_count").GetInt64());
            Assert.True(
                second.GetProperty("executions_per_second").GetDouble() > 0,
                "executions_per_second must survive a rate below 1/sec that execution_count truncates to zero");

            /*
                ── the Query Store series: each interval counted ONCE, at the hour the work ran ──
                Two runtime intervals, each fetched twice while it was open, the second fetch carrying the
                higher cumulative count. Charging every fetch to its collection time would give four points
                and double the work; the dedup + placement gives two, at the interval starts.
            */
            /* One base instant for both, so the two interval starts are EXACTLY an hour apart. Two
               separate UtcNow reads truncated to the second can land 3599 apart and quietly break the
               rate assertion below for a reason that has nothing to do with the read. */
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var intervalA = baseNow.AddHours(-3);
            var intervalB = baseNow.AddHours(-2);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(150), intervalId: 41, intervalStart: intervalA, executions: 10, avgDurationUs: 2000);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(145), intervalId: 41, intervalStart: intervalA, executions: 40, avgDurationUs: 2000);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(90), intervalId: 42, intervalStart: intervalB, executions: 5, avgDurationUs: 3000);
            await SeedQueryStoreAsync(connection, ct, collectionTime: MinutesAgo(85), intervalId: 42, intervalStart: intervalB, executions: 25, avgDurationUs: 3000);

            var store = JsonDocument.Parse(
                await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, ServerName, 6)).RootElement;
            var storeTrend = store.GetProperty("trend");

            /* Four rows in, two points out — one per interval, not one per fetch. */
            Assert.Equal(2, storeTrend.GetArrayLength());
            Assert.StartsWith(intervalA.ToString("o")[..16], storeTrend[0].GetProperty("time").GetString()!, StringComparison.Ordinal);
            Assert.StartsWith(intervalB.ToString("o")[..16], storeTrend[1].GetProperty("time").GetString()!, StringComparison.Ordinal);

            /*
                The surviving snapshot is the FINAL one (25 executions over the 3600 seconds between the two
                interval starts), not the first (5) and not their sum (30). A dedup keeping the wrong row
                would still return two points and would still look right.
            */
            Assert.Equal(
                25d / 3600d,
                storeTrend[1].GetProperty("executions_per_second").GetDouble(),
                6);

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

    private static async Task SeedProcedureAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long executions, long elapsedUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name,
     delta_execution_count, delta_elapsed_time, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            Db, "dbo", "usp_Trend", executions, elapsedUs, elapsedUs / 2);

    private static async Task SeedQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long executions, long elapsedUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     delta_execution_count, delta_elapsed_time, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc), ServerId, ServerName,
            Db, "0xTRENDSIB", executions, elapsedUs, elapsedUs / 2);

    private static async Task SeedQueryStoreAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, long intervalId,
        DateTime intervalStart, long executions, long avgDurationUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            Db, 7L, 9L, "Regular", executions, avgDurationUs, intervalId,
            DarlingMcpTestData.Naive(intervalStart));

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM procedure_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_store_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
