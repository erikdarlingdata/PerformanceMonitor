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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5245 (part of #5244), PR1: the database filter on the readers behind get_query_store_regressions and
/// get_query_heatmap. Each tool's internal overload takes a <see cref="DatabaseFilter"/>, so an overload call with
/// [A, B] over a seed of A, B and C must return only A's and B's rows, one name must read exactly as the public
/// (one-name) method does, and every one-name consumer on the empty answers must be list-aware: the echoed
/// <c>database_name</c> field, the three filtered empty answers of the regressions read, and the covered-claim sentence
/// of the heatmap's idle-grid answer. Both reads are one tier (<c>query_store_stats</c> and <c>query_stats</c>).
/// The public methods still pass one name, so no MCP schema, dispatch or tools/list byte changes.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreReadsDatabaseFilterLiveTests
{
    private const int ServerId = -950456;
    private const string ServerName = "qs-reads-database-filter";

    private const string A = "QsFilterA";
    private const string B = "QsFilterB";
    private const string C = "QsFilterC";
    // Both sides collected, nothing regressed.
    private const string QuietH = "QsFilterQuietH";
    private const string QuietI = "QsFilterQuietI";
    // Baseline captures only.
    private const string BaseOnlyF = "QsFilterBaseOnlyF";
    private const string BaseOnlyG = "QsFilterBaseOnlyG";
    // Recent captures only.
    private const string RecentOnlyD = "QsFilterRecentOnlyD";
    private const string RecentOnlyE = "QsFilterRecentOnlyE";
    // Heatmap: databases whose every capture ran nothing.
    private const string IdleX = "QsFilterIdleX";
    private const string IdleY = "QsFilterIdleY";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime FloorToHour(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerHour), value.Kind);

    private static DatabaseFilter Of(params string[] names) => DatabaseFilter.Of(names);

    private static List<string> RegressionDatabases(JsonElement root) =>
        root.GetProperty("regressions").EnumerateArray().Select(r => r.GetProperty("database_name").GetString()!).OrderBy(x => x, StringComparer.Ordinal).ToList();

    private static List<string> HeatmapHashes(JsonElement root) =>
        root.GetProperty("cells").EnumerateArray().Select(c => c.GetProperty("top_query_hash").GetString()!).OrderBy(x => x, StringComparer.Ordinal).ToList();

    private static Task<string> Regressions(NpgsqlDataSource postgres, DatabaseFilter filter) =>
        DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(
            postgres, ServerName, 24, filter, 30, false, null, null, CancellationToken.None);

    private static Task<string> Heatmap(NpgsqlDataSource postgres, DatabaseFilter filter) =>
        DarlingMcpQueryHeatmapTools.GetQueryHeatmap(
            postgres, ServerName, 24, null, filter, 5, 100, null, false, null, CancellationToken.None);

    [Fact]
    public async Task TheRegressionsOverload_WithAListOfDatabases_ReturnsOnlyThoseDatabases_AndEveryOneNameConsumerIsListAware_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database filter test.");

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
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            // A, B, C each carry one query that got 4x slower in the window.
            long queryId = 10;
            foreach (var db in new[] { A, B, C })
            {
                queryId++;
                await SeedQsAsync(connection, ct, db, baseNow.AddHours(-40), 100, 1000, queryId, intervalId: 2);
                await SeedQsAsync(connection, ct, db, baseNow.AddMinutes(-30), 200, 4000, queryId, intervalId: 1);
            }

            // H and I: both sides collected, flat.
            foreach (var db in new[] { QuietH, QuietI })
            {
                await SeedQsAsync(connection, ct, db, baseNow.AddHours(-40), 100, 1000, 21, intervalId: 2);
                await SeedQsAsync(connection, ct, db, baseNow.AddMinutes(-30), 100, 1000, 21, intervalId: 1);
            }

            foreach (var db in new[] { BaseOnlyF, BaseOnlyG })
                await SeedQsAsync(connection, ct, db, baseNow.AddHours(-40), 100, 1000, 31, intervalId: 2);

            foreach (var db in new[] { RecentOnlyD, RecentOnlyE })
                await SeedQsAsync(connection, ct, db, baseNow.AddMinutes(-30), 100, 1000, 41, intervalId: 1);

            // The rows: [A, B] over A, B and C is A and B only; one name is that one database; no filter is all three.
            var all = Root(await Regressions(postgres, DatabaseFilter.All));
            Assert.Equal([A, B, C], RegressionDatabases(all));
            Assert.Equal(JsonValueKind.Null, all.GetProperty("database_name").ValueKind);

            var two = Root(await Regressions(postgres, Of(A, B)));
            Assert.Equal([A, B], RegressionDatabases(two));
            Assert.Equal(DatabaseFilter.ManyDatabasesDescription, two.GetProperty("database_name").GetString());

            var reordered = Root(await Regressions(postgres, Of(C, A)));
            Assert.Equal([A, C], RegressionDatabases(reordered));

            // One name is what the public method returns for that name, byte for byte.
            var oneOverload = await Regressions(postgres, DatabaseFilter.One(B));
            var onePublic = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, B);
            Assert.Equal(StripWindowEcho(onePublic), StripWindowEcho(oneOverload));
            Assert.Equal([B], RegressionDatabases(Root(oneOverload)));
            Assert.Equal(B, Root(oneOverload).GetProperty("database_name").GetString());

            // The empty answers (EmptyAsync): each names the chosen databases, in the plural, and keeps its status.
            var neither = Root(await Regressions(postgres, Of("QsNoSuchOne", "QsNoSuchTwo")));
            Assert.Equal("empty", neither.GetProperty("status").GetString());
            var neitherText = neither.GetProperty("message").GetString()!;
            Assert.Contains("matched the chosen databases, so the filter matched nothing and this is NOT the all-clear: no query in those databases was compared. Check the database names,", neitherText, StringComparison.Ordinal);

            var noBaseline = Root(await Regressions(postgres, Of(RecentOnlyD, RecentOnlyE)));
            Assert.Equal("unavailable", noBaseline.GetProperty("status").GetString());
            var noBaselineText = noBaseline.GetProperty("message").GetString()!;
            Assert.Contains("no Query Store capture of the chosen databases in the 7-day baseline window", noBaselineText, StringComparison.Ordinal);
            Assert.Contains("no baseline for those databases to compare against and no regression of their queries", noBaselineText, StringComparison.Ordinal);
            Assert.Contains("Either those databases' whole collected history falls inside the last 24 hour(s), or they have none older", noBaselineText, StringComparison.Ordinal);

            var noRecent = Root(await Regressions(postgres, Of(BaseOnlyF, BaseOnlyG)));
            Assert.Equal("empty", noRecent.GetProperty("status").GetString());
            var noRecentText = noRecent.GetProperty("message").GetString()!;
            Assert.Contains("has Query Store history of the chosen databases from before this window but nothing of them collected IN the last 24 hour(s), so those databases' recent side is missing", noRecentText, StringComparison.Ordinal);

            // Both sides collected for every chosen database: the filtered answers fall through to the all-clear's own text.
            var quiet = Root(await Regressions(postgres, Of(QuietH, QuietI)));
            Assert.Equal("empty", quiet.GetProperty("status").GetString());
            Assert.DoesNotContain("NOT the all-clear", quiet.GetProperty("message").GetString()!, StringComparison.Ordinal);

            // One name: the three filtered answers read exactly as they did (the public method, which passes one name).
            var oneNeither = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, "QsNoSuchOne"));
            Assert.Contains("matched database_name 'QsNoSuchOne', so the filter matched nothing and this is NOT the all-clear: no query in that database was compared. Check the database name, or drop", oneNeither.GetProperty("message").GetString()!, StringComparison.Ordinal);
            var oneNoBaseline = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, RecentOnlyD));
            Assert.Equal("unavailable", oneNoBaseline.GetProperty("status").GetString());
            Assert.Contains($"no Query Store capture of database_name '{RecentOnlyD}' in the 7-day baseline window before this window, so there is no baseline for that database to compare against and no regression of its queries", oneNoBaseline.GetProperty("message").GetString()!, StringComparison.Ordinal);
            Assert.Contains("Either that database's whole collected history falls inside the last 24 hour(s), or it has none older", oneNoBaseline.GetProperty("message").GetString()!, StringComparison.Ordinal);
            var oneNoRecent = Root(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, 24, BaseOnlyF));
            Assert.Contains($"has Query Store history of database_name '{BaseOnlyF}' from before this window but nothing of it collected IN the last 24 hour(s), so that database's recent side is missing", oneNoRecent.GetProperty("message").GetString()!, StringComparison.Ordinal);
            // ... and the overload with one name says the same thing.
            Assert.Equal(oneNoRecent.GetProperty("message").GetString(), Root(await Regressions(postgres, DatabaseFilter.One(BaseOnlyF))).GetProperty("message").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheHeatmapOverload_WithAListOfDatabases_ReturnsOnlyThoseDatabases_AndEveryOneNameConsumerIsListAware_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database filter test.");

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
            var t1 = FloorToHour(DateTime.UtcNow.AddHours(-3));

            // One hot query per database, each in its own 5-minute bin so they cannot share a cell.
            await SeedQueryStatsAsync(connection, ct, A, t1, "0xHOTA", 5, 250_000);
            await SeedQueryStatsAsync(connection, ct, B, t1.AddMinutes(10), "0xHOTB", 5, 250_000);
            await SeedQueryStatsAsync(connection, ct, C, t1.AddMinutes(20), "0xHOTC", 5, 250_000);

            // Idle databases: captured, zero executions.
            await SeedQueryStatsAsync(connection, ct, IdleX, t1.AddMinutes(30), "0xIDLEX", 0, 999_000);
            await SeedQueryStatsAsync(connection, ct, IdleY, t1.AddMinutes(30), "0xIDLEY", 0, 999_000);

            var all = Root(await Heatmap(postgres, DatabaseFilter.All));
            Assert.Equal(["0xHOTA", "0xHOTB", "0xHOTC"], HeatmapHashes(all));
            Assert.Equal(JsonValueKind.Null, all.GetProperty("database_name").ValueKind);

            var two = Root(await Heatmap(postgres, Of(A, B)));
            Assert.Equal(["0xHOTA", "0xHOTB"], HeatmapHashes(two));
            Assert.Equal(DatabaseFilter.ManyDatabasesDescription, two.GetProperty("database_name").GetString());

            var oneOverload = await Heatmap(postgres, DatabaseFilter.One(C));
            var onePublic = await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24, database_name: C);
            Assert.Equal(StripWindowEcho(onePublic), StripWindowEcho(oneOverload));
            Assert.Equal(["0xHOTC"], HeatmapHashes(Root(oneOverload)));
            Assert.Equal(C, Root(oneOverload).GetProperty("database_name").GetString());

            // The idle-grid answer: the covered claim names the filter, and the window floor does not speak for it.
            var twoIdle = Root(await Heatmap(postgres, Of(IdleX, IdleY)));
            Assert.Equal("empty", twoIdle.GetProperty("status").GetString());
            var twoIdleText = twoIdle.GetProperty("message").GetString()!;
            Assert.Contains("and so does a filter on the chosen databases matching nothing collected.", twoIdleText, StringComparison.Ordinal);
            Assert.DoesNotContain("database_name filter", twoIdleText, StringComparison.Ordinal);

            var oneIdleOverload = Root(await Heatmap(postgres, DatabaseFilter.One(IdleX)));
            var oneIdlePublic = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24, database_name: IdleX));
            Assert.Contains("and so does a database_name filter matching nothing collected.", oneIdlePublic.GetProperty("message").GetString()!, StringComparison.Ordinal);
            Assert.Equal(oneIdlePublic.GetProperty("message").GetString(), oneIdleOverload.GetProperty("message").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>One name is the same JSON whichever method built it, except for the window instants, which each call
    /// takes from its own clock. Those fields are dropped from both before they are compared.</summary>
    private static string StripWindowEcho(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return RemoveVolatile(doc.RootElement);
    }

    private static string RemoveVolatile(JsonElement root)
    {
        var parts = new List<string>();
        foreach (var property in root.EnumerateObject())
        {
            if (property.Name is "recent_window_start" or "recent_window_end" or "baseline_start" or "baseline_end" or "window_start" or "window_end")
                continue;
            parts.Add($"\"{property.Name}\":{property.Value.GetRawText()}");
        }

        return "{" + string.Join(",", parts) + "}";
    }

    private static async Task SeedQsAsync(
        NpgsqlConnection connection, CancellationToken ct, string database, DateTime collectionTime, long executions,
        long avgUs, long queryId, long intervalId) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            database, queryId, 9L, "Regular", executions, avgUs, avgUs, 100L, intervalId,
            "SELECT * FROM dbo.Widgets", DarlingMcpTestData.Naive(collectionTime));

    private static async Task SeedQueryStatsAsync(
        NpgsqlConnection connection, CancellationToken ct, string database, DateTime collectionTime, string queryHash,
        long deltaExec, long deltaElapsed) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     sample_interval_seconds, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            database, queryHash, 60, deltaExec, 0L, deltaElapsed, 0L, 0L, "SELECT * FROM dbo.Widgets");

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_store_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
