/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #8812: <c>get_query_trend</c> and <c>get_query_fleet_summary</c> keep to the 32,768-byte response budget at the arguments they
/// accept, not only at their defaults, and say what they cut. The trend's automatic width is sized to the budget, a width the caller
/// names is cut to the newest whole points that fit (the oldest are left off), and the fleet summary ends at the last whole entry that fits, heaviest CPU first,
/// under its own <c>budget_truncated</c> and <c>budget_note</c> (the limit's <c>truncated</c> and <c>note</c> stay the limit's).
/// </summary>
public sealed class QueryReadsResponseBudgetLiveTests
{
    private const int TrendServerId = -881201;
    private const string TrendServerName = "a8812-trend";
    private const string Db = "BudgetDb";
    private const string TrendHash = "0xQ8812T";
    private const string FleetHash = "0xQ8812F";
    private const int FleetServers = 150;

    private static int Bytes(string json) => Encoding.UTF8.GetByteCount(json);

    private static DateTime WindowEnd()
    {
        var now = DateTime.UtcNow;
        var onTheHour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
        return onTheHour.AddHours(-1);
    }

    private static string Stamp(DateTime t) => t.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture);

    private static string AsOf(DateTime end) => end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    [Fact]
    public async Task QueryTrend_AWeekAtTheDefaultWidth_FitsTheBudget_AndANamedWidthOverItIsCutAndSaysSo()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query trend response budget test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, TrendServerId, TrendServerName, ct);

        var bodySucceeded = false;
        try
        {
            var end = WindowEnd();
            var weekStart = end.AddHours(-168);
            /* A collection every five minutes for the week, one handle, one plan, each a minute past its five-minute mark: none is stamped
               on a window's start, which belongs to the window before, so the week holds all 2,016 and a day all 288. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, min_dop, max_dop,
                      query_plan_hash, sample_interval_seconds)
                  SELECT $1 + g, $2 + g * INTERVAL '5 minutes', $3, $4, $5, $6, '0xH1', 10, 10000, 20000, 100, 100, 1, 1, '0xPLAN8812', 300
                  FROM generate_series(0, $7) AS g",
                CollectionIdGenerator.Next(), weekStart.AddMinutes(1), TrendServerId, TrendServerName, Db, TrendHash, (168 * 12) - 1);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* The default width over a week: the old target of 200 points put 169 hourly points on the wire, about 56 KB. */
            var whole = await DarlingMcpTrendTools.GetQueryTrend(postgres, TrendHash, Db, TrendServerName, 168, AsOf(end), cancellationToken: ct);
            using (var doc = JsonDocument.Parse(whole))
            {
                var root = doc.RootElement;
                Assert.False(root.TryGetProperty("error", out _), whole);
                Assert.True(Bytes(whole) <= McpResponseBudget.DefaultBytes, $"a week at the default width answered {Bytes(whole):N0} bytes");
                Assert.False(root.TryGetProperty("budget_truncated", out var cutFlag) && cutFlag.ValueKind == JsonValueKind.True, whole);
                var points = root.GetProperty("trend").EnumerateArray().ToArray();
                Assert.Equal(points.Length, root.GetProperty("data_points").GetInt32());
                Assert.True(points.Length <= TrendBuckets.QueryTrendMcpAutoPoints, $"{points.Length} points");
                Assert.Equal(168 * 12, points.Sum(p => p.GetProperty("collection_count").GetInt32()));
            }

            /* A named width the cap allows (289 points over a day at 5 minutes) is cut to the budget: the OLDEST points are left off and the newest kept. */
            var cutJson = await DarlingMcpTrendTools.GetQueryTrend(postgres, TrendHash, Db, TrendServerName, 24, AsOf(end), 5, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(cutJson))
            {
                var root = doc.RootElement;
                Assert.False(root.TryGetProperty("error", out _), cutJson);
                Assert.True(Bytes(cutJson) <= McpResponseBudget.DefaultBytes, $"a named 5-minute width answered {Bytes(cutJson):N0} bytes");
                Assert.True(root.GetProperty("budget_truncated").GetBoolean(), cutJson);
                Assert.False(root.GetProperty("window_truncated").GetBoolean());
                var points = root.GetProperty("trend").EnumerateArray().ToArray();
                Assert.Equal(points.Length, root.GetProperty("data_points").GetInt32());
                Assert.InRange(points.Length, 20, 288);
                /* The newest point of the window (the bucket of the last collection, five minutes before its end) is in the reply, and the window's oldest is not. */
                Assert.Equal(Stamp(end.AddMinutes(-5)), points[^1].GetProperty("collection_time").GetString()![..19]);
                var firstStamp = points[0].GetProperty("collection_time").GetString()![..19];
                Assert.NotEqual(Stamp(end.AddHours(-24)), firstStamp);

                var note = root.GetProperty("budget_note").GetString()!;
                Assert.Contains("response budget", note, StringComparison.Ordinal);
                Assert.Contains($"only {points.Length:#,0} fit", note, StringComparison.Ordinal);
                Assert.Contains($"first point returned is at {firstStamp}Z", note, StringComparison.Ordinal);

                /* The width the note names reads the whole day inside the points that fit. */
                var named = int.Parse(Regex.Match(note, @"bucket_minutes (\d+)").Groups[1].Value, CultureInfo.InvariantCulture);
                Assert.True(TrendBuckets.PointsFor(24 * 60, named, 1) <= points.Length, note);
                var fitted = await DarlingMcpTrendTools.GetQueryTrend(postgres, TrendHash, Db, TrendServerName, 24, AsOf(end), named, cancellationToken: ct);
                using var fittedDoc = JsonDocument.Parse(fitted);
                Assert.False(fittedDoc.RootElement.GetProperty("budget_truncated").GetBoolean(), fitted);
                Assert.Equal(24 * 12, fittedDoc.RootElement.GetProperty("trend").EnumerateArray().Sum(p => p.GetProperty("collection_count").GetInt32()));
            }

            /* Following the note's as_of reads the points before the first one returned, and repeating it reaches the window's oldest point. */
            var reachedOldest = false;
            var asOfToRead = AsOf(end);
            for (var page = 0; page < 12 && !reachedOldest; page++)
            {
                var pageJson = await DarlingMcpTrendTools.GetQueryTrend(postgres, TrendHash, Db, TrendServerName, 24, asOfToRead, 5, cancellationToken: ct);
                using var pageDoc = JsonDocument.Parse(pageJson);
                var pagePoints = pageDoc.RootElement.GetProperty("trend").EnumerateArray().ToArray();
                if (pagePoints.Any(p => p.GetProperty("collection_time").GetString()![..19] == Stamp(end.AddHours(-24))))
                {
                    reachedOldest = true;
                    break;
                }

                var next = Regex.Match(pageDoc.RootElement.GetProperty("budget_note").GetString()!, @"set as_of to (\S+Z) to read the points before it");
                Assert.True(next.Success, pageJson);
                asOfToRead = next.Groups[1].Value;
            }

            Assert.True(reachedOldest, "following the note's as_of never reached the window's oldest point");

            /* The web viewer's mirror passes no byte budget and keeps every point the cap allows. */
            var chart = await DarlingMcpTrendTools.GetQueryTrend(
                postgres, TrendHash, Db, TrendServerName, 24, AsOf(end), 5, TrendBudget.Mcp(TrendBuckets.QueryTrendMaxPoints), null, ct);
            using (var doc = JsonDocument.Parse(chart))
            {
                Assert.False(doc.RootElement.TryGetProperty("budget_truncated", out _), chart);
                Assert.InRange(doc.RootElement.GetProperty("trend").GetArrayLength(), 288, 290);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>One entry per server, the statement running on every one: server 001 is the lightest, server 150 the heaviest. Returns the window's end.</summary>
    private static async Task<DateTime> SeedFleetAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        for (var i = 1; i <= FleetServers; i++)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, -881300 - i, $"a8812-fleet-{i:D3}", ct);
        }

        var end = WindowEnd();
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
              SELECT $1 + g, $2, -881300 - g, 'a8812-fleet-' || lpad(g::text, 3, '0'), 'BudgetDb', $3, '0xH1', 10, g * 1000000, g * 2000000, 100, 100, 3600
              FROM generate_series(1, $4) AS g",
            CollectionIdGenerator.Next(), end.AddHours(-3), FleetHash, FleetServers);
        return end;
    }

    [Fact]
    public async Task QueryFleetSummary_AtTheWidestLimit_FitsTheBudget_HeaviestFirst_AndNamesItsOwnCut()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet summary response budget test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            var end = await SeedFleetAsync(connection, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, FleetHash, 24, AsOf(end), 200, ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.False(root.TryGetProperty("error", out _), json);
            Assert.True(Bytes(json) <= McpResponseBudget.DefaultBytes, $"limit 200 answered {Bytes(json):N0} bytes");

            Assert.Equal(FleetServers, root.GetProperty("server_database_pairs").GetInt32());
            /* The limit did not cut: truncated and note keep saying so, and the budget names its own cut. */
            Assert.False(root.GetProperty("truncated").GetBoolean(), json);
            Assert.Equal(JsonValueKind.Null, root.GetProperty("note").ValueKind);
            Assert.True(root.GetProperty("budget_truncated").GetBoolean(), json);
            var servers = root.GetProperty("servers").EnumerateArray().ToArray();
            Assert.InRange(servers.Length, 20, FleetServers - 1);
            Assert.Equal(servers.Length, root.GetProperty("rows_returned").GetInt32());
            Assert.Equal(servers.Length, root.GetProperty("servers_returned").GetInt32());

            var note = root.GetProperty("budget_note").GetString()!;
            Assert.Contains("response budget", note, StringComparison.Ordinal);
            Assert.Contains($"only {servers.Length:#,0} fit", note, StringComparison.Ordinal);
            Assert.Contains($"{FleetServers - servers.Length:#,0} were left out", note, StringComparison.Ordinal);

            /* Heaviest by CPU first, and the heaviest server is kept: the cut takes the lightest. */
            var cpu = servers.Select(s => s.GetProperty("cpu_ms").GetDouble()).ToArray();
            Assert.Equal(cpu.OrderByDescending(c => c).ToArray(), cpu);
            Assert.Equal($"a8812-fleet-{FleetServers:D3}", servers[0].GetProperty("server_name").GetString());

            /* The default limit already fits, and says nothing was cut for the budget. */
            var small = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, FleetHash, 24, AsOf(end), 50, ct);
            using var smallDoc = JsonDocument.Parse(small);
            Assert.False(smallDoc.RootElement.GetProperty("budget_truncated").GetBoolean(), small);
            Assert.Equal(50, smallDoc.RootElement.GetProperty("rows_returned").GetInt32());
            Assert.True(smallDoc.RootElement.GetProperty("truncated").GetBoolean());

            /* The web viewer's mirror passes no byte budget and keeps every entry the limit allows. */
            var web = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, FleetHash, 24, AsOf(end), 200, null, ct);
            using var webDoc = JsonDocument.Parse(web);
            Assert.False(webDoc.RootElement.TryGetProperty("budget_truncated", out _), web);
            Assert.Equal(FleetServers, webDoc.RootElement.GetProperty("rows_returned").GetInt32());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task QueryFleetSummary_WhenTheLimitAndTheBudgetBothCut_TheLimitNoteCountsTheRowsListed()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet summary response budget test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var end = await SeedFleetAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* 150 pairs, limit 130: the limit leaves 20 off, and the budget keeps fewer than the limit admitted. */
            const int limit = 130;
            var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, FleetHash, 24, AsOf(end), limit, ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.False(root.TryGetProperty("error", out _), json);
            Assert.True(root.GetProperty("truncated").GetBoolean(), json);
            Assert.True(root.GetProperty("budget_truncated").GetBoolean(), json);
            var servers = root.GetProperty("servers").EnumerateArray().ToArray();
            Assert.InRange(servers.Length, 20, limit - 1);
            Assert.Equal(servers.Length, root.GetProperty("rows_returned").GetInt32());

            /* The limit's note names the rows the reply lists, not the rows the limit admitted before the budget cut. */
            var note = root.GetProperty("note").GetString()!;
            Assert.Contains($"limit {limit} cut the list: {servers.Length} of {FleetServers} server, database and source rows are shown", note, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
