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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #8063: Darling's <c>get_query_trend</c> buckets its raw tier the way Lite's does (#7907) and the sibling trends do
/// (#3897). The width is sized to the shared <see cref="TrendBuckets.McpPointBudget"/> unless <c>bucket_minutes</c>
/// names one, each point sums the collections it holds and carries <c>collection_count</c>, the first point is stamped
/// at the window's start, <c>effective_start</c> stays the first collection the store held, a named width over
/// <see cref="TrendBuckets.QueryTrendMaxPoints"/> is refused, and the hourly tier takes the same width rule as
/// <c>get_query_duration_trend</c>, so a 90-day window is daily points. The collections are one query under one
/// handle at a steady 10 executions a minute, so every sum and every average is known in advance.
/// </summary>
public sealed class QueryTrendBucketingLiveTests
{
    private const int ServerId = -806301;
    private const string ServerName = "a8063-buckets";
    private const string Db = "TrendDb";
    private const string QueryHash = "0xQ8063";
    private const string PlanHash = "0xPLAN8063";
    private const long ExecutionsPerCollection = 10;
    private const long WorkerUsPerCollection = 10_000;
    private const long ElapsedUsPerCollection = 20_000;
    private const string Stamp = "yyyy-MM-ddTHH:mm:ss";

    [Fact]
    public async Task ADayOfMinuteCollections_IsBucketedWithinTheBudget_AndEachPointSumsItsCollections()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query trend bucketing test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            /* The window ends on the ladder's 10-minute grid, so every bucket is whole and the first sits exactly at the start. */
            var end = WindowEnd(minuteModTen: 0);
            var start = end.AddHours(-24);
            await SeedMinuteCollectionsAsync(connection, ct, start, 1440);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpTrendTools.GetQueryTrend(postgres, QueryHash, Db, ServerName, 24, AsOf(end), cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.False(root.TryGetProperty("error", out _), json);
            Assert.Equal("raw", root.GetProperty("source").GetString());

            /* Red on dev: the raw tier writes bucket_minutes null and one point per collection. */
            Assert.True(root.GetProperty("bucket_minutes").ValueKind == JsonValueKind.Number, "bucket_minutes is not a number: " + root.GetProperty("bucket_minutes"));
            var width = root.GetProperty("bucket_minutes").GetInt32();
            Assert.True(width > 1, $"a day of minute collections was served at {width}-minute points");
            Assert.Equal(TrendBuckets.Word(width), root.GetProperty("bucket").GetString());

            var points = root.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(points.Length, root.GetProperty("data_points").GetInt32());
            Assert.InRange(points.Length, 1, TrendBuckets.McpPointBudget);

            foreach (var point in points)
            {
                Assert.True(point.TryGetProperty("collection_count", out var count), "a point has no collection_count: " + point);
                var collections = count.GetInt32();
                Assert.InRange(collections, 1, width);
                Assert.Equal(collections * ExecutionsPerCollection, point.GetProperty("execution_count").GetInt64());
                Assert.Equal(1.0, point.GetProperty("avg_cpu_ms").GetDouble());
                Assert.Equal(2.0, point.GetProperty("avg_elapsed_ms").GetDouble());
                Assert.Equal(collections * 100, point.GetProperty("logical_reads").GetInt64());
                Assert.Equal(1, point.GetProperty("min_dop").GetInt32());
                Assert.Equal(1, point.GetProperty("max_dop").GetInt32());
                Assert.Equal(PlanHash, point.GetProperty("query_plan_hash").GetString());
                Assert.Equal(1, point.GetProperty("plan_count").GetInt32());
            }

            Assert.Equal(1440, points.Sum(p => p.GetProperty("collection_count").GetInt32()));
            Assert.Equal(1440 * ExecutionsPerCollection, points.Sum(p => p.GetProperty("execution_count").GetInt64()));

            /* Stamped at the bucket's start: the first at the window's start, the second one width on. */
            Assert.Equal(start.ToString(Stamp), points[0].GetProperty("collection_time").GetString()![..19]);
            Assert.Equal(start.AddMinutes(width).ToString(Stamp), points[1].GetProperty("collection_time").GetString()![..19]);
            Assert.Equal(start.ToString(Stamp), root.GetProperty("effective_start").GetString()![..19]);
            Assert.False(root.GetProperty("window_truncated").GetBoolean());

            var note = root.GetProperty("aggregate_note").GetString();
            Assert.NotNull(note);
            Assert.Contains("collection_count", note, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The window starts off the bucket grid: the first bucket is clamped to the window's start (as every bucketed
    /// trend's is, <see cref="TrendBuckets.BucketStart"/>), the second sits on the grid, and <c>effective_start</c> is
    /// the first collection, not the first bucket's clamped stamp.
    /// </summary>
    [Fact]
    public async Task AWindowStartOffTheGrid_StampsTheFirstPointAtTheWindowStart_AndTheSecondOnTheGrid()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query trend bucketing test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            /* The window ends at :x7, so it starts at :x7 too, three minutes before the 10-minute grid's next line. The
               first collection is a minute after the start. */
            var end = WindowEnd(minuteModTen: 7);
            var start = end.AddHours(-24);
            var firstCollection = start.AddMinutes(1);
            await SeedMinuteCollectionsAsync(connection, ct, firstCollection, 30);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpTrendTools.GetQueryTrend(postgres, QueryHash, Db, ServerName, 24, AsOf(end), cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.False(root.TryGetProperty("error", out _), json);

            /* Red on dev: bucket_minutes null. 24 hours at the shared 200-point target is the ladder's 10-minute rung. */
            Assert.True(root.GetProperty("bucket_minutes").ValueKind == JsonValueKind.Number, "bucket_minutes is not a number: " + root.GetProperty("bucket_minutes"));
            Assert.Equal(10, root.GetProperty("bucket_minutes").GetInt32());

            var points = root.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(4, points.Length);

            /* :x8 and :x9 fall in the grid bucket that begins before the window, so that bucket is stamped at the start. */
            Assert.Equal(start.ToString(Stamp), points[0].GetProperty("collection_time").GetString()![..19]);
            Assert.Equal(2, points[0].GetProperty("collection_count").GetInt32());
            Assert.Equal(2 * ExecutionsPerCollection, points[0].GetProperty("execution_count").GetInt64());

            var grid = TrendBuckets.BucketStart(start.AddMinutes(3), 10, start);
            Assert.Equal(start.AddMinutes(3), grid);
            Assert.Equal(grid.ToString(Stamp), points[1].GetProperty("collection_time").GetString()![..19]);
            Assert.Equal(10, points[1].GetProperty("collection_count").GetInt32());
            Assert.Equal(grid.AddMinutes(10).ToString(Stamp), points[2].GetProperty("collection_time").GetString()![..19]);
            Assert.Equal(10, points[2].GetProperty("collection_count").GetInt32());
            Assert.Equal(grid.AddMinutes(20).ToString(Stamp), points[3].GetProperty("collection_time").GetString()![..19]);
            Assert.Equal(8, points[3].GetProperty("collection_count").GetInt32());

            /* The coverage is the first collection the store held, a minute after the clamped first stamp. */
            Assert.Equal(firstCollection.ToString(Stamp), root.GetProperty("effective_start").GetString()![..19]);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A named width is served, and one that would put more points on the wire than
    /// <see cref="TrendBuckets.QueryTrendMaxPoints"/> is refused naming <c>bucket_minutes</c>. The parameter is
    /// reached by reflection so this builds against a tool without it.
    /// </summary>
    [Fact]
    public async Task ANamedWidth_IsServed_AndOneOverTheCapIsRefused()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query trend bucketing test.");
        var ct = TestContext.Current.CancellationToken;

        var method = typeof(DarlingMcpTrendTools).GetMethod(nameof(DarlingMcpTrendTools.GetQueryTrend), BindingFlags.Public | BindingFlags.Static)!;
        Assert.True(method.GetParameters().Any(p => p.Name == "bucket_minutes"), "get_query_trend has no bucket_minutes parameter");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            var end = WindowEnd(minuteModTen: 0);
            var start = end.AddHours(-24);
            await SeedMinuteCollectionsAsync(connection, ct, start, 1440);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* Hourly points over the day: 24 when the window starts on the hour, 25 when its first hour is cut. */
            var hourly = JsonDocument.Parse(await InvokeAsync(method, postgres, 24, AsOf(end), 60, ct)).RootElement;
            Assert.False(hourly.TryGetProperty("error", out _), hourly.ToString());
            Assert.Equal(60, hourly.GetProperty("bucket_minutes").GetInt32());
            Assert.Equal("1 hour", hourly.GetProperty("bucket").GetString());
            var points = hourly.GetProperty("trend").EnumerateArray().ToArray();
            Assert.InRange(points.Length, 24, 25);
            Assert.Equal(1440, points.Sum(p => p.GetProperty("collection_count").GetInt32()));
            Assert.Equal(start.ToString(Stamp), points[0].GetProperty("collection_time").GetString()![..19]);
            Assert.EndsWith("The width is the bucket_minutes you passed.", hourly.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);

            /* One-minute points over four days are about 5,760, over the 750-point cap. */
            var refusedJson = await InvokeAsync(method, postgres, 96, AsOf(end), 1, ct);
            var refused = JsonDocument.Parse(refusedJson).RootElement;
            Assert.Equal("bucket_minutes", refused.GetProperty("hints").GetProperty("parameter").GetString());
            Assert.Contains($"{TrendBuckets.QueryTrendMaxPoints}-point cap", refusedJson, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The hourly tier takes the width the way <c>get_query_duration_trend</c> does: the automatic width is the ladder's
    /// pick for the shared 200-point target, raised to at least the rollup's hour, so a 90-day window is 12-hour points
    /// (181 of them) gathering the rollup's hours, each stamped at its bucket's start. The rollup does not count
    /// collections, so <c>collection_count</c> is null there, as the columns the rollup does not carry are.
    /// </summary>
    [Fact]
    public async Task TheHourlyTier_ServesANinetyDayWindow_InTwelveHourBucketsOfTheRollupsHours()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly query trend bucketing test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The hourly tier needs TimescaleDB: it is the rollup's continuous aggregate.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            /* Three whole days of one collection an hour, ten days back: past the raw tier's four days, inside the 90-day reach. */
            var day0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-10), DateTimeKind.Unspecified);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, min_dop, max_dop,
                      query_plan_hash, sample_interval_seconds)
                  SELECT $1 + g, $2 + g * INTERVAL '1 hour', $3, $4, $5, $6, '0xH1', $7, $8, $9, 100, 100, 1, 1, $10, 3600
                  FROM generate_series(0, 71) AS g",
                CollectionIdGenerator.Next(), day0.AddMinutes(5), ServerId, ServerName, Db, QueryHash,
                ExecutionsPerCollection, WorkerUsPerCollection, ElapsedUsPerCollection, PlanHash);

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await using (var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStatsHourlyView}'::regclass, $1::timestamp, $2::timestamp)", connection))
            {
                refresh.Parameters.AddWithValue(day0);
                refresh.Parameters.AddWithValue(day0.AddDays(4));
                await refresh.ExecuteNonQueryAsync(ct);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpTrendTools.GetQueryTrend(postgres, QueryHash, Db, ServerName, WebReadReach.RollupTrendHours, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.False(root.TryGetProperty("error", out _), json);
            Assert.Equal("hourly", root.GetProperty("source").GetString());

            /* Red on dev: the hourly tier answers one point per hour (bucket_minutes 60, 72 points) whatever the window. The
               width is the one the sibling trends resolve for 90 hours times 24 at the shared target: the 720-minute rung. */
            var width = TrendBuckets.OnHourlyTier(null, TrendBuckets.AutoMinutes(WebReadReach.RollupTrendHours * 60, 1, TrendBuckets.McpPointBudget));
            Assert.Equal(720, width);
            Assert.Equal(width, root.GetProperty("bucket_minutes").GetInt32());
            Assert.Equal("12 hours", root.GetProperty("bucket").GetString());

            var points = root.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(6, points.Length);
            Assert.Equal(6, root.GetProperty("data_points").GetInt32());
            for (var half = 0; half < 6; half++)
            {
                var point = points[half];
                Assert.Equal(day0.AddHours(12 * half).ToString(Stamp), point.GetProperty("collection_time").GetString()![..19]);
                Assert.Equal(12 * ExecutionsPerCollection, point.GetProperty("execution_count").GetInt64());
                Assert.Equal(12 * WorkerUsPerCollection / 1000.0, point.GetProperty("cpu_ms").GetDouble());
                Assert.Equal(1.0, point.GetProperty("avg_cpu_ms").GetDouble());
                Assert.True(point.TryGetProperty("collection_count", out var count), "a point has no collection_count: " + point);
                Assert.Equal(JsonValueKind.Null, count.ValueKind);
                Assert.Equal(JsonValueKind.Null, point.GetProperty("logical_reads").ValueKind);
                Assert.Equal(JsonValueKind.Null, point.GetProperty("query_plan_hash").ValueKind);
            }

            Assert.Equal(day0.ToString(Stamp), root.GetProperty("effective_start").GetString()![..19]);
            Assert.Contains("hourly rollup", root.GetProperty("aggregate_note").GetString()!, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>An instant a few minutes before now, on the minute, whose minute ends in <paramref name="minuteModTen"/>, as the window's end.</summary>
    private static DateTime WindowEnd(int minuteModTen)
    {
        var now = DateTime.UtcNow;
        var onTheMinute = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Unspecified).AddMinutes(-2);
        return onTheMinute.AddMinutes(-((onTheMinute.Minute - minuteModTen + 10) % 10));
    }

    private static string AsOf(DateTime end) => end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'");

    /// <summary><paramref name="count"/> collections a minute apart from <paramref name="first"/>, one handle, one plan, the
    /// same 10 executions, 10 ms of CPU, 20 ms elapsed and 100 reads in each.</summary>
    private static Task SeedMinuteCollectionsAsync(NpgsqlConnection connection, CancellationToken ct, DateTime first, int count) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, min_dop, max_dop,
                  query_plan_hash, sample_interval_seconds)
              SELECT $1 + g, $2 + g * INTERVAL '1 minute', $3, $4, $5, $6, '0xH1', $7, $8, $9, 100, 100, 1, 1, $10, 60
              FROM generate_series(0, $11) AS g",
            CollectionIdGenerator.Next(), first, ServerId, ServerName, Db, QueryHash,
            ExecutionsPerCollection, WorkerUsPerCollection, ElapsedUsPerCollection, PlanHash, count - 1);

    /// <summary>Calls <c>get_query_trend</c> with a <c>bucket_minutes</c>, binding the arguments by parameter name.</summary>
    private static Task<string> InvokeAsync(MethodInfo method, NpgsqlDataSource postgres, int hoursBack, string asOf, int bucketMinutes, CancellationToken ct)
    {
        var args = method.GetParameters().Select(p => p.Name switch
        {
            "postgres" => postgres,
            "query_hash" => QueryHash,
            "database_name" => Db,
            "server_name" => ServerName,
            "hours_back" => hoursBack,
            "as_of" => asOf,
            "bucket_minutes" => bucketMinutes,
            "cancellationToken" => ct,
            _ => p.HasDefaultValue ? p.DefaultValue : throw new InvalidOperationException($"get_query_trend has an unexpected parameter {p.Name}"),
        }).ToArray();
        return (Task<string>)method.Invoke(null, args)!;
    }
}
