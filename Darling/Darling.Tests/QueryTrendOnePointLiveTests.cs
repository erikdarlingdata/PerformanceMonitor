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

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #7608: <c>get_query_trend</c> gives ONE point per collection on the raw tier. One <c>query_hash</c> runs under several
/// <c>sql_handle</c>s, the collector writes a row per handle per collection, and the tool used to return every row
/// under one <c>collection_time</c> with nothing to tell them apart. The rows are now summed per collection, the DOP
/// pair spans them, and the plan hash is the hash only when every row at that time carries the same one.
/// (The hourly tier's sum is asserted in <see cref="WebReadReachLiveTests"/>, which holds the TimescaleDB store.)
/// </summary>
public sealed class QueryTrendOnePointLiveTests
{
    private const int ServerId = -760801;
    private const string ServerName = "a7608-one-point";
    private const string Db = "TrendDb";
    private const string QueryHash = "0xQ7608";

    [Fact]
    public async Task RawTier_ThreeHandlesAtOneCollection_IsOnePointWithSummedDeltas()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query trend test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            var first = DateTime.UtcNow.AddHours(-3);
            first = new DateTime(first.Year, first.Month, first.Day, first.Hour, first.Minute, 0, DateTimeKind.Unspecified);
            var second = first.AddMinutes(5);
            var third = first.AddMinutes(10);

            /* First collection: three handles, three plans. Second: three handles, ONE plan. Third: one handle. */
            await InsertAsync(connection, ct, first, "0xH1", 10, 1000, 2000, 100, 1, 4, "0xPLAN1");
            await InsertAsync(connection, ct, first, "0xH2", 5, 500, 700, 50, 2, 8, "0xPLAN2");
            await InsertAsync(connection, ct, first, "0xH3", 7, 300, 400, 20, 1, 2, "0xPLAN3");
            await InsertAsync(connection, ct, second, "0xH1", 1, 10, 20, 1, 1, 1, "0xSAME");
            await InsertAsync(connection, ct, second, "0xH2", 2, 20, 30, 2, 1, 2, "0xSAME");
            await InsertAsync(connection, ct, second, "0xH3", 3, 30, 40, 3, 2, 3, "0xSAME");
            await InsertAsync(connection, ct, third, "0xH1", 4, 40, 50, 4, 1, 1, "0xONLY");

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpTrendTools.GetQueryTrend(postgres, QueryHash, Db, ServerName, 24, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
            Assert.Equal("raw", doc.RootElement.GetProperty("source").GetString());

            var points = doc.RootElement.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(3, points.Length);
            Assert.Equal(3, doc.RootElement.GetProperty("data_points").GetInt32());

            /* Mixed plans: sums, DOP spread, and a null plan hash (more than one plan ran). */
            Assert.Equal(22, points[0].GetProperty("execution_count").GetInt64());
            Assert.Equal(1.8, points[0].GetProperty("cpu_ms").GetDouble());
            Assert.Equal(3.1, points[0].GetProperty("elapsed_ms").GetDouble());
            Assert.Equal(170, points[0].GetProperty("logical_reads").GetInt64());
            Assert.Equal(1, points[0].GetProperty("min_dop").GetInt32());
            Assert.Equal(8, points[0].GetProperty("max_dop").GetInt32());
            Assert.Equal(JsonValueKind.Null, points[0].GetProperty("query_plan_hash").ValueKind);
            Assert.Equal(3, points[0].GetProperty("plan_count").GetInt32());

            /* One plan across three handles: the hash survives. */
            Assert.Equal(6, points[1].GetProperty("execution_count").GetInt64());
            Assert.Equal("0xSAME", points[1].GetProperty("query_plan_hash").GetString());
            Assert.Equal(1, points[1].GetProperty("min_dop").GetInt32());
            Assert.Equal(3, points[1].GetProperty("max_dop").GetInt32());
            Assert.Equal(1, points[1].GetProperty("plan_count").GetInt32());

            /* A single handle reads exactly as before. */
            Assert.Equal(4, points[2].GetProperty("execution_count").GetInt64());
            Assert.Equal("0xONLY", points[2].GetProperty("query_plan_hash").GetString());
            Assert.Equal(1, points[2].GetProperty("plan_count").GetInt32());

            Assert.Equal(
                points.Select(p => p.GetProperty("collection_time").GetString()).Distinct().Count(),
                points.Length);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task RawTier_AStalePlanWithNoExecutions_DoesNotLendItsLifetimeDopToThePoint()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query trend test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            var first = DateTime.UtcNow.AddHours(-3);
            first = new DateTime(first.Year, first.Month, first.Day, first.Hour, first.Minute, 0, DateTimeKind.Unspecified);
            var second = first.AddMinutes(5);

            /* First collection: a plan from before a MAXDOP change sits in the cache with max_dop 8 and ran nothing;
               the plan that ran has max_dop 2. Second collection: nothing ran at all, so both plans still show. */
            await InsertAsync(connection, ct, first, "0xH1", 0, 0, 0, 0, 1, 8, "0xSTALE");
            await InsertAsync(connection, ct, first, "0xH2", 6, 600, 900, 60, 1, 2, "0xLIVE");
            await InsertAsync(connection, ct, second, "0xH1", 0, 0, 0, 0, 1, 8, "0xSTALE");
            await InsertAsync(connection, ct, second, "0xH2", 0, 0, 0, 0, 1, 2, "0xLIVE");

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpTrendTools.GetQueryTrend(postgres, QueryHash, Db, ServerName, 24, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
            var points = doc.RootElement.GetProperty("trend").EnumerateArray().ToArray();
            Assert.Equal(2, points.Length);

            Assert.Equal(6, points[0].GetProperty("execution_count").GetInt64());
            Assert.Equal(2, points[0].GetProperty("max_dop").GetInt32());
            Assert.Equal(1, points[0].GetProperty("min_dop").GetInt32());
            Assert.Equal("0xLIVE", points[0].GetProperty("query_plan_hash").GetString());
            Assert.Equal(1, points[0].GetProperty("plan_count").GetInt32());

            /* No row ran: every row counts, so the point shows both plans and the highest DOP among them. */
            Assert.Equal(0, points[1].GetProperty("execution_count").GetInt64());
            Assert.Equal(8, points[1].GetProperty("max_dop").GetInt32());
            Assert.Equal(JsonValueKind.Null, points[1].GetProperty("query_plan_hash").ValueKind);
            Assert.Equal(2, points[1].GetProperty("plan_count").GetInt32());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #7608 review: the hourly read groups by <c>bucket</c> over the STITCHED relation (legacy rollup below the
    /// boundary, interval-honest successor at and above it). Several handles per hour on both sides of the boundary
    /// must still give one point per hour with the exact sums: an hour read from both relations would double its sum.
    /// </summary>
    [Fact]
    public async Task HourlyTier_SeveralHandlesPerHour_OnBothSidesOfTheStitch_IsOnePointPerHourWithExactSums()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live stitched query trend test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The stitched hourly read needs TimescaleDB: the legacy/successor split exists only between materializations.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var bodySucceeded = false;
        try
        {
            var legacy = TimescaleSupport.QueryStatsHourlyView;
            var successor = TimescaleSupport.QueryStatsIntervalHourlyView;

            /* Four whole hours: H0 and H1 below the boundary F (legacy only), H2 = F and H3 above it (successor only).
               Each hour gets several handles, one row each, so a stitched read that counted an hour from both relations
               would show up as a doubled sum. */
            var h0 = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-10), DateTimeKind.Unspecified);
            DateTime Hour(int n) => h0.AddHours(n);
            var f = Hour(2);

            await InsertAsync(connection, ct, Hour(0).AddMinutes(5), "0xH1", 1, 100, 1000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(0).AddMinutes(10), "0xH2", 2, 200, 2000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(0).AddMinutes(15), "0xH3", 4, 400, 4000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(1).AddMinutes(5), "0xH1", 8, 800, 8000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(1).AddMinutes(10), "0xH2", 16, 1600, 16000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(2).AddMinutes(5), "0xH1", 32, 3200, 32000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(2).AddMinutes(10), "0xH2", 64, 6400, 64000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(2).AddMinutes(15), "0xH3", 128, 12800, 128000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(3).AddMinutes(5), "0xH1", 256, 25600, 256000, 1, 1, 1, "0xP");
            await InsertAsync(connection, ct, Hour(3).AddMinutes(10), "0xH2", 512, 51200, 512000, 1, 1, 1, "0xP");

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            /* The legacy is materialized over the whole span, the successor only from F: the shape a successor that
               started later than the legacy always has. */
            await RefreshAsync(connection, legacy, h0, h0.AddHours(6), ct);
            await RefreshAsync(connection, successor, f, h0.AddHours(6), ct);

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            Assert.True(rollups.QueryGrainIntervalHourly);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
            Assert.Equal(f, coverage.FloorOf(successor));

            var fromClause = coverage.StitchedRelationSql(legacy, "h", h0, RollupCoverage.StitchTier.Hourly);
            Assert.Contains("UNION ALL", fromClause, StringComparison.Ordinal);
            var history = await DarlingTrendReader.GetQueryHistoryAsync(
                dataSource, ServerId, Db, QueryHash, h0, h0.AddHours(6),
                hourlyAvailable: rollups.QueryGrainHourly,
                coverage: coverage.For(legacy, TimescaleSupport.QueryStatsDailyView),
                hourlyRelation: fromClause,
                cancellationToken: ct);

            Assert.Equal("hourly", history.Source);
            Assert.Equal(new[] { Hour(0), Hour(1), Hour(2), Hour(3) }, history.Points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(new long[] { 7, 24, 224, 768 }, history.Points.Select(p => p.DeltaExecutions).ToArray());
            Assert.Equal(new long[] { 700, 2400, 22400, 76800 }, history.Points.Select(p => p.DeltaCpuUs).ToArray());
            Assert.Equal(new long[] { 7000, 24000, 224000, 768000 }, history.Points.Select(p => p.DeltaElapsedUs).ToArray());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, System.Threading.CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static Task InsertAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, DateTime at, string handle,
        long executions, long workerUs, long elapsedUs, long reads, int minDop, int maxDop, string planHash) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, min_dop, max_dop,
                  query_plan_hash, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11, $12, $13, $14, 300)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, Db, QueryHash, handle,
            executions, workerUs, elapsedUs, reads, minDop, maxDop, planHash);
}
