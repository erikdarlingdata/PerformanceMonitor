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

            /* One plan across three handles: the hash survives. */
            Assert.Equal(6, points[1].GetProperty("execution_count").GetInt64());
            Assert.Equal("0xSAME", points[1].GetProperty("query_plan_hash").GetString());
            Assert.Equal(1, points[1].GetProperty("min_dop").GetInt32());
            Assert.Equal(3, points[1].GetProperty("max_dop").GetInt32());

            /* A single handle reads exactly as before. */
            Assert.Equal(4, points[2].GetProperty("execution_count").GetInt64());
            Assert.Equal("0xONLY", points[2].GetProperty("query_plan_hash").GetString());

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
