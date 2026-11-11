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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8575: the Query Store interval still open is rated over the time it has covered, not over its whole length. A server with
/// two closed hourly intervals and an open one last captured 23.5 minutes in reads the open point as its executions over 1,410
/// seconds and marks it partial; the closed points keep their whole hour. An open interval last captured 30 seconds in is
/// unrated (a rate over a few seconds is noise) and counted in <c>unrated_points</c>. Both routes run: the raw read the MCP tool
/// uses where the corrected rollup is absent, and the rollup route's builder, which the viewer shares.
/// </summary>
/* #1776 own-store: not [Collection("live-postgres")]. The rollup route needs the continuous aggregates, so the test works entirely
   inside its own ScratchPostgres-created database. */
public sealed class QueryStoreOpenIntervalTrendLiveTests
{
    private const int ServerId = -958201;
    private const string ServerName = "qs-open-interval-trend";
    private const string Db = "AppDb";
    private const long OpenExecutions = 2_020_046;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheOpenInterval_IsRatedOverTheTimeItHasCovered_AndIsMarkedPartial_WhileClosedIntervalsKeepTheirHour()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live open-interval trend test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* The open interval began 40 minutes ago and ends 20 minutes from now. Two closed hourly intervals precede it. */
        var openStart = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-40));
        var closedA = openStart.AddHours(-2);
        var closedB = openStart.AddHours(-1);
        await SeedAsync(connection, ct, closedA.AddHours(1).AddMinutes(5), 41, closedA, 4_412_812, 1000);
        await SeedAsync(connection, ct, closedB.AddHours(1).AddMinutes(5), 42, closedB, 4_955_449, 1000);

        /* Captured 23.5 minutes into the interval, so 1,410 seconds covered. */
        await SeedAsync(connection, ct, openStart.AddSeconds(1410), 43, openStart, OpenExecutions, 1000);

        var rollupRoute = new QueryStoreTrendRouting.QueryStoreTrendRoute(UseRollup: true, RawStartUtc: closedA.AddHours(-1), RollupFloorUtc: null);

        AssertPartialPoint(await ReadToolAsync(postgres, ct));
        await AssertRollupRouteAsync(postgres, rollupRoute, ct);

        /* The viewer's own raw read rates the same way: its chart shows the open interval at its covered rate. */
        var viewerRaw = await ReadViewerRawAsync(postgres, ct);
        Assert.Equal(3, viewerRaw.Count);
        Assert.Equal(4_412_812d / 3600d, viewerRaw[0]!.Value, 6);
        Assert.Equal(OpenExecutions / 1410d, viewerRaw[2]!.Value, 6);

        /* The same interval last captured 30 seconds in: under a minute, so the point is unrated and counted. */
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_store_stats WHERE server_id = $1 AND runtime_stats_interval_id = 43", ServerId);
        await SeedAsync(connection, ct, openStart.AddSeconds(30), 43, openStart, 4_000, 1000);

        var brief = await ReadToolAsync(postgres, ct);
        var trend = brief.GetProperty("trend");
        Assert.Equal(3, trend.GetArrayLength());
        var open = trend[2];
        Assert.Equal(JsonValueKind.Null, open.GetProperty("executions_per_second").ValueKind);
        Assert.Equal(JsonValueKind.Null, open.GetProperty("elapsed_ms_per_second").ValueKind);
        Assert.Equal(4_000, open.GetProperty("execution_count").GetInt64());
        Assert.True(open.GetProperty("partial").GetBoolean());
        Assert.Equal(30, open.GetProperty("covered_seconds").GetDouble(), 6);
        Assert.Equal(1, brief.GetProperty("unrated_points").GetInt32());
        Assert.Contains("still open", brief.GetProperty("unrated_note").GetString());
        Assert.Equal(4_412_812d / 3600d, trend[0].GetProperty("executions_per_second").GetDouble(), 6);
        Assert.Null((await ReadViewerRawAsync(postgres, ct))[2]);

        var briefRollup = await ReadRollupAsync(postgres, rollupRoute, ct);
        Assert.False(briefRollup[2].HasRate);
        Assert.Equal(30, briefRollup[2].PartialCoveredSeconds!.Value, 6);

    }

    private static async Task<JsonElement> ReadToolAsync(NpgsqlDataSource postgres, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpTrendTools.GetQueryStoreDurationTrend(postgres, ServerName, hours_back: 6, cancellationToken: ct)).RootElement;

    private static Task<List<DarlingTrendReader.QueryDurationTrendPoint>> ReadRollupAsync(
        NpgsqlDataSource postgres, QueryStoreTrendRouting.QueryStoreTrendRoute route, CancellationToken ct) =>
        DarlingTrendReader.GetQueryStoreDurationTrendAsync(
            postgres, ServerId, DateTime.UtcNow.AddHours(-6), DateTime.UtcNow, route, DatabaseFilter.All, ct);

    /// <summary>The viewer's raw statement's executions per second for each point, null where the point is unrated.</summary>
    private static async Task<List<double?>> ReadViewerRawAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var items = new List<double?>();
        await using var command = postgres.CreateCommand(ViewerDataService.QueryStoreDurationTrendSql);
        ViewerDataService.AddServerWindowParameters(command, ServerId, DateTime.UtcNow.AddHours(-6), DateTime.UtcNow);
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            items.Add(reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)));
        }

        return items;
    }

    private static void AssertPartialPoint(JsonElement answer)
    {
        var trend = answer.GetProperty("trend");
        Assert.Equal(3, trend.GetArrayLength());
        Assert.Equal(0, answer.GetProperty("unrated_points").GetInt32());

        /* The closed points are rated over their own stored hour and carry no partial marker. */
        Assert.Equal(4_412_812d / 3600d, trend[0].GetProperty("executions_per_second").GetDouble(), 6);
        Assert.Equal(4_955_449d / 3600d, trend[1].GetProperty("executions_per_second").GetDouble(), 6);
        Assert.False(trend[0].TryGetProperty("partial", out _));
        Assert.False(trend[1].TryGetProperty("partial", out _));

        var open = trend[2];
        Assert.Equal(OpenExecutions, open.GetProperty("execution_count").GetInt64());
        Assert.Equal(OpenExecutions / 1410d, open.GetProperty("executions_per_second").GetDouble(), 6);
        Assert.True(open.GetProperty("partial").GetBoolean());
        Assert.Equal(1410, open.GetProperty("covered_seconds").GetDouble(), 6);
    }

    private static async Task AssertRollupRouteAsync(
        NpgsqlDataSource postgres, QueryStoreTrendRouting.QueryStoreTrendRoute route, CancellationToken ct)
    {
        var points = await ReadRollupAsync(postgres, route, ct);
        Assert.Equal(3, points.Count);
        Assert.Equal(4_412_812d / 3600d, points[0].ExecutionsPerSecond!.Value, 6);
        Assert.Equal(4_955_449d / 3600d, points[1].ExecutionsPerSecond!.Value, 6);
        Assert.Null(points[0].PartialCoveredSeconds);
        Assert.Null(points[1].PartialCoveredSeconds);
        Assert.Equal(OpenExecutions / 1410d, points[2].ExecutionsPerSecond!.Value, 6);
        Assert.Equal(1410, points[2].PartialCoveredSeconds!.Value, 6);
        Assert.True(points.All(p => p.HasRate));
    }

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, long intervalId,
        DateTime intervalStart, long executions, long avgDurationUs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            Db, 7L, 9L, "Regular", executions, avgDurationUs, intervalId,
            DarlingMcpTestData.Naive(intervalStart), DarlingMcpTestData.Naive(intervalStart.AddHours(1)));
}
