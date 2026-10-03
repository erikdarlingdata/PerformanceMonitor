/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A panel read from a rollup takes its start from the rollup it read, not from the raw table. Server B's rows
/// were rolled up from 10 days back, and raw now keeps the last 3 days, as its retention leaves it. Server A's
/// rollup reaches 20 days back, so the store as a whole covers a 14-day window and routes it to the hourly
/// rollup. B's own hourly rows start 10 days back, so B's panel says so and names that start. The raw table's
/// start, 3 days back, would be the wrong answer.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ComposeRollupDataFloorLiveTests
{
    private const int OldServerId = -497201;
    private const string OldServerName = "rollup-floor-old";
    private const int NewServerId = -497202;
    private const string NewServerName = "rollup-floor-new";
    private const int StitchedDeepServerId = -497203;
    private const string StitchedDeepServerName = "rollup-floor-stitched-deep";
    private const int StitchedScopedServerId = -497204;
    private const string StitchedScopedServerName = "rollup-floor-stitched-scoped";

    [Fact]
    public async Task AnHourlyRoutedPanel_TakesItsStartFromTheHourlyRollup_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the rollup data-start test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The rollup data-start test needs TimescaleDB: the panel reads a continuous aggregate.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, OldServerId, OldServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, NewServerId, NewServerName, ct);

        var today = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        await InsertDailyAsync(connection, OldServerId, OldServerName, today.AddDays(-20), today.AddDays(-1), ct);
        await InsertDailyAsync(connection, NewServerId, NewServerName, today.AddDays(-10), today.AddDays(-1), ct);

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        foreach (var view in new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView })
        {
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
            refresh.Parameters.AddWithValue(today.AddDays(-21));
            refresh.Parameters.AddWithValue(today.AddDays(1));
            await refresh.ExecuteNonQueryAsync(ct);
        }

        /* Raw keeps the last 3 days, as its retention would leave it; the rollups keep everything they built. */
        await using (var trim = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE collection_time < $1", connection))
        {
            trim.Parameters.AddWithValue(today.AddDays(-3));
            await trim.ExecuteNonQueryAsync(ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var newServer = await RunAsync(dataSource, NewServerName, ct);
        Assert.True(newServer.Error is null, $"compose run failed: {newServer.Error}");
        var sql = (string)newServer.Payload!["sql"]!;
        Assert.Contains("_hourly", sql, StringComparison.Ordinal);

        var notice = newServer.Payload["notice"]?.GetValue<string>();
        Assert.NotNull(notice);
        Assert.StartsWith("partial window:", notice, StringComparison.Ordinal);
        Assert.Contains(Minute(today.AddDays(-10).AddHours(10)) + " UTC", notice, StringComparison.Ordinal);
        Assert.DoesNotContain(today.AddDays(-3).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), notice, StringComparison.Ordinal);

        /* The old server's hourly rows cover the whole window: no notice. */
        var oldServer = await RunAsync(dataSource, OldServerName, ct);
        Assert.True(oldServer.Error is null, $"compose run failed: {oldServer.Error}");
        Assert.Contains("_hourly", (string)oldServer.Payload!["sql"]!, StringComparison.Ordinal);
        Assert.Null(oldServer.Payload["notice"]);
    }

    /// <summary>
    /// A stitched read takes the successor rollup only from the stitch boundary up, so for a server the superseded
    /// rollup holds nothing for, the panel's data starts at the boundary, whatever older buckets the successor holds
    /// for it. The deep server's superseded hourly reaches back 20 days and the successor hourly was built from 8 days
    /// back, so the first run measures the boundary at 8 days back and caches it. The scoped server's rows then arrive
    /// after the superseded hourly was refreshed, and a wide refresh materializes the successor back past the
    /// boundary: it holds buckets for the scoped server older than the window's start, none of which the stitched
    /// read uses. The notice names the boundary, not the first of those buckets.
    /// </summary>
    [Fact]
    public async Task AStitchedHourlyPanel_StartsAtTheStitchBoundary_WhenTheSuccessorWasMaterializedBackPastIt_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the stitched rollup data-start test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The stitched rollup data-start test needs TimescaleDB: the panel reads continuous aggregates.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* The scratch database's own scheduler stops before the ensure sweep creates its refresh policies: a policy
           run would refresh the superseded hourly over the scoped server's rows, which this test keeps out of it. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, StitchedDeepServerId, StitchedDeepServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, StitchedScopedServerId, StitchedScopedServerName, ct);

        var today = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        await InsertDailyAsync(connection, StitchedDeepServerId, StitchedDeepServerName, today.AddDays(-20), today.AddDays(-1), ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, today.AddDays(-21), today.AddDays(1), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, today.AddDays(-8), today.AddDays(1), ct);
        var boundary = today.AddDays(-8).AddHours(10);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* The first run measures the store's coverage and caches it per data source: the boundary is the successor's
           first bucket, 8 days back. The deep server's superseded hourly covers the whole window, so it has no notice. */
        var deep = await RunAsync(dataSource, StitchedDeepServerName, ct);
        Assert.True(deep.Error is null, $"compose run failed: {deep.Error}");
        Assert.Contains("UNION ALL", (string)deep.Payload!["sql"]!, StringComparison.Ordinal);
        Assert.Null(deep.Payload["notice"]);

        await InsertDailyAsync(connection, StitchedScopedServerId, StitchedScopedServerName, today.AddDays(-19), today.AddDays(-1), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, today.AddDays(-21), today.AddDays(1), ct);

        Assert.Equal(0L, await CountAsync(connection, TimescaleSupport.QueryStatsHourlyView, StitchedScopedServerId, ct));
        var olderBucket = today.AddDays(-19).AddHours(10);
        Assert.Equal(olderBucket, await FirstBucketAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, StitchedScopedServerId, ct));

        var scoped = await RunAsync(dataSource, StitchedScopedServerName, ct);
        Assert.True(scoped.Error is null, $"compose run failed: {scoped.Error}");
        var sql = (string)scoped.Payload!["sql"]!;
        Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
        Assert.Contains("TIMESTAMP '" + boundary.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'", sql, StringComparison.Ordinal);

        var notice = scoped.Payload["notice"]?.GetValue<string>();
        Assert.NotNull(notice);
        Assert.StartsWith("partial window:", notice, StringComparison.Ordinal);
        Assert.Contains(Minute(boundary) + " UTC", notice, StringComparison.Ordinal);
        Assert.DoesNotContain(Minute(olderBucket), notice, StringComparison.Ordinal);
    }

    private static string Minute(DateTime utc) => utc.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string view, int serverId, CancellationToken ct)
    {
        await using var count = new NpgsqlCommand($"SELECT count(*) FROM collect.{view} WHERE server_id = $1", connection);
        count.Parameters.AddWithValue(serverId);
        return (long)(await count.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime> FirstBucketAsync(NpgsqlConnection connection, string view, int serverId, CancellationToken ct)
    {
        await using var first = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{view} WHERE server_id = $1", connection);
        first.Parameters.AddWithValue(serverId);
        return (DateTime)(await first.ExecuteScalarAsync(ct))!;
    }

    private static Task<DarlingWebEndpoints.ComposeRunOutcome> RunAsync(NpgsqlDataSource dataSource, string server, CancellationToken ct)
    {
        var body = new JsonObject
        {
            ["panel"] = JsonNode.Parse(
                "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"day\",\"viz\":\"line\"}"),
            ["server"] = server,
            ["hours"] = 14 * 24,
        };

        return DarlingWebEndpoints.RunComposedPanelAsync(dataSource, body, ct);
    }

    /// <summary>One query_stats row a day at 10:05, from <paramref name="firstDay"/> to <paramref name="lastDay"/>.</summary>
    private static async Task InsertDailyAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime firstDay, DateTime lastDay, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT row_number() OVER (), d + interval '10 hours 5 minutes', $1, $2, 'FloorDb', 'HASHFLOOR', '0xFLOOR', 1000, 1000, 10, 300
FROM generate_series($3::timestamp, $4::timestamp, interval '1 day') AS d", connection);
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
        insert.Parameters.AddWithValue(firstDay);
        insert.Parameters.AddWithValue(lastDay);
        await insert.ExecuteNonQueryAsync(ct);
    }
}
