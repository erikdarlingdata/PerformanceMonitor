/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's Deadlocks, Top Queries, Top Procedures and Query Store strips against the store. Each read buckets a one-hour window by
/// the minute, the width <see cref="SlicerBuckets"/> picks for it, so the strip's line reaches the window's end instead of stopping at
/// the last whole hour while the grid below holds rows up to the end. Over a day the same reads keep the hour. Skips without
/// <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerStripsShortWindowLiveTests
{
    private const string ServerName = "darling-strips-short-window-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live strip short-window tests.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<NpgsqlConnection> OpenAndResetAsync(string cs, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        return connection;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.deadlocks WHERE server_id = {ServerId}; DELETE FROM collect.query_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM collect.procedure_stats WHERE server_id = {ServerId}; DELETE FROM collect.query_store_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM collect.servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static DateTime MinuteFloor(DateTime at) => new(at.Ticks - at.Ticks % TimeSpan.TicksPerMinute, at.Kind);

    /* Two instants, six and three minutes before the window's end, each a few seconds past the minute so the bucket stamp is the minute and not the row's time. */
    private static (DateTime End, DateTime Older, DateTime Newer) Window()
    {
        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        return (end, MinuteFloor(end).AddMinutes(-6).AddSeconds(7), MinuteFloor(end).AddMinutes(-3).AddSeconds(7));
    }

    private static async Task InsertDeadlockAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1, $2, $3, $4, $2, '<deadlock/>', 'AppDb')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName);

    private static async Task InsertQueryStatsAsync(NpgsqlConnection connection, DateTime at, string hash, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count, delta_worker_time, delta_elapsed_time) VALUES ($1, $2, $3, $4, 'AppDb', $5, 1, 1000, 1000)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, hash);

    private static async Task InsertProcedureStatsAsync(NpgsqlConnection connection, DateTime at, string objectName, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds) VALUES ($1, $2, $3, $4, 'AppDb', 'dbo', $5, '0x01', 1000, 1000, 1, 300)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, objectName);

    /* A one-minute Query Store interval stamped on its start, collected twenty seconds later. */
    private static async Task InsertQueryStoreAsync(NpgsqlConnection connection, DateTime intervalStart, long queryId, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, replica_role, runtime_stats_interval_id, interval_start_time_utc) " +
            "VALUES ($1, $2, $3, $4, 'AppDb', $5, $5, 'Regular', $2, 1, 1000, 2000, 'PRIMARY', $5, $6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(intervalStart.AddSeconds(20)), ServerId, ServerName, queryId, DarlingMcpTestData.Naive(MinuteFloor(intervalStart)));

    private static void AssertTwoMinuteBuckets(System.Collections.Generic.List<TimeSliceBucket> buckets, DateTime older, DateTime newer)
    {
        Assert.Equal(new[] { DarlingMcpTestData.Naive(MinuteFloor(older)), DarlingMcpTestData.Naive(MinuteFloor(newer)) }, buckets.Select(b => b.BucketTime).ToArray());
        Assert.Equal(new long[] { 1, 2 }, buckets.Select(b => b.SessionCount).ToArray());
    }

    [Fact]
    public async Task TheDeadlocksStrip_BucketsAOneHourWindowByTheMinute()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var (end, older, newer) = Window();
            await InsertDeadlockAsync(connection, older, ct);
            await InsertDeadlockAsync(connection, newer, ct);
            await InsertDeadlockAsync(connection, newer.AddSeconds(1), ct);

            await using var viewer = new ViewerDataService(cs!);
            var buckets = await viewer.GetDeadlockSlicerDataAsync(ServerId, end.AddHours(-1), end, ct);

            AssertTwoMinuteBuckets(buckets, older, newer);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheTopQueriesStrip_BucketsAOneHourWindowByTheMinute()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var (end, older, newer) = Window();
            await InsertQueryStatsAsync(connection, older, "0x01", ct);
            await InsertQueryStatsAsync(connection, newer, "0x02", ct);
            await InsertQueryStatsAsync(connection, newer, "0x03", ct);

            await using var viewer = new ViewerDataService(cs!);
            var buckets = await viewer.GetQueryStatsSlicerDataAsync(ServerId, end.AddHours(-1), end, cancellationToken: ct);

            AssertTwoMinuteBuckets(buckets, older, newer);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheTopProceduresStrip_BucketsAOneHourWindowByTheMinute()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var (end, older, newer) = Window();
            await InsertProcedureStatsAsync(connection, older, "usp_One", ct);
            await InsertProcedureStatsAsync(connection, newer, "usp_Two", ct);
            await InsertProcedureStatsAsync(connection, newer, "usp_Three", ct);

            await using var viewer = new ViewerDataService(cs!);
            var buckets = await viewer.GetProcStatsSlicerDataAsync(ServerId, end.AddHours(-1), end, cancellationToken: ct);

            AssertTwoMinuteBuckets(buckets, older, newer);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheQueryStoreStrip_BucketsAOneHourWindowByTheMinute()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var (end, older, newer) = Window();
            await InsertQueryStoreAsync(connection, older, 1, ct);
            await InsertQueryStoreAsync(connection, newer, 2, ct);
            await InsertQueryStoreAsync(connection, newer, 3, ct);

            await using var viewer = new ViewerDataService(cs!);
            var buckets = await viewer.GetQueryStoreSlicerDataAsync(ServerId, end.AddHours(-1), end, cancellationToken: ct);

            AssertTwoMinuteBuckets(buckets, older, newer);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* Over a day the strip keeps the hour: a 24-hour window with two deadlocks in one hour is one bucket stamped on the hour. */
    [Fact]
    public async Task TheDeadlocksStrip_KeepsTheHourOverADay()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            using var connection = await OpenAndResetAsync(cs!, ct);
            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var hour = new DateTime(end.Ticks - end.Ticks % TimeSpan.TicksPerHour, DateTimeKind.Utc).AddHours(-3);
            await InsertDeadlockAsync(connection, hour.AddMinutes(5), ct);
            await InsertDeadlockAsync(connection, hour.AddMinutes(25), ct);

            await using var viewer = new ViewerDataService(cs!);
            var buckets = await viewer.GetDeadlockSlicerDataAsync(ServerId, end.AddHours(-24), end, ct);

            var bucket = Assert.Single(buckets);
            Assert.Equal(DarlingMcpTestData.Naive(hour), bucket.BucketTime);
            Assert.Equal(2, bucket.SessionCount);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
