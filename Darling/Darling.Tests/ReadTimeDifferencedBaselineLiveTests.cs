/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A row that stored no interval takes its interval from the row before it. These reads run over a real store holding a
/// row stamped exactly on the window start and rows after it, none with a stored interval. The row on the start is the
/// first counted row's predecessor and nothing more: its own delta is in no total and it is no point, and the first row
/// after it is rated over the seconds since it. Lite's twin is <c>ReadTimeDifferencedBaselineTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class ReadTimeDifferencedBaselineLiveTests
{
    private const string ServerName = "read-time-baseline-e2e";
    private const int ServerId = -9472;
    private const string Wait = "LCK_M_X";
    private const string Latch = "BUFFER";

    private static readonly DateTime s_start = TopOfTheHour(DateTime.UtcNow).AddHours(-3);
    private static readonly DateTime s_end = s_start.AddMinutes(10);
    private static readonly DateTime s_first = s_start.AddSeconds(60);
    private static readonly DateTime s_second = s_start.AddSeconds(120);

    private static DateTime TopOfTheHour(DateTime utc) => new(utc.Year, utc.Month, utc.Day, utc.Hour, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public Task TheWaitTrend_RatesTheFirstRowAfterTheStart_AndLeavesTheStartRowOut() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, DarlingDataReader.WaitTrendSql, ServerId, Wait, s_start, s_end);

        Assert.Equal(2, rows.Count);
        Assert.Equal(s_first, (DateTime)rows[0][0]);
        Assert.Equal(10.0, Convert.ToDouble(rows[0][1]), 6);
        Assert.Equal(s_second, (DateTime)rows[1][0]);
        Assert.Equal(20.0, Convert.ToDouble(rows[1][1]), 6);
    });

    [Fact]
    public Task TheBucketedWaitTrend_RatesBothRowsAfterTheStart() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, DarlingDataReader.WaitTrendBucketedSql, ServerId, Wait, s_start, s_end, 60);

        var bucket = Assert.Single(rows);
        Assert.Equal(15.0, Convert.ToDouble(bucket[1]), 6);
    });

    [Fact]
    public Task TheLockWaitTypes_TotalTheRowsAfterTheStart_OverTheirOwnSeconds() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, DarlingBlockingTrendReader.LockWaitTypesSql, ServerId, s_start, s_end);

        var type = Assert.Single(rows);
        Assert.Equal(Wait, (string)type[0]);
        Assert.Equal(1800.0, Convert.ToDouble(type[1]), 6);
        Assert.Equal(120.0, Convert.ToDouble(type[2]), 6);
    });

    [Fact]
    public Task TheTotalWaitsTrend_CountsTwoCollections_AndRatesBoth() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, ServerTrendSql.TotalWaits, ServerId, s_start, s_end, 60);

        var bucket = Assert.Single(rows);
        Assert.Equal(15.0, Convert.ToDouble(bucket[1]), 6);
        Assert.Equal(s_first, (DateTime)bucket[2]);
        Assert.Equal(2L, Convert.ToInt64(bucket[3]));
    });

    [Fact]
    public Task TheLatchStats_RateTheNewestRowOverTheSecondsSinceTheStartRow() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, DarlingLatchSpinlockReader.LatchStatsTopNSql, ServerId, s_start, s_end, 10);

        var latch = Assert.Single(rows);
        Assert.Equal(Latch, (string)latch[0]);
        Assert.Equal(600L, Convert.ToInt64(latch[1]));
        Assert.Equal(10.0, Convert.ToDouble(latch[4]), 6);
        Assert.Equal(s_first, (DateTime)latch[6]);
        Assert.Equal(60.0, Convert.ToDouble(latch[7]), 6);
    });

    [Fact]
    public Task TheLatchWaitsTrend_RatesItsOneRowAfterTheStart() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, ServerTrendSql.LatchWaits(1), ServerId, s_start, s_end, Latch, 60);

        var bucket = Assert.Single(rows);
        Assert.Equal(10.0, Convert.ToDouble(bucket[2]), 6);
        Assert.Equal(1L, Convert.ToInt64(bucket[4]));
    });

    [Fact]
    public Task TheViewerLockWaitTrend_RatesBothRowsAfterTheStart() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, ViewerDataService.LockWaitTrendSql, ServerId, s_start, s_end, 60);

        var bucket = Assert.Single(rows);
        Assert.Equal(15.0, Convert.ToDouble(bucket[2]), 6);
        Assert.Equal(2L, Convert.ToInt64(bucket[4]));
    });

    [Fact]
    public Task TheViewerWaitTrends_RateBothRowsAfterTheStart() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, ViewerDataService.WaitTrendsSql(1), ServerId, s_start, s_end, Wait, 60);

        var bucket = Assert.Single(rows);
        Assert.Equal(15.0, Convert.ToDouble(bucket[2]), 6);
        Assert.Equal(2L, Convert.ToInt64(bucket[6]));
    });

    [Fact]
    public Task TheViewerLatchTrend_RatesItsOneRowAfterTheStart() => RunAsync(async (postgres, ct) =>
    {
        var rows = await ReadAsync(postgres, ct, ViewerDataService.LatchTrendSql, ServerId, s_start, s_end);

        var point = Assert.Single(rows);
        Assert.Equal(s_first, (DateTime)point[1]);
        Assert.Equal(10.0, Convert.ToDouble(point[2]), 6);
    });

    private static async Task<List<object[]>> ReadAsync(NpgsqlDataSource postgres, CancellationToken ct, string sql, params object[] values)
    {
        var rows = new List<object[]>();
        await using var command = postgres.CreateCommand(sql);
        foreach (var value in values)
        {
            if (value is DateTime time)
                command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, time);
            else
                command.Parameters.AddWithValue(value);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var row = new object[reader.FieldCount];
            reader.GetValues(row);
            rows.Add(row);
        }

        return rows;
    }

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var (time, delta) in new[] { (s_start, 111L), (s_first, 600L), (s_second, 1200L) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 5, $6, 0, NULL)",
                CollectionIdGenerator.Next(), time, ServerId, ServerName, Wait, delta);
        }

        foreach (var (time, delta) in new[] { (s_start, 111L), (s_first, 600L) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.latch_stats
    (collection_id, collection_time, server_id, server_name, latch_class,
     waiting_requests_count, wait_time_ms, max_wait_time_ms,
     delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, 5, $6, 0, NULL)",
                CollectionIdGenerator.Next(), time, ServerId, ServerName, Latch, delta);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.wait_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.latch_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.servers WHERE server_id = $1", ServerId);
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live read-time baseline tests.");

        var ct = TestContext.Current.CancellationToken;

        await using (var migration = new NpgsqlConnection(cs))
        {
            await migration.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migration, ct);
        }

        var pinned = new NpgsqlConnectionStringBuilder(cs) { SearchPath = "collect,config" }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(pinned);
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await SeedAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await body(postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }
}
