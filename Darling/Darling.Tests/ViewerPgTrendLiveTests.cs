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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6385, against a live store: the Viewer's four PostgreSQL trend reads return what the web's <c>/api/read</c>
/// returns for the same window. The web calls <c>DarlingMcpPgTrendTools</c> with the chart budget and no named
/// subject (DarlingWebEndpoints' read table); each fact here makes that exact call and compares its points, its
/// chosen subject and its bucket width with <see cref="ViewerDataService"/>'s read on the same seeded rows. Each
/// fact also asserts the web answered with points, so an empty answer on both sides cannot pass.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerPgTrendLiveTests
{
    private const string ServerName = "viewer-pg-trend-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly DateTime AsOf = new(2026, 3, 5, 11, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime T0 = new(2026, 3, 5, 10, 0, 0);
    private const string AsOfText = "2026-03-05T11:00:00Z";
    private const int Hours = 2;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static (DateTime Start, DateTime End) Window() => (AsOf.AddHours(-Hours), AsOf);

    [Fact]
    public async Task DatabaseTrend_MatchesTheWebRead()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            /* Cumulative (commit, rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks), one cliff. */
            var rows = new (DateTime T, long Commit, long Rollback, long Read, long Hit, long TempFiles, long TempBytes, long Deadlocks)[]
            {
                (T0,                1_000, 10,   100,   900, 0,         0, 0),
                (T0.AddMinutes(1),  1_600, 10,   200, 1_800, 0,         0, 0),
                (T0.AddMinutes(2),  1_660, 70, 1_100, 1_900, 2, 6_000_000, 0),
                (T0.AddMinutes(3),  2_260, 70, 1_200, 2_800, 2, 6_000_000, 0),
                (T0.AddMinutes(25), 2_270, 70, 1_210, 2_890, 3, 7_320_000, 1),
            };
            foreach (var r in rows)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
VALUES ($1, $2, $3, $4, 'appdb', $5, $6, $7, $8, $9, $10, $11, NULL)",
                    CollectionIdGenerator.Next(), r.T, ServerId, ServerName, r.Commit, r.Rollback, r.Read, r.Hit, r.TempFiles, r.TempBytes, r.Deadlocks);
            }

            var web = Root(await DarlingMcpPgTrendTools.GetPgDatabaseTrend(postgres, ServerName, null, Hours, AsOfText, null, TrendBudget.Chart, ct));
            var (start, end) = Window();
            var read = await viewer.GetPgDatabaseTrendAsync(ServerId, start, end, ct);

            var webPoints = web.GetProperty("points").EnumerateArray().ToArray();
            Assert.NotEmpty(webPoints);
            Assert.Equal(web.GetProperty("database").GetString(), read.Subject);
            Assert.Equal(web.GetProperty("bucket_minutes").GetInt32(), read.BucketMinutes);
            Assert.Equal(webPoints.Length, read.Points.Count);
            for (var i = 0; i < webPoints.Length; i++)
            {
                var w = webPoints[i];
                var v = read.Points[i];
                Assert.Equal(Stamp(v.CollectionTimeUtc), Stamp(w.GetProperty("collection_time")));
                Assert.Equal(w.GetProperty("temp_files").GetInt64(), v.TempFiles);
                Assert.Equal(w.GetProperty("temp_bytes").GetInt64(), v.TempBytes);
                Assert.Equal(w.GetProperty("deadlocks").GetInt64(), v.Deadlocks);
                Near(w.GetProperty("cache_hit_pct"), v.CacheHitPct, 2);
                Near(w.GetProperty("worst_cache_hit_pct"), v.WorstCacheHitPct, 2);
                Near(w.GetProperty("transactions_per_second"), v.TransactionsPerSecond, 3);
                Near(w.GetProperty("rollback_pct"), v.RollbackPct, 2);
            }
        });
    }

    [Fact]
    public async Task QueryDurationTrend_MatchesTheWebRead()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            /* The costlier statement (queryid 77) is the subject; 88 is the one the choice must skip. Per-snapshot
               deltas, as the collector stores them. */
            for (var i = 0; i < 5; i++)
            {
                await SeedStatementAsync(connection, ct, T0.AddMinutes(i), 77, deltaCalls: 10 + i, deltaMs: 100.0 * (i + 1), interval: 60);
                await SeedStatementAsync(connection, ct, T0.AddMinutes(i), 88, deltaCalls: 5, deltaMs: 5.0, interval: 60);
            }

            var web = Root(await DarlingMcpPgTrendTools.GetPgQueryDurationTrend(postgres, ServerName, null, Hours, AsOfText, null, TrendBudget.Chart, ct));
            var (start, end) = Window();
            var read = await viewer.GetPgQueryDurationTrendAsync(ServerId, start, end, ct);

            var webPoints = web.GetProperty("points").EnumerateArray().ToArray();
            Assert.NotEmpty(webPoints);
            Assert.Equal("77", web.GetProperty("queryid").GetString());
            Assert.Equal(web.GetProperty("queryid").GetString(), read.Subject);
            Assert.Equal(web.GetProperty("bucket_minutes").GetInt32(), read.BucketMinutes);
            Assert.Equal(webPoints.Length, read.Points.Count);
            for (var i = 0; i < webPoints.Length; i++)
            {
                var w = webPoints[i];
                var v = read.Points[i];
                Assert.Equal(Stamp(v.CollectionTimeUtc), Stamp(w.GetProperty("collection_time")));
                Assert.Equal(w.GetProperty("calls").GetInt64(), v.Calls);
                Near(w.GetProperty("calls_per_second"), v.CallsPerSecond, 3);
                Near(w.GetProperty("total_exec_ms"), v.TotalExecMs, 3);
                Near(w.GetProperty("mean_exec_ms"), v.Calls > 0 ? v.MeanExecMs : null, 3);
                Near(w.GetProperty("peak_mean_exec_ms"), v.PeakMeanExecMs, 3);
            }
        });
    }

    [Fact]
    public async Task WaitTrend_MatchesTheWebRead()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            /* 'relation' (Lock) outweighs 'DataFileRead' (IO), so the choice is not the alphabetical first. */
            for (var i = 0; i < 5; i++)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms) VALUES ($1, $2, $3, $4, 'Lock', 'relation', 1, $5, 10, 2, $6)",
                    CollectionIdGenerator.Next(), T0.AddMinutes(i), ServerId, ServerName, 100L + 40L * i * (i + 1), 1_000L + 400L * i * (i + 1));
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO pg_wait_sampling (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms) VALUES ($1, $2, $3, $4, 'IO', 'DataFileRead', 1, $5, 10, 1, $6)",
                    CollectionIdGenerator.Next(), T0.AddMinutes(i), ServerId, ServerName, 10L * i, 100L * i);
            }

            var web = Root(await DarlingMcpPgTrendTools.GetPgWaitTrend(postgres, ServerName, null, Hours, AsOfText, ct));
            var (start, end) = Window();
            var read = await viewer.GetPgWaitTrendAsync(ServerId, start, end, ct);

            var webPoints = web.GetProperty("points").EnumerateArray().ToArray();
            Assert.NotEmpty(webPoints);
            Assert.Equal("relation", web.GetProperty("wait_event").GetString());
            Assert.Equal(web.GetProperty("wait_event").GetString(), read.Subject);
            Assert.Equal(webPoints.Length, read.Points.Count);
            for (var i = 0; i < webPoints.Length; i++)
            {
                var w = webPoints[i];
                var v = read.Points[i];
                Assert.Equal(Stamp(v.CollectionTimeUtc), Stamp(w.GetProperty("collection_time")));
                Assert.Equal(w.GetProperty("sample_count").GetInt64(), v.SampleCount);
                Assert.Equal(w.GetProperty("backend_count").GetInt32(), v.BackendCount);
                Near(w.GetProperty("estimated_wait_ms_per_second"), v.EstimatedWaitMsPerSecond, 3);
            }
        });
    }

    [Fact]
    public async Task IoTrend_MatchesTheWebRead_IncludingTheTimingGate()
    {
        await RunAsync(async (connection, postgres, viewer, ct) =>
        {
            /* Two pairs; the client backend moves more, so it is the subject. Read time is 0 throughout, which is what
               a server with track_io_timing off stores: the web nulls avg_read_ms there, and the viewer's read says so. */
            for (var i = 0; i < 5; i++)
            {
                await SeedIoAsync(connection, ct, T0.AddMinutes(i), "client backend", reads: 1_000 + 600 * i, hits: 10_000 + 500 * i);
                await SeedIoAsync(connection, ct, T0.AddMinutes(i), "checkpointer", reads: 50 + i, hits: 0);
            }

            var web = Root(await DarlingMcpPgTrendTools.GetPgIoTrend(postgres, ServerName, null, null, Hours, AsOfText, null, TrendBudget.Chart, ct));
            var (start, end) = Window();
            var read = await viewer.GetPgIoTrendAsync(ServerId, start, end, ct);

            var webPoints = web.GetProperty("points").EnumerateArray().ToArray();
            Assert.NotEmpty(webPoints);
            Assert.Equal(web.GetProperty("backend_type").GetString() + " / " + web.GetProperty("context").GetString(), read.Subject);
            Assert.Equal(web.GetProperty("bucket_minutes").GetInt32(), read.BucketMinutes);
            Assert.Equal(web.GetProperty("io_timing_tracked").GetBoolean(), read.TimingTracked);
            Assert.Equal(webPoints.Length, read.Points.Count);
            for (var i = 0; i < webPoints.Length; i++)
            {
                var w = webPoints[i];
                var v = read.Points[i];
                Assert.Equal(Stamp(v.CollectionTimeUtc), Stamp(w.GetProperty("collection_time")));
                Near(w.GetProperty("reads_per_second"), v.ReadsPerSecond, 3);
                Near(w.GetProperty("peak_reads_per_second"), v.PeakReadsPerSecond, 3);
                Near(w.GetProperty("writes_per_second"), v.WritesPerSecond, 3);
                Near(w.GetProperty("cache_hit_pct"), v.CacheHitPct, 2);
                Near(w.GetProperty("avg_read_ms"), read.TimingTracked ? v.AvgReadMs : null, 3);
                Near(w.GetProperty("evictions_per_second"), v.IntervalSeconds > 0 ? v.Evictions / v.IntervalSeconds : null, 3);
            }
        });
    }

    private static string Stamp(DateTime value) => value.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture);

    private static string Stamp(JsonElement value) => value.GetString()![..19];

    /// <summary>The web rounds to <paramref name="digits"/>; the viewer's value must round to the same number, and a
    /// null on one side is a null on the other.</summary>
    private static void Near(JsonElement web, double? viewer, int digits)
    {
        if (web.ValueKind == JsonValueKind.Null)
        {
            Assert.Null(viewer);
            return;
        }

        Assert.NotNull(viewer);
        Assert.Equal(web.GetDouble(), viewer.Value, digits);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static async Task SeedStatementAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, long queryId, long deltaCalls, double deltaMs, int interval) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO pg_statement_stats (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel, calls, total_exec_time_ms, min_exec_time_ms, max_exec_time_ms, mean_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, temp_blks_read, temp_blks_written, blk_read_time_ms, blk_write_time_ms, wal_records, wal_fpi, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, 1, 1, TRUE, 10, 100.0, 1.0, 20.0, 10.0, 100, 100, 10, 0, 0, 0, 0, 1.0, 0.0, 0, 0, 0, $6, $7, 50, $8)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, queryId, deltaCalls, deltaMs, interval);

    private static async Task SeedIoAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime t, string backendType, long reads, long hits) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, extends, op_bytes, hits, evictions, stats_reset)
VALUES ($1, $2, $3, $4, $5, 'relation', 'normal', $6, 0, 50, 0, 0, 8192, $7, 3, NULL)",
            CollectionIdGenerator.Next(), t, ServerId, ServerName, backendType, reads, hits);

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, ViewerDataService, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live viewer PostgreSQL trend tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        await using var viewer = new ViewerDataService(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await body(connection, postgres, viewer, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "pg_io_stats", "pg_database_stats", "pg_wait_sampling", "pg_statement_stats", "servers" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);
        }
    }
}
