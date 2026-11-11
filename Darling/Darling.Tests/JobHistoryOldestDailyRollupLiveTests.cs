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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Job History "Data starts" note reads each server's oldest run from <c>collect.job_history_oldest_daily</c> for the days the
/// rollup holds whole, and from raw <c>job_history</c> for every other day. The answer must be the answer the ordered one-row reads give:
/// a late collection holding an older run, the day at raw's retention edge (whose bucket still counts rows retention dropped), a store whose
/// rollup is the plain view, and a backfill over a store that already holds history.
///
/// <para><b>Own store</b> (<c>#1776 own-store</c>): each test mints its own scratch database through <see cref="ScratchPostgres"/>.</para>
/// </summary>
public sealed class JobHistoryOldestDailyRollupLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8549 job history tests.";

    [Fact]
    public void TheRangeRule_StartsAfterRawsEdgeDay_AndEndsAfterTheLastBucket()
    {
        var edge = new DateTime(2026, 11, 1, 13, 0, 0, DateTimeKind.Unspecified);

        /* The rollup starts before raw's oldest day: the first usable bucket is the day after it. */
        Assert.Equal((new DateTime(2026, 11, 2), new DateTime(2026, 11, 10)),
            ViewerDataService.JobHistoryOldestDailyRange(new DateTime(2026, 10, 25), new DateTime(2026, 11, 9), edge)!.Value);

        /* The rollup starts after it (a backfill still running, or a young rollup): its own first bucket. */
        Assert.Equal((new DateTime(2026, 11, 5), new DateTime(2026, 11, 10)),
            ViewerDataService.JobHistoryOldestDailyRange(new DateTime(2026, 11, 5), new DateTime(2026, 11, 9), edge)!.Value);

        /* No bucket past raw's edge day: the rollup is not read. */
        Assert.Null(ViewerDataService.JobHistoryOldestDailyRange(new DateTime(2026, 10, 25), new DateTime(2026, 11, 1), edge));
    }

    [Fact]
    public async Task TheRollupRead_EqualsTheOrderedReads_ForALateOlderRun_TheRetentionEdge_AndABackfillOverHistory()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = await BuildTimescaleStoreAsync(scratch, ct);
        await SeedAsync(connection, ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* The aggregate is there, empty (WITH NO DATA): no usable range, every day reads raw. */
        Assert.True(await ScalarAsync<bool>(connection, ViewerDataService.JobHistoryOldestDailyIsAggregateSql, ct), "a hypertable store holds the aggregate");
        Assert.Null(await viewer.ReadJobHistoryOldestDailyRangeAsync(ct));
        await AssertSameAnswersAsync(viewer, connection, ct);

        /* The backfill over history that is already there fills the closed days, once. */
        Assert.True(await TimescaleSupport.BackfillJobHistoryOldestDailyAsync(connection, null, LiveClock.Now(), ct));
        Assert.False(await TimescaleSupport.BackfillJobHistoryOldestDailyAsync(connection, null, LiveClock.Now(), ct), "and converges");
        var range = await viewer.ReadJobHistoryOldestDailyRangeAsync(ct);
        Assert.NotNull(range);
        Assert.Equal(LiveClock.Now().Date, range!.Value.Hi);
        await AssertSameAnswersAsync(viewer, connection, ct);

        /* A late collection: today's rows for server 8002 carry a run from a year back. It is in the open day, which reads raw. */
        await ExecuteAsync(connection, @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, run_datetime, run_status)
VALUES (860000001, (now() AT TIME ZONE 'utc'), 8002, 'jh-oldest', 1, 'j', 'late job', (now() AT TIME ZONE 'utc') - interval '365 days', 1)", ct);
        var late = (await viewer.GetJobHistoryOldestRunsAsync(null, ct)).Single(r => r.ServerId == 8002).RunWall;
        Assert.True(late < LiveClock.Now().AddDays(-300), "the open day's older run is the answer");
        await AssertSameAnswersAsync(viewer, connection, ct);

        /* Retention drops the oldest raw chunks. The rollup still holds buckets for them; the read must not take a bucket on raw's edge day or
           before it, so the answer stays what raw holds. */
        await ExecuteAsync(connection, "SELECT drop_chunks('collect.job_history', older_than => (now() AT TIME ZONE 'utc') - interval '12 days')", ct);
        Assert.True(await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_history_oldest_daily WHERE bucket < date_trunc('day', (SELECT min(collection_time) FROM collect.job_history))", ct) > 0,
            "the rollup still holds buckets raw no longer has");
        await AssertSameAnswersAsync(viewer, connection, ct);

        /* The retention pass trims the rollup to the same horizon. */
        await ExecuteAsync(connection, PerformanceMonitor.Darling.Service.DarlingRetention.DropJobHistoryOldestDailySql(12), ct);
        Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) FROM collect.job_history_oldest_daily WHERE bucket < date_trunc('day', now() AT TIME ZONE 'utc') - interval '25 days'", ct));
        await AssertSameAnswersAsync(viewer, connection, ct);
    }

    [Fact]
    public async Task OnAPlainStore_TheRollupIsThePlainView_AndTheNoteTakesTheOrderedReads()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);

        Assert.True(await ScalarAsync<bool>(connection, "SELECT to_regclass('collect.job_history_oldest_daily') IS NOT NULL", ct), "the rung leaves the relation on every store");
        Assert.False(await ScalarAsync<bool>(connection, ViewerDataService.JobHistoryOldestDailyIsAggregateSql, ct), "and on a store with no hypertable it is the plain view");

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        Assert.Null(await viewer.ReadJobHistoryOldestDailyRangeAsync(ct));
        await AssertSameAnswersAsync(viewer, connection, ct);
    }

    /// <summary>The rollup-aware read returns the same (server, oldest run) rows as the two ordered one-row reads it replaces, for the fleet and for each server.</summary>
    private static async Task AssertSameAnswersAsync(ViewerDataService viewer, NpgsqlConnection connection, CancellationToken ct)
    {
        var expected = new Dictionary<int, DateTime>();
        await using (var command = new NpgsqlCommand(ViewerDataService.JobHistoryOldestRunsFleetSql, connection) { CommandTimeout = 300 })
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                if (!reader.IsDBNull(1))
                {
                    expected[reader.GetInt32(0)] = reader.GetDateTime(1);
                }
            }
        }

        Assert.NotEmpty(expected);
        var actual = (await viewer.GetJobHistoryOldestRunsAsync(null, ct)).ToDictionary(r => r.ServerId, r => r.RunWall);
        Assert.Equal(expected.OrderBy(p => p.Key), actual.OrderBy(p => p.Key));

        foreach (var (serverId, oldest) in expected)
        {
            var one = await viewer.GetJobHistoryOldestRunsAsync(serverId, ct);
            Assert.Equal(oldest, Assert.Single(one).RunWall);
        }
    }

    private static async Task<NpgsqlConnection> BuildTimescaleStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live #8549 job history tests need TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        return connection;
    }

    /// <summary>Three servers, 20 days of an hourly collection each. Server 8001's first collection (the oldest day) holds runs from 90 days back, as a
    /// first collection copies msdb's history; server 8002's runs follow the collection; server 8003 has a NULL run on its oldest row. Compresses the
    /// chunks older than two days where the store can.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecuteAsync(connection, "INSERT INTO servers (server_id, server_name, display_name) VALUES (8001, 'jh-oldest-a', 'a'), (8002, 'jh-oldest-b', 'b'), (8003, 'jh-oldest-c', 'c') ON CONFLICT DO NOTHING", ct);
        await ExecuteAsync(connection, @"
INSERT INTO collect.job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, run_datetime, run_status)
SELECT 860000100 + row_number() OVER (), t, s.server_id, 'jh-oldest', 1, 'j', 'job',
       CASE WHEN s.server_id = 8001 AND t < date_trunc('day', now() AT TIME ZONE 'utc') - interval '19 days' THEN t - interval '90 days'
            WHEN s.server_id = 8003 AND t = date_trunc('day', now() AT TIME ZONE 'utc') - interval '19 days' THEN NULL
            ELSE t - interval '30 minutes' END,
       1
FROM generate_series(date_trunc('day', now() AT TIME ZONE 'utc') - interval '19 days', now() AT TIME ZONE 'utc', interval '1 hour') AS t
CROSS JOIN (VALUES (8001), (8002), (8003)) AS s(server_id)", ct);

        if (await ScalarAsync<bool>(connection, "SELECT to_regclass('timescaledb_information.hypertables') IS NOT NULL", ct)
            && await ScalarAsync<bool>(connection, "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'job_history')", ct))
        {
            await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
            await ExecuteAsync(connection,
                "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('collect.job_history', older_than => (now() AT TIME ZONE 'utc') - interval '2 days') AS c", ct);
        }
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
