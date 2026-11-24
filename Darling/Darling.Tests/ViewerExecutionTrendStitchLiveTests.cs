/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8821 on a live TimescaleDB store: the legacy hourly rollup stops at the upgrade and the interval-honest successor carries every
/// hour after it. The Query Duration chart reads the two as one stitched relation. The Executions chart used to read the legacy view
/// alone, so a window past the freeze drew Query Duration and told the viewer "No Executions data in this window". Both charts must
/// now draw the same hours.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It goes through ScratchPostgres.CreateAsync, which reaches
   DARLING_TEST_PG only to CREATE and DROP its own database and then works entirely inside it, so it cannot race the shared
   database. Leave it out; this comment is here so the next sweep does not "fix" it. */
public sealed class ViewerExecutionTrendStitchLiveTests
{
    private const int ServerId = -938821;
    private const string ServerName = "viewer-exec-trend-stitch";
    private const string Db = "StitchDb";

    /// <summary>Fixed anchor (never the wall clock): the first planted hour.</summary>
    private static readonly DateTime D0 = new(2026, 3, 2, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task ExecutionCountTrend_DrawsTheSameHours_AsQueryDuration_WhenTheLegacyRollupIsFrozen()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8821 stitch test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "The live #8821 stitch test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        /* Stop the scratch database's own scheduler before the ensure sweep creates refresh policies: the test refreshes each view
           over exactly the span it wants, and an auto-fired policy run would refresh the frozen legacy view past it. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* Twelve days, one row an hour: 8,000 ms elapsed and 36 executions, so a bucket reads 36 / 3,600 = 0.01 exec/sec. */
        await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    extract(epoch FROM g)::bigint,
    g,
    $3,
    $4,
    $5,
    decode(md5('q'), 'hex'),
    decode(md5('h'), 'hex'),
    1000,
    8000000,
    36,
    3600
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g", connection))
        {
            insert.Parameters.AddWithValue(D0);
            insert.Parameters.AddWithValue(D0.AddDays(12).AddHours(-1));
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(ServerName);
            insert.Parameters.AddWithValue(Db);
            await insert.ExecuteNonQueryAsync(ct);
        }

        var now = D0.AddDays(12);

        /* The legacy view holds only the first day (frozen at the upgrade). The successor holds the rest. */
        await RefreshAsync(connection, TimescaleSupport.QueryStatsHourlyView, D0, D0.AddDays(1), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, D0.AddDays(1), now, ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* Past week: the whole window is past the freeze. */
        var weekStart = now.AddDays(-7);
        var week = await viewer.GetQueryDurationTrendAsync(ServerId, weekStart, now, nowUtc: now, cancellationToken: ct);
        var weekExecutions = await viewer.GetExecutionCountTrendAsync(ServerId, weekStart, now, nowUtc: now, cancellationToken: ct);
        Assert.Equal(RetentionTier.Hourly, week.Tier);
        Assert.Equal(RetentionTier.Hourly, weekExecutions.Tier);
        Assert.NotEmpty(week.Points);
        Assert.Equal(week.Points.Count, weekExecutions.Points.Count);
        Assert.Equal(week.Points[0].CollectionTime, weekExecutions.Points[0].CollectionTime);
        Assert.All(weekExecutions.Points, p => Assert.Equal(36.0 / 3600.0, p.Value, 6));

        /* A window that spans the freeze reads the legacy hours first and the successor's after them. */
        var spanning = await viewer.GetQueryDurationTrendAsync(ServerId, D0, now, nowUtc: now, cancellationToken: ct);
        var spanningExecutions = await viewer.GetExecutionCountTrendAsync(ServerId, D0, now, nowUtc: now, cancellationToken: ct);
        Assert.Equal(12 * 24, spanning.Points.Count);
        Assert.Equal(spanning.Points.Count, spanningExecutions.Points.Count);
        Assert.Equal(spanning.Points[^1].CollectionTime, spanningExecutions.Points[^1].CollectionTime);
        Assert.All(spanningExecutions.Points, p => Assert.Equal(36.0 / 3600.0, p.Value, 6));
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }
}
