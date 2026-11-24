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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V148 (#4442 scope 2): <c>collect.read_latency</c>, the hourly read-latency histogram
/// the worker flushes on the same tick as <c>collector_cost</c>. This file is the RUNG (ladder, DDL, viewer
/// probe) and the FLUSH (<see cref="ReadLatencyAccumulator.FlushAsync"/>): a field-shaped record-and-flush
/// round trip, a same-hour second flush, retention, and an empty flush.
///
/// <para>This file's "I am the top rung" claim moved to <c>QueryStoreLivenessHotTouchLiveTests</c> (V149)
/// now that V149 has landed; this file's own rung/probe facts below keep asserting what stays true forever
/// (present, in-order, gated behind the arm above it) rather than "is exactly the top".</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each fact mints its own scratch database
   through ScratchPostgres and never touches the shared store's tables, so it cannot race the live collection
   and serializing it would be pure slowdown. */
public sealed class ReadLatencyFlushLiveTests
{
    private const int RungVersion = 148;
    private const int PreviousVersion = 147;

    /// <summary>This rung's sentinel ordinal in the viewer probe \u2014 the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 123;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The rung is registered and is the new top of the ladder \u2014 the claim this class takes over from
    /// <c>ComposeStatementTimeoutV147MigrationLiveTests</c> (V147) now that V148 has landed.
    /// </summary>
    [Fact]
    public void TheRungIsRegistered_AndTheLadderIsDenseAboveTheHistoricalGap()
    {
        Assert.Equal("read-latency", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung, and the map treats it as the TOP arm: a missing top arm
    /// maps a fully-migrated store one rung short, permanently, because <see cref="ViewerDataService.RequiredStoreSchemaVersion"/>
    /// is <see cref="StorageVersion.SchemaVersion"/>.
    /// </summary>
    [Fact]
    public void TheProbeCarriesThisRungsSentinel_AndTheArmSitsBelowTheCurrentTop()
    {
        Assert.Contains(
            "table_name = 'read_latency'",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal("hasReadLatency", method.GetParameters()[ProbeOrdinal].Name);

        /* Every rung above this one (V149's hasHotLivenessTouch, V150's
           hasCollectionLogWatermarkAndJobHistoryIndexes) must also be false, or the map finds a newer arm
           first and this assertion is checking the wrong rung's fallthrough. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        var behind = (object[])all.Clone();
        for (var i = ProbeOrdinal; i < arity; i++)
        {
            behind[i] = false;
        }
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* V150 (#4469, #4477) is now the top rung, so this arm no longer needs to be the LAST one — it only
           has to sit below the current top's arm, which is what the ladder-dense invariant above already
           guarantees is registered ahead of it. */
        var thisArm = viewer.IndexOf("if (hasReadLatency)", StringComparison.Ordinal);
        var topArm = viewer.IndexOf("if (hasCollectionLogWatermarkAndJobHistoryIndexes)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V148 sentinel arm \u2014 a fully-migrated store would map one rung short");
        Assert.True(topArm >= 0 && topArm < thisArm, "the current top rung's arm must sit above the V148 arm");
        Assert.Contains(
            "return " + RungVersion.ToString(System.Globalization.CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..(viewer.IndexOf("if (hasComposeTimeoutSixty)", StringComparison.Ordinal))], StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARecordAndFlushRoundTrip_MatchesCountsTotalsMaxesAndBucketsElementWise()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V148 flush round-trip.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 42);
        accumulator.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 4_200);
        accumulator.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Timeout, 61_000);
        accumulator.Record(ReadSurface.Compose, "custom-view", ReadOutcome.Ok, 900);

        var metricTime = new DateTime(2026, 1, 1, 5, 0, 0, DateTimeKind.Utc);
        await accumulator.FlushAsync(connection, metricTime, logger: null, ct);

        using var read = new NpgsqlCommand(
            "SELECT surface, route, outcome, run_count, total_ms, max_ms, bucket_counts FROM collect.read_latency ORDER BY surface, outcome", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);

        var rows = 0;
        while (await reader.ReadAsync(ct))
        {
            rows++;
            var surface = reader.GetString(0);
            var route = reader.GetString(1);
            var outcome = reader.GetString(2);
            var runCount = reader.GetInt32(3);
            var totalMs = reader.GetInt64(4);
            var maxMs = reader.GetInt64(5);
            var buckets = (long[])reader.GetValue(6);

            if (surface == "web" && outcome == "ok")
            {
                Assert.Equal("get_wait_stats", route);
                Assert.Equal(2, runCount);
                Assert.Equal(42 + 4_200, totalMs);
                Assert.Equal(4_200, maxMs);
                Assert.Equal(2, buckets.Sum());
            }
            else if (surface == "web" && outcome == "timeout")
            {
                Assert.Equal("get_wait_stats", route);
                Assert.Equal(1, runCount);
                Assert.Equal(61_000, totalMs);
                Assert.Equal(61_000, maxMs);
                Assert.Equal(1, buckets.Sum());
            }
            else if (surface == "compose")
            {
                Assert.Equal("custom-view", route);
                Assert.Equal("ok", outcome);
                Assert.Equal(1, runCount);
                Assert.Equal(900, totalMs);
                Assert.Equal(900, maxMs);
                Assert.Equal(1, buckets.Sum());
            }
            else
            {
                Assert.Fail($"unexpected row: {surface}/{route}/{outcome}");
            }
        }

        Assert.Equal(3, rows);
    }

    [Fact]
    public async Task AFallbackAndAGateFailure_AreStoredWithTheirOwnOutcomeLabels()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the read-latency outcome pin.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Mcp, "get_query_store_top", ReadOutcome.FallbackRaw, 40);
        accumulator.Record(ReadSurface.Mcp, "get_query_store_top", ReadOutcome.GateFailed, 40);
        await accumulator.FlushAsync(connection, new DateTime(2026, 1, 1, 5, 0, 0, DateTimeKind.Utc), logger: null, ct);

        using var read = new NpgsqlCommand("SELECT outcome FROM collect.read_latency ORDER BY outcome", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        var labels = new System.Collections.Generic.List<string>();
        while (await reader.ReadAsync(ct))
        {
            labels.Add(reader.GetString(0));
        }

        Assert.Equal(new[] { "fallback_raw", "gate_failed" }, labels);
    }

    [Fact]
    public async Task ASecondFlushInTheSameHour_AddsToTheFirst()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V148 same-hour add.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var metricTime = new DateTime(2026, 1, 1, 5, 0, 0, DateTimeKind.Utc);

        var first = new ReadLatencyAccumulator();
        first.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100);
        await first.FlushAsync(connection, metricTime, logger: null, ct);

        var second = new ReadLatencyAccumulator();
        second.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 200);
        await second.FlushAsync(connection, metricTime, logger: null, ct);

        using var read = new NpgsqlCommand(
            "SELECT run_count, total_ms, max_ms, bucket_counts FROM collect.read_latency WHERE surface = 'web' AND route = 'get_wait_stats' AND outcome = 'ok'", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(2, reader.GetInt32(0));
        Assert.Equal(300, reader.GetInt64(1));
        Assert.Equal(200, reader.GetInt64(2));
        var buckets = (long[])reader.GetValue(3);
        Assert.Equal(2, buckets.Sum());
        Assert.False(await reader.ReadAsync(ct));
    }

    [Fact]
    public async Task RetentionRemovesRowsOlderThan90Days_AndKeepsNewerOnes()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V148 retention pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = new DateTime(2026, 6, 1, 5, 0, 0, DateTimeKind.Utc);
        var old = new ReadLatencyAccumulator();
        old.Record(ReadSurface.Web, "old_route", ReadOutcome.Ok, 10);
        await old.FlushAsync(connection, now.AddDays(-91), logger: null, ct);

        var recent = new ReadLatencyAccumulator();
        recent.Record(ReadSurface.Web, "recent_route", ReadOutcome.Ok, 10);
        await recent.FlushAsync(connection, now.AddDays(-1), logger: null, ct);

        /* The retention DELETE in the recent flush's own call already pruned the old row (metric_time < $1
           uses the flush's own metricTimeUtc); flush once more at "now" with nothing recorded to prove it
           against the actual current time too. */
        var final = new ReadLatencyAccumulator();
        await final.FlushAsync(connection, now, logger: null, ct);

        using var read = new NpgsqlCommand("SELECT route FROM collect.read_latency", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        var routes = new System.Collections.Generic.List<string>();
        while (await reader.ReadAsync(ct))
        {
            routes.Add(reader.GetString(0));
        }

        Assert.DoesNotContain("old_route", routes);
        Assert.Contains("recent_route", routes);
    }

    /// <summary>#5506 (V182): the window band is written as part of the key, max_in_flight folds by MAX and wait_total_ms
    /// by addition when a second flush lands in the same hour, and two bands of one route are two rows. #5793 (V183):
    /// distinct_callers is each key's count of distinct callers in the flush (a read with no caller counts none), and a
    /// second flush of the same hour folds by GREATEST, because two partial sets cannot be unioned once counted.</summary>
    [Fact]
    public async Task FlushWritesTheBandInFlightAndWait_AndASecondFlushFoldsThem()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V182 capacity-field flush pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var metricTime = new DateTime(2026, 1, 1, 7, 0, 0, DateTimeKind.Utc);
        var first = new ReadLatencyAccumulator();
        first.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.OneDay, 4, 30, "client-a/1.0");
        first.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 150, ReadWindowBand.OneDay, 3, 0, "client-b/2.0");
        first.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 9_000, ReadWindowBand.SevenDays, 1, 0, null);
        await first.FlushAsync(connection, metricTime, logger: null, ct);

        var second = new ReadLatencyAccumulator();
        second.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 200, ReadWindowBand.OneDay, 2, 12, "client-a/1.0");
        await second.FlushAsync(connection, metricTime, logger: null, ct);

        using var read = new NpgsqlCommand(
            "SELECT window_band, run_count, max_in_flight, wait_total_ms, distinct_callers FROM collect.read_latency ORDER BY window_band",
            connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        var rows = new System.Collections.Generic.List<(string Band, int Runs, int MaxInFlight, long Wait, int Callers)>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2), reader.GetInt64(3), reader.GetInt32(4)));
        }

        /* 24h: two callers in the first flush and one in the second fold by GREATEST to 2 (a sum would say 3). */
        Assert.Equal(new[] { ("24h", 3, 4, 42L, 2), ("7d", 1, 1, 0L, 0) }, rows);
    }

    /// <summary>#9433 (V317): the reply bytes of two flushes of one hour add, and the read's mean covers only the hours that carry a size: an hour
    /// an older build wrote (NULL, no size) is left out of the average and never counted as an empty reply. The slow-read row keeps its size, and
    /// a row without one reads back null.</summary>
    [Fact]
    public async Task ReplyBytes_AreSummedAcrossFlushes_TheMeanSkipsHoursWithoutASize_AndTheSlowReadRowKeepsItsSize()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V317 reply-bytes pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var hour = DateTime.UtcNow.AddHours(-1);
        var metricTime = new DateTime(hour.Year, hour.Month, hour.Day, hour.Hour, 0, 0, DateTimeKind.Utc);

        /* An hour an older build wrote: the row names no reply total, so the column is NULL. 4 runs, no size. */
        await using (var older = new NpgsqlCommand(@"
INSERT INTO collect.read_latency (metric_time, surface, route, outcome, window_band, run_count, total_ms, max_ms, bucket_counts)
VALUES ($1, 'mcp', 'get_wait_stats', 'ok', 'none', 4, 400, 200, ARRAY[4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::bigint[]);", connection))
        {
            older.Parameters.AddWithValue(metricTime.AddHours(-1));
            await older.ExecuteNonQueryAsync(ct);
        }

        var first = new ReadLatencyAccumulator();
        first.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, 0, 0, null, replyBytes: 100);
        first.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, 0, 0, null, replyBytes: 300);
        await first.FlushAsync(connection, metricTime, logger: null, ct);
        var second = new ReadLatencyAccumulator();
        second.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, 0, 0, null, replyBytes: 200);
        await second.FlushAsync(connection, metricTime, logger: null, ct);

        using (var stored = new NpgsqlCommand("SELECT reply_bytes_total FROM collect.read_latency WHERE metric_time = $1", connection))
        {
            stored.Parameters.AddWithValue(metricTime);
            Assert.Equal(600L, await stored.ExecuteScalarAsync(ct));
        }

        var rows = await DarlingReadLatencyReader.GetSummaryAsync(postgres, metricTime.AddHours(-3), "mcp", "get_wait_stats", ct);
        var row = Assert.Single(rows);
        Assert.Equal(7, row.RunCount);
        Assert.Equal(600L, row.ReplyBytesTotal);
        Assert.Equal(3L, row.ReplyBytesCount);
        Assert.Equal(200L, row.MeanReplyBytes);

        var onlyOlder = await DarlingReadLatencyReader.GetSummaryAsync(postgres, metricTime.AddHours(-3), "mcp", "get_wait_stats", ct);
        Assert.NotNull(Assert.Single(onlyOlder).MeanReplyBytes);

        var log = new SlowReadLog(0);
        var scope = ReadScope.Open(null).Scope;
        await log.StoreAsync(postgres, SlowReadLog.Build(scope, ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 9_000, null, null, DateTime.UtcNow, null, 4_321), null);
        await log.StoreAsync(postgres, SlowReadLog.Build(scope, ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 9_000, null, null, DateTime.UtcNow.AddSeconds(-5)), null);
        var page = await DarlingSlowReadReader.GetAsync(postgres, DateTime.UtcNow.AddHours(-1), null, null, "get_wait_stats", 10, ct);
        Assert.Equal(new long?[] { 4_321, null }, page.Reads.Select(r => r.ReplyBytes).ToArray());
    }

    /// <summary>
    /// #9433: a read whose reply size was never taken (a dedicated web route sample, a composed panel) stores NULL in
    /// <c>reply_bytes_total</c>, not 0, so <c>mean_reply_bytes</c> reads null for the route instead of a false 0. A key that
    /// later flushes a measured size keeps that size, and an empty reply that WAS measured stays a real 0.
    /// </summary>
    [Fact]
    public async Task AnUnmeasuredReply_IsStoredAsNull_AndTheMeanIsNull_WhileAMeasuredEmptyReplyIsAZero()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #9433 unmeasured reply pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var hour = DateTime.UtcNow.AddHours(-1);
            var metricTime = new DateTime(hour.Year, hour.Month, hour.Day, hour.Hour, 0, 0, DateTimeKind.Utc);
            var context = new Microsoft.AspNetCore.Http.DefaultHttpContext();
            var scope = ReadScope.Open(null).Scope;

            /* Two dedicated-route samples take no reply size, and one MCP read measured an empty reply. */
            var first = new ReadLatencyAccumulator();
            var recorder = new ReadLatencyRecorder(first, logger: null);
            DarlingWebEndpoints.RecordWebRouteSample(recorder, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, scope, context);
            DarlingWebEndpoints.RecordWebRouteSample(recorder, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, scope, context);
            first.Record(ReadSurface.Mcp, "get_plan_cache", ReadOutcome.Ok, 100, ReadWindowBand.None, 0, 0, null, replyBytes: 0);
            await first.FlushAsync(connection, metricTime, logger: null, ct);

            async Task<object?> StoredTotal(string route)
            {
                using var stored = new NpgsqlCommand("SELECT reply_bytes_total FROM collect.read_latency WHERE route = $1", connection);
                stored.Parameters.AddWithValue(route);
                var value = await stored.ExecuteScalarAsync(ct);
                return value is DBNull ? null : value;
            }

            Assert.Null(await StoredTotal("get_wait_stats"));
            Assert.Equal(0L, await StoredTotal("get_plan_cache"));

            var rows = await DarlingReadLatencyReader.GetSummaryAsync(postgres, metricTime.AddHours(-1), null, null, ct);
            var unmeasured = Assert.Single(rows, r => r.Route == "get_wait_stats");
            Assert.Equal(2, unmeasured.RunCount);
            Assert.Null(unmeasured.ReplyBytesTotal);
            Assert.Equal(0L, unmeasured.ReplyBytesCount);
            Assert.Null(unmeasured.MeanReplyBytes);
            Assert.Equal(0L, Assert.Single(rows, r => r.Route == "get_plan_cache").MeanReplyBytes);

            /* A second flush of the same unmeasured key leaves NULL. A flush that measured one read adds that size to it. */
            var second = new ReadLatencyAccumulator();
            DarlingWebEndpoints.RecordWebRouteSample(new ReadLatencyRecorder(second, logger: null), "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, scope, context);
            await second.FlushAsync(connection, metricTime, logger: null, ct);
            Assert.Null(await StoredTotal("get_wait_stats"));

            var third = new ReadLatencyAccumulator();
            third.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, 0, 0, null, replyBytes: 500);
            await third.FlushAsync(connection, metricTime, logger: null, ct);
            Assert.Equal(500L, await StoredTotal("get_wait_stats"));

            /* The row now holds 3 unmeasured runs and one measured 500-byte read: the mean is that one read, not 500 over 4 runs. */
            var mixed = Assert.Single(await DarlingReadLatencyReader.GetSummaryAsync(postgres, metricTime.AddHours(-1), null, null, ct), r => r.Route == "get_wait_stats");
            Assert.Equal(4, mixed.RunCount);
            Assert.Equal(500L, mixed.ReplyBytesTotal);
            Assert.Equal(1L, mixed.ReplyBytesCount);
            Assert.Equal(500L, mixed.MeanReplyBytes);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9433: one measured read of 10,000 bytes and one read that took no size land on one key in one hour. The hour stores the
    /// 10,000 bytes and a measured count of 1, and the mean is 10,000: the unmeasured read is not a 0-byte reply. A window whose
    /// reads were all unmeasured reports a null total and a null mean, not 0.
    /// </summary>
    [Fact]
    public async Task AMeasuredAndAnUnmeasuredRead_OnOneKey_AverageTheMeasuredReadOnly()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #9433 mixed key pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var hour = DateTime.UtcNow.AddHours(-1);
            var metricTime = new DateTime(hour.Year, hour.Month, hour.Day, hour.Hour, 0, 0, DateTimeKind.Utc);

            var accumulator = new ReadLatencyAccumulator();
            accumulator.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, 0, 0, null, replyBytes: 10_000);
            accumulator.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, 0, 0, null);
            accumulator.Record(ReadSurface.Web, "get_memory_stats", ReadOutcome.Ok, 100, ReadWindowBand.None, 0, 0, null);
            await accumulator.FlushAsync(connection, metricTime, logger: null, ct);

            using (var stored = new NpgsqlCommand("SELECT reply_bytes_total, reply_bytes_count FROM collect.read_latency WHERE route = 'get_wait_stats'", connection))
            await using (var storedReader = await stored.ExecuteReaderAsync(ct))
            {
                Assert.True(await storedReader.ReadAsync(ct));
                Assert.Equal(10_000L, storedReader.GetInt64(0));
                Assert.Equal(1L, storedReader.GetInt64(1));
            }

            using (var unmeasuredStored = new NpgsqlCommand("SELECT reply_bytes_total IS NULL AND reply_bytes_count IS NULL FROM collect.read_latency WHERE route = 'get_memory_stats'", connection))
            {
                Assert.Equal(true, await unmeasuredStored.ExecuteScalarAsync(ct));
            }

            var rows = await DarlingReadLatencyReader.GetSummaryAsync(postgres, metricTime.AddHours(-1), null, null, ct);
            var mixed = Assert.Single(rows, r => r.Route == "get_wait_stats");
            Assert.Equal(2, mixed.RunCount);
            Assert.Equal(10_000L, mixed.ReplyBytesTotal);
            Assert.Equal(1L, mixed.ReplyBytesCount);
            Assert.Equal(10_000L, mixed.MeanReplyBytes);

            var unmeasured = Assert.Single(rows, r => r.Route == "get_memory_stats");
            Assert.Equal(1, unmeasured.RunCount);
            Assert.Null(unmeasured.ReplyBytesTotal);
            Assert.Null(unmeasured.MeanReplyBytes);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AFlushWithNothingRecorded_WritesNoRows()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V148 empty-flush pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var accumulator = new ReadLatencyAccumulator();
        await accumulator.FlushAsync(connection, DateTime.UtcNow, logger: null, ct);

        using var read = new NpgsqlCommand("SELECT count(*) FROM collect.read_latency", connection);
        Assert.Equal(0L, await read.ExecuteScalarAsync(ct));
    }

    /// <summary>
    /// #5506 part 3: <see cref="DarlingReadLatencyReader.GetSummaryAsync"/>'s SQL keeps the rows that are not a read running
    /// (queue_wait, busy, cache_hit, cancelled) out of the run count, the run histogram, total, max, max_in_flight and
    /// wait_total_ms, and counts them in their own columns, with the queue wait's own histogram; and
    /// <see cref="DarlingMcpReadLatencyTools.GetReadLatency"/> projects them. A route with only such rows still answers, with
    /// a run count of zero.
    /// </summary>
    [Fact]
    public async Task TheReader_KeepsAdmissionRowsOutOfTheRunFigures_AndCountsThemOnTheirOwn()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 read-latency reader pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const string route = "get_wait_stats";
        var band = ReadWindowBand.OneDay;
        var accumulator = new ReadLatencyAccumulator();
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Ok, 100, band, 2, 30, "client-a/1.0");
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Ok, 3_000, band, 1, 0, "client-a/1.0");
        /* Each admission row carries an in-flight count and a wait that must not reach the run figures. */
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.QueueWait, 400, band, 9, 500, "client-a/1.0");
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.QueueWait, 400, band, 9, 500, "client-a/1.0");
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.QueueWait, 2_500, band, 9, 500, "client-a/1.0");
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Busy, 45_000, band, 9, 500, "client-a/1.0");
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Cancelled, 60_000, band, 9, 500, "client-a/1.0");
        /* A cache hit is stored as the text cache_hit; Limit stands in for it here and is relabelled below. */
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Limit, 70_000, band, 9, 500, "client-a/1.0");
        accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Limit, 70_000, band, 9, 500, "client-a/1.0");
        accumulator.Record(ReadSurface.Web, "get_fleet_overview", ReadOutcome.Busy, 0, ReadWindowBand.OneHour, 0, 0, null);
        await accumulator.FlushAsync(connection, DateTime.UtcNow.AddHours(-1), logger: null, ct);
        await using (var relabel = new NpgsqlCommand("UPDATE collect.read_latency SET outcome = 'cache_hit' WHERE outcome = 'limit'", connection))
        {
            Assert.Equal(1, await relabel.ExecuteNonQueryAsync(ct));
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rows = await DarlingReadLatencyReader.GetSummaryAsync(postgres, DateTime.UtcNow.AddHours(-24), surface: null, route: null, ct);

        var wait = Assert.Single(rows, r => r.Route == route);
        Assert.Equal((2L, 3_100L, 3_000L, 2, 30L), (wait.RunCount, wait.TotalMs, wait.MaxMs, wait.MaxInFlight, wait.WaitTotalMs));
        Assert.Equal(2L, wait.BucketCounts.Sum());
        Assert.Equal((3L, 3_300L, 2_500L), (wait.Queued, wait.QueueWaitMs, wait.QueueWaitMaxMs));
        Assert.Equal(3L, wait.QueueWaitBucketCounts!.Sum());
        Assert.Equal((1L, 2L, 1L), (wait.Rejected, wait.CacheHits, wait.Cancelled));

        var fleet = Assert.Single(rows, r => r.Route == "get_fleet_overview");
        Assert.Equal((0L, 0L, 1L), (fleet.RunCount, fleet.MaxMs, fleet.Rejected));
        Assert.Equal(0L, fleet.BucketCounts.Sum());

        var json = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 24, route: route, cancellationToken: ct);
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var row = Assert.Single(document.RootElement.GetProperty("reads").EnumerateArray());
        Assert.Equal(2, row.GetProperty("count").GetInt64());
        Assert.Equal(3_000, row.GetProperty("p95_ms").GetInt64());
        Assert.Equal(30, row.GetProperty("wait_total_ms").GetInt64());
        Assert.Equal(3, row.GetProperty("queued").GetInt64());
        Assert.Equal(3_300, row.GetProperty("queue_wait_ms").GetInt64());
        /* 400, 400 and 2,500 ms: the 95th percentile lands in the (2,000, 3,000] bucket, capped to the 2,500 ms maximum. */
        Assert.Equal(2_500, row.GetProperty("queue_wait_p95_ms").GetInt64());
        Assert.Equal(1, row.GetProperty("rejected").GetInt64());
        Assert.Equal(2, row.GetProperty("cache_hits").GetInt64());
        Assert.Equal(1, row.GetProperty("cancelled").GetInt64());

        /* The empty answer carries the live admission object too, under hints. */
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, logger: null);
        var empty = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 24, route: "no_such_route", admission: admission, cancellationToken: ct);
        using var emptyDocument = System.Text.Json.JsonDocument.Parse(empty);
        Assert.Equal("empty", emptyDocument.RootElement.GetProperty("status").GetString());
        var live = emptyDocument.RootElement.GetProperty("hints").GetProperty("admission");
        Assert.Equal((ReadAdmission.DefaultMaxHeavyReads, 0, 0), (live.GetProperty("limit").GetInt32(), live.GetProperty("running").GetInt32(), live.GetProperty("queued").GetInt32()));
    }

    /// <summary>
    /// #5506 part 3: <see cref="ReadAdmission.ClassificationSql"/> run against PostgreSQL. The long-read table is the 7-day
    /// p90 per (surface, route, band) from flushed ok, fallback_raw and gate_failed rows only: errors, timeouts and the
    /// admission rows do not move it, the two surfaces keep separate entries, and a row older than 7 days is not read
    /// (that pair falls back to the window-size rule).
    /// </summary>
    [Fact]
    public async Task TheLongReadTable_IsThe7DayP90PerSurfaceRouteAndBand_FromCompletedReadsOnly()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 long-read table pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var day = ReadWindowBand.OneDay;
        var recent = new ReadLatencyAccumulator();
        for (var i = 0; i < 8; i++)
        {
            recent.Record(ReadSurface.Web, "get_heavy", ReadOutcome.Ok, 2_500, day, 0, 0);
        }

        recent.Record(ReadSurface.Web, "get_heavy", ReadOutcome.FallbackRaw, 2_500, day, 0, 0);
        recent.Record(ReadSurface.Web, "get_heavy", ReadOutcome.GateFailed, 2_500, day, 0, 0);
        for (var i = 0; i < 30; i++)
        {
            /* Fast errors would pull the p90 under the threshold if they were counted. */
            recent.Record(ReadSurface.Web, "get_heavy", ReadOutcome.Error, 100, day, 0, 0);
        }

        for (var i = 0; i < 10; i++)
        {
            recent.Record(ReadSurface.Web, "get_light", ReadOutcome.Ok, 100, day, 0, 0);
            recent.Record(ReadSurface.Web, "get_light", ReadOutcome.QueueWait, 60_000, day, 0, 0);
            recent.Record(ReadSurface.Web, "get_light", ReadOutcome.Busy, 60_000, day, 0, 0);
            recent.Record(ReadSurface.Web, "get_light", ReadOutcome.Timeout, 60_000, day, 0, 0);
            recent.Record(ReadSurface.Web, "get_light", ReadOutcome.Cancelled, 60_000, day, 0, 0);
            recent.Record(ReadSurface.Mcp, "get_heavy", ReadOutcome.Ok, 100, day, 0, 0);
        }

        await recent.FlushAsync(connection, DateTime.UtcNow.AddHours(-1), logger: null, ct);

        var old = new ReadLatencyAccumulator();
        old.Record(ReadSurface.Web, "get_old", ReadOutcome.Ok, 9_000, ReadWindowBand.OneHour, 0, 0);
        await old.FlushAsync(connection, DateTime.UtcNow.AddDays(-8), logger: null, ct);

        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, logger: null);
        Assert.True(await admission.RefreshAsync(connection, live: null, ct));

        /* Ten completed reads of 2,500 ms: the p90 bucket is (2,000, 3,000], capped to the 2,500 ms maximum. */
        Assert.True(admission.IsHeavy(ReadSurface.Web, "get_heavy", day, out var heavyMs));
        Assert.Equal(2_500, heavyMs);
        Assert.False(admission.IsHeavy(ReadSurface.Web, "get_light", day, out var lightMs));
        Assert.Equal(100, lightMs);
        Assert.False(admission.IsHeavy(ReadSurface.Mcp, "get_heavy", day, out var mcpMs));
        Assert.Equal(100, mcpMs);

        /* No row in the last 7 days: the window-size rule, with the threshold as the expected time. */
        Assert.False(admission.IsHeavy(ReadSurface.Web, "get_old", ReadWindowBand.OneHour, out var oldMs));
        Assert.Equal(ReadAdmission.HeavyThresholdMs, oldMs);
        Assert.True(admission.IsHeavy(ReadSurface.Web, "get_heavy", ReadWindowBand.SevenDays, out _));
    }
}
