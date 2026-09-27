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
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("read-latency", PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);

        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
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
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

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

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
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
    public async Task ASecondFlushInTheSameHour_AddsToTheFirst()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V148 same-hour add.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
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

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
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

    [Fact]
    public async Task AFlushWithNothingRecorded_WritesNoRows()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V148 empty-flush pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var accumulator = new ReadLatencyAccumulator();
        await accumulator.FlushAsync(connection, DateTime.UtcNow, logger: null, ct);

        using var read = new NpgsqlCommand("SELECT count(*) FROM collect.read_latency", connection);
        Assert.Equal(0L, await read.ExecuteScalarAsync(ct));
    }
}
