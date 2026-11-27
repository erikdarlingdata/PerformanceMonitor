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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The build split of <c>get_read_latency</c> (#9434): a bucket carries the start of the service process that wrote it, a restart
/// inside one hour writes a second row for that hour, and <c>split_at</c> divides each route's count, median and max by that start.
/// </summary>
public sealed class ReadLatencyBuildSplitLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Hour(int hoursAgo)
    {
        var now = DateTime.UtcNow;
        return DateTime.SpecifyKind(new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0).AddHours(-hoursAgo), DateTimeKind.Utc);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task ARestartWithinAnHour_WritesTwoRows_AndTheSameProcessStillAddsIntoOne()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the V315 key pin.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var hour = Hour(5);
        var a = new ReadLatencyAccumulator(hour.AddMinutes(-30));
        a.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100);
        await a.FlushAsync(connection, hour, logger: null, ct);
        a.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100);
        await a.FlushAsync(connection, hour, logger: null, ct);
        var b = new ReadLatencyAccumulator(hour.AddMinutes(20));
        b.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 2_000);
        await b.FlushAsync(connection, hour, logger: null, ct);

        await using var read = new NpgsqlCommand("SELECT run_count FROM collect.read_latency ORDER BY process_started_at", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(2, reader.GetInt32(0));
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(1, reader.GetInt32(0));
        Assert.False(await reader.ReadAsync(ct));
    }

    [Fact]
    public async Task TwoRowsOfOneKeyWithNoProcessStart_AreOneKey()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the V315 NULL key pin.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const string insert = "INSERT INTO collect.read_latency (metric_time, surface, route, outcome, run_count, total_ms, max_ms, bucket_counts) VALUES ('2026-10-01 05:00', 'web', 'r', 'ok', 1, 1, 1, ARRAY[1]::bigint[])";
        await ExecAsync(connection, insert, ct);
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, insert, ct));
        Assert.Equal("23505", ex.SqlState);
    }

    [Fact]
    public async Task TheSplit_DividesAMixedHourByProcessStart_AndPlacesOldRowsByTheirHour()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the get_read_latency split pin.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var h = Hour(10);
            var split = h.AddHours(2).AddMinutes(20);

            /* Process A started before the split: 3 reads of 100 ms in the first hour, 1 more in the next. */
            var a = new ReadLatencyAccumulator(h.AddMinutes(-30));
            for (var i = 0; i < 3; i++) { a.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100); }
            await a.FlushAsync(connection, h, logger: null, ct);
            a.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100);
            await a.FlushAsync(connection, h.AddHours(2), logger: null, ct);

            /* Process B started at the split, in the SAME hour A was still writing: 2 reads of 2000 ms there, 1 in the next hour. */
            var b = new ReadLatencyAccumulator(split);
            for (var i = 0; i < 2; i++) { b.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 2_000); }
            await b.FlushAsync(connection, h.AddHours(2), logger: null, ct);
            b.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 2_000);
            await b.FlushAsync(connection, h.AddHours(4), logger: null, ct);

            /* Rows stored before the start was recorded (NULL): placed by their hour. 5 reads an hour before, 7 in the hour that holds the split, 2 an hour after. */
            string Old(DateTime hour, int count, int max) =>
                $"INSERT INTO collect.read_latency (metric_time, surface, route, outcome, run_count, total_ms, max_ms, bucket_counts) VALUES ('{hour:yyyy-MM-dd HH:mm}', 'web', 'get_wait_stats', 'ok', {count}, {count * max}, {max}, ARRAY[0,0,0,{count}]::bigint[]);";
            await ExecAsync(connection, Old(h.AddHours(-1), 5, 50) + Old(h.AddHours(2).AddHours(0), 7, 50) + Old(h.AddHours(5), 2, 40), ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, route: "get_wait_stats", split_at: split.ToString("yyyy-MM-ddTHH:mm:ssZ"), cancellationToken: ct);
            using var document = JsonDocument.Parse(json);
            var row = document.RootElement.GetProperty("reads").EnumerateArray().Single();
            var s = row.GetProperty("split");

            /* Before: A's 3 + 1, the old hour's 5 and the mixed old hour's 7 (its hour starts before the split). After: B's 2 + 1 and the old later hour's 2. */
            Assert.Equal(16, s.GetProperty("before").GetProperty("count").GetInt64());
            Assert.Equal(100, s.GetProperty("before").GetProperty("max_ms").GetInt64());
            Assert.Equal(50, s.GetProperty("before").GetProperty("median_ms").GetInt64());
            Assert.Equal(5, s.GetProperty("after").GetProperty("count").GetInt64());
            Assert.Equal(2_000, s.GetProperty("after").GetProperty("max_ms").GetInt64());
            Assert.Equal(2_000, s.GetProperty("after").GetProperty("median_ms").GetInt64());
            Assert.Equal(7, s.GetProperty("ambiguous_count").GetInt64());
            Assert.Equal(21, row.GetProperty("count").GetInt64());

            /* A read with no split_at carries no split member and no split_at in the envelope. */
            using var plain = JsonDocument.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, route: "get_wait_stats", cancellationToken: ct));
            Assert.False(plain.RootElement.TryGetProperty("split_at", out _));
            Assert.False(plain.RootElement.GetProperty("reads").EnumerateArray().Single().TryGetProperty("split", out _));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /* A stored row's bucket array is always one longer than the bounds (the overflow bucket), so a hand-written row carries a full-length one. */
    private static string Buckets(int count) =>
        $"(SELECT array_agg(CASE WHEN i = 4 THEN {count} ELSE 0 END ORDER BY i)::bigint[] FROM generate_series(1, {ReadLatencyAccumulator.BucketUpperBoundsMs.Length + 1}) AS i)";

    private static string Utc(DateTime t) => t.ToString("yyyy-MM-ddTHH:mm:ss.FFFFFFF'Z'", System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public async Task EveryRead_ListsTheStoredProcessStarts_NewestFirstAndCapped_AndACopiedValueSplitsExactlyThere()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the process_starts pin.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var h = Hour(6);
            /* A started before B, and B's start carries microseconds: the value a caller copies must round-trip to the same instant. */
            var startA = h.AddMinutes(-30);
            var startB = h.AddMinutes(20).AddTicks(1230);

            var a = new ReadLatencyAccumulator(startA);
            for (var i = 0; i < 3; i++) { a.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100); }
            await a.FlushAsync(connection, h, logger: null, ct);
            var b = new ReadLatencyAccumulator(startB);
            for (var i = 0; i < 2; i++) { b.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 2_000); }
            await b.FlushAsync(connection, h, logger: null, ct);
            await ExecAsync(connection, "INSERT INTO collect.read_latency (metric_time, surface, route, outcome, run_count, total_ms, max_ms, bucket_counts) VALUES (date_trunc('hour', now()) - interval '2 hours', 'web', 'old_route', 'ok', 1, 1, 1, " + Buckets(1) + ")", ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* With no split_at: the two stored starts, newest first, and the old row's NULL start is not listed. */
            using (var plain = JsonDocument.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, cancellationToken: ct)))
            {
                var starts = plain.RootElement.GetProperty("process_starts").EnumerateArray().Select(e => e.GetString()).ToArray();
                Assert.Equal(new[] { Utc(startB), Utc(startA) }, starts);
            }

            /* A filter that matches no read still answers the starts, in the empty answer's hints. */
            using (var empty = JsonDocument.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, route: "no_such_route", cancellationToken: ct)))
            {
                Assert.Equal("empty", empty.RootElement.GetProperty("status").GetString());
                Assert.Equal(2, empty.RootElement.GetProperty("hints").GetProperty("process_starts").GetArrayLength());
            }

            /* The value copied from process_starts is the split: B's reads, and only B's, are after it. */
            using (var split = JsonDocument.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, route: "get_wait_stats", split_at: Utc(startB), cancellationToken: ct)))
            {
                var s = split.RootElement.GetProperty("reads").EnumerateArray().Single().GetProperty("split");
                Assert.Equal(3, s.GetProperty("before").GetProperty("count").GetInt64());
                Assert.Equal(2, s.GetProperty("after").GetProperty("count").GetInt64());
                Assert.Equal(2, split.RootElement.GetProperty("process_starts").GetArrayLength());
            }

            /* More than 20 starts: the newest 20, newest first. */
            await ExecAsync(connection, "INSERT INTO collect.read_latency (metric_time, surface, route, outcome, run_count, total_ms, max_ms, bucket_counts, process_started_at) SELECT date_trunc('hour', now()) - interval '1 hour', 'web', 'many', 'ok', 1, 1, 1, " + Buckets(1) + ", now() - interval '1 hour' - make_interval(mins => g) FROM generate_series(1, 25) AS g", ct);
            using (var capped = JsonDocument.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, cancellationToken: ct)))
            {
                var starts = capped.RootElement.GetProperty("process_starts").EnumerateArray()
                    .Select(e => DateTime.Parse(e.GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal)).ToArray();
                Assert.Equal(20, starts.Length);
                Assert.Equal(starts.OrderByDescending(x => x), starts);
                Assert.DoesNotContain(startA, starts);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AnOldBucketInTheHourThatStartsAtTheSplit_IsAfterAndNotAmbiguous()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the split boundary pin.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var h = Hour(8);
            string Old(DateTime hour, int count) =>
                $"INSERT INTO collect.read_latency (metric_time, surface, route, outcome, run_count, total_ms, max_ms, bucket_counts) VALUES ('{hour:yyyy-MM-dd HH:mm}', 'web', 'get_wait_stats', 'ok', {count}, {count * 50}, 50, {Buckets(count)});";
            /* The hour before the split (before), the hour that starts exactly at it (after), and the hour after it (after). */
            await ExecAsync(connection, Old(h.AddHours(-1), 5) + Old(h, 7) + Old(h.AddHours(1), 2), ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var exact = JsonDocument.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, route: "get_wait_stats", split_at: Utc(h), cancellationToken: ct));
            var s = exact.RootElement.GetProperty("reads").EnumerateArray().Single().GetProperty("split");
            Assert.Equal(5, s.GetProperty("before").GetProperty("count").GetInt64());
            Assert.Equal(9, s.GetProperty("after").GetProperty("count").GetInt64());
            Assert.Equal(0, s.GetProperty("ambiguous_count").GetInt64());

            /* One minute into that hour the same row is ambiguous: its hour holds the instant, so it counts as before. */
            using var inside = JsonDocument.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, route: "get_wait_stats", split_at: Utc(h.AddMinutes(1)), cancellationToken: ct));
            var t = inside.RootElement.GetProperty("reads").EnumerateArray().Single().GetProperty("split");
            Assert.Equal(12, t.GetProperty("before").GetProperty("count").GetInt64());
            Assert.Equal(2, t.GetProperty("after").GetProperty("count").GetInt64());
            Assert.Equal(7, t.GetProperty("ambiguous_count").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task TheSplitParameter_FollowsTheSurfaceAndRouteFilters_InEveryCombination(bool withSurface, bool withRoute)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the split parameter pin.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var h = Hour(6);
            var split = h.AddMinutes(20);

            /* Before the split: web get_wait_stats 3, web get_other 4, mcp get_wait_stats 5. After it: 1, 6 and 7. */
            var a = new ReadLatencyAccumulator(h.AddMinutes(-30));
            for (var i = 0; i < 3; i++) { a.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 100); }
            for (var i = 0; i < 4; i++) { a.Record(ReadSurface.Web, "get_other", ReadOutcome.Ok, 100); }
            for (var i = 0; i < 5; i++) { a.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 100); }
            await a.FlushAsync(connection, h, logger: null, ct);
            var b = new ReadLatencyAccumulator(split);
            b.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 2_000);
            for (var i = 0; i < 6; i++) { b.Record(ReadSurface.Web, "get_other", ReadOutcome.Ok, 2_000); }
            for (var i = 0; i < 7; i++) { b.Record(ReadSurface.Mcp, "get_wait_stats", ReadOutcome.Ok, 2_000); }
            await b.FlushAsync(connection, h, logger: null, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var answer = JsonDocument.Parse(await DarlingMcpReadLatencyTools.GetReadLatency(
                postgres, hours: 48, surface: withSurface ? "web" : null, route: withRoute ? "get_wait_stats" : null, split_at: Utc(split), cancellationToken: ct));
            var reads = answer.RootElement.GetProperty("reads").EnumerateArray()
                .ToDictionary(r => r.GetProperty("surface").GetString() + "/" + r.GetProperty("route").GetString(), r => r.GetProperty("split"));

            var expected = new System.Collections.Generic.Dictionary<string, (long Before, long After)>
            {
                ["web/get_wait_stats"] = (3, 1),
                ["web/get_other"] = (4, 6),
                ["mcp/get_wait_stats"] = (5, 7),
            }.Where(kv => (!withSurface || kv.Key.StartsWith("web/")) && (!withRoute || kv.Key.EndsWith("/get_wait_stats"))).ToDictionary(kv => kv.Key, kv => kv.Value);

            Assert.Equal(expected.Keys.OrderBy(k => k), reads.Keys.OrderBy(k => k));
            foreach (var (key, counts) in expected)
            {
                Assert.Equal(counts.Before, reads[key].GetProperty("before").GetProperty("count").GetInt64());
                Assert.Equal(counts.After, reads[key].GetProperty("after").GetProperty("count").GetInt64());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Theory]
    [InlineData("not a date")]
    [InlineData("01/02/2026")]
    public void AnUnparsableSplit_IsRefused(string value)
    {
        var error = DarlingMcpReadLatencyTools.ResolveSplitAt(value, DateTime.UtcNow.AddHours(-24), out var utc);
        Assert.NotNull(error);
        Assert.Null(utc);
    }

    [Fact]
    public void ASplitOutsideTheWindow_IsRefusedNotMoved_AndOneInsideIsKept()
    {
        var start = DateTime.UtcNow.AddHours(-24);
        Assert.NotNull(DarlingMcpReadLatencyTools.ResolveSplitAt(start.AddHours(-1).ToString("yyyy-MM-ddTHH:mm:ssZ"), start, out var early));
        Assert.Null(early);
        Assert.NotNull(DarlingMcpReadLatencyTools.ResolveSplitAt(DateTime.UtcNow.AddHours(2).ToString("yyyy-MM-ddTHH:mm:ssZ"), start, out var late));
        Assert.Null(late);
        Assert.Null(DarlingMcpReadLatencyTools.ResolveSplitAt(null, start, out var none));
        Assert.Null(none);
        var inside = start.AddHours(3);
        Assert.Null(DarlingMcpReadLatencyTools.ResolveSplitAt(inside.ToString("yyyy-MM-ddTHH:mm:ssZ"), start, out var kept));
        Assert.Equal(inside.ToString("yyyy-MM-ddTHH:mm:ss"), kept!.Value.ToString("yyyy-MM-ddTHH:mm:ss"));
    }
}
