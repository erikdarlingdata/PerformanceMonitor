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
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the V306 key pin.");
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
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the V306 NULL key pin.");
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
