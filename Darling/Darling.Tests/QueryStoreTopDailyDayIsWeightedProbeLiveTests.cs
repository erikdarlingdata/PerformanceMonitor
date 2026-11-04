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
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7379: <see cref="QueryStoreTopDaily.DayIsWeightedSql"/> decides from one probe row of a day which builder wrote it, so the
/// plan of the V240 rebuild does not read every summary row of every day built before V240. The facts: the probe answers what
/// the earlier text (kept below as the reference) answers for every shape of day, and the plan finds many old days at a buffer
/// cost that does not grow with the rows per day.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live fact here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it, so it cannot race live
   collection. */
public sealed class QueryStoreTopDailyDayIsWeightedProbeLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the day-is-weighted probe's live pins (each mints its own scratch database).";

    private const int RetentionDays = 9;

    private static readonly DateTime Now = new(2026, 1, 20, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The text before #7379, kept verbatim as the reference the probe must agree with.</summary>
    private const string ReferenceDayIsWeightedSql = @"(
    EXISTS (SELECT 1 FROM collect.query_store_top_daily AS w
            WHERE w.server_id = b.server_id AND w.day = b.day
              AND (w.avg_duration_us_wsum IS NOT NULL OR w.avg_cpu_time_us_wsum IS NOT NULL OR w.avg_logical_io_reads_wsum IS NOT NULL
                OR w.avg_logical_io_writes_wsum IS NOT NULL OR w.avg_physical_io_reads_wsum IS NOT NULL OR w.avg_rowcount_wsum IS NOT NULL))
    OR NOT EXISTS (SELECT 1 FROM collect.query_store_top_daily AS o
            WHERE o.server_id = b.server_id AND o.day = b.day AND o.execution_count_sum IS NOT NULL
              AND ((o.avg_duration_us_n > 0 AND o.avg_duration_us_wsum IS NULL) OR (o.avg_cpu_time_us_n > 0 AND o.avg_cpu_time_us_wsum IS NULL)
                OR (o.avg_logical_io_reads_n > 0 AND o.avg_logical_io_reads_wsum IS NULL) OR (o.avg_logical_io_writes_n > 0 AND o.avg_logical_io_writes_wsum IS NULL)
                OR (o.avg_physical_io_reads_n > 0 AND o.avg_physical_io_reads_wsum IS NULL) OR (o.avg_rowcount_n > 0 AND o.avg_rowcount_wsum IS NULL))))";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task RunLiveAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The columns that differ between builders, as SQL expressions. Every row has two intervals behind it.</summary>
    private sealed record Shape(string ExecutionCount, int AverageN, string WeightedSum);

    private static readonly Shape V240Day = new("200", 2, "2000");
    private static readonly Shape OlderDay = new("200", 2, "NULL");
    private static readonly Shape NullCountDay = new("NULL", 2, "NULL");
    private static readonly Shape FallbackNewDay = new("200", 1, "1000");
    private static readonly Shape FallbackOlderDay = new("200", 1, "NULL");
    private static readonly Shape FallbackNoAverageDay = new("200", 0, "NULL");

    private static async Task SeedDayAsync(NpgsqlConnection connection, int serverId, DateOnly day, Shape? shape, int rows, CancellationToken ct)
    {
        await ExecAsync(connection,
            $"INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) VALUES ({serverId}, DATE '{day:yyyy-MM-dd}', 2, TIMESTAMP '2026-01-05 00:00:00', {rows * 2})", ct);
        if (shape is null)
        {
            return;
        }

        var n = shape.AverageN.ToString(CultureInfo.InvariantCulture);
        var w = shape.WeightedSum;
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_top_daily
(server_id, day, query_id, plan_id, interval_rows, execution_count_sum,
 avg_duration_us_sum, avg_duration_us_n, avg_cpu_time_us_sum, avg_cpu_time_us_n,
 avg_logical_io_reads_sum, avg_logical_io_reads_n, avg_logical_io_writes_sum, avg_logical_io_writes_n,
 avg_physical_io_reads_sum, avg_physical_io_reads_n, avg_rowcount_sum, avg_rowcount_n,
 avg_duration_us_wsum, avg_cpu_time_us_wsum, avg_logical_io_reads_wsum, avg_logical_io_writes_wsum,
 avg_physical_io_reads_wsum, avg_rowcount_wsum)
SELECT {serverId}, DATE '{day:yyyy-MM-dd}', g, g, 2, {shape.ExecutionCount},
 20, {n}, 20, {n}, 20, {n}, 20, {n}, 20, {n}, 20, {n},
 {w}, {w}, {w}, {w}, {w}, {w}
FROM generate_series(1, {rows}) AS g", ct);
    }

    private static async Task<bool> AnswerAsync(NpgsqlConnection connection, string predicate, int serverId, DateOnly day, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT {predicate} FROM collect.query_store_top_daily_built AS b WHERE b.server_id = {serverId} AND b.day = DATE '{day:yyyy-MM-dd}'", connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    [Theory]
    [InlineData("v240", true)]
    [InlineData("older", false)]
    [InlineData("empty", true)]
    [InlineData("nullcount", true)]
    [InlineData("fallbacknew", true)]
    [InlineData("fallbackolder", false)]
    [InlineData("fallbacknoaverage", true)]
    public async Task TheProbe_AnswersWhatTheEarlierTextAnswers_ForEveryShapeOfDay(string shapeName, bool expected)
    {
        await RunLiveAsync(async (connection, ct) =>
        {
            Shape? shape = shapeName switch
            {
                "v240" => V240Day,
                "older" => OlderDay,
                "empty" => null,
                "nullcount" => NullCountDay,
                "fallbacknew" => FallbackNewDay,
                "fallbackolder" => FallbackOlderDay,
                "fallbacknoaverage" => FallbackNoAverageDay,
                _ => throw new ArgumentOutOfRangeException(nameof(shapeName)),
            };
            var day = new DateOnly(2026, 1, 15);
            await SeedDayAsync(connection, 1, day, shape, 20, ct);

            Assert.Equal(expected, await AnswerAsync(connection, ReferenceDayIsWeightedSql, 1, day, ct));
            Assert.Equal(expected, await AnswerAsync(connection, QueryStoreTopDaily.DayIsWeightedSql, 1, day, ct));
        });
    }

    /// <summary>
    /// Plans the rebuild over a store of old days (<paramref name="rowsPerDay"/> summary rows each) and returns the buffers
    /// the statement touched, after checking it found every old day, newest first.
    /// </summary>
    private static async Task<long> PlanBuffersAsync(int rowsPerDay)
    {
        long buffers = 0;
        await RunLiveAsync(async (connection, ct) =>
        {
            /* Five servers by ten days (Jan 10 to Jan 19 for now = Jan 20 12:00 with 9 retention days): 50 old days, under the cap. */
            for (var serverId = 1; serverId <= 5; serverId++)
            {
                await ExecAsync(connection, $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({serverId}, 'srv{serverId}', TRUE)", ct);
                await ExecAsync(connection,
                    $"INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES ({serverId}, TIMESTAMP '2025-12-01 00:00:00', TIMESTAMP '2026-03-01 00:00:00')", ct);
                for (var d = 10; d <= 19; d++)
                {
                    await SeedDayAsync(connection, serverId, new DateOnly(2026, 1, d), OlderDay, rowsPerDay, ct);
                }
            }

            await ExecAsync(connection, "ANALYZE collect.query_store_top_daily; ANALYZE collect.query_store_top_daily_built", ct);

            var found = await QueryStoreTopDaily.PlanBuildsAsync(connection, Now, RetentionDays, ct);
            Assert.Equal(50, found.Count);
            Assert.Equal(found.Select(b => b.Day).OrderByDescending(d => d).ToArray(), found.Select(b => b.Day).ToArray());
            Assert.Equal(Enumerable.Range(10, 10).ToHashSet(), found.Select(b => b.Day.Day).ToHashSet());

            await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + QueryStoreTopDaily.PlanSql, connection) { CommandTimeout = 120 };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Now });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = RetentionDays });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = QueryStoreTopDaily.BuildAfter });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = QueryStoreTopDaily.FinalBuildAfter });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = QueryStoreTopDaily.MaxBuildsPerTick });
            var json = (string)(await command.ExecuteScalarAsync(ct))!;
            using var document = JsonDocument.Parse(json);
            var plan = document.RootElement[0].GetProperty("Plan");
            buffers = plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();
        });
        return buffers;
    }

    /// <summary>
    /// #7379: the plan's cost is a probe per old day, not a read of the day. Ten times the rows per day must not cost ten times
    /// the buffers: the earlier text read every row of every old day, so it grew with the rows. Buffers, never milliseconds.
    /// </summary>
    [Fact]
    public async Task ThePlan_FindsManyOlderDays_AtABufferCostThatDoesNotGrowWithTheRowsPerDay()
    {
        var small = await PlanBuffersAsync(50);
        var large = await PlanBuffersAsync(500);

        TestContext.Current.SendDiagnosticMessage($"PlanSql buffers over 50 old days: {small} at 50 rows per day, {large} at 500.");
        Assert.True(large <= small * 2,
            $"Planning 50 old days read {small} buffers at 50 rows per day and {large} at 500: the probe must not grow with the rows per day.");
    }
}
