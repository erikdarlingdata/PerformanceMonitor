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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#9523: a prior day whose runs booked no sql time is not a sql-cost measurement, for the paging
/// read or the digest read.
///
/// <para>The AWS-API collectors (pg_deadlocks, pg_log_events, pg_cpu_utilization on RDS and Aurora) book
/// their AWS call as sql time, and a store whose history holds days of zero sql time for them, written by a
/// build that books that call as storage time, has to read their first days of sql time without paging on
/// every server and without filling the digest. The fixtures are one-minute collectors, 1,440 runs a day at
/// 680 ms a run, the shape of those three.</para>
///
/// <para><b>Booking day 1</b> is the case the paging read would otherwise report: twelve zero days, then a
/// day holding 30% of the new booking (an upgrade late in the UTC day), then the first full day. The zero days
/// drag the baseline mean to about 16 ms a run, and the 204 ms partial day is the worst prior day, so the
/// full day clears both conjuncts at 2.0 and the added-cost floor by three orders of magnitude.
/// <b>Booking day 4</b> is three full days later, where the percentile conjunct holds it on its own and the
/// digest's baseline has to be the four measured days, not thirteen. <b>Real rise</b> is the control: a
/// collector measured at 100 ms a run on every prior day that rises to 680, reported by both reads.</para></summary>
[Collection("live-postgres")]
public sealed class CollectorCostZeroSqlBaselineTests
{
    private const string ServerName = "darling-costzerosql-9523-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string BookingDay1 = "aws_booking_day1_9523";
    private const string BookingDay4 = "aws_booking_day4_9523";
    private const string RealRise = "real_rise_9523";

    private const int RunsPerDay = 1_440;
    private const long FullDayMs = RunsPerDay * 680L;
    private const long PartialDayMs = FullDayMs * 3 / 10;

    /* Fixed dates, not offsets from now(): date_trunc('day') buckets the rows. */
    private static readonly DateTime Day0 = new(2026, 7, 14, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DaysWithNoSqlTime_AreNotABaseline_ForEitherThePageOrTheDigest()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the zero-sql baseline test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            for (var back = 13; back >= 1; back--)
            {
                var day = Day0.AddDays(-back);
                await InsertCostAsync(connection, ct, day, BookingDay1, back >= 2 ? 0 : PartialDayMs);
                await InsertCostAsync(connection, ct, day, BookingDay4,
                    back >= 5 ? 0 : back == 4 ? PartialDayMs : FullDayMs);
                if (back <= 12)
                {
                    await InsertCostAsync(connection, ct, day, RealRise, RunsPerDay * 100L);
                }
            }

            foreach (var collector in new[] { BookingDay1, BookingDay4, RealRise })
            {
                await InsertCostAsync(connection, ct, Day0, collector, FullDayMs);
            }

            var regressions = await DarlingCollectorCostReader.GetCostRegressionsAsync(
                postgres, Day0.AddDays(-13), baselineFloorMs: 1_000, factor: 2.0,
                addedMsFloor: DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor, cancellationToken: ct);
            var movers = await DarlingCollectorCostReader.GetCostMoversAsync(
                postgres, Day0.AddDays(-13), DarlingSelfAlertEvaluator.CostRegressionAddedMsFloor,
                maxRows: 1_000, cancellationToken: ct);

            var mineRegressions = regressions.Where(r => r.ServerId == ServerId).ToList();
            var mineMovers = movers.Where(m => m.ServerId == ServerId).ToList();

            /* The page: only the control. Booking day 1 is held by baseline_cost_days (one measured prior
               day), booking day 4 by the percentile conjunct (its worst measured day is its full days). */
            var rise = Assert.Single(mineRegressions);
            Assert.Equal(RealRise, rise.CollectorName);
            Assert.Equal(100.0, rise.BaselineMsPerRun, 3);

            /* The digest: booking day 1 has one measured prior day, under the three-day floor. */
            Assert.DoesNotContain(mineMovers, m => m.CollectorName == BookingDay1);

            /* Booking day 4's baseline is its four measured days, not thirteen: with the nine zero days in it
               the mean would be 172.6 ms a run and the row would rank as a 730 s/day mover. */
            var day4 = Assert.Single(mineMovers, m => m.CollectorName == BookingDay4);
            Assert.Equal(4, day4.BaselineDays);
            Assert.Equal((PartialDayMs + 3 * FullDayMs) / (4.0 * RunsPerDay), day4.BaselineMsPerRun, 3);

            var riseMover = Assert.Single(mineMovers, m => m.CollectorName == RealRise);
            Assert.Equal(12, riseMover.BaselineDays);
            Assert.Equal(100.0, riseMover.BaselineMsPerRun, 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertCostAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime metricTime,
        string collector, long totalSqlMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.collector_cost
    (metric_time, server_id, database_name, collector_name, run_count, total_sql_ms, max_sql_ms, total_storage_ms, total_rows)
VALUES ($1, $2, NULL, $3, $4, $5, $6, 0, 0);",
            DarlingMcpTestData.Naive(metricTime), ServerId, collector, RunsPerDay, totalSqlMs,
            totalSqlMs / RunsPerDay);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            "DELETE FROM collect.collector_cost WHERE server_id = " + ServerId + "; " +
            "DELETE FROM servers WHERE server_id = " + ServerId + ";", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
