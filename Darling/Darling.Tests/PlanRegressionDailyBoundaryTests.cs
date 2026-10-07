/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The day arithmetic of the PLAN_REGRESSION daily read (#5448): M, the day-aligned window start (no store), and the
/// live floor that bounds the interval-table half of the read (a scratch database, no tables).
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The one live fact reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres and then works entirely inside it. */
public sealed class PlanRegressionDailyBoundaryTests
{
    private static DateOnly D(int day) => new(2026, 9, day);

    private static DateTime At(int day, int hour = 0, int minute = 0) => new(2026, 9, day, hour, minute, 0, DateTimeKind.Unspecified);

    [Fact]
    public void TheWindowFloor_IsTheStartOfTheDayTheExactEdgeFallsIn()
    {
        Assert.Equal(At(10), PgFactCollector.PlanRegressionWindowFloor(At(10)));
        Assert.Equal(At(10), PgFactCollector.PlanRegressionWindowFloor(At(10, 17, 45)));
        /* One tick before midnight is still the same day; midnight is the next. */
        Assert.Equal(At(10), PgFactCollector.PlanRegressionWindowFloor(At(11).AddTicks(-1)));
        Assert.Equal(At(11), PgFactCollector.PlanRegressionWindowFloor(At(11)));
        Assert.Equal(DateTimeKind.Unspecified, PgFactCollector.PlanRegressionWindowFloor(DateTime.SpecifyKind(At(10, 5), DateTimeKind.Utc)).Kind);
    }

    /// <summary>The read's <c>live_floor</c> CTE over made-up built days, on a scratch database (the CTE reads no table):
    /// $1 the built days, $2 M. Its floor is what the live half's <c>first_execution_time</c> bound uses.</summary>
    private static async Task<DateTime> LiveFloorAsync(NpgsqlConnection connection, DateTime floor, DateOnly[] builtDays, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "WITH built_days AS (SELECT d AS day FROM unnest($1::date[]) AS d), " + PgFactCollector.PlanRegressionLiveFloorCte
            + " SELECT floor_ts FROM live_floor", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = builtDays });
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task TheLiveFloor_IsADayBelowTheFirstUnbuiltDay_StopsAtTheFirstGap_AndIgnoresDaysBelowTheWindow()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the live floor facts.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        /* Nothing built: the first day itself is unbuilt, so the floor is the day before the window. */
        Assert.Equal(At(1).AddDays(-1), await LiveFloorAsync(connection, At(1), [], ct));

        /* A contiguous run from the start: the floor follows its end (day 5 is the first unbuilt, so day 4's start). */
        Assert.Equal(At(4), await LiveFloorAsync(connection, At(1), [D(1), D(2), D(3), D(4)], ct));

        /* Days 1-2 and 4-9 built, 3 missing: every unbuilt row from day 3 on must be reachable, and a row of day 3 may
           have begun a day earlier, so the floor is day 2's start. */
        Assert.Equal(At(2), await LiveFloorAsync(connection, At(1), [D(1), D(2), D(4), D(5), D(6), D(7), D(8), D(9)], ct));

        /* A gap at the very start, though later days are built. */
        Assert.Equal(At(1).AddDays(-1), await LiveFloorAsync(connection, At(1), [D(2), D(3)], ct));

        /* Window from day 5; days below it are ignored, 5 and 6 built, 7 is the first unbuilt: day 6's start. */
        Assert.Equal(At(6), await LiveFloorAsync(connection, At(5), [D(1), D(2), D(5), D(5), D(6)], ct));

        /* An interval that opens 23:40 on day 3 and closes 00:35 on day 4 belongs to day 4 (its last execution). With days
           1-3 built and day 4 live, the live floor is day 3's start, so first_execution_time >= floor admits it. */
        var floor = await LiveFloorAsync(connection, At(1), [D(1), D(2), D(3)], ct);
        Assert.Equal(At(3), floor);
        Assert.True(At(3, 23, 40) >= floor);

        /* An interval ending exactly at midnight belongs to the new day; one ending a tick before to the old one. */
        Assert.Equal(D(4), DateOnly.FromDateTime(At(4)));
        Assert.Equal(D(3), DateOnly.FromDateTime(At(4).AddTicks(-1)));
    }

    [Fact]
    public void TheDailyRead_WorksOutTheBuiltDaysInsideItsOwnStatement_SoBothHalvesShareOneSnapshot()
    {
        var sql = System.Text.RegularExpressions.Regex.Replace(PgFactCollector.PlanRegressionDailySql, @"\s+", " ");

        /* The built days are a CTE made of the very text the fact's own pre-read runs, so the two cannot drift. */
        var builtDays = System.Text.RegularExpressions.Regex.Replace(PlanRegressionDaily.BuiltDaysSelect, @"\s+", " ").Trim();
        Assert.Contains("WITH built_days AS ( " + builtDays + " ), live_floor AS", sql, StringComparison.Ordinal);

        /* The built-days CTE has more than one reader, so it is evaluated once: the totals half, the live half's day filter
           and the live floor all read that one result. */
        Assert.Contains("d.day IN (SELECT bd.day FROM built_days AS bd)", sql, StringComparison.Ordinal);
        Assert.Contains("l.last_execution_time::date NOT IN (SELECT bd.day FROM built_days AS bd)", sql, StringComparison.Ordinal);
        Assert.Contains("l.first_execution_time >= (SELECT lf.floor_ts FROM live_floor AS lf)", sql, StringComparison.Ordinal);

        /* Nothing the fact worked out from an earlier read is passed in: no day array and no floor of its own. */
        Assert.DoesNotContain("date[]", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$5", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDailyRead_IsInTheCensus_AndSharesTheSuffixWithTheTableRead()
    {
        Assert.Contains(PgFactCollector.PlanRegressionDailySql, PgFactCollector.AllSql);
        var marker = "plan_dedup AS";
        var tail = PgFactCollector.PlanRegressionTableSql[PgFactCollector.PlanRegressionTableSql.IndexOf(marker, StringComparison.Ordinal)..];
        Assert.EndsWith(tail, PgFactCollector.PlanRegressionDailySql, StringComparison.Ordinal);
    }
}
