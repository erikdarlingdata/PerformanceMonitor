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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9280: the daily Query Store statement list reads the whole hours of its two edges from <c>collect.query_store_top_hourly</c> where the
/// stamp ledger says the hour is valid, and from the wide table for every other hour. The summary rows here are written by a test helper
/// from the read's own expressions (the builder is exercised by its own tests), so these tests cover the read: which hours it takes from the
/// summary, and that its answer is the daily statement's.
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and then works
   entirely inside it, the same shape as QueryStoreTopDailyReadLiveTests. */
public sealed class QueryStoreTopHourlyReadLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the hourly summary read's live pins (each mints its own scratch database).";
    private static readonly DateTime FirstHour = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime LastHour = new(2026, 1, 14, 11, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Hour(int day, int hour) => new(2026, 1, day, hour, 0, 0, DateTimeKind.Unspecified);

    /* ───────────────────────────── the segmenting function ───────────────────────────── */

    private static readonly DateOnly SpanStart = new(2026, 1, 12);
    private static readonly DateOnly SpanEnd = new(2026, 1, 14);

    private static IEnumerable<DateTime> Hours(DateTime from, DateTime to)
    {
        for (var h = from; h < to; h = h.AddHours(1))
        {
            yield return h;
        }
    }

    [Fact]
    public void ALeadingPartialHour_AndATrailingTail_AreRaw_AndEveryValidWholeHourIsTheSummarys()
    {
        var readStart = new DateTime(2026, 1, 11, 5, 30, 0);
        var end = new DateTime(2026, 1, 14, 6, 30, 0);
        var plan = QueryStoreTopHourlyEdges.Segment(readStart, end, SpanStart, SpanEnd, Hours(Hour(11, 5), Hour(14, 6)));

        Assert.Equal(new[] { (readStart, Hour(11, 6)) }, plan.LeadRanges);
        Assert.Equal(new[] { (Hour(14, 6), end) }, plan.TrailRanges);
        Assert.Equal(Hours(Hour(11, 6), Hour(12, 0)).Concat(Hours(Hour(14, 0), Hour(14, 6))), plan.SummaryHours);
        Assert.True(plan.UsesSummary);
    }

    [Fact]
    public void AWindowOnTheHour_HasNoPartialHour_AndTheLastWholeHourIsTheSummarys()
    {
        var plan = QueryStoreTopHourlyEdges.Segment(Hour(11, 6), Hour(14, 6), SpanStart, SpanEnd, Hours(Hour(11, 6), Hour(14, 6)));

        Assert.Empty(plan.LeadRanges);
        Assert.Empty(plan.TrailRanges);
        Assert.Equal(Hours(Hour(11, 6), Hour(12, 0)).Concat(Hours(Hour(14, 0), Hour(14, 6))), plan.SummaryHours);
    }

    [Fact]
    public void AnInvalidHourInsideAnEdge_IsItsOwnRawRange_AndTouchingRawHoursMerge()
    {
        var valid = Hours(Hour(11, 6), Hour(12, 0)).Where(h => h != Hour(11, 9) && h != Hour(11, 10) && h != Hour(11, 14));
        var plan = QueryStoreTopHourlyEdges.Segment(Hour(11, 6), Hour(14, 0), SpanStart, SpanEnd, valid);

        Assert.Equal(new[] { (Hour(11, 9), Hour(11, 11)), (Hour(11, 14), Hour(11, 15)) }, plan.LeadRanges);
        Assert.Empty(plan.TrailRanges);
        Assert.DoesNotContain(Hour(11, 9), plan.SummaryHours);
        Assert.DoesNotContain(Hour(11, 14), plan.SummaryHours);
        Assert.Equal(15, plan.SummaryHours.Count);
    }

    [Fact]
    public void TheHoursNotBuiltYet_AtTheTail_ReadFromTheWideTable_UpToTheWindowsEnd()
    {
        var end = new DateTime(2026, 1, 14, 6, 30, 0);
        var plan = QueryStoreTopHourlyEdges.Segment(Hour(11, 6), end, SpanStart, SpanEnd, Hours(Hour(11, 6), Hour(14, 3)));

        Assert.Equal(new[] { (Hour(14, 3), end) }, plan.TrailRanges);
        Assert.Equal(Hour(14, 2), plan.SummaryHours[^1]);
    }

    [Fact]
    public void WithNoValidHour_NothingIsTheSummarys_AndTheRangesAreTodaysTwoEdges()
    {
        var readStart = new DateTime(2026, 1, 11, 5, 30, 0);
        var end = new DateTime(2026, 1, 14, 6, 30, 0);
        var plan = QueryStoreTopHourlyEdges.Segment(readStart, end, SpanStart, SpanEnd, Array.Empty<DateTime>());

        Assert.False(plan.UsesSummary);
        Assert.Equal(new[] { (readStart, Hour(12, 0)) }, plan.LeadRanges);
        Assert.Equal(new[] { (Hour(14, 0), end) }, plan.TrailRanges);
    }

    [Fact]
    public void AValidHourOutsideTheEdges_IsNotTheSummarys()
    {
        /* The days of the span are the daily table's, and an hour before the window starts is not read at all. */
        var plan = QueryStoreTopHourlyEdges.Segment(Hour(11, 6), Hour(14, 6), SpanStart, SpanEnd, new[] { Hour(11, 2), Hour(12, 5), Hour(13, 23), Hour(14, 8) });

        Assert.False(plan.UsesSummary);
    }

    /* ───────────────────────────── live ───────────────────────────── */

    /// <summary>The hourly summary and the ledger for every hour of the seed, as the builder leaves them: one row per (hour, key) from the wide
    /// rows with the read's expressions, every pair's <c>built_seq</c> and <c>top_built_seq</c> level with its <c>late_seq</c>, and an hours row.</summary>
    private const string BuildHoursSql = @"
INSERT INTO collect.query_store_top_hourly
(hour, server_id, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name, wide_rows,
 execution_count_sum, avg_duration_us_wsum, avg_cpu_time_us_wsum, avg_logical_io_reads_wsum, avg_logical_io_writes_wsum,
 avg_physical_io_reads_wsum, avg_rowcount_wsum, last_execution_time_max, query_plan_hash_max)
SELECT date_trunc('hour', collection_time), server_id, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name,
       count(*), sum(CAST(execution_count AS numeric)),
       sum(CAST(avg_duration_us AS numeric) * execution_count), sum(CAST(avg_cpu_time_us AS numeric) * execution_count),
       sum(CAST(avg_logical_io_reads AS numeric) * execution_count), sum(CAST(avg_logical_io_writes AS numeric) * execution_count),
       sum(CAST(avg_physical_io_reads AS numeric) * execution_count), sum(CAST(avg_rowcount AS numeric) * execution_count),
       max(last_execution_time), max(query_plan_hash)
FROM collect.query_store_interval_wide
WHERE server_id = 1
GROUP BY date_trunc('hour', collection_time), server_id, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name;

INSERT INTO collect.query_store_compose_stamp_hours (hour, built_at)
SELECT g, TIMESTAMP '2026-01-15 12:00:00' FROM generate_series(@first, @last, interval '1 hour') AS g;

UPDATE collect.query_store_compose_stamp_built
SET built_seq = late_seq, top_built_seq = late_seq, built_at = TIMESTAMP '2026-01-15 12:00:00';";

    private async Task RunLiveAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body, Func<NpgsqlConnection, CancellationToken, Task>? beforeHours = null)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await QueryStoreTopDailyReadLiveTests.SeedAsync(connection, ct);
            if (beforeHours is not null)
            {
                await beforeHours(connection, ct);
            }

            var daily = await QueryStoreTopDaily.RunTickAsync(source, QueryStoreTopDailyReadLiveTests.BuildNow, QueryStoreTopDailyReadLiveTests.RetentionDays, NullLogger.Instance, ct);
            Assert.Equal(0, daily.Failed);
            await using (var build = new NpgsqlCommand(BuildHoursSql, connection))
            {
                build.Parameters.Add(new NpgsqlParameter("first", NpgsqlDbType.Timestamp) { Value = FirstHour });
                build.Parameters.Add(new NpgsqlParameter("last", NpgsqlDbType.Timestamp) { Value = LastHour });
                await build.ExecuteNonQueryAsync(ct);
            }

            await body(connection, source, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The daily statement as it was before #9280, over the same span and window, on the same connection.</summary>
    private static async Task<List<string>> OldStatementAsync(
        NpgsqlConnection connection, DarlingDataReader.QueryStoreTopRead read, DateTime end, string? db, string? outcome, string? module, string? sql, CancellationToken ct) =>
        QueryStoreTopDailyReadLiveTests.Format((await QueryStoreTopDailyReadLiveTests.RunSqlAsync(
            connection, sql ?? DarlingDataReader.QueryStoreTopDailyTableSql, read.Table!.Value.ReadStart, end, db, outcome, module, read.DailySpan, ct)).Select(QueryStoreTopDailyReadLiveTests.ToRow));

    public static IEnumerable<object?[]> Cells()
    {
        /* start offset from the whole hour (minutes), end offset: a window starting and ending mid-hour, one on the hour, and one of each. */
        foreach (var (startMinutes, endMinutes) in new[] { (-30, 30), (0, 0), (-30, 0), (0, 30) })
        {
            yield return new object?[] { startMinutes, endMinutes, null, null, null };
            yield return new object?[] { startMinutes, endMinutes, "db1", null, null };
            yield return new object?[] { startMinutes, endMinutes, null, "Aborted", null };
            yield return new object?[] { startMinutes, endMinutes, null, null, "dbo.m1" };
        }
    }

    [Theory]
    [MemberData(nameof(Cells))]
    public async Task TheRoutedAnswer_EqualsTheDailyStatements_ForWindowsMidHourAndOnTheHour(int startMinutes, int endMinutes, string? db, string? outcome, string? module)
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = QueryStoreTopDailyReadLiveTests.End.AddHours(-72).AddMinutes(startMinutes);
            var end = QueryStoreTopDailyReadLiveTests.End.AddMinutes(endMinutes);
            var read = await QueryStoreTopDailyReadLiveTests.RouteAsync(source, start, end, db, outcome, module, ct);

            Assert.NotNull(read.Table);
            Assert.NotNull(read.DailySpan);
            Assert.NotEmpty(read.Rows);
            Assert.Equal(await OldStatementAsync(connection, read, end, db, outcome, module, null, ct), QueryStoreTopDailyReadLiveTests.Format(read.Rows));
        }, async (connection, ct) =>
        {
            /* A row inside each partial hour, so the raw arms have rows to read. */
            await QueryStoreTopDailyReadLiveTests.InsertRowAsync(connection, Hour(11, 5).AddMinutes(45), 9101, ct);
            await QueryStoreTopDailyReadLiveTests.InsertRowAsync(connection, Hour(14, 6).AddMinutes(15), 9102, ct);
        });
    }

    [Fact]
    public async Task TheHourlyStatement_EqualsTheDailyStatement_WithTheNarrowedAndTheFullEdgeMargin()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = new DateTime(2026, 1, 11, 5, 30, 0);
            var end = new DateTime(2026, 1, 14, 6, 30, 0);
            var span = (SpanStart, SpanEnd);
            var plan = QueryStoreTopHourlyEdges.Segment(start, end, SpanStart, SpanEnd, Hours(Hour(11, 6), Hour(14, 6)));
            var hourlySql = QueryStoreTopHourlyEdges.BuildSql(plan);
            Assert.NotEqual(DarlingDataReader.QueryStoreTopDailyTableSql, hourlySql);

            var margin = QueryStoreIntervalWide.EdgeMarginSqlFor(QueryStoreIntervalWide.EdgeMarginFor(TimeSpan.FromMinutes(60)));
            foreach (var narrow in new[] { false, true })
            {
                var oldSql = narrow ? QueryStoreIntervalWide.WithEdgeMargin(DarlingDataReader.QueryStoreTopDailyTableSql, margin) : DarlingDataReader.QueryStoreTopDailyTableSql;
                var newSql = narrow ? QueryStoreIntervalWide.WithEdgeMargin(hourlySql, margin) : hourlySql;
                Assert.Equal(!narrow, newSql.Contains(QueryStoreIntervalWide.PurgeEdgeMarginSql, StringComparison.Ordinal));
                var expected = QueryStoreTopDailyReadLiveTests.Format((await QueryStoreTopDailyReadLiveTests.RunSqlAsync(
                    connection, oldSql, start, end, null, null, null, span, ct, extra: null)).Select(QueryStoreTopDailyReadLiveTests.ToRow));
                var actual = QueryStoreTopDailyReadLiveTests.Format((await QueryStoreTopDailyReadLiveTests.RunSqlAsync(
                    connection, newSql, start, end, null, null, null, span, ct, extra: plan)).Select(QueryStoreTopDailyReadLiveTests.ToRow));
                Assert.NotEmpty(expected);
                Assert.Equal(expected, actual);
            }
        }, async (connection, ct) =>
        {
            await QueryStoreTopDailyReadLiveTests.InsertRowAsync(connection, Hour(11, 5).AddMinutes(45), 9101, ct);
            await QueryStoreTopDailyReadLiveTests.InsertRowAsync(connection, Hour(14, 6).AddMinutes(15), 9102, ct);
        });
    }

    private const string ChangeSummarySql = @"
UPDATE collect.query_store_top_hourly
SET execution_count_sum = @count, avg_duration_us_wsum = @wsum
WHERE hour = @hour AND query_id = @q;";

    private static async Task ChangeSummaryAsync(NpgsqlConnection connection, DateTime hour, long queryId, decimal wsum, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ChangeSummarySql, connection);
        command.Parameters.Add(new NpgsqlParameter("count", NpgsqlDbType.Numeric) { Value = 1_000_000m });
        command.Parameters.Add(new NpgsqlParameter("wsum", NpgsqlDbType.Numeric) { Value = wsum });
        command.Parameters.Add(new NpgsqlParameter("hour", NpgsqlDbType.Timestamp) { Value = hour });
        command.Parameters.Add(new NpgsqlParameter("q", NpgsqlDbType.Bigint) { Value = queryId });
        Assert.True(await command.ExecuteNonQueryAsync(ct) > 0, "the fixture hour holds a summary row for the statement");
    }

    private static long TotalExecutions(DarlingDataReader.QueryStoreTopRead read, long queryId) =>
        read.Rows.Where(r => r.QueryId == queryId).Sum(r => r.TotalExecutions);

    [Fact]
    public async Task AValidHour_IsServedFromTheSummary()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = QueryStoreTopDailyReadLiveTests.End.AddHours(-72);
            var end = QueryStoreTopDailyReadLiveTests.End;
            var before = await QueryStoreTopDailyReadLiveTests.RouteAsync(source, start, end, null, null, null, ct);

            /* A leading-edge hour and a trailing-edge hour, each changed in the summary only. */
            await ChangeSummaryAsync(connection, Hour(11, 20), 1, 9_000_000_000_000_000m, ct);
            await ChangeSummaryAsync(connection, Hour(14, 3), 2, 8_000_000_000_000_000m, ct);
            var after = await QueryStoreTopDailyReadLiveTests.RouteAsync(source, start, end, null, null, null, ct);

            Assert.NotEqual(QueryStoreTopDailyReadLiveTests.Format(before.Rows), QueryStoreTopDailyReadLiveTests.Format(after.Rows));
            Assert.True(TotalExecutions(after, 1) >= 1_000_000);
            Assert.True(TotalExecutions(after, 2) >= 1_000_000);
        });
    }

    /// <summary>#9310: a read answered by the hourly-edges statement names it in the scope, with its summary hours and the daily span's days.</summary>
    [Fact]
    public async Task TheHourlyStatement_IsNamedInTheReadScope_WithItsSummaryHoursAndDays()
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            using var scope = ReadScope.Open(null);
            var read = await QueryStoreTopDailyReadLiveTests.RouteAsync(
                source, QueryStoreTopDailyReadLiveTests.End.AddHours(-72), QueryStoreTopDailyReadLiveTests.End, null, null, null, ct);

            Assert.Equal(2, read.DailyDaysUsed);
            Assert.Equal(ReadScope.SourceHourlyEdges, scope.Scope.Source);
            Assert.Equal("24 summary hours, 2 days", scope.Scope.SourceReason);
        });
    }

    public static IEnumerable<object?[]> NotValid()
    {
        yield return new object?[] { "late write after the build" };
        yield return new object?[] { "hour not built" };
        yield return new object?[] { "hour built before the table" };
    }

    [Theory]
    [MemberData(nameof(NotValid))]
    public async Task AnHourThatIsNotValid_ReadsRaw_SoTheSummarysChangeDoesNotShow_AndALateRowIsCountedOnce(string kind)
    {
        await RunLiveAsync(async (connection, source, ct) =>
        {
            var start = QueryStoreTopDailyReadLiveTests.End.AddHours(-72);
            var end = QueryStoreTopDailyReadLiveTests.End;
            var hour = Hour(14, 3);

            switch (kind)
            {
                case "late write after the build":
                    /* A row written into the built hour: the trigger bumps the pair's late_seq past its top_built_seq. */
                    await QueryStoreTopDailyReadLiveTests.InsertRowAsync(connection, hour.AddMinutes(20), 9301, ct);
                    break;
                case "hour not built":
                    await QueryStoreTopDailyReadLiveTests.ExecAsync(connection, $"DELETE FROM collect.query_store_compose_stamp_hours WHERE hour = TIMESTAMP '{QueryStoreTopDailyReadLiveTests.At(hour)}'", ct);
                    break;
                default:
                    await QueryStoreTopDailyReadLiveTests.ExecAsync(connection, $"UPDATE collect.query_store_compose_stamp_built SET top_built_seq = NULL WHERE hour = TIMESTAMP '{QueryStoreTopDailyReadLiveTests.At(hour)}'", ct);
                    break;
            }

            await ChangeSummaryAsync(connection, hour, 2, 8_000_000_000_000_000m, ct);
            var read = await QueryStoreTopDailyReadLiveTests.RouteAsync(source, start, end, null, null, null, ct);

            Assert.Equal(await OldStatementAsync(connection, read, end, null, null, null, null, ct), QueryStoreTopDailyReadLiveTests.Format(read.Rows));
            Assert.True(TotalExecutions(read, 2) < 1_000_000);
            if (kind == "late write after the build")
            {
                Assert.Equal(1000L, TotalExecutions(read, 9301));
            }
        });
    }
}
