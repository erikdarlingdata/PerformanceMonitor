/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9606: the regressions read takes the recent window's partial day from <c>collect.plan_regression_daily</c> up to the
/// build's watermark and reads only that day's rows from the watermark on live. A 168 hour read used to read every open day
/// live: today, and yesterday from midnight until its whole build lands about three hours later.
///
/// <para>Each test seeds its own store with SQL: one interval per plan per slot over sixteen days, ending at a chosen
/// instant two days back, so every seeded row is below the late-row trigger's bound whatever hour the test runs. The builder
/// then runs, which builds every seeded day whole, because its watermark is the store's own clock. The open days of the
/// scenario are then built again up to the watermark the builder stamps at the scenario's hour, with the builder's own
/// statements (<see cref="BuildAsync"/>). The answer of the plan the resolver chose is compared, every column of every row,
/// with the answer of the same plan without the partial day (the read before #9606), with the recent window all live, and
/// with both sides all live.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreRegressionsRecentPartialDayLiveTests
{
    private const int ServerId = 9606;
    private const string Db = "qsPartial";

    /// <summary>The ordinary queries, two plans each, with an interval in every slot.</summary>
    internal const int DefaultQueries = 30;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the #9606 live tests.";

    /// <summary>A seeded store and the read's inputs. <see cref="Day0"/> is the scenario's today, <see cref="Now"/> the instant
    /// the builder ran at and the window ends at, <see cref="PartialDay"/> the open day whose watermark
    /// (<see cref="Through"/>) lies inside it.</summary>
    internal sealed class Store : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public required DateTime Day0 { get; init; }
        public required DateTime Now { get; init; }
        public required DateTime Start { get; init; }
        public required DateTime BaselineStart { get; init; }
        public required DateTime PartialDay { get; init; }
        public required DateTime Through { get; init; }
        public required TimeSpan Margin { get; init; }
        public required int IntervalMinutes { get; init; }

        /// <summary>Every day built again, as after the seed: what the builder's next tick leaves at the scenario's hour.</summary>
        public Task RebuildAsync(CancellationToken ct) => BuildAsync(Postgres, Now, PartialDay, Through, Day0, ct);

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static string Stamp(DateTime value) => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// A store seeded up to <paramref name="minutesIntoDay0"/> minutes into the scenario's today, with the builder run once at
    /// that instant. The window is the 168 hours ending there, the baseline starts at the midnight seven days before the
    /// window does (the start the table read binds on a store whose interval table holds it), and the margin is the one the
    /// read resolves for intervals of <paramref name="intervalMinutes"/>.
    ///
    /// <para>Slots are <paramref name="intervalMinutes"/> long. The 60 minute ones start on the hour; the longer ones start two
    /// hours after a multiple of their length, so one of them crosses each midnight. An interval's first execution is a minute
    /// into its slot and its last two minutes before the slot ends, or 30 seconds before the store's now for the open slot.
    /// Queries 1 to <paramref name="queries"/> run in every slot under two plans, and their CPU moves at the window start by a
    /// factor that depends on the query (half, 1.3, 1.8, 2.4 or 3 times), so the answer holds regressions and leaves
    /// improvements out. Query 9001 runs only in the window and 9002 only before it. 9003 runs in the window only on the
    /// partial day below the watermark, so its recent side and its text come from that day's totals alone. 9004 runs only
    /// from the watermark on. 9005 runs all of the partial day, and its second plan only from the watermark on. 9006 runs
    /// only after the partial day.</para>
    /// </summary>
    internal static async Task<Store> SeedAsync(int minutesIntoDay0, int intervalMinutes, CancellationToken ct, int queries = DefaultQueries)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var day0 = LiveClock.Now().Date.AddDays(-2);
        var now = day0.AddMinutes(minutesIntoDay0);
        var start = now.AddHours(-168);
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays).Date;
        var seedFrom = day0.AddDays(-16).AddHours(intervalMinutes == 60 ? 0 : 2);
        /* The builder's watermark: the hour of now, less the lag. The partial day is the day it lies strictly inside. */
        var through = now.AddTicks(-(now.Ticks % TimeSpan.TicksPerHour)).AddHours(-PlanRegressionDaily.PartialWatermarkLagHours);
        var partialDay = through.Date;
        Assert.True(through > partialDay, "the scenario's watermark must lie inside a day");

        await ExecAsync(connection, $@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ({ServerId}, 'qspartial', 'qspartial', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", ct);

        var text = string.Create(CultureInfo.InvariantCulture, $@"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time,
    query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan,
    force_failure_count, query_text
)
SELECT
    {ServerId}, '{Db}', r.q, r.q * 10 + r.p, NULL, r.slot_no, r.first_exec,
    LEAST(r.last_exec + interval '5 minutes', TIMESTAMP '{Stamp(now)}'),
    '0x' || to_hex(r.q * 10 + r.p), '0x' || to_hex(r.q),
    5 + ((r.q * 7 + r.slot_no * 3 + r.p) % 11),
    r.cpu, r.cpu + 20000, r.last_exec, false, 0,
    CASE WHEN r.q = 9003 THEN 'SELECT 9003 /* the partial day */' WHEN r.q % 4 = 0 THEN 'SELECT ' || r.q END
FROM
(
    SELECT
        k.q, k.p, g.slot_no, g.first_exec, g.last_exec,
        CAST((100000 + 1000 * (k.q % 97) + 137 * (g.slot_no % 7))
             * CASE WHEN g.last_exec < TIMESTAMP '{Stamp(start)}' THEN 1.0
                    WHEN k.q > 9000 THEN 4.0
                    ELSE (ARRAY[0.5, 3.0, 1.8, 2.4, 1.3])[1 + k.q % 5] END AS bigint) AS cpu
    FROM
    (
        SELECT
            s.slot_no,
            s.slot + interval '1 minute' AS first_exec,
            LEAST(s.slot + make_interval(mins => {intervalMinutes - 2}), TIMESTAMP '{Stamp(now)}' - interval '30 seconds') AS last_exec
        FROM generate_series(TIMESTAMP '{Stamp(seedFrom)}', TIMESTAMP '{Stamp(now)}' - interval '2 minutes', make_interval(mins => {intervalMinutes}))
             WITH ORDINALITY AS s(slot, slot_no)
    ) AS g
    CROSS JOIN
    (
        SELECT q, p FROM generate_series(1, {queries}) AS q CROSS JOIN generate_series(1, 2) AS p
        UNION ALL
        SELECT q, p FROM generate_series(9001, 9006) AS q CROSS JOIN generate_series(1, 2) AS p
    ) AS k
    WHERE CASE
              WHEN k.q <= 9000 THEN true
              WHEN k.p = 2 AND k.q <> 9005 THEN false
              WHEN k.q = 9001 THEN g.last_exec >= TIMESTAMP '{Stamp(start)}'
              WHEN k.q = 9002 THEN g.last_exec < TIMESTAMP '{Stamp(start)}'
              /* Every special query has a baseline, so its rise is a cost row. */
              WHEN g.last_exec < TIMESTAMP '{Stamp(start)}' THEN true
              WHEN k.q = 9003 THEN CAST(g.last_exec AS date) = DATE '{Stamp(partialDay)}' AND g.first_exec < TIMESTAMP '{Stamp(through)}'
              WHEN k.q = 9004 THEN g.first_exec >= TIMESTAMP '{Stamp(through)}'
              WHEN k.q = 9005 THEN CAST(g.last_exec AS date) = DATE '{Stamp(partialDay)}' AND (k.p = 1 OR g.first_exec >= TIMESTAMP '{Stamp(through)}')
              WHEN k.q = 9006 THEN g.last_exec >= TIMESTAMP '{Stamp(partialDay.AddDays(1))}'
              ELSE false
          END
) AS r;");
        await ExecAsync(connection, text, ct);
        await ExecAsync(connection, $@"
INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through)
VALUES ({ServerId}, TIMESTAMP '{Stamp(seedFrom)}', TIMESTAMP '{Stamp(now)}')
ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since, applied_through = EXCLUDED.applied_through;", ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_latest", ct);

        await BuildAsync(postgres, now, partialDay, through, day0, ct);
        await ExecAsync(connection, "ANALYZE collect.plan_regression_daily", ct);

        return new Store
        {
            Scratch = scratch,
            Connection = connection,
            Postgres = postgres,
            Day0 = day0,
            Now = now,
            Start = start,
            BaselineStart = baselineStart,
            PartialDay = partialDay,
            Through = through,
            Margin = QueryStoreIntervalWide.EdgeMarginFor(TimeSpan.FromMinutes(intervalMinutes)),
            IntervalMinutes = intervalMinutes,
        };
    }

    /// <summary>
    /// The builder's tick, then the scenario's open days (the partial day through the day after the scenario's today) built
    /// as the builder builds them at the scenario's hour. The builder takes its watermark from the store's clock, days after
    /// the seed, so its own tick builds those days whole. Each is rebuilt here by the builder's own statements in the
    /// builder's order (<see cref="PlanRegressionDaily.BuildDayAsync"/>): the day's <c>late_seq</c> is read, the totals are
    /// deleted and inserted again with the scenario's watermark as their bound, and the build is stamped with that
    /// <c>late_seq</c> and watermark, which makes it a partial build.
    /// </summary>
    internal static async Task BuildAsync(NpgsqlDataSource postgres, DateTime now, DateTime partialDay, DateTime through, DateTime day0, CancellationToken ct)
    {
        var tick = await PlanRegressionDaily.RunTickAsync(postgres, now, NullLogger.Instance, ct);
        Assert.True(tick.Built > 0, "the builder built no day");

        await using var connection = await postgres.OpenConnectionAsync(ct);
        for (var day = partialDay; day <= day0.AddDays(1); day = day.AddDays(1))
        {
            await BuildPartialDayAsync(connection, ServerId, day, through, now, ct);
        }
    }

    /// <summary>
    /// One day of <paramref name="serverId"/> built as the builder builds an open day whose watermark is
    /// <paramref name="through"/>, by the builder's own statements in the builder's order
    /// (<see cref="PlanRegressionDaily.BuildDayAsync"/>): the day's <c>late_seq</c> is read, the totals are deleted and inserted
    /// again with the watermark as their bound, and the build is stamped with that <c>late_seq</c> and watermark, which makes
    /// it a partial build.
    /// </summary>
    internal static async Task BuildPartialDayAsync(NpgsqlConnection connection, int serverId, DateTime day, DateTime through, DateTime now, CancellationToken ct)
    {
        NpgsqlCommand Statement(string sql, NpgsqlTransaction transaction)
        {
            var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = 300 };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Date, Value = DateOnly.FromDateTime(day) });
            return command;
        }

        NpgsqlParameter Time(DateTime value) => new() { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = value };

        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using (var ensure = Statement(PlanRegressionDaily.EnsureBuiltSql, transaction))
        {
            await ensure.ExecuteNonQueryAsync(ct);
        }

        long seq;
        await using (var read = Statement("SELECT late_seq FROM collect.plan_regression_daily_built WHERE server_id = $1::integer AND day = $2::date FOR UPDATE", transaction))
        {
            seq = Convert.ToInt64(await read.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
        }

        await using (var delete = Statement(PlanRegressionDaily.DeleteDaySql, transaction))
        {
            await delete.ExecuteNonQueryAsync(ct);
        }

        long rows;
        await using (var insert = Statement(PlanRegressionDaily.BuildDaySql, transaction))
        {
            insert.Parameters.Add(Time(day.AddDays(1)));
            insert.Parameters.Add(Time(day.AddDays(-1)));
            insert.Parameters.Add(Time(through));
            rows = Convert.ToInt64(await insert.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
        }

        await using (var mark = Statement(PlanRegressionDaily.MarkBuiltSql, transaction))
        {
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint, Value = seq });
            mark.Parameters.Add(Time(now));
            mark.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint, Value = rows });
            mark.Parameters.Add(Time(through));
            Assert.Equal(1, await mark.ExecuteNonQueryAsync(ct));
        }

        await transaction.CommitAsync(ct);
    }

    internal static async Task<QueryStoreRegressionBaseline.Plan> ResolveAsync(Store s, CancellationToken ct)
    {
        await using var connection = await s.Postgres.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        return await QueryStoreRegressionBaseline.ResolveAsync(
            connection, transaction, ServerId, s.BaselineStart, s.Start, 60, null, ct, windowEnd: s.Now);
    }

    /// <summary>The statement the MCP read runs for <paramref name="plan"/>, as it runs it: the text for the plan, the measured
    /// margin in the text and in the high starts, and the read's own parameter binder.</summary>
    internal static NpgsqlCommand Command(Store s, NpgsqlConnection connection, QueryStoreRegressionBaseline.Plan plan, TimeSpan margin, string prefix = "")
    {
        var raised = QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, margin);
        var sql = DarlingQueryStoreRegressionReader.TableSqlFor(raised);
        if (margin < QueryStoreIntervalWide.PurgeEdgeMargin)
        {
            sql = QueryStoreIntervalWide.WithEdgeMargin(sql, QueryStoreIntervalWide.EdgeMarginSqlFor(margin));
        }

        var command = new NpgsqlCommand(prefix + sql, connection) { CommandTimeout = 300 };
        DarlingQueryStoreRegressionReader.AddParameters(command, ServerId, s.Start, s.Now, DatabaseFilter.All, 5000, s.BaselineStart, raised);
        return command;
    }

    /// <summary>Every column of every row, in order, each value in a round-trip text form, so two forms compare byte for byte.</summary>
    private static async Task<List<string>> RowsAsync(Store s, QueryStoreRegressionBaseline.Plan plan, CancellationToken ct, TimeSpan? margin = null)
    {
        await using var command = Command(s, s.Connection, plan, margin ?? s.Margin);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? "<null>" : reader.GetValue(i) switch
                {
                    double d => d.ToString("R", CultureInfo.InvariantCulture),
                    DateTime t => t.ToString("O", CultureInfo.InvariantCulture),
                    IFormattable x => x.ToString(null, CultureInfo.InvariantCulture),
                    var v => v.ToString() ?? "",
                };
            }

            rows.Add(string.Join("|", values));
        }

        return rows;
    }

    private static string[] Row(List<string> rows, long queryId) =>
        rows.Single(r => r.StartsWith(Db + "|" + queryId.ToString(CultureInfo.InvariantCulture) + "|", StringComparison.Ordinal)).Split('|');

    /// <summary>The plan without the partial day: its whole recent days only, the read before #9606.</summary>
    private static QueryStoreRegressionBaseline.Plan WholeDaysOnly(Store s, QueryStoreRegressionBaseline.Plan plan) =>
        QueryStoreRegressionBaseline.WithRecentDays(plan, plan.RecentDays.Where(d => d != DateOnly.FromDateTime(s.PartialDay)).ToArray());

    /// <summary>
    /// The answer of <paramref name="plan"/> is the answer without the partial day, with the recent window all live, and with
    /// both sides all live. Returns it.
    /// </summary>
    private static async Task<List<string>> AssertSameAnswerOnEveryFormAsync(Store s, QueryStoreRegressionBaseline.Plan plan, CancellationToken ct)
    {
        var served = await RowsAsync(s, plan, ct);
        Assert.NotEmpty(served);
        Assert.Equal(await RowsAsync(s, WholeDaysOnly(s, plan), ct), served);
        Assert.Equal(await RowsAsync(s, plan with { RecentDays = [] }, ct), served);
        Assert.Equal(await RowsAsync(s, QueryStoreRegressionBaseline.AllLive(s.Start) with { RegularFromLatest = true }, ct), served);
        return served;
    }

    /// <summary>The partial day is the last recent day, the whole days before it are a run that reaches it, and the live high
    /// range starts at the watermark or at the next midnight less the margin, whichever is earlier.</summary>
    private static void AssertTakesThePartialDay(Store s, QueryStoreRegressionBaseline.Plan plan)
    {
        var partial = DateOnly.FromDateTime(s.PartialDay);
        Assert.True(plan.RegularFromLatest);
        Assert.NotEmpty(plan.RecentDays);
        Assert.Equal(partial, plan.RecentDays[^1]);
        var firstMidnight = s.Start == s.Start.Date ? s.Start : s.Start.Date.AddDays(1);
        Assert.Equal(firstMidnight, plan.RecentLowEnd);
        Assert.Equal(
            Enumerable.Range(0, plan.RecentDays.Length).Select(i => DateOnly.FromDateTime(firstMidnight).AddDays(i)).ToArray(),
            plan.RecentDays);
        var raised = QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, s.Margin);
        var beforeNextMidnight = s.PartialDay.AddDays(1) - s.Margin;
        var expected = beforeNextMidnight < s.PartialDay ? s.PartialDay : beforeNextMidnight < s.Through ? beforeNextMidnight : s.Through;
        Assert.Equal(expected, raised.RecentHighStart);
    }

    /// <summary>
    /// The read just after midnight and in the afternoon, at 60 minute intervals and at six hour ones. Just after midnight the
    /// partial day is yesterday, built through 21:00; in the afternoon it is today, built through 12:00. The six hour slots
    /// cross midnight and the watermark, so the store holds an interval dated after the partial day that began before the
    /// watermark: the live high range has to start at the next midnight less the margin of that interval length to read it.
    /// </summary>
    [Theory]
    [InlineData(30, 60)]
    [InlineData(15 * 60, 60)]
    [InlineData(30, 360)]
    [InlineData(15 * 60, 360)]
    public async Task ThePartialDayFromTheDailyTotals_IsTheAllLiveAnswer(int minutesIntoDay0, int intervalMinutes)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var s = await SeedAsync(minutesIntoDay0, intervalMinutes, ct);

        var plan = await ResolveAsync(s, ct);
        Assert.Equal(7, plan.BuiltDayCount);
        AssertTakesThePartialDay(s, plan);
        var served = await AssertSameAnswerOnEveryFormAsync(s, plan, ct);

        /* The queries whose recent side lies on the partial day are in the answer, 9003 with the text only that day's rows carry. */
        Assert.Equal("SELECT 9003 /* the partial day */", Row(served, 9003)[14]);
        if (intervalMinutes == 60)
        {
            /* At 60 minutes there are slots from the watermark on: 9004 ran only there, and so did 9005's second plan. */
            Assert.Equal("1", Row(served, 9004)[12]);
            Assert.Equal("2", Row(served, 9005)[12]);
        }

        Assert.Contains(served, r => r.StartsWith(Db + "|1|", StringComparison.Ordinal));

        /* The partial day's totals are what the read holds for it: changing them changes the answer. */
        await ExecAsync(s.Connection, $"UPDATE collect.plan_regression_daily SET execs = execs + 1000 WHERE server_id = {ServerId} AND day = DATE '{Stamp(s.PartialDay)}'", ct);
        Assert.NotEqual(served, await RowsAsync(s, plan, ct));
    }

    /// <summary>
    /// The margin comes from the interval length. On the store with six hour intervals, just after midnight, the interval that
    /// began at 20:01 yesterday and last ran today is dated today, which is not a recent day, and it began before yesterday's
    /// 21:00 watermark. A high range that started at the watermark, or at midnight less the three hours of a 60 minute
    /// interval, would leave it out.
    /// </summary>
    [Fact]
    public async Task ALongerInterval_NeedsItsOwnMargin_BelowTheWatermark()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var s = await SeedAsync(30, 360, ct);
        var plan = await ResolveAsync(s, ct);
        AssertTakesThePartialDay(s, plan);
        Assert.Equal(TimeSpan.FromHours(8), s.Margin);
        Assert.Equal(s.Day0.AddHours(-8), QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, s.Margin).RecentHighStart);

        var served = await RowsAsync(s, plan, ct);
        Assert.NotEqual(served, await RowsAsync(s, plan, ct, TimeSpan.FromHours(3)));
    }

    /// <summary>
    /// The gates. A late row moves <c>late_seq</c>, so its day and the next are no longer valid builds: a stale whole day in
    /// the middle leaves the later run and the partial day after it; stale days right before the partial day leave it out,
    /// because the run no longer reaches it; a late row on the partial day below the watermark leaves it out. A pending
    /// batch leaves every day out and sends the arms to the wide table. The answer is the same every time.
    /// </summary>
    [Fact]
    public async Task AStaleDay_ALateRowBelowTheWatermark_AndAPendingBatch_KeepTheAnswer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var s = await SeedAsync(15 * 60, 60, ct);
        var partial = DateOnly.FromDateTime(s.PartialDay);
        Assert.Equal(s.Day0, s.PartialDay);
        AssertTakesThePartialDay(s, await ResolveAsync(s, ct));

        async Task LateRowAsync(DateTime first, DateTime last) => await ExecAsync(s.Connection, $@"
INSERT INTO collect.query_store_interval_latest
    (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time,
     query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count, query_text)
VALUES ({ServerId}, '{Db}', 1, 11, NULL, 990000 + {first.Ticks % 100000}, TIMESTAMP '{Stamp(first)}', TIMESTAMP '{Stamp(s.Now)}',
        '0xb', '0x1', 40, 900000, 950000, TIMESTAMP '{Stamp(last)}', false, 0, NULL);", ct);

        /* 1. A late row on the day four days back: that day and the next are stale. The run is the two days after them. */
        await LateRowAsync(s.Day0.AddDays(-4).AddHours(10), s.Day0.AddDays(-4).AddHours(10).AddMinutes(20));
        var middle = await ResolveAsync(s, ct);
        Assert.Equal(new[] { partial.AddDays(-2), partial.AddDays(-1), partial }, middle.RecentDays);
        var afterMiddle = await AssertSameAnswerOnEveryFormAsync(s, middle, ct);

        /* 2. A late row that began two days back and last ran yesterday: both are stale, so the run ends before yesterday
              and no longer reaches today. Today's partial build is still valid, and it is not taken. */
        await LateRowAsync(s.Day0.AddDays(-2).AddHours(23).AddMinutes(50), s.Day0.AddDays(-1).AddMinutes(10));
        var gap = await ResolveAsync(s, ct);
        Assert.DoesNotContain(partial, gap.RecentDays);
        Assert.DoesNotContain(partial.AddDays(-1), gap.RecentDays);
        var afterGap = await AssertSameAnswerOnEveryFormAsync(s, gap, ct);
        Assert.NotEqual(afterMiddle, afterGap);

        /* Rebuild, so every day is valid again and today is taken. */
        await s.RebuildAsync(ct);
        AssertTakesThePartialDay(s, await ResolveAsync(s, ct));

        /* 3. A late row on today, below the 12:00 watermark: today's partial build is stale. Yesterday is a whole day still. */
        await LateRowAsync(s.Day0.AddHours(5), s.Day0.AddHours(5).AddMinutes(20));
        var late = await ResolveAsync(s, ct);
        Assert.DoesNotContain(partial, late.RecentDays);
        Assert.Equal(partial.AddDays(-1), late.RecentDays[^1]);
        var afterLate = await AssertSameAnswerOnEveryFormAsync(s, late, ct);
        Assert.NotEqual(afterGap, afterLate);

        /* 4. A pending batch: no day from the totals on either side, and the Regular arms on the wide table. This seed holds
              no wide row, so only the plan is checked. */
        await s.RebuildAsync(ct);
        AssertTakesThePartialDay(s, await ResolveAsync(s, ct));
        await ExecAsync(s.Connection, $"INSERT INTO collect.query_store_interval_latest_pending (server_id, collection_time, database_name, recorded_at, failure) VALUES ({ServerId}, TIMESTAMP '{Stamp(s.Now)}', '{Db}', TIMESTAMP '{Stamp(s.Now)}', 'test')", ct);
        var pending = await ResolveAsync(s, ct);
        Assert.False(pending.RegularFromLatest);
        Assert.Empty(pending.RecentDays);
        Assert.Equal(0, pending.BuiltDayCount);
    }

    /// <summary>
    /// A window that ends before the watermark, or before the newest batch latest has applied, takes no partial day: the
    /// totals have no upper bound of their own.
    /// </summary>
    [Fact]
    public async Task AWindowThatEndsBeforeTheWatermark_TakesNoPartialDay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var s = await SeedAsync(15 * 60, 60, ct);
        var partial = DateOnly.FromDateTime(s.PartialDay);

        async Task<QueryStoreRegressionBaseline.Plan> ResolveToAsync(DateTime end)
        {
            await using var connection = await s.Postgres.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
            return await QueryStoreRegressionBaseline.ResolveAsync(connection, transaction, ServerId, s.BaselineStart, s.Start, 60, null, ct, windowEnd: end);
        }

        /* Before latest's applied_through: no recent day at all. */
        Assert.Empty((await ResolveToAsync(s.Through.AddHours(-1))).RecentDays);

        /* With applied_through moved back below the watermark, the end is the only bound left: an hour before the watermark
           takes the whole days and not the partial one, the watermark itself takes it. */
        await ExecAsync(s.Connection, $"UPDATE collect.query_store_interval_latest_coverage SET applied_through = TIMESTAMP '{Stamp(s.Through.AddHours(-2))}' WHERE server_id = {ServerId}", ct);
        var early = await ResolveToAsync(s.Through.AddHours(-1));
        Assert.NotEmpty(early.RecentDays);
        Assert.DoesNotContain(partial, early.RecentDays);
        Assert.Equal(partial, (await ResolveToAsync(s.Through)).RecentDays[^1]);
    }

    /// <summary>
    /// The rows the recent arm reads from <c>query_store_interval_latest</c>, counted from the executed plan. In the afternoon
    /// the 168 hour read keeps live only the day the window starts in and today's rows from the watermark on: at 60 minute
    /// intervals that is the 12 slots from the window start less the margin to the next midnight, and the 3 slots from 12:00
    /// on. Reading today from midnight is 15 slots more, and the whole window is 171.
    /// </summary>
    [Fact]
    public async Task TheRecentArm_ReadsOnlyTheEdgeDayAndTheRowsFromTheWatermarkOn()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var s = await SeedAsync(15 * 60, 60, ct);
        var plan = await ResolveAsync(s, ct);

        var perSlot = Convert.ToInt64(await ScalarAsync(s.Connection, $"SELECT max(n) FROM (SELECT count(*) AS n FROM collect.query_store_interval_latest WHERE server_id = {ServerId} GROUP BY runtime_stats_interval_id) AS x", ct), CultureInfo.InvariantCulture);
        Assert.True(perSlot >= 2 * DefaultQueries, "the seed holds two plans of every query in a slot");

        var read = await RecentArmRowsReadAsync(s, plan, ct);
        Assert.True(read > 0, "the recent arm read no live row");
        Assert.True(read <= 17 * perSlot, $"the recent arm read {read} rows of query_store_interval_latest, more than 17 slots of {perSlot}");

        /* The count can tell the difference: with no recent day the arm reads the whole window. */
        var allLive = await RecentArmRowsReadAsync(s, plan with { RecentDays = [] }, ct);
        Assert.True(allLive >= 150 * (long)(2 * DefaultQueries), $"the all-live recent arm read {allLive} rows");
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>
    /// The rows of <c>query_store_interval_latest</c> the <c>deduped_recent</c> arm fetched, from EXPLAIN (ANALYZE): for each scan
    /// of the table under that CTE, the rows it returned plus the rows its filter and its recheck removed, times its loops.
    /// Sequential scans are off for the statement, so the count is the rows inside the arm's <c>first_execution_time</c>
    /// ranges and not the table.
    /// </summary>
    internal static async Task<long> RecentArmRowsReadAsync(Store s, QueryStoreRegressionBaseline.Plan plan, CancellationToken ct)
    {
        await using var connection = await s.Postgres.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using (var off = new NpgsqlCommand("SET LOCAL enable_seqscan = off", connection, transaction))
        {
            await off.ExecuteNonQueryAsync(ct);
        }

        string json;
        await using (var command = Command(s, connection, plan, s.Margin, "EXPLAIN (ANALYZE, FORMAT JSON) "))
        {
            command.Transaction = transaction;
            json = Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
        }

        using var document = JsonDocument.Parse(json);
        long total = 0;
        var found = false;
        void Walk(JsonElement node, bool inRecent)
        {
            inRecent |= node.TryGetProperty("Subplan Name", out var name) && name.GetString() == "CTE deduped_recent";
            if (inRecent && node.TryGetProperty("Relation Name", out var relation)
                && relation.GetString()!.StartsWith("query_store_interval_latest", StringComparison.Ordinal))
            {
                found = true;
                double Number(string property) => node.TryGetProperty(property, out var value) ? value.GetDouble() : 0;
                total += (long)Math.Round((Number("Actual Rows") + Number("Rows Removed by Filter") + Number("Rows Removed by Index Recheck")) * Number("Actual Loops"));
            }

            if (node.TryGetProperty("Plans", out var plans))
            {
                foreach (var child in plans.EnumerateArray())
                {
                    Walk(child, inRecent);
                }
            }
        }

        Walk(document.RootElement[0].GetProperty("Plan"), false);
        Assert.True(found, "no scan of query_store_interval_latest under deduped_recent in the plan");
        return total;
    }
}
