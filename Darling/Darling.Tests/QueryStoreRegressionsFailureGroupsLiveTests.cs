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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5506 (Erik, 2026-10-21): the Query Store regressions read shows Regular, Aborted and Exception runs side by side per query
/// and flags a rise in timeouts or errors as well as cost. Seeded through the real write path (raw and the wide table), read
/// through the product's own reader on the wide arm and on the raw arm, and through the viewer's twin: every group's count and
/// per-execution figures equal a hand-worked figure; a query whose Regular cost is flat but whose Aborted rate rises past the
/// threshold is flagged <c>timeouts</c> (one below the threshold is not), the same for <c>errors</c>; a query with both a cost
/// rise and a timeout rise carries both reasons; a flood of error rows leaves the cost rows their share of the cap; the read
/// gives the same rows with and without the partial background index, and the index builder creates it.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it. It shares the gap-cache collection with the other
   regressions table tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsFailureGroupsLiveTests
{
    internal const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    internal const string Db = "qsFail";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    internal sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public required DateTime Start { get; init; }
        public required DateTime End { get; init; }
        public required DateTime BaselineStart { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>One interval of one query and outcome, written through the real collector write path. Its last execution is 30
    /// minutes after its first, so its day and its side of the window start are those of <c>first + 30 minutes</c>.</summary>
    internal static async Task WriteAsync(
        DarlingCollectorRunner runner, ServerRuntime server, CollectorContext context, string type, long intervalId,
        DateTime first, long queryId, long executions, long cpuUs, long durationUs, CancellationToken ct,
        string? databaseName = null, string? queryText = null)
    {
        var row = new QueryStoreCollector.Row
        {
            DatabaseName = databaseName ?? Db,
            QueryId = queryId,
            PlanId = queryId * 10,
            ExecutionTypeDesc = type,
            FirstExecutionTime = first,
            LastExecutionTime = first.AddMinutes(30),
            QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
            QueryText = queryText ?? "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = cpuUs,
            AvgDurationUs = durationUs,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = intervalId,
            IntervalStartTimeUtc = first,
        };
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, [row], server, first.AddMinutes(40), context, ct);
    }

    internal static async Task<Rig> OpenAsync(Func<DarlingCollectorRunner, ServerRuntime, CollectorContext, DateTime, DateTime, Task> seed, CancellationToken ct)
    {
        var baseCs = BaseConnectionString;
        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, 'qsfail', 'qsfail', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        /* The window starts at 06:00 yesterday, ends now: at any hour the baseline [start - 7 days, start) is whole days apart
           from its two ends, and the recent window is 24 h or more. */
        var today = LiveClock.Now().Date;
        var start = today.AddDays(-1).AddHours(6);
        var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsfail-host", CollectionTime = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsfail", Host = "qsfail-host" },
            ConnectionString = "Server=qsfail-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsfail-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };
        await seed(runner, server, context, today, start);

        var end = LiveClock.Now();
        end = end.AddTicks(-(end.Ticks % TimeSpan.TicksPerSecond)).AddSeconds(1);
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, baselineStart.AddDays(-5), end, null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, baselineStart.AddDays(-3), ct);
        return new Rig { Scratch = scratch, Connection = connection, Postgres = postgres, Start = start, End = end, BaselineStart = baselineStart };
    }

    /// <summary>The fixture. The baseline holds 7 days, so a baseline count of N runs is N / 168 per hour; the recent window
    /// runs from 06:00 yesterday to now, so its hours depend on the clock and the tests compare counts, and per-hour figures as
    /// count / the read's own window hours.
    /// <list type="bullet">
    /// <item>10 flat Regular (cost 1000 us), Aborted 0 in the baseline and 4 recent: flagged timeouts, not cost.</item>
    /// <item>11 flat Regular, Aborted 8 baseline and 3 recent over 24 h+: rate falls, not flagged.</item>
    /// <item>12 flat Regular, Aborted 2 recent only (below the minimum of 3): not flagged.</item>
    /// <item>13 flat Regular, Exception 0 baseline and 5 recent: flagged errors.</item>
    /// <item>14 Regular CPU doubles AND Aborted 0 to 3: flagged cost and timeouts.</item>
    /// <item>15 Regular CPU doubles only: flagged cost.</item>
    /// <item>16 Exception 168 baseline (1 per hour) and 5 recent: rate below twice the baseline, not flagged.</item>
    /// </list></summary>
    private static async Task SeedFixtureAsync(DarlingCollectorRunner runner, ServerRuntime server, CollectorContext context, DateTime today, DateTime start, CancellationToken ct)
    {
        long id = 9000;
        async Task W(string type, DateTime first, long q, long n, long cpu, long dur)
        {
            id++;
            await WriteAsync(runner, server, context, type, id, first, q, n, cpu, dur, ct);
        }

        var b1 = start.AddDays(-5).AddHours(2);   /* a baseline instant, mid-day, well inside the 7 days */
        var b2 = start.AddDays(-3).AddHours(3);
        var r1 = start.AddMinutes(-10);           /* last = start + 20 min: recent */
        var r2 = start.AddHours(2);
        var r3 = start.AddHours(3);

        /* The table's floor: an interval before the baseline, outside both arms. */
        await W("Regular", start.AddDays(-12), 90, 5, 1_000, 1_000);

        foreach (var q in new long[] { 10, 11, 12, 13, 16 })
        {
            await W("Regular", b1, q, 100, 1_000, 2_000);
            await W("Regular", r2, q, 100, 1_000, 2_000);
        }

        await W("Regular", b1, 14, 100, 1_000, 2_000);
        /* #6781: enough recent runs that the rise adds at least 1 s of CPU (QueryStoreRegressionRules.CostMinAddedCpuMs). */
        await W("Regular", r2, 14, 2_000, 2_000, 4_000);
        await W("Regular", b1, 15, 100, 1_000, 2_000);
        await W("Regular", r2, 15, 2_000, 2_000, 4_000);

        /* 10: timeouts 0 -> 4 runs (two intervals of 2, 30 s and 50 s a run, CPU 500 ms and 700 ms a run). */
        await W("Aborted", r2, 10, 2, 500_000, 30_000_000);
        await W("Aborted", r3, 10, 2, 700_000, 50_000_000);
        /* 11: 80 baseline runs (0.48 an hour) and 3 recent (about 0.1 an hour over a window of 24 h or more): the rate fell. */
        await W("Aborted", b1, 11, 40, 10, 100);
        await W("Aborted", b2, 11, 40, 10, 100);
        await W("Aborted", r2, 11, 3, 10, 100);
        /* 12: two recent timeouts only. */
        await W("Aborted", r2, 12, 2, 10, 100);
        /* 13: errors 0 -> 5 (two intervals: 2 runs at 10 us CPU, 3 runs at 40 us CPU, 100 us and 400 us duration). */
        await W("Exception", r2, 13, 2, 10, 100);
        await W("Exception", r3, 13, 3, 40, 400);
        /* 14: cost and 3 timeouts. */
        await W("Aborted", r2, 14, 3, 10, 100);
        /* 16: 168 baseline errors (1 an hour), 5 recent: the recent rate is 5 / recent_hours, which is below 2 per hour. */
        await W("Exception", b1, 16, 84, 10, 100);
        await W("Exception", b2, 16, 84, 10, 100);
        await W("Exception", r2, 16, 5, 10, 100);
    }

    private static Task<Rig> OpenFixtureAsync(CancellationToken ct) =>
        OpenAsync((runner, server, context, today, start) => SeedFixtureAsync(runner, server, context, today, start, ct), ct);

    private static async Task<List<DarlingQueryStoreRegressionReader.RegressionRow>> ServedAsync(Rig rig, int limit, CancellationToken ct) =>
        (await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), limit, rig.BaselineStart, ct)).Rows;

    /// <summary>The same statement run by hand on the raw arm (no run days, all live): the wide table and raw must agree.</summary>
    private static async Task<List<DarlingQueryStoreRegressionReader.RegressionRow>> RunAsync(Rig rig, bool wide, int limit, CancellationToken ct)
    {
        var plan = QueryStoreRegressionBaseline.AllLive(rig.Start);
        await using var command = new NpgsqlCommand(DarlingQueryStoreRegressionReader.BuildSql(wide, useDailyTotals: false), rig.Connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.Start });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.End });
        command.Parameters.Add(DatabaseFilter.One(Db).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.BaselineStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.LowEnd });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.HighStart });
        command.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
        /* #6750: the start of the hours held, the row filters' start here. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.BaselineStart });
        /* #6750: the failure arm's row-filter start and its hours held, the same on this one-tier read. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.BaselineStart });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.BaselineStart });
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await DarlingQueryStoreRegressionReader.ReadRowsAsync(reader, ct);
    }

    private static string Describe(DarlingQueryStoreRegressionReader.RegressionRow r) => string.Join("|",
        r.QueryId, r.RegressionReason, r.Severity, r.BaselineExecCount, r.RecentExecCount,
        Math.Round(r.BaselineCpuMs, 6), Math.Round(r.RecentCpuMs, 6),
        r.Aborted!.BaselineCount, r.Aborted.RecentCount, r.Aborted.RecentCpuMs is { } ac ? Math.Round(ac, 6) : "-", r.Aborted.RecentDurationMs is { } ad ? Math.Round(ad, 6) : "-",
        r.Exception!.BaselineCount, r.Exception.RecentCount, r.Exception.RecentCpuMs is { } ec ? Math.Round(ec, 6) : "-", r.Exception.RecentDurationMs is { } ed ? Math.Round(ed, 6) : "-");

    private static DarlingQueryStoreRegressionReader.RegressionRow Row(IEnumerable<DarlingQueryStoreRegressionReader.RegressionRow> rows, long queryId) =>
        rows.Single(r => r.QueryId == queryId);

    /// <summary>
    /// #6654: the monitor's own statement and a dropped database's queries are not regressions. Four queries each triple their CPU:
    /// 20 (a normal statement), 21 and 22 (their text carries the collector's self-query marker: 21 as an inline sample, 22 only in
    /// query_store_text with a NULL inline sample, the shape a live Darling store has) and 30 in a
    /// database an earlier database_states snapshot lists and the newest does not. With no database chosen only 20 is listed and the
    /// dropped database is named in <c>databases_left_out</c>; naming the dropped database reads it as asked, and the marker stays out.
    /// </summary>
    [Fact]
    public async Task TheMonitorsOwnStatementAndADroppedDatabase_AreNotListed_AndTheDroppedOneIsNamed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6654 live tests.");
        var ct = TestContext.Current.CancellationToken;
        const string Gone = "qsGone";
        var now = LiveClock.Now();
        await using var rig = await OpenAsync(async (runner, server, context, today, start) =>
        {
            long id = 9500;
            async Task W(DateTime first, long q, long cpu, string database, string text)
            {
                id++;
                /* #6781: enough recent runs that the rise adds at least 1 s of CPU (QueryStoreRegressionRules.CostMinAddedCpuMs). */
                await WriteAsync(runner, server, context, "Regular", id, first, q, 1_000, cpu, cpu * 2, ct, database, text);
            }

            foreach (var (q, database, text) in new[]
            {
                (20L, Db, "SELECT normal"),
                (21L, Db, "/* " + QueryStoreCollector.SelfQueryMarker + " */ SELECT own"),
                (22L, Db, "SELECT own_in_text_table"),
                (30L, Gone, "SELECT in_dropped"),
            })
            {
                await W(now.AddDays(-5), q, 1_000, database, text);
                await W(now.AddHours(-3), q, 3_000, database, text);
            }

            await W(start.AddDays(-12), 90, 1_000, Db, "SELECT floor");
        }, ct);

        /* Query 22 is the shape a live Darling store has (#6665): the text fetch is separate, so the stats rows carry NO inline text
           and the marker sits only in collect.query_store_text, which is the arm the predicate's lookup reads. Query 21 keeps the
           inline sample, the fallback arm for a query the text table has no row for. */
        await using (var blank = new NpgsqlCommand("UPDATE query_store_stats SET query_text = NULL WHERE server_id = $1 AND query_id = 22", rig.Connection))
        {
            blank.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            await blank.ExecuteNonQueryAsync(ct);
        }

        await using (var production = new NpgsqlCommand(
            "INSERT INTO query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) VALUES ($1, $2, 22, $3, $4)",
            rig.Connection))
        {
            production.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            production.Parameters.Add(new NpgsqlParameter<string> { TypedValue = Db });
            production.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "/* " + QueryStoreCollector.SelfQueryMarker + " */ SELECT own_in_text_table" });
            production.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(now, DateTimeKind.Unspecified) });
            await production.ExecuteNonQueryAsync(ct);
        }

        foreach (var (age, names) in new[] { (TimeSpan.FromHours(3.5), new[] { Db, Gone }), (TimeSpan.FromHours(1), new[] { Db, Gone }), (TimeSpan.FromMinutes(5), new[] { Db }) })
        {
            var at = DateTime.SpecifyKind(DateTime.UtcNow - age, DateTimeKind.Unspecified);
            var n = 1;
            foreach (var name in names)
            {
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,'qsfail-host',$4,$5,'ONLINE',FALSE)",
                    rig.Connection);
                insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = CollectionIdGenerator.Next() });
                insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = at });
                insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });
                insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = n++ });
                await insert.ExecuteNonQueryAsync(ct);
            }
        }

        static (List<long> Ids, List<string> LeftOut) Parse(string json)
        {
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var ids = doc.RootElement.GetProperty("regressions").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).OrderBy(x => x).ToList();
            var left = doc.RootElement.TryGetProperty("databases_left_out", out var l) && l.ValueKind == System.Text.Json.JsonValueKind.Array
                ? l.EnumerateArray().Select(x => x.GetString()!).ToList()
                : [];
            return (ids, left);
        }

        var all = Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, "qsfail", 24, DatabaseFilter.All, 30, false, null, NullLogger.Instance, ct));
        Assert.Equal([20L], all.Ids);
        Assert.Equal([Gone], all.LeftOut);

        var named = Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, "qsfail", 24, DatabaseFilter.One(Gone), 30, false, null, NullLogger.Instance, ct));
        Assert.Equal([30L], named.Ids);
        Assert.Empty(named.LeftOut);

        var db = Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, "qsfail", 24, DatabaseFilter.One(Db), 30, false, null, NullLogger.Instance, ct));
        Assert.Equal([20L], db.Ids);

        /* The Viewer grid reads the same way and hands the left-out names back for its banner (#6654 review): with no database chosen
           the dropped one is named and its query is not listed; naming it reads it as asked and names nothing. */
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var viewerStart = now.AddHours(-24);
        var viewerAll = await viewer.GetQueryStoreRegressionsWithScopeAsync(ServerId, viewerStart, now, cancellationToken: ct);
        Assert.Equal([20L], viewerAll.Rows.Select(r => r.QueryId).OrderBy(x => x).ToList());
        Assert.Equal([Gone], viewerAll.DatabasesLeftOut);
        var viewerNamed = await viewer.GetQueryStoreRegressionsWithScopeAsync(ServerId, viewerStart, now, [Gone], cancellationToken: ct);
        Assert.Equal([30L], viewerNamed.Rows.Select(r => r.QueryId).OrderBy(x => x).ToList());
        Assert.Null(viewerNamed.DatabasesLeftOut);

        /* #6665: a custom range that ended BEFORE the drop is judged by the databases the server had then (the dropped set is
           anchored at the range's end, as get_query_store_regressions does with as_of), so the database dropped later still shows
           its rows for that range. The newest snapshot at or before the range's end (3.5 hours ago) lists both databases. */
        var rangeEnd = now.AddHours(-2);
        var viewerHistorical = await viewer.GetQueryStoreRegressionsWithScopeAsync(ServerId, rangeEnd.AddHours(-24), rangeEnd, literalEndUtc: rangeEnd, cancellationToken: ct);
        Assert.Equal([20L, 30L], viewerHistorical.Rows.Select(r => r.QueryId).OrderBy(x => x).ToList());
        Assert.Null(viewerHistorical.DatabasesLeftOut);
    }

    [Fact]
    public async Task EachGroupsCountAndPerExecutionFigures_AreTheHandWorkedOnes_OnTheWideArmAndTheRawArm()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenFixtureAsync(ct);

        var wide = await RunAsync(rig, wide: true, 50, ct);
        var raw = await RunAsync(rig, wide: false, 50, ct);
        Assert.Equal(wide.Select(Describe).ToList(), raw.Select(Describe).ToList());
        var served = await ServedAsync(rig, 50, ct);
        Assert.Equal(wide.Select(Describe).ToList(), served.Select(Describe).ToList());

        /* Query 10, hand-worked. Aborted: recent 4 runs; CPU (2 x 500,000 + 2 x 700,000) / 4 = 600,000 us = 600 ms; duration
           (2 x 30,000,000 + 2 x 50,000,000) / 4 = 40,000,000 us = 40,000 ms. None in the baseline: count 0, figures NULL. */
        var q10 = Row(wide, 10);
        Assert.Equal("timeouts", q10.RegressionReason);
        Assert.Equal(0L, q10.Aborted!.BaselineCount);
        Assert.Equal(4L, q10.Aborted.RecentCount);
        Assert.Null(q10.Aborted.BaselineCpuMs);
        Assert.Null(q10.Aborted.BaselineDurationMs);
        Assert.Equal(600.0, q10.Aborted.RecentCpuMs!.Value, 6);
        Assert.Equal(40_000.0, q10.Aborted.RecentDurationMs!.Value, 6);
        Assert.Equal(0.0, q10.Aborted.BaselinePerHour!.Value, 9);
        Assert.Equal(4L, q10.Aborted.RecentCount);
        Assert.Equal(0L, q10.Exception!.RecentCount);
        Assert.Null(q10.Exception.RecentCpuMs);
        /* Regular, flat: 100 executions a side at 1 ms CPU and 2 ms duration. */
        Assert.Equal(100L, q10.BaselineExecCount);
        Assert.Equal(100L, q10.RecentExecCount);
        Assert.Equal(1.0, q10.BaselineCpuMs, 6);
        Assert.Equal(1.0, q10.RecentCpuMs, 6);
        /* Per hour: 100 runs over the 168 baseline hours, and 4 timeouts over the recent window's own hours. */
        Assert.Equal(100.0 / 168.0, q10.BaselineRegularPerHour!.Value, 6);
        var recentHours = (rig.End - rig.Start).TotalHours;
        Assert.Equal(4.0 / recentHours, q10.Aborted.RecentPerHour!.Value, 4);

        /* Query 13, hand-worked. Exception recent 5 runs: CPU (2 x 10 + 3 x 40) / 5 = 28 us = 0.028 ms; duration
           (2 x 100 + 3 x 400) / 5 = 280 us = 0.28 ms. */
        var q13 = Row(wide, 13);
        Assert.Equal("errors", q13.RegressionReason);
        Assert.Equal(5L, q13.Exception!.RecentCount);
        Assert.Equal(0.028, q13.Exception.RecentCpuMs!.Value, 6);
        Assert.Equal(0.28, q13.Exception.RecentDurationMs!.Value, 6);
        Assert.Equal(0L, q13.Aborted!.RecentCount);
    }

    [Fact]
    public async Task ARiseInTimeoutsOrErrors_FlagsAQueryWhoseRegularCostIsFlat_AndOneBelowTheThresholdIsNot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenFixtureAsync(ct);
        var rows = await ServedAsync(rig, 50, ct);

        /* Flagged: 10 timeouts (0 -> 4), 13 errors (0 -> 5), 14 cost and timeouts, 15 cost. */
        Assert.Equal(new long[] { 10, 13, 14, 15 }, rows.Select(r => r.QueryId).OrderBy(id => id).ToArray());
        Assert.Equal("timeouts", Row(rows, 10).RegressionReason);
        Assert.Equal("errors", Row(rows, 13).RegressionReason);
        Assert.Equal("cost,timeouts", Row(rows, 14).RegressionReason);
        Assert.Equal("cost", Row(rows, 15).RegressionReason);
        /* A timeouts-only row's Regular cost is flat (no cost rise), and is listed anyway. */
        Assert.Equal(Row(rows, 10).BaselineCpuMs, Row(rows, 10).RecentCpuMs, 6);

        /* Not flagged: 11 has a rate that fell (80 baseline, 3 recent), 12 has 2 recent runs (below the minimum of 3), 16 has a
           recent rate below twice the baseline's (5 runs over the window against 168 over 168 hours). */
        foreach (var quiet in new long[] { 11, 12, 16 })
        {
            Assert.DoesNotContain(rows, r => r.QueryId == quiet);
        }
    }

    [Fact]
    public async Task ACostRiseAndATimeoutRise_CarryBothReasons_AndCostRowsRankBeforeTheFailureOnlyRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenFixtureAsync(ct);
        var rows = await ServedAsync(rig, 50, ct);

        var both = Row(rows, 14);
        Assert.Equal("cost,timeouts", both.RegressionReason);
        Assert.Equal(100.0, both.CpuRegressionPercent!.Value, 4);
        Assert.Equal(3L, both.Aborted!.RecentCount);
        /* Severity: the duration doubled exactly, which is not over 100%, so the cost band is HIGH (over 50); the timeout rose from
           none, which is HIGH too. */
        Assert.Equal("HIGH", both.Severity);

        /* Cost rows come first, then the failure-only rows; a failure-only row is never CRITICAL. */
        var costCount = rows.Count(r => r.RegressionReason.Contains("cost", StringComparison.Ordinal));
        Assert.All(rows.Take(costCount), r => Assert.Contains("cost", r.RegressionReason, StringComparison.Ordinal));
        Assert.All(rows.Skip(costCount), r => Assert.DoesNotContain("cost", r.RegressionReason, StringComparison.Ordinal));
        Assert.All(rows.Skip(costCount), r => Assert.NotEqual("CRITICAL", r.Severity));
    }

    [Fact]
    public async Task AFloodOfFailureOnlyRows_LeavesTheCostRowsTheirShareOfTheCap()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(async (runner, server, context, today, start) =>
        {
            long id = 9000;
            async Task W(string type, DateTime first, long q, long n, long cpu)
            {
                id++;
                await WriteAsync(runner, server, context, type, id, first, q, n, cpu, cpu * 2, ct);
            }

            await W("Regular", start.AddDays(-12), 900, 5, 1_000);
            var b1 = start.AddDays(-5).AddHours(2);
            var r2 = start.AddHours(2);
            /* Three cost rows, then 30 error-only rows with more failed runs than any cost row has. */
            for (long q = 1; q <= 3; q++)
            {
                await W("Regular", b1, q, 100, 1_000);
                /* #6781: enough recent runs that the rise adds at least 1 s of CPU (QueryStoreRegressionRules.CostMinAddedCpuMs). */
                await W("Regular", r2, q, 1_000, 2_000 + (q * 100));
            }

            for (long q = 100; q < 130; q++)
            {
                await W("Regular", b1, q, 100, 1_000);
                await W("Regular", r2, q, 100, 1_000);
                await W("Exception", r2, q, 50 + q, 10);
            }
        }, ct);

        /* A cap of 9: failure-only rows take at most 9 / 3 = 3, the three cost rows keep their slots. */
        var rows = await ServedAsync(rig, 9, ct);
        Assert.Equal(9, rows.Count);
        Assert.Equal(new long[] { 1, 2, 3 }, rows.Where(r => r.RegressionReason == "cost").Select(r => r.QueryId).OrderBy(id => id).ToArray());
        Assert.Equal(6, rows.Count(r => r.RegressionReason == "errors"));
        /* The cap was 9 but the cost rows took only 3, so the failure-only rows fill the rest, the most failed runs first. */
        Assert.Equal(new long[] { 129, 128, 127, 126, 125, 124 }, rows.Where(r => r.RegressionReason == "errors").Select(r => r.QueryId).ToArray());

        /* A cap of 4: cost rows 3 of the 4 slots, failure-only the share of 4 / 3 = 1. */
        var tight = await ServedAsync(rig, 4, ct);
        Assert.Equal(3, tight.Count(r => r.RegressionReason == "cost"));
        Assert.Equal(1, tight.Count(r => r.RegressionReason == "errors"));
        Assert.Equal(33L, tight[0].TotalCount);
    }

    [Fact]
    public async Task TheViewersReadFlagsAndCarriesTheSameGroups()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenFixtureAsync(ct);
        var served = await ServedAsync(rig, 50, ct);

        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var rows = await viewer.GetQueryStoreRegressionsAsync(ServerId, rig.Start, rig.End, [Db], null, ct);
        Assert.Equal(served.Select(r => r.QueryId).ToList(), rows.Select(r => r.QueryId).ToList());
        var q10 = rows.Single(r => r.QueryId == 10);
        Assert.Equal("timeouts", q10.RegressionReason);
        Assert.Equal(4L, q10.RecentAbortedCount);
        Assert.Equal(600.0, q10.RecentAbortedCpuMs!.Value, 6);
        Assert.Equal(40_000.0, q10.RecentAbortedDurationMs!.Value, 6);
        Assert.Equal("cost,timeouts", rows.Single(r => r.QueryId == 14).RegressionReason);
        Assert.Equal(0.028, rows.Single(r => r.QueryId == 13).RecentExceptionCpuMs!.Value, 6);
    }

    [Fact]
    public async Task TheMcpToolReportsTheGroupsAndTheReason()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenFixtureAsync(ct);
        var hours = (int)Math.Ceiling((rig.End - rig.Start).TotalHours) + 1;
        var json = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, "qsfail", Math.Max(24, hours), Db, 50, false, null, null, ct);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var rows = doc.RootElement.GetProperty("regressions").EnumerateArray().ToList();
        var q10 = rows.Single(r => r.GetProperty("query_id").GetInt64() == 10);
        Assert.Equal("timeouts", q10.GetProperty("regression_reason").GetString());
        Assert.Equal(4, q10.GetProperty("recent_aborted_count").GetInt64());
        Assert.Equal(0, q10.GetProperty("baseline_aborted_count").GetInt64());
        /* A side with no runs has no per-execution figure, so the field is left out. */
        Assert.False(q10.TryGetProperty("baseline_aborted_cpu_ms", out _));
        Assert.Equal(600.0, q10.GetProperty("recent_aborted_cpu_ms").GetDouble(), 2);
        Assert.False(q10.TryGetProperty("baseline_reads", out _));
        Assert.Contains("timeouts", doc.RootElement.GetProperty("gate").GetString(), StringComparison.Ordinal);
    }

    private static System.Text.Json.JsonValueKind JsonValueKind(System.Text.Json.JsonElement row, string name) => row.GetProperty(name).ValueKind;

    [Fact]
    public async Task TheReadGivesTheSameRows_WithAndWithoutThePartialIndex_AndTheBuilderCreatesIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #5506 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenFixtureAsync(ct);

        var spec = QueryStoreBackgroundIndexes.WideNonRegularFirstExec;
        Assert.Contains(spec, QueryStoreBackgroundIndexes.All);
        Assert.Null(await ScalarAsync(rig.Connection, $"SELECT to_regclass('{spec.IndexName}')::text", ct));
        var without = (await ServedAsync(rig, 50, ct)).Select(Describe).ToList();
        Assert.NotEmpty(without);

        await QueryStoreBackgroundIndexes.EnsureAsync(rig.Connection, spec, NullLogger.Instance, ct);

        /* Built and valid, partial, keyed (server_id, first_execution_time), with the Regular rows left out of it. */
        Assert.True((bool)(await ScalarAsync(rig.Connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{spec.IndexName}'::regclass", ct))!);
        var definition = (string)(await ScalarAsync(rig.Connection, $"SELECT pg_get_indexdef('{spec.IndexName}'::regclass)", ct))!;
        Assert.Contains("(server_id, first_execution_time)", definition, StringComparison.Ordinal);
        Assert.Contains("WHERE (execution_type_desc <> 'Regular'", definition, StringComparison.Ordinal);
        /* Idempotent: a second ensure on the built index changes nothing. */
        await QueryStoreBackgroundIndexes.EnsureAsync(rig.Connection, spec, NullLogger.Instance, ct);

        var with = (await ServedAsync(rig, 50, ct)).Select(Describe).ToList();
        Assert.Equal(without, with);
        Assert.Equal(without, (await RunAsync(rig, wide: true, 50, ct)).Select(Describe).ToList());

        /* The planner can use it: with the read's own predicate and a seq scan switched off, the plan names the partial index. */
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection, "ANALYZE collect.query_store_interval_wide;", null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection, "SET enable_seqscan = off;", null, ct);
        await using var explain = new NpgsqlCommand(
            "EXPLAIN (FORMAT TEXT) SELECT execution_count FROM collect.query_store_interval_wide WHERE server_id = " + ServerId.ToString(CultureInfo.InvariantCulture)
            + " AND " + QueryStoreRegressionBaseline.NonRegularPredicate + " AND first_execution_time >= timestamp '2000-01-01' AND first_execution_time <= timestamp '2100-01-01'", rig.Connection);
        var text = new System.Text.StringBuilder();
        await using (var reader = await explain.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                text.AppendLine(reader.GetString(0));
            }
        }

        Assert.Contains("ix_query_store_interval_wide_nonregular_first_exec", text.ToString(), StringComparison.Ordinal);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        return value is DBNull ? null : value;
    }
}
