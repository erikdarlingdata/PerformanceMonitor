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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6750 part 3: the cost baseline's first day comes from the daily totals. Latest drops whole days at 15 days, so on a full store
/// its floor is the start of the day before the baseline's first day, and the floor plus the 26-hour purge-edge margin is 02:00 of
/// the first day. Before the change the cost baseline started there: the first day's 00:00 to 02:00 rows were never compared, and
/// the rest of the day was read live. Now, when that day is a whole valid build at the head of the run, the cost baseline starts at
/// its midnight and the day comes from <c>plan_regression_daily</c>. The answer must equal an all-live read over the same span
/// taken while latest still held the day before. Seeded through the real write path, with the daily days built. Every instant
/// is anchored to the seeded rows, so the class passes at any hour.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test creates and drops its own database through
   ScratchPostgres. It shares the gap-cache collection with the other regressions table tests, because the gate's cadence check
   reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsBuiltFirstDayLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsFirst";
    private const string ServerName = "qsfirst";
    private const long FillerQuery = 71;
    private const long EarlyQuery = 72;
    private const int Limit = 500;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public DateTime Now { get; init; }

        /// <summary>The reads' end, taken after the seed: the store stamps <c>applied_through</c> with its own clock, and a literal
        /// end before it would send the read to raw.</summary>
        public DateTime End { get; set; }

        public DateTime WindowStart => End.AddHours(-168);
        public DateTime BaselineStart => End.AddHours(-336);

        /// <summary>The baseline's first day: the requested start's midnight.</summary>
        public DateTime FirstDay => BaselineStart.Date;

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// 17 days of <see cref="FillerQuery"/> at 1 ms CPU, one interval every 6 hours. <see cref="EarlyQuery"/> ran in the baseline
    /// only between 00:00 and 02:00 of its first day, at 1 ms, and in the recent window at 5 ms: its regression shows only with those
    /// two hours. One interval begins at exactly the midnight of the day before the first day, so once latest's older rows are
    /// gone its floor plus 26 hours is 02:00 of the first day. Raw keeps less than the recent window and the wide table about two
    /// days before it, the large production store's shape, so the cost arms read latest below raw's floor.
    /// </summary>
    private static async Task<Rig> StartAsync(CancellationToken ct)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");

        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var now = LiveClock.Now();
        if (now.TimeOfDay > TimeSpan.FromHours(23.95))
        {
            /* The seed takes seconds, and the reads' end must fall on the same UTC day as the seed's clock. */
            await Task.Delay(now.Date.AddDays(1).AddSeconds(5) - now, ct);
            now = LiveClock.Now();
        }

        var rig = new Rig { Scratch = scratch, Connection = connection, Postgres = postgres, Now = now, End = now };

        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, $@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, '{ServerName}', '{ServerName}', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName + "-host", CollectionTime = DateTime.SpecifyKind(rig.Now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = ServerName + "-host" },
            ConnectionString = "Server=" + ServerName + "-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = ServerName + "-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = 67_700;
        QueryStoreCollector.Row Row(long queryId, DateTime lastRun, long count, long cpuUs)
        {
            intervalId++;
            return new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = queryId + 91,
                ExecutionTypeDesc = "Regular",
                FirstExecutionTime = lastRun.AddMinutes(-50),
                LastExecutionTime = lastRun,
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + (queryId + 91).ToString("X8", CultureInfo.InvariantCulture),
                QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ExecutionCount = count,
                AvgCpuTimeUs = cpuUs,
                AvgDurationUs = cpuUs * 2,
                AvgLogicalIoReads = 100,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = intervalId,
                IntervalStartTimeUtc = lastRun.AddMinutes(-60),
            };
        }

        var first = rig.FirstDay;
        var rows = new List<QueryStoreCollector.Row>
        {
            /* The interval that sets latest's floor once the older rows are gone: it begins at the midnight before the first day. */
            Row(FillerQuery, first.AddDays(-1).AddMinutes(50), 10, 1_000),
            Row(EarlyQuery, first.AddMinutes(40), 10, 1_000),
            Row(EarlyQuery, first.AddMinutes(100), 10, 1_000),
        };
        for (var t = rig.Now.AddDays(-17).AddHours(3); t < rig.Now.AddHours(-1); t = t.AddHours(6))
        {
            rows.Add(Row(FillerQuery, t, 10, 1_000));
        }

        for (var t = rig.WindowStart.AddHours(6); t < rig.Now.AddHours(-1); t = t.AddHours(12))
        {
            /* #6781: enough recent runs that the rise adds at least 1 s of CPU (QueryStoreRegressionRules.CostMinAddedCpuMs). */
            rows.Add(Row(EarlyQuery, t, 10, 20_000));
        }

        /* One collection per day: each batch is collected ten minutes after its last interval. */
        foreach (var day in rows.GroupBy(r => r.LastExecutionTime!.Value.Date).OrderBy(g => g.Key))
        {
            var collected = day.Max(r => r.LastExecutionTime!.Value).AddMinutes(10);
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, day.ToList(), server, DateTime.SpecifyKind(collected, DateTimeKind.Unspecified), context, ct);
        }

        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, rig.Now.AddDays(-20), rig.Now.AddMinutes(-30), null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, rig.Now.AddDays(-20), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection,
            "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @cutoff WHERE server_id = @server_id", rig.Now.AddDays(-20), ct);
        await PlanRegressionDaily.RunTickAsync(postgres, rig.Now, NullLogger.Instance, ct);
        Assert.Equal(1L, Convert.ToInt64(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection,
            $"SELECT count(*) FROM collect.plan_regression_daily_built WHERE server_id = {ServerId} AND day = DATE '{first:yyyy-MM-dd}' AND built_seq = late_seq", ct), CultureInfo.InvariantCulture));

        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(
            new QueryStoreIntervalWideBelowFloorLiveTests.Rig { Scratch = scratch, Connection = connection, Postgres = postgres }, rig.Now.AddDays(-5), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.PurgeTableAsync(connection, rig.WindowStart.AddDays(-2), ct);
        rig.End = DateTime.SpecifyKind(DateTime.UtcNow.AddSeconds(1), DateTimeKind.Unspecified);
        Assert.Equal(rig.Now.Date, rig.End.Date);
        return rig;
    }

    /// <summary>Latest's retention: the whole days below the one before the first day are gone, as a 15-day day-partition drop
    /// leaves them. Latest's floor is then the midnight before the first day, and its floor plus 26 hours is 02:00 of the first day.</summary>
    private static async Task DropLatestOldestDaysAsync(Rig rig, CancellationToken ct)
    {
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection,
            "DELETE FROM collect.query_store_interval_latest WHERE server_id = @server_id AND first_execution_time < @cutoff", rig.FirstDay.AddDays(-1), ct);
        var floor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(rig.Connection,
            QueryStoreIntervalLatest.PlainTableFloorSql.Replace("$1", "@server_id", StringComparison.Ordinal), ct))!;
        Assert.Equal(rig.FirstDay.AddDays(-1), floor);
        Assert.Equal(rig.FirstDay.AddHours(2), floor + QueryStoreIntervalWide.PurgeEdgeMargin);
    }

    private static Task BumpLateSeqAsync(Rig rig, DateTime day, CancellationToken ct) =>
        QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(rig.Connection,
            $"UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1 WHERE server_id = @server_id AND day = DATE '{day:yyyy-MM-dd}'", null, ct);

    /// <summary>The product's table read, its rows and its cost start (<see cref="DarlingQueryStoreRegressionReader.ReadWithSpansAsync"/>).</summary>
    private static async Task<(List<DarlingQueryStoreRegressionReader.RegressionRow> Rows, DateTime CostStart, DateTime FailureStart)> ProductAsync(Rig rig, CancellationToken ct)
    {
        var read = await DarlingQueryStoreRegressionReader.ReadWithSpansAsync(
            rig.Postgres, ServerId, rig.WindowStart, rig.End, DatabaseFilter.All, Limit, rig.BaselineStart, null, ct);
        Assert.Equal("interval_table", read.Source);
        Assert.True(read.Spans!.CostFromLatest);
        return (read.Rows, read.Spans.CostStart, read.Spans.FailureStart);
    }

    /// <summary>The statement with every Regular row read live from latest (no day from the daily totals), the cost arms from
    /// <paramref name="costStart"/> and the failure arm from <paramref name="failureStart"/>, as the product binds them.</summary>
    private static async Task<List<DarlingQueryStoreRegressionReader.RegressionRow>> AllLiveAsync(Rig rig, DateTime costStart, DateTime failureStart, CancellationToken ct)
    {
        var plan = QueryStoreRegressionBaseline.AllLive(rig.WindowStart) with { RegularFromLatest = true, HoursHeldStart = costStart, FailureHoursHeldStart = failureStart };
        await using var transaction = await rig.Connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        await using var command = new NpgsqlCommand(DarlingQueryStoreRegressionReader.TableSqlFor(plan), rig.Connection, transaction);
        DarlingQueryStoreRegressionReader.AddParameters(command, ServerId, rig.WindowStart, rig.End, DatabaseFilter.All, Limit, costStart, plan, failureStart);
        List<DarlingQueryStoreRegressionReader.RegressionRow> rows;
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            rows = await DarlingQueryStoreRegressionReader.ReadRowsAsync(reader, ct);
        }

        await transaction.RollbackAsync(ct);
        return rows;
    }

    private static void AssertSameAnswer(List<DarlingQueryStoreRegressionReader.RegressionRow> expected, List<DarlingQueryStoreRegressionReader.RegressionRow> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i], actual[i]);
        }
    }

    private static DateTime Instant(JsonElement payload, string key) =>
        DateTime.Parse(payload.GetProperty(key).GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

    /// <summary>The core case: the first day is a whole valid build, so the cost baseline starts at its midnight, the query whose
    /// baseline is only the 00:00 to 02:00 rows is compared and flagged, and the answer equals the all-live read over the same span
    /// taken before latest dropped the day before. Before the change the cost start was 02:00 and that query had no baseline.</summary>
    [Fact]
    public async Task AValidFirstDay_StartsTheCostBaselineAtItsMidnight_AndEqualsTheAllLiveRead()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);
        var (_, _, failureStart) = await ProductAsync(rig, ct);
        var expected = await AllLiveAsync(rig, rig.FirstDay, failureStart, ct);

        await DropLatestOldestDaysAsync(rig, ct);
        var (rows, costStart, _) = await ProductAsync(rig, ct);
        Assert.Equal(rig.FirstDay, costStart);
        var early = Assert.Single(rows, r => r.QueryId == EarlyQuery);
        Assert.Equal(20, early.BaselineExecCount);
        Assert.Equal("cost", early.RegressionReason);
        Assert.InRange(early.BaselineCpuMs, 0.99, 1.01);
        AssertSameAnswer(expected, rows);
        /* The baseline's hours start at the midnight too. */
        var perHour = 20 / (rig.WindowStart - rig.FirstDay).TotalHours;
        Assert.InRange(early.BaselineRegularPerHour!.Value, perHour - 0.01, perHour + 0.01);

        /* The MCP payload and the Viewer's banner name the same start. */
        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168, limit: 30)).RootElement;
        Assert.Equal(McpHelpers.FormatEffectiveStart(Instant(hit, "baseline_start").Date), hit.GetProperty("cost_baseline_start").GetString());
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var read = await viewer.GetQueryStoreRegressionsWithScopeAsync(ServerId, rig.WindowStart, rig.End, [Db], cancellationToken: ct);
        Assert.Equal("interval_table", read.Source);
        Assert.Equal(rig.FirstDay, read.CostBaselineStart);
        Assert.Equal(20, Assert.Single(read.Rows, r => r.QueryId == EarlyQuery).BaselineExecCount);
    }

    /// <summary>The first day is stale (a late row bumped its <c>late_seq</c>): its totals may undercount, so the cost baseline keeps
    /// today's start, latest's floor plus 26 hours, and the answer still equals the all-live read from there.</summary>
    [Fact]
    public async Task AStaleFirstDay_KeepsTheFloorStart_AndStaysExact()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);
        await BumpLateSeqAsync(rig, rig.FirstDay, ct);
        var (_, _, failureStart) = await ProductAsync(rig, ct);
        var expected = await AllLiveAsync(rig, rig.FirstDay.AddHours(2), failureStart, ct);

        await DropLatestOldestDaysAsync(rig, ct);
        var (rows, costStart, _) = await ProductAsync(rig, ct);
        Assert.Equal(rig.FirstDay.AddHours(2), costStart);
        Assert.DoesNotContain(rows, r => r.QueryId == EarlyQuery && r.BaselineExecCount > 0);
        AssertSameAnswer(expected, rows);
    }

    /// <summary>A gap in the run: the day after the first is stale, so the first day is not at the head of the run the read takes,
    /// and the cost baseline keeps latest's floor plus 26 hours.</summary>
    [Fact]
    public async Task AGapAfterTheFirstDay_KeepsTheFloorStart_AndStaysExact()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);
        await BumpLateSeqAsync(rig, rig.FirstDay.AddDays(1), ct);
        var (_, _, failureStart) = await ProductAsync(rig, ct);
        var expected = await AllLiveAsync(rig, rig.FirstDay.AddHours(2), failureStart, ct);

        await DropLatestOldestDaysAsync(rig, ct);
        var (rows, costStart, _) = await ProductAsync(rig, ct);
        Assert.Equal(rig.FirstDay.AddHours(2), costStart);
        AssertSameAnswer(expected, rows);
    }

    /// <summary>With the run starting at the cost start, the baseline's live low arm has no range left, and the read does not scan
    /// latest for it: the executed plan has one fewer latest scan than the same statement bound with a start one second earlier,
    /// where the arm has a range again. Measured on the large production store, that empty arm still read 16 MB cold.</summary>
    [Fact]
    public async Task WithTheRunAtTheCostStart_TheLowLiveArmScansNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);
        await DropLatestOldestDaysAsync(rig, ct);
        var (_, _, failureStart) = await ProductAsync(rig, ct);

        var wide = await QueryStoreIntervalWide.ResolveReadAsync(
            rig.Connection, ServerId, rig.BaselineStart, rig.End, rig.End, DarlingDataReader.QueryStoreTopMinWindow, 60, null,
            QueryStoreIntervalWide.LegacyRows.Allow, ct);
        await using var transaction = await rig.Connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        var spans = await QueryStoreRegressionBaseline.ResolveSpansAsync(rig.Connection, transaction, ServerId, rig.BaselineStart, wide, 60, null, ct);
        var plan = await QueryStoreRegressionBaseline.ResolveAsync(rig.Connection, transaction, ServerId, spans.RegularStart, rig.WindowStart, 60, null, ct, windowEnd: rig.End, spans: spans);
        Assert.Equal(rig.FirstDay, plan.RegularStart);
        Assert.Equal(rig.FirstDay, plan.LowEnd);
        plan = plan with { HoursHeldStart = rig.FirstDay, FailureHoursHeldStart = failureStart };
        var sql = DarlingQueryStoreRegressionReader.TableSqlFor(plan);

        async Task<int> LatestScansAsync(DateTime costStart)
        {
            await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, rig.Connection, transaction);
            DarlingQueryStoreRegressionReader.AddParameters(command, ServerId, rig.WindowStart, rig.End, DatabaseFilter.All, Limit, costStart, plan, failureStart);
            using var doc = JsonDocument.Parse((string)(await command.ExecuteScalarAsync(ct))!);
            var count = 0;
            void Walk(JsonElement node)
            {
                if (node.ValueKind == JsonValueKind.Object)
                {
                    if (node.TryGetProperty("Relation Name", out var name)
                        && name.GetString()!.Contains(QueryStoreIntervalLatest.TableName, StringComparison.Ordinal)
                        && node.TryGetProperty("Actual Loops", out var loops) && loops.GetDouble() > 0)
                    {
                        count++;
                    }

                    foreach (var p in node.EnumerateObject()) Walk(p.Value);
                }
                else if (node.ValueKind == JsonValueKind.Array)
                {
                    foreach (var e in node.EnumerateArray()) Walk(e);
                }
            }

            Walk(doc.RootElement);
            return count;
        }

        var atStart = await LatestScansAsync(rig.FirstDay);
        var secondEarlier = await LatestScansAsync(rig.FirstDay.AddSeconds(-1));
        Assert.True(atStart < secondEarlier, $"the low arm scanned latest {atStart} times at the cost start, {secondEarlier} a second earlier");
        await transaction.RollbackAsync(ct);
    }
}
