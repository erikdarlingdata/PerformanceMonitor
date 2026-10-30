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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6750: the raw read divides each per-hour rate by the hours the raw table HOLDS, not by the 7 days it was asked for. On a
/// store whose raw floor falls inside the requested baseline (read here at 24 hours), the baseline failure rates came out
/// understated and a timeout rate that had not moved was flagged as a rise. The rows read do not change: every row filter keeps
/// the requested baseline start, so an interval collected after the floor whose last execution is before it is still counted.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and works entirely inside it. It shares the gap-cache collection with the other regressions table
   tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsHoursHeldLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsHeld";
    private const string ServerName = "qsheld";

    /// <summary>The same timeout rate per hour on both sides over the hours held, so it is not a rise.</summary>
    private const long SteadyQuery = 10;

    /// <summary>A real rise: 20 in the baseline (10 of them the edge interval), 120 in the 24 hour window.</summary>
    private const long RisingQuery = 20;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public DateTime Start { get; init; }
        public DateTime End { get; init; }
        public DateTime BaselineStart => Start.AddDays(-7);
        public DateTime RawFloor { get; set; }

        /// <summary>Where THIS server's data starts: the per-server probe the answer's <c>effective_start</c> comes from.</summary>
        public DateTime DataStart { get; set; }

        /// <summary>The table path's failure start (#6750 part 2): this server's own oldest wide row plus the purge-edge margin, and the same
        /// for the OTHER server's older row. Both come from the seeded times (a snapshot's first execution is 50 minutes before its collection).</summary>
        public DateTime OwnFailureStart { get; set; }
        public DateTime OtherFailureStart { get; set; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// A hypertable store read at 24 hours. <paramref name="floorInsideBaseline"/> drops every raw chunk older than the marker
    /// interval (three days back), so raw's floor is inside the requested baseline. False keeps a row nine days back, so raw holds
    /// the whole baseline. The interval table's coverage is forced to start after the baseline, so the gate reads raw either way.
    /// </summary>
    private static async Task<Rig> StartAsync(bool floorInsideBaseline, CancellationToken ct)
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
        var rig = new Rig { Scratch = scratch, Connection = connection, Postgres = postgres, End = now.AddSeconds(1), Start = now.AddSeconds(1).AddHours(-24) };

        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, $@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, '{ServerName}', '{ServerName}', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = "qsheld-host", CollectionTime = DateTime.SpecifyKind(now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = "qsheld-host" },
            ConnectionString = "Server=qsheld-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsheld-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = 9000;
        /* One snapshot of one Aborted interval: collected at `collected`, last run at `lastRun`. */
        async Task SnapshotAsync(long queryId, DateTime collected, DateTime lastRun, long count, string outcome = "Aborted")
        {
            intervalId++;
            var row = new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = queryId + 91,
                ExecutionTypeDesc = outcome,
                FirstExecutionTime = lastRun.AddMinutes(-30),
                LastExecutionTime = lastRun,
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + (queryId + 91).ToString("X8", CultureInfo.InvariantCulture),
                QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ExecutionCount = count,
                AvgCpuTimeUs = 40_000,
                AvgDurationUs = 80_000,
                AvgLogicalIoReads = 4_000,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = intervalId,
                IntervalStartTimeUtc = lastRun.AddMinutes(-30),
            };
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { row }, server, collected, context, ct);
        }

        /* The marker is the oldest row that survives: the chunk holding it is raw's floor once the older chunks are dropped. A row
           outside the baseline (last run before its start, collected after the floor) is not counted by either binding. */
        var marker = now.AddDays(-3);
        await SnapshotAsync(SteadyQuery, marker, marker.AddMinutes(-20), 10);
        await SnapshotAsync(SteadyQuery, now.AddHours(-3), now.AddHours(-3).AddMinutes(-20), 15);
        await SnapshotAsync(RisingQuery, now.AddHours(-3), now.AddHours(-3).AddMinutes(-20), 120);
        await SnapshotAsync(SteadyQuery, now.AddDays(-9), now.AddDays(-9).AddMinutes(-20), 10);
        if (floorInsideBaseline)
        {
            await SnapshotAsync(SteadyQuery, now.AddDays(-7).AddHours(-2), now.AddDays(-7).AddHours(-2).AddMinutes(-20), 10);
        }

        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, now.AddHours(-1), ct);
        if (floorInsideBaseline)
        {
            await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(
                new QueryStoreIntervalWideBelowFloorLiveTests.Rig { Scratch = scratch, Connection = connection, Postgres = postgres }, marker.AddMinutes(-1), ct);
        }

        rig.RawFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection, QueryStoreIntervalWideBelowFloorLiveTests.ChunkFloorSql, ct))!;
        if (floorInsideBaseline)
        {
            Assert.True(rig.RawFloor > rig.BaselineStart && rig.RawFloor < rig.Start, $"raw's floor {rig.RawFloor:o} should be inside the baseline {rig.BaselineStart:o} to {rig.Start:o}");
        }
        else
        {
            Assert.True(rig.RawFloor <= rig.BaselineStart, $"raw's floor {rig.RawFloor:o} should be at or before the baseline start {rig.BaselineStart:o}");
        }

        /* This server's own data start, the instant its answer reports as effective_start: the marker's collection time when the older
           chunks are gone, the nine-day-old row's when raw holds the whole baseline. */
        rig.DataStart = (await DarlingMcpWindowNotice.Probe(postgres, "query_store_stats", ServerName, rig.BaselineStart, rig.End, ct))!.Value;

        /* The baseline rows, seeded once the data start is known so the steady query's rate per hour over the hours held equals its
           recent rate (15 in 24 hours): the count (the marker's 10 included) is the rate times the hours held. Over the 168 hours
           asked for, the old binding sees under half that rate and flags it. */
        var heldHours = (rig.Start - (rig.DataStart > rig.BaselineStart ? rig.DataStart : rig.BaselineStart)).TotalHours;
        var steadyBaseline = (long)Math.Round(15.0 / 24 * heldHours);
        Assert.InRange(steadyBaseline, 30, 200);
        await SnapshotAsync(SteadyQuery, now.AddHours(-25), now.AddHours(-25).AddMinutes(-20), steadyBaseline - 20);
        await SnapshotAsync(SteadyQuery, now.AddHours(-24.5), now.AddHours(-24.5).AddMinutes(-20), 10);
        await SnapshotAsync(RisingQuery, now.AddHours(-25), now.AddHours(-25).AddMinutes(-20), 10);
        if (floorInsideBaseline)
        {
            /* The edge interval: collected after the data start, last run before it. The requested baseline start still reaches it. */
            await SnapshotAsync(RisingQuery, rig.DataStart.AddMinutes(10), rig.DataStart.AddHours(-1), 10);
        }

        return rig;
    }

    [Fact]
    public async Task RawFloorInsideTheBaseline_McpRead_DividesByTheHoursHeld_AndStillCountsTheEdgeInterval()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(floorInsideBaseline: true, ct);

        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, rig.DataStart, ct);
        Assert.Equal("raw", served.Source);

        /* The steady query's rate per hour is the same on both sides over the hours held, so it is not a rise. */
        var rising = Assert.Single(served.Rows);
        Assert.Equal(RisingQuery, rising.QueryId);
        Assert.Equal("timeouts", rising.RegressionReason);

        /* 20 baseline runs: the 10 collected after the floor and the edge interval's 10 (collected after the floor, last run before it). */
        Assert.Equal(20, rising.Aborted!.BaselineCount);
        Assert.Equal(120, rising.Aborted.RecentCount);
        var held = (rig.Start - rig.DataStart).TotalHours;
        Assert.InRange(rising.Aborted.BaselinePerHour!.Value, (20 / held) - 0.001, (20 / held) + 0.001);
        Assert.InRange(rising.Aborted.RecentPerHour!.Value, (120 / 24.0) - 0.01, (120 / 24.0) + 0.01);

        /* The same statement bound to the REQUESTED start, which is what the read did before: the steady query is flagged as a rise. */
        var plan = await ResolveAsync(rig, ct);
        Assert.Equal(rig.DataStart, plan.HoursHeldStart);
        var before = await RunAsync(rig.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: false, useDailyTotals: false), rig, plan, rig.BaselineStart, ct);
        Assert.Contains(before, line => line.Contains("query_id=" + SteadyQuery + "|", StringComparison.Ordinal) && line.Contains("regression_reason=timeouts", StringComparison.Ordinal));
        var after = await RunAsync(rig.Connection, DarlingQueryStoreRegressionReader.BuildSql(wide: false, useDailyTotals: false), rig, plan, plan.HoursHeldStart!.Value, ct);
        Assert.DoesNotContain(after, line => line.Contains("query_id=" + SteadyQuery + "|", StringComparison.Ordinal));
        /* Only the divisors moved: the counts the two bindings read are the same. */
        Assert.Equal(
            before.Where(l => l.Contains("query_id=" + RisingQuery + "|", StringComparison.Ordinal)).Select(CountsOf),
            after.Where(l => l.Contains("query_id=" + RisingQuery + "|", StringComparison.Ordinal)).Select(CountsOf));
    }

    [Fact]
    public async Task RawFloorInsideTheBaseline_ViewerRead_DividesByTheHoursHeld_AndStillCountsTheEdgeInterval()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(floorInsideBaseline: true, ct);

        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var rows = await viewer.GetQueryStoreRegressionsAsync(ServerId, rig.Start, rig.End, [Db], cancellationToken: ct);
        var rising = Assert.Single(rows);
        Assert.Equal(RisingQuery, rising.QueryId);
        Assert.Equal(20, rising.BaselineAbortedCount);
        Assert.Equal(120, rising.RecentAbortedCount);
        var held = (rig.Start - rig.DataStart).TotalHours;
        Assert.InRange(rising.BaselineAbortedPerHour!.Value, (20 / held) - 0.001, (20 / held) + 0.001);
        Assert.InRange(rising.RecentAbortedPerHour!.Value, (120 / 24.0) - 0.01, (120 / 24.0) + 0.01);
    }

    [Fact]
    public async Task RawHoldingTheWholeBaseline_IsByteIdenticalToTheReadBeforeTheHoursHeldStart()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(floorInsideBaseline: false, ct);

        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, rig.DataStart, ct);
        Assert.Equal("raw", served.Source);
        Assert.NotEmpty(served.Rows);

        var plan = await ResolveAsync(rig, ct);
        /* The store holds the whole baseline, so the hours held start where the requested baseline does. */
        Assert.Equal(rig.BaselineStart, plan.HoursHeldStart);

        var current = DarlingQueryStoreRegressionReader.BuildSql(wide: false, useDailyTotals: false);
        var old = current.Replace("CAST($11 AS timestamp)", "CAST($6 AS timestamp)", StringComparison.Ordinal);
        Assert.NotEqual(current, old);
        var now = await RunAsync(rig.Connection, current, rig, plan, plan.HoursHeldStart!.Value, ct);
        var before = await RunAsync(rig.Connection, old, rig, plan, plan.HoursHeldStart!.Value, ct);
        Assert.NotEmpty(now);
        Assert.Equal(before, now);
        Assert.Contains(now, line => line.Contains("baseline_aborted_per_hour=", StringComparison.Ordinal) && !line.Contains("baseline_aborted_per_hour=NULL", StringComparison.Ordinal));
    }

    private const int OtherServerId = ServerId - 1;
    private const string OtherServerName = "qsheld-other";

    /// <summary>
    /// Two servers in one store, read at 24 hours. The server asked about (<see cref="ServerId"/>) holds 30 hours of data: a baseline
    /// of 6 hours before the window (12 timeouts of the rising query) and 120 in the window. The OTHER server's rows reach nine days
    /// back, before the 7 days the baseline asks for, so the store-wide raw chunk floor is at or before the baseline's start, and the
    /// gate's clamp is the baseline's start. Neither says when THIS server's data starts. <paramref name="tablePath"/> sends the read
    /// through the interval table (coverage forced back and a run log seeded for the server), false through raw.
    /// </summary>
    private static async Task<Rig> StartTwoServersAsync(bool tablePath, CancellationToken ct, bool baselineHoldsNoHours = false, bool regularRows = false)
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
        var rig = new Rig { Scratch = scratch, Connection = connection, Postgres = postgres, End = now.AddSeconds(1), Start = now.AddSeconds(1).AddHours(-24) };

        foreach (var (id, name) in new[] { (ServerId, ServerName), (OtherServerId, OtherServerName) })
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES (@id, @name, @name, TRUE, 16, now(), now())", connection);
            insert.Parameters.AddWithValue("id", id);
            insert.Parameters.AddWithValue("name", name);
            await insert.ExecuteNonQueryAsync(ct);
        }

        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        long intervalId = 9000;
        async Task SnapshotAsync(int serverId, string serverName, long queryId, DateTime collected, long count, DateTime? lastRunAt = null, string executionType = "Aborted", long avgCpuUs = 40_000)
        {
            intervalId++;
            var context = new CollectorContext { ServerId = serverId, ServerName = serverName + "-host", CollectionTime = DateTime.SpecifyKind(now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = serverName, Host = serverName + "-host" },
                ConnectionString = "Server=" + serverName + "-host",
                Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
                StorageName = serverName + "-host",
                ServerId = serverId,
                EngineEdition = 3,
            };
            var lastRun = lastRunAt ?? collected.AddMinutes(-20);
            var row = new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = queryId + 91,
                ExecutionTypeDesc = executionType,
                FirstExecutionTime = lastRun.AddMinutes(-30),
                LastExecutionTime = lastRun,
                QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + (queryId + 91).ToString("X8", CultureInfo.InvariantCulture),
                QueryText = "SELECT " + queryId.ToString(CultureInfo.InvariantCulture),
                ExecutionCount = count,
                AvgCpuTimeUs = avgCpuUs,
                AvgDurationUs = 80_000,
                AvgLogicalIoReads = 4_000,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                RuntimeStatsIntervalId = intervalId,
                IntervalStartTimeUtc = lastRun.AddMinutes(-30),
            };
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { row }, server, collected, context, ct);
        }

        /* The other server reaches nine days back, so the oldest chunk of the whole table is older than the baseline's start. */
        await SnapshotAsync(OtherServerId, OtherServerName, 77, now.AddDays(-9), 5);
        await SnapshotAsync(OtherServerId, OtherServerName, 77, now.AddDays(-3), 5);
        await SnapshotAsync(OtherServerId, OtherServerName, 77, now.AddHours(-3), 5);
        /* The server asked about: its first RAW row is 30 hours back. On the table path it also has a row six days back, which the table
           keeps and raw's dropped chunks do not: the gate needs the table to start well before raw's floor. */
        if (tablePath)
        {
            await SnapshotAsync(ServerId, ServerName, RisingQuery, now.AddDays(-6), 7);
            rig.OwnFailureStart = now.AddDays(-6).AddMinutes(-50) + QueryStoreIntervalWide.PurgeEdgeMargin;
            rig.OtherFailureStart = now.AddDays(-9).AddMinutes(-50) + QueryStoreIntervalWide.PurgeEdgeMargin;
        }

        if (baselineHoldsNoHours && regularRows)
        {
            /* The cost variant (#6750): the same two collections as below, as Regular intervals. The first, ten hours back, carries an
               interval last run 26 hours back at 1000 us average CPU, so the baseline has a CPU average over no hours. The second,
               inside the window, runs at 20,000 us (#6781: 120 runs then add at least 1 s of CPU, CostMinAddedCpuMs). */
            await SnapshotAsync(ServerId, ServerName, RisingQuery, now.AddHours(-10), 12, lastRunAt: now.AddHours(-26), executionType: "Regular", avgCpuUs: 1_000);
            await SnapshotAsync(ServerId, ServerName, RisingQuery, now.AddHours(-3), 120, executionType: "Regular", avgCpuUs: 20_000);
        }
        else if (baselineHoldsNoHours)
        {
            /* The reviewer's state (#6750): this server's FIRST collection is ten hours back, inside the 24 hour window, and carries an
               interval whose last run was 26 hours back, before the window start. The row filters (collected and last run at or after
               the requested baseline start) keep it, so the baseline counts 12 runs, while the server's data starts after the window
               start, so the baseline holds no hours. The other server's nine-day-old row anchors the store-wide chunk floor before the
               baseline start, whatever the time of day. */
            await SnapshotAsync(ServerId, ServerName, RisingQuery, now.AddHours(-10), 12, lastRunAt: now.AddHours(-26));
            await SnapshotAsync(ServerId, ServerName, RisingQuery, now.AddHours(-3), 120);
        }
        else
        {
            await SnapshotAsync(ServerId, ServerName, RisingQuery, now.AddHours(-30), 12);
            await SnapshotAsync(ServerId, ServerName, RisingQuery, now.AddHours(-3), 120);
        }

        if (tablePath)
        {
            await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, now.AddDays(-20), now.AddHours(1), null, null, ct);
            await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, now.AddDays(-16), ct);

            /* Raw's chunks older than three days go, so the gate's clamp is raw's floor (the other server's row three days back),
               inside the baseline and long before this server's first row. */
            await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(
                new QueryStoreIntervalWideBelowFloorLiveTests.Rig { Scratch = scratch, Connection = connection, Postgres = postgres }, now.AddDays(-3).AddMinutes(-1), ct);
        }
        else
        {
            await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, now.AddHours(-1), ct);
        }

        rig.RawFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection, QueryStoreIntervalWideBelowFloorLiveTests.ChunkFloorSql, ct))!;
        if (tablePath)
        {
            Assert.True(rig.RawFloor > rig.BaselineStart && rig.RawFloor < rig.Start.AddHours(-30), $"the table's clamp {rig.RawFloor:o} should sit inside the baseline, before this server's first raw row");
        }
        else
        {
            Assert.True(rig.RawFloor <= rig.BaselineStart, $"the other server's rows should put raw's floor {rig.RawFloor:o} at or before the baseline start {rig.BaselineStart:o}");
        }

        return rig;
    }

    /// <summary>
    /// The baseline's failure rate per hour over the hours from the answer's start to the baseline's end. On raw that start is this server's
    /// data start (<c>effective_start</c>, about 6 h). On the table path (#6750 part 2) the failure arm reads wide from its own start,
    /// <c>failure_baseline_start</c>: this server's oldest wide row plus 26 h. The other server's 9 day old row must not move it.
    /// </summary>
    private static void AssertDividesByTheHoursFromTheAnswersDataStart(JsonElement hit, string expectedSource, Rig rig)
    {
        Assert.Equal(expectedSource, hit.GetProperty("history_source").GetString());
        if (expectedSource == "interval_table")
        {
            AssertTheTablePathDivisor(hit, rig);
            return;
        }

        var effective = DateTime.Parse(hit.GetProperty("effective_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var baselineEnd = DateTime.Parse(hit.GetProperty("baseline_end").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var baselineStart = DateTime.Parse(hit.GetProperty("baseline_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.True(effective > baselineStart && effective < baselineEnd, "this server's data starts inside the baseline");

        var row = Assert.Single(hit.GetProperty("regressions").EnumerateArray());
        Assert.Equal("timeouts", row.GetProperty("regression_reason").GetString());
        var held = (baselineEnd - effective).TotalHours;
        Assert.InRange(held, 5.9, 6.1);
        Assert.InRange(row.GetProperty("baseline_aborted_per_hour").GetDouble(), (12 / held) - 0.001, (12 / held) + 0.001);
        Assert.InRange(row.GetProperty("recent_aborted_per_hour").GetDouble(), (120 / 24.0) - 0.01, (120 / 24.0) + 0.01);
    }

    private static void AssertTheTablePathDivisor(JsonElement hit, Rig rig)
    {
        var baselineEnd = DateTime.Parse(hit.GetProperty("baseline_end").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var failureStart = DateTime.Parse(hit.GetProperty("failure_baseline_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.InRange(Math.Abs((failureStart - rig.OwnFailureStart).TotalSeconds), 0, 2);
        Assert.True(Math.Abs((failureStart - rig.OtherFailureStart).TotalHours) > 24, "the other server's older row must not set this server's start");

        var row = Assert.Single(hit.GetProperty("regressions").EnumerateArray());
        Assert.Equal("timeouts", row.GetProperty("regression_reason").GetString());
        var held = (baselineEnd - failureStart).TotalHours;
        Assert.InRange(held, 94.5, 95.1);
        /* The payload rounds the rate to two decimals. */
        Assert.InRange(row.GetProperty("baseline_aborted_per_hour").GetDouble(), Math.Round(12 / held, 2) - 0.001, Math.Round(12 / held, 2) + 0.001);
        Assert.InRange(row.GetProperty("recent_aborted_per_hour").GetDouble(), (120 / 24.0) - 0.01, (120 / 24.0) + 0.01);
    }

    [Theory]
    [InlineData(false, "raw")]
    [InlineData(true, "interval_table")]
    public async Task AnotherServersOlderRows_DoNotMoveThisServersDivisor_McpRead(bool tablePath, string expectedSource)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartTwoServersAsync(tablePath, ct);

        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 24)).RootElement;
        AssertDividesByTheHoursFromTheAnswersDataStart(hit, expectedSource, rig);
    }

    /// <summary>
    /// A baseline that holds no hours counts as empty (#6750). The server's first collection lands inside the window and carries an
    /// interval last run before the window start, so the baseline counts 12 timeouts over no hours. Its rate is NULL, and the
    /// comparison with it is not made: the row is listed as against an empty baseline (before the hours-held divisor the negative
    /// hours gave a true comparison and listed it; a NULL comparison alone dropped it).
    /// </summary>
    [Fact]
    public async Task ABaselineThatHoldsNoHours_ButCountsRuns_ListsTheTimeoutsRowAsAgainstAnEmptyBaseline_McpRead()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartTwoServersAsync(tablePath: false, ct, baselineHoldsNoHours: true);

        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 24)).RootElement;
        Assert.Equal("raw", hit.GetProperty("history_source").GetString());
        var effective = DateTime.Parse(hit.GetProperty("effective_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var baselineEnd = DateTime.Parse(hit.GetProperty("baseline_end").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        Assert.True(effective > baselineEnd, "this server's data starts after the baseline's end, so the baseline holds no hours");
        Assert.EndsWith(QueryStoreRegressionRules.EmptyBaselineTail + " " + QueryStoreRegressionRules.RecentCutTail + " " + QueryStoreRegressionRules.RecentHoursHeldTail,
            hit.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);

        var row = Assert.Single(hit.GetProperty("regressions").EnumerateArray());
        Assert.Equal(RisingQuery, row.GetProperty("query_id").GetInt64());
        Assert.Equal("timeouts", row.GetProperty("regression_reason").GetString());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("baseline_aborted_per_hour").ValueKind);
        Assert.Equal(12, row.GetProperty("baseline_aborted_count").GetInt64());
        Assert.Equal(120, row.GetProperty("recent_aborted_count").GetInt64());
    }

    /// <summary>
    /// The cost side of the same state (#6750). Cost compares per-execution averages, which need no hours, so a Regular interval that
    /// ran before the window and was collected inside it (1000 us) is a baseline for the Regular interval inside the window (20,000
    /// us). The row is listed for cost beside a note that says what the zero-hour baseline can and cannot flag.
    /// </summary>
    [Fact]
    public async Task ABaselineThatHoldsNoHours_ButHoldsARegularInterval_ListsTheCostRow_McpRead()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartTwoServersAsync(tablePath: false, ct, baselineHoldsNoHours: true, regularRows: true);

        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 24)).RootElement;
        Assert.Equal("raw", hit.GetProperty("history_source").GetString());
        Assert.EndsWith(QueryStoreRegressionRules.EmptyBaselineTail + " " + QueryStoreRegressionRules.RecentCutTail + " " + QueryStoreRegressionRules.RecentHoursHeldTail,
            hit.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);

        var row = Assert.Single(hit.GetProperty("regressions").EnumerateArray());
        Assert.Equal(RisingQuery, row.GetProperty("query_id").GetInt64());
        Assert.Contains("cost", row.GetProperty("regression_reason").GetString()!, StringComparison.Ordinal);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("baseline_regular_per_hour").ValueKind);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnotherServersOlderRows_DoNotMoveThisServersDivisor_ViewerRead(bool tablePath)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartTwoServersAsync(tablePath, ct);

        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var read = await viewer.GetQueryStoreRegressionsWithScopeAsync(ServerId, rig.Start, rig.End, [Db], cancellationToken: ct);
        var rising = Assert.Single(read.Rows);
        Assert.Equal(12, rising.BaselineAbortedCount);
        double held;
        if (tablePath)
        {
            /* #6750 part 2: the failure arm starts at this server's oldest wide row plus 26 h, not at the other server's. */
            Assert.Equal("interval_table", read.Source);
            var failureStart = read.FailureBaselineStart!.Value;
            Assert.InRange(Math.Abs((failureStart - rig.OwnFailureStart).TotalSeconds), 0, 2);
            Assert.True(Math.Abs((failureStart - rig.OtherFailureStart).TotalHours) > 24, "the other server's older row must not set this server's start");
            held = (rig.Start - failureStart).TotalHours;
            Assert.InRange(held, 94.5, 95.1);
        }
        else
        {
            var data = await viewer.GetQueryStoreRegressionsDataStartAsync(ServerId, rig.Start, rig.End, ct);
            held = (rig.Start - data!.Value).TotalHours;
            Assert.InRange(held, 5.9, 6.1);
        }

        Assert.InRange(rising.BaselineAbortedPerHour!.Value, (12 / held) - 0.001, (12 / held) + 0.001);
        Assert.InRange(rising.RecentAbortedPerHour!.Value, (120 / 24.0) - 0.01, (120 / 24.0) + 0.01);
    }

    private static string CountsOf(string line) => string.Join("|", line.Split('|').Where(c =>
        c.StartsWith("query_id=", StringComparison.Ordinal) || (c.Contains("_count=", StringComparison.Ordinal) && !c.StartsWith("total_count=", StringComparison.Ordinal))));

    private static async Task<QueryStoreRegressionBaseline.Plan> ResolveAsync(Rig rig, CancellationToken ct)
    {
        await using var connection = await rig.Postgres.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        var plan = await QueryStoreRegressionBaseline.ResolveAsync(connection, transaction, ServerId, rig.BaselineStart, rig.Start, 60, null, ct, clampToRawFloor: true);
        return QueryStoreRegressionBaseline.WithHoursHeldStart(plan, rig.BaselineStart, rig.DataStart);
    }

    /// <summary>The statement run by hand, every column of every row in a text form. <paramref name="hoursHeldStart"/> is $11.</summary>
    private static async Task<List<string>> RunAsync(NpgsqlConnection connection, string sql, Rig rig, QueryStoreRegressionBaseline.Plan plan, DateTime hoursHeldStart, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.Start });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.End });
        command.Parameters.Add(DatabaseFilter.One(Db).Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 50 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.BaselineStart });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = plan.RunDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.LowEnd });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = plan.HighStart });
        command.Parameters.Add(DatabaseFilter.All.ExcludeParameter());
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = hoursHeldStart });
        /* #6750: the failure arm's row-filter start ($12) and its hours held ($13), the same as the Regular arms' on this raw read. */
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = rig.BaselineStart });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = hoursHeldStart });
        var lines = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i =>
                reader.GetName(i) + "=" + (reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)))));
        }

        return lines;
    }
}
