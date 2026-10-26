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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
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

        /* The baseline rows, seeded once the floor is known so the steady query's rate per hour over the hours held equals its
           recent rate (15 in 24 hours): the count (the marker's 10 included) is the rate times the hours held. Over the 168 hours
           asked for, the old binding sees under half that rate and flags it. */
        var heldHours = (rig.Start - (rig.RawFloor > rig.BaselineStart ? rig.RawFloor : rig.BaselineStart)).TotalHours;
        var steadyBaseline = (long)Math.Round(15.0 / 24 * heldHours);
        Assert.InRange(steadyBaseline, 30, 200);
        await SnapshotAsync(SteadyQuery, now.AddHours(-25), now.AddHours(-25).AddMinutes(-20), steadyBaseline - 20);
        await SnapshotAsync(SteadyQuery, now.AddHours(-24.5), now.AddHours(-24.5).AddMinutes(-20), 10);
        await SnapshotAsync(RisingQuery, now.AddHours(-25), now.AddHours(-25).AddMinutes(-20), 10);
        if (floorInsideBaseline)
        {
            /* The edge interval: collected after the floor, last run before it. The requested baseline start still reaches it. */
            await SnapshotAsync(RisingQuery, rig.RawFloor.AddMinutes(10), rig.RawFloor.AddHours(-1), 10);
        }

        return rig;
    }

    [Fact]
    public async Task RawFloorInsideTheBaseline_McpRead_DividesByTheHoursHeld_AndStillCountsTheEdgeInterval()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(floorInsideBaseline: true, ct);

        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, ct);
        Assert.Equal("raw", served.Source);

        /* The steady query's rate per hour is the same on both sides over the hours held, so it is not a rise. */
        var rising = Assert.Single(served.Rows);
        Assert.Equal(RisingQuery, rising.QueryId);
        Assert.Equal("timeouts", rising.RegressionReason);

        /* 20 baseline runs: the 10 collected after the floor and the edge interval's 10 (collected after the floor, last run before it). */
        Assert.Equal(20, rising.Aborted!.BaselineCount);
        Assert.Equal(120, rising.Aborted.RecentCount);
        var held = (rig.Start - rig.RawFloor).TotalHours;
        Assert.InRange(rising.Aborted.BaselinePerHour!.Value, (20 / held) - 0.001, (20 / held) + 0.001);
        Assert.InRange(rising.Aborted.RecentPerHour!.Value, (120 / 24.0) - 0.01, (120 / 24.0) + 0.01);

        /* The same statement bound to the REQUESTED start, which is what the read did before: the steady query is flagged as a rise. */
        var plan = await ResolveAsync(rig, ct);
        Assert.Equal(rig.RawFloor, plan.HoursHeldStart);
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
        var held = (rig.Start - rig.RawFloor).TotalHours;
        Assert.InRange(rising.BaselineAbortedPerHour!.Value, (20 / held) - 0.001, (20 / held) + 0.001);
        Assert.InRange(rising.RecentAbortedPerHour!.Value, (120 / 24.0) - 0.01, (120 / 24.0) + 0.01);
    }

    [Fact]
    public async Task RawHoldingTheWholeBaseline_IsByteIdenticalToTheReadBeforeTheHoursHeldStart()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(floorInsideBaseline: false, ct);

        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, ct);
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

    private static string CountsOf(string line) => string.Join("|", line.Split('|').Where(c =>
        c.StartsWith("query_id=", StringComparison.Ordinal) || (c.Contains("_count=", StringComparison.Ordinal) && !c.StartsWith("total_count=", StringComparison.Ordinal))));

    private static async Task<QueryStoreRegressionBaseline.Plan> ResolveAsync(Rig rig, CancellationToken ct)
    {
        await using var connection = await rig.Postgres.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        return await QueryStoreRegressionBaseline.ResolveAsync(connection, transaction, ServerId, rig.BaselineStart, rig.Start, 60, null, ct, clampToRawFloor: true);
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
