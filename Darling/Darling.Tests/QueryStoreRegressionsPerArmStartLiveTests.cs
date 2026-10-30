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
/// #6750 part 2: on the table path each arm of the regressions read starts where its own table proves complete history. The store
/// here is the large production store's shape at hours_back 168: raw query_store_stats keeps less than the recent window (its floor
/// is after the window start), the wide table keeps about 2 days before the window, and query_store_interval_latest keeps more than
/// the whole baseline. Before the fix every arm bound max(baseline start, raw's floor), so the cost baseline was empty and a query
/// whose CPU went from 1 ms to 5 ms was never flagged. Seeded through the real write path, with the daily days built.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Each test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres. It shares the gap-cache collection with the other regressions table tests, because the gate's
   cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsPerArmStartLiveTests
{
    private const int ServerId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
    private const string Db = "qsArm";
    private const string ServerName = "qsarm";
    private const long CostQuery = 61;
    private const long TimeoutQuery = 62;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public DateTime Now { get; init; }
        public DateTime RawFloor { get; set; }
        public DateTime WideFloor { get; set; }
        public DateTime LatestFloor { get; set; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// 16 days of <see cref="CostQuery"/>: one Regular interval every 6 hours at 1 ms CPU up to 7 days before now, then at
    /// <paramref name="recentCpuUs"/> through the recent 7 days. One more interval sits 2 days before the baseline start's midnight, so
    /// latest's floor is more than a day before the baseline. <see cref="TimeoutQuery"/> has 10 Aborted runs 12 hours before the window
    /// and 50 every 12 hours inside it. Each day is its own collection, so raw's chunks age with the rows. Then raw's chunks older than 5 days are
    /// dropped (raw's floor is after the window start), and the wide table is purged below 2 days before the window start.
    /// <paramref name="latestFloorAfterBaseline"/> also deletes latest's rows below 3 days after the baseline start, so latest's own
    /// floor sets the cost start.
    /// </summary>
    private static async Task<Rig> StartAsync(CancellationToken ct, long recentCpuUs = 5_000, bool latestFloorAfterBaseline = false, bool timeouts = true)
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
        var rig = new Rig { Scratch = scratch, Connection = connection, Postgres = postgres, Now = now };

        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection, $@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (@server_id, '{ServerName}', '{ServerName}', TRUE, 16, now(), now())
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE;", null, ct);

        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName + "-host", CollectionTime = DateTime.SpecifyKind(now, DateTimeKind.Utc), Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = ServerName + "-host" },
            ConnectionString = "Server=" + ServerName + "-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = ServerName + "-host",
            ServerId = ServerId,
            EngineEdition = 3,
        };

        long intervalId = 67_500;
        QueryStoreCollector.Row Row(long queryId, DateTime lastRun, long count, long cpuUs, string outcome = "Regular")
        {
            intervalId++;
            return new QueryStoreCollector.Row
            {
                DatabaseName = Db,
                QueryId = queryId,
                PlanId = queryId + 91,
                ExecutionTypeDesc = outcome,
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

        var windowStart = now.AddHours(-168);
        var baselineMidnight = windowStart.AddDays(-7).Date;
        var rows = new List<QueryStoreCollector.Row> { Row(CostQuery, baselineMidnight.AddDays(-2).AddHours(1), 10, 1_000) };
        for (var t = now.AddDays(-16).AddHours(3); t < now.AddHours(-1); t = t.AddHours(6))
        {
            rows.Add(Row(CostQuery, t, 10, t < windowStart ? 1_000 : recentCpuUs));
        }

        rows.Add(Row(TimeoutQuery, windowStart.AddHours(-12), 10, 30_000_000, "Aborted"));
        for (var t = windowStart.AddHours(12); timeouts && t < now.AddHours(-1); t = t.AddHours(12))
        {
            rows.Add(Row(TimeoutQuery, t, 50, 30_000_000, "Aborted"));
        }

        /* One collection per day: each batch is collected ten minutes after its last interval. */
        foreach (var day in rows.GroupBy(r => r.LastExecutionTime!.Value.Date).OrderBy(g => g.Key))
        {
            var collected = day.Max(r => r.LastExecutionTime!.Value).AddMinutes(10);
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, day.ToList(), server, DateTime.SpecifyKind(collected, DateTimeKind.Unspecified), context, ct);
        }

        /* A healthy collection log and coverage claims older than every seeded row, as on a store that has filled for weeks. */
        await QueryStoreIntervalWideBelowFloorLiveTests.SeedQueryStoreLogAsync(connection, ServerId, now.AddDays(-20), now.AddMinutes(-30), null, null, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(connection, now.AddDays(-20), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection,
            "UPDATE collect.query_store_interval_latest_coverage SET filled_since = @cutoff WHERE server_id = @server_id", now.AddDays(-20), ct);
        await PlanRegressionDaily.RunTickAsync(postgres, now, NullLogger.Instance, ct);

        /* Raw keeps less than the recent window, and the wide table keeps about 2 days before it. */
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(
            new QueryStoreIntervalWideBelowFloorLiveTests.Rig { Scratch = scratch, Connection = connection, Postgres = postgres }, now.AddDays(-5), ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.PurgeTableAsync(connection, windowStart.AddDays(-2), ct);
        if (latestFloorAfterBaseline)
        {
            await QueryStoreIntervalWideBelowFloorLiveTests.ExecAsync(connection,
                "DELETE FROM collect.query_store_interval_latest WHERE server_id = @server_id AND first_execution_time < @cutoff", baselineMidnight.AddDays(3), ct);
        }

        /* Every floor is read back from the store, not derived from a chunk boundary. */
        rig.RawFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection, QueryStoreIntervalWideBelowFloorLiveTests.ChunkFloorSql, ct))!;
        rig.WideFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection, QueryStoreIntervalWide.PlainTableFloorSql.Replace("$1", "@server_id", StringComparison.Ordinal), ct))!;
        rig.LatestFloor = (DateTime)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(connection, QueryStoreIntervalLatest.PlainTableFloorSql.Replace("$1", "@server_id", StringComparison.Ordinal), ct))!;
        Assert.True(rig.RawFloor > windowStart.AddHours(2), $"raw's floor {rig.RawFloor:o} should be after the window start {windowStart:o}");
        Assert.True(rig.WideFloor >= windowStart.AddDays(-2) && rig.WideFloor < windowStart.AddDays(-1), $"the wide floor {rig.WideFloor:o} should be about 2 days before the window start {windowStart:o}");
        return rig;
    }

    private static DateTime Instant(JsonElement payload, string key) =>
        DateTime.Parse(payload.GetProperty(key).GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

    private static string Z(DateTime instant) => McpHelpers.FormatEffectiveStart(instant);

    /// <summary>The core case: at 168 hours the query whose CPU went from 1 ms to 5 ms is flagged for cost, and the payload names
    /// each arm's start and the table that served it. Before the fix there was no cost row at all.</summary>
    [Fact]
    public async Task At168Hours_CostComparesAgainstLatestsFullBaseline_AndTimeoutsAgainstTheWideTablesShorterOne()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);

        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168, limit: 30)).RootElement;
        Assert.Equal("interval_table", hit.GetProperty("history_source").GetString());
        var baselineStart = Instant(hit, "baseline_start");
        var baselineEnd = Instant(hit, "baseline_end");

        /* The cost baseline starts at the requested start's midnight (a baseline of 7 to 8 days), the failure baseline at the wide
           table's floor plus the purge-edge margin. */
        var costStart = baselineStart.Date;
        var failureStart = rig.WideFloor + QueryStoreIntervalWide.PurgeEdgeMargin;
        Assert.Equal(Z(costStart), hit.GetProperty("cost_baseline_start").GetString());
        Assert.Equal(Z(failureStart), hit.GetProperty("failure_baseline_start").GetString());
        Assert.Equal(Z(failureStart), hit.GetProperty("effective_start").GetString());
        Assert.True(hit.GetProperty("window_truncated").GetBoolean());
        var note = hit.GetProperty("truncation_note").GetString()!;
        Assert.Contains(Z(costStart), note, StringComparison.Ordinal);
        Assert.Contains(Z(failureStart), note, StringComparison.Ordinal);
        Assert.Contains("query_store_interval_latest", note, StringComparison.Ordinal);
        Assert.Contains("query_store_interval_wide", note, StringComparison.Ordinal);

        var regressions = hit.GetProperty("regressions").EnumerateArray().ToList();
        var cost = Assert.Single(regressions, r => r.GetProperty("query_id").GetInt64() == CostQuery);
        Assert.Equal("cost", cost.GetProperty("regression_reason").GetString());
        Assert.InRange(cost.GetProperty("baseline_cpu_ms").GetDouble(), 0.99, 1.01);
        Assert.InRange(cost.GetProperty("recent_cpu_ms").GetDouble(), 4.99, 5.01);
        /* The recent window is whole again: 4 intervals a day for 7 days, not only the days after raw's floor. */
        Assert.InRange(cost.GetProperty("recent_exec_count").GetInt64(), 260, 290);

        /* The failure rates divide by the hours from the failure arm's own start, not by 168 hours. */
        var timeouts = Assert.Single(regressions, r => r.GetProperty("query_id").GetInt64() == TimeoutQuery);
        Assert.Equal("timeouts", timeouts.GetProperty("regression_reason").GetString());
        Assert.Equal(10, timeouts.GetProperty("baseline_aborted_count").GetInt64());
        var failureHours = (baselineEnd - failureStart).TotalHours;
        Assert.InRange(timeouts.GetProperty("baseline_aborted_per_hour").GetDouble(), Math.Round(10 / failureHours, 2) - 0.01, Math.Round(10 / failureHours, 2) + 0.01);
    }

    /// <summary>Nothing regressed, on a store whose raw tier holds no row before the window. The latest table holds the baseline, so
    /// the answer is the all-clear, not "no baseline" (raw's coverage probe answered that before the fix).</summary>
    [Fact]
    public async Task At168Hours_NothingRegressed_IsTheAllClear_NotNoBaseline()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct, recentCpuUs: 1_000, timeouts: false);

        var clear = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168)).RootElement;
        Assert.Equal("empty", clear.GetProperty("status").GetString());
        Assert.DoesNotContain("no baseline", clear.GetProperty("message").GetString()!, StringComparison.OrdinalIgnoreCase);
        /* The hints carry the table read's notice: the instant from which every arm is complete. */
        Assert.Equal(Z(rig.WideFloor + QueryStoreIntervalWide.PurgeEdgeMargin), clear.GetProperty("hints").GetProperty("effective_start").GetString());
    }

    /// <summary>Latest's own floor sets the cost start when it is later than the baseline start: the floor clause
    /// <c>LatestCoversRegularArms</c> gained, so no Regular row older than latest still holds is assumed.</summary>
    [Fact]
    public async Task At168Hours_ALatestFloorInsideTheBaseline_StartsTheCostBaselineAMarginAfterIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct, latestFloorAfterBaseline: true);

        var hit = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, ServerName, 168)).RootElement;
        Assert.Equal("interval_table", hit.GetProperty("history_source").GetString());
        var costStart = rig.LatestFloor + QueryStoreIntervalWide.PurgeEdgeMargin;
        Assert.True(costStart > Instant(hit, "baseline_start"));
        Assert.Equal(Z(costStart), hit.GetProperty("cost_baseline_start").GetString());
        Assert.Contains(Z(costStart), hit.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);
        var cost = Assert.Single(hit.GetProperty("regressions").EnumerateArray(), r => r.GetProperty("query_id").GetInt64() == CostQuery);
        Assert.Equal("cost", cost.GetProperty("regression_reason").GetString());
    }

    /// <summary>The Viewer's grid binds the same per-arm starts through the shared SQL, and carries the cost start for its banner.</summary>
    [Fact]
    public async Task At168Hours_TheViewerRead_FlagsCost_AndCarriesTheCostStart()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync(ct);

        var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var start = end.AddHours(-168);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var read = await viewer.GetQueryStoreRegressionsWithScopeAsync(ServerId, start, end, [Db], cancellationToken: ct);
        Assert.Equal("interval_table", read.Source);
        Assert.Equal(start.AddDays(-7).Date, read.CostBaselineStart);
        Assert.Equal(rig.WideFloor + QueryStoreIntervalWide.PurgeEdgeMargin, read.FailureBaselineStart);
        var cost = Assert.Single(read.Rows, r => r.QueryId == CostQuery);
        Assert.Equal("cost", cost.RegressionReason);
        Assert.InRange(cost.RecentCpuMs, 4.99, 5.01);
    }
}
