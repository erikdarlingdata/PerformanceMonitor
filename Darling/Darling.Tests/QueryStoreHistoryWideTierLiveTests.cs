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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5300: get_query_store_query_history follows the grid's tier choice, so a window wider than raw retention is read from
/// <c>query_store_interval_wide</c> and the history of a query the grid lists is never empty. Seeded through the real write
/// path on the below-floor rig (the same store shape as #4689's tests): a query's interval collected three times, with an
/// Aborted run beside the Regular ones, in a day whose raw chunk is then dropped.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres (inside QueryStoreIntervalWideBelowFloorLiveTests.StartAsync) and works
   entirely inside it, so it cannot race live collection. It shares that class's gap-cache collection, because the gate's
   cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreHistoryWideTierLiveTests
{
    private const string ServerName = "qsiw-below-floor";
    private const string Db = "qsH";
    private const long QueryId = 7000;

    private static async Task SeedHistoryQueryAsync(Npgsql.NpgsqlDataSource postgres, DateTime first, CancellationToken ct)
    {
        var serverId = QueryStoreIntervalWideBelowFloorLiveTests.ServerId;
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var context = new CollectorContext { ServerId = serverId, ServerName = "qsiw-grid-host", CollectionTime = DateTime.UtcNow, Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsiw-grid", Host = "qsiw-grid-host" },
            ConnectionString = "Server=qsiw-grid-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsiw-grid-host",
            ServerId = serverId,
            EngineEdition = 3,
        };

        QueryStoreCollector.Row Row(long planId, string outcome, long executions, long durationUs, DateTime last) => new()
        {
            DatabaseName = Db,
            QueryId = QueryId,
            PlanId = planId,
            ExecutionTypeDesc = outcome,
            FirstExecutionTime = first,
            LastExecutionTime = last,
            QueryHash = "0x00001B58",
            QueryPlanHash = "0x" + planId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = durationUs / 2,
            AvgDurationUs = durationUs,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            RuntimeStatsIntervalId = 7001,
            IntervalStartTimeUtc = first,
        };

        /* One open interval re-fetched at three cycles with a growing count: only the last snapshot may be counted (20, not 37). */
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { Row(71, "Regular", 5, 2_000, first.AddMinutes(9)) }, server, first.AddMinutes(10), context, ct);
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { Row(71, "Regular", 12, 2_000, first.AddMinutes(24)) }, server, first.AddMinutes(25), context, ct);
        await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row>
        {
            Row(71, "Regular", 20, 2_000, first.AddMinutes(39)),
            Row(71, "Aborted", 2, 30_000_000, first.AddMinutes(39)),
            Row(72, "Regular", 4, 1_000, first.AddMinutes(39)),
        }, server, first.AddMinutes(40), context, ct);
    }

    [Fact]
    public async Task AWindowPastRawRetention_IsReadFromTheIntervalTable_CountsAnIntervalOnce_AndFoldsOutcomesIntoOnePoint()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await QueryStoreIntervalWideBelowFloorLiveTests.StartAsync(timescale: true, ct);
        var s = QueryStoreIntervalWideBelowFloorLiveTests.S;
        var first = s.AddHours(6);
        await SeedHistoryQueryAsync(rig.Postgres, first, ct);
        await QueryStoreIntervalWideBelowFloorLiveTests.ForceFilledSinceAsync(rig.Connection, s.AddDays(-1), ct);

        /* The day the seed lives in leaves raw. Only the interval table still holds it. */
        await QueryStoreIntervalWideBelowFloorLiveTests.DropRawChunksOlderThanAsync(rig, s.AddDays(1), ct);
        var rawRows = (long)(await QueryStoreIntervalWideBelowFloorLiveTests.ScalarAsync(rig.Connection,
            $"SELECT COUNT(*) FROM collect.query_store_stats WHERE query_id = {QueryId}", ct))!;
        Assert.Equal(0, rawRows);
        var plan = await QueryStoreIntervalWideBelowFloorLiveTests.ResolveAsync(rig, ct);
        Assert.True(plan.UseTable);
        Assert.True(plan.ReadStart <= first);

        using var doc = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(
            rig.Postgres, Db, QueryId, ServerName, hours_back: 168, cancellationToken: ct));
        var root = doc.RootElement;
        var plans = root.GetProperty("plans");
        Assert.Equal(new long[] { 71, 72 }, Enumerable.Range(0, plans.GetArrayLength()).Select(n => plans[n].GetProperty("plan_id").GetInt64()));
        /* 20 Regular (the interval's last snapshot, not 5 + 12 + 20) plus 2 Aborted. */
        Assert.Equal(22, plans[0].GetProperty("execution_count").GetInt64());
        Assert.Equal((20 * 2.0 + 2 * 30_000.0) / 22, plans[0].GetProperty("avg_duration_ms").GetDouble(), 6);
        Assert.Equal(4, plans[1].GetProperty("execution_count").GetInt64());

        /* One point per plan at the instant, the Regular and Aborted rows of plan 71 combined. */
        var points = root.GetProperty("points");
        Assert.Equal(2, points.GetArrayLength());
        Assert.Equal(22, points[0].GetProperty("execution_count").GetInt64());
        Assert.Equal(first.AddMinutes(40).ToString("yyyy-MM-dd'T'HH:mm", System.Globalization.CultureInfo.InvariantCulture), points[0].GetProperty("collection_time").GetString()![..16]);

        /* An empty answer over the same window still says what the window reached. */
        using var empty = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(
            rig.Postgres, Db, 9999, ServerName, hours_back: 168, cancellationToken: ct));
        Assert.Equal("empty", empty.RootElement.GetProperty("status").GetString());
        Assert.False(string.IsNullOrEmpty(empty.RootElement.GetProperty("hints").GetProperty("effective_start").GetString()));
    }
}
