/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6781: the cost test of get_query_store_regressions also needs the query to add at least
/// <see cref="QueryStoreRegressionRules.CostMinAddedCpuMs"/> ms of CPU over the recent window (recent runs times the rise in CPU per
/// run). Before this, any query whose CPU per run rose more than 25% was listed and counted in <c>regression_count</c>, however few
/// times it ran and however cheap it was, so on a production store about half of the thousands of rows were a few runs of a cheap
/// query. Seeded through the real write path and read through the MCP tool, the reader's raw and table statements, and the Viewer:
/// <list type="bullet">
/// <item>61: a cheap query that ran twice recently, 10 ms to 15 ms CPU a run (+50%): 10 ms added. Not listed.</item>
/// <item>62: once in the window, a job whose CPU went from 5 minutes to 50 minutes: 2,700 s added in one run. Listed for cost.</item>
/// <item>63: 5,000 recent runs, 1 ms to 3 ms CPU a run: 10 s added. Listed for cost, second by added duration, after 62.</item>
/// <item>64: a plan that went parallel: CPU per run 100 ms to 400 ms while the duration fell 500 ms to 300 ms, over 100 runs: 30 s of
/// CPU added and negative added duration. Listed for cost, which a floor on added duration would have dropped.</item>
/// <item>65: a cheap query with a timeout rise: its CPU rose 50% on 2 runs (cost fails the floor) and its timeouts rose 0 to 4.
/// Listed for timeouts only: the floor touches the cost test and nothing else.</item>
/// </list>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres (QueryStoreRegressionsFailureGroupsLiveTests.OpenAsync) and works entirely inside it. It
   shares the gap-cache collection with the other regressions table tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsAddedCpuFloorLiveTests
{
    private const int ServerId = QueryStoreRegressionsFailureGroupsLiveTests.ServerId;
    private const string Db = QueryStoreRegressionsFailureGroupsLiveTests.Db;

    private static string? BaseConnectionString => System.Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task<QueryStoreRegressionsFailureGroupsLiveTests.Rig> OpenAsync(CancellationToken ct) =>
        QueryStoreRegressionsFailureGroupsLiveTests.OpenAsync(async (runner, server, context, today, start) =>
        {
            long id = 9700;
            async Task W(string type, System.DateTime first, long q, long n, long cpuUs, long durUs) =>
                await QueryStoreRegressionsFailureGroupsLiveTests.WriteAsync(runner, server, context, type, ++id, first, q, n, cpuUs, durUs, ct);

            var b = start.AddDays(-4).AddHours(2);
            var r = start.AddHours(2);
            await W("Regular", start.AddDays(-12), 90, 5, 1_000, 1_000);   /* the table's floor, outside both arms */

            await W("Regular", b, 61, 2, 10_000, 20_000);
            await W("Regular", r, 61, 2, 15_000, 30_000);

            await W("Regular", b, 62, 1, 300_000_000, 310_000_000);
            await W("Regular", r, 62, 1, 3_000_000_000, 3_010_000_000);

            await W("Regular", b, 63, 5_000, 1_000, 2_000);
            await W("Regular", r, 63, 5_000, 3_000, 6_000);

            await W("Regular", b, 64, 100, 100_000, 500_000);
            await W("Regular", r, 64, 100, 400_000, 300_000);

            await W("Regular", b, 65, 2, 10_000, 20_000);
            await W("Regular", r, 65, 2, 15_000, 30_000);
            await W("Aborted", r, 65, 4, 10_000, 30_000_000);
        }, ct);

    private static void AssertTheFloor(IReadOnlyList<(long Id, string Reason)> rows, string label)
    {
        Assert.True(rows.Select(r => r.Id).ToArray() is [62, 63, 64, 65], label + ": " + string.Join(",", rows.Select(r => r.Id + ":" + r.Reason)));
        Assert.All(rows.Take(3), r => Assert.Equal("cost", r.Reason));
        Assert.Equal("timeouts", rows[3].Reason);
    }

    private static async Task<List<(long, string)>> RunAsync(QueryStoreRegressionsFailureGroupsLiveTests.Rig rig, bool wide, CancellationToken ct)
    {
        var plan = QueryStoreRegressionBaseline.AllLive(rig.Start);
        await using var command = new NpgsqlCommand(DarlingQueryStoreRegressionReader.BuildSql(wide, useDailyTotals: false), rig.Connection);
        DarlingQueryStoreRegressionReader.AddParameters(command, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, plan);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return (await DarlingQueryStoreRegressionReader.ReadRowsAsync(reader, ct)).Select(r => (r.QueryId, r.RegressionReason)).ToList();
    }

    [Fact]
    public async Task ACheapLowRunRise_IsNotACostRegression_AndARareExpensiveOneAndAParallelPlanAre_OnEveryPath()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6781 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        /* The served read, whichever tier its gate picks, and each tier's statement run by hand. */
        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, ct);
        AssertTheFloor(served.Rows.Select(r => (r.QueryId, r.RegressionReason)).ToList(), "served (" + served.Source + ")");
        AssertTheFloor(await RunAsync(rig, wide: false, ct), "raw");
        AssertTheFloor(await RunAsync(rig, wide: true, ct), "wide table");
        Assert.Equal(4L, served.Rows[0].TotalCount);
        /* The ranking is unchanged: cost rows by added duration (63 adds 10 s, 62 adds 2,700 s, 64 adds -20 s). */
        Assert.Equal(new long[] { 62, 63, 64 }, served.Rows.Take(3).Select(r => r.QueryId).ToArray());
        Assert.True(served.Rows.Single(r => r.QueryId == 64).AdditionalDurationMs < 0, "64's duration fell");

        /* The MCP tool: the rows, and regression_count counts only what passed. */
        var json = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, "qsfail", 48, DatabaseFilter.One(Db), 30, false, null, NullLogger.Instance, ct);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var rows = doc.RootElement.GetProperty("regressions").EnumerateArray()
            .Select(r => (r.GetProperty("query_id").GetInt64(), r.GetProperty("regression_reason").GetString()!)).ToList();
        AssertTheFloor(rows, "mcp");
        Assert.Equal(4, doc.RootElement.GetProperty("regression_count").GetInt32());

        /* The Viewer grid. */
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var grid = await viewer.GetQueryStoreRegressionsAsync(ServerId, rig.Start, rig.End, [Db], null, ct);
        AssertTheFloor(grid.Select(r => (r.QueryId, r.RegressionReason)).ToList(), "viewer");
    }
}
