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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7592: the severity of a cost row follows how much time it added, not its percent alone. A row that added under
/// <see cref="QueryStoreRegressionRules.GradeMinAddedDurationMs"/> over the window AND under
/// <see cref="QueryStoreRegressionRules.GradeMinAddedPerRunMs"/> per run reads LOW and stays listed. CPU and duration move together
/// in each row, so every one passes the cost gate. Seeded through the real write path and read through the served read, the MCP
/// tool and the Viewer:
/// <list type="bullet">
/// <item>71: 4.73 ms to 12.67 ms over 162 runs (+168%): 1.3 s added, 7.9 ms a run. LOW (it read CRITICAL).</item>
/// <item>72: 0.12 ms to 3,263 ms over 4 runs: 13 s added, 3.3 s a run. CRITICAL, on the per-run floor.</item>
/// <item>73: 1.5 ms to 1.95 ms (+30%) over 2,900 runs: 1.3 s added, 0.45 ms a run. LOW (it read MEDIUM).</item>
/// <item>74: 3 ms to 8 ms (+167%) over 13,000 runs: 65 s added. CRITICAL, on the total floor.</item>
/// </list>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres (QueryStoreRegressionsFailureGroupsLiveTests.OpenAsync) and works entirely inside it. It
   shares the gap-cache collection with the other regressions table tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreRegressionsGradeFloorLiveTests
{
    private const int ServerId = QueryStoreRegressionsFailureGroupsLiveTests.ServerId;
    private const string Db = QueryStoreRegressionsFailureGroupsLiveTests.Db;

    private static string? BaseConnectionString => System.Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task<QueryStoreRegressionsFailureGroupsLiveTests.Rig> OpenAsync(CancellationToken ct) =>
        QueryStoreRegressionsFailureGroupsLiveTests.OpenAsync(async (runner, server, context, today, start) =>
        {
            long id = 9800;
            async Task Pair(long q, long baseUs, long recentUs, long recentRuns)
            {
                await QueryStoreRegressionsFailureGroupsLiveTests.WriteAsync(runner, server, context, "Regular", ++id, start.AddDays(-4).AddHours(2), q, 100, baseUs, baseUs, ct);
                await QueryStoreRegressionsFailureGroupsLiveTests.WriteAsync(runner, server, context, "Regular", ++id, start.AddHours(2), q, recentRuns, recentUs, recentUs, ct);
            }

            await QueryStoreRegressionsFailureGroupsLiveTests.WriteAsync(runner, server, context, "Regular", ++id, start.AddDays(-12), 90, 5, 1_000, 1_000, ct);
            await Pair(71, 4_730, 12_670, 162);
            await Pair(72, 120, 3_263_000, 4);
            await Pair(73, 1_500, 1_950, 2_900);
            await Pair(74, 3_000, 8_000, 13_000);
        }, ct);

    private static void AssertTheGrade(IReadOnlyDictionary<long, string> severities, string label)
    {
        Assert.True(severities.Count == 4, label + ": " + string.Join(",", severities.Select(s => s.Key + ":" + s.Value)));
        Assert.Equal("LOW", severities[71]);
        Assert.Equal("CRITICAL", severities[72]);
        Assert.Equal("LOW", severities[73]);
        Assert.Equal("CRITICAL", severities[74]);
    }

    [Fact]
    public async Task ARowThatAddedLittleTime_ReadsLow_AndARowThatAddedAMinuteOrASecondARun_KeepsItsPercentBand()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #7592 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);

        var served = await DarlingQueryStoreRegressionReader.ReadQueryStoreRegressionsAsync(
            rig.Postgres, ServerId, rig.Start, rig.End, DatabaseFilter.One(Db), 50, rig.BaselineStart, ct);
        AssertTheGrade(served.Rows.ToDictionary(r => r.QueryId, r => r.Severity), "served (" + served.Source + ")");

        var json = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, "qsfail", 48, DatabaseFilter.One(Db), 30, false, null, NullLogger.Instance, ct);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        AssertTheGrade(doc.RootElement.GetProperty("regressions").EnumerateArray()
            .ToDictionary(r => r.GetProperty("query_id").GetInt64(), r => r.GetProperty("severity").GetString()!), "mcp");

        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var grid = await viewer.GetQueryStoreRegressionsAsync(ServerId, rig.Start, rig.End, [Db], null, ct);
        AssertTheGrade(grid.ToDictionary(r => r.QueryId, r => r.Severity), "viewer");
    }
}
