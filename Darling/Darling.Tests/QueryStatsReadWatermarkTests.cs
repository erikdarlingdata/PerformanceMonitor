/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9533: query_stats reads only the statements that completed since its last successful read. These drive the real build
/// and read twice or more on one calculator, the way a host does, and look at the parameters the next statement carries and
/// at the intervals the next stored rows report.
/// </summary>
public sealed class QueryStatsReadWatermarkTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);

    private sealed record Run(CollectorQuery Query, List<QueryStatsCollector.Row> Rows);

    private static CollectorContext Context(CollectorDeltaCalculator deltas, DateTime at, bool azureSqlDb = false) => new()
    {
        ServerId = 7,
        ServerName = "test-server",
        CollectionTime = at,
        Deltas = deltas,
        Target = new CollectorTargetInfo { IsAzureSqlDb = azureSqlDb },
        DeferTextFetch = true,
    };

    /// <summary>
    /// One run as a host makes it: the query is built (which takes the watermark), then the rows are read. The server's clock
    /// is the collection time, so a statement's <c>last_execution_time</c> and the clock share a frame. A null
    /// <paramref name="withServerClock"/> is a read that carries no clock column.
    /// </summary>
    private static Task<Run> RunAsync(
        CollectorDeltaCalculator deltas, DateTime at, bool withServerClock, params (int Handle, long Work)[] statements) =>
        RunOnAsync(deltas, at, false, withServerClock, statements);

    private static async Task<Run> RunOnAsync(
        CollectorDeltaCalculator deltas, DateTime at, bool azureSqlDb, bool withServerClock, params (int Handle, long Work)[] statements)
    {
        var context = Context(deltas, at, azureSqlDb);
        var query = QueryStatsCollector.Instance.BuildQuery(context);
        using var table = QueryStatsAbandonedRunBaselineTests.Table(at, statements);
        if (withServerClock)
        {
            table.Columns.Add("server_now", typeof(DateTime));
            foreach (DataRow row in table.Rows)
            {
                row[44] = at;
            }
        }

        await using var reader = table.CreateDataReader();
        var rows = await QueryStatsCollector.Instance.ReadAsync(reader, context, TestContext.Current.CancellationToken);
        return new Run(query, rows);
    }

    private static object? Parameter(CollectorQuery query, string name) =>
        query.Parameters.Single(p => p.Name == name).Value;

    /// <summary>The next statement, built and then abandoned the way a run that fails before its read is.</summary>
    private static CollectorQuery BuildOnly(CollectorDeltaCalculator deltas, DateTime at) =>
        QueryStatsCollector.Instance.BuildQuery(Context(deltas, at));

    [Fact]
    public async Task FirstRun_ReadsTheWholeWindow_AndTheRunAfterItReadsFromTheServersClock()
    {
        var deltas = new CollectorDeltaCalculator();
        var first = await RunAsync(deltas, T0, true, (1, 100));
        Assert.Null(Parameter(first.Query, "@changed_since"));
        Assert.Null(Parameter(first.Query, "@max_age_seconds"));

        var second = BuildOnly(deltas, T0.AddSeconds(60));
        Assert.Equal(T0.AddSeconds(-QueryStatsCollector.WatermarkMarginSeconds), Parameter(second, "@changed_since"));
        Assert.Equal(60 + CollectorDeltaCalculator.ReadWatermarkGapMarginSeconds, Parameter(second, "@max_age_seconds"));
    }

    [Fact]
    public async Task AFailedRun_LeavesTheNextRunOnTheWholeWindow()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, true, (1, 100));

        /* The second run builds its statement, so it takes the watermark, and never reads: its query failed. */
        BuildOnly(deltas, T0.AddSeconds(60));

        var third = BuildOnly(deltas, T0.AddSeconds(120));
        Assert.Null(Parameter(third, "@changed_since"));
        Assert.Null(Parameter(third, "@max_age_seconds"));
    }

    [Fact]
    public async Task ARunWhoseBaselinesRolledBack_LeavesTheNextRunOnTheWholeWindow()
    {
        var deltas = new CollectorDeltaCalculator();
        var first = await RunAsync(deltas, T0, true, (1, 100));
        ((IBaselineRollback)first.Rows).RollBackBaselines();

        Assert.Null(Parameter(BuildOnly(deltas, T0.AddSeconds(60)), "@changed_since"));
    }

    [Fact]
    public async Task ARollbackOfAnEarlierRun_DoesNotClearTheWatermarkALaterRunArmed()
    {
        var deltas = new CollectorDeltaCalculator();
        var first = await RunAsync(deltas, T0, true, (1, 100));
        await RunAsync(deltas, T0.AddSeconds(60), true, (1, 110));
        ((IBaselineRollback)first.Rows).RollBackBaselines();

        Assert.Equal(T0.AddSeconds(58), Parameter(BuildOnly(deltas, T0.AddSeconds(120)), "@changed_since"));
    }

    [Fact]
    public async Task AGapOverTenMinutes_FallsBack_AndTenMinutesExactlyDoesNot()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, true, (1, 100));
        var atTheLimit = BuildOnly(deltas, T0.AddSeconds(CollectorDeltaCalculator.MaxReadWatermarkGapSeconds));
        Assert.Equal(CollectorDeltaCalculator.MaxReadWatermarkGapSeconds, Parameter(atTheLimit, "@max_age_seconds"));

        var other = new CollectorDeltaCalculator();
        await RunAsync(other, T0, true, (1, 100));
        Assert.Null(Parameter(BuildOnly(other, T0.AddSeconds(CollectorDeltaCalculator.MaxReadWatermarkGapSeconds + 1)), "@changed_since"));
    }

    [Fact]
    public async Task ARunThatReturnedNoRows_KeepsTheWatermarkItWasGiven_MeasuredFromTheRunThatSetIt()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, true, (1, 100));
        await RunAsync(deltas, T0.AddSeconds(60), true);

        var third = BuildOnly(deltas, T0.AddSeconds(120));
        Assert.Equal(T0.AddSeconds(-QueryStatsCollector.WatermarkMarginSeconds), Parameter(third, "@changed_since"));
        Assert.Equal(120 + CollectorDeltaCalculator.ReadWatermarkGapMarginSeconds, Parameter(third, "@max_age_seconds"));
    }

    [Fact]
    public async Task AReadWithNoServerClock_ArmsNothing()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, false, (1, 100));

        Assert.Null(Parameter(BuildOnly(deltas, T0.AddSeconds(60)), "@changed_since"));
    }

    [Fact]
    public async Task ClearingTheServer_ForgetsTheWatermark()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, true, (1, 100));
        deltas.ClearServer(7);

        Assert.Null(Parameter(BuildOnly(deltas, T0.AddSeconds(60)), "@changed_since"));
    }

    /// <summary>
    /// The point of the advance. Key 2 is baselined by run one and then does nothing, so runs two and three do not return it.
    /// When it moves in run four, its interval is the one since run three, as it was when it came back idle every run.
    /// </summary>
    [Fact]
    public async Task AKeyTheWatermarkReadDidNotReturn_KeepsItsValues_AndItsIntervalIsMeasuredFromThePreviousRead()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, true, (1, 100), (2, 50));
        await RunAsync(deltas, T0.AddSeconds(60), true, (1, 130));
        await RunAsync(deltas, T0.AddSeconds(120), true, (1, 160));

        var fourth = await RunAsync(deltas, T0.AddSeconds(180), true, (2, 80));
        var key2 = fourth.Rows.Single().Precomputed!;
        Assert.Equal(30, key2.Exec);
        Assert.Equal(60, key2.ExecInterval);
    }

    /// <summary>
    /// A narrow read that returns no rows has no server clock, but it still proves key 2 idle. Run two returns nothing, so
    /// key 2's baseline moves to it anyway, and when it runs in run three its interval is the 60 s since run two, as an idle row
    /// coming back every run would have left it, not the 120 s since run one.
    /// </summary>
    [Fact]
    public async Task ARunThatReturnedNoRows_StillAdvancesTheKeysItDidNotReturn()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, true, (1, 100), (2, 50));
        await RunAsync(deltas, T0.AddSeconds(60), true);

        var third = await RunAsync(deltas, T0.AddSeconds(120), true, (2, 80));
        var key2 = third.Rows.Single().Precomputed!;
        Assert.Equal(30, key2.Exec);
        Assert.Equal(60, key2.ExecInterval);
    }

    /// <summary>
    /// An Azure SQL Database target takes no watermark and arms none: every statement sends NULL for both parameters, and
    /// no key's baseline is advanced, so key 2's interval is measured from its own last baseline.
    /// </summary>
    [Fact]
    public async Task AnAzureSqlDatabaseTarget_TakesNoWatermark_ArmsNone_AndAdvancesNothing()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunOnAsync(deltas, T0, true, true, (1, 100), (2, 50));
        var second = await RunOnAsync(deltas, T0.AddSeconds(60), true, true, (1, 130));
        var third = await RunOnAsync(deltas, T0.AddSeconds(120), true, true, (2, 80));

        foreach (var run in new[] { second, third })
        {
            Assert.Null(Parameter(run.Query, "@changed_since"));
            Assert.Null(Parameter(run.Query, "@max_age_seconds"));
        }

        Assert.Equal(120, third.Rows.Single().Precomputed!.ExecInterval);
    }

    /// <summary>Without a watermark nothing is advanced, so the same key's interval is measured from its own last baseline.</summary>
    [Fact]
    public async Task ARunWithoutAWatermark_AdvancesNoBaselines()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, true, (1, 100), (2, 50));

        /* Run two fails before its read, so run three has no watermark and does not return key 2. */
        BuildOnly(deltas, T0.AddSeconds(60));
        await RunAsync(deltas, T0.AddSeconds(120), true, (1, 160));

        var fourth = await RunAsync(deltas, T0.AddSeconds(180), true, (2, 80));
        Assert.Equal(180, fourth.Rows.Single().Precomputed!.ExecInterval);
    }

    /// <summary>
    /// A read that came back nearly full may have been cut by the candidate cap, so a key it did not return may have changed.
    /// Its baseline stays where it was, and its interval is measured from there.
    /// </summary>
    [Fact]
    public async Task AReadNearTheCandidateCap_AdvancesNothing()
    {
        var deltas = new CollectorDeltaCalculator();
        var all = Enumerable.Range(0, QueryStatsCollector.CapBoundRowThreshold + 1).Select(i => (i, 100L)).ToArray();
        await RunAsync(deltas, T0, true, all);

        var full = all.Skip(1).Select(s => (s.i, 110L)).ToArray();
        await RunAsync(deltas, T0.AddSeconds(60), true, full);

        var third = await RunAsync(deltas, T0.AddSeconds(120), true, (0, 130));
        Assert.Equal(120, third.Rows.Single().Precomputed!.ExecInterval);
    }

    /// <summary>A key whose last execution is older than ten minutes is out of the window an idle read had, so its baseline is left alone.</summary>
    [Fact]
    public async Task AKeyOlderThanTheWindow_IsNotAdvanced()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, true, (1, 100), (2, 50));
        await RunAsync(deltas, T0.AddSeconds(300), true, (1, 130));
        await RunAsync(deltas, T0.AddSeconds(590), true, (1, 160));
        await RunAsync(deltas, T0.AddSeconds(640), true, (1, 190));

        /* Key 2 last ran at T0, so by 640 s it is past the ten-minute window and was no longer advanced. Its baseline is the 590 s one. */
        var last = await RunAsync(deltas, T0.AddSeconds(700), true, (2, 80));
        Assert.Equal(110, last.Rows.Single().Precomputed!.ExecInterval);
    }
}
