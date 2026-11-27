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
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9533 through Lite's own calculator: the read watermark and the baseline advance live in the shared core, and Lite's
/// <see cref="DeltaCalculator"/> inherits them. A restart gives Lite's calculator no watermark, because it is never seeded.
/// </summary>
public sealed class QueryStatsReadWatermarkLiteTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
    private static readonly string[] Groups = { "query_stats_exec" };

    [Fact]
    public void ARestartedCalculator_HasNoWatermark_AndOneArmedByARead_IsTakenOnceWithItsGap()
    {
        ICollectorDeltaCalculator deltas = new DeltaCalculator(NullLogger.Instance);
        Assert.Null(deltas.TakeReadWatermark(1, "query_stats_exec", T0));

        deltas.ArmReadWatermark(1, "query_stats_exec", T0.AddSeconds(-2), T0);
        var taken = deltas.TakeReadWatermark(1, "query_stats_exec", T0.AddSeconds(60));
        Assert.Equal(T0.AddSeconds(-2), taken!.Value.ChangedSince);
        Assert.Equal(90, taken.Value.MaxAgeSeconds);
        Assert.Null(deltas.TakeReadWatermark(1, "query_stats_exec", T0.AddSeconds(120)));
    }

    [Fact]
    public void AKeyTheReadDidNotReturn_KeepsItsValue_AndItsIntervalRunsFromTheAdvancedTimestamp()
    {
        ICollectorDeltaCalculator deltas = new DeltaCalculator(NullLogger.Instance);
        const string key = "0x01:0:-1:0x02";
        deltas.CalculateDeltaWithInterval(1, "query_stats_exec", key, 50, out _, T0, 3600);
        deltas.AdvanceUnreturnedBaselines(1, "query_stats_exec", Groups, new Dictionary<string, DateTime> { [key] = T0 }, T0.AddMinutes(-10), false, T0);

        deltas.AdvanceUnreturnedBaselines(1, "query_stats_exec", Groups, new Dictionary<string, DateTime>(), T0.AddSeconds(60).AddMinutes(-10), true, T0.AddSeconds(60));

        var delta = deltas.CalculateDeltaWithInterval(1, "query_stats_exec", key, 80, out var interval, T0.AddSeconds(120), 3600);
        Assert.Equal(30, delta);
        Assert.Equal(60, interval);
    }

    private static object[] Statement(int handle, long executions, DateTime at)
    {
        var row = new object[45];
        row[0] = "SO"; row[1] = "0xQH" + handle; row[2] = "0xQPH";
        row[3] = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        row[4] = at;
        for (var i = 5; i < 36; i++)
        {
            row[i] = 0L;
        }

        row[5] = executions;
        row[36] = "0xSH" + handle; row[37] = "0xPH" + handle; row[38] = "SELECT 1";
        row[39] = 1L; row[40] = 0; row[41] = -1;
        row[42] = "dbo.P";
        row[43] = 100_000; /* compiled long ago: a key new to us is not credited its whole lifetime */
        row[44] = at;      /* server_now, in the same frame as last_execution_time */
        return row;
    }

    /// <summary>One run as a host makes it: the query is built (which takes the watermark), then the rows are read.</summary>
    private static async Task<List<QueryStatsCollector.Row>> RunAsync(
        ICollectorDeltaCalculator deltas, DateTime at, params (int Handle, long Executions)[] statements)
    {
        var context = new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = at,
            Deltas = deltas,
            Target = new CollectorTargetInfo(),
        };
        QueryStatsCollector.Instance.BuildQuery(context);
        using var reader = new FakeCollectorDataReader(statements.Select(s => Statement(s.Handle, s.Executions, at)).ToArray());
        return await QueryStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
    }

    /// <summary>
    /// #9533: a narrow read that returned no rows still proves key 2 idle. Run two returns nothing, so key 2's baseline moves
    /// to it, and its interval in run three is the 60 s since run two, not the 120 s since run one.
    /// </summary>
    [Fact]
    public async Task ARunThatReturnedNoRows_StillAdvancesTheKeysItDidNotReturn_OnLitesCalculator()
    {
        ICollectorDeltaCalculator deltas = new DeltaCalculator(NullLogger.Instance);
        await RunAsync(deltas, T0, (1, 100), (2, 50));
        await RunAsync(deltas, T0.AddSeconds(60));

        var third = await RunAsync(deltas, T0.AddSeconds(120), (2, 80));
        var key2 = third.Single().Precomputed!;
        Assert.Equal(30, key2.Exec);
        Assert.Equal(60, key2.ExecInterval);
    }
}
