/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7156: query_stats keeps every statement that ran in the last ten minutes, not the 200 heaviest by lifetime elapsed
/// time, so the read has to drop the statements that did nothing since the last run (otherwise every run would store
/// hundreds of zero-delta rows) and rank the rest by the work done in the interval, so a plan that has just recompiled
/// or regressed is not behind plans that were heavy last week. Runs ReadAsync and WritePayload through the real
/// <see cref="CollectorDeltaCalculator"/> the way a host does.
/// </summary>
public sealed class QueryStatsRecentCandidatesTests
{
    private static readonly DateTime s_t0 = new(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc);

    private sealed record Stmt(string PlanHandle, long Executions, long WorkerUs, long ElapsedUs);

    private static CollectorContext Context(ICollectorDeltaCalculator deltas, DateTime time) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = time,
        Deltas = deltas,
        Target = new CollectorTargetInfo(),
    };

    private static object[] DmvRow(Stmt s)
    {
        var row = new object[44];
        row[0] = "SO"; row[1] = "0xQH" + s.PlanHandle; row[2] = "0xQPH";
        row[3] = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        row[4] = new DateTime(2026, 7, 2, 11, 59, 0, DateTimeKind.Utc);
        for (var i = 5; i < 36; i++)
        {
            row[i] = 0L;
        }

        row[5] = s.Executions;
        row[6] = s.WorkerUs;
        row[7] = s.ElapsedUs;
        row[36] = "0xSH" + s.PlanHandle; row[37] = s.PlanHandle; row[38] = "SELECT 1";
        row[39] = 1L; row[40] = 0; row[41] = -1;
        row[42] = "dbo.P";
        row[43] = 100_000; /* compiled long ago: a key new to us is not credited its whole lifetime */
        return row;
    }

    /// <summary>One collection: ReadAsync, then WritePayload for what it kept, as the host does. Returns the kept rows.</summary>
    private static async Task<List<(QueryStatsCollector.Row Row, List<object?> Values)>> CycleAsync(
        ICollectorDeltaCalculator deltas, DateTime time, params Stmt[] statements)
    {
        var context = Context(deltas, time);
        using var reader = new FakeCollectorDataReader(statements.Select(DmvRow).ToArray());
        var rows = await QueryStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var written = new List<(QueryStatsCollector.Row, List<object?>)>();
        foreach (var row in rows)
        {
            var writer = new RecordingCollectorRowWriter();
            QueryStatsCollector.Instance.WritePayload(row, writer, context);
            written.Add((row, writer.Values));
        }

        return written;
    }

    [Fact]
    public async Task ReadAsync_DropsStatementsWhoseCountersDidNotMove_AndKeepsMovedAndNewOnes()
    {
        var deltas = new CollectorDeltaCalculator();
        var idle = new Stmt("0xA", 10, 1_000, 2_000);
        await CycleAsync(deltas, s_t0, idle, new Stmt("0xB", 5, 500, 900));

        var second = await CycleAsync(
            deltas, s_t0.AddSeconds(60),
            idle,                                  /* nothing moved */
            new Stmt("0xB", 7, 900, 1_700),        /* ran again */
            new Stmt("0xC", 1, 50, 80));           /* first sighting */

        Assert.Equal(new[] { "0xB", "0xC" }, second.Select(r => r.Row.PlanHandle).OrderBy(h => h, StringComparer.Ordinal));
    }

    [Fact]
    public async Task ReadAsync_RanksByWorkDoneInTheInterval_NotByLifetimeElapsedTime()
    {
        var deltas = new CollectorDeltaCalculator();
        var heavyAllWeek = new Stmt("0xA", 1_000_000, 9_000_000_000, 90_000_000_000);
        var regressing = new Stmt("0xB", 10, 1_000, 2_000);
        await CycleAsync(deltas, s_t0, heavyAllWeek, regressing);

        var second = await CycleAsync(
            deltas, s_t0.AddSeconds(60),
            heavyAllWeek with { Executions = 1_000_001, ElapsedUs = 90_000_000_100 },   /* 100 us of new work */
            regressing with { Executions = 40, ElapsedUs = 50_000_000 });               /* 50 s of new work, lifetime still tiny */

        Assert.Equal(new[] { "0xB", "0xA" }, second.Select(r => r.Row.PlanHandle));
    }

    [Fact]
    public async Task ReadAsync_KeepsAnUnchangedStatement_AfterAGapPastThePolicy_WithTheUnknowableIntervalPair()
    {
        var deltas = new CollectorDeltaCalculator();
        var idle = new Stmt("0xA", 10, 1_000, 2_000);
        await CycleAsync(deltas, s_t0, idle);

        var afterGap = await CycleAsync(deltas, s_t0.AddSeconds(CollectorDeltaCalculator.DefaultMaxGapSeconds + 1), idle);

        var (row, values) = Assert.Single(afterGap);
        Assert.NotNull(row.Precomputed);
        Assert.Equal(0L, values[40]);   /* delta_execution_count */
        Assert.Equal(0, values[49]);    /* sample_interval_seconds: 0 is "no delta knowable", not "idle" */
    }
}
