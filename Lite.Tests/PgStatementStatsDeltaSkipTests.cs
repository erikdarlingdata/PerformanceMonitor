/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the row-count reduction that keeps <c>pg_statement_stats</c> sustainable at a 1-minute cadence
/// across a 50-cluster fleet: <c>pg_stat_statements</c> has no server-side "changed since I last looked"
/// filter, so without this a statement with zero new calls is re-written every cycle regardless — the
/// store reached 176.9 GB of a 180 GB total within 36 hours of the fleet's onboarding, growing 126 GB in
/// a single day.
///
/// <para>Uses the REAL <see cref="CollectorDeltaCalculator"/>, not the recording fake other collector
/// tests reach for — the skip decision depends on the calculator's own first-sight / counter-reset /
/// gap-reset semantics (all reported as delta 0 alongside interval 0) being told apart from a CONFIRMED
/// zero-activity interval (delta 0, interval &gt; 0), and a fake that always returns a fixed multiple of
/// the input cannot exercise that state machine.</para>
/// </summary>
public class PgStatementStatsDeltaSkipTests
{
    private const int ServerId = 7;

    private static readonly DateTime T0 = new(2026, 8, 30, 0, 0, 0, DateTimeKind.Utc);

    private static CollectorContext Context(DateTime collectionTime, ICollectorDeltaCalculator deltas) => new()
    {
        ServerId = ServerId,
        ServerName = "test-server",
        CollectionTime = collectionTime,
        Deltas = deltas,
        Target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            IsAurora = false,
            PostgresMajorVersion = 17,
            PostgresVersionNum = 170000,
        },
    };

    /// <summary>
    /// One row's worth of reader values in ordinal order. Only the fields a scenario cares about are
    /// parameterized; everything else reads exactly as an idle statement's row would (zero counters, NULL
    /// on the Aurora-only six).
    /// </summary>
    private static object[] Row(long queryId, long calls, double totalExecTimeMs, long rowsReturned = 0, long databaseId = 1, long userId = 1) => new object[]
    {
        queryId, databaseId, userId, true, calls, totalExecTimeMs,
        0d, 0d, 0d, rowsReturned,
        0L, 0L, 0L, 0L,
        0L, 0L, 0d, 0d,
        DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
        0L, 0L, 0L,
        DBNull.Value, DBNull.Value,
    };

    private static async Task<List<PgStatementStatsCollector.Row>> ReadAsync(
        object[] row, DateTime collectionTime, ICollectorDeltaCalculator deltas)
    {
        using var reader = new FakeCollectorDataReader(row);
        return await PgStatementStatsCollector.Instance.ReadAsync(reader, Context(collectionTime, deltas), CancellationToken.None);
    }

    [Fact]
    public async Task FirstSighting_ShipsEvenWithZeroCalls()
    {
        var deltas = new CollectorDeltaCalculator();

        var rows = await ReadAsync(Row(queryId: 1, calls: 0, totalExecTimeMs: 0), T0, deltas);

        Assert.Single(rows);
    }

    [Fact]
    public async Task RepeatWithNoNewCalls_IsSkipped()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(queryId: 2, calls: 100, totalExecTimeMs: 1000), T0, deltas);

        var repeat = await ReadAsync(Row(queryId: 2, calls: 100, totalExecTimeMs: 1000), T0.AddSeconds(60), deltas);

        Assert.Empty(repeat);
    }

    [Fact]
    public async Task RepeatWithNewCalls_ShipsWithTheMeasuredDeltas()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(queryId: 3, calls: 100, totalExecTimeMs: 1000, rowsReturned: 10), T0, deltas);

        var repeat = await ReadAsync(Row(queryId: 3, calls: 150, totalExecTimeMs: 1500, rowsReturned: 25), T0.AddSeconds(60), deltas);

        var row = Assert.Single(repeat);
        Assert.Equal(50, row.DeltaCalls);
        Assert.Equal(500, row.DeltaTotalExecTimeMs);
        Assert.Equal(15, row.DeltaRows);
    }

    /// <summary>
    /// A counter DECREASE is <see cref="CollectorDeltaCalculator"/>'s reset branch, which reports
    /// (delta 0, interval 0) — the same shape as a first sighting, and for the same reason: the work
    /// between the two readings is unknowable, not zero, so it must still ship rather than reading as a
    /// confirmed-idle interval.
    /// </summary>
    [Fact]
    public async Task CounterReset_Ships()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(queryId: 4, calls: 100, totalExecTimeMs: 1000), T0, deltas);

        var afterReset = await ReadAsync(Row(queryId: 4, calls: 40, totalExecTimeMs: 300), T0.AddSeconds(60), deltas);

        Assert.Single(afterReset);
    }

    /// <summary>
    /// A gap past <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/> is re-baselined rather than
    /// diffed — also (delta 0, interval 0) — so an unchanged value read after a restart still ships instead
    /// of reading as a confirmed-idle interval it never observed.
    /// </summary>
    [Fact]
    public async Task UnchangedValueAcrossAnExceededGap_Ships()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(queryId: 5, calls: 100, totalExecTimeMs: 1000), T0, deltas);

        var afterGap = await ReadAsync(
            Row(queryId: 5, calls: 100, totalExecTimeMs: 1000),
            T0.AddSeconds(CollectorDeltaCalculator.DefaultMaxGapSeconds + 60), deltas);

        Assert.Single(afterGap);
    }

    /// <summary>
    /// The regression this design has to avoid: skipping a row must not ALSO skip refreshing the other two
    /// series' baselines, or a later real gap measured against a stale timestamp would falsely read as
    /// exceeding <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/> and zero a real accrual.
    /// Three cycles, none individually exceeding the gap cap against the timestamp it actually left behind
    /// — 1800s, then 3200s. Only a design that skipped the total_exec_time call on the idle middle cycle
    /// would measure cycle three's gap from cycle one instead (5000s, over the cap) and wrongly reset it.
    /// </summary>
    [Fact]
    public async Task IdleCycle_StillRefreshesTheOtherSeriesBaselines_SoALaterRealGapIsNotFalselyReset()
    {
        var deltas = new CollectorDeltaCalculator();

        await ReadAsync(Row(queryId: 6, calls: 100, totalExecTimeMs: 1000), T0, deltas);

        var idle = await ReadAsync(Row(queryId: 6, calls: 100, totalExecTimeMs: 1000), T0.AddSeconds(1800), deltas);
        Assert.Empty(idle);

        var active = await ReadAsync(Row(queryId: 6, calls: 150, totalExecTimeMs: 1500), T0.AddSeconds(5000), deltas);

        var row = Assert.Single(active);
        Assert.Equal(50, row.DeltaCalls);
        Assert.Equal(500, row.DeltaTotalExecTimeMs);
    }

    /// <summary>
    /// Two statements never share a baseline (the delta key is queryid/dbid/userid/toplevel) — an active
    /// sibling must not mask a genuinely idle one, or vice versa, when both are read in the same pass.
    /// </summary>
    [Fact]
    public async Task IndependentStatements_AreDecidedSeparatelyInTheSamePass()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(queryId: 100, calls: 50, totalExecTimeMs: 500), T0, deltas);
        await ReadAsync(Row(queryId: 101, calls: 50, totalExecTimeMs: 500), T0, deltas);

        using var reader = new FakeCollectorDataReader(
            Row(queryId: 100, calls: 50, totalExecTimeMs: 500),   // idle — should be dropped
            Row(queryId: 101, calls: 80, totalExecTimeMs: 900));  // active — should ship
        var rows = await PgStatementStatsCollector.Instance.ReadAsync(reader, Context(T0.AddSeconds(60), deltas), CancellationToken.None);

        var shipped = Assert.Single(rows);
        Assert.Equal(101, shipped.QueryId);
    }
}
