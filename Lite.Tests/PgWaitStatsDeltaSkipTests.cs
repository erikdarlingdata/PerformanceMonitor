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
/// Pins the row-count reduction that stops <c>pg_wait_stats</c> re-shipping an unchanged wait event
/// every cycle forever (#2694, the same defect #2691 fixed for <c>pg_statement_stats</c>): Aurora's
/// <c>wait_time</c> is cumulative since instance start with no reset function, so once any wait type has
/// fired even once, the collector's own <c>WHERE w.wait_time &gt; 0</c> keeps matching it on every
/// subsequent cycle whether or not it fired again. On a 1-minute cadence across a 50-cluster fleet this
/// made <c>pg_wait_stats</c> the 2nd-largest table in the pgmon store (1.25 GB) on nothing but idle
/// repeats.
///
/// <para>Uses the REAL <see cref="CollectorDeltaCalculator"/>, not the recording fake other collector
/// tests reach for — the skip decision depends on the calculator's own first-sight / counter-reset /
/// gap-reset semantics (all reported as delta 0 alongside interval 0) being told apart from a CONFIRMED
/// zero-activity interval (delta 0, interval &gt; 0), and a fake that always returns a fixed multiple of
/// the input cannot exercise that state machine.</para>
/// </summary>
public class PgWaitStatsDeltaSkipTests
{
    private const int ServerId = 7;

    private static readonly DateTime T0 = new(2026, 8, 30, 0, 0, 0, DateTimeKind.Utc);

    private static CollectorContext Context(DateTime collectionTime, ICollectorDeltaCalculator deltas) => new()
    {
        ServerId = ServerId,
        ServerName = "test-server",
        CollectionTime = collectionTime,
        Deltas = deltas,
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true },
        ExcludedDatabases = Array.Empty<string>(),
    };

    /// <summary>One row's worth of reader values in ordinal order (type_id, event_id, type_name, event_name, waits, wait_time_us).</summary>
    private static object[] Row(long eventId, long waits, long waitTimeMicroseconds, int typeId = 10, string typeName = "IO", string eventName = "DataFileRead") => new object[]
    {
        typeId, eventId, typeName, eventName, waits, waitTimeMicroseconds,
    };

    private static async Task<List<PgWaitStatsCollector.Row>> ReadAsync(
        object[] row, DateTime collectionTime, ICollectorDeltaCalculator deltas)
    {
        using var reader = new FakeCollectorDataReader(row);
        return await PgWaitStatsCollector.Instance.ReadAsync(reader, Context(collectionTime, deltas), CancellationToken.None);
    }

    [Fact]
    public async Task FirstSighting_ShipsEvenWithZeroWaits()
    {
        var deltas = new CollectorDeltaCalculator();

        var rows = await ReadAsync(Row(eventId: 1, waits: 0, waitTimeMicroseconds: 0), T0, deltas);

        Assert.Single(rows);
    }

    [Fact]
    public async Task RepeatWithNoNewWaits_IsSkipped()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(eventId: 2, waits: 100, waitTimeMicroseconds: 5000), T0, deltas);

        var repeat = await ReadAsync(Row(eventId: 2, waits: 100, waitTimeMicroseconds: 5000), T0.AddSeconds(60), deltas);

        Assert.Empty(repeat);
    }

    [Fact]
    public async Task RepeatWithNewWaits_ShipsWithTheMeasuredDeltas()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(eventId: 3, waits: 100, waitTimeMicroseconds: 5000), T0, deltas);

        var repeat = await ReadAsync(Row(eventId: 3, waits: 150, waitTimeMicroseconds: 8000), T0.AddSeconds(60), deltas);

        var row = Assert.Single(repeat);
        Assert.Equal(50, row.DeltaWaits);
        Assert.Equal(3000, row.DeltaWaitTime);
    }

    /// <summary>
    /// A counter DECREASE is <see cref="CollectorDeltaCalculator"/>'s reset branch, which reports
    /// (delta 0, interval 0) — the same shape as a first sighting, and for the same reason: the activity
    /// between the two readings is unknowable, not zero, so it must still ship rather than reading as a
    /// confirmed-idle interval. An instance restart is the real-world cause here.
    /// </summary>
    [Fact]
    public async Task CounterReset_Ships()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(eventId: 4, waits: 100, waitTimeMicroseconds: 5000), T0, deltas);

        var afterReset = await ReadAsync(Row(eventId: 4, waits: 40, waitTimeMicroseconds: 1200), T0.AddSeconds(60), deltas);

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
        await ReadAsync(Row(eventId: 5, waits: 100, waitTimeMicroseconds: 5000), T0, deltas);

        var afterGap = await ReadAsync(
            Row(eventId: 5, waits: 100, waitTimeMicroseconds: 5000),
            T0.AddSeconds(CollectorDeltaCalculator.DefaultMaxGapSeconds + 60), deltas);

        Assert.Single(afterGap);
    }

    /// <summary>
    /// The regression this design has to avoid: skipping a row must not ALSO skip refreshing the
    /// wait-time series' baseline, or a later real gap measured against a stale timestamp would falsely
    /// read as exceeding <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/> and zero a real
    /// accrual. Three cycles, none individually exceeding the gap cap against the timestamp it actually
    /// left behind — 1800s, then 3200s. Only a design that skipped the wait-time call on the idle middle
    /// cycle would measure cycle three's gap from cycle one instead (5000s, over the cap) and wrongly
    /// reset it.
    /// </summary>
    [Fact]
    public async Task IdleCycle_StillRefreshesTheWaitTimeBaseline_SoALaterRealGapIsNotFalselyReset()
    {
        var deltas = new CollectorDeltaCalculator();

        await ReadAsync(Row(eventId: 6, waits: 100, waitTimeMicroseconds: 5000), T0, deltas);

        var idle = await ReadAsync(Row(eventId: 6, waits: 100, waitTimeMicroseconds: 5000), T0.AddSeconds(1800), deltas);
        Assert.Empty(idle);

        var active = await ReadAsync(Row(eventId: 6, waits: 150, waitTimeMicroseconds: 8000), T0.AddSeconds(5000), deltas);

        var row = Assert.Single(active);
        Assert.Equal(50, row.DeltaWaits);
        Assert.Equal(3000, row.DeltaWaitTime);
    }

    /// <summary>
    /// Two wait events never share a baseline (the delta key is the numeric event id) — an active
    /// sibling must not mask a genuinely idle one, or vice versa, when both are read in the same pass.
    /// </summary>
    [Fact]
    public async Task IndependentWaitEvents_AreDecidedSeparatelyInTheSamePass()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(Row(eventId: 100, waits: 50, waitTimeMicroseconds: 2000), T0, deltas);
        await ReadAsync(Row(eventId: 101, waits: 50, waitTimeMicroseconds: 2000), T0, deltas);

        using var reader = new FakeCollectorDataReader(
            Row(eventId: 100, waits: 50, waitTimeMicroseconds: 2000),   // idle — should be dropped
            Row(eventId: 101, waits: 80, waitTimeMicroseconds: 3200));  // active — should ship
        var rows = await PgWaitStatsCollector.Instance.ReadAsync(reader, Context(T0.AddSeconds(60), deltas), CancellationToken.None);

        var shipped = Assert.Single(rows);
        Assert.Equal(101L, shipped.EventId);
    }
}
