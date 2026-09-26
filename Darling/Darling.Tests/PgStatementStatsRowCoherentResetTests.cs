/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4428: the same row-coherent restart decision <c>QueryStatsRowCoherentResetTests</c> pins for
/// <c>query_stats</c>, applied to <see cref="PgStatementStatsCollector"/> — with one difference the
/// PostgreSQL side forces: the series-age signal (<c>stats_since</c>, pg_stat_statements 1.11+) is a
/// TIMESTAMP on the monitored server's own clock, not an age relative to the collector host's clock, so
/// placement compares it ONLY against the previous pass's <c>target_now</c> — captured in the SAME read
/// — never against the collector's own <see cref="CollectorContext.CollectionTime"/>.
/// </summary>
public sealed class PgStatementStatsRowCoherentResetTests
{
    private const int ServerId = 1;
    private static DateTime T0 => new(2026, 9, 1, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>One row, ordinals matching PgStatementStatsCollector's BuildQuery, 0-29.</summary>
    private static object[] Row(
        long queryId, long dbid, long userid, bool toplevel, long calls, double totalExecTimeMs,
        long rowsReturned, DateTime? statsReset, DateTime? statsSince, DateTime targetNow)
        => new object[]
        {
            queryId, dbid, userid, toplevel, calls, totalExecTimeMs,
            0.0, 0.0, 0.0, rowsReturned,
            0L, 0L, 0L, 0L, 0L, 0L, 0.0, 0.0,
            (object?)null!, (object?)null!, (object?)null!, (object?)null!,
            0L, 0L, 0L,
            (object?)null!, (object?)null!,
            (object?)statsReset ?? DBNull.Value,
            (object?)statsSince ?? DBNull.Value,
            targetNow,
        };

    private static async Task<System.Collections.Generic.List<PgStatementStatsCollector.Row>> RunReadAsync(
        ICollectorDeltaCalculator deltas, DateTime collectionTime, object[] row)
    {
        var reader = new FakeReader(row);
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "target-a",
            CollectionTime = collectionTime,
            Deltas = deltas,
        };

        return await PgStatementStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
    }

    /// <summary>
    /// #4431's shape, adapted: calls re-grown past the old value, total time DROPPED (the counter that a
    /// per-family delta alone would call a reset), stats_since inside the gap on the TARGET clock. The
    /// whole row credits current values over the real interval.
    /// </summary>
    [Fact]
    public async Task Reset_StatsSinceInsideGap_CreditsCurrentValues()
    {
        var deltas = new CollectorDeltaCalculator();
        var t0Target = T0;
        var t1Target = T0.AddSeconds(60);

        await RunReadAsync(deltas, T0,
            Row(1, 1, 1, true, calls: 1, totalExecTimeMs: 57_695_259, rowsReturned: 1,
                statsReset: null, statsSince: null, targetNow: t0Target));

        /* Restart between t0Target and t1Target: total time FELL (57,695,259 -> 703,943), while calls and
           rows RE-GREW past their own old values (1 -> 16) — the #4431 field shape. A per-family delta
           alone would read this as "total time reset, calls +15"; row-coherent placement instead credits
           every counter as its current value over the real interval. stats_since sits inside the gap on
           the TARGET clock. */
        var restartedAt = t0Target.AddSeconds(30);
        var rows = await RunReadAsync(deltas, T0.AddSeconds(60),
            Row(1, 1, 1, true, calls: 16, totalExecTimeMs: 703_943, rowsReturned: 16,
                statsReset: null, statsSince: restartedAt, targetNow: t1Target));

        var restarted = Assert.Single(rows);
        Assert.Equal(16, restarted.DeltaCalls);
        Assert.Equal(703_943, restarted.DeltaTotalExecTimeMs);
        Assert.Equal(16, restarted.DeltaRows);
        Assert.Equal(60, restarted.SampleIntervalSeconds);
    }

    /// <summary>
    /// stats_since BEFORE the previous pass's target_now: the restart happened before we last looked, so
    /// it is not placeable inside the gap. A decreasing row reports (0, 0).
    /// </summary>
    [Fact]
    public async Task Reset_StatsSinceBeforePreviousTargetNow_ReportsZeroZero()
    {
        var deltas = new CollectorDeltaCalculator();
        var t0Target = T0;
        var t1Target = T0.AddSeconds(60);

        await RunReadAsync(deltas, T0,
            Row(1, 1, 1, true, calls: 100, totalExecTimeMs: 5000, rowsReturned: 100,
                statsReset: null, statsSince: null, targetNow: t0Target));

        /* stats_since is BEFORE t0Target — the entry existed before our previous look, so a decrease here
           cannot be a "credit the current values" restart. */
        var beforePreviousLook = t0Target.AddSeconds(-10);
        var rows = await RunReadAsync(deltas, T0.AddSeconds(60),
            Row(1, 1, 1, true, calls: 5, totalExecTimeMs: 900, rowsReturned: 5,
                statsReset: null, statsSince: beforePreviousLook, targetNow: t1Target));

        var reset = Assert.Single(rows);
        Assert.Equal(0, reset.DeltaCalls);
        Assert.Equal(0, reset.DeltaTotalExecTimeMs);
        Assert.Equal(0, reset.DeltaRows);
        Assert.Equal(0, reset.SampleIntervalSeconds);
    }

    /// <summary>stats_since absent (PostgreSQL 16, or Aurora without it): strict (0, 0) for a decreasing row.</summary>
    [Fact]
    public async Task Reset_StatsSinceAbsent_ReportsZeroZero()
    {
        var deltas = new CollectorDeltaCalculator();

        await RunReadAsync(deltas, T0,
            Row(1, 1, 1, true, calls: 100, totalExecTimeMs: 5000, rowsReturned: 100,
                statsReset: null, statsSince: null, targetNow: T0));

        var rows = await RunReadAsync(deltas, T0.AddSeconds(60),
            Row(1, 1, 1, true, calls: 5, totalExecTimeMs: 900, rowsReturned: 5,
                statsReset: null, statsSince: null, targetNow: T0.AddSeconds(60)));

        var reset = Assert.Single(rows);
        Assert.Equal(0, reset.DeltaCalls);
        Assert.Equal(0, reset.DeltaTotalExecTimeMs);
        Assert.Equal(0, reset.DeltaRows);
        Assert.Equal(0, reset.SampleIntervalSeconds);
    }

    /// <summary>
    /// Clock skew: the target's clock runs several minutes AHEAD of the collector host's clock. An entry
    /// created BEFORE the previous pass on the TARGET's clock must not be credited, even though its
    /// stats_since reads "after" the previous pass's context.CollectionTime on the collector's clock —
    /// proving placement never compares against the collector's own clock.
    /// </summary>
    [Fact]
    public async Task Reset_TargetClockSkew_NeverComparedAgainstCollectorClock()
    {
        var deltas = new CollectorDeltaCalculator();

        /* Target clock runs 5 minutes ahead of the collector host: collector sees T0, target's now() is
           T0 + 5 minutes. */
        var skew = TimeSpan.FromMinutes(5);
        var t0Target = T0 + skew;
        var t1Target = T0.AddSeconds(60) + skew;

        await RunReadAsync(deltas, T0,
            Row(1, 1, 1, true, calls: 100, totalExecTimeMs: 5000, rowsReturned: 100,
                statsReset: null, statsSince: null, targetNow: t0Target));

        /* The restart's stats_since is T0 + 90s on the COLLECTOR's clock — later than the collector's own
           T0 (context.CollectionTime for pass one) — but on the TARGET's clock it is BEFORE t0Target
           (T0 + 300s), i.e. before the previous pass. A calculator that compared stats_since against the
           collector's clock would wrongly credit this; comparing against target_now correctly refuses it. */
        var statsSinceOnCollectorClock = T0.AddSeconds(90);
        var rows = await RunReadAsync(deltas, T0.AddSeconds(60),
            Row(1, 1, 1, true, calls: 5, totalExecTimeMs: 900, rowsReturned: 5,
                statsReset: null, statsSince: statsSinceOnCollectorClock, targetNow: t1Target));

        var reset = Assert.Single(rows);
        Assert.Equal(0, reset.DeltaCalls);
        Assert.Equal(0, reset.DeltaTotalExecTimeMs);
        Assert.Equal(0, reset.DeltaRows);
        Assert.Equal(0, reset.SampleIntervalSeconds);
    }

    /// <summary>No reset — every counter only ever increases — takes the ordinary per-family path unchanged.</summary>
    [Fact]
    public async Task NoReset_OrdinaryIncrease_Unchanged()
    {
        var deltas = new CollectorDeltaCalculator();

        await RunReadAsync(deltas, T0,
            Row(1, 1, 1, true, calls: 100, totalExecTimeMs: 5000, rowsReturned: 100,
                statsReset: null, statsSince: null, targetNow: T0));

        var rows = await RunReadAsync(deltas, T0.AddSeconds(60),
            Row(1, 1, 1, true, calls: 120, totalExecTimeMs: 5200, rowsReturned: 110,
                statsReset: null, statsSince: null, targetNow: T0.AddSeconds(60)));

        var ordinary = Assert.Single(rows);
        Assert.Equal(20, ordinary.DeltaCalls);
        Assert.Equal(200, ordinary.DeltaTotalExecTimeMs);
        Assert.Equal(10, ordinary.DeltaRows);
        Assert.Equal(60, ordinary.SampleIntervalSeconds);
    }

    /// <summary>
    /// The mutation: with <see cref="ICollectorDeltaCalculator.DecideRow"/> forced to always report "no
    /// reset" (a stub calculator whose DecideRow override never sets AnyReset), the row-coherent pins
    /// above go RED — a decreasing row is read as an ordinary per-family delta instead of a restart.
    /// </summary>
    [Fact]
    public async Task Mutation_DecideRowAlwaysNoReset_GoesRed()
    {
        var deltas = new NeverResetsDeltaCalculator();

        await RunReadAsync(deltas, T0,
            Row(1, 1, 1, true, calls: 1, totalExecTimeMs: 57_695_259, rowsReturned: 1,
                statsReset: null, statsSince: null, targetNow: T0));

        var restartedAt = T0.AddSeconds(30);
        var rows = await RunReadAsync(deltas, T0.AddSeconds(60),
            Row(1, 1, 1, true, calls: 16, totalExecTimeMs: 703_943, rowsReturned: 16,
                statsReset: null, statsSince: restartedAt, targetNow: T0.AddSeconds(60)));

        var mutated = Assert.Single(rows);

        /* With the mutation, total_exec_time's own per-family call sees 703,943 < 57,695,259 and reports
           (0, 0) for THAT family alone, while calls and rows (both increased) report ordinary per-family
           deltas — the exact mixed reset-plus-inflated-increment #4428 exists to prevent. This assertion
           is the one meant to FAIL against the mutated calculator; it demonstrates the pin catches the
           mutation rather than the mutation being silently tolerated. */
        Assert.True(
            mutated.DeltaTotalExecTimeMs == 703_943 && mutated.DeltaCalls == 16,
            $"Mutation escaped detection: expected row-coherent credit (703943, 16) but got " +
            $"(DeltaTotalExecTimeMs={mutated.DeltaTotalExecTimeMs}, DeltaCalls={mutated.DeltaCalls}) — " +
            "the mutated calculator's family-by-family behaviour must differ from the row-coherent one " +
            "for this pin to be meaningful.");
    }

    /// <summary>
    /// A calculator whose <see cref="ICollectorDeltaCalculator.DecideRow"/> always reports no reset — the
    /// #4428 mutation — with every other member delegated to a real <see cref="CollectorDeltaCalculator"/>
    /// so the per-family behaviour under test is otherwise identical. Implements the interface directly
    /// (rather than subclassing) because <see cref="CollectorDeltaCalculator.DecideRow"/> is not virtual —
    /// a <c>new</c> hide in a subclass would not intercept a call made through the interface reference
    /// <see cref="CollectorContext.Deltas"/> actually holds, which is exactly the shape
    /// <see cref="PgStatementStatsCollector.ReadAsync"/> uses.
    /// </summary>
    private sealed class NeverResetsDeltaCalculator : ICollectorDeltaCalculator
    {
        private readonly CollectorDeltaCalculator _inner = new();

        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0)
            => _inner.CalculateDelta(serverId, collectorName, key, currentValue, collectionTime, maxGapSeconds);

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
            => _inner.CalculateDeltaWithInterval(serverId, collectorName, key, currentValue, out intervalSeconds, collectionTime, maxGapSeconds);

        public long CalculateDeltaWithSeriesAge(int serverId, string collectorName, string key, long currentValue,
            int? seriesAgeSeconds, out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
            => _inner.CalculateDeltaWithSeriesAge(serverId, collectorName, key, currentValue, seriesAgeSeconds, out intervalSeconds, collectionTime, maxGapSeconds);

        public RowResetDecision DecideRow(int serverId,
            System.Collections.Generic.IReadOnlyList<(string Family, long Current)> counters, string key,
            int? seriesAgeSeconds, DateTime? collectionTime, int maxGapSeconds)
            => default;

        public DateTime? PreviousPass(int serverId, string group, DateTime observedTime)
            => _inner.PreviousPass(serverId, group, observedTime);

        public void ClearServer(int serverId, string? discontinuity = null)
            => _inner.ClearServer(serverId, discontinuity);

        public void ClearGroups(int serverId, string? discontinuity, params string[] groups)
            => _inner.ClearGroups(serverId, discontinuity, groups);
    }

    /// <summary>A minimal multi-call <see cref="DbDataReader"/> over one boxed row, ordinal access only.</summary>
    private sealed class FakeReader : DbDataReader
    {
        private readonly object[] _values;
        private int _row = -1;

        public FakeReader(object[] values) => _values = values;

        private object Raw(int ordinal) => _values[ordinal];

        public override bool IsDBNull(int ordinal) => Raw(ordinal) is DBNull;
        public override DateTime GetDateTime(int ordinal) => (DateTime)Raw(ordinal);
        public override long GetInt64(int ordinal) => Convert.ToInt64(Raw(ordinal));
        public override int GetInt32(int ordinal) => Convert.ToInt32(Raw(ordinal));
        public override bool GetBoolean(int ordinal) => (bool)Raw(ordinal);
        public override double GetDouble(int ordinal) => Convert.ToDouble(Raw(ordinal));
        public override string GetString(int ordinal) => (string)Raw(ordinal);
        public override T GetFieldValue<T>(int ordinal) => (T)Raw(ordinal);
        public override object GetValue(int ordinal) => Raw(ordinal);
        public override int FieldCount => _values.Length;
        public override bool Read() => ++_row == 0;
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());
        public override bool HasRows => true;

        public override int Depth => 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override object this[int ordinal] => Raw(ordinal);
        public override object this[string name] => throw new NotSupportedException("ordinal access only");
        public override int GetOrdinal(string name) => throw new NotSupportedException("ordinal access only");
        public override string GetName(int ordinal) => throw new NotSupportedException("ordinal access only");
        public override bool NextResult() => false;
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();
        public override Type GetFieldType(int ordinal) => Raw(ordinal).GetType();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
    }
}
