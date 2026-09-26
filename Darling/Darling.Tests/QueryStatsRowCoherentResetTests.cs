/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4428: when a cached plan's statistics restart under the SAME delta key, the counters do not all
/// shrink in the same instant a per-family <see cref="ICollectorDeltaCalculator.CalculateDeltaWithSeriesAge"/>
/// call observes them — one family can already have re-grown PAST its pre-restart value while a sibling
/// is still below its own, so a per-family decision reads the row as one family resetting (unknowable)
/// and another family incrementing by a giant, wrong amount. <see cref="CollectorDeltaCalculator.DecideRow"/>
/// (used from <see cref="QueryStatsCollector.WritePayload"/>) makes the reset decision once for the whole
/// row: if ANY family would reset, every counter's fate is the same, decided by the #2235 series-age test
/// (<see cref="DeltaSeriesAgeTests"/>) exactly as a single family's reset already decides for itself.
/// </summary>
public sealed class QueryStatsRowCoherentResetTests
{
    private const int ServerId = 1;
    private static DateTime T0 => new(2026, 9, 1, 12, 0, 0, DateTimeKind.Unspecified);
    private static DateTime T1 => T0.AddSeconds(60);

    private static (QueryStatsCollector.Row Row, RecordingCollectorRowWriter Writer, System.Collections.Generic.Dictionary<string, int> Columns)
        RunPass(CollectorDeltaCalculator deltas, string sqlHandle, string planHandle, int startOffset, int endOffset,
            long execCount, long workerTime, long elapsedTime, long reads, long writes, long physReads, long rows, long spills,
            int? compileAge, DateTime collectionTime)
    {
        var row = new QueryStatsCollector.Row
        {
            QueryHash = "queryhash",
            ExecutionCount = execCount,
            TotalWorkerTime = workerTime,
            TotalElapsedTime = elapsedTime,
            TotalLogicalReads = reads,
            TotalLogicalWrites = writes,
            TotalPhysicalReads = physReads,
            TotalRows = rows,
            TotalSpills = spills,
            SqlHandle = sqlHandle,
            PlanHandle = planHandle,
            StatementStartOffset = startOffset,
            StatementEndOffset = endOffset,
            CompileAgeSeconds = compileAge,
        };

        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "target-a",
            CollectionTime = collectionTime,
            Deltas = deltas,
        };

        var writer = new RecordingCollectorRowWriter();
        QueryStatsCollector.Instance.WritePayload(row, writer, context);

        var columnIndex = QueryStatsCollector.Instance.PayloadColumns
            .Select((c, i) => (c.Name, i))
            .ToDictionary(p => p.Name, p => p.i);

        return (row, writer, columnIndex);
    }

    /// <summary>
    /// Field fixture 1: prev exec 1 / CPU 57,695,259 ; now exec 16 / CPU 703,943 (dropped) with
    /// elapsed re-grown too, compile age inside the gap. The whole row is a restart credited in the gap:
    /// every counter becomes its CURRENT value over the real interval — exec 16, CPU 703,943, elapsed its
    /// current value, not "exec +15 / CPU NULL".
    /// </summary>
    [Fact]
    public void FieldFixture_ExecAndElapsedRegrewPastCpuDrop_WholeRowCreditsCurrentValues()
    {
        const string sqlHandle = "0x0102";
        const string planHandle = "0x0304";
        const int startOffset = 0;
        const int endOffset = -1;

        var deltas = new CollectorDeltaCalculator();
        var key = $"{sqlHandle}:{startOffset}:{endOffset}:{planHandle}";

        /* Pass one: the pre-restart baseline. */
        RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 1, workerTime: 57_695_259, elapsedTime: 100_000_000,
            reads: 1, writes: 1, physReads: 1, rows: 1, spills: 1,
            compileAge: null, collectionTime: T0);

        /* Pass two: the restart. CPU dropped (703,943 < 57,695,259) — a per-family call on the worker
           family alone would call this a reset. Exec (16 > 1) and elapsed (110,000,000 > 100,000,000)
           look like ordinary increases to their own per-family calls: "exec +15" and "elapsed +10,000,000"
           — both wrong, because the whole row restarted and 703,943/16/110,000,000 are each the CURRENT,
           post-restart totals, not increments over the stale pre-restart baseline. Compile age (20s) sits
           inside the 60s gap. */
        var (_, writer, columns) = RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 16, workerTime: 703_943, elapsedTime: 110_000_000,
            reads: 5, writes: 5, physReads: 5, rows: 5, spills: 5,
            compileAge: 20, collectionTime: T1);

        Assert.Equal(16L, writer.Values[columns["delta_execution_count"]]);
        Assert.Equal(703_943L, writer.Values[columns["delta_worker_time"]]);
        Assert.Equal(110_000_000L, writer.Values[columns["delta_elapsed_time"]]);
        Assert.Equal(5L, writer.Values[columns["delta_logical_reads"]]);
        Assert.Equal(60, writer.Values[columns["sample_interval_seconds"]]);
    }

    /// <summary>
    /// Field fixture 1, elapsed-time variant: same shape but the elapsed counter DROPPED too (both CPU
    /// and elapsed reset, exec re-grew). Still a row-coherent restart: every counter becomes its current
    /// value over the real interval.
    /// </summary>
    [Fact]
    public void FieldFixture_ElapsedAlsoDropped_WholeRowCreditsCurrentValues()
    {
        const string sqlHandle = "0x0102";
        const string planHandle = "0x0304";
        const int startOffset = 0;
        const int endOffset = -1;

        var deltas = new CollectorDeltaCalculator();

        RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 1, workerTime: 57_695_259, elapsedTime: 200_000_000,
            reads: 1, writes: 1, physReads: 1, rows: 1, spills: 1,
            compileAge: null, collectionTime: T0);

        var (_, writer, columns) = RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 16, workerTime: 703_943, elapsedTime: 50_000_000,
            reads: 5, writes: 5, physReads: 5, rows: 5, spills: 5,
            compileAge: 20, collectionTime: T1);

        Assert.Equal(16L, writer.Values[columns["delta_execution_count"]]);
        Assert.Equal(703_943L, writer.Values[columns["delta_worker_time"]]);
        Assert.Equal(50_000_000L, writer.Values[columns["delta_elapsed_time"]]);
        Assert.Equal(60, writer.Values[columns["sample_interval_seconds"]]);
    }

    /// <summary>
    /// Field fixture 2 (the second shape #4428 cites): exec 1,801 -> 98 (dropped) with CPU also dropping,
    /// compile age inside the gap. Both counters become their current values.
    /// </summary>
    [Fact]
    public void SecondFieldShape_ExecAndCpuBothDropped_WholeRowCreditsCurrentValues()
    {
        const string sqlHandle = "0x0506";
        const string planHandle = "0x0708";
        const int startOffset = 0;
        const int endOffset = -1;

        var deltas = new CollectorDeltaCalculator();

        RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 1_801, workerTime: 40_000_000, elapsedTime: 400_000_000,
            reads: 100, writes: 100, physReads: 100, rows: 100, spills: 100,
            compileAge: null, collectionTime: T0);

        var (_, writer, columns) = RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 98, workerTime: 900_000, elapsedTime: 9_000_000,
            reads: 10, writes: 10, physReads: 10, rows: 10, spills: 10,
            compileAge: 10, collectionTime: T1);

        Assert.Equal(98L, writer.Values[columns["delta_execution_count"]]);
        Assert.Equal(900_000L, writer.Values[columns["delta_worker_time"]]);
        Assert.Equal(9_000_000L, writer.Values[columns["delta_elapsed_time"]]);
        Assert.Equal(60, writer.Values[columns["sample_interval_seconds"]]);
    }

    /// <summary>No reset: an ordinary increasing row is unchanged — delta = current - previous.</summary>
    [Fact]
    public void NoReset_OrdinaryIncreasingRow_IsUnchanged()
    {
        const string sqlHandle = "0x0102";
        const string planHandle = "0x0304";
        const int startOffset = 0;
        const int endOffset = -1;

        var deltas = new CollectorDeltaCalculator();

        RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 10, workerTime: 1_000, elapsedTime: 2_000,
            reads: 5, writes: 5, physReads: 5, rows: 5, spills: 5,
            compileAge: null, collectionTime: T0);

        var (_, writer, columns) = RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 13, workerTime: 1_500, elapsedTime: 2_600,
            reads: 7, writes: 6, physReads: 6, rows: 8, spills: 6,
            compileAge: null, collectionTime: T1);

        Assert.Equal(3L, writer.Values[columns["delta_execution_count"]]);
        Assert.Equal(500L, writer.Values[columns["delta_worker_time"]]);
        Assert.Equal(600L, writer.Values[columns["delta_elapsed_time"]]);
        Assert.Equal(60, writer.Values[columns["sample_interval_seconds"]]);
    }

    /// <summary>
    /// Erik's case: a CPU counter unchanged over a real interval (a query that ran but burned no
    /// measurable CPU) keeps its measured 0 and the real interval — it must not be mistaken for a reset.
    /// </summary>
    [Fact]
    public void UnchangedCpuOverARealInterval_KeepsTheMeasuredZeroAndTheRealInterval()
    {
        const string sqlHandle = "0x0102";
        const string planHandle = "0x0304";
        const int startOffset = 0;
        const int endOffset = -1;

        var deltas = new CollectorDeltaCalculator();

        RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 10, workerTime: 5_000, elapsedTime: 2_000,
            reads: 5, writes: 5, physReads: 5, rows: 5, spills: 5,
            compileAge: null, collectionTime: T0);

        var (_, writer, columns) = RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 13, workerTime: 5_000, elapsedTime: 2_600,
            reads: 7, writes: 6, physReads: 6, rows: 8, spills: 6,
            compileAge: null, collectionTime: T1);

        Assert.Equal(3L, writer.Values[columns["delta_execution_count"]]);
        Assert.Equal(0L, writer.Values[columns["delta_worker_time"]]);
        Assert.Equal(60, writer.Values[columns["sample_interval_seconds"]]);
    }

    /// <summary>
    /// A restart the #2235 series-age test cannot place inside the gap (age older than the gap): the
    /// whole row is unknowable, (0, 0) across every counter, not the mixed reset-plus-inflated-increment
    /// a per-family decision would otherwise produce.
    /// </summary>
    [Fact]
    public void RestartOlderThanTheGap_WholeRowStaysUnknown()
    {
        const string sqlHandle = "0x0102";
        const string planHandle = "0x0304";
        const int startOffset = 0;
        const int endOffset = -1;

        var deltas = new CollectorDeltaCalculator();

        RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 1, workerTime: 57_695_259, elapsedTime: 100_000_000,
            reads: 1, writes: 1, physReads: 1, rows: 1, spills: 1,
            compileAge: null, collectionTime: T0);

        /* Compile age (7200s) is OLDER than the 60s gap since T0 — the series demonstrably did not begin
           inside the window we can vouch for, so crediting it would invent work rather than merely lose
           some. */
        var (_, writer, columns) = RunPass(deltas, sqlHandle, planHandle, startOffset, endOffset,
            execCount: 16, workerTime: 703_943, elapsedTime: 110_000_000,
            reads: 5, writes: 5, physReads: 5, rows: 5, spills: 5,
            compileAge: 7_200, collectionTime: T1);

        Assert.Equal(0L, writer.Values[columns["delta_execution_count"]]);
        Assert.Equal(0L, writer.Values[columns["delta_worker_time"]]);
        Assert.Equal(0, writer.Values[columns["sample_interval_seconds"]]);
    }

    private sealed class RecordingCollectorRowWriter : ICollectorRowWriter
    {
        public System.Collections.Generic.List<object?> Values { get; } = new();
        private ICollectorRowWriter Add(object? v) { Values.Add(v); return this; }
        public ICollectorRowWriter Value(string? value) => Add(value);
        public ICollectorRowWriter Value(long value) => Add(value);
        public ICollectorRowWriter Value(long? value) => Add(value);
        public ICollectorRowWriter Value(int value) => Add(value);
        public ICollectorRowWriter Value(int? value) => Add(value);
        public ICollectorRowWriter Value(short value) => Add(value);
        public ICollectorRowWriter Value(short? value) => Add(value);
        public ICollectorRowWriter Value(double value) => Add(value);
        public ICollectorRowWriter Value(double? value) => Add(value);
        public ICollectorRowWriter Value(decimal value) => Add(value);
        public ICollectorRowWriter Value(decimal? value) => Add(value);
        public ICollectorRowWriter Value(bool value) => Add(value);
        public ICollectorRowWriter Value(bool? value) => Add(value);
        public ICollectorRowWriter Value(System.DateTime value) => Add(value);
        public ICollectorRowWriter Value(System.DateTime? value) => Add(value);
        public ICollectorRowWriter NullValue() => Add(null);
    }
}
