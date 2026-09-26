/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4394: a <c>query_stats</c> row whose worker (CPU) counter's own delta call returned interval 0 — a
/// first sighting, a plan reset, or a gap past the policy — while the exec-count or elapsed-time
/// counters (same row, same pass) returned a real, knowable delta used to write a false CPU of 0 over
/// interval 0, and the interval-honest filter (<c>sample_interval_seconds IS DISTINCT FROM 0</c>) then
/// discarded that row's real executions and duration along with the fabricated CPU. #2234 established
/// interval 0 as the pairing for "no delta knowable"; <see cref="QueryStatsCollector.ResolveWorkerDelta"/>
/// makes that hold per counter instead of per row.
/// </summary>
public sealed class QueryStatsUnknownCpuTests
{
    /// <summary>Worker (0, 0) — unknowable — with a real exec delta: CPU becomes NULL, interval comes from exec.</summary>
    [Fact]
    public void WorkerUnknown_ExecKnown_WritesNullCpuAndTheExecInterval()
    {
        var (workerDelta, intervalSeconds) = QueryStatsCollector.ResolveWorkerDelta(
            workerDelta: 0, workerIntervalSeconds: 0, execIntervalSeconds: 60, elapsedIntervalSeconds: 0);

        Assert.Null(workerDelta);
        Assert.Equal(60, intervalSeconds);
    }

    /// <summary>Worker AND exec unknown, but elapsed is real: CPU becomes NULL, interval falls back to elapsed.</summary>
    [Fact]
    public void WorkerUnknown_ExecUnknown_ElapsedKnown_WritesNullCpuAndTheElapsedInterval()
    {
        var (workerDelta, intervalSeconds) = QueryStatsCollector.ResolveWorkerDelta(
            workerDelta: 0, workerIntervalSeconds: 0, execIntervalSeconds: 0, elapsedIntervalSeconds: 60);

        Assert.Null(workerDelta);
        Assert.Equal(60, intervalSeconds);
    }

    /// <summary>
    /// THE STAKEHOLDER'S CASE: a worker delta of 0 over a REAL interval (a query that ran but burned no
    /// measurable CPU, e.g. blocked the whole window) is a MEASURED zero, not an unknowable one — it
    /// must not become NULL just because it happens to equal zero.
    /// </summary>
    [Fact]
    public void WorkerZeroOverARealInterval_KeepsTheMeasuredZero()
    {
        var (workerDelta, intervalSeconds) = QueryStatsCollector.ResolveWorkerDelta(
            workerDelta: 0, workerIntervalSeconds: 60, execIntervalSeconds: 60, elapsedIntervalSeconds: 60);

        Assert.Equal(0L, workerDelta);
        Assert.Equal(60, intervalSeconds);
    }

    /// <summary>All three counters unknown (first sighting, reset, or gap across the board): unchanged — (0, 0).</summary>
    [Fact]
    public void AllCountersUnknown_StaysZeroAndZero()
    {
        var (workerDelta, intervalSeconds) = QueryStatsCollector.ResolveWorkerDelta(
            workerDelta: 0, workerIntervalSeconds: 0, execIntervalSeconds: 0, elapsedIntervalSeconds: 0);

        Assert.Equal(0L, workerDelta);
        Assert.Equal(0, intervalSeconds);
    }

    /// <summary>A normal worker delta with a real interval passes through untouched.</summary>
    [Fact]
    public void ANormalWorkerDelta_PassesThroughUnchanged()
    {
        var (workerDelta, intervalSeconds) = QueryStatsCollector.ResolveWorkerDelta(
            workerDelta: 100, workerIntervalSeconds: 60, execIntervalSeconds: 60, elapsedIntervalSeconds: 60);

        Assert.Equal(100L, workerDelta);
        Assert.Equal(60, intervalSeconds);
    }

    /// <summary>
    /// Drives the real collector payload: a delta calculator that already knows the exec/elapsed keys
    /// (a prior pass) but sees this row's WORKER key for the first time. On dev this writes a false CPU
    /// of 0 at interval 0 and discards the row's real executions/elapsed under the interval-honest
    /// filter; after the fix it writes CPU as NULL and takes the interval from the exec counter.
    /// </summary>
    [Fact]
    public void ThroughTheCollectorPayload_AFirstSightingCpuWithKnownExecAndElapsed_WritesNullCpuAndARealInterval()
    {
        const string sqlHandle = "0x0102";
        const string planHandle = "0x0304";
        const int startOffset = 0;
        const int endOffset = -1;
        var deltaKey = $"{sqlHandle}:{startOffset}:{endOffset}:{planHandle}";

        var deltas = new CollectorDeltaCalculator();
        var t0 = new System.DateTime(2026, 9, 1, 12, 0, 0, System.DateTimeKind.Unspecified);
        var t1 = t0.AddSeconds(60);

        /* Establish exec and elapsed at pass one so pass two has a real, knowable delta for both — but
           never touch the worker family, so its pass-two call is a genuine first sighting. */
        deltas.CalculateDeltaWithSeriesAge(1, "query_stats_exec", deltaKey, 10, null, out _, t0, CollectorDeltaCalculator.DefaultMaxGapSeconds);
        deltas.CalculateDeltaWithSeriesAge(1, "query_stats_elapsed", deltaKey, 5_000_000, null, out _, t0, CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var row = new QueryStatsCollector.Row
        {
            QueryHash = "queryhash",
            ExecutionCount = 13,
            TotalWorkerTime = 999,
            TotalElapsedTime = 5_010_000,
            SqlHandle = sqlHandle,
            PlanHandle = planHandle,
            StatementStartOffset = startOffset,
            StatementEndOffset = endOffset,
        };

        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "target-a",
            CollectionTime = t1,
            Deltas = deltas,
        };

        var writer = new RecordingCollectorRowWriter();
        QueryStatsCollector.Instance.WritePayload(row, writer, context);

        var columnIndex = QueryStatsCollector.Instance.PayloadColumns
            .Select((c, i) => (c.Name, i))
            .ToDictionary(p => p.Name, p => p.i);

        Assert.Null(writer.Values[columnIndex["delta_worker_time"]]);
        Assert.Equal(3L, writer.Values[columnIndex["delta_execution_count"]]);
        Assert.Equal(60, writer.Values[columnIndex["sample_interval_seconds"]]);
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
