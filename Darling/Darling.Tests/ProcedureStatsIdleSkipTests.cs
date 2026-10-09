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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5449: <c>procedure_stats</c> keeps every candidate that did work. A row whose seven counters all equal their cached
/// baselines is dropped at read time (the calculator advances its timestamp and reports zero over a measured interval);
/// every other row is stored exactly as before, with its delta computed by the write. These tests drive
/// <c>ReadAsync</c> and <c>WritePayload</c> through the real <see cref="CollectorDeltaCalculator"/> the way a host does.
/// </summary>
public sealed class ProcedureStatsIdleSkipTests
{
    private static readonly DateTime s_t0 = new(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>One DMV row's counters: handle, executions, worker, elapsed, logical reads, logical writes, physical reads, spills.</summary>
    private readonly record struct Counters(string Handle, long Exec, long Worker, long Elapsed, long Reads, long Writes, long Phys, long Spills)
    {
        public static Counters Of(string handle, long n, long elapsed = -1) =>
            new(handle, n, n * 10, elapsed < 0 ? n * 100 : elapsed, n * 5, n * 2, n, 0);
    }

    private readonly record struct Written(string Handle, long[] Deltas, int Interval);

    /// <summary>Delegates to a real calculator and counts every delta call per (group, key).</summary>
    private sealed class CountingCalculator : ICollectorDeltaCalculator
    {
        private readonly CollectorDeltaCalculator _inner = new();
        public Dictionary<string, int> Calls { get; } = new();
        public int CallsFor(string key) => Calls.Where(c => c.Key.EndsWith("|" + key, StringComparison.Ordinal)).Sum(c => c.Value);

        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue, DateTime? collectionTime = null, int maxGapSeconds = 0)
            => _inner.CalculateDelta(serverId, collectorName, key, currentValue, collectionTime, maxGapSeconds);

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            var id = collectorName + "|" + key;
            Calls[id] = Calls.GetValueOrDefault(id) + 1;
            return _inner.CalculateDeltaWithInterval(serverId, collectorName, key, currentValue, out intervalSeconds, collectionTime, maxGapSeconds);
        }

        public IReadOnlyDictionary<string, long> PeekBaselines(int serverId, string collectorName) => _inner.PeekBaselines(serverId, collectorName);
    }

    /// <summary>A calculator that peeks nothing: the interface's empty default, as every older test double does.</summary>
    private sealed class NoPeekCalculator : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue, DateTime? collectionTime = null, int maxGapSeconds = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 60;
            return 0;
        }
    }

    private sealed class CapturingWriter : ICollectorRowWriter
    {
        public List<long> Longs { get; } = new();
        public List<int> Ints { get; } = new();
        public ICollectorRowWriter Value(string? value) => this;
        public ICollectorRowWriter Value(long value) { Longs.Add(value); return this; }
        public ICollectorRowWriter Value(long? value) => this;
        public ICollectorRowWriter Value(int value) { Ints.Add(value); return this; }
        public ICollectorRowWriter Value(int? value) => this;
        public ICollectorRowWriter Value(short value) => this;
        public ICollectorRowWriter Value(short? value) => this;
        public ICollectorRowWriter Value(double value) => this;
        public ICollectorRowWriter Value(double? value) => this;
        public ICollectorRowWriter Value(decimal value) => this;
        public ICollectorRowWriter Value(decimal? value) => this;
        public ICollectorRowWriter Value(bool value) => this;
        public ICollectorRowWriter Value(bool? value) => this;
        public ICollectorRowWriter Value(DateTime value) => this;
        public ICollectorRowWriter Value(DateTime? value) => this;
        public ICollectorRowWriter NullValue() => this;
    }

    private static CollectorContext Context(ICollectorDeltaCalculator deltas, DateTime time) => new()
    {
        ServerId = 7,
        ServerName = "example-sql-01",
        CollectionTime = time,
        Deltas = deltas,
        Target = new CollectorTargetInfo(),
        CapturePlanXml = false,
    };

    private static async Task<List<ProcedureStatsCollector.Row>> ReadAsync(ICollectorDeltaCalculator deltas, DateTime time, params Counters[] rows)
    {
        using var table = new DataTable();
        for (var i = 0; i < 27; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), i switch
            {
                4 or 5 => typeof(DateTime),
                0 or 1 or 2 or 3 or 25 or 26 => typeof(string),
                _ => typeof(long),
            });
        }

        foreach (var r in rows)
        {
            var v = new object[27];
            v[0] = "db";
            v[1] = "dbo";
            v[2] = "proc_" + r.Handle;
            v[3] = "PROCEDURE";
            v[4] = s_t0.AddDays(-1);
            v[5] = time;
            v[6] = r.Exec;
            v[7] = r.Worker;
            v[8] = r.Elapsed;
            v[9] = r.Reads;
            v[10] = r.Phys;
            v[11] = r.Writes;
            for (var i = 12; i <= 21; i++)
            {
                v[i] = 0L;
            }

            v[22] = r.Spills;
            v[23] = 0L;
            v[24] = 0L;
            v[25] = "0xAA" + r.Handle;
            v[26] = r.Handle;
            table.Rows.Add(v);
        }

        await using var reader = table.CreateDataReader();
        return await ProcedureStatsCollector.Instance.ReadAsync(reader, Context(deltas, time), CancellationToken.None);
    }

    private static List<Written> Write(ICollectorDeltaCalculator deltas, DateTime time, IEnumerable<ProcedureStatsCollector.Row> rows)
    {
        var result = new List<Written>();
        foreach (var row in rows)
        {
            var writer = new CapturingWriter();
            ProcedureStatsCollector.Instance.WritePayload(row, writer, Context(deltas, time));
            result.Add(new Written(row.PlanHandle!, writer.Longs.Skip(19).Take(7).ToArray(), Assert.Single(writer.Ints)));
        }

        return result;
    }

    /// <summary>One full cycle: read, then write what the read kept.</summary>
    private static async Task<List<Written>> CycleAsync(ICollectorDeltaCalculator deltas, DateTime time, params Counters[] rows)
        => Write(deltas, time, await ReadAsync(deltas, time, rows));

    [Fact]
    public async Task AFirstSighting_IsKept_WithZeroDeltasAndNoInterval()
    {
        var calc = new CountingCalculator();

        var written = await CycleAsync(calc, s_t0, Counters.Of("01", 10));

        var row = Assert.Single(written);
        Assert.All(row.Deltas, d => Assert.Equal(0L, d));
        Assert.Equal(0, row.Interval);
    }

    [Fact]
    public async Task AnUnchangedRow_OverAMeasuredInterval_IsDropped_AndItsBaselineAdvances()
    {
        var calc = new CountingCalculator();
        await CycleAsync(calc, s_t0, Counters.Of("01", 10));

        var kept = await ReadAsync(calc, s_t0.AddSeconds(60), Counters.Of("01", 10));

        Assert.Empty(kept);
        /* The idle row called the calculator exactly once per group at read time, and nothing writes it. */
        Assert.Equal(7 * 2, calc.CallsFor("01"));

        /* Its baseline moved to the new timestamp: a busy cycle one interval later reports one interval, not two. */
        var busy = await CycleAsync(calc, s_t0.AddSeconds(120), Counters.Of("01", 13));
        var row = Assert.Single(busy);
        Assert.Equal(3L, row.Deltas[0]);
        Assert.Equal(60, row.Interval);
    }

    [Fact]
    public async Task AMovedRow_IsKept_AndKeepsTheWritesDeltaPath_WithNoReadTimeCall()
    {
        var calc = new CountingCalculator();
        await CycleAsync(calc, s_t0, Counters.Of("01", 10));
        var before = calc.CallsFor("01");

        var kept = await ReadAsync(calc, s_t0.AddSeconds(60), Counters.Of("01", 12));

        var row = Assert.Single(kept);
        Assert.Null(row.Precomputed);
        Assert.Equal(before, calc.CallsFor("01")); /* the read did not call the calculator for a moved row */

        var written = Write(calc, s_t0.AddSeconds(60), kept);
        Assert.Equal(2L, written[0].Deltas[0]);
        Assert.Equal(60, written[0].Interval);
        Assert.Equal(before + 7, calc.CallsFor("01")); /* the write called it once per group */
    }

    [Fact]
    public async Task AGapPastThePolicy_KeepsAnUnchangedRow_WithZeroZero_AndNoSecondCalculatorCall()
    {
        var calc = new CountingCalculator();
        await CycleAsync(calc, s_t0, Counters.Of("01", 10));
        var before = calc.CallsFor("01");
        var later = s_t0.AddSeconds(CollectorDeltaCalculator.DefaultMaxGapSeconds + 1);

        var kept = await ReadAsync(calc, later, Counters.Of("01", 10));

        var row = Assert.Single(kept);
        Assert.NotNull(row.Precomputed);
        Assert.Equal(0, row.Precomputed!.Value.IntervalSeconds);

        var written = Write(calc, later, kept);
        Assert.All(written[0].Deltas, d => Assert.Equal(0L, d));
        Assert.Equal(0, written[0].Interval);
        Assert.Equal(before + 7, calc.CallsFor("01")); /* seven calls in all for this run, made at read time, none in the write */
    }

    [Fact]
    public async Task ACalculatorThatPeeksNothing_DropsNothing()
    {
        var calc = new NoPeekCalculator();

        var kept = await ReadAsync(calc, s_t0, Counters.Of("01", 10), Counters.Of("02", 20));
        var again = await ReadAsync(calc, s_t0.AddSeconds(60), Counters.Of("01", 10), Counters.Of("02", 20));

        Assert.Equal(2, kept.Count);
        Assert.Equal(2, again.Count);
        Assert.All(again, r => Assert.Null(r.Precomputed));
    }

    [Fact]
    public async Task ARowIdleForLongerThanTheGapPolicyCovers_ThenBusy_GetsARealDeltaOverOneInterval()
    {
        var calc = new CountingCalculator();
        await CycleAsync(calc, s_t0, Counters.Of("01", 10));

        /* 70 minutes of idle cycles, each one a drop that advances the baseline: more than DefaultMaxGapSeconds in all. */
        var time = s_t0;
        for (var i = 0; i < 70; i++)
        {
            time = time.AddSeconds(60);
            Assert.Empty(await ReadAsync(calc, time, Counters.Of("01", 10)));
        }

        time = time.AddSeconds(60);
        var busy = await CycleAsync(calc, time, Counters.Of("01", 16));

        var row = Assert.Single(busy);
        Assert.Equal(6L, row.Deltas[0]); /* a real delta, never a re-baseline */
        Assert.Equal(60, row.Interval);
    }

    [Fact]
    public async Task AReadWithNoWrite_ThenABusyCycle_GetsBothSlicesOfWork()
    {
        /* The abandon case (#2673): the cycle read, blew its budget and shipped nothing. The next cycle has to cover the slice. */
        var calc = new CountingCalculator();
        await CycleAsync(calc, s_t0, Counters.Of("01", 10));

        var abandoned = await ReadAsync(calc, s_t0.AddSeconds(60), Counters.Of("01", 14));
        Assert.Single(abandoned); /* read, kept, never written */

        var next = await CycleAsync(calc, s_t0.AddSeconds(120), Counters.Of("01", 19));

        var row = Assert.Single(next);
        Assert.Equal(9L, row.Deltas[0]); /* 4 from the abandoned slice plus 5 from this one */
        Assert.Equal(120, row.Interval);
    }

    [Fact]
    public async Task EveryFetchedRow_CallsTheCalculatorOncePerGroup_PerRun()
    {
        var calc = new CountingCalculator();
        await CycleAsync(calc, s_t0, Counters.Of("01", 10), Counters.Of("02", 20), Counters.Of("03", 30));
        var b1 = calc.CallsFor("01");
        var b2 = calc.CallsFor("02");
        var b3 = calc.CallsFor("03");

        /* 01 and 03 idle (dropped at read), 02 moved (written). */
        var later = s_t0.AddSeconds(30);
        var rows = await ReadAsync(calc, later, Counters.Of("01", 10), Counters.Of("02", 25), Counters.Of("03", 30));
        Write(calc, later, rows);

        Assert.Equal(b1 + 7, calc.CallsFor("01"));
        Assert.Equal(b2 + 7, calc.CallsFor("02"));
        Assert.Equal(b3 + 7, calc.CallsFor("03"));
    }

    [Fact]
    public async Task OnePartlyMovedCounter_KeepsTheRow()
    {
        var calc = new CountingCalculator();
        await CycleAsync(calc, s_t0, Counters.Of("01", 10));

        /* Only the spills counter moved: not idle. */
        var kept = await ReadAsync(calc, s_t0.AddSeconds(60), Counters.Of("01", 10) with { Spills = 3 });

        Assert.Single(kept);
    }

    [Fact]
    public async Task TheKeptRows_AreRankedByInIntervalElapsedTime_NotLifetime()
    {
        var calc = new CountingCalculator();
        /* "01" has the biggest lifetime elapsed time, "02" the smallest. */
        await CycleAsync(calc, s_t0, Counters.Of("01", 1, elapsed: 1_000_000), Counters.Of("02", 1, elapsed: 10), Counters.Of("03", 1, elapsed: 500));

        var kept = await ReadAsync(
            calc, s_t0.AddSeconds(60),
            Counters.Of("01", 2, elapsed: 1_000_050),   /* +50 */
            Counters.Of("02", 2, elapsed: 9_010),       /* +9000 */
            Counters.Of("03", 2, elapsed: 2_500));      /* +2000 */

        Assert.Equal(new[] { "02", "03", "01" }, kept.Select(r => r.PlanHandle));
    }

    [Fact]
    public async Task OnAColdStart_TheRankIsLifetimeElapsedTime_AndTiesBreakByPlanHandle()
    {
        var calc = new CountingCalculator();

        var kept = await ReadAsync(
            calc, s_t0,
            Counters.Of("03", 1, elapsed: 500),
            Counters.Of("02", 1, elapsed: 900),
            Counters.Of("05", 1, elapsed: 900),
            Counters.Of("01", 1, elapsed: 900),
            Counters.Of("04", 1, elapsed: 100));

        /* 900 x3 by handle ordinal, then 500, then 100. */
        Assert.Equal(new[] { "01", "02", "05", "03", "04" }, kept.Select(r => r.PlanHandle));
    }

    [Fact]
    public async Task ACounterThatWentBackwards_RanksByLifetimeElapsedTime()
    {
        var calc = new CountingCalculator();
        await CycleAsync(calc, s_t0, Counters.Of("01", 5, elapsed: 1_000), Counters.Of("02", 5, elapsed: 400));

        /* 01's counter reset (a plan cache eviction): current < baseline, so its rank is its lifetime value. */
        var kept = await ReadAsync(calc, s_t0.AddSeconds(60), Counters.Of("01", 6, elapsed: 300), Counters.Of("02", 6, elapsed: 700));

        Assert.Equal(new[] { "01", "02" }, kept.Select(r => r.PlanHandle)); /* 300 (lifetime) vs +300: tie, handle order */
    }
}
