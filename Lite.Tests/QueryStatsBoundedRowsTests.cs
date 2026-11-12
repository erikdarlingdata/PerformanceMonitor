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
/// #7419: a run stores a row only when it is in the top <see cref="QueryStatsCollector.RowsPerRankingPerRun"/> of the run by
/// one of the four interval deltas (CPU, elapsed time, executions, logical reads), and every row, stored or not, still
/// advances its delta baseline. Runs ReadAsync and WritePayload through the real <see cref="CollectorDeltaCalculator"/> the
/// way a host does, so the ranks are the deltas the calculator reports. Both hosts run this one definition and one calculator
/// class (Lite's DeltaCalculator and Darling's DarlingDeltaCalculator both derive from it).
/// </summary>
public sealed class QueryStatsBoundedRowsTests
{
    private static readonly DateTime s_t0 = new(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc);
    private const int N = QueryStatsCollector.RowsPerRankingPerRun;
    private const int Run = 3 * N + 50;

    /// <summary>A statement's cumulative counters and its compile age.</summary>
    private sealed record Stmt(string PlanHandle, long Exec, long Worker, long Elapsed, long Reads, int CompileAge = 100_000);

    private sealed class SeedableCalculator : CollectorDeltaCalculator
    {
        /* The four counters the Stmt record carries get the value, the other four stay at the 0 the DMV row presents. */
        public void SeedAll(int serverId, string key, long value, DateTime timestamp)
        {
            foreach (var group in new[] { "query_stats_exec", "query_stats_worker", "query_stats_elapsed", "query_stats_reads" })
            {
                Seed(serverId, group, key, value, timestamp);
            }

            foreach (var group in new[] { "query_stats_writes", "query_stats_phys_reads", "query_stats_rows", "query_stats_spills" })
            {
                Seed(serverId, group, key, 0, timestamp);
            }
        }
    }

    private static CollectorContext Context(ICollectorDeltaCalculator deltas, DateTime time) => new()
    {
        ServerId = 7,
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

        row[5] = s.Exec; row[6] = s.Worker; row[7] = s.Elapsed; row[8] = s.Reads;
        row[36] = "0xSH" + s.PlanHandle; row[37] = s.PlanHandle; row[38] = "SELECT 1";
        row[39] = 1L; row[40] = 0; row[41] = -1;
        row[42] = "dbo.P";
        row[43] = s.CompileAge;
        return row;
    }

    private static async Task<List<(QueryStatsCollector.Row Row, List<object?> Values)>> CycleAsync(
        ICollectorDeltaCalculator deltas, DateTime time, IEnumerable<Stmt> statements)
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

    private static string Handle(int i) => "0x" + i.ToString("D6", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Statement i's interval deltas: four different permutations of 1..Run, so no ranking has a tie.</summary>
    private static (long Exec, long Worker, long Elapsed, long Reads) Moved(int i) =>
        (i * 7 % Run + 1, i * 11 % Run + 1, i * 17 % Run + 1, i * 19 % Run + 1);

    private static IEnumerable<Stmt> Baseline() =>
        Enumerable.Range(0, Run).Select(i => new Stmt(Handle(i), 1_000, 1_000, 1_000, 1_000));

    private static IEnumerable<Stmt> AfterMove() =>
        Enumerable.Range(0, Run).Select(i =>
        {
            var m = Moved(i);
            return new Stmt(Handle(i), 1_000 + m.Exec, 1_000 + m.Worker, 1_000 + m.Elapsed, 1_000 + m.Reads);
        });

    private static HashSet<string> ExpectedUnion()
    {
        var keep = new HashSet<string>(StringComparer.Ordinal);
        foreach (var pick in new Func<(long Exec, long Worker, long Elapsed, long Reads), long>[]
        {
            m => m.Worker, m => m.Elapsed, m => m.Exec, m => m.Reads,
        })
        {
            foreach (var i in Enumerable.Range(0, Run).OrderByDescending(i => pick(Moved(i))).Take(N))
            {
                keep.Add(Handle(i));
            }
        }

        return keep;
    }

    [Fact]
    public async Task ARunOfMoreThanTheCapStoresExactlyTheUnionOfTheTopRowsByEachOfTheFourDeltas()
    {
        var deltas = new CollectorDeltaCalculator();
        await CycleAsync(deltas, s_t0, Baseline());

        var second = await CycleAsync(deltas, s_t0.AddSeconds(60), AfterMove());

        var expected = ExpectedUnion();
        Assert.True(expected.Count > N && expected.Count < Run, "the permutations must give a union between one list and the whole run");
        Assert.Equal(expected.OrderBy(h => h, StringComparer.Ordinal), second.Select(r => r.Row.PlanHandle!).OrderBy(h => h, StringComparer.Ordinal));
    }

    [Fact]
    public async Task TheKeptRowsComeBackByIntervalElapsedTimeDescending_AndARowKeptOnlyByExecutionsSitsAtItsElapsedPosition()
    {
        var deltas = new CollectorDeltaCalculator();
        await CycleAsync(deltas, s_t0, Baseline());

        var second = await CycleAsync(deltas, s_t0.AddSeconds(60), AfterMove());

        var elapsed = second.Select(r => (long)r.Values[42]!).ToList();   /* delta_elapsed_time */
        Assert.Equal(elapsed.OrderByDescending(e => e), elapsed);

        /* A row in the top by executions that is NOT in the top by any other delta is still returned, at its elapsed rank. */
        var onlyByExec = Enumerable.Range(0, Run)
            .Where(i => Moved(i).Exec > Run - N
                        && Moved(i).Worker <= Run - N && Moved(i).Elapsed <= Run - N && Moved(i).Reads <= Run - N)
            .Select(Handle).ToList();
        Assert.NotEmpty(onlyByExec);
        var order = second.Select(r => r.Row.PlanHandle!).ToList();
        foreach (var handle in onlyByExec)
        {
            Assert.Contains(handle, order);
        }
    }

    [Fact]
    public async Task ADroppedRowStillAdvancesItsBaseline_SoItsNextStoredDeltaCoversOnlyItsOwnInterval()
    {
        var deltas = new CollectorDeltaCalculator();
        var low = new Stmt("0xLOW", 10, 10, 10, 10);
        await CycleAsync(deltas, s_t0, Baseline().Append(low));

        /* Run 2: the low statement moves by 1 in every counter, far below the cut in all four rankings: dropped. */
        var second = await CycleAsync(deltas, s_t0.AddSeconds(60), AfterMove().Append(low with { Exec = 11, Worker = 11, Elapsed = 11, Reads = 11 }));
        Assert.DoesNotContain(second, r => r.Row.PlanHandle == "0xLOW");

        /* Run 3: only the low statement moves, by 5. If run 2 had not advanced its baseline the delta would be 6 over 120 s. */
        var third = await CycleAsync(deltas, s_t0.AddSeconds(120), new[] { low with { Exec = 16, Worker = 16, Elapsed = 16, Reads = 16 } });

        var (_, values) = Assert.Single(third);
        Assert.Equal(5L, values[40]);   /* delta_execution_count */
        Assert.Equal(5L, values[42]);   /* delta_elapsed_time */
        Assert.Equal(60, values[49]);   /* sample_interval_seconds: one interval, not two */
    }

    [Fact]
    public async Task APlanThatJustRecompiledIsKeptOnItsWorkSinceCreation_WhileAnOldFirstSightingTakesNoSlot()
    {
        var deltas = new CollectorDeltaCalculator();
        await CycleAsync(deltas, s_t0, Baseline());

        var fresh = new Stmt("0xFRESH", 50, 40_000_000, 90_000_000, 1_000_000, CompileAge: 30);   /* compiled inside the interval */
        var oldFirstSighting = new Stmt("0xOLD", 9_000_000, 9_000_000_000, 9_000_000_000, 9_000_000_000);   /* huge lifetime, no baseline */
        var second = await CycleAsync(deltas, s_t0.AddSeconds(60), AfterMove().Append(fresh).Append(oldFirstSighting));

        var kept = second.Select(r => r.Row.PlanHandle).ToList();
        Assert.Equal("0xFRESH", kept[0]);                   /* its whole accrual is the interval's work, so it ranks first */
        Assert.DoesNotContain("0xOLD", kept);                /* ranks on a stored delta of 0, behind rows that did work */
    }

    [Fact]
    public async Task TiesInEveryRankingBreakTheSameWayWhateverTheInputOrder()
    {
        async Task<List<string>> RunAsync(bool reversed)
        {
            var deltas = new CollectorDeltaCalculator();
            var ids = Enumerable.Range(0, Run).ToList();
            var baseline = ids.Select(i => new Stmt(Handle(i), 1_000, 1_000, 1_000, 1_000)).ToList();
            var moved = ids.Select(i => new Stmt(Handle(i), 1_001, 1_001, 1_001, 1_001)).ToList();   /* every delta ties */
            if (reversed)
            {
                baseline.Reverse();
                moved.Reverse();
            }

            await CycleAsync(deltas, s_t0, baseline);
            var second = await CycleAsync(deltas, s_t0.AddSeconds(60), moved);
            return second.Select(r => r.Row.PlanHandle!).ToList();
        }

        var forward = await RunAsync(reversed: false);
        var backward = await RunAsync(reversed: true);

        Assert.Equal(N, forward.Count);
        Assert.Equal(forward, backward);
        Assert.Equal(Enumerable.Range(0, N).Select(Handle), forward);   /* all four rankings tie, so the plan_handle order decides */
    }

    [Fact]
    public async Task AFirstRunWithNoBaselinesStoresOneRankingsWorthOfRows_TheHeaviestByLifetimeElapsedTime()
    {
        var deltas = new CollectorDeltaCalculator();

        var first = await CycleAsync(
            deltas, s_t0,
            Enumerable.Range(0, Run).Select(i => new Stmt(Handle(i), 5, 5, 1_000 + i, 5)));

        Assert.Equal(N, first.Count);
        Assert.Equal(
            Enumerable.Range(Run - N, N).Select(Handle).OrderBy(h => h, StringComparer.Ordinal),
            first.Select(r => r.Row.PlanHandle!).OrderBy(h => h, StringComparer.Ordinal));
    }

    [Fact]
    public async Task ARunOfTheCapOrFewerRowsIsStoredWhole_IncludingFirstSightings()
    {
        var deltas = new CollectorDeltaCalculator();
        var first = await CycleAsync(deltas, s_t0, Enumerable.Range(0, N).Select(i => new Stmt(Handle(i), 5, 5, 5, 5)));
        Assert.Equal(N, first.Count);
    }

    [Fact]
    public async Task ASeededBaselineInsideTheGapPolicyGivesOneHonestIntervalDelta_AndOneOlderThanThePolicyGivesTheUnknowablePair()
    {
        /* Both hosts seed from the last hour of stored rows (QueryStatsSeedLookback is the gap policy), so a seeded
           baseline is never older than the policy at seed time. A baseline restored from a row stored 50 minutes ago is a
           delta over 50 minutes with that interval stored beside it, never a hidden spike. One that has aged past the policy
           by the time of the first run (a slow restart) is the (0, 0) pair. A key with no seed is a first sighting. */
        var seeded = new SeedableCalculator();
        seeded.SeedAll(7, "0xSH0xinside:0:-1:0xinside", 1_000, s_t0.AddMinutes(-50));
        seeded.SeedAll(7, "0xSH0xpast:0:-1:0xpast", 1_000, s_t0.AddSeconds(-(CollectorDeltaCalculator.DefaultMaxGapSeconds + 30)));

        var rows = await CycleAsync(seeded, s_t0, new[]
        {
            new Stmt("0xinside", 1_100, 1_100, 1_100, 1_100),
            new Stmt("0xpast", 1_100, 1_100, 1_100, 1_100),
            new Stmt("0xunseeded", 1_100, 1_100, 1_100, 1_100),
        });

        var byHandle = rows.ToDictionary(r => r.Row.PlanHandle!, r => r.Values);
        Assert.Equal(100L, byHandle["0xinside"][40]);
        Assert.Equal(3000, byHandle["0xinside"][49]);
        Assert.Equal(0L, byHandle["0xpast"][40]);
        Assert.Equal(0, byHandle["0xpast"][49]);
        Assert.Equal(0L, byHandle["0xunseeded"][40]);
        Assert.Equal(0, byHandle["0xunseeded"][49]);
    }
}
