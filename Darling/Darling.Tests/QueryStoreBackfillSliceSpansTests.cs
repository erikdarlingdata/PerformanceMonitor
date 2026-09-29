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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4771: the window a Query Store backfill slice uses is kept per server. A failed slice halves it as before,
/// but a completed slice now keeps the span that just worked, and only a run of completed slices widens it by
/// one halving step, never above the full span. It used to reset to the full hour on any completed slice, so a
/// database whose 30-minute slice fit and whose 60-minute slice did not ran 60 (timed out), 30, 60 (timed out),
/// 30, a full command-timeout read wasted on every other tick.
///
/// <para>Nothing here needs a store. The tracker is a plain object, and the worker's own accounting is driven
/// through <see cref="QueryStoreBackfill.SliceOverrideForTests"/>, which replaces the slice body before any
/// connection is opened.</para>
/// </summary>
public sealed class QueryStoreBackfillSliceSpansTests
{
    private const int FirstServerId = -477101;
    private const int SecondServerId = -477102;

    /// <summary>Never connected to: the slice body is replaced, so no read is issued.</summary>
    private const string UnusedStore = "Host=/no-such-directory-4771;Username=x;Password=x;Database=x;Timeout=2";

    private static TimeSpan Full => QueryStoreBackfillState.MaxSliceSpan;

    private static int WidenAfter => QueryStoreBackfillState.WidenAfterConsecutiveSuccesses;

    private static TimeSpan Minutes(int minutes) => TimeSpan.FromMinutes(minutes);

    [Fact]
    public void TheWidenThreshold_IsThreeCompletedSlices()
    {
        Assert.Equal(3, WidenAfter);
        Assert.Equal(Minutes(60), Full);
    }

    [Fact]
    public void ANewServer_StartsAtTheFullSpan_AndEachFailureHalvesItLikeAdaptiveSpan()
    {
        var spans = new QueryStoreBackfillSliceSpans();
        Assert.Equal(Full, spans.Current(1));

        for (var failures = 1; failures <= 8; failures++)
        {
            spans.RecordFailure(1);
            Assert.Equal(QueryStoreBackfillState.AdaptiveSpan(Full, failures), spans.Current(1));
        }

        /* Floored at the narrowest span, and another server is untouched. */
        Assert.Equal(QueryStoreBackfillState.MinAdaptiveSpan, spans.Current(1));
        Assert.Equal(Full, spans.Current(2));
    }

    [Fact]
    public void AfterATimeout_ACompletedSlice_KeepsTheSpan_AndARunOfCompletionsWidensItOneStep()
    {
        var spans = new QueryStoreBackfillSliceSpans();

        /* The 60-minute slice times out, and the 30-minute slice completes. */
        spans.RecordFailure(1);
        Assert.Equal(Minutes(30), spans.Current(1));

        for (var completed = 1; completed < WidenAfter; completed++)
        {
            spans.RecordCompletion(1);
            Assert.Equal(Minutes(30), spans.Current(1));
        }

        spans.RecordCompletion(1);
        Assert.Equal(Full, spans.Current(1));
    }

    [Fact]
    public void WideningIsOneHalvingStepAtATime_AndNeverPassesTheFullSpan()
    {
        var spans = new QueryStoreBackfillSliceSpans();
        spans.RecordFailure(1);
        spans.RecordFailure(1);
        Assert.Equal(Minutes(15), spans.Current(1));

        for (var completed = 0; completed < WidenAfter; completed++)
        {
            spans.RecordCompletion(1);
        }

        Assert.Equal(Minutes(30), spans.Current(1));

        for (var completed = 0; completed < WidenAfter; completed++)
        {
            spans.RecordCompletion(1);
        }

        Assert.Equal(Full, spans.Current(1));

        /* At the full span there is nothing wider to go to. */
        for (var completed = 0; completed < 3 * WidenAfter; completed++)
        {
            spans.RecordCompletion(1);
            Assert.Equal(Full, spans.Current(1));
        }
    }

    [Fact]
    public void AFailure_EndsTheRunOfCompletions_SoTheCountStartsOver()
    {
        var spans = new QueryStoreBackfillSliceSpans();
        spans.RecordFailure(1);

        /* One short of a widening, then a failure: the run is gone, and the failure halves again. */
        for (var completed = 1; completed < WidenAfter; completed++)
        {
            spans.RecordCompletion(1);
        }

        spans.RecordFailure(1);
        Assert.Equal(Minutes(15), spans.Current(1));

        /* A full run is needed again from the start. */
        for (var completed = 1; completed < WidenAfter; completed++)
        {
            spans.RecordCompletion(1);
            Assert.Equal(Minutes(15), spans.Current(1));
        }

        spans.RecordCompletion(1);
        Assert.Equal(Minutes(30), spans.Current(1));
    }

    [Fact]
    public void TheSpanIsKeptPerServer()
    {
        var spans = new QueryStoreBackfillSliceSpans();
        spans.RecordFailure(1);
        spans.RecordFailure(1);

        Assert.Equal(Minutes(15), spans.Current(1));
        Assert.Equal(Full, spans.Current(2));

        /* Another server's completions do not count toward this server's run. */
        for (var completed = 0; completed < WidenAfter; completed++)
        {
            spans.RecordCompletion(2);
        }

        Assert.Equal(Minutes(15), spans.Current(1));
        Assert.Equal(Full, spans.Current(2));
    }

    [Fact]
    public async Task TheBackfillWorker_KeepsTheSpanThatFit_AfterATimeout_AndWidensAfterARunOfSuccesses()
    {
        /* The first slice times out and every later one completes: 60 (fails), 30, 30, 30, then 60. */
        var spans = await RunSlicesAsync(
            count: 6, serverIds: [FirstServerId],
            fails: attempt => attempt == 1);

        Assert.Equal(new[] { 60, 30, 30, 30, 60, 60 }.Select(Minutes), spans);
    }

    [Fact]
    public async Task TheBackfillWorker_KeepsTheSpanPerServer()
    {
        /* Only the first server's first slice fails; the second server's slices run at the full span throughout,
           and its completions do not widen the first server's span. */
        var attempts = new List<(int ServerId, TimeSpan Span)>();
        var spans = await RunSlicesAsync(
            count: 4, serverIds: [FirstServerId, SecondServerId],
            fails: attempt => attempt == 1,
            record: (serverId, span) => attempts.Add((serverId, span)));

        Assert.Equal(new[] { 60, 30, 30, 30 }.Select(Minutes), spans);
        Assert.All(attempts.FindAll(a => a.ServerId == SecondServerId), a => Assert.Equal(Full, a.Span));
    }

    /// <summary>Runs <paramref name="count"/> slices through the worker's own accounting for each server in turn and
    /// returns the first server's spans in order. <paramref name="fails"/> is asked with the 1-based attempt number
    /// of the FIRST server's slice; the other servers' slices always complete.</summary>
    private static async Task<List<TimeSpan>> RunSlicesAsync(
        int count, int[] serverIds, Func<int, bool> fails, Action<int, TimeSpan>? record = null)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(UnusedStore);
        var backfill = new QueryStoreBackfill(
            postgres, new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), new CollectorDeltaCalculator(), logger: null);

        var current = serverIds[0];
        var first = new List<TimeSpan>();
        backfill.SliceOverrideForTests = (database, span) =>
        {
            record?.Invoke(current, span);
            if (current != serverIds[0])
            {
                return Task.CompletedTask;
            }

            first.Add(span);
            return fails(first.Count) ? throw new InvalidOperationException("simulated slice failure") : Task.CompletedTask;
        };

        for (var i = 0; i < count; i++)
        {
            foreach (var serverId in serverIds)
            {
                current = serverId;
                try
                {
                    await backfill.RunCountedSliceAsync(
                        NewServer(serverId), "spans_db", DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, isHole: false, ct);
                }
                catch (InvalidOperationException ex) when (ex.Message.StartsWith("simulated", StringComparison.Ordinal))
                {
                    /* The worker's outer catch logs a failed slice and carries on to the next tick. */
                }
            }
        }

        return first;
    }

    private static ServerRuntime NewServer(int serverId) => new()
    {
        Config = new MonitoredServer { Name = "backfill-span-test-" + serverId, Host = "backfill-span-test-" + serverId },
        ConnectionString = "Server=backfill-span-test",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "backfill-span-test-" + serverId,
        ServerId = serverId,
        EngineEdition = 3,
    };
}
