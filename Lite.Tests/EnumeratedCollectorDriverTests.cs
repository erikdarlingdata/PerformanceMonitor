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
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #1556: the shared enumeration driver's control flow, via fake delegates (no SQL, no store). Pins the
/// properties the field incident hinged on: each item is FLUSHED separately (no cross-item accumulation),
/// empty batches open no write, a per-item failure is skipped while the rest continue, an OutOfMemoryException
/// is rethrown rather than swallowed, cancellation propagates, and the warn hook sees each item's batch count.
/// </summary>
public sealed class EnumeratedCollectorDriverTests
{
    [Fact]
    public async Task RunAsync_FlushesEachItemSeparately_WithNoCrossItemAccumulation()
    {
        var items = new[] { "a", "b", "c" };
        var writtenBatches = new List<List<int>>();

        var result = await EnumeratedCollectorDriver.RunAsync<int>(
            items,
            perItemWatermark: null,
            readItem: (item, ct) => Task.FromResult(new List<int> { item[0] }),
            writeBatch: (batch, ct) => { writtenBatches.Add(batch.ToList()); return Task.CompletedTask; },
            onItemComplete: (item, count, sqlMs, storageMs) => { },
            onItemError: (item, ex) => { },
            CancellationToken.None);

        /* One flush per item, and each flushed batch is exactly that item's rows — never an accumulation
           of the ones before it (the byte-blow-up the driver exists to prevent). */
        Assert.Equal(3, writtenBatches.Count);
        Assert.All(writtenBatches, b => Assert.Single(b));
        Assert.Equal(new[] { (int)'a', (int)'b', (int)'c' }, writtenBatches.Select(b => b[0]));
        Assert.Equal(3, result.Rows);
    }

    [Fact]
    public async Task RunAsync_EmptyBatch_OpensNoWrite_ButStillWarns()
    {
        var items = new[] { "a", "b", "c" };
        var writeCount = 0;
        var completed = new List<(string Item, int Count)>();

        var result = await EnumeratedCollectorDriver.RunAsync<int>(
            items,
            perItemWatermark: null,
            readItem: (item, ct) => Task.FromResult(item == "b" ? new List<int>() : new List<int> { 1 }),
            writeBatch: (batch, ct) => { writeCount++; return Task.CompletedTask; },
            onItemComplete: (item, count, sqlMs, storageMs) => completed.Add((item, count)),
            onItemError: (item, ex) => { },
            CancellationToken.None);

        /* Empty "b" contributes no COPY/appender and no rows, but the warn hook still ran for it (count 0). */
        Assert.Equal(2, writeCount);
        Assert.Equal(2, result.Rows);
        Assert.Equal(3, completed.Count);
        Assert.Equal(0, completed.Single(c => c.Item == "b").Count);
    }

    [Fact]
    public async Task RunAsync_PerItemError_IsSkipped_AndTheRestContinue()
    {
        var items = new[] { "a", "b", "c" };
        var errors = new List<string>();
        var completed = new List<string>();
        var writeCount = 0;

        var result = await EnumeratedCollectorDriver.RunAsync<int>(
            items,
            perItemWatermark: null,
            readItem: (item, ct) => item == "b"
                ? throw new InvalidOperationException("boom")
                : Task.FromResult(new List<int> { 1 }),
            writeBatch: (batch, ct) => { writeCount++; return Task.CompletedTask; },
            onItemComplete: (item, count, sqlMs, storageMs) => completed.Add(item),
            onItemError: (item, ex) => errors.Add(item),
            CancellationToken.None);

        /* "b" is skipped (logged via onItemError, never onItemComplete, never written); "a"+"c" still land. */
        Assert.Equal(new[] { "b" }, errors);
        Assert.Equal(new[] { "a", "c" }, completed);
        Assert.Equal(2, writeCount);
        Assert.Equal(2, result.Rows);
    }

    [Fact]
    public async Task RunAsync_OutOfMemory_IsRethrown_NotSwallowedAsASkip()
    {
        var items = new[] { "a", "b", "c" };
        var errors = new List<string>();
        var writeCount = 0;

        await Assert.ThrowsAsync<OutOfMemoryException>(async () =>
            await EnumeratedCollectorDriver.RunAsync<int>(
                items,
                perItemWatermark: null,
                readItem: (item, ct) => item == "b"
                    ? throw new OutOfMemoryException()
                    : Task.FromResult(new List<int> { 1 }),
                writeBatch: (batch, ct) => { writeCount++; return Task.CompletedTask; },
                onItemComplete: (item, count, sqlMs, storageMs) => { },
                onItemError: (item, ex) => errors.Add(item),
                CancellationToken.None));

        /* OOM is fatal, not a per-item skip: it is NOT filed through onItemError, and "c" is never reached.
           "a" was flushed before "b" blew up (commit-1..N-1). */
        Assert.Empty(errors);
        Assert.Equal(1, writeCount);
    }

    [Fact]
    public async Task RunAsync_ReadItemThrowsOperationCanceled_Propagates()
    {
        var items = new[] { "a" };
        var errors = new List<string>();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await EnumeratedCollectorDriver.RunAsync<int>(
                items,
                perItemWatermark: null,
                readItem: (item, ct) => throw new OperationCanceledException(),
                writeBatch: (batch, ct) => Task.CompletedTask,
                onItemComplete: (item, count, sqlMs, storageMs) => { },
                onItemError: (item, ex) => errors.Add(item),
                CancellationToken.None));

        /* Cancellation is not a per-item skip either — the filtered catch lets it through. */
        Assert.Empty(errors);
    }

    [Fact]
    public async Task RunAsync_AlreadyCancelledToken_ThrowsBeforeAnyItem()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var reads = 0;

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await EnumeratedCollectorDriver.RunAsync<int>(
                new[] { "a", "b" },
                perItemWatermark: null,
                readItem: (item, ct) => { reads++; return Task.FromResult(new List<int> { 1 }); },
                writeBatch: (batch, ct) => Task.CompletedTask,
                onItemComplete: (item, count, sqlMs, storageMs) => { },
                onItemError: (item, ex) => { },
                cts.Token));

        Assert.Equal(0, reads);
    }

    [Fact]
    public async Task RunAsync_PerItemWatermark_RunsBeforeEachRead()
    {
        var order = new List<string>();

        await EnumeratedCollectorDriver.RunAsync<int>(
            new[] { "a", "b" },
            perItemWatermark: (item, ct) => { order.Add($"wm:{item}"); return Task.CompletedTask; },
            readItem: (item, ct) => { order.Add($"read:{item}"); return Task.FromResult(new List<int>()); },
            writeBatch: (batch, ct) => Task.CompletedTask,
            onItemComplete: (item, count, sqlMs, storageMs) => { },
            onItemError: (item, ex) => { },
            CancellationToken.None);

        /* The per-database cutoff (watermark + clamp) is computed before that database's query is built. */
        Assert.Equal(new[] { "wm:a", "read:a", "wm:b", "read:b" }, order);
    }

    [Fact]
    public async Task RunAsync_WarnHook_ReceivesEachItemsBatchCount()
    {
        var completed = new List<(string Item, int Count)>();

        var result = await EnumeratedCollectorDriver.RunAsync<int>(
            new[] { "a", "b" },
            perItemWatermark: null,
            readItem: (item, ct) => Task.FromResult(item == "a"
                ? new List<int> { 1, 2, 3 }
                : new List<int> { 9 }),
            writeBatch: (batch, ct) => Task.CompletedTask,
            onItemComplete: (item, count, sqlMs, storageMs) => completed.Add((item, count)),
            onItemError: (item, ex) => { },
            CancellationToken.None);

        /* The count is the per-database delta the host compares to the row cap; under per-item flush it is
           exactly the batch size. */
        Assert.Equal((3, 1), (completed.Single(c => c.Item == "a").Count, completed.Single(c => c.Item == "b").Count));
        Assert.Equal(4, result.Rows);
    }

    /* ---------------- #2150: the per-item wall-clock budget ---------------- */

    /// <summary>
    /// A single slow database is abandoned and the rest of the cycle continues. THE property the field
    /// report needs: two Azure SQL DB databases produced per-database passes of up to 99.8 minutes, and
    /// because a host's live collectors run one after another, that one pass starved every other collector
    /// on the server (#2148's "all collection stopped").
    /// </summary>
    [Fact]
    public async Task RunAsync_AnItemThatExceedsItsBudget_IsAbandoned_AndTheRestStillCollect()
    {
        var errors = new List<(string Item, string Message)>();
        var written = new List<int>();

        var result = await EnumeratedCollectorDriver.RunAsync<int>(
            new[] { "fast-a", "slow", "fast-b" },
            perItemWatermark: null,
            readItem: async (item, ct) =>
            {
                if (item == "slow")
                {
                    /* Longer than any test would tolerate, so the budget is what ends it — and awaited on
                       the token so the wait is what gets cancelled rather than the delay elapsing. */
                    await Task.Delay(TimeSpan.FromMinutes(5), ct);
                }

                return new List<int> { item.Length };
            },
            writeBatch: (batch, ct) => { written.AddRange(batch); return Task.CompletedTask; },
            onItemComplete: (item, count, sqlMs, storageMs) => { },
            onItemError: (item, ex) => errors.Add((item, ex.Message)),
            CancellationToken.None,
            perItemBudget: TimeSpan.FromMilliseconds(150));

        /* The two healthy databases collected; the slow one did not, and nothing threw. */
        Assert.Equal(new[] { 6, 6 }, written);
        Assert.Equal(2, result.Rows);

        /* And it reported ITSELF, with the budget named — a skipped database that says only "cancelled"
           sends an operator looking for a shutdown that did not happen. */
        var failure = Assert.Single(errors);
        Assert.Equal("slow", failure.Item);
        Assert.Contains("wall-clock budget", failure.Message, StringComparison.Ordinal);
        Assert.Contains("re-read next cycle", failure.Message, StringComparison.Ordinal);
        /* And it names the actual NUMBER. Asserted because "contains 'wall-clock budget'" passed happily
           against a first cut that rendered this 150 ms budget as "0.0-minute" — a message with no number
           in it at all, on the one line an operator works from. */
        Assert.Contains("0.15-second", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The budget renders in the unit it was set in. The shipped value is 10 minutes, so a minutes-only
    /// format would never have looked wrong in the field — but small values are what a person types while
    /// diagnosing, which is exactly when the message matters. Found by a scratch harness, not by reading.
    /// </summary>
    [Theory]
    [InlineData(600, "10-minute")]
    [InlineData(90, "1.5-minute")]
    [InlineData(60, "1-minute")]
    [InlineData(59.5, "59.5-second")]
    [InlineData(30, "30-second")]
    [InlineData(0.15, "0.15-second")]
    public void DescribeBudget_NamesANumberAtEveryScale(double seconds, string expected)
    {
        Assert.Equal(expected, EnumeratedCollectorDriver.DescribeBudget(TimeSpan.FromSeconds(seconds)));
    }

    /// <summary>
    /// Shutdown still propagates. The budget's whole risk is misreading a real cancellation as a per-item
    /// skip, which would have the loop keep collecting through a service stop — so this is the arm that
    /// makes the feature safe rather than the one that makes it work.
    /// </summary>
    [Fact]
    public async Task RunAsync_HostShutdown_StillPropagates_EvenWithABudgetSet()
    {
        using var cts = new CancellationTokenSource();
        var errors = new List<string>();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await EnumeratedCollectorDriver.RunAsync<int>(
                new[] { "a", "b" },
                perItemWatermark: null,
                readItem: async (item, ct) =>
                {
                    await cts.CancelAsync();
                    ct.ThrowIfCancellationRequested();
                    return new List<int> { 1 };
                },
                writeBatch: (batch, ct) => Task.CompletedTask,
                onItemComplete: (item, count, sqlMs, storageMs) => { },
                onItemError: (item, ex) => errors.Add(item),
                cts.Token,
                /* Generous, so the ONLY cancelled token is the outer one — the ambiguous case is covered by
                   ItemBudgetExpired's own tests below. */
                perItemBudget: TimeSpan.FromMinutes(10)));

        Assert.Empty(errors);
    }

    /// <summary>
    /// No budget means the loop is what it always was: the delegates get the caller's own token, not a
    /// linked one. Asserted by IDENTITY rather than by behaviour, because "no wrapper" is the property —
    /// a linked token with no timer behaves identically and would hide an unnecessary allocation per item.
    /// </summary>
    [Fact]
    public async Task RunAsync_WithNoBudget_PassesTheCallersOwnToken()
    {
        using var cts = new CancellationTokenSource();
        var seen = new List<CancellationToken>();

        await EnumeratedCollectorDriver.RunAsync<int>(
            new[] { "a" },
            perItemWatermark: (item, ct) => { seen.Add(ct); return Task.CompletedTask; },
            readItem: (item, ct) => { seen.Add(ct); return Task.FromResult(new List<int>()); },
            writeBatch: (batch, ct) => Task.CompletedTask,
            onItemComplete: (item, count, sqlMs, storageMs) => { },
            onItemError: (item, ex) => { },
            cts.Token);

        Assert.Equal(2, seen.Count);
        Assert.All(seen, token => Assert.Equal(cts.Token, token));
    }

    /// <summary>
    /// The WRITE is outside the budget. Abandoning a flush already in flight would trade a slow cycle for a
    /// partially-written one, which is the worse of the two — so the write gets the caller's token even when
    /// the read was bounded.
    /// </summary>
    [Fact]
    public async Task RunAsync_TheWrite_IsNotSubjectToTheItemBudget()
    {
        using var cts = new CancellationTokenSource();
        CancellationToken readToken = default, writeToken = default;

        await EnumeratedCollectorDriver.RunAsync<int>(
            new[] { "a" },
            perItemWatermark: null,
            readItem: (item, ct) => { readToken = ct; return Task.FromResult(new List<int> { 1 }); },
            writeBatch: (batch, ct) => { writeToken = ct; return Task.CompletedTask; },
            onItemComplete: (item, count, sqlMs, storageMs) => { },
            onItemError: (item, ex) => { },
            cts.Token,
            perItemBudget: TimeSpan.FromMinutes(10));

        Assert.NotEqual(cts.Token, readToken);   // the read was bounded
        Assert.Equal(cts.Token, writeToken);     // the write was not
    }

    /// <summary>
    /// An OutOfMemoryException that fires while the budget has ALREADY expired must still propagate.
    ///
    /// <para>The reason it is not obvious: <see cref="EnumeratedCollectorDriver.ItemBudgetExpired"/>
    /// classifies on the TOKENS and never looks at the exception type — which is deliberate, because a
    /// cancelled SqlClient command does not reliably arrive as an OperationCanceledException. The cost is
    /// that the budget arm would happily claim an unrelated fatal exception, so its ORDERING behind the
    /// OOM rethrow is load-bearing rather than stylistic. Review found both hosts' per-database loops
    /// missing that ordering; this pins the shared driver's.</para>
    /// </summary>
    [Fact]
    public async Task RunAsync_AnOomWithAnExpiredBudget_StillPropagates()
    {
        var errors = new List<string>();

        await Assert.ThrowsAsync<OutOfMemoryException>(async () =>
            await EnumeratedCollectorDriver.RunAsync<int>(
                new[] { "a" },
                perItemWatermark: null,
                readItem: async (item, ct) =>
                {
                    /* Let the budget expire FIRST, then throw something unrelated and fatal. */
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5), ct);
                    }
                    catch (OperationCanceledException)
                    {
                    }

                    throw new OutOfMemoryException("unrelated to the budget");
                },
                writeBatch: (batch, ct) => Task.CompletedTask,
                onItemComplete: (item, count, sqlMs, storageMs) => { },
                onItemError: (item, ex) => errors.Add(item),
                CancellationToken.None,
                perItemBudget: TimeSpan.FromMilliseconds(100)));

        /* Not swallowed as a routine per-database timeout. */
        Assert.Empty(errors);
    }

    /// <summary>
    /// The classifier, all four combinations. It decides whether an exception is a per-item skip or a
    /// shutdown, and it is deliberately asked of the TOKENS rather than of the exception type: cancelling a
    /// SqlClient command does not reliably surface as an OperationCanceledException, so the type cannot
    /// answer this. Shutdown must win the ambiguous case, or the loop keeps collecting through a stop.
    /// </summary>
    [Fact]
    public void ItemBudgetExpired_TellsABudgetApartFromAShutdown()
    {
        using var outer = new CancellationTokenSource();
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(outer.Token);

        /* Neither: an ordinary per-item failure, which the generic catch owns. */
        Assert.False(EnumeratedCollectorDriver.ItemBudgetExpired(budget, outer.Token));

        /* No budget at all (every collector but query_store) can never be a budget expiry. */
        Assert.False(EnumeratedCollectorDriver.ItemBudgetExpired(null, outer.Token));

        /* The budget alone: a per-item skip. */
        budget.Cancel();
        Assert.True(EnumeratedCollectorDriver.ItemBudgetExpired(budget, outer.Token));

        /* Both — the race between a budget firing and a service stop. Shutdown wins. */
        outer.Cancel();
        Assert.False(EnumeratedCollectorDriver.ItemBudgetExpired(budget, outer.Token));
    }

    /// <summary>A zero or negative budget is treated as no budget rather than as an instantly-expired one,
    /// so a misconfigured value degrades to today's behaviour instead of collecting nothing at all.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void StartItemBudget_ANonPositiveBudget_IsNoBudget(int minutes)
    {
        Assert.Null(EnumeratedCollectorDriver.StartItemBudget(
            TimeSpan.FromMinutes(minutes), CancellationToken.None));
    }

    /// <summary>
    /// query_store is the collector that declares a budget, and the only one. The value is pinned because
    /// it is a published constant with a measured justification (#2150): 4.8 s median and 31 s max over 198
    /// healthy field passes against 37.6 minutes at the low end of the pathological ones.
    /// </summary>
    [Fact]
    public void TheHeavyCollectors_DeclareAWallClockBudget_AndTheLightOnesDoNot()
    {
        // query_store: the per-database plan/text fetch (#2150).
        Assert.Equal(TimeSpan.FromMinutes(10), QueryStoreCollector.Instance.PerItemWallClockBudget);
        Assert.Equal(QueryStoreCollector.PerDatabaseWallClockBudget, QueryStoreCollector.Instance.PerItemWallClockBudget);

        /* #2673: the two heaviest server-scoped collectors get a tighter bound on the execute + drain
           (measured tails 176s / 168s on prod). Bounds the tail so no collector runs minutes on a monitored
           server; a cycle that blows it ships nothing and retries next. */
        Assert.Equal(TimeSpan.FromSeconds(120), ProcedureStatsCollector.Instance.PerItemWallClockBudget);
        Assert.Equal(TimeSpan.FromSeconds(120), QueryStatsCollector.Instance.PerItemWallClockBudget);
        Assert.Equal(TimeSpan.FromSeconds(120), PlanCorrectionCollector.Instance.PerItemWallClockBudget);

        /* The light per-database siblings stay unbounded: neither has an unbounded-input shape, and a budget
           on a collector with no field evidence for one is a cut waiting to surprise somebody. */
        Assert.Null(DatabaseSizeStatsCollector.Instance.PerItemWallClockBudget);
        Assert.Null(DatabaseScopedConfigCollector.Instance.PerItemWallClockBudget);
    }

    /* ---------------- the status a returned run gets ---------------- */

    /// <summary>
    /// A cycle the #2673 whole-server budget abandoned must not be recorded as a success. It reaches the
    /// logging site by RETURNING rather than throwing, so it took the ordinary path and inherited that
    /// path's hardcoded "SUCCESS" in BOTH hosts -- while having stored nothing and advanced no watermark.
    ///
    /// <para>Observed on prod-pos-use1-ayr-01: every one of the 36 abandonments in the store's 17-day
    /// retention was status SUCCESS with rows_collected = 0. The harm is not only that a health check
    /// counting non-SUCCESS rows reported zero: DarlingSelfAlertEvaluator.ReadCollectionSignalsAsync
    /// takes last_success from status IN ('SUCCESS', 'SKIPPED'), so a collector abandoning every cycle
    /// read as perpetually FRESH.</para>
    ///
    /// <para>Asserted over the freshness-success FAMILY rather than against the "SUCCESS" literal: the
    /// bug was a status silently joining a set, and a pin naming one member would miss the next status
    /// added to that set.</para>
    /// </summary>
    [Fact]
    public void ClassifyReturnedRun_AnAbandonedCycle_IsOutsideTheFreshnessSuccessFamily()
    {
        var abandoned = EnumeratedCollectorDriver.ClassifyReturnedRun(abandoned: true);

        Assert.NotEqual("SUCCESS", abandoned);
        Assert.DoesNotContain(abandoned, EnumeratedCollectorDriver.FreshnessSuccessStatuses);

        /* And it must not have solved that by landing somewhere worse. ERROR/PERMISSIONS feed the error
           counts, the health bands and the collection-failure self-alerts, so classifying a guard that is
           working as one of those would page on healthy behaviour. YIELDED is documented as the 1s
           LOCK_TIMEOUT guard and is read as evidence of lock contention on the TARGET -- reusing it would
           send an operator hunting contention that is not there. */
        Assert.NotEqual("ERROR", abandoned);
        Assert.NotEqual("PERMISSIONS", abandoned);
        Assert.NotEqual("YIELDED", abandoned);
    }

    /// <summary>An ordinary returned run is untouched: still SUCCESS, still counted as fresh.</summary>
    [Fact]
    public void ClassifyReturnedRun_AnOrdinaryRun_IsStillSuccess()
    {
        var ordinary = EnumeratedCollectorDriver.ClassifyReturnedRun(abandoned: false);

        Assert.Equal("SUCCESS", ordinary);
        Assert.Contains(ordinary, EnumeratedCollectorDriver.FreshnessSuccessStatuses);
    }
}
