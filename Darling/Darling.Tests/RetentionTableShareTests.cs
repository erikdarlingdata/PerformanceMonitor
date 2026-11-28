/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9956: a table's share of the pass's wall budget (<see cref="RetentionCollectionYield.BeginTableShare"/>). On a fake clock that
/// only the batches and the delays move: the drain stops at the share, the table counts as left without stopping the pass, and the
/// next table drains in full.
/// </summary>
public sealed class RetentionTableShareTests
{
    private sealed class NeverBehind : ICollectionPressure
    {
        public string? BehindReason() => null;
    }

    private sealed class FakeTime
    {
        public double Now;

        public double Clock() => Now;

        public Task Delay(TimeSpan wait, CancellationToken cancellationToken)
        {
            Now += wait.TotalSeconds;
            return Task.CompletedTask;
        }
    }

    /* A table whose batches take `cap` rows and `seconds` of fake time each. */
    private static Func<CancellationToken, Task<(int Deleted, int Cap, long WalBytes)>> Table(FakeTime time, int rows, int cap, double seconds)
    {
        var left = rows;
        return _ =>
        {
            time.Now += seconds;
            var deleted = Math.Min(cap, left);
            left -= deleted;
            return Task.FromResult((deleted, cap, 0L));
        };
    }

    [Fact]
    public async Task ADrainStopsAtItsShare_CountsAsATableLeft_AndThePassGoesOnToTheNextTable()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = new FakeTime();
        var gate = new RetentionCollectionYield(new NeverBehind(), TimeSpan.FromSeconds(1_800), logger: null, time.Delay, time.Clock);
        var pacer = new RetentionWalPacer(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask) { Yield = gate };

        /* A third of 1,800 s is 600 s. Each batch takes 250 s and pauses 60 s after it: 310 s, then 620 s, past the share. */
        gate.BeginTableShare(1.0 / 3);
        var moved = await DarlingRetention.DrainBatchesAsync(Table(time, rows: 1_000, cap: 100, seconds: 250), pacer, ct);
        Assert.True(gate.ShareStopPending);
        Assert.False(gate.BudgetStopPending);
        gate.EndTableShare();
        gate.FinishTable("collect.query_tag_sightings", moved);

        Assert.Equal(200, moved);
        Assert.Equal(600, gate.ShareSeconds);
        Assert.Equal(1, gate.TablesStoppedOnShare);
        Assert.Equal("collect.query_tag_sightings", gate.FirstShareStoppedTable);
        Assert.Equal(1, gate.TablesLeft);
        Assert.Equal(0, gate.TablesNotReached);
        Assert.False(gate.StoppedOnBudget);
        Assert.False(gate.ShareStopPending);

        /* The next table has no share: it drains all of its rows inside the pass's budget. */
        var after = await DarlingRetention.DrainBatchesAsync(Table(time, rows: 250, cap: 100, seconds: 10), pacer, ct);
        gate.FinishTable("collect.query_tag_sightings_slim", after);
        Assert.Equal(250, after);
        Assert.Equal(1, gate.TablesLeft);
        Assert.False(gate.StoppedOnBudget);

        var note = gate.Describe();
        Assert.NotNull(note);
        Assert.Contains("the drain of collect.query_tag_sightings stopped at its 10-minute share of the time budget", note, StringComparison.Ordinal);
        Assert.DoesNotContain("stopped at its 30-minute time budget", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ThePassBudget_StillStopsADrainThatHasAShare_AsABudgetStop()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = new FakeTime();
        var gate = new RetentionCollectionYield(new NeverBehind(), TimeSpan.FromSeconds(1_800), logger: null, time.Delay, time.Clock);
        var pacer = new RetentionWalPacer(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask) { Yield = gate };

        /* The share would end at 1,700 s + 600 s, and the pass's budget ends first, at 1,800 s. */
        time.Now = 1_700;
        gate.BeginTableShare(1.0 / 3);
        var moved = await DarlingRetention.DrainBatchesAsync(Table(time, rows: 1_000, cap: 100, seconds: 40), pacer, ct);
        Assert.False(gate.ShareStopPending);
        Assert.True(gate.BudgetStopPending);
        gate.EndTableShare();
        gate.FinishTable("collect.query_tag_sightings", moved);

        Assert.Equal(200, moved);
        Assert.True(gate.StoppedOnBudget);
        Assert.Equal(0, gate.TablesStoppedOnShare);
        Assert.Equal(1, gate.TablesLeft);
    }
}
