/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8539: a server writes at most one <c>collection_cycle</c> row in <see cref="DarlingCycleSkip.RowMinInterval"/>, and the
/// slots it loses in between wait for the next row. The body that lost them measured why (its wait for a fleet slot and how
/// many slots analysis passes held, its previous body, its previous analysis pass), and the row that finally reports them is
/// written by a later body whose own wait was short. These tests lose slots under one body's causes and have a quick body
/// write the row: through the worker's flush for the bodies inside the interval, and through the accumulator and the report
/// builder, with a stamp past the interval, for the body that writes.
/// </summary>
public sealed class CycleSkipCarriedCauseTests
{
    private const string NoCause = "no single cause was measured";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A body with nothing to blame: a three-second previous body and a wait under half a second.</summary>
    private static CycleSkipCauses Quick => new(PrevBodyMs: 3_000, PrevBodySlowestName: "wait_stats", PrevBodySlowestMs: 1_200, GateWaitMs: 400);

    private static CycleSkipCauses WaitedForASlot(long waitMs, int slotsHeldByAnalysis) =>
        new(PrevBodyMs: 3_000, PrevBodySlowestName: "wait_stats", PrevBodySlowestMs: 1_200, GateWaitMs: waitMs,
            PrevAnalysisMs: 0, GateWaitAnalysisHolders: slotsHeldByAnalysis);

    private sealed class Rows
    {
        internal List<CycleSkipReport> Written { get; } = new();

        /// <summary>Whether the next row's write lands. A write that does not land keeps its slots pending.</summary>
        internal bool Lands { get; set; } = true;
    }

    private static (DarlingWorker Worker, DarlingWorker.ServerLoopState Server, Rows Rows) NewServer()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var rows = new Rows();
        worker.CycleSkipWriteOverride = report =>
        {
            if (rows.Lands)
            {
                rows.Written.Add(report);
            }

            return Task.FromResult(rows.Lands);
        };
        return (worker, SelfAlertFleetPassTests.MakeServer("server", connected: true), rows);
    }

    /// <summary>One body: it steps over <paramref name="slots"/> one-minute slots, then reaches the flush with what it
    /// measured.</summary>
    private static Task BodyAsync(DarlingWorker worker, DarlingWorker.ServerLoopState server, int slots, CycleSkipCauses causes)
    {
        if (slots > 0)
        {
            server.CycleSkips.AddGrid("wait_stats", slots, DateTime.UtcNow.AddMinutes(-slots));
        }

        return worker.FlushCycleSkipAsync(server, server.Runtime!, causes, Ct);
    }

    /// <summary>The row a body with <paramref name="causes"/> writes once the interval since the last row has passed.</summary>
    private static CycleSkipReport RowAfterTheInterval(DarlingWorker.ServerLoopState server, CycleSkipCauses causes)
    {
        var stamp = DateTime.UtcNow + DarlingCycleSkip.RowMinInterval + TimeSpan.FromMinutes(1);
        var taken = server.CycleSkips.TryTake(stamp, DarlingCycleSkip.RowMinInterval);
        Assert.NotNull(taken);
        var report = DarlingCycleSkip.Build(taken, causes, stamp);
        Assert.NotNull(report);
        return report;
    }

    [Fact]
    public async Task TheLateSecondRow_NamesTheSlotWait_AndTheAnalysisHold_OfTheBodyThatLostTheSlots()
    {
        var (worker, server, rows) = NewServer();

        /* The first late body writes its row at once, with its own cause. */
        await BodyAsync(worker, server, slots: 1, WaitedForASlot(75_000, slotsHeldByAnalysis: 4));
        Assert.Single(rows.Written);
        Assert.Contains("this pass waited 75s for a fleet collection slot", rows.Written[0].Message, StringComparison.Ordinal);

        /* A minute later the next body waits 190 s behind four analysis passes and steps over three more slots. Its row
           is held back: the last one is under five minutes old. The bodies after it are quick and lose nothing. */
        await BodyAsync(worker, server, slots: 3, WaitedForASlot(190_000, slotsHeldByAnalysis: 4));
        await BodyAsync(worker, server, slots: 0, Quick);
        await BodyAsync(worker, server, slots: 0, Quick);
        Assert.Single(rows.Written);

        var late = RowAfterTheInterval(server, Quick);

        Assert.Equal(3, late.SlotsSkipped);
        Assert.DoesNotContain(NoCause, late.Message, StringComparison.Ordinal);
        Assert.Contains(
            "an earlier pass waited 190s for a fleet collection slot (4 slots were held by scheduled analysis passes when it got one)",
            late.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("this pass waited", late.Message, StringComparison.Ordinal);
        Assert.Equal(DarlingCycleSkip.GateWaitName, late.SlowestItem);
        Assert.Equal(190_000, late.DurationMs);
        Assert.Equal(190_000, late.SlowestItemMs);
    }

    [Fact]
    public async Task TheLateSecondRow_NamesAnEarlierBodysAnalysisPass_AndItsSlowPreviousBody()
    {
        var (worker, server, rows) = NewServer();
        await BodyAsync(worker, server, slots: 1, WaitedForASlot(75_000, slotsHeldByAnalysis: 2));

        /* Two more bodies lose a slot each inside the interval: one behind its own server's 65 s analysis pass, one behind
           a 48 s previous body. */
        await BodyAsync(worker, server, slots: 1, new CycleSkipCauses(
            PrevBodyMs: 2_000, PrevBodySlowestName: "wait_stats", PrevBodySlowestMs: 900, GateWaitMs: 300, PrevAnalysisMs: 65_000));
        await BodyAsync(worker, server, slots: 1, new CycleSkipCauses(
            PrevBodyMs: 48_000, PrevBodySlowestName: "query_stats", PrevBodySlowestMs: 39_000, GateWaitMs: 300));
        Assert.Single(rows.Written);

        var late = RowAfterTheInterval(server, Quick);

        Assert.Equal(2, late.SlotsSkipped);
        Assert.Contains(
            "an earlier pass's scheduled analysis held this server's fleet collection slot for 65s after its collectors",
            late.Message, StringComparison.Ordinal);
        Assert.Contains(
            "an earlier collection pass took 48s and was still running when the next one was due (slowest: query stats 39s)",
            late.Message, StringComparison.Ordinal);
        Assert.Equal(DarlingCycleSkip.AnalysisPassName, late.SlowestItem);
        Assert.Equal(65_000, late.DurationMs);
    }

    [Fact]
    public async Task ARowWhoseWriteDidNotLand_KeepsItsCauseForTheBodyThatWritesItNext()
    {
        var (worker, server, rows) = NewServer();
        rows.Lands = false;
        await BodyAsync(worker, server, slots: 2, WaitedForASlot(170_000, slotsHeldByAnalysis: 3));
        Assert.Empty(rows.Written);

        rows.Lands = true;
        await BodyAsync(worker, server, slots: 0, Quick);

        var row = Assert.Single(rows.Written);
        Assert.Equal(2, row.SlotsSkipped);
        Assert.Contains(
            "an earlier pass waited 170s for a fleet collection slot (3 slots were held by scheduled analysis passes when it got one)",
            row.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheWritingBodysOwnLongerWait_IsStillWordedAsItsOwn()
    {
        var (worker, server, rows) = NewServer();
        await BodyAsync(worker, server, slots: 1, WaitedForASlot(75_000, slotsHeldByAnalysis: 4));
        await BodyAsync(worker, server, slots: 1, WaitedForASlot(60_000, slotsHeldByAnalysis: 2));
        Assert.Single(rows.Written);

        server.CycleSkips.AddGrid("wait_stats", 2, DateTime.UtcNow.AddMinutes(-2));
        var late = RowAfterTheInterval(server, WaitedForASlot(140_000, slotsHeldByAnalysis: 5));

        Assert.Equal(3, late.SlotsSkipped);
        Assert.Contains(
            "this pass waited 140s for a fleet collection slot (5 slots were held by scheduled analysis passes when it got one)",
            late.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("an earlier pass waited", late.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARowThatLands_TakesItsCausesWithIt_SoLaterSlotsDoNotInheritThem()
    {
        var (worker, server, rows) = NewServer();
        rows.Lands = false;
        await BodyAsync(worker, server, slots: 2, WaitedForASlot(170_000, slotsHeldByAnalysis: 3));
        rows.Lands = true;
        await BodyAsync(worker, server, slots: 0, Quick);
        Assert.Single(rows.Written);

        /* Slots lost after that row, by bodies that measured nothing long: the next row has no cause to name. */
        await BodyAsync(worker, server, slots: 1, Quick);
        var late = RowAfterTheInterval(server, Quick);

        Assert.Equal(1, late.SlotsSkipped);
        Assert.Contains(NoCause, late.Message, StringComparison.Ordinal);
        Assert.Null(late.DurationMs);
    }

    [Fact]
    public async Task ABodyThatMeasuresALongWait_WithNoSlotPending_LeavesNothingForALaterRow()
    {
        var (worker, server, _) = NewServer();

        /* A long wait that cost no slot is not a cause of anything: slots lost afterwards are not blamed on it. */
        await BodyAsync(worker, server, slots: 0, WaitedForASlot(55_000, slotsHeldByAnalysis: 2));
        server.CycleSkips.AddGrid("wait_stats", 1, DateTime.UtcNow.AddMinutes(-1));

        var row = RowAfterTheInterval(server, Quick);

        Assert.Contains(NoCause, row.Message, StringComparison.Ordinal);
    }
}
