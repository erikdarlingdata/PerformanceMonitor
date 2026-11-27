/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9192: a <c>collection_cycle</c> row that blames the wait for a fleet collection slot names what held the slots. Two real
/// collection bodies share one fleet slot. The first runs a collector that a test holds open, then lets it finish with a
/// stated read and store-write time. The second queues behind it, gets the slot when the first lets go, and writes its row at
/// the end of its own body. The time the second body reports for its wait is stated through
/// <see cref="DarlingWorker.GateWaitedForTests"/>, so the test needs no 30-second wait.
/// </summary>
public sealed class FleetSlotHolderTests
{
    private const string HeldCollector = "pg_buffer_usage";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    private static (DarlingWorker Worker, List<CycleSkipReport> Rows) NewWorker()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var rows = new List<CycleSkipReport>();
        worker.CycleSkipWriteOverride = report =>
        {
            lock (rows)
            {
                rows.Add(report);
            }

            return Task.FromResult(true);
        };
        return (worker, rows);
    }

    private static DarlingWorker.ServerLoopState PostgresServer(string name, int serverId)
    {
        var config = new MonitoredServer { Name = name, Host = name };
        return new DarlingWorker.ServerLoopState
        {
            Config = config,
            Runtime = new ServerRuntime
            {
                Config = config,
                ConnectionString = $"Host={name};Port=5432",
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                StorageName = name,
                ServerId = serverId,
            },
        };
    }

    private static Task Body(DarlingWorker worker, DarlingWorker.ServerLoopState server, SemaphoreSlim fleetGate)
    {
        var process = typeof(DarlingWorker).GetMethod("ProcessServerSweepAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var config = new DarlingConfig();
        config.Analysis.Enabled = false;
        return (Task)process.Invoke(worker, new object?[] { server, null, null, null, null, config, fleetGate, Ct })!;
    }

    /// <summary>
    /// Runs the scenario: the first body holds the one fleet slot with the collector open, the second queues behind it
    /// carrying slots it lost, the first lets go with the stated split, and the second reports its wait as
    /// <paramref name="reportedWait"/>. Returns the row the second body wrote.
    /// </summary>
    private static async Task<CycleSkipReport> SecondBodyRowAsync(long readMs, long storeMs, TimeSpan reportedWait)
    {
        var (worker, rows) = NewWorker();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var letGo = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.DispatchOverrideForTests = async (_, _, _) =>
        {
            entered.TrySetResult();
            await letGo.Task;
            return new CollectorRunResult(10, readMs, storeMs, []);
        };
        worker.GateWaitedForTests = _ => reportedWait;

        var holder = PostgresServer("holder", 1);
        holder.NextDue[HeldCollector] = DateTime.UtcNow.AddMinutes(-5);
        var late = PostgresServer("late", 2);
        late.CycleSkips.AddGrid(HeldCollector, 3, DateTime.UtcNow.AddMinutes(-3));

        using var fleetGate = new SemaphoreSlim(1, 1);
        var first = Body(worker, holder, fleetGate);
        await entered.Task.WaitAsync(Patience, Ct);

        var second = Body(worker, late, fleetGate);
        Assert.False(second.IsCompleted, "the second body must be parked on the full fleet gate");
        Assert.Equal(0, fleetGate.CurrentCount);

        letGo.SetResult();
        await first.WaitAsync(Patience, Ct);
        await second.WaitAsync(Patience, Ct);

        lock (rows)
        {
            return Assert.Single(rows);
        }
    }

    [Fact]
    public async Task ARowThatBlamesTheSlotWait_NamesTheCollectorThatHeldTheSlot_AndPutsItsTimeInTheStorePhase()
    {
        var row = await SecondBodyRowAsync(readMs: 900, storeMs: 7_100, TimeSpan.FromSeconds(45));

        Assert.Equal(DarlingCycleSkip.GateWaitName, row.SlowestItem);
        Assert.Contains("this pass waited 45s for a fleet collection slot", row.Message, StringComparison.Ordinal);
        Assert.Contains("slots were held by 1 collection pass during the wait", row.Message, StringComparison.Ordinal);
        Assert.Contains("longest collector: pg buffer usage 8s, slowest in 1 of them, store write 7.1s of 8s", row.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("monitored-server read", row.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARowThatBlamesTheSlotWait_PutsTheHolderTimeInTheReadPhase_WhenTheMonitoredServerHeldIt()
    {
        var row = await SecondBodyRowAsync(readMs: 7_100, storeMs: 900, TimeSpan.FromSeconds(45));

        Assert.Equal(DarlingCycleSkip.GateWaitName, row.SlowestItem);
        Assert.Contains("longest collector: pg buffer usage 8s, slowest in 1 of them, monitored-server read 7.1s of 8s", row.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("store write", row.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARowWhoseWaitIsUnderTheCauseFloor_NamesNoHolder_EvenThoughABodyHeldTheSlot()
    {
        var row = await SecondBodyRowAsync(readMs: 900, storeMs: 7_100, TimeSpan.FromSeconds(5));

        Assert.DoesNotContain("longest collector", row.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("held by", row.Message, StringComparison.Ordinal);
        Assert.NotEqual(DarlingCycleSkip.GateWaitName, row.SlowestItem);
    }

    [Fact]
    public void TheHolder_IsTheLongestCollectorOfTheBodiesThatHeldSlotsDuringTheWait_AndCountsTheBodiesItWasSlowestIn()
    {
        var holds = new FleetSlotHolds();
        holds.Record(new SlotHold("before_the_wait", 90_000, 1, 89_999));

        var mark = holds.Mark();
        holds.Record(new SlotHold("query_stats", 8_000, 900, 7_100));
        holds.Record(new SlotHold("wait_stats", 2_000, 1_000, 1_000));
        holds.Record(new SlotHold("query_stats", 3_000, 500, 2_500));
        holds.Record(new SlotHold("procedure_stats", 12_000, 11_000, 1_000));

        var holder = holds.HolderSince(mark, DarlingCycleSkip.CauseFloorMs);

        Assert.NotNull(holder);
        Assert.Equal("procedure_stats", holder.Collector);
        Assert.Equal(12_000, holder.TotalMs);
        Assert.Equal(1, holder.BodiesSlowestIn);
        Assert.Equal(4, holder.Bodies);
        Assert.Equal(11_000, holder.ReadMs);

        Assert.Null(holds.HolderSince(mark, DarlingCycleSkip.CauseFloorMs - 1));
        Assert.Null(holds.HolderSince(holds.Mark(), DarlingCycleSkip.CauseFloorMs));
    }

    [Fact]
    public void TheLedger_KeepsOnlyTheLastHolds_SoALongWaitCannotGrowIt()
    {
        var holds = new FleetSlotHolds();
        var mark = holds.Mark();
        for (var i = 0; i < FleetSlotHolds.Capacity + 50; i++)
        {
            holds.Record(new SlotHold("query_stats", 1_000 + i, null, null));
        }

        var holder = holds.HolderSince(mark, DarlingCycleSkip.CauseFloorMs);

        Assert.NotNull(holder);
        Assert.Equal(FleetSlotHolds.Capacity, holder.Bodies);
    }
}
