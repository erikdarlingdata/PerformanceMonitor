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
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8361: the scheduler does not run <c>query_store_hints</c> on a server whose every Query Store database is a readable
/// secondary. The collector's own statement skips such a database, so a run there reads the database list, collects nothing
/// and logs a success with zero rows every slot. The slot is served the way a refused collector's backoff serves one (#6186):
/// counted, the stamp moved on, and no run, so no collection_log row. A primary runs it, and the gate follows the server's
/// last known role in both directions.
/// </summary>
public sealed class QueryStoreHintsSecondaryGateTests
{
    private static readonly string Hints = QueryStoreHintsCollector.Instance.Name;
    private const string Peer = "wait_stats";

    private sealed record Rig(DarlingWorker Worker, DarlingWorker.ServerLoopState Server, Dictionary<string, int> Runs, bool[] Secondary);

    private static Rig MakeRig(int serverId)
    {
        var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
        var server = DailyRunKit.MakeServer(serverId, Hints);
        var runs = new Dictionary<string, int>(StringComparer.Ordinal);
        var secondary = new[] { true };
        worker.RunOneBodyOverride = (_, _, collector, _) =>
        {
            runs[collector] = runs.GetValueOrDefault(collector) + 1;
            return Task.FromResult(1);
        };
        SetRoleSeam(worker, id => id == serverId && secondary[0]);
        return new Rig(worker, server, runs, secondary);
    }

    /// <summary>
    /// The seam the worker reads the server's last known role through in a test, standing in for the store's facts: true means
    /// every Query Store database on the server is a readable secondary.
    /// </summary>
    private static void SetRoleSeam(DarlingWorker worker, Func<int, bool> answer)
    {
        var seam = typeof(DarlingWorker).GetProperty(
            "QueryStoreReadableSecondaryOverride", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        if (seam is null)
        {
            Assert.Fail("DarlingWorker has no QueryStoreReadableSecondaryOverride seam: the scheduler does not gate query_store_hints on the server's role.");
        }

        seam.SetValue(worker, answer);
    }

    /// <summary>One scheduler pass with the named collectors due, as they are every interval.</summary>
    private static async Task PassAsync(Rig rig, params string[] due)
    {
        foreach (var name in due)
        {
            rig.Server.NextDue[name] = DateTime.UtcNow.AddMinutes(-10);
        }

        await rig.Worker.RunDueCollectorsAsync(rig.Server, null!, TestContext.Current.CancellationToken);
        await Task.WhenAll(rig.Worker.InFlightDetachedRuns);
    }

    private static int RunsOf(Rig rig, string collector) => rig.Runs.GetValueOrDefault(collector);

    [Fact]
    public async Task OnAReadableSecondary_TheHintsSlotIsServedWithoutARun_AndItsPeersStillRun()
    {
        var rig = MakeRig(8361_01);
        var stamp = DateTime.UtcNow.AddMinutes(-10);

        await PassAsync(rig, Hints, Peer);

        Assert.Equal(0, RunsOf(rig, Hints));
        Assert.Equal(1, RunsOf(rig, Peer));
        Assert.True(rig.Server.NextDue[Hints] > stamp, "the slot was served: the stamp moved on, so the gate is asked again at the next slot, not every tick");
    }

    [Fact]
    public async Task APrimaryRunsIt_AndTheGateFollowsTheRoleBothWays()
    {
        var rig = MakeRig(8361_02);

        rig.Secondary[0] = false;
        await PassAsync(rig, Hints);
        Assert.Equal(1, RunsOf(rig, Hints));

        rig.Secondary[0] = true;
        await PassAsync(rig, Hints);
        Assert.Equal(1, RunsOf(rig, Hints));

        rig.Secondary[0] = false;
        await PassAsync(rig, Hints);
        Assert.Equal(2, RunsOf(rig, Hints));
    }

    [Fact]
    public async Task AGatedSlot_IsCountedByTheFleetGate_LikeAnyOtherSlot()
    {
        /* The fleet gate measures whether the sweep loop keeps cadence. A slot the grid served is counted whether or not the
           role gate then holds the collector back from running it. */
        var rig = MakeRig(8361_03);
        rig.Worker.SkipCreditFloorForTest.Resume(DateTime.UtcNow.AddHours(-1));
        const int passes = 3;

        for (var pass = 0; pass < passes; pass++)
        {
            await PassAsync(rig, Hints);
        }

        Assert.Equal(0, RunsOf(rig, Hints));
        var snapshot = rig.Worker.FleetGateStatsForTest!.Snapshot();
        Assert.Equal(passes, snapshot.Run);
    }
}
