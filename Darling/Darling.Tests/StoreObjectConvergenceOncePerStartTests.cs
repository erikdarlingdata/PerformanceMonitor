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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9858: one start ran the store object convergence twice, the start-path pass and then the first hourly pass seconds later,
/// and wrote the tuning and group-index lines twice. The first hourly pass now leaves out the steps the start's own pass ran
/// without throwing. Driven with fake steps through the same step runner and selection the worker uses.
/// </summary>
public sealed class StoreObjectConvergenceOncePerStartTests
{
    private static DarlingWorker.StoreObjectConvergenceStep Step(
        string name, DarlingWorker.StoreObjectConvergenceStage stage, List<string> ran, bool throws = false) =>
        new(name, stage, DarlingWorker.StoreObjectChangeSignal.InPlace,
            (_, _, _) =>
            {
                ran.Add(name);
                return throws ? throw new InvalidOperationException("boom") : Task.FromResult(0);
            });

    private static async Task<DarlingWorker.StoreObjectConvergenceTally> RunStartPassAsync(DarlingWorker.StoreObjectConvergenceStep[] steps)
    {
        var tally = new DarlingWorker.StoreObjectConvergenceTally();
        foreach (var step in steps)
        {
            await DarlingWorker.RunStoreObjectConvergenceStepAsync(
                null!, step, tally, new CapturingTestLogger(), TestContext.Current.CancellationToken);
        }

        return tally;
    }

    [Fact]
    public async Task TheFirstHourlyPass_RunsOnlyWhatTheStartPassFailed_AndEachStepRunsOncePerStart()
    {
        var ran = new List<string>();
        var steps = new[]
        {
            Step("ok one", DarlingWorker.StoreObjectConvergenceStage.Timescale, ran),
            Step("fails at start", DarlingWorker.StoreObjectConvergenceStage.TimescaleAfterRepairLaunch, ran, throws: true),
            Step("ok two", DarlingWorker.StoreObjectConvergenceStage.Ungated, ran),
            Step("ok tuning", DarlingWorker.StoreObjectConvergenceStage.Tuning, ran),
        };

        var startTally = await RunStartPassAsync(steps);
        Assert.Equal(new[] { "ok one", "ok two", "ok tuning" }, startTally.Completed);
        Assert.Equal(new[] { "fails at start" }, startTally.Failed);

        var ranAtStartup = new HashSet<string>(startTally.Completed, StringComparer.Ordinal);
        var firstHourly = DarlingWorker.SelectHourlyConvergenceSteps(steps, timescaleAvailable: true, ranAtStartup, out var skipped);

        Assert.Equal(new[] { "fails at start" }, firstHourly.Select(s => s.Name));
        Assert.Equal(3, skipped);

        /* The pass after the first has no set to skip with: the whole list, on its own schedule. */
        var laterHourly = DarlingWorker.SelectHourlyConvergenceSteps(steps, timescaleAvailable: true, ranAtStartup: null, out var laterSkipped);
        Assert.Equal(steps.Select(s => s.Name), laterHourly.Select(s => s.Name));
        Assert.Equal(0, laterSkipped);
    }

    [Fact]
    public async Task AStepTheStartNeverReached_StillRunsOnTheFirstHourlyPass()
    {
        /* A store that was plain PostgreSQL at start ran only the Ungated and Tuning segments. The hourly pass that finds
           TimescaleDB available after all must still run the TimescaleDB steps, and skip the two the start ran. */
        var ran = new List<string>();
        var timescale = Step("timescale step", DarlingWorker.StoreObjectConvergenceStage.Timescale, ran);
        var ungated = Step("ungated step", DarlingWorker.StoreObjectConvergenceStage.Ungated, ran);
        var tuning = Step("tuning step", DarlingWorker.StoreObjectConvergenceStage.Tuning, ran);

        var startTally = await RunStartPassAsync(new[] { ungated, tuning });
        var ranAtStartup = new HashSet<string>(startTally.Completed, StringComparer.Ordinal);
        var all = new[] { timescale, ungated, tuning };

        var plain = DarlingWorker.SelectHourlyConvergenceSteps(all, timescaleAvailable: false, ranAtStartup, out _);
        Assert.Empty(plain);

        var reprobed = DarlingWorker.SelectHourlyConvergenceSteps(all, timescaleAvailable: true, ranAtStartup, out var skipped);
        Assert.Equal(new[] { "timescale step" }, reprobed.Select(s => s.Name));
        Assert.Equal(2, skipped);
    }

    [Fact]
    public void WhenEveryRealStepRanAtStartup_TheFirstHourlyPassRunsNone_AndALaterPassRunsAll()
    {
        var ranAtStartup = new HashSet<string>(DarlingWorker.StoreObjectConvergence.Select(s => s.Name), StringComparer.Ordinal);

        var first = DarlingWorker.SelectHourlyConvergenceSteps(DarlingWorker.StoreObjectConvergence, timescaleAvailable: true, ranAtStartup, out var skipped);
        Assert.Empty(first);
        Assert.Equal(DarlingWorker.StoreObjectConvergence.Count, skipped);

        var later = DarlingWorker.SelectHourlyConvergenceSteps(DarlingWorker.StoreObjectConvergence, timescaleAvailable: true, null, out _);
        Assert.Equal(DarlingWorker.StoreObjectConvergence.Count, later.Count);
    }
}
